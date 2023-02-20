package lazydb

import (
	"errors"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type (
	LazyDB struct {
		cfg              *DBConfig
		index            *ConcurrentMap[string]
		fidsMap          map[valueType]*MutexFids
		activeLogFileMap map[valueType]*MutexLogFile
		archivedLogFile  map[valueType]*ConcurrentMap[uint32] // [uint32]*MutexLogFile
		mu               sync.RWMutex
	}

	MutexFids struct {
		fids []uint32
		mu   sync.RWMutex
	}

	MutexLogFile struct {
		lf *logfile.LogFile
		mu sync.RWMutex
	}

	valueType uint8

	Value struct {
		value     []byte
		vType     valueType
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	// 写LogFile之后返回位置信息的结构体
	ValuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}
)

const (
	valueTypeString valueType = iota
	valueTypeList
	valueTypeHash
	valueTypeSet
	valueTypeZSet
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrLogFileNotExist = errors.New("log file is not exist")
	ErrOpenLogFile     = errors.New("open Log file error")
)

func Open(cfg DBConfig) (*LazyDB, error) {
	// create the dir path if not exist
	if !util.PathExist(cfg.DBPath) {
		if err := os.MkdirAll(cfg.DBPath, os.ModePerm); err != nil {
			log.Fatalf("Create db directory in %s error: %v", cfg.DBPath, err)
			return nil, err
		}
	}

	db := &LazyDB{
		cfg:              &cfg,
		index:            NewConcurrentMap(int(cfg.HashIndexShardCount)),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ConcurrentMap[uint32]),
	}

	if err := db.buildLogFiles(); err != nil {
		log.Fatalf("Build Log Files error: %v", err)
		return nil, err
	}

	// todo: add methods for building index

	return db, nil
}

// Sync flush the buffer into stable storage.
func (db *LazyDB) Sync() error {
	for _, mlf := range db.activeLogFileMap {
		mlf.mu.Lock()
		if err := mlf.lf.Sync(); err != nil {
			return err
		}
		mlf.mu.Unlock()
	}
	return nil
}

// Close db
func (db *LazyDB) Close() error {
	for _, mlf := range db.activeLogFileMap {
		mlf.lf.Close()
	}
	for typ, mutexFids := range db.fidsMap {
		for _, fid := range mutexFids.fids {
			mlf := db.getArchivedLogFile(typ, fid)
			if mlf == nil {
				continue
			}
			mlf.lf.Sync()
			mlf.lf.Close()
		}
	}
	db.index = nil
	db.fidsMap = nil
	db.activeLogFileMap = nil
	db.archivedLogFile = nil
	return nil
}

func (db *LazyDB) Merge(typ valueType, targetFid uint32) error {
	return nil
}

// readLogEntry Reads entry from log files by fid and offset.
// Return error if entry does not exist.
func (db *LazyDB) readLogEntry(typ valueType, fid uint32, offset int64) (*logfile.LogEntry, error) {
	var lf *logfile.LogFile
	activelf := db.activeLogFileMap[typ]

	lf = activelf.lf
	if lf == nil {
		return nil, ErrOpenLogFile
	}

	if lf.Fid != fid {
		mlf := db.getArchivedLogFile(typ, fid)
		if mlf == nil || mlf.lf == nil {
			return nil, ErrLogFileNotExist
		}
		lf = mlf.lf
	}
	lf.Mu.RLock()
	defer lf.Mu.RUnlock()
	entry, _, err := lf.ReadLogEntry(offset)
	return entry, err
}

// writeLogEntry writes entry into active log file and returns position.
// Return nil and error if writing fails.
func (db *LazyDB) writeLogEntry(typ valueType, entry *logfile.LogEntry) (*ValuePos, error) {
	activeLogFile := db.activeLogFileMap[typ]
	activeLogFile.mu.Lock()
	defer activeLogFile.mu.Unlock()

	lf := activeLogFile.lf
	entBuf, entSize := logfile.EncodeEntry(entry)

	// maxsize exceeded
	if lf.Offset+int64(entSize) > db.cfg.MaxLogFileSize {
		if err := lf.Sync(); err != nil {
			return nil, err
		}

		newFid := lf.Fid + 1
		newActiveLF, err := logfile.Open(db.cfg.DBPath, newFid, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
		if err != nil {
			return nil, err
		}

		// move activeLogFile to archive
		db.archivedLogFile[typ].Set(newFid, &MutexLogFile{lf: newActiveLF})

		// insert new fid
		fids := db.fidsMap[typ]
		fids.mu.Lock()
		fids.fids = append(fids.fids, newFid)
		fids.mu.Unlock()

		// update activeLogFile
		activeLogFile.lf = newActiveLF
	}

	lf = activeLogFile.lf
	writeAt := lf.Offset
	if err := lf.Write(entBuf); err != nil {
		return nil, err
	}
	valPos := &ValuePos{
		fid:       lf.Fid,
		offset:    writeAt,
		entrySize: entSize,
	}
	return valPos, nil
}

// buildLogFiles Recover archivedLogFile from disk.
// Only run once when program start running.
func (db *LazyDB) buildLogFiles() error {
	fileInfos, err := os.ReadDir(db.cfg.DBPath)
	if err != nil {
		return err
	}
	for _, file := range fileInfos {
		if !strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			continue
		}
		splitInfo := strings.Split(file.Name(), ".")
		if len(splitInfo) != 3 {
			log.Printf("Invalid log file name: %s", file.Name())
			continue
		}
		typ := valueType(logfile.FileTypesMap[splitInfo[1]])
		fid, err := strconv.Atoi(splitInfo[2])
		if err != nil {
			log.Printf("Invalid log file name: %s", file.Name())
			continue
		}
		fids := db.fidsMap[typ]
		fids.fids = append(fids.fids, uint32(fid))
	}

	for typ := valueTypeString; typ < valueTypeZSet; typ++ {
		fids := db.fidsMap[typ].fids
		// newly created log file has bigger fid
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		archivedLogFiles := NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
		for i, fid := range fids {
			lf, err := logfile.Open(db.cfg.DBPath, fid, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
			if err != nil {
				log.Fatalf("Open Log File error:%v. Type: %v, Fid: %v,", err, typ, fid)
				continue
			}

			// latest one is the active log file
			if i == len(fids)-1 {
				activeMutexLogFile := db.activeLogFileMap[typ]
				activeMutexLogFile.mu.Lock()
				activeMutexLogFile.lf = lf
				activeMutexLogFile.mu.Unlock()
			} else {
				archivedLogFiles.Set(fid, &MutexLogFile{lf: lf})
			}
		}
		db.archivedLogFile[typ] = archivedLogFiles
	}

	return nil
}

// getArchivedLogFile Util function for get archivedLogFile from ConcurrentMap.
// Returns nil when target log file does not exist
func (db *LazyDB) getArchivedLogFile(typ valueType, fid uint32) *MutexLogFile {
	lfs, ok := db.archivedLogFile[typ]
	if !ok {
		db.archivedLogFile[typ] = NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
		return nil
	}
	v, ok := lfs.Get(fid)
	if !ok {
		return nil
	}
	lf := v.(*MutexLogFile)
	return lf
}
