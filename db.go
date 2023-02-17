package lazydb

import (
	"errors"
	"lazydb/logfile"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type (
	LazyDB struct {
		cfg             *DBConfig
		index           *ConcurrentMap[string]
		fidsMap         *ConcurrentMap[valueType]            // [valueType][]uint32
		curLogFile      *ConcurrentMap[valueType]            // [valueType]*LogFile
		archivedLogFile map[valueType]*ConcurrentMap[uint32] // [uint32]*LogFile
		mu              sync.RWMutex
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
	ErrLogFileNotExist = errors.New("Log file is not exist")
	ErrOpenLogFile     = errors.New("Open Log file error")
)

func Open() (*LazyDB, error) {
	return nil, nil
}

func (db *LazyDB) Sync() error {
	return nil
}

func (db *LazyDB) Close() error {
	return nil
}

func (db *LazyDB) Merge(typ valueType, targetFid uint32) error {
	return nil
}

func (db *LazyDB) getValue(key []byte) (*Value, error) {
	return nil, nil
}

// readLogEntry Reads entry from log files by fid and offset.
// Return error if entry does not exist.
func (db *LazyDB) readLogEntry(typ valueType, fid uint32, offset int64) (*logfile.LogEntry, error) {
	var lf *logfile.LogFile

	curLogFile := db.getCurLogFile(typ)
	if curLogFile == nil {
		return nil, ErrOpenLogFile
	}

	if curLogFile.Fid == fid {
		lf = curLogFile
	} else {
		lf = db.getArchivedLogFile(typ, fid)
		if lf == nil {
			return nil, ErrLogFileNotExist
		}
	}

	entry, _, err := lf.ReadLogEntry(offset)
	return entry, err
}

// writeLogEntry writes entry into active log file and returns position.
// Return nil and error if writing fails.
func (db *LazyDB) writeLogEntry(typ valueType, entry *logfile.LogEntry) (*ValuePos, error) {
	curLogFile := db.getCurLogFile(typ)
	if curLogFile == nil {
		return nil, ErrOpenLogFile
	}

	entBuf, entSize := logfile.EncodeEntry(entry)

	curLogFile.Mu.RLock()
	// maxsize exceeded
	if curLogFile.Offset+int64(entSize) > db.cfg.MaxLogFileSize {
		if err := curLogFile.Sync(); err != nil {
			curLogFile.Mu.RUnlock()
			return nil, err
		}

		newFid := curLogFile.Fid + 1
		newCurLogFile, err := logfile.Open(db.cfg.DBPath, newFid, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
		if err != nil {
			curLogFile.Mu.RUnlock()
			return nil, err
		}

		// move curLogFile to archive
		db.archivedLogFile[typ].Set(newFid, newCurLogFile)

		// insert new fid
		fidList := db.getFidListByType(typ)
		fidList = append(fidList, newFid)
		db.fidsMap.Set(typ, fidList)

		// update curLogFile
		db.curLogFile.Set(typ, newCurLogFile)
		curLogFile.Mu.RUnlock()
		curLogFile = newCurLogFile
	}

	curLogFile.Mu.Lock()
	defer curLogFile.Mu.Unlock()
	writeAt := curLogFile.Offset
	if err := curLogFile.Write(entBuf); err != nil {
		return nil, err
	}
	valPos := &ValuePos{
		fid:       curLogFile.Fid,
		offset:    writeAt,
		entrySize: entSize,
	}
	return valPos, nil
}

// buildLogFiles Recover archivedLogFile from disk.
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
		fids := db.getFidListByType(typ)
		fids = append(fids, uint32(fid))
		db.fidsMap.Set(typ, fids)
	}

	for typ := valueTypeString; typ < valueTypeZSet; typ++ {
		fids := db.getFidListByType(typ)
		// newly created log file has bigger fid
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		archivedLogFiles := NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
		for i, fid := range fids {
			lf, err := logfile.Open(db.cfg.DBPath, fid, db.cfg.MaxLogFileSize, uint8(typ), db.cfg.IOType)
			if err != nil {
				log.Fatalf("Open Log File error:%v. Type: %v, Fid: %v,", err, typ, fid)
				continue
			}

			// latest one is the active log file
			if i == len(fids)-1 {
				db.curLogFile.Set(typ, lf)
			} else {
				archivedLogFiles.Set(fid, lf)
			}
		}
		db.archivedLogFile[typ] = archivedLogFiles
	}

	return nil
}

// getCurLogFile Util function for get curLogFile from ConcurrentMap.
// Initiate a new LogFile if curLogFile of typ is empty
func (db *LazyDB) getCurLogFile(typ valueType) *logfile.LogFile {
	v, ok := db.curLogFile.Get(typ)
	// create a new LogFile if not exist
	if !ok {
		lf, err := logfile.Open(db.cfg.DBPath, 1, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
		if err != nil {
			log.Fatalf("Create log file error: %v", err)
			return nil
		}

		db.curLogFile.Set(typ, lf)
		return lf
	}
	lf, _ := v.(*logfile.LogFile)
	return lf
}

// getArchivedLogFile Util function for get archivedLogFile from ConcurrentMap.
// Returns nil when target log file does not exist
func (db *LazyDB) getArchivedLogFile(typ valueType, fid uint32) *logfile.LogFile {
	lfs, ok := db.archivedLogFile[typ]
	if !ok {
		db.archivedLogFile[typ] = NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
		return nil
	}
	v, ok := lfs.Get(fid)
	if !ok {
		return nil
	}
	lf := v.(*logfile.LogFile)
	return lf
}

// getFidListByType returns a slice of fid by valueType
// It initializes an empty slice if fid list have not yet been created.
func (db *LazyDB) getFidListByType(typ valueType) []uint32 {
	v, ok := db.fidsMap.Get(typ)
	if !ok {
		newFids := make([]uint32, 0)
		db.fidsMap.Set(typ, newFids)
		return newFids
	}
	fids, _ := v.([]uint32)
	return fids
}
