package lazydb

import (
	"errors"
	"io"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	LazyDB struct {
		cfg              *DBConfig
		index            *ds.ConcurrentMap[string]
		strIndex         *strIndex
		hashIndex        *hashIndex
		fidsMap          map[valueType]*MutexFids
		activeLogFileMap map[valueType]*MutexLogFile
		archivedLogFile  map[valueType]*ds.ConcurrentMap[uint32] // [uint32]*MutexLogFile
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

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *ds.AdaptiveRadixTree
	}

	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*ds.AdaptiveRadixTree
	}

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

	logFileTypeNum = 5
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrLogFileNotExist = errors.New("log file is not exist")
	ErrOpenLogFile     = errors.New("open Log file error")
)

func newStrIndex() *strIndex {
	return &strIndex{idxTree: ds.NewART(), mu: new(sync.RWMutex)}
}

func newHashIndex() *hashIndex {
	return &hashIndex{trees: make(map[string]*ds.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

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
		index:            ds.NewConcurrentMap(int(cfg.HashIndexShardCount)),
		strIndex:         newStrIndex(),
		hashIndex:        newHashIndex(),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ds.ConcurrentMap[uint32]),
	}

	for i := 0; i < logFileTypeNum; i++ {
		db.fidsMap[valueType(i)] = &MutexFids{fids: make([]uint32, 0)}
		db.archivedLogFile[valueType(i)] = ds.NewWithCustomShardingFunction[uint32](ds.DefaultShardCount, ds.SimpleSharding)
	}

	if err := db.buildLogFiles(); err != nil {
		log.Fatalf("Build Log Files error: %v", err)
		return nil, err
	}

	if err := db.buildIndexFromLogFiles(); err != nil {
		log.Fatalf("Build Index From Log Files error: %v", err)
		return nil, err
	}

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
		err := mlf.lf.Close()
		if err != nil {
			log.Fatalf("Close log file err: %v", err)
		}
	}
	for typ, mutexFids := range db.fidsMap {
		for _, fid := range mutexFids.fids {
			mlf := db.getArchivedLogFile(typ, fid)
			if mlf == nil {
				continue
			}
			mlf.lf.Sync()
			err := mlf.lf.Close()
			if err != nil {
				log.Fatalf("Close log file err: %v", err)
			}
		}
	}
	db.index = nil
	db.fidsMap = nil
	db.activeLogFileMap = nil
	db.archivedLogFile = nil
	return nil
}

func (db *LazyDB) mergeStr(fid uint32, offset int64, ent *logfile.LogEntry) error {
	strKey := util.ByteToString(ent.Key)

	shard := db.index.GetShardByWriting(strKey)
	defer shard.Unlock()

	indexVal, _ := shard.Get(strKey)
	if indexVal == nil {
		return nil
	}

	val, _ := indexVal.(*Value)
	if val != nil && val.fid == fid && val.offset == offset {
		// rewrite entry
		valuePos, err := db.writeLogEntry(valueTypeString, ent)
		if err != nil {
			return err
		}
		// update index
		shard.Set(strKey, Value{
			value:     val.value,
			vType:     valueTypeString,
			fid:       valuePos.fid,
			offset:    valuePos.offset,
			entrySize: valuePos.entrySize,
		})
	}
	return nil
}

func (db *LazyDB) Merge(typ valueType, targetFid uint32) error {
	archivedFile := db.getArchivedLogFile(typ, targetFid)
	if archivedFile == nil {
		return nil
	}

	var offset int64
	for {
		ent, size, err := archivedFile.lf.ReadLogEntry(offset)
		if err != nil {
			if err == io.EOF || err == logfile.ErrLogEndOfFile {
				break
			}
			return err
		}
		var off = offset
		offset += int64(size)
		if ent.Stat == logfile.SDelete {
			continue
		}
		ts := time.Now().Unix()
		if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
			continue
		}
		var mergeErr error
		switch typ {
		case valueTypeString:
			mergeErr = db.mergeStr(archivedFile.lf.Fid, off, ent)
		}
		if mergeErr != nil {
			return mergeErr
		}
	}

	// delete older log file
	archivedLogFiles := db.archivedLogFile[typ]
	shard := archivedLogFiles.GetShardByWriting(targetFid)

	val, _ := shard.Get(targetFid)
	lf := val.(*logfile.LogFile)

	_ = lf.Delete()         // close file and remove local file
	shard.Remove(targetFid) // remove index from memory

	shard.Unlock()

	// TODO: clear discard state.

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
	activeLogFile := db.getActiveLogFile(typ)
	if activeLogFile == nil {
		return nil, ErrOpenLogFile
	}
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
		db.archivedLogFile[typ].Set(lf.Fid, &MutexLogFile{lf: lf})

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

	build := func(typ valueType) {
		mutexFids := db.fidsMap[typ]
		fids := mutexFids.fids
		if len(fids) == 0 {
			return
		}
		// newly created log file has bigger fid
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		archivedLogFiles := db.archivedLogFile[typ]
		for i, fid := range fids {
			lf, err := logfile.Open(db.cfg.DBPath, fid, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
			if err != nil {
				log.Fatalf("Open Log File error:%v. Type: %v, Fid: %v,", err, typ, fid)
				continue
			}

			// latest one is the active log file
			if i == len(fids)-1 {
				db.activeLogFileMap[typ] = &MutexLogFile{lf: lf}
			} else {
				archivedLogFiles.Set(fid, &MutexLogFile{lf: lf})
			}
		}
	}
	for typ := 0; typ < logFileTypeNum; typ++ {
		build(valueType(typ))
	}
	return nil
}

// getArchivedLogFile Util function for get archivedLogFile from ConcurrentMap.
// Returns nil when target log file does not exist
func (db *LazyDB) getArchivedLogFile(typ valueType, fid uint32) *MutexLogFile {
	lfs := db.archivedLogFile[typ]
	v, ok := lfs.Get(fid)
	if !ok {
		return nil
	}
	lf := v.(*MutexLogFile)
	return lf
}

func (db *LazyDB) getActiveLogFile(typ valueType) *MutexLogFile {
	mutexLf, ok := db.activeLogFileMap[typ]
	if !ok {
		lf, err := logfile.Open(db.cfg.DBPath, 1, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
		if err != nil {
			log.Fatalf("Create New Log File error: %v", err)
			return nil
		}
		newMutexLf := &MutexLogFile{lf: lf}
		db.activeLogFileMap[typ] = newMutexLf

		fids := db.fidsMap[typ]
		fids.mu.Lock()
		fids.fids = append(fids.fids, lf.Fid)
		fids.mu.Unlock()

		return newMutexLf
	}
	return mutexLf
}
