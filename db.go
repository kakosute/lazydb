package lazydb

import (
	"errors"
	"lazydb/logfile"
	"log"
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

	return nil, nil
}

func (db *LazyDB) initLogFiles(typ valueType) error {
	return nil
}

// buildLogFiles Recover archivedLogFile from disk.
func (db *LazyDB) buildLogFiles() error {
	return nil
}

// getCurLogFile Util function for get curLogFile from ConcurrentMap.
// Initiate a new LogFile if curLogFile of typ is empty
func (db *LazyDB) getCurLogFile(typ valueType) *logfile.LogFile {
	v, ok := db.curLogFile.Get(typ)
	// create a new LogFile
	if !ok {
		//todo param typ
		lf, err := logfile.Open(db.cfg.DBPath, 1, db.cfg.MaxLogFileSize, typ, db.cfg.IOType)
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
	lfs := db.archivedLogFile[typ]
	v, ok := lfs.Get(fid)
	if !ok {
		return nil
	}
	lf := v.(*logfile.LogFile)
	return lf
}

func (db *LazyDB) getFidListByType(typ valueType) []uint32 {
	return nil
}
