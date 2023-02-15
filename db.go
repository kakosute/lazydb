package lazydb

import (
	"lazydb/logfile"
	"sync"
)

type (
	LazyDB struct {
		cfg             *DBConfig
		index           *ConcurrentMap[string]
		fidsMap         *ConcurrentMap[valueType]         // [valueType][]uint32
		curLogFile      *ConcurrentMap[valueType]         // [valueType]*LogFile
		archivedLogFile map[valueType]*ConcurrentMap[int] // [int]*LogFile
		mu              sync.RWMutex
	}

	valueType uint8

	Value struct {
		value     []byte    // 这个是他的memory mode用的，它支持和redis一样的内存读数据
		vType     valueType // 可以加个type 用于type指令和一些操作前的判断，比如string的key不能lpush
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

func (db *LazyDB) readLogEntry(typ valueType, fid uint32, offset int64) (*logfile.LogEntry, error) {
	return nil, nil
}

func (db *LazyDB) writeLogEntry(typ valueType, entry *logfile.LogEntry) (*ValuePos, error) {
	return nil, nil
}

func (db *LazyDB) initLogFiles(typ valueType) error {
	return nil
}

func (db *LazyDB) buildLogFiles() error {
	return nil
}
