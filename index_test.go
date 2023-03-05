package lazydb

import (
	"lazydb/logfile"
	"lazydb/util"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLazyDB_buildIndexFromLogFiles(t *testing.T) {
	// Create Two Log File for test, same logic as TestLazyDB_WriteLogEntry
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "test_build_log_file")
	if !util.PathExist(path) {
		err := os.MkdirAll(path, os.ModePerm)
		assert.Nil(t, err)
	}
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 //  set max file so that it can only contain 2 entry in a file
	db := &LazyDB{
		cfg:              &cfg,
		index:            NewConcurrentMap(int(cfg.HashIndexShardCount)),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ConcurrentMap[uint32]),
	}
	for i := 0; i < logFileTypeNum; i++ {
		db.fidsMap[valueType(i)] = &MutexFids{fids: make([]uint32, 0)}
		db.archivedLogFile[valueType(i)] = NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
	}

	// test buildLogFiles with empty directory
	err := db.buildLogFiles()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), db.getActiveLogFile(valueTypeString).lf.Fid)

	val1 := GetValue32()
	val2 := GetValue32()
	val3 := GetValue32()
	db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: GetKey(1), Value: val1})
	db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: GetKey(2), Value: val2})
	db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: GetKey(3), Value: val3})
	db.Close()

	// test buildLogFiles with existing log files
	newDB := &LazyDB{
		cfg:              &cfg,
		index:            NewConcurrentMap(int(cfg.HashIndexShardCount)),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ConcurrentMap[uint32]),
	}
	defer destroyDB(newDB)

	for i := 0; i < logFileTypeNum; i++ {
		newDB.fidsMap[valueType(i)] = &MutexFids{fids: make([]uint32, 0)}
		newDB.archivedLogFile[valueType(i)] = NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
	}
	err = newDB.buildLogFiles()
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), newDB.getActiveLogFile(valueTypeString).lf.Fid)
	assert.NotNil(t, newDB.getArchivedLogFile(valueTypeString, 1))

	err = newDB.buildIndexFromLogFiles()
	assert.NoError(t, err)

	got, err := newDB.Get(GetKey(1))
	assert.NoError(t, err)
	assert.Equal(t, true, reflect.DeepEqual(got, val1))

	got, err = newDB.Get(GetKey(2))
	assert.NoError(t, err)
	assert.Equal(t, true, reflect.DeepEqual(got, val2))

	got, err = newDB.Get(GetKey(3))
	assert.NoError(t, err)
	assert.Equal(t, true, reflect.DeepEqual(got, val3))
}
