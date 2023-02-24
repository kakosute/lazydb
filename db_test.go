package lazydb

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"lazydb/logfile"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestOpen(t *testing.T) {
	// empty db directory
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	db, err := Open(cfg)
	assert.Nil(t, err)
	defer destroyDB(db)

	// db directory with existing files
	path = filepath.Join(wd, "testLogFiles", "open")
	cfg = DefaultDBConfig(path)
	db, err = Open(cfg)
	assert.Nil(t, err)
	defer destroyDB(db)
}

func TestLazyDB_Merge(t *testing.T) {

}

func TestLazyDB_ReadLogEntry(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 // set max file size to 150B, only contain 2 entry in 1 file
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)

	// set up initial data
	entry1 := &logfile.LogEntry{Key: GetKey(1), Value: GetValue32()}
	entry2 := &logfile.LogEntry{Key: GetKey(2), Value: GetValue32(), ExpiredAt: time.Now().Unix()}
	entry3 := &logfile.LogEntry{Key: GetKey(3), Value: GetValue32()}
	db.writeLogEntry(valueTypeString, entry1)
	db.writeLogEntry(valueTypeString, entry2)
	db.writeLogEntry(valueTypeString, entry3)

	type arg struct {
		typ    valueType
		fid    uint32
		offset int64
	}
	tests := []struct {
		arg              arg
		expectedKey      []byte
		expectedValue    []byte
		expectedExpireAt int64
	}{
		{
			arg: arg{
				typ:    valueTypeString,
				fid:    1,
				offset: 0,
			},
			expectedKey:      entry1.Key,
			expectedValue:    entry1.Value,
			expectedExpireAt: entry1.ExpiredAt,
		},
		{
			arg: arg{
				typ:    valueTypeString,
				fid:    1,
				offset: 72,
			},
			expectedKey:      entry2.Key,
			expectedValue:    entry2.Value,
			expectedExpireAt: entry2.ExpiredAt,
		},
		{
			arg: arg{
				typ:    valueTypeString,
				fid:    2,
				offset: 0,
			},
			expectedKey:      entry3.Key,
			expectedValue:    entry3.Value,
			expectedExpireAt: entry3.ExpiredAt,
		},
	}

	for _, tt := range tests {
		entry, err := db.readLogEntry(tt.arg.typ, tt.arg.fid, tt.arg.offset)
		assert.Nil(t, err)
		assert.Equal(t, entry.Key, tt.expectedKey)
		assert.Equal(t, entry.Value, tt.expectedValue)
		assert.Equal(t, entry.ExpiredAt, tt.expectedExpireAt)
	}
}

func TestLazyDB_WriteLogEntry(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 // set max file size to 150B, only contain 2 entry in 1 file
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)

	type args struct {
		key   []byte
		value []byte
	}

	tests := []struct {
		args          args
		wantFid       uint32
		wantOffset    int64
		wantEntrySize int
	}{
		{
			args: args{
				key:   GetKey(1),
				value: GetValue32(),
			},
			wantFid:       1,
			wantOffset:    0,
			wantEntrySize: 72,
		},
		{
			args: args{
				key:   GetKey(1),
				value: GetValue32(),
			},
			wantFid:       1,
			wantOffset:    72,
			wantEntrySize: 72,
		},
		{
			args: args{
				key:   GetKey(1),
				value: GetValue32(),
			},
			wantFid:       2,
			wantOffset:    0,
			wantEntrySize: 72,
		},
	}

	for _, tt := range tests {
		valPos, err := db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: tt.args.key, Value: tt.args.value})
		assert.Nil(t, err)
		assert.Equal(t, valPos.fid, tt.wantFid)
		assert.Equal(t, valPos.offset, tt.wantOffset)
		assert.Equal(t, valPos.entrySize, tt.wantEntrySize)
	}
}

func TestLazyDB_BuildLogFile(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "testLogFiles", "build_log_file")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 // set max file size to 120B, only contain 2 entry in 1 file
	db := &LazyDB{
		cfg:              &cfg,
		index:            NewConcurrentMap(int(cfg.HashIndexShardCount)),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ConcurrentMap[uint32]),
	}
	defer destroyDB(db)

	for i := 0; i < logFileTypeNum; i++ {
		db.fidsMap[valueType(i)] = &MutexFids{fids: make([]uint32, 0)}
		db.archivedLogFile[valueType(i)] = NewWithCustomShardingFunction[uint32](defaultShardCount, simpleSharding)
	}

	err := db.buildLogFiles()
	assert.Nil(t, err)
	assert.Equal(t, db.getActiveLogFile(valueTypeString).lf.Fid, uint32(2))
	assert.NotNil(t, db.getArchivedLogFile(valueTypeString, 1))
}

func destroyDB(db *LazyDB) {
	if db != nil {
		err := db.Close()
		if err != nil {
			log.Fatalf("destory DB error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		err = os.RemoveAll(db.cfg.DBPath)
		if err != nil {
			log.Fatalf("destory DB error: %v", err)
		}
	}
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// GetKey Generate a 32Bytes key
func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("%09d", n))
}

// GetValue32 Generates a 32Bytes value
func GetValue32() []byte {
	return GetValue(32)
}

func GetValue(n int) []byte {
	var str bytes.Buffer
	for i := 0; i < n; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return str.Bytes()
}
