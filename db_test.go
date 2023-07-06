package lazydb

import (
	"bytes"
	"fmt"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	entry1 := &logfile.LogEntry{Key: GetKey(1), Value: GetValue32()}
	entry2 := &logfile.LogEntry{Key: GetKey(2), Value: GetValue32(), ExpiredAt: time.Now().Unix()}
	entry3 := &logfile.LogEntry{Key: GetKey(3), Value: GetValue32()}
	_, _ = db.writeLogEntry(valueTypeString, entry1)
	_, _ = db.writeLogEntry(valueTypeString, entry2)
	_, _ = db.writeLogEntry(valueTypeString, entry3)
	_ = db.Close()
	defer destroyDB(db)

	// db directory with existing files
	db2, err := Open(cfg)
	assert.Nil(t, err)
	defer destroyDB(db2)
}

func TestLazyDB_Close(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	db, err := Open(cfg)
	assert.Nil(t, err)
	entry1 := &logfile.LogEntry{Key: GetKey(1), Value: GetValue32()}
	entry2 := &logfile.LogEntry{Key: GetKey(2), Value: GetValue32(), ExpiredAt: time.Now().Unix()}
	entry3 := &logfile.LogEntry{Key: GetKey(3), Value: GetValue32()}
	_, _ = db.writeLogEntry(valueTypeString, entry1)
	_, _ = db.writeLogEntry(valueTypeString, entry2)
	_, _ = db.writeLogEntry(valueTypeString, entry3)
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(db.cfg.DBPath)
	err = db.Close()
	assert.Nil(t, err)
}

func TestLazyDB_Merge(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 500
	db, err := Open(cfg)
	assert.Nil(t, err)

	for i := 0; i < 100; i++ {
		db.Set(GetKey(1), GetKey(32))
		db.HSet(GetKey(1), []byte("f1"), GetValue(32))
		db.ZAdd(GetKey(1), [][]byte{util.Float64ToByte(1), []byte("m1")}...)
	}
	for i := 0; i < 100; i++ {
		db.Set(GetKey(2), GetKey(32))
		db.HSet(GetKey(2), []byte("f1"), GetValue(32))
		db.ZAdd(GetKey(2), [][]byte{util.Float64ToByte(1), []byte("m1")}...)
	}

	valueTypeMap := map[valueType]string{
		valueTypeString: "String",
		valueTypeList:   "List",
		valueTypeHash:   "Hash",
		valueTypeSet:    "Set",
		valueTypeZSet:   "ZSet",
	}
	fmt.Printf("File number before garbage collection: \n")
	for i := valueTypeString; i <= valueTypeZSet; i++ {
		fmt.Printf("Type: %s\tFileNumber: %d\n", valueTypeMap[i], db.archivedLogFile[i].Size()+1)
	}

	for i := valueTypeString; i <= valueTypeZSet; i++ {
		lfs := db.fidsMap[i]
		for _, fid := range lfs.fids {
			err := db.Merge(i, fid, 0.1)
			assert.Nil(t, err)
		}
	}

	fmt.Printf("File number after garbage collection: \n")
	for i := valueTypeString; i <= valueTypeZSet; i++ {
		fmt.Printf("Type: %s\tFileNumber: %d\n", valueTypeMap[i], db.archivedLogFile[i].Size()+1)
	}

	defer destroyDB(db)
}

func TestLazyDB_ReadLogEntry(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 160 // set max file size to 150B, only contain 2 entry in 1 file
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)

	// set up initial data
	entry1 := &logfile.LogEntry{Key: GetKey(1), Value: GetValue32()}
	entry2 := &logfile.LogEntry{Key: GetKey(2), Value: GetValue32(), ExpiredAt: time.Now().Unix()}
	entry3 := &logfile.LogEntry{Key: GetKey(3), Value: GetValue32()}
	_, _ = db.writeLogEntry(valueTypeString, entry1)
	_, _ = db.writeLogEntry(valueTypeString, entry2)
	_, _ = db.writeLogEntry(valueTypeString, entry3)

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
				offset: 74,
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
		if err != nil {
			continue
		}
		assert.Equal(t, tt.expectedKey, entry.Key)
		assert.Equal(t, tt.expectedValue, entry.Value)
		assert.Equal(t, tt.expectedExpireAt, entry.ExpiredAt)
	}
}

func TestLazyDB_WriteLogEntry(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 //  set max file so that it can only contain 2 entry in a file
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
			wantEntrySize: 74,
		},
		{
			args: args{
				key:   GetKey(2),
				value: GetValue32(),
			},
			wantFid:       1,
			wantOffset:    74,
			wantEntrySize: 74,
		},
		{
			args: args{
				key:   GetKey(3),
				value: GetValue32(),
			},
			wantFid:       2,
			wantOffset:    0,
			wantEntrySize: 74,
		},
	}

	for _, tt := range tests {
		valPos, err := db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: tt.args.key, Value: tt.args.value})
		assert.Nil(t, err)
		assert.Equal(t, tt.wantFid, valPos.fid)
		assert.Equal(t, tt.wantOffset, valPos.offset)
		assert.Equal(t, tt.wantEntrySize, valPos.entrySize)
	}
}

func TestLazyDB_BuildLogFile(t *testing.T) {
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

	// test buildLogFiles with empty directory
	err := db.buildLogFiles()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), db.getActiveLogFile(valueTypeString).lf.Fid)

	_, _ = db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: GetKey(1), Value: GetValue32()})
	_, _ = db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: GetKey(2), Value: GetValue32()})
	_, _ = db.writeLogEntry(valueTypeString, &logfile.LogEntry{Key: GetKey(3), Value: GetValue32()})
	_ = db.Close()

	// test buildLogFiles with existing log files
	newDB := &LazyDB{
		cfg:              &cfg,
		strIndex:         newStrIndex(),
		hashIndex:        newHashIndex(),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ds.ConcurrentMap[uint32]),
	}
	for i := 0; i < logFileTypeNum; i++ {
		newDB.fidsMap[valueType(i)] = &MutexFids{fids: make([]uint32, 0)}
		newDB.archivedLogFile[valueType(i)] = ds.NewWithCustomShardingFunction[uint32](ds.DefaultShardCount, ds.SimpleSharding)
	}
	err = newDB.buildLogFiles()
	defer destroyDB(newDB)

	assert.Nil(t, err)
	assert.Equal(t, uint32(2), newDB.getActiveLogFile(valueTypeString).lf.Fid)
	assert.NotNil(t, newDB.getArchivedLogFile(valueTypeString, 1))
}

func TestEncodeKey_DecodeKey(t *testing.T) {
	type args struct {
		key    []byte
		subKey []byte
	}

	tests := []struct {
		name string
		args args
	}{
		{name: "normal", args: args{key: []byte("k1"), subKey: []byte("f1")}},
		{name: "both empty", args: args{key: []byte(""), subKey: []byte("")}},
		{name: "empty ", args: args{key: []byte(""), subKey: []byte("")}},
		{name: "both empty", args: args{key: []byte(""), subKey: []byte("")}},
	}

	for _, tt := range tests {
		encoded := encodeKey(tt.args.key, tt.args.subKey)
		gotKey, gotSubkey := decodeKey(encoded)
		assert.NotEqual(t, tt.args.key, gotKey)
		assert.NotEqual(t, tt.args.subKey, gotSubkey)
	}
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
