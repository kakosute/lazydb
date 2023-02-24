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

}

func TestLazyDB_Close(t *testing.T) {

}

func TestLazyDB_Sync(t *testing.T) {

}

func TestLazyDB_Merge(t *testing.T) {

}

func TestLazyDB_ReadLogEntry(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 // set max file size to 120B, only contain 2 entry in 1 file
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)

	// set up initial data
	entry1 := &logfile.LogEntry{Key: GetKey(1), Value: GetValue32()}
	entry2 := &logfile.LogEntry{Key: GetKey(2), Value: GetValue32()}
	entry3 := &logfile.LogEntry{Key: GetKey(3), Value: GetValue32()}
	db.writeLogEntry(valueTypeString, entry1)
	db.writeLogEntry(valueTypeString, entry2)
	db.writeLogEntry(valueTypeString, entry3)

	type arg{

	}
}

func TestLazyDB_WriteLogEntry(t *testing.T) {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "tmp")
	cfg := DefaultDBConfig(path)
	cfg.MaxLogFileSize = 150 // set max file size to 120B, only contain 2 entry in 1 file
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
			wantEntrySize: 66,
		},
		{
			args: args{
				key:   GetKey(1),
				value: GetValue32(),
			},
			wantFid:       1,
			wantOffset:    66,
			wantEntrySize: 66,
		},
		{
			args: args{
				key:   GetKey(1),
				value: GetValue32(),
			},
			wantFid:       2,
			wantOffset:    0,
			wantEntrySize: 66,
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

func GetKey(n int) []byte {
	return []byte("kvstore-bench-key------" + fmt.Sprintf("09%d", n))
}

func GetValue16() []byte {
	return GetValue(16)
}

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
