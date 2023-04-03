package lazydb

import (
	"github.com/stretchr/testify/assert"
	"lazydb/util"
	"os"
	"path/filepath"
	"testing"
)

func initTestDB() *LazyDB {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "test_hash")
	if !util.PathExist(path) {
		os.MkdirAll(path, os.ModePerm)
	}
	cfg := DefaultDBConfig(path)
	db, _ := Open(cfg)
	return db
}

func TestLazyDB_HSet(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	type args struct {
		key  []byte
		args [][]byte
	}

	tests := []struct {
		name        string
		args        args
		wantErr     bool
		expectedErr error
	}{
		{
			name: "single pair",
			args: args{
				key:  []byte("h1"),
				args: [][]byte{[]byte("f1"), []byte("v1")},
			},
			wantErr: false,
		},
		{
			name: "multiple pairs",
			args: args{
				key:  []byte("h2"),
				args: [][]byte{[]byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")},
			},
			wantErr: false,
		},
		{
			name: "update existed field",
			args: args{
				key:  []byte("h2"),
				args: [][]byte{[]byte("f1"), []byte("v100"), []byte("f3"), []byte("v3")},
			},
			wantErr: false,
		},
		{
			name: "odd number of param",
			args: args{
				key:  []byte("h3"),
				args: [][]byte{[]byte("f1")},
			},
			wantErr:     true,
			expectedErr: ErrInvalidParam,
		},
		{
			name: "zero pair",
			args: args{
				key:  []byte("h4"),
				args: [][]byte{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		err := db.HSet(tt.args.key, tt.args.args...)
		if tt.wantErr {
			assert.NotNil(t, err)
			assert.Equal(t, tt.expectedErr, err)
		}
	}
}

func TestLazyDB_HGet(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.HSet([]byte("k1"), []byte("k1_f1"), []byte("k1_v1"), []byte("k1_f2"), []byte("k1_v2"))
	db.HSet([]byte("k1"), []byte("k1_f2"), []byte("k1_v2_updated"))

	type args struct {
		key   []byte
		field []byte
	}

	tests := []struct {
		name          string
		args          args
		expectedValue []byte
		expectedErr   error
	}{
		{
			name: "existed key and field",
			args: args{
				key:   []byte("k1"),
				field: []byte("k1_f1"),
			},
			expectedValue: []byte("k1_v1"),
			expectedErr:   nil,
		},
		{
			name: "updated field",
			args: args{
				key:   []byte("k1"),
				field: []byte("k1_f2"),
			},
			expectedValue: []byte("k1_v2_updated"),
			expectedErr:   nil,
		},
		{
			name: "not existed key",
			args: args{
				key:   []byte("k_not_exist"),
				field: []byte("k_not_exist_field"),
			},
			expectedValue: nil,
			expectedErr:   nil,
		},
		{
			name: "not existed field",
			args: args{
				key:   []byte("k1"),
				field: []byte("k1_not_exist_field"),
			},
			expectedValue: nil,
			expectedErr:   nil,
		},
	}
	for _, tt := range tests {
		got, err := db.HGet(tt.args.key, tt.args.field)
		assert.Equal(t, tt.expectedValue, got)
		assert.Equal(t, tt.expectedErr, err)
	}

}

func TestLazyDB_HDel(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.HSet([]byte("k1"), []byte("k1_f1"), []byte("k1_v1"), []byte("k1_f2"), []byte("k1_v2"))
	db.HSet([]byte("k2"), []byte("k2_f1"), []byte("k1_v2_updated"))

	type args struct {
		key    []byte
		fields [][]byte
	}

	tests := []struct {
		name        string
		args        args
		wantErr     bool
		expectedErr error
		wantCnt     int
	}{
		{
			name: "normal",
			args: args{
				key:    []byte("k1"),
				fields: [][]byte{[]byte("k1_f1"), []byte("k1_f2")},
			},
			wantErr: false,
			wantCnt: 2,
		},
		{
			name: "some field not exist",
			args: args{
				key:    []byte("k2"),
				fields: [][]byte{[]byte("k2_f1"), []byte("f123123")},
			},
			wantErr: false,
			wantCnt: 1,
		},
		{
			name: "all field not exist",
			args: args{
				key:    []byte("k2"),
				fields: [][]byte{[]byte("f123456"), []byte("f123123")},
			},
			wantErr: false,
			wantCnt: 0,
		},
		{
			name: "key not exist",
			args: args{
				key:    []byte("k123456"),
				fields: [][]byte{[]byte("f123456"), []byte("f123123")},
			},
			wantErr: false,
			wantCnt: 0,
		},
	}
	for _, tt := range tests {
		cnt, err := db.HDel(tt.args.key, tt.args.fields...)
		assert.Equal(t, tt.wantErr, err != nil)
		if tt.wantErr {
			assert.Equal(t, tt.expectedErr, err)
		}
		assert.Equal(t, tt.wantCnt, cnt)
	}
}
