package lazydb

import (
	"lazydb/util"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
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
		t.Run(tt.name, func(t *testing.T) {
			err := db.HSet(tt.args.key, tt.args.args...)
			if tt.wantErr {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedErr, err)
			}
		})
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
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HGet(tt.args.key, tt.args.field)
			assert.Equal(t, tt.expectedValue, got)
			assert.Equal(t, tt.expectedErr, err)
		})
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
		t.Run(tt.name, func(t *testing.T) {
			cnt, err := db.HDel(tt.args.key, tt.args.fields...)
			assert.Equal(t, tt.wantErr, err != nil)
			if tt.wantErr {
				assert.Equal(t, tt.expectedErr, err)
			}
			assert.Equal(t, tt.wantCnt, cnt)
		})
	}
}

func TestLazyDB_HExists(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	db.HSet([]byte("k1"), []byte("k1_f1"), []byte("k1_v1"))

	type args struct {
		key   []byte
		field []byte
	}

	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name:    "normal",
			args:    args{key: []byte("k1"), field: []byte("k1_f1")},
			want:    true,
			wantErr: false,
		},
		{
			name:    "key not exist",
			args:    args{key: []byte("k1909"), field: []byte("k1_f1")},
			want:    false,
			wantErr: false,
		},
		{
			name:    "field not exist",
			args:    args{key: []byte("k1"), field: []byte("k1_f1123123")},
			want:    false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HExists(tt.args.key, tt.args.field)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLazyDB_HGetAll(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	value1, value2, value3 := GetValue32(), GetValue32(), GetValue32()
	db.HSet([]byte("k1"), GetKey(1), value1, GetKey(2), value2, GetKey(3), value3)
	db.HSet([]byte("k2"), GetKey(1), GetValue32())
	db.HDel([]byte("k2"), GetKey(1))

	tests := []struct {
		name    string
		key     []byte
		want    [][]byte
		wantErr bool
	}{
		{
			name:    "normal",
			key:     []byte("k1"),
			want:    [][]byte{GetKey(1), value1, GetKey(2), value2, GetKey(3), value3},
			wantErr: false,
		},
		{
			name:    "key exists but contains no field",
			key:     []byte("k2"),
			want:    [][]byte{},
			wantErr: false,
		},
		{
			name:    "key not exists",
			key:     []byte("k3"),
			want:    [][]byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HGetAll(tt.key)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, true, reflect.DeepEqual(got, tt.want))
		})
	}
}

func TestLazyDB_HKeys(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	db.HSet([]byte("k1"), GetKey(1), GetValue32(), GetKey(2), GetValue32(), GetKey(3), GetValue32())
	db.HSet([]byte("k2"), GetKey(1), GetValue32())
	db.HDel([]byte("k2"), GetKey(1))

	tests := []struct {
		name    string
		key     []byte
		want    [][]byte
		wantErr bool
	}{
		{
			name:    "normal",
			key:     []byte("k1"),
			want:    [][]byte{GetKey(1), GetKey(2), GetKey(3)},
			wantErr: false,
		},
		{
			name:    "key exists but contains no field",
			key:     []byte("k2"),
			want:    [][]byte{},
			wantErr: false,
		},
		{
			name:    "key not exists ",
			key:     []byte("k3"),
			want:    [][]byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HKeys(tt.key)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, true, reflect.DeepEqual(got, tt.want))
		})
	}

	t.Run("Test large number of keys", func(t *testing.T) {
		writeCnt := 100000
		hashKey := []byte("k_lots_of_fields")
		for i := 0; i < writeCnt; i++ {
			db.HSet(hashKey, GetKey(i), GetValue32())
		}
		keys, err := db.HKeys(hashKey)
		assert.Nil(t, err)
		assert.Equal(t, writeCnt, len(keys))
		for i := 0; i < writeCnt; i++ {
			assert.Equal(t, GetKey(i), keys[i])
		}
	})
}

func TestLazyDB_HVals(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	k1V1, k1V2, k1V3 := GetValue32(), GetValue32(), GetValue32()
	db.HSet([]byte("k1"), GetKey(1), k1V1, GetKey(2), k1V2, GetKey(3), k1V3)
	db.HSet([]byte("k2"), GetKey(1), GetValue32())
	db.HDel([]byte("k2"), GetKey(1))

	tests := []struct {
		name    string
		key     []byte
		want    [][]byte
		wantErr bool
	}{
		{
			name:    "normal",
			key:     []byte("k1"),
			want:    [][]byte{k1V1, k1V2, k1V3},
			wantErr: false,
		},
		{
			name:    "key exists but contains no field",
			key:     []byte("k2"),
			want:    [][]byte{},
			wantErr: false,
		},
		{
			name:    "key not exists ",
			key:     []byte("k3"),
			want:    [][]byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HVals(tt.key)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, true, reflect.DeepEqual(got, tt.want))
		})
	}

	t.Run("Test large number of keys", func(t *testing.T) {
		writeCnt := 100000
		hashKey := []byte("k_lots_of_fields")
		wantVal := [][]byte{}
		for i := 0; i < writeCnt; i++ {
			val := GetValue32()
			wantVal = append(wantVal, val)
			db.HSet(hashKey, GetKey(i), val)
		}
		vals, err := db.HVals(hashKey)
		assert.Nil(t, err)
		assert.Equal(t, writeCnt, len(vals))
		for i := 0; i < writeCnt; i++ {
			assert.Equal(t, wantVal[i], vals[i])
		}
	})
}

func TestLazyDB_HSetNX(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	v1 := GetValue32()
	db.HSet([]byte("k1"), GetKey(1), v1)

	type args struct {
		key   []byte
		field []byte
		value []byte
	}

	tests := []struct {
		name    string
		args    args
		wantVal []byte
	}{
		{
			name: "normal",
			args: args{
				key:   []byte("k1"),
				field: GetKey(2),
				value: []byte("field2"),
			},
			wantVal: []byte("field2"),
		},
		{
			name: "already exist",
			args: args{
				key:   []byte("k1"),
				field: GetKey(1),
				value: []byte("field2"),
			},
			wantVal: v1,
		},
		{
			name: "key not exist",
			args: args{
				key:   []byte("k2"),
				field: GetKey(1),
				value: []byte("k2-field1"),
			},
			wantVal: []byte("k2-field1"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.HSetNX(tt.args.key, tt.args.field, tt.args.value)
			assert.Nil(t, err)
			got, err := db.HGet(tt.args.key, tt.args.field)
			assert.Nil(t, err)
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestLazyDB_HMGet(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	v1, v2, v3 := GetValue32(), GetValue32(), GetValue32()
	db.HSet([]byte("k1"), GetKey(1), v1, GetKey(2), v2, GetKey(3), v3)

	type args struct {
		key    []byte
		fields [][]byte
	}

	tests := []struct {
		name string
		args args
		want [][]byte
	}{
		{
			name: "normal",
			args: args{
				key:    []byte("k1"),
				fields: [][]byte{GetKey(1), GetKey(2), GetKey(3)},
			},
			want: [][]byte{v1, v2, v3},
		},
		{
			name: "one field doesn't exist",
			args: args{
				key:    []byte("k1"),
				fields: [][]byte{GetKey(1), GetKey(4), GetKey(3)},
			},
			want: [][]byte{v1, v3},
		},
		{
			name: "all fields don't exist",
			args: args{
				key:    []byte("k1"),
				fields: [][]byte{GetKey(4), GetKey(5)},
			},
			want: [][]byte{},
		},
		{
			name: "key doesn't exist",
			args: args{
				key:    []byte("k2"),
				fields: [][]byte{GetKey(4), GetKey(5)},
			},
			want: [][]byte{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.HMGet(tt.args.key, tt.args.fields...)
			assert.Nil(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLazyDB_HLen(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	v1, v2, v3 := GetValue32(), GetValue32(), GetValue32()
	db.HSet([]byte("k1"), GetKey(1), v1, GetKey(2), v2, GetKey(3), v3)

	type args struct {
		key []byte
	}

	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "normal",
			args: args{key: []byte("k1")},
			want: 3,
		},
		{
			name: "empty key",
			args: args{key: []byte("k2")},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := db.HLen(tt.args.key)
			assert.Equal(t, tt.want, got)
		})
	}
	// test deletion
	db.HDel([]byte("k1"), GetKey(1))
	t.Run("test deletion", func(t *testing.T) {
		got := db.HLen([]byte("k1"))
		assert.Equal(t, 2, got)
	})
	db.HDel([]byte("k1"), GetKey(2), GetKey(3))
	t.Run("test deletion_all", func(t *testing.T) {
		got := db.HLen([]byte("k1"))
		assert.Equal(t, 0, got)
	})
}
