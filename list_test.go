package lazydb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// LPush LPushX LPop RPush RPushX RPop
func TestLazyDB_LPush(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *LazyDB
		args    args
		wantErr bool
	}{
		{"no value", db, args{key: []byte("a"), values: [][]byte{}}, false},
		{"one value", db, args{key: []byte("b"), values: [][]byte{[]byte("a")}}, false},
		{"multi values", db, args{key: []byte("c"), values: [][]byte{[]byte("b"), []byte("b"), []byte("c")}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.LPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
				t.Errorf("LPush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLazyDB_LPushX(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	err := db.LPush([]byte("a"), []byte("1"))
	assert.Nil(t, err)
	err = db.LPush([]byte("b"), []byte("2"))
	assert.Nil(t, err)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *LazyDB
		args    args
		wantErr bool
	}{
		{
			"one value", db, args{key: []byte("a"), values: [][]byte{[]byte("a")}}, false,
		},
		{
			"multi values", db, args{key: []byte("b"), values: [][]byte{[]byte("b"), []byte("b"), []byte("c")}}, false,
		},
		{
			"no key", db, args{key: []byte{}, values: [][]byte{[]byte("a")}}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.LPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
				t.Errorf("LPushX() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLazyDB_LPop(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	// pop when no value
	listKey := []byte("my_list")
	pop, err := db.LPop(listKey)
	assert.Nil(t, pop)
	assert.Nil(t, err)
	// pop when there is one value
	err = db.LPush(listKey, []byte("a"))
	assert.Nil(t, err)
	v1, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v1)
	// lpush one value
	err = db.RPush(listKey, []byte("a"))
	assert.Nil(t, err)
	v2, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v2)
	//	push multi values
	err = db.LPush(listKey, []byte("b"), []byte("b"), []byte("c"))
	assert.Nil(t, err)
	var values [][]byte
	// pop when there are multi values
	for db.LLen(listKey) > 0 {
		v, err := db.LPop(listKey)
		assert.Nil(t, err)
		values = append(values, v)
	}
	expected := [][]byte{[]byte("c"), []byte("b"), []byte("b")}
	assert.Equal(t, expected, values)
	// lRange
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, values)
}

func TestLazyDB_RPush(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *LazyDB
		args    args
		wantErr bool
	}{
		{"no value", db, args{key: []byte("a"), values: [][]byte{}}, false},
		{"one value", db, args{key: []byte("b"), values: [][]byte{[]byte("a")}}, false},
		{"multi values", db, args{key: []byte("c"), values: [][]byte{[]byte("b"), []byte("b"), []byte("c")}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.RPush(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
				t.Errorf("RPush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLazyDB_RPushX(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	err := db.LPush([]byte("a"), []byte("1"))
	assert.Nil(t, err)
	err = db.LPush([]byte("b"), []byte("2"))
	assert.Nil(t, err)

	type args struct {
		key    []byte
		values [][]byte
	}
	tests := []struct {
		name    string
		db      *LazyDB
		args    args
		wantErr bool
	}{
		{
			"one value", db, args{key: []byte("a"), values: [][]byte{[]byte("a")}}, false,
		},
		{
			"multi values", db, args{key: []byte("b"), values: [][]byte{[]byte("b"), []byte("b"), []byte("c")}}, false,
		},
		{
			"no key", db, args{key: []byte{}, values: [][]byte{[]byte("a")}}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.RPushX(tt.args.key, tt.args.values...); (err != nil) != tt.wantErr {
				t.Errorf("RPushX() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLazyDB_RPop(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	// pop when no value
	listKey := []byte("my_list")
	pop, err := db.RPop(listKey)
	assert.Nil(t, pop)
	assert.Nil(t, err)
	// pop when there is one value
	err = db.RPush(listKey, []byte("a"))
	assert.Nil(t, err)
	v1, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v1)
	// lpush one value
	err = db.LPush(listKey, []byte("a"))
	assert.Nil(t, err)
	v2, err := db.RPop(listKey)
	assert.Nil(t, err)
	assert.NotNil(t, v2)
	//	push multi values
	err = db.RPush(listKey, []byte("b"), []byte("b"), []byte("c"))
	assert.Nil(t, err)
	var values [][]byte
	// pop when there are multi values
	for db.LLen(listKey) > 0 {
		v, err := db.RPop(listKey)
		assert.Nil(t, err)
		values = append(values, v)
	}
	expected := [][]byte{[]byte("c"), []byte("b"), []byte("b")}
	assert.Equal(t, expected, values)
	// lRange
	values, err = db.LRange(listKey, 0, -1)
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, values)
}

// LSet LIndex LLen LRange LMove
func TestLazyDB_LSet(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	// no key
	listKey := []byte("my_list")
	err := db.LSet(listKey, 0, []byte("a"))
	assert.Equal(t, err, ErrKeyNotFound)

	// one value
	err = db.RPush(listKey, []byte("a"))
	assert.Nil(t, err)
	err = db.LSet(listKey, 0, []byte("b"))
	assert.Nil(t, err)
	lPop, err := db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, []byte("b"), lPop)

	// set 3 values, pop continuously
	err = db.RPush(listKey, []byte("a"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("b"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("c"))
	assert.Nil(t, err)
	err = db.LSet(listKey, 0, []byte("aa"))
	assert.Nil(t, err)
	err = db.LSet(listKey, 1, []byte("bb"))
	assert.Nil(t, err)
	err = db.LSet(listKey, -1, []byte("cc"))
	assert.Nil(t, err)
	lPop, err = db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, []byte("aa"), lPop)
	lPop, err = db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, []byte("bb"), lPop)
	lPop, err = db.LPop(listKey)
	assert.Nil(t, err)
	assert.Equal(t, []byte("cc"), lPop)

	// set out of range
	err = db.RPush(listKey, []byte("a"))
	assert.Nil(t, err)
	err = db.LSet(listKey, 1, []byte("aa"))
	assert.Equal(t, err, ErrWrongIndex)
	err = db.LSet(listKey, -2, []byte("aaa"))
	assert.Equal(t, err, ErrWrongIndex)
}

func TestLazyDB_LIndex(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	// no value
	listKey := []byte("my_list")
	v, err := db.LIndex(listKey, 0)
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Nil(t, v)

	// one value
	err = db.RPush(listKey, []byte("a"))
	assert.Nil(t, err)

	lVal1, err := db.LIndex(listKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, lVal1, []byte("a"))

	rVal1, err := db.LIndex(listKey, -1)
	assert.Nil(t, err)
	assert.Equal(t, rVal1, []byte("a"))

	// out of right range with one
	rOut1, err := db.LIndex(listKey, 1)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, rOut1)

	// out of left range with one
	lOut1, err := db.LIndex(listKey, -2)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, lOut1)

	// two values
	err = db.RPush(listKey, []byte("b"))
	assert.Nil(t, err)

	lVal1, err = db.LIndex(listKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, lVal1, []byte("a"))

	lVal2, err := db.LIndex(listKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, lVal2, []byte("b"))

	rVal1, err = db.LIndex(listKey, -2)
	assert.Nil(t, err)
	assert.Equal(t, rVal1, []byte("a"))

	rVal2, err := db.LIndex(listKey, -1)
	assert.Nil(t, err)
	assert.Equal(t, rVal2, []byte("b"))

	// out of right range with two
	rOut2, err := db.LIndex(listKey, 2)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, rOut2)

	// out of left range with two
	lOut2, err := db.LIndex(listKey, -3)
	assert.Equal(t, ErrWrongIndex, err)
	assert.Nil(t, lOut2)
}

func TestLazyDB_LLen(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)
	listKey := []byte("my_list")
	err := db.LPush(listKey, []byte("a"), []byte("b"), []byte("c"))
	assert.Nil(t, err)
	assert.Equal(t, 3, db.LLen(listKey))
}

func TestLazyDB_LRange(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	type args struct {
		key   []byte
		start int
		end   int
	}

	listKey := []byte("my_list")
	// prepare List
	err := db.LPush(listKey, []byte("a"))
	assert.Nil(t, err)
	err = db.LPush(listKey, []byte("b"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("c"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("d"))
	assert.Nil(t, err)
	err = db.RPush(listKey, []byte("e"))
	assert.Nil(t, err)

	tests := []struct {
		name       string
		db         *LazyDB
		args       args
		wantValues [][]byte
		wantErr    bool
	}{
		{"no key", db, args{key: nil, start: 0, end: 3}, [][]byte(nil), true},
		{"wrong key", db, args{key: []byte("myL"), start: 0, end: 3}, [][]byte(nil), true},
		{"start == stop", db, args{key: listKey, start: 1, end: 1}, [][]byte{[]byte("a")}, false},
		{"start == stop == tailSeq", db, args{key: listKey, start: 4, end: 4}, [][]byte{[]byte("e")}, false},
		{"stop reset to tailSeq", db, args{key: listKey, start: 0, end: 8}, [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("d"), []byte("e")}, false},
		{"start reset to headSeq", db, args{key: listKey, start: -9, end: 4}, [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("d"), []byte("e")}, false},
		{"start and end reset", db, args{key: listKey, start: -100, end: 100}, [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("d"), []byte("e")}, false},
		{"start negative end positive", db, args{key: listKey, start: -4, end: 2}, [][]byte{[]byte("a"), []byte("c")}, false},
		{"start out of range", db, args{key: listKey, start: 5, end: 10}, [][]byte(nil), true},
		{"stop out of range", db, args{key: listKey, start: 1, end: -8}, [][]byte(nil), true},
		{"stop larger than start", db, args{key: listKey, start: -1, end: 1}, [][]byte(nil), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, actualErr := tt.db.LRange(tt.args.key, tt.args.start, tt.args.end)
			assert.Equal(t, tt.wantValues, actual, "actual is not the same with expected")
			if (actualErr != nil) != tt.wantErr {
				t.Errorf("LRange() error = %v, wantErr %v", actualErr, tt.wantErr)
			}
		})
	}
}

func TestLazyDB_LMove(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	// none
	srcListKey := []byte("sourceL")
	dstListKey := []byte("distL")
	v, err := db.LMove(srcListKey, dstListKey, true, true)
	assert.Nil(t, v)
	assert.Nil(t, err)

	err = db.RPush(srcListKey, []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))
	assert.Nil(t, err)

	// left pop, left push
	v, err = db.LMove(srcListKey, dstListKey, true, true)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("a"))

	// left pop, right push
	v, err = db.LMove(srcListKey, dstListKey, true, false)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("b"))

	// right pop, left push
	v, err = db.LMove(srcListKey, dstListKey, false, true)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("e"))

	// right pop, right push
	v, err = db.LMove(srcListKey, dstListKey, false, false)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("d"))
	//distL: ["e","a","b","d"]

	v, err = db.LIndex(dstListKey, 0)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("e"))

	v, err = db.LIndex(dstListKey, 1)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("a"))

	v, err = db.LIndex(dstListKey, 2)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("b"))

	v, err = db.LIndex(dstListKey, 3)
	assert.Nil(t, err)
	assert.Equal(t, v, []byte("d"))
}
