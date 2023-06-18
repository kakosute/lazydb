package lazydb

import (
	"github.com/stretchr/testify/assert"
	"lazydb/util"
	"os"
	"path/filepath"
	"testing"
)

func initTestZset() *LazyDB {
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "test_zset")
	if !util.PathExist(path) {
		os.MkdirAll(path, os.ModePerm)
	}
	cfg := DefaultDBConfig(path)
	db, _ := Open(cfg)
	return db
}

func TestLazyDB_ZAdd(t *testing.T) {
	db := initTestZset()
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
				key:  []byte("z1"),
				args: [][]byte{util.Float64ToByte(1), []byte("m1")},
			},
			wantErr: false,
		},
		{
			name: "multiple pairs",
			args: args{
				key:  []byte("z2"),
				args: [][]byte{util.Float64ToByte(1), []byte("m1"), util.Float64ToByte(2), []byte("m2")},
			},
			wantErr: false,
		},
		{
			name: "update existed score",
			args: args{
				key:  []byte("z2"),
				args: [][]byte{util.Float64ToByte(11), []byte("m1"), util.Float64ToByte(12), []byte("m2")},
			},
			wantErr: false,
		},
		{
			name: "odd number of param",
			args: args{
				key:  []byte("z3"),
				args: [][]byte{util.Float64ToByte(1)},
			},
			wantErr:     true,
			expectedErr: ErrInvalidParam,
		},
		{
			name: "zero pair",
			args: args{
				key:  []byte("z4"),
				args: [][]byte{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.ZAdd(tt.args.key, tt.args.args...)
			if tt.wantErr {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedErr, err)
			}
		})
	}
}

func TestLazyDB_ZScore(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

	type args struct {
		key    []byte
		member []byte
	}

	tests := []struct {
		name          string
		args          args
		expectedValue float64
		expectedErr   error
	}{
		{
			name: "existed member and score",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_m1"),
			},
			expectedValue: 1,
			expectedErr:   nil,
		},
		{
			name: "updated member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_m2"),
			},
			expectedValue: 11,
			expectedErr:   nil,
		},
		{
			name: "not existed key",
			args: args{
				key:    []byte("k_not_exist"),
				member: []byte("k_not_exist_member"),
			},
			expectedValue: 0,
			expectedErr:   ErrZSetKeyNotExist,
		},
		{
			name: "not existed member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_not_exist_member"),
			},
			expectedValue: 0,
			expectedErr:   ErrZSetMemberNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.ZScore(tt.args.key, tt.args.member)
			assert.Equal(t, tt.expectedValue, got)
			assert.Equal(t, tt.expectedErr, err)
		})
	}

}

func TestLazyDB_ZCard(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

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
			want: 2,
		},
		{
			name: "empty key",
			args: args{key: []byte("k2")},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := db.ZCard(tt.args.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLazyDB_ZRank(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

	type args struct {
		key    []byte
		member []byte
	}

	tests := []struct {
		name          string
		args          args
		expectedValue int
		expectedErr   error
	}{
		{
			name: "existed key and member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_m1"),
			},
			expectedValue: 0,
			expectedErr:   nil,
		},
		{
			name: "updated member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_m2"),
			},
			expectedValue: 1,
			expectedErr:   nil,
		},
		{
			name: "not existed key",
			args: args{
				key:    []byte("k_not_exist"),
				member: []byte("k_not_exist_member"),
			},
			expectedValue: -1,
			expectedErr:   ErrZSetKeyNotExist,
		},
		{
			name: "not existed member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_not_exist_member"),
			},
			expectedValue: -1,
			expectedErr:   ErrZSetMemberNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.ZRank(tt.args.key, tt.args.member)
			assert.Equal(t, tt.expectedValue, got)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZRevRank(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

	type args struct {
		key    []byte
		member []byte
	}

	tests := []struct {
		name          string
		args          args
		expectedValue int
		expectedErr   error
	}{
		{
			name: "existed key and member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_m1"),
			},
			expectedValue: 1,
			expectedErr:   nil,
		},
		{
			name: "updated member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_m2"),
			},
			expectedValue: 0,
			expectedErr:   nil,
		},
		{
			name: "not existed key",
			args: args{
				key:    []byte("k_not_exist"),
				member: []byte("k_not_exist_member"),
			},
			expectedValue: -1,
			expectedErr:   ErrZSetKeyNotExist,
		},
		{
			name: "not existed member",
			args: args{
				key:    []byte("k1"),
				member: []byte("k1_not_exist_member"),
			},
			expectedValue: -1,
			expectedErr:   ErrZSetMemberNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.ZRevRank(tt.args.key, tt.args.member)
			assert.Equal(t, tt.expectedValue, got)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZRange(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

	type args struct {
		key   []byte
		start int
		stop  int
	}

	tests := []struct {
		name          string
		args          args
		expectedValue [][]byte
	}{
		{
			name: "existed key and member",
			args: args{
				key:   []byte("k1"),
				start: 0,
				stop:  1,
			},
			expectedValue: [][]byte{util.StringToByte("k1_m1"), util.StringToByte("k1_m2")},
		},
		{
			name: "negative members",
			args: args{
				key:   []byte("k1"),
				start: -2,
				stop:  -1,
			},
			expectedValue: [][]byte{util.StringToByte("k1_m1"), util.StringToByte("k1_m2")},
		},
		{
			name: "not existed key",
			args: args{
				key:   []byte("k2"),
				start: 0,
				stop:  1,
			},
			expectedValue: nil,
		},
		{
			name: "out of range",
			args: args{
				key:   []byte("k1"),
				start: 0,
				stop:  4,
			},
			expectedValue: [][]byte{util.StringToByte("k1_m1"), util.StringToByte("k1_m2")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := db.ZRange(tt.args.key, tt.args.start, tt.args.stop)
			assert.Equal(t, tt.expectedValue, got)
		})
	}
}

func TestLazyDB_ZRevRange(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

	type args struct {
		key   []byte
		start int
		stop  int
	}

	tests := []struct {
		name          string
		args          args
		expectedValue [][]byte
	}{
		{
			name: "existed key and member",
			args: args{
				key:   []byte("k1"),
				start: 0,
				stop:  1,
			},
			expectedValue: [][]byte{util.StringToByte("k1_m2"), util.StringToByte("k1_m1")},
		},
		{
			name: "negative members",
			args: args{
				key:   []byte("k1"),
				start: -2,
				stop:  -1,
			},
			expectedValue: [][]byte{util.StringToByte("k1_m2"), util.StringToByte("k1_m1")},
		},
		{
			name: "not existed key",
			args: args{
				key:   []byte("k2"),
				start: 0,
				stop:  1,
			},
			expectedValue: nil,
		},
		{
			name: "out of range",
			args: args{
				key:   []byte("k1"),
				start: 0,
				stop:  4,
			},
			expectedValue: [][]byte{util.StringToByte("k1_m2"), util.StringToByte("k1_m1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := db.ZRevRange(tt.args.key, tt.args.start, tt.args.stop)
			assert.Equal(t, tt.expectedValue, got)
		})
	}
}

func TestLazyDB_ZIncrBy(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"))
	db.ZAdd([]byte("k1"), util.Float64ToByte(11), []byte("k1_m2"))

	type args struct {
		key       []byte
		increment float64
		member    []byte
	}

	tests := []struct {
		name          string
		args          args
		expectedValue float64
		expectedErr   error
	}{
		{
			name: "existed key and member",
			args: args{
				key:       []byte("k1"),
				increment: 1.1,
				member:    []byte("k1_m1"),
			},
			expectedValue: 2.1,
			expectedErr:   nil,
		},
		{
			name: "not existed key",
			args: args{
				key:       []byte("k_not_exist"),
				increment: 2.2,
				member:    []byte("k_not_exist_member"),
			},
			expectedValue: 2.2,
			expectedErr:   nil,
		},
		{
			name: "not existed member",
			args: args{
				key:       []byte("k1"),
				increment: -2.2,
				member:    []byte("k1_not_exist_member"),
			},
			expectedValue: -2.2,
			expectedErr:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.ZIncrBy(tt.args.key, tt.args.increment, tt.args.member)
			assert.Equal(t, tt.expectedValue, got)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZRem(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"),
		util.Float64ToByte(3), []byte("k1_m3"), util.Float64ToByte(4), []byte("k1_m4"), util.Float64ToByte(5), []byte("k1_m5"))

	type args struct {
		key     []byte
		members [][]byte
	}
	tests := []struct {
		name          string
		args          args
		expectedValue int
		expectedErr   error
	}{
		{
			name: "single member",
			args: args{
				key:     []byte("k1"),
				members: [][]byte{[]byte("k1_m1")},
			},
			expectedValue: 1,
			expectedErr:   nil,
		},
		{
			name: "multiple members",
			args: args{
				key:     []byte("k1"),
				members: [][]byte{[]byte("k1_m2"), []byte("k1_m3")},
			},
			expectedValue: 2,
			expectedErr:   nil,
		},
		{
			name: "not existed key",
			args: args{
				key:     []byte("k_not_exist"),
				members: [][]byte{[]byte("k1_m1"), []byte("k1_m2")},
			},
			expectedValue: 0,
			expectedErr:   nil,
		},
		{
			name: "part of existed member",
			args: args{
				key:     []byte("k1"),
				members: [][]byte{[]byte("k1_m4"), []byte("k1_not_exist_member")},
			},
			expectedValue: 1,
			expectedErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.ZRem(tt.args.key, tt.args.members...)
			assert.Equal(t, tt.expectedValue, got)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZPopMax(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"),
		util.Float64ToByte(3), []byte("k1_m3"), util.Float64ToByte(4), []byte("k1_m4"), util.Float64ToByte(5), []byte("k1_m5"))

	type args struct {
		key []byte
	}

	tests := []struct {
		name           string
		args           args
		expectedMember []byte
		expectedScore  float64
		expectedErr    error
	}{
		{
			name: "existed key and member",
			args: args{
				key: []byte("k1"),
			},
			expectedMember: util.StringToByte("k1_m5"),
			expectedScore:  5,
			expectedErr:    nil,
		},
		{
			name: "second pop",
			args: args{
				key: []byte("k1"),
			},
			expectedMember: util.StringToByte("k1_m4"),
			expectedScore:  4,
			expectedErr:    nil,
		},
		{
			name: "not existed key",
			args: args{
				key: []byte("k2"),
			},
			expectedMember: nil,
			expectedScore:  0,
			expectedErr:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			member, score, err := db.ZPopMax(tt.args.key)
			assert.Equal(t, tt.expectedMember, member)
			assert.Equal(t, tt.expectedScore, score)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZPopMaxWithCount(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"),
		util.Float64ToByte(3), []byte("k1_m3"), util.Float64ToByte(4), []byte("k1_m4"), util.Float64ToByte(5), []byte("k1_m5"))

	type args struct {
		key   []byte
		count int
	}

	tests := []struct {
		name            string
		args            args
		expectedMembers [][]byte
		expectedScores  []float64
		expectedErr     error
	}{
		{
			name: "count is 1",
			args: args{
				key:   []byte("k1"),
				count: 1,
			},
			expectedMembers: [][]byte{util.StringToByte("k1_m5")},
			expectedScores:  []float64{5},
			expectedErr:     nil,
		},
		{
			name: "count is 2",
			args: args{
				key:   []byte("k1"),
				count: 2,
			},
			expectedMembers: [][]byte{util.StringToByte("k1_m4"), util.StringToByte("k1_m3")},
			expectedScores:  []float64{4, 3},
			expectedErr:     nil,
		},
		{
			name: "count is bigger than card",
			args: args{
				key:   []byte("k1"),
				count: 10,
			},
			expectedMembers: [][]byte{util.StringToByte("k1_m2"), util.StringToByte("k1_m1")},
			expectedScores:  []float64{2, 1},
			expectedErr:     nil,
		},
		{
			name: "not existed key",
			args: args{
				key: []byte("k2"),
			},
			expectedMembers: nil,
			expectedScores:  nil,
			expectedErr:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members, scores, err := db.ZPopMaxWithCount(tt.args.key, tt.args.count)
			assert.Equal(t, tt.expectedMembers, members)
			assert.Equal(t, tt.expectedScores, scores)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZPopMin(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"),
		util.Float64ToByte(3), []byte("k1_m3"), util.Float64ToByte(4), []byte("k1_m4"), util.Float64ToByte(5), []byte("k1_m5"))

	type args struct {
		key []byte
	}

	tests := []struct {
		name           string
		args           args
		expectedMember []byte
		expectedScore  float64
		expectedErr    error
	}{
		{
			name: "existed key and member",
			args: args{
				key: []byte("k1"),
			},
			expectedMember: util.StringToByte("k1_m1"),
			expectedScore:  1,
			expectedErr:    nil,
		},
		{
			name: "second pop",
			args: args{
				key: []byte("k1"),
			},
			expectedMember: util.StringToByte("k1_m2"),
			expectedScore:  2,
			expectedErr:    nil,
		},
		{
			name: "not existed key",
			args: args{
				key: []byte("k2"),
			},
			expectedMember: nil,
			expectedScore:  0,
			expectedErr:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			member, score, err := db.ZPopMin(tt.args.key)
			assert.Equal(t, tt.expectedMember, member)
			assert.Equal(t, tt.expectedScore, score)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestLazyDB_ZPopMinWithCount(t *testing.T) {
	db := initTestZset()
	defer destroyDB(db)
	assert.NotNil(t, db)

	db.ZAdd([]byte("k1"), util.Float64ToByte(1), []byte("k1_m1"), util.Float64ToByte(2), []byte("k1_m2"),
		util.Float64ToByte(3), []byte("k1_m3"), util.Float64ToByte(4), []byte("k1_m4"), util.Float64ToByte(5), []byte("k1_m5"))

	type args struct {
		key   []byte
		count int
	}

	tests := []struct {
		name            string
		args            args
		expectedMembers [][]byte
		expectedScores  []float64
		expectedErr     error
	}{
		{
			name: "count is 1",
			args: args{
				key:   []byte("k1"),
				count: 1,
			},
			expectedMembers: [][]byte{util.StringToByte("k1_m1")},
			expectedScores:  []float64{1},
			expectedErr:     nil,
		},
		{
			name: "count is 2",
			args: args{
				key:   []byte("k1"),
				count: 2,
			},
			expectedMembers: [][]byte{util.StringToByte("k1_m2"), util.StringToByte("k1_m3")},
			expectedScores:  []float64{2, 3},
			expectedErr:     nil,
		},
		{
			name: "count is bigger than card",
			args: args{
				key:   []byte("k1"),
				count: 10,
			},
			expectedMembers: [][]byte{util.StringToByte("k1_m4"), util.StringToByte("k1_m5")},
			expectedScores:  []float64{4, 5},
			expectedErr:     nil,
		},
		{
			name: "not existed key",
			args: args{
				key: []byte("k2"),
			},
			expectedMembers: nil,
			expectedScores:  nil,
			expectedErr:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members, scores, err := db.ZPopMinWithCount(tt.args.key, tt.args.count)
			assert.Equal(t, tt.expectedMembers, members)
			assert.Equal(t, tt.expectedScores, scores)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}
