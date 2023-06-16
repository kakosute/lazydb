package lazydb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLazyDB_SAdd(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	type args struct {
		key     []byte
		members [][]byte
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "normal add 1",
			args: args{
				key:     []byte("add1"),
				members: [][]byte{[]byte("v1"), []byte("v2")},
			},
			wantErr: false,
		},
		{
			name: "normal add empty",
			args: args{
				key:     []byte("add_empty"),
				members: [][]byte{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.SAdd(tt.args.key, tt.args.members...)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLazyDB_SIsMember(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	_ = db.SAdd([]byte("key1"), [][]byte{[]byte("v1"), []byte("v2")}...)
	_ = db.SAdd([]byte("key2"))

	type args struct {
		key    []byte
		member []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "has value",
			args: args{
				key:    []byte("key1"),
				member: []byte("v1"),
			},
			want: true,
		},
		{
			name: "not has value",
			args: args{
				key:    []byte("key1"),
				member: []byte("v3"),
			},
			want: false,
		},
		{
			name: "not has in empty set",
			args: args{
				key:    []byte("key2"),
				member: []byte("v3"),
			},
			want: false,
		},
		{
			name: "not has with wrong key",
			args: args{
				key:    []byte("key3"),
				member: []byte("v3"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := db.SIsMember(tt.args.key, tt.args.member)
			assert.Equal(t, tt.want, res)
		})
	}
}

func TestLazyDB_SMembers(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	_ = db.SAdd([]byte("key1"), [][]byte{[]byte("v1"), []byte("v2")}...)
	_ = db.SAdd([]byte("key2"))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			name: "case1",
			args: args{
				key: []byte("key1"),
			},
			want:    [][]byte{[]byte("v1"), []byte("v2")},
			wantErr: false,
		},
		{
			name: "case2: empty set",
			args: args{
				key: []byte("key2"),
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "case3: wrong key",
			args: args{
				key: []byte("key3"),
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.SMembers(tt.args.key)
			assert.Equalf(t, tt.want, got, "SMembers(%v)", tt.args.key)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLazyDB_SPop(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	_ = db.SAdd([]byte("key1"), [][]byte{[]byte("v1")}...)
	_ = db.SAdd([]byte("key2"))
	_ = db.SAdd([]byte("key3"), [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")}...)
	_ = db.SAdd([]byte("key4"), [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")}...)

	type args struct {
		key []byte
		num uint
	}
	tests := []struct {
		name    string
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			name: "case1",
			args: args{
				key: []byte("key1"),
				num: 1,
			},
			want:    [][]byte{[]byte("v1")},
			wantErr: false,
		},
		{
			name: "case2: pop empty",
			args: args{
				key: []byte("key2"),
				num: 1,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "case3: pop wrong key",
			args: args{
				key: []byte("key19"),
				num: 1,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := db.SPop(tt.args.key, tt.args.num)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLazyDB_SRem(t *testing.T) {
	db := initTestDB()
	defer destroyDB(db)
	assert.NotNil(t, db)

	_ = db.SAdd([]byte("key1"), [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")}...)
	_ = db.SAdd([]byte("key2"))
	_ = db.SAdd([]byte("key3"), [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")}...)
	_ = db.SAdd([]byte("key4"), [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")}...)

	type args struct {
		key     []byte
		members [][]byte
	}
	tests := []struct {
		name    string
		args    args
		want    [][]byte
		wantErr bool
	}{
		{
			name: "remove1",
			args: args{
				key:     []byte("key1"),
				members: [][]byte{[]byte("v3")},
			},
			want:    [][]byte{[]byte("v4"), []byte("v1"), []byte("v2")},
			wantErr: false,
		},
		{
			name: "remove empty key",
			args: args{
				key:     []byte("key19"),
				members: [][]byte{[]byte("v3")},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "remove empty value",
			args: args{
				key:     []byte("key2"),
				members: [][]byte{[]byte("v19")},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "remove empty value",
			args: args{
				key:     []byte("key2"),
				members: [][]byte{[]byte("v19")},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "remove multi value 1",
			args: args{
				key:     []byte("key3"),
				members: [][]byte{[]byte("v1"), []byte("v2")},
			},
			want:    [][]byte{[]byte("v3"), []byte("v4")},
			wantErr: false,
		},
		{
			name: "remove multi value 2",
			args: args{
				key:     []byte("key3"),
				members: [][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")},
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.SRem(tt.args.key, tt.args.members...)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.NoError(t, err)
			}
			got, _ := db.SMembers(tt.args.key)
			assert.Equal(t, tt.want, got)
		})
	}
}
