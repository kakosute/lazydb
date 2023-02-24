package logfile

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestLogFile_Close(t *testing.T) {
	lf, err := Open("/tmp", 1, 100, Strs, FileIO)
	assert.Nil(t, err)
	err = lf.Delete()
	assert.Nil(t, err)
}

func TestLogFile_Delete(t *testing.T) {
	lf, err := Open("/tmp", 0, 100, Strs, FileIO)
	assert.Nil(t, err)
	err = lf.Delete()
	assert.Nil(t, err)
}

func writeSomeData(lf *LogFile, data [][]byte) []int64 {
	var offset []int64
	for _, v := range data {
		off := atomic.LoadInt64(&lf.Offset)
		offset = append(offset, off)
		if err := lf.Write(v); err != nil {
			panic(fmt.Sprintf("write data err.%+v", err))
		}
	}
	return offset
}

func TestLogFile_ReadLogEntry(t *testing.T) {
	lf, err := Open("/tmp", 1, 1<<20, Strs, FileIO)
	assert.Nil(t, err)
	// write some entries.
	entries := []*LogEntry{
		{ExpiredAt: 123332, Stat: SListMeta},
		{ExpiredAt: 123332, Stat: SDelete},
		{ExpiredAt: 994332343, Stat: SListMeta, Key: []byte(""), Value: []byte("")},
		{ExpiredAt: 7844332343, kSize: 2, vSize: 0, Key: []byte("k1"), Value: nil},
		{ExpiredAt: 99400542343, kSize: 0, vSize: 9, Key: nil, Value: []byte("test test")},
		{ExpiredAt: 8847333912, kSize: 2, vSize: 4, Key: []byte("k2"), Value: []byte("test")},
		{ExpiredAt: 8847333912, Stat: SDelete, kSize: 2, vSize: 9, Key: []byte("k3"), Value: []byte("some data")},
	}
	var vals [][]byte
	for _, e := range entries {
		v, _ := EncodeEntry(e)
		e.crc = crc32.ChecksumIEEE(v[4:])
		vals = append(vals, v)
	}
	offsets := writeSomeData(lf, vals)
	type fields struct {
		lf *LogFile
	}
	type args struct {
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *LogEntry
		want1   int
		wantErr bool
	}{
		{
			"read-entry-0", fields{lf: lf}, args{offset: offsets[0]}, entries[0], len(vals[0]), false,
		},
		{
			"read-entry-1", fields{lf: lf}, args{offset: offsets[1]}, entries[1], len(vals[1]), false,
		},
		{
			"read-entry-2", fields{lf: lf}, args{offset: offsets[2]}, &LogEntry{crc: entries[2].crc, ExpiredAt: 994332343, Stat: SListMeta}, len(vals[2]), false,
		},
		{
			"read-entry-3", fields{lf: lf}, args{offset: offsets[3]}, &LogEntry{crc: entries[3].crc, ExpiredAt: 7844332343, kSize: entries[3].kSize, vSize: entries[3].vSize, Key: []byte("k1"), Value: []byte{}}, len(vals[3]), false,
		},
		{
			"read-entry-4", fields{lf: lf}, args{offset: offsets[4]}, &LogEntry{crc: entries[4].crc, ExpiredAt: 99400542343, kSize: entries[4].kSize, vSize: entries[4].vSize, Key: []byte{}, Value: []byte("test test")}, len(vals[4]), false,
		},
		{
			"read-entry-5", fields{lf: lf}, args{offset: offsets[5]}, entries[5], len(vals[5]), false,
		},
		{
			"read-entry-6", fields{lf: lf}, args{offset: offsets[6]}, entries[6], len(vals[6]), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := lf.ReadLogEntry(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadLogEntry() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ReadLogEntry() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestLogFile_Sync(t *testing.T) {
	lf, err := Open("/tmp", 0, 100, Strs, FileIO)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			_ = lf.Delete()
		}
	}()
	err = lf.Sync()
	assert.Nil(t, err)
}

func TestLogFile_Write(t *testing.T) {
	lf, err := Open("/tmp", 1, 1<<20, Strs, FileIO)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			_ = lf.Delete()
		}
	}()
	type fields struct {
		lf *LogFile
	}
	type args struct {
		buf []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"nil", fields{lf: lf}, args{buf: nil}, false,
		},
		{
			"no-value", fields{lf: lf}, args{buf: []byte{}}, false,
		},
		{
			"normal-1", fields{lf: lf}, args{buf: []byte("test test test")}, false,
		},
		{
			"normal-2", fields{lf: lf}, args{buf: []byte("some data")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := lf.Write(tt.args.buf); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOpenLogFile(t *testing.T) {
	type args struct {
		path   string
		fid    uint32
		fsize  int64
		ftype  FType
		ioType IOType
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"zero-size", args{"/tmp", 0, 0, Strs, FileIO}, true,
		},
		{
			"normal-size", args{"/tmp", 1, 100, Strs, FileIO}, false,
		},
		{
			"big-size", args{"/tmp", 2, 1024 << 20, Strs, FileIO}, false,
		},
		{
			"negative-size", args{"/tmp", 3, -1024, Strs, FileIO}, true,
		},
		{
			"illegal-ftype", args{"/tmp", 4, 1024, 1, FileIO}, true,
		},
		{
			"illegal-iotype", args{"/tmp", 5, 1024, Strs, 1}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.path, tt.args.fid, tt.args.fsize, tt.args.ftype, tt.args.ioType)
			defer func() {
				if got != nil && got.IoController != nil {
					_ = got.Delete()
				}
			}()
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				t.Errorf("OpenLogFile() gotLf =nil, want not nil")
			}
		})
	}
}
