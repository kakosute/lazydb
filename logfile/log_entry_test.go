package logfile

import (
	"reflect"
	"testing"
)

func TestEncodeEntry(t *testing.T) {
	type args struct {
		e *LogEntry
	}
	tests := []struct {
		situation string
		args      args
		want      []byte
		want1     int
	}{
		{
			"nil", args{e: nil}, nil, 0,
		},
		{
			"no_content", args{&LogEntry{}}, []byte{28, 223, 68, 33, 0, 0, 0, 0}, 8,
		},
		{
			"expiredAt_key_value", args{&LogEntry{ExpiredAt: 1676969769, status: SListMeta, Key: []byte("a"), Value: []byte("abc")}}, []byte{111, 64, 3, 225, 2, 210, 156, 164, 191, 12, 2, 6, 97, 97, 98, 99}, 16,
		},
		{
			"delete", args{&LogEntry{status: SDelete}}, []byte{121, 184, 248, 153, 1, 0, 0, 0}, 8,
		},
	}
	for _, tt := range tests {
		t.Run(tt.situation, func(t *testing.T) {
			got, got1 := EncodeEntry(tt.args.e)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeEntry() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("EncodeEntry() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_decodeHeader(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		situation string
		args      args
		want      *LogEntry
		want1     int
	}{
		{
			"nil", args{buf: nil}, nil, 0,
		},
		{
			"no_enough_content", args{buf: []byte{105, 223, 34, 101}}, nil, 0,
		},
		{
			"no_content", args{buf: []byte{105, 223, 34, 101, 0, 0, 0, 0}}, &LogEntry{crc: 1696784233}, 8,
		},
		{
			"expiredAt", args{buf: []byte{85, 205, 109, 118, 2, 210, 156, 164, 191, 12, 2, 6}}, &LogEntry{crc: 1986907477, ExpiredAt: 1676969769, status: SListMeta, kSize: 1, vSize: 3}, 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.situation, func(t *testing.T) {
			got, got1 := decodeHeader(tt.args.buf)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeHeader() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("decodeHeader() got1 = %v, want1 %v", got1, tt.want1)
			}
		})
	}
}

func Test_getEntryCrc(t *testing.T) {
	type args struct {
		buf []byte
		le  *LogEntry
	}
	tests := []struct {
		situation string
		args      args
		want      uint32
	}{
		{
			"nil", args{buf: nil, le: nil}, 0,
		},
		{
			"no_enough_content", args{buf: []byte{105, 223, 34, 101}, le: &LogEntry{crc: 2077607535}}, 0,
		},
		{
			"no_content", args{buf: []byte{105, 223, 34, 101, 0, 0, 0, 0}, le: &LogEntry{crc: 558161692}}, 558161692,
		},
		{
			"expiredAt_key_value", args{buf: []byte{85, 205, 109, 118, 2, 210, 156, 164, 191, 12, 2, 6}, le: &LogEntry{Key: []byte("a"), Value: []byte("abc")}}, 3775086703,
		},
	}
	for _, tt := range tests {
		t.Run(tt.situation, func(t *testing.T) {
			got := getEntryCrc(tt.args.buf, tt.args.le)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getEntryCrc() got = %v, want %v", got, tt.want)
			}
		})
	}
}
