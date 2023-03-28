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
			"crc", args{&LogEntry{crc: 1696784233}}, []byte{163, 161, 194, 177, 0, 0, 0, 0, 0, 0}, 10,
		},
		{
			"no_content", args{&LogEntry{}}, []byte{163, 161, 194, 177, 0, 0, 0, 0, 0, 0}, 10,
		},
		{
			"expiredAt_key_value", args{&LogEntry{ExpiredAt: 1676969769, Stat: SListMeta, TxID: 11111111, TxStat: TxUncommited, Key: []byte("a"), Value: []byte("abc")}}, []byte{17, 148, 49, 87, 2, 210, 156, 164, 191, 12, 142, 171, 204, 10, 4, 2, 6, 97, 97, 98, 99}, 21,
		},
		{
			"delete", args{&LogEntry{Stat: SDelete}}, []byte{6, 114, 158, 122, 1, 0, 0, 0, 0, 0}, 10,
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
			"no_content", args{buf: []byte{163, 161, 194, 177, 0, 0, 0, 0, 0, 0}}, &LogEntry{crc: 2982322595}, 10,
		},
		{
			"expiredAt", args{buf: []byte{43, 161, 225, 52, 2, 210, 156, 164, 191, 12, 142, 171, 204, 10, 2, 2, 6, 97, 97, 98, 99}}, &LogEntry{crc: 887202091, ExpiredAt: 1676969769, Stat: SListMeta, TxID: 11111111, TxStat: 1, kSize: 1, vSize: 3}, 17,
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
			"no_content", args{buf: []byte{163, 161, 194, 177, 0, 0, 0, 0, 0, 0}, le: &LogEntry{crc: 2982322595}}, 2982322595,
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
