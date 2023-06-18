package mmap

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

var testData = []byte("0123456789ABCDEF")
var testPath = filepath.Join(os.TempDir(), "testdata")

func init() {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	f.Write(testData)
	f.Close()
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0644)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func TestMMap(t *testing.T) {
	f := openFile(os.O_CREATE | os.O_RDWR)
	defer f.Close()
	type args struct {
		fd       *os.File
		writable bool
		size     int64
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"normal-size", args{fd: f, writable: true, size: 100}, false,
		},
		{
			"big-size", args{fd: f, writable: true, size: 128 << 20}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MMap(tt.args.fd, tt.args.writable, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mmap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if int64(len(b)) != tt.args.size {
				t.Errorf("Mmap() want buf size = %d, actual = %d", tt.args.size, len(b))
			}
		})
	}
}

func TestMUnmap(t *testing.T) {
	f := openFile(os.O_RDONLY | os.O_RDWR)
	defer f.Close()
	b, err := MMap(f, true, 128)
	assert.Nil(t, err)
	err = MUnmap(b)
	assert.Nil(t, err)

}
func TestMSync(t *testing.T) {
	f := openFile(os.O_CREATE | os.O_RDWR)
	defer f.Close()
	b, err := MMap(f, true, 128)
	assert.Nil(t, err)
	err = MSync(b)
	assert.Nil(t, err)
}

func TestMAdvise(t *testing.T) {
	f := openFile(os.O_CREATE | os.O_RDWR)
	defer f.Close()
	b, err := MMap(f, true, 128)
	assert.Nil(t, err)
	err = MAdvise(b, false)
	assert.Nil(t, err)
}
