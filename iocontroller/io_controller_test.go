package iocontroller

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIOSelector(t *testing.T) {
	testNewIOController(t, 0)
}

func TestFileIOController_Write(t *testing.T) {
	testIOControllerWrite(t, 0)
}

func TestFileIOController_Read(t *testing.T) {
	testIOControllerRead(t, 0)
}

func TestFileIOController_Sync(t *testing.T) {
	testIOControllerSync(t, 0)
}

func TestFileIOController_Close(t *testing.T) {
	testIOControllerClose(t, 0)
}

func TestFileIOController_Delete(t *testing.T) {
	testIOControllerDelete(t, 0)
}

func testNewIOController(t *testing.T, ioType uint8) {
	type args struct {
		fName string
		fsize int64
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"size-zero", args{fName: "000000001.wal", fsize: 0},
		},
		{
			"size-negative", args{fName: "000000002.wal", fsize: -1},
		},
		{
			"size-big", args{fName: "000000003.wal", fsize: 1024 << 20},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", tt.args.fName))
			assert.Nil(t, err)
			var ioController IOController
			if ioType == 0 {
				ioController, err = NewFileIOController(absPath, tt.args.fsize)
			}

			defer func() {
				if ioController != nil {
					err = ioController.Delete()
					assert.Nil(t, err)
				}
			}()
			if tt.args.fsize > 0 {
				assert.Nil(t, err)
				assert.NotNil(t, ioController)
			} else {
				assert.Equal(t, err, ErrInvalidFsize)
			}
		})
	}
}

func testIOControllerWrite(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.vlog"))
	assert.Nil(t, err)
	var size int64 = 1048576

	var ioController IOController
	if ioType == 0 {
		ioController, err = NewFileIOController(absPath, size)
	}

	assert.Nil(t, err)
	defer func() {
		if ioController != nil {
			_ = ioController.Delete()
		}
	}()

	type fields struct {
		ioController IOController
	}
	type args struct {
		b      []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil-byte", fields{ioController: ioController}, args{b: nil, offset: 0}, 0, false,
		},
		{
			"one-byte", fields{ioController: ioController}, args{b: []byte("0"), offset: 0}, 1, false,
		},
		{
			"many-bytes", fields{ioController: ioController}, args{b: []byte("lazydb"), offset: 0}, 6, false,
		},
		{
			"bigvalue-byte", fields{ioController: ioController}, args{b: []byte(fmt.Sprintf("%01048576d", 123)), offset: 0}, 1048576, false,
		},
		{
			"exceed-size", fields{ioController: ioController}, args{b: []byte(fmt.Sprintf("%01048577d", 123)), offset: 0}, 1048577, false,
		},
		{
			"EOF-error", fields{ioController: ioController}, args{b: []byte("lazydb"), offset: -1}, 0, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.ioController.Write(tt.args.b, tt.args.offset)
			// io.EOF err in mmmap.
			if tt.want == 1048577 && ioType == 1 {
				tt.wantErr = true
				tt.want = 0
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func testIOControllerRead(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "00000001.wal"))
	var ioController IOController
	if ioType == 0 {
		ioController, err = NewFileIOController(absPath, 100)
	}

	assert.Nil(t, err)
	defer func() {
		if ioController != nil {
			_ = ioController.Delete()
		}
	}()
	offsets := writeSomeData(ioController, t)
	results := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lazydb"),
	}

	type fields struct {
		ioController IOController
	}
	type args struct {
		b      []byte
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil", fields{ioController: ioController}, args{b: make([]byte, 0), offset: offsets[0]}, 0, false,
		},
		{
			"one-byte", fields{ioController: ioController}, args{b: make([]byte, 1), offset: offsets[1]}, 1, false,
		},
		{
			"many-bytes", fields{ioController: ioController}, args{b: make([]byte, 6), offset: offsets[2]}, 6, false,
		},
		{
			"EOF-1", fields{ioController: ioController}, args{b: make([]byte, 100), offset: -1}, 0, true,
		},
		{
			"EOF-2", fields{ioController: ioController}, args{b: make([]byte, 100), offset: 1024}, 0, true,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.ioController.Read(tt.args.b, tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			if !tt.wantErr {
				assert.Equal(t, tt.args.b, results[i])
			}
		})
	}
}

func writeSomeData(ioController IOController, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lazydb"),
	}

	var offsets []int64
	var offset int64
	for _, tt := range tests {
		offsets = append(offsets, offset)
		n, err := ioController.Write(tt, offset)
		assert.Nil(t, err)
		offset += int64(n)
	}
	return offsets
}

func testIOControllerSync(t *testing.T, ioType uint8) {
	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		assert.Nil(t, err)
		var ioController IOController
		if ioType == 0 {
			ioController, err = NewFileIOController(absPath, fsize)
		}

		assert.Nil(t, err)
		defer func() {
			if ioController != nil {
				_ = ioController.Delete()
			}
		}()
		writeSomeData(ioController, t)
		err = ioController.Sync()
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIOControllerClose(t *testing.T, ioType uint8) {
	sync := func(id int, fsize int64) {
		absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", id)))
		defer func() {
			_ = os.Remove(absPath)
		}()
		assert.Nil(t, err)
		var ioController IOController
		if ioType == 0 {
			ioController, err = NewFileIOController(absPath, fsize)
		}

		assert.Nil(t, err)
		defer func() {
			if ioController != nil {
				err := ioController.Close()
				assert.Nil(t, err)
			}
		}()
		writeSomeData(ioController, t)
		assert.Nil(t, err)
	}

	for i := 1; i < 4; i++ {
		sync(i, int64(i*100))
	}
}

func testIOControllerDelete(t *testing.T, ioType uint8) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"1", false},
		{"2", false},
		{"3", false},
		{"4", false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", fmt.Sprintf("0000000%d.wal", i)))
			assert.Nil(t, err)
			var ioController IOController
			if ioType == 0 {
				ioController, err = NewFileIOController(absPath, int64((i+1)*100))
			}

			assert.Nil(t, err)

			if err := ioController.Delete(); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
