package iocontroller

import (
	"errors"
	"os"
)

// FilePerm default permission of the newly created log file.
const FilePerm = 0644

// ErrInvalidFsize invalid file size.
var ErrInvalidFsize = errors.New("fsize can`t be zero or negative")

// FileIOController represents using standard file I/O.
type FileIOController struct {
	fd *os.File // system file descriptor.
}

// NewFileIOController creates a new file io selector.
func NewFileIOController(fName string, fsize int64) (IOController, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOController{fd: file}, nil
}

func (f *FileIOController) Write(b []byte, offset int64) (int, error) {
	return f.fd.WriteAt(b, offset)
}

func (f *FileIOController) Read(b []byte, offset int64) (int, error) {
	return f.fd.ReadAt(b, offset)
}

func (f *FileIOController) Sync() error {
	return f.fd.Sync()
}

func (f *FileIOController) Close() error {
	return f.fd.Close()
}

func (f *FileIOController) Delete() error {
	if err := f.fd.Close(); err != nil {
		return err
	}
	return os.Remove(f.fd.Name())
}

// open file and truncate it if necessary.
func openFile(fName string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}
