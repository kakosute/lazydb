package iocontroller

import (
	"io"
	"lazydb/mmap"
	"os"
)

// MMapController represents using memory map I/O.
type MMapController struct {
	fd     *os.File
	buf    []byte
	bufLen int64
}

// NewMMapController creates a new MMap controller
func NewMMapController(fName string, fsize int64) (IOController, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.MMap(file, true, fsize)
	if err != nil {
		return nil, err
	}
	return &MMapController{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

// Write writes slice b into mapped region(buf) at offset
func (m *MMapController) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	if offset < 0 || offset+length > m.bufLen {
		return 0, io.EOF
	}
	return copy(m.buf[offset:], b), nil

}

// Read reads mapped region at offset into slice b
func (m *MMapController) Read(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if offset < 0 || offset >= m.bufLen || offset+length >= m.bufLen {
		return 0, io.EOF
	}
	return copy(b, m.buf[offset:]), nil
}

// Sync synchronize the mapped buffer to the file's contents on disk.
func (m *MMapController) Sync() error {
	return mmap.MSync(m.buf)
}

// Close closes fd
func (m *MMapController) Close() error {
	err := mmap.MSync(m.buf)
	if err != nil {
		return err
	}
	err = mmap.MUnmap(m.buf)
	if err != nil {
		return err
	}
	return m.fd.Close()
}

// Delete deleted file on disk
func (m *MMapController) Delete() error {
	err := mmap.MUnmap(m.buf)
	if err != nil {
		return err
	}
	m.buf = nil
	err = m.fd.Truncate(0)
	if err != nil {
		return err
	}
	err = m.fd.Close()
	if err != nil {
		return err
	}
	return os.Remove(m.fd.Name())
}
