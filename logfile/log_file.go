package logfile

import (
	"lazydb/iocontroller"
	"sync"
)

// IOType represents different types of file io: FileIO(standard file io).
type IOType uint8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// can add more type when needed
)

const (
	// FilePrefix log file prefix. Full name of a file for example file of strings is like: "path/log.strs.00000001".
	FilePrefix = "log."
)

// FileType represents different types of log file: wal and value log.
type FType uint8

const (
	Strs FType = iota
)

// LogFile is an abstraction of a disk file, entry`s read and write will go through it.
type LogFile struct {
	Fid          uint32
	Offset       int64
	IoController iocontroller.IOController
	Mu           sync.RWMutex
}

// Open opens an existing or create a new log file.
// fsize must be a postitive number.And we will create io controller according to ioType.
func Open(path string, fid uint32, fsize int64, ftype uint8, ioType IOType) (lf *LogFile, err error) {
	return &LogFile{}, nil
}

// ReadLogEntry read a LogEntry from log file at offset.
// it returns LogEntry, entrySize and err if any
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int, error) {
	return &LogEntry{}, 0, nil
}

// Write a byte slice at the end of log file.
func (lf *LogFile) Write(buf []byte) error {
	return nil
}

// Sync commits the current contents of the log file to stable storage.
func (lf *LogFile) Sync() error {
	return nil
}

// Close current log file.
func (lf *LogFile) Close() error {
	return nil
}

// Delete delete current log file.
func (lf *LogFile) Delete() error {
	return nil
}
