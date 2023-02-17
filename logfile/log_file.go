package logfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"lazydb/iocontroller"
	"path/filepath"
	"sync"
	"sync/atomic"
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

var FTypeToString = map[FType]string{
	Strs: "log.strs.",
}

// FileType represents different types of log file: wal and value log.
type FType uint8

const (
	Strs FType = iota
)

var (
	//  convert string in filename to FType
	FileTypesMap = map[string]FType{}
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
func Open(path string, fid uint32, fsize int64, ftype FType, ioType IOType) (*LogFile, error) {
	if fsize <= 0 {
		return nil, errors.New("logfile: illegal file size")
	}
	if _, ok := FTypeToString[ftype]; !ok {
		return nil, errors.New("logfile: file type is not supported")
	}
	fileName := filepath.Join(path, FTypeToString[ftype]+fmt.Sprintf("%08d", fid))
	lf := &LogFile{Fid: fid}
	var controller iocontroller.IOController
	var err error
	switch ioType {
	case FileIO:
		if controller, err = iocontroller.NewFileIOController(fileName, fsize); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("logfile: io type is not supported")
	}
	lf.IoController = controller
	return lf, nil
}

// ReadLogEntry read a LogEntry from log file at offset.
// it returns LogEntry, entrySize and err if any
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int, error) {
	headerBuf := make([]byte, MaxHeaderSize)
	//read the header of the logEntry from the file
	_, err := lf.IoController.Read(headerBuf, offset)
	if err != nil {
		return nil, 0, err
	}
	le, size := decodeHeader(headerBuf)
	if le.crc == 0 && le.kSize == 0 && le.vSize == 0 {
		return nil, 0, errors.New("logfile: end of logEntry file")
	}
	kSize, vSize := int(le.kSize), int(le.vSize)
	var entrySize = size + kSize + vSize
	// use the size to read the key and value
	var kvBuf []byte
	if kSize > 0 || vSize > 0 {
		kvBuf = make([]byte, kSize+vSize)
		_, err = lf.IoController.Read(kvBuf, offset+int64(size))
		if err != nil {
			return nil, 0, err
		}
		le.Key = kvBuf[:kSize]
		le.Value = kvBuf[kSize:]
	}
	// check whether the crc is correct
	if crc := getEntryCrc(le, headerBuf[crc32.Size:size]); crc != le.crc {
		return nil, 0, errors.New("logfile: logEntry crc isn't correct")
	}
	return le, entrySize, nil
}

// Write a byte slice at the end of log file.
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.Offset)
	size, err := lf.IoController.Write(buf, offset)
	if err != nil {
		return err
	}
	if size != len(buf) {
		return errors.New("logfile: fail to write the whole entry")
	}
	atomic.AddInt64(&lf.Offset, int64(size))
	return nil
}

// Sync commits the current contents of the log file to stable storage.
func (lf *LogFile) Sync() error {
	return lf.IoController.Sync()
}

// Close current log file.
func (lf *LogFile) Close() error {
	return lf.IoController.Close()
}

// Delete delete current log file.
func (lf *LogFile) Delete() error {
	return lf.IoController.Delete()
}
