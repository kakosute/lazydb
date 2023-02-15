package lazydb

import (
	"lazydb/logfile"
	"time"
)

const (
	defaultMaxLogFileSize       int64          = 512 << 20
	defaultLogFileMergeInterval time.Duration  = time.Hour * 8
	defaultIOType               logfile.IOType = logfile.FileIO
)

type DBConfig struct {
	DBPath               string        // Directory path for storing log files on disk.
	MaxLogFileSize       int64         // Max capacity of a log file.
	LogFileMergeInterval time.Duration // Max time interval for merging log files.

	//  IOType
	//  Only support FileIO at the moment
	IOType logfile.IOType
}

func DefaultDBConfig(path string) *DBConfig {
	return &DBConfig{
		DBPath:               path,
		MaxLogFileSize:       defaultMaxLogFileSize,
		LogFileMergeInterval: defaultLogFileMergeInterval,
		IOType:               defaultIOType,
	}
}
