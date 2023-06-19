package lazydb

import (
	"lazydb/ds"
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
	HashIndexShardCount  int64         // default 32
	MaxLogFileSize       int64         // Max capacity of a log file.
	LogFileMergeInterval time.Duration // Max time interval for merging log files.

	//  IOType
	//  Only support FileIO at the moment
	IOType logfile.IOType
	// DiscardBufferSize a channel will be created to send the older entry size when a key updated or deleted.
	// Entry size will be saved in the discard file, recording the invalid size in a log file, and be used when log file gc is running.
	// This option represents the size of that channel.
	// If you got errors like `send discard chan fail`, you can increase this option to avoid it.
	DiscardBufferSize int

	// LogFileGCRatio if discarded data in log file exceeds this ratio, it can be picked up for compaction(garbage collection)
	// And if there are many files reached the ratio, we will pick the highest one by one.
	// The recommended ratio is 0.5, half of the file can be compacted.
	// Default value is 0.5.
	LogFileGCRatio float64
}

func DefaultDBConfig(path string) DBConfig {
	return DBConfig{
		DBPath:               path,
		HashIndexShardCount:  ds.DefaultShardCount,
		MaxLogFileSize:       defaultMaxLogFileSize,
		LogFileMergeInterval: defaultLogFileMergeInterval,
		IOType:               defaultIOType,
		DiscardBufferSize:    8 << 20,
		LogFileGCRatio:       0.5,
	}
}
