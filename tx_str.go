package lazydb

import (
	"lazydb/logfile"
)

func (tx *Tx) Set(key, value []byte) {
	entry := &logfile.LogEntry{Key: key, Value: value}
	tx.pendingStr = append(tx.pendingStr, entry)
}
