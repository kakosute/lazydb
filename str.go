package lazydb

import (
	"lazydb/logfile"
)

// Get the value of the given key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *LazyDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getValue(db.strIndex.idxTree, key, valueTypeString)
}

// Set the key to hold the string value. If key already holds a value, it is overwritten.
func (db *LazyDB) Set(key, value []byte) error {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}

	err = db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
	return err
}

// Del the value by the given key.
func (db *LazyDB) Del(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &logfile.LogEntry{Key: key, Stat: logfile.SDelete}
	_, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}
	_, _ = db.strIndex.idxTree.Delete(key)

	// TODO: send discard
	return nil
}
