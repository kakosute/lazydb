package lazydb

import (
	"lazydb/logfile"
	"lazydb/util"
)

// Get the value of the given key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *LazyDB) Get(key []byte) ([]byte, error) {
	shard := db.index.GetShardByReading(util.ByteToString(key))
	defer shard.RUnlock()

	value, err := db.getValue(key, valueTypeString)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Set the key to hold the string value. If key already holds a value, it is overwritten.
func (db *LazyDB) Set(key, value []byte) error {
	shard := db.index.GetShardByWriting(util.ByteToString(key))
	defer shard.Unlock()

	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}

	shard.Set(util.ByteToString(key), Value{
		value:     value,
		vType:     valueTypeString,
		fid:       valuePos.fid,
		offset:    valuePos.offset,
		entrySize: valuePos.entrySize,
	})
	return nil
}

// Delete value by the given key.
func (db *LazyDB) Delete(key []byte) error {
	shard := db.index.GetShardByWriting(util.ByteToString(key))
	defer shard.Unlock()

	entry := &logfile.LogEntry{Key: key, Stat: logfile.SDelete}
	_, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}

	shard.Remove(util.ByteToString(key))

	// TODO: send discard
	return nil
}
