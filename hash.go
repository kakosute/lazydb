package lazydb

import (
	"errors"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
)

var (
	ErrInvalidParam = errors.New("parameters are invalid")
)

// HSet is used to insert a field value pair for key. If key does not exist, a new key will be created.
// If the field already exist, the value will be updated.
// Multiple field-value pair could be inserted in the format of "key field1 value1 field2 value2"
func (db *LazyDB) HSet(key []byte, args ...[]byte) error {
	if len(args)&1 == 1 {
		return ErrInvalidParam
	}
	if len(args) == 0 {
		return nil
	}
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	strKey := util.ByteToString(key)
	if db.hashIndex.trees[strKey] == nil {
		db.hashIndex.trees[strKey] = ds.NewART()
	}

	idxTree := db.hashIndex.trees[strKey]
	for i := 0; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		hashKey := encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Value: value}
		valPos, err := db.writeLogEntry(valueTypeHash, entry)
		if err != nil {
			return err
		}
		// TODO: sendDiscard
		err = db.updateIndexTree(valueTypeHash, idxTree, entry, valPos, false)
		if err != nil {
			return err
		}
	}
	return nil
}

// HGet returns value of given key and field. It will return empty if key is not found.
func (db *LazyDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if idxTree == nil {
		return nil, nil
	}

	hashKey := encodeKey(key, field)
	val, err := db.getValue(idxTree, hashKey, valueTypeHash)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// HDel delete the field-value pair under the given key
func (db *LazyDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if idxTree == nil {
		return 0, nil
	}
	var count int
	for _, field := range fields {
		hashKey := encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Stat: logfile.SDelete}
		_, err := db.writeLogEntry(valueTypeHash, entry)
		if err != nil {
			return count, err
		}
		_, updated := idxTree.Delete(hashKey)
		if updated {
			count++
		}
		// TODO send discard
	}
	return count, nil
}
