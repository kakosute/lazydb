package lazydb

import (
	"errors"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
	"log"
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
		pos, err := db.writeLogEntry(valueTypeHash, entry)
		if err != nil {
			return count, err
		}
		val, updated := idxTree.Delete(hashKey)
		if updated {
			count++
		}
		// delete invalid entry
		db.sendDiscard(val, updated, valueTypeHash)
		// also merge the delete entry
		_, size := logfile.EncodeEntry(entry)
		node := &Value{fid: pos.fid, entrySize: size}
		select {
		case db.discardsMap[valueTypeHash].valChan <- node:
		default:
			log.Fatal("send discard fail")
		}
	}
	return count, nil
}

// HExists returns whether the field exists in the hash stored at key
// Returns false either key or field is not exist
func (db *LazyDB) HExists(key []byte, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if idxTree == nil {
		return false, nil
	}

	hashkey := encodeKey(key, field)
	_, err := db.getValue(idxTree, hashkey, valueTypeHash)
	if err == ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// HGetAll returns all field-value pair exist in the hash stored at key
func (db *LazyDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if idxTree == nil {
		return [][]byte{}, nil
	}

	results := make([][]byte, 0)
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		value, err := db.getValue(idxTree, node.Key(), valueTypeHash)
		if err == ErrKeyNotFound {
			continue
		} else if err != nil {
			return [][]byte{}, err
		}
		_, field := decodeKey(node.Key())
		results = append(results, field)
		results = append(results, value)
	}
	return results, nil
}

// HKeys returns all fields exist in the hash stored at key
func (db *LazyDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if idxTree == nil {
		return [][]byte{}, nil
	}
	fields := make([][]byte, 0)
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		_, field := decodeKey(node.Key())
		fields = append(fields, field)
	}
	return fields, nil
}

// HVals returns all values exist in the hash stored at key
func (db *LazyDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if idxTree == nil {
		return [][]byte{}, nil
	}
	values := make([][]byte, 0)
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getValue(idxTree, node.Key(), valueTypeHash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		if err == ErrKeyNotFound {
			continue
		}
		values = append(values, val)
	}
	return values, nil
}

// HSetNX sets the given value if the key-field pair does not exist.
// Creates a new hash if key is not exist.
func (db *LazyDB) HSetNX(key, field, value []byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	strKey := util.ByteToString(key)
	if db.hashIndex.trees[strKey] == nil {
		db.hashIndex.trees[strKey] = ds.NewART()
	}
	idxTree := db.hashIndex.trees[strKey]

	hashKey := encodeKey(key, field)
	_, err := db.getValue(idxTree, hashKey, valueTypeHash)
	// field already exists
	if err == nil {
		return nil
	}
	if err != ErrKeyNotFound {
		return err
	}

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
	return nil
}

// HMGet returns multiple values by given fields
// It will skip those fields which don't exist.
func (db *LazyDB) HMGet(key []byte, fields ...[]byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	vals := make([][]byte, 0)
	idxTree, ok := db.hashIndex.trees[util.ByteToString(key)]
	if !ok {
		return vals, nil
	}

	for _, field := range fields {
		hashKey := encodeKey(key, field)
		val, err := db.getValue(idxTree, hashKey, valueTypeHash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		if err == ErrKeyNotFound {
			continue
		}
		vals = append(vals, val)
	}
	return vals, nil
}

func (db *LazyDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree, ok := db.hashIndex.trees[util.ByteToString(key)]
	if !ok {
		return 0
	}
	return idxTree.Size()
}
