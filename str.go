package lazydb

import (
	"bytes"
	"errors"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"math"
	"regexp"
	"strconv"
	"time"
)

var (
	ErrWrongValueType  = errors.New("value is not an integer")
	ErrIntegerOverFlow = errors.New("integer overflow")
)

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *LazyDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
	return err
}

// Get get the value of key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *LazyDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getValue(db.strIndex.idxTree, key, valueTypeString)
}

// MGet get the values of all specified keys.
// If the key that does not hold a string value or does not exist, nil is returned.
func (db *LazyDB) MGet(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, ErrInvalidParam
	}
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	values := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

// GetRange returns the substring of the string value stored at key,
// determined by the offsets start and end.
func (db *LazyDB) GetRange(key []byte, start, end int) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return []byte{}, nil
	}
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}
	if end > len(val)-1 {
		end = len(val) - 1
	}
	if start > len(val)-1 || start > end {
		return []byte{}, nil
	}
	return val[start : end+1], nil
}

// GetDel gets the value of the key and deletes the key. This method is similar
// to Get method. It also deletes the key if it exists.
func (db *LazyDB) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	entry := &logfile.LogEntry{Key: key, Stat: logfile.SDelete}
	pos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return nil, err
	}
	delVal, updated := db.strIndex.idxTree.Delete(key)

	// delete invalid entry
	db.sendDiscard(delVal, updated, valueTypeString)
	// also merge the delete entry
	_, size := logfile.EncodeEntry(entry)
	node := &Value{fid: pos.fid, entrySize: size}
	select {
	case db.discardsMap[valueTypeString].valChan <- node:
	default:
		log.Fatal("send discard fail")
	}
	return val, nil
}

// Delete value at the given key.
func (db *LazyDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &logfile.LogEntry{Key: key, Stat: logfile.SDelete}
	pos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}
	delVal, updated := db.strIndex.idxTree.Delete(key)

	// delete invalid entry
	db.sendDiscard(delVal, updated, valueTypeString)
	// also merge the delete entry
	_, size := logfile.EncodeEntry(entry)
	node := &Value{fid: pos.fid, entrySize: size}
	select {
	case db.discardsMap[valueTypeString].valChan <- node:
	default:
		log.Fatal("send discard fail")
	}
	return nil
}

// SetEX set key to hold the string value and set key to timeout after the given duration.
func (db *LazyDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	expiredAt := time.Now().Add(duration).Unix()
	entry := &logfile.LogEntry{Key: key, Value: value, ExpiredAt: expiredAt}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}
	return db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *LazyDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if val != nil {
		return nil
	}
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}
	return db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
}

// MSet is multiple set command. Parameter order should be like "key", "value", "key", "value", ...
func (db *LazyDB) MSet(args ...[]byte) error {
	if len(args) == 0 || len(args)%2 == 1 {
		return ErrInvalidParam
	}
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key, val := args[i], args[i+1]
		entry := &logfile.LogEntry{Key: key, Value: val}
		valuePos, err := db.writeLogEntry(valueTypeString, entry)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// MSetNX sets given keys to their respective values. MSetNX will not perform
// any operation at all even if just a single key already exists.
func (db *LazyDB) MSetNX(args ...[]byte) error {
	if len(args) == 0 || len(args)%2 != 0 {
		return ErrInvalidParam
	}
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if val != nil {
			return nil
		}
	}
	var newKeys = make(map[uint64]struct{})
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		h := util.MemHash(key)
		if _, ok := newKeys[h]; ok {
			continue
		}
		entry := &logfile.LogEntry{Key: key, Value: value}
		valPos, err := db.writeLogEntry(valueTypeString, entry)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valPos, true)
		if err != nil {
			return err
		}
		newKeys[h] = struct{}{}
	}
	return nil
}

// Append appends the value at the end of the old value if key already exists.
// It will be similar to Set if key does not exist.
func (db *LazyDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if val != nil {
		value = append(val, value...)
	}
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return err
	}
	return db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
}

// Decr decrements the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *LazyDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -1)
}

// DecrBy decrements the number stored at key by decr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *LazyDB) DecrBy(key []byte, decr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -decr)
}

// Incr increments the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *LazyDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, 1)
}

// IncrBy increments the number stored at key by incr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *LazyDB) IncrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)
}

// incrDecrBy is a helper method for Incr, IncrBy, Decr, and DecrBy methods. It updates the key by incr.
func (db *LazyDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongValueType
	}
	if incr > 0 && valInt64 > 0 && incr > math.MaxInt64-valInt64 ||
		incr < 0 && valInt64 < 0 && incr < math.MinInt64-valInt64 {
		return 0, ErrIntegerOverFlow
	}
	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	entry := &logfile.LogEntry{Key: key, Value: val}
	valuePos, err := db.writeLogEntry(valueTypeString, entry)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(valueTypeString, db.strIndex.idxTree, entry, valuePos, true)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// StrLen returns the length of the string value stored at key. If the key
// doesn't exist, it returns 0.
func (db *LazyDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil {
		return 0
	}
	return len(val)
}

// Count returns the total number of keys of String.
func (db *LazyDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.strIndex.idxTree.Size()
}

// Scan iterates over all keys of type String and finds its value.
// Parameter prefix will match key`s prefix, and pattern is a regular expression that also matchs the key.
// Parameter count limits the number of keys, a nil slice will be returned if count is not a positive number.
// The returned values will be a mixed data of keys and values, like [key1, value1, key2, value2, etc...].
func (db *LazyDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return nil, nil
	}
	keys := db.strIndex.idxTree.PrefixScan(prefix, count)
	if len(keys) == 0 {
		return nil, nil
	}
	var values [][]byte
	for _, key := range keys {
		if reg != nil && !reg.Match(key) {
			continue
		}
		val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return nil, err
		}
		if !errors.Is(err, ErrKeyNotFound) {
			values = append(values, key, val)
		}
	}
	return values, nil
}

// Expire set the expiration time for the given key.
func (db *LazyDB) Expire(key []byte, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	db.strIndex.mu.RLock()
	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil {
		db.strIndex.mu.RUnlock()
		return err
	}
	db.strIndex.mu.RUnlock()
	return db.SetEX(key, val, duration)
}

// TTL get ttl(time to live) for the given key.
func (db *LazyDB) TTL(key []byte) (int64, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	rawValue := db.strIndex.idxTree.Get(key)
	if rawValue == nil {
		return 0, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*Value)
	if idxNode == nil {
		return 0, nil
	}
	var ttl int64
	if idxNode.expiredAt != 0 {
		ttl = idxNode.expiredAt - time.Now().Unix()
	}
	return ttl, nil
}

// Persist remove the expiration time for the given key.
func (db *LazyDB) Persist(key []byte) error {
	db.strIndex.mu.RLock()
	val, err := db.getValue(db.strIndex.idxTree, key, valueTypeString)
	if err != nil {
		db.strIndex.mu.RUnlock()
		return err
	}
	db.strIndex.mu.RUnlock()
	return db.Set(key, val)
}

// GetStrsKeys get all stored keys of type String.
func (db *LazyDB) GetStrsKeys() ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	var keys [][]byte
	iter := db.strIndex.idxTree.Iterator()
	ts := time.Now().Unix()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		indexNode, _ := node.Value().(*Value)
		if indexNode == nil {
			continue
		}
		if indexNode.expiredAt != 0 && indexNode.expiredAt <= ts {
			continue
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}
