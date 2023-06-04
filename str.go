package lazydb

import "time"

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *LazyDB) Set(key, value []byte) error {
	return nil
}

// Get get the value of key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *LazyDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

// MGet get the values of all specified keys.
// If the key that does not hold a string value or does not exist, nil is returned.
func (db *LazyDB) MGet(keys [][]byte) ([][]byte, error) {
	return nil, nil
}

// GetRange returns the substring of the string value stored at key,
// determined by the offsets start and end.
func (db *LazyDB) GetRange(key []byte, start, end int) ([]byte, error) {
	return nil, nil
}

// GetDel gets the value of the key and deletes the key. This method is similar
// to Get method. It also deletes the key if it exists.
func (db *LazyDB) GetDel(key []byte) ([]byte, error) {
	return nil, nil
}

// Delete value at the given key.
func (db *LazyDB) Delete(key []byte) error {
	return nil
}

// SetEX set key to hold the string value and set key to timeout after the given duration.
func (db *LazyDB) SetEX(key, value []byte, duration time.Duration) error {
	return nil
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *LazyDB) SetNX(key, value []byte) error {
	return nil
}

// MSet is multiple set command. Parameter order should be like "key", "value", "key", "value", ...
func (db *LazyDB) MSet(args ...[]byte) error {
	return nil
}

// MSetNX sets given keys to their respective values. MSetNX will not perform
// any operation at all even if just a single key already exists.
func (db *LazyDB) MSetNX(args ...[]byte) error {
	return nil
}

// Append appends the value at the end of the old value if key already exists.
// It will be similar to Set if key does not exist.
func (db *LazyDB) Append(key, value []byte) error {
	return nil
}

// Decr decrements the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *LazyDB) Decr(key []byte) (int64, error) {
	return 0, nil
}

// DecrBy decrements the number stored at key by decr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *LazyDB) DecrBy(key []byte, decr int64) (int64, error) {
	return 0, nil
}

// Incr increments the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *LazyDB) Incr(key []byte) (int64, error) {
	return 0, nil
}

// IncrBy increments the number stored at key by incr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *LazyDB) IncrBy(key []byte, incr int64) (int64, error) {
	return 0, nil
}

// incrDecrBy is a helper method for Incr, IncrBy, Decr, and DecrBy methods. It updates the key by incr.
func (db *LazyDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	return 0, nil
}

// StrLen returns the length of the string value stored at key. If the key
// doesn't exist, it returns 0.
func (db *LazyDB) StrLen(key []byte) int {
	return 0
}

// Count returns the total number of keys of String.
func (db *LazyDB) Count() int {
	return 0
}

// Scan iterates over all keys of type String and finds its value.
// Parameter prefix will match key`s prefix, and pattern is a regular expression that also matchs the key.
// Parameter count limits the number of keys, a nil slice will be returned if count is not a positive number.
// The returned values will be a mixed data of keys and values, like [key1, value1, key2, value2, etc...].
func (db *LazyDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	return nil, nil
}

// Expire set the expiration time for the given key.
func (db *LazyDB) Expire(key []byte, duration time.Duration) error {
	return nil
}

// TTL get ttl(time to live) for the given key.
func (db *LazyDB) TTL(key []byte) (int64, error) {
	return 0, nil
}

// Persist remove the expiration time for the given key.
func (db *LazyDB) Persist(key []byte) error {
	return nil
}

// GetStrsKeys get all stored keys of type String.
func (db *LazyDB) GetStrsKeys() ([][]byte, error) {
	return nil, nil
}
