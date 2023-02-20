package lazydb

// Set the key to hold the string value. If key already holds a value, it is overwritten.
func (db *LazyDB) Set(key, value []byte) error {
	return nil
}

// Get the value of the given key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *LazyDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

// Delete value by the given key.
func (db *LazyDB) Delete(key []byte) error {
	return nil
}
