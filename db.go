package lazydb

import (
	"sync"
)

type LazyDB struct {
	cfg   *DBConfig
	index *ConcurrentMap[string]
	mu    sync.RWMutex
}

func Open() (*LazyDB, error) {
	return nil, nil
}

func (db *LazyDB) Sync() error {
	return nil
}

func (db *LazyDB) Close() error {
	return nil
}
