package lazydb

import (
	"sync"
)

const shardCount = 32 // or 64

type MapShard[K comparable, V any] struct {
	simpleMap map[K]V
	mu        sync.RWMutex
}

type ConcurrentMap[K comparable, V any] struct {
	shards []*MapShard[K, V]
}
