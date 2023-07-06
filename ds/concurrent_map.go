package ds

import (
	"hash/fnv"
	"sync"
)

const (
	DefaultShardCount = 32
)

type MapShard[K comparable] struct {
	simpleMap    map[K]any
	sync.RWMutex // r&w lock for every shard
}

// Get gets the value under a given key.
func (ms *MapShard[K]) Get(key K) (any, bool) {
	val, ok := ms.simpleMap[key]
	return val, ok
}

// Set sets the key and value under a specific MapShard.
func (ms *MapShard[K]) Set(key K, value any) {
	ms.simpleMap[key] = value
}

// Has returns if the map contains a specific key.
func (ms *MapShard[K]) Has(key K) bool {
	_, ok := ms.simpleMap[key]
	return ok
}

// Remove deletes an element from the map.
func (ms *MapShard[K]) Remove(key K) {
	delete(ms.simpleMap, key)
}

// Pop deletes an element from the map and returns it.
func (ms *MapShard[K]) Pop(key K) (any, bool) {
	val, exist := ms.simpleMap[key]
	delete(ms.simpleMap, key)
	return val, exist
}

type ConcurrentMap[K comparable] struct {
	shards     []*MapShard[K]
	sharding   func(key K) uint32
	shardCount uint8
}

// NewConcurrentMap returns a ConcurrentMap[string] with string keys by default.
func NewConcurrentMap(mapShardCount int) *ConcurrentMap[string] {
	// fnv32 function only supports string keys
	cm := newConcurrentMap[string](mapShardCount, fnv32)
	return &cm
}

// NewWithCustomShardingFunction creates a new concurrent map.
func NewWithCustomShardingFunction[K comparable](mapShardCount int, sharding func(key K) uint32) *ConcurrentMap[K] {
	cm := newConcurrentMap[K](mapShardCount, sharding)
	return &cm
}

func newConcurrentMap[K comparable](mapShardCount int, sharding func(key K) uint32) ConcurrentMap[K] {
	// suggest powers of 2
	if mapShardCount < DefaultShardCount {
		mapShardCount = DefaultShardCount
	}

	cm := ConcurrentMap[K]{
		sharding:   sharding,
		shards:     make([]*MapShard[K], mapShardCount),
		shardCount: uint8(mapShardCount),
	}

	for i := 0; i < mapShardCount; i++ {
		cm.shards[i] = &MapShard[K]{simpleMap: make(map[K]any)}
	}

	return cm
}

func fnv32(key string) uint32 {
	h := fnv.New32()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func SimpleSharding(key uint32) uint32 {
	return key
}

// GetShard returns the MapShard under the given key.
func (cm *ConcurrentMap[K]) GetShard(key K) *MapShard[K] {
	return cm.shards[uint(cm.sharding(key))%uint(cm.shardCount)]
}

// GetShardByReading returns the MapShard under the given key after RLocking.
// Remember to unlock the shard!
func (cm *ConcurrentMap[K]) GetShardByReading(key K) *MapShard[K] {
	shard := cm.GetShard(key)
	shard.RLock()
	// remember to RUnlock
	return shard
}

// GetShardByWriting returns the MapShard under the given key after Locking.
// Remember to unlock the shard!
func (cm *ConcurrentMap[K]) GetShardByWriting(key K) *MapShard[K] {
	shard := cm.GetShard(key)
	shard.Lock()
	// remember to Unlock
	return shard
}

// Get gets the value under a given key.
func (cm *ConcurrentMap[K]) Get(key K) (any, bool) {
	shard := cm.GetShard(key)
	return shard.Get(key)
}

// Set sets the key and value under a specific MapShard.
func (cm *ConcurrentMap[K]) Set(key K, value any) {
	shard := cm.GetShard(key)
	shard.Set(key, value)
}

// Has returns if the map contains a specific key.
func (cm *ConcurrentMap[K]) Has(key K) bool {
	shard := cm.GetShard(key)
	return shard.Has(key)
}

// Remove deletes an element from the map.
func (cm *ConcurrentMap[K]) Remove(key K) {
	shard := cm.GetShard(key)
	shard.Remove(key)
}

// Pop deletes an element from the map and returns it.
func (cm *ConcurrentMap[K]) Pop(key K) (any, bool) {
	shard := cm.GetShard(key)
	return shard.Pop(key)
}

// Size returns the number of keys
func (cm *ConcurrentMap[K]) Size() int {
	cnt := 0
	for _, m := range cm.shards {
		cnt += len(m.simpleMap)
	}
	return cnt
}
