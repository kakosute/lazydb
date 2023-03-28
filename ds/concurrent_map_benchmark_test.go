package ds

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// go test -bench='Map$' -benchtime=5s -count=1 -benchmem

// write

var mapKeyCount = 10000000

func benchmarkWriteMap(b *testing.B) {
	mp := make(map[string]any)
	lock := sync.RWMutex{}
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(key int) {
			lock.Lock()
			mp[strconv.Itoa(key)] = "value"
			lock.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func benchmarkWriteSyncMap(b *testing.B) {
	sm := sync.Map{}
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(key int) {
			sm.Store(strconv.Itoa(key), "value")
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func benchmarkWriteShardConcurrentMap(b *testing.B, shardCount int) {
	cm := NewConcurrentMap(shardCount)
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(key int) {
			strKey := strconv.Itoa(key)
			shard := cm.GetShardByWriting(strKey)
			shard.Set(strKey, "value")
			shard.Unlock()
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func BenchmarkWriteMap(b *testing.B) {
	benchmarkWriteMap(b)
}

func BenchmarkWriteSyncMap(b *testing.B) {
	benchmarkWriteSyncMap(b)
}

func BenchmarkWrite16ShardConcurrentMap(b *testing.B) {
	benchmarkWriteShardConcurrentMap(b, 16)
}

func BenchmarkWrite32ShardConcurrentMap(b *testing.B) {
	benchmarkWriteShardConcurrentMap(b, 32)
}

func BenchmarkWrite64ShardConcurrentMap(b *testing.B) {
	benchmarkWriteShardConcurrentMap(b, 64)
}

func BenchmarkWrite128ShardConcurrentMap(b *testing.B) {
	benchmarkWriteShardConcurrentMap(b, 128)
}

// read
func benchmarkReadMap(b *testing.B) {
	mp := make(map[string]any)
	for i := 0; i < mapKeyCount; i++ {
		mp[strconv.Itoa(i)] = "value"
	}
	rand.Seed(time.Now().UnixNano())
	lock := sync.RWMutex{}
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			lock.RLock()
			_, _ = mp[strconv.Itoa(rand.Intn(mapKeyCount))]
			lock.RUnlock()
			wg.Done()
		}()
	}
	wg.Wait()
}

func benchmarkReadSyncMap(b *testing.B) {
	sm := sync.Map{}
	wg := sync.WaitGroup{}
	for i := 0; i < mapKeyCount; i++ {
		sm.Store(strconv.Itoa(i), "value")
	}
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			_, _ = sm.Load(strconv.Itoa(rand.Intn(mapKeyCount)))
			wg.Done()
		}()
	}
	wg.Wait()
}

func benchmarkReadShardConcurrentMap(b *testing.B, shardCount int) {
	cm := NewConcurrentMap(shardCount)
	for i := 0; i < mapKeyCount; i++ {
		cm.Set(strconv.Itoa(i), "value")
	}
	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			strKey := strconv.Itoa(rand.Intn(mapKeyCount))
			shard := cm.GetShardByReading(strKey)
			shard.Get(strKey)
			shard.RUnlock()
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkReadMap(b *testing.B) {
	benchmarkReadMap(b)
}

func BenchmarkReadSyncMap(b *testing.B) {
	benchmarkReadSyncMap(b)
}

func BenchmarkRead16ShardConcurrentMap(b *testing.B) {
	benchmarkReadShardConcurrentMap(b, 16)
}

func BenchmarkRead32ShardConcurrentMap(b *testing.B) {
	benchmarkReadShardConcurrentMap(b, 32)
}

func BenchmarkRead64ShardConcurrentMap(b *testing.B) {
	benchmarkReadShardConcurrentMap(b, 64)
}

func BenchmarkRead128ShardConcurrentMap(b *testing.B) {
	benchmarkReadShardConcurrentMap(b, 128)
}

// read or write
func benchmarkRWMap(b *testing.B) {
	mp := make(map[string]any)
	for i := 0; i < mapKeyCount; i++ {
		mp[strconv.Itoa(i)] = "value"
	}
	rand.Seed(time.Now().UnixNano())
	lock := sync.RWMutex{}
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			switch rand.Intn(2) {
			case 0:
				lock.RLock()
				_, _ = mp[strconv.Itoa(rand.Intn(mapKeyCount))]
				lock.RUnlock()
			case 1:
				lock.Lock()
				mp[strconv.Itoa(rand.Intn(mapKeyCount))] = "value1"
				lock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func benchmarkRWSyncMap(b *testing.B) {
	sm := sync.Map{}
	wg := sync.WaitGroup{}
	for i := 0; i < mapKeyCount; i++ {
		sm.Store(strconv.Itoa(i), "value")
	}
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			switch rand.Intn(2) {
			case 0:
				_, _ = sm.Load(strconv.Itoa(rand.Intn(mapKeyCount)))
			case 1:
				sm.Store(strconv.Itoa(rand.Intn(mapKeyCount)), "value1")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func benchmarkRWShardConcurrentMap(b *testing.B, shardCount int) {
	cm := NewConcurrentMap(shardCount)
	for i := 0; i < mapKeyCount; i++ {
		cm.Set(strconv.Itoa(i), "value")
	}
	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			switch rand.Intn(2) {
			case 0:
				strKey := strconv.Itoa(rand.Intn(mapKeyCount))
				shard := cm.GetShardByReading(strKey)
				shard.Get(strKey)
				shard.RUnlock()
			case 1:
				strKey := strconv.Itoa(rand.Intn(mapKeyCount))
				shard := cm.GetShardByWriting(strKey)
				shard.Set(strKey, "value1")
				shard.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkRWMap(b *testing.B) {
	benchmarkRWMap(b)
}

func BenchmarkRWSyncMap(b *testing.B) {
	benchmarkRWSyncMap(b)
}

func BenchmarkRW16ShardConcurrentMap(b *testing.B) {
	benchmarkRWShardConcurrentMap(b, 16)
}

func BenchmarkRW32ShardConcurrentMap(b *testing.B) {
	benchmarkRWShardConcurrentMap(b, 32)
}

func BenchmarkRW64ShardConcurrentMap(b *testing.B) {
	benchmarkRWShardConcurrentMap(b, 64)
}

func BenchmarkRW128ShardConcurrentMap(b *testing.B) {
	benchmarkRWShardConcurrentMap(b, 128)
}
