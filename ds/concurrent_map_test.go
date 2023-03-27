package ds

import (
	"reflect"
	"testing"
)

func TestMapShard_Get(t *testing.T) {
	type args[K comparable] struct {
		key K
	}
	type testCase[K comparable] struct {
		name      string
		ms        *MapShard[K]
		args      args[K]
		valueWant any
		flagWant  bool
	}
	tests := []testCase[string]{
		{
			name: "test1",
			ms: &MapShard[string]{
				simpleMap: map[string]any{
					"LazyDB": "test1",
				},
			},
			args: args[string]{
				key: "LazyDB",
			},
			valueWant: "test1",
			flagWant:  true,
		},
		{
			name: "test2",
			ms: &MapShard[string]{
				simpleMap: make(map[string]any),
			},
			args: args[string]{
				key: "LazyDB",
			},
			valueWant: nil,
			flagWant:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueGot, flagGot := tt.ms.Get(tt.args.key)
			if !reflect.DeepEqual(valueGot, tt.valueWant) {
				t.Errorf("Get() valueGot = %v, valueWant %v", valueGot, tt.valueWant)
			}
			if flagGot != tt.flagWant {
				t.Errorf("Get() flagGot = %v, flagWant %v", flagGot, tt.flagWant)
			}
		})
	}
}

func TestMapShard_Set(t *testing.T) {
	type args[K comparable] struct {
		key   K
		value any
	}
	type testCase[K comparable] struct {
		name string
		ms   *MapShard[K]
		args args[K]
	}
	tests := []testCase[string]{
		{
			name: "test1",
			ms: &MapShard[string]{
				simpleMap: make(map[string]any),
			},
			args: args[string]{
				key:   "LazyDB",
				value: "test1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.ms.Set(tt.args.key, tt.args.value)
			valueGot, flagGot := tt.ms.Get(tt.args.key)
			if !reflect.DeepEqual(valueGot, tt.args.value) {
				t.Errorf("Get() valueGot = %v, valueWant %v", valueGot, tt.args.value)
			}
			if flagGot != true {
				t.Errorf("Get() flagGot = %v, flagWant %v", flagGot, true)
			}
		})
	}
}

func TestMapShard_Has(t *testing.T) {
	type args[K comparable] struct {
		key K
	}
	type testCase[K comparable] struct {
		name string
		ms   *MapShard[K]
		args args[K]
		want bool
	}
	tests := []testCase[string]{
		{
			name: "test1",
			ms: &MapShard[string]{
				simpleMap: map[string]any{
					"LazyDB": "test1",
				},
			},
			args: args[string]{
				key: "LazyDB",
			},
			want: true,
		},
		{
			name: "test2",
			ms: &MapShard[string]{
				simpleMap: make(map[string]any),
			},
			args: args[string]{
				key: "LazyDB",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ms.Has(tt.args.key); got != tt.want {
				t.Errorf("Has() = %v, valueWant %v", got, tt.want)
			}
		})
	}
}

func TestMapShard_Remove(t *testing.T) {
	type args[K comparable] struct {
		key K
	}
	type testCase[K comparable] struct {
		name string
		ms   *MapShard[K]
		args args[K]
	}
	tests := []testCase[string]{
		{
			name: "test1",
			ms: &MapShard[string]{
				simpleMap: map[string]any{
					"LazyDB": "test1",
				},
			},
			args: args[string]{
				key: "LazyDB",
			},
		},
		{
			name: "test2",
			ms: &MapShard[string]{
				simpleMap: make(map[string]any),
			},
			args: args[string]{
				key: "LazyDB",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.ms.Remove(tt.args.key)
			valueGot, flagGot := tt.ms.Get(tt.args.key)
			if !reflect.DeepEqual(valueGot, nil) {
				t.Errorf("Get() valueGot = %v, valueWant %v", valueGot, nil)
			}
			if flagGot != false {
				t.Errorf("Get() flagGot = %v, flagWant %v", flagGot, false)
			}
		})
	}
}

func TestMapShard_Pop(t *testing.T) {
	type args[K comparable] struct {
		key K
	}
	type testCase[K comparable] struct {
		name      string
		ms        *MapShard[K]
		args      args[K]
		valueWant any
		flagWant  bool
	}
	tests := []testCase[string]{
		{
			name: "test1",
			ms: &MapShard[string]{
				simpleMap: map[string]any{
					"LazyDB": "test1",
				},
			},
			args: args[string]{
				key: "LazyDB",
			},
			valueWant: "test1",
			flagWant:  true,
		},
		{
			name: "test2",
			ms: &MapShard[string]{
				simpleMap: make(map[string]any),
			},
			args: args[string]{
				key: "LazyDB",
			},
			valueWant: nil,
			flagWant:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valueGot, flagGot := tt.ms.Pop(tt.args.key)
			if !reflect.DeepEqual(valueGot, tt.valueWant) {
				t.Errorf("Pop() valueGot = %v, valueWant %v", valueGot, tt.valueWant.(string))
			}
			if flagGot != tt.flagWant {
				t.Errorf("Pop() flagGot = %v, flagWant %v", flagGot, tt.flagWant)
			}
			valueGot, flagGot = tt.ms.Get(tt.args.key)
			if !reflect.DeepEqual(valueGot, nil) {
				t.Errorf("Get() valueGot = %v, valueWant %v", valueGot, nil)
			}
			if flagGot != false {
				t.Errorf("Get() flagGot = %v, flagWant %v", flagGot, false)
			}
		})
	}
}

func TestNewConcurrentMap(t *testing.T) {
	type args struct {
		mapShardCount int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test1",
			args: args{
				mapShardCount: -4,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewConcurrentMap(tt.args.mapShardCount)
			if len(got.shards) != DefaultShardCount {
				t.Errorf("ShardCount Got = %v, Want %v", got.shardCount, DefaultShardCount)
			}
		})
	}
}
