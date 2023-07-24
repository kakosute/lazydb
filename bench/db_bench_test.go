package bench

import (
	"bytes"
	"fmt"
	"lazydb"
	"math/rand"
	"path/filepath"
	"testing"
	"time"
)

var db *lazydb.LazyDB

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func GetKey(n int) []byte {
	return []byte("bench_test_key_" + fmt.Sprintf("%d", n))
}

func GetValue() []byte {
	var str bytes.Buffer
	for i := 0; i < 512; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}

func initDB() {
	for i := 0; i < 500000; i++ {
		err := db.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkGetValue(b *testing.B) {
	initDB()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(GetKey(i))
		if err != nil && err != lazydb.ErrKeyNotFound {
			panic(err)
		}
	}
}

func BenchmarkSetValue(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Set(GetKey(i), GetValue())
		if err != nil {
			panic(err)
		}
	}
}

func init() {
	rand.Seed(time.Now().Unix())
	opts := lazydb.DefaultDBConfig(filepath.Join("bench_records"))
	opts.IOType = 1
	var err error
	db, err = lazydb.Open(opts)
	if err != nil {
		panic(err)
	}
}
