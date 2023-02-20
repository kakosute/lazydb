package util

import (
	"testing"
)

// go test -bench='Conv$' -count=3 -benchmem

var str = "adsfasdfadsfadsfasdfadfadfasdfasdfadsfasdfasdfasdfsadfas"

func BenchmarkB2sStdConv(b *testing.B) {
	var _ []byte
	for i := 0; i < b.N; i++ {
		_ = []byte(str)
	}
}

func BenchmarkB2sFastConv(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = StringToByte(str)
	}
}

var bt = []byte("adsfasdfadsfadsfasdfadfadfasdfasdfadsfasdfasdfasdfsadfas")

func BenchmarkS2BStdConv(b *testing.B) {
	var _ []byte
	for i := 0; i < b.N; i++ {
		_ = string(bt)
	}
}

func BenchmarkS2BFastConv(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ByteToString(bt)
	}
}
