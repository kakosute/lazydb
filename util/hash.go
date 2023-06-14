package util

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/spaolacci/murmur3"
)

//go:linkname runtimeMemhash runtime.memhash
//go:noescape
func runtimeMemhash(p unsafe.Pointer, seed, s uintptr) uintptr

// MemHash is the hash function used by go map, it utilizes available hardware instructions(behaves
// as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHash(buf []byte) uint64 {
	return rthash(buf, 923)
}

func rthash(b []byte, seed uint64) uint64 {
	if len(b) == 0 {
		return seed
	}
	// The runtime hasher only works on uintptr. For 64-bit
	// architectures, we use the hasher directly. Otherwise,
	// we use two parallel hashers on the lower and upper 32 bits.
	if unsafe.Sizeof(uintptr(0)) == 8 {
		return uint64(runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b))))
	}
	lo := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b)))
	hi := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed>>32), uintptr(len(b)))
	return uint64(hi)<<32 | uint64(lo)
}

type Murmur128 struct {
	mur murmur3.Hash128
}

func NewMurmur128() *Murmur128 {
	return &Murmur128{mur: murmur3.New128()}
}

func (m *Murmur128) Write(p []byte) error {
	n, err := m.mur.Write(p)
	if n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

func (m *Murmur128) EncodeSum128() []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	s1, s2 := m.mur.Sum128()
	var index int
	index += binary.PutUvarint(buf[index:], s1)
	index += binary.PutUvarint(buf[index:], s2)
	return buf[:index]
}

func (m *Murmur128) Reset() {
	m.mur.Reset()
}
