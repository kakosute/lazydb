package mmap

import (
	"golang.org/x/sys/unix"
	"os"
	"reflect"
	"unsafe"
)

const MREMAP_MAYMOVE = 0x1

// MMap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func mMap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mType := unix.PROT_READ
	if writable {
		mType |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mType, unix.MAP_SHARED)
}

// mUnmap unmaps a mapped slice
// unix.MUnmap maintains an internal list of mmapped addresses, and only calls munmap
// if the address is present in that list. If we use mremap, this list is not updated.
// To bypass this, we call munmap ourselves.
func mUnmap(b []byte) error {
	if len(b) == 0 || len(b) != cap(b) {
		return unix.EINVAL
	}
	_, _, err := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)),
		0,
	)
	if err != 0 {
		return err
	}
	return nil
}

// mRemap is a specific function in linux. It can remap pages in memory, which is a hybrid of mUnmap and mMap.
func mRemap(b []byte, size int) ([]byte, error) {
	header := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	addr, _, err := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,
		uintptr(header.Len),
		uintptr(size),
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if err != 0 {
		return nil, err
	}
	header.Data = addr
	header.Len = size
	header.Cap = size
	return b, nil
}

// mAdvise provide advice on memory usage.
// If the page references are expected to be in random order, set the randomRead flag to true.
func mAdvise(b []byte, randomRead bool) error {
	advice := unix.MADV_NORMAL
	if randomRead {
		advice = unix.MADV_RANDOM
	}

	return unix.Madvise(b, advice)
}

// mSync syncs the mapped data
func mSync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
