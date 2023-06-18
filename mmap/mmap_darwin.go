package mmap

import (
	"golang.org/x/sys/unix"
	"os"
	"syscall"
	"unsafe"
)

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
func mUnmap(b []byte) error {
	return unix.Munmap(b)
}

// mAdvise provide advice on memory usage.
// If the page references are expected to be in random order, set the randomRead flag to true.
func mAdvise(b []byte, randomRead bool) error {
	advice := unix.MADV_NORMAL
	if randomRead {
		advice = unix.MADV_RANDOM
	}
	_, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
		uintptr(len(b)), uintptr(advice))
	if err != 0 {
		return err
	}
	return nil
}

// mSync syncs the mapped data
func mSync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
