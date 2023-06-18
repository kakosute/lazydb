package mmap

import "os"

// MMap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func MMap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mMap(fd, writable, size)
}

// MUnMap unmaps a mapped slice
func MUnmap(b []byte) error {
	return mUnmap(b)
}

// MAdvise provide advice on memory usage.
// If the page references are expected to be in random order, set the randomRead flag to true.
func MAdvise(b []byte, randomRead bool) error {
	return mAdvise(b, randomRead)
}

// MSync syncs the mapped data
func MSync(b []byte) error {
	return mSync(b)
}
