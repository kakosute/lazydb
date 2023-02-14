package logfile

// EntryType type of Entry.
type Status uint8

const (
	// SDelete represents entry type is delete.
	Delete Status = iota
	// SListMeta represents entry is list meta.
	ListMeta
)

// LogEntry is the data will be appended in log file.
type LogEntry struct {
	crc      uint32 // crc32 --check sum
	ExpireAt int64  // expire time
	status   Status // delete or list meta
	ksize    uint32 // key size
	vsize    uint32 // value size
	Key      []byte // key
	Value    []byte // value
}

// encode LogEntry into binary form
// return binary LogEntry and the size of LogEntry
func EncodeEntry(le *LogEntry) ([]byte, int) {
	var size = 1
	buf := make([]byte, size)
	return buf, size
}

// only used in log_file.go when reading LogEntry from files
func decodeEntry(b []byte) (le *LogEntry) {
	return &LogEntry{}
}
