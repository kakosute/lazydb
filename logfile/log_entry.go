package logfile

// Status of LogEntry.
type Status uint8

const (
	// SDelete represents entry type is delete.
	SDelete Status = iota
	// SListMeta represents entry is list meta.
	SListMeta
)

// MaxHeaderSize max entry header size.
// Notice that the length of crc32 and typ are fixed and the rest are variant.
// crc32	typ    kSize	vSize	expiredAt
//
//	4    +   1   +   5   +   5    +    10      = 25 (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)
const MaxHeaderSize = 25

// LogEntry is the data will be appended in log file.
type LogEntry struct {
	crc      uint32 // crc32 --check sum
	ExpireAt int64  // expire time
	Stat     Status // delete or list meta
	kSize    uint32 // key size
	vSize    uint32 // value size
	Key      []byte // key
	Value    []byte // value
}

// EncodeEntry encodes LogEntry into binary form, returns binary LogEntry and the size of LogEntry.
func EncodeEntry(le *LogEntry) ([]byte, int) {
	var size = 1
	buf := make([]byte, size)
	return buf, size
}

// only used in log_file.go when reading LogEntry from files
// read in the header buffer read from the file
// return a new logEntry (with the header part updated and the rest empty) and the total size of the header
func decodeHeader(buf []byte) (*LogEntry, int) {
	return &LogEntry{}, 0
}

func getEntryCrc(le *LogEntry, buf []byte) uint32 {
	return 0
}
