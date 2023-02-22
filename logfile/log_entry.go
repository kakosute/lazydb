package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

// Status of LogEntry.
type Status uint8

const (
	// SDelete represents entry has been deleted.
	SDelete Status = iota + 1
	// SListMeta represents entry is list meta.
	SListMeta
)

// MaxHeaderSize max entry header size.
// 4    +    1    +    10    +    5    +    5   =   25
// crc     status   ExpireAt    kSize     vSize
// (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)
const MaxHeaderSize = 25

// LogEntry is the data will be appended in log file.
type LogEntry struct {
	crc       uint32 // crc32 --check sum
	status    Status // delete or list meta
	ExpiredAt int64  // expire time
	kSize     uint32 // key size
	vSize     uint32 // value size
	Key       []byte // key
	Value     []byte // value
}

// EncodeEntry encodes LogEntry into binary form, returns binary LogEntry and the size of LogEntry.
func EncodeEntry(le *LogEntry) ([]byte, int) {
	if le == nil {
		return nil, 0
	}
	var size = MaxHeaderSize
	buf := make([]byte, size)
	buf[4] = byte(le.status)

	offset := 5
	expireAtByte := binary.PutVarint(buf[offset:], le.ExpiredAt)
	offset += expireAtByte
	kSizeByte := binary.PutVarint(buf[offset:], int64(len(le.Key)))
	offset += kSizeByte
	vSizeByte := binary.PutVarint(buf[offset:], int64(len(le.Value)))
	offset += vSizeByte

	size = offset + len(le.Key) + len(le.Value)
	newBuf := make([]byte, size)

	copy(newBuf[:offset], buf[:offset])
	copy(newBuf[offset:], le.Key)
	copy(newBuf[offset+len(le.Key):], le.Value)

	crc := crc32.ChecksumIEEE(newBuf[4:])
	binary.LittleEndian.PutUint32(newBuf[:4], crc)
	return newBuf, size
}

// decodeHeader decodes header from a bytes array to LogEntry struct, returns LogEntry and offset.
func decodeHeader(buf []byte) (*LogEntry, int) {
	if len(buf) <= 4 {
		return nil, 0
	}
	le := &LogEntry{}
	le.crc = binary.LittleEndian.Uint32(buf[0:4])
	le.status = Status(buf[4])

	offset := 5
	expireAt, size := binary.Varint(buf[offset:])
	le.ExpiredAt = expireAt
	offset += size
	kSize, size := binary.Varint(buf[offset:])
	le.kSize = uint32(kSize)
	offset += size
	vSize, size := binary.Varint(buf[offset:])
	le.vSize = uint32(vSize)
	offset += size

	return le, offset
}

// getEntryCrc get the crc32 from the header without crc part, as well as the key and the value .
func getEntryCrc(buf []byte, le *LogEntry) uint32 {
	if len(buf) <= 4 {
		return 0
	}
	if le == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, le.Key)
	crc = crc32.Update(crc, crc32.IEEETable, le.Value)
	return crc
}
