package lazydb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"lazydb/iocontroller"
	"lazydb/logfile"
	"log"
	"path/filepath"
	"sort"
	"sync"
)

const (
	discardRecordSize       = 12
	discardFileSize   int64 = 2 << 12
	discardFileName         = "discard"
)

var (
	ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")
)

// format of discard file` record:
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
type discard struct {
	sync.Mutex
	once     *sync.Once
	file     iocontroller.IOController
	valChan  chan *Value
	freeList []int64          // contains file offset that can be allocated
	location map[uint32]int64 // offset of each fid
}

// initDiscard returns a new
func newDiscard(path, name string, buffersize int) (*discard, error) {
	fname := filepath.Join(path, name)
	file, err := iocontroller.NewFileIOController(fname, discardFileSize)
	if err != nil {
		return nil, err
	}

	freeList := make([]int64, 0)
	location := map[uint32]int64{}
	var offset int64
	for {
		buf := make([]byte, discardRecordSize)
		if _, err := file.Read(buf, offset); err != nil {
			if err == io.EOF || err == logfile.ErrLogEndOfFile {
				break
			}
			return nil, err
		}
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		if fid == 0 && total == 0 {
			freeList = append(freeList, offset)
		} else {
			location[fid] = offset
		}
		offset += discardRecordSize
	}

	d := &discard{
		once:     new(sync.Once),
		file:     file,
		valChan:  make(chan *Value, buffersize),
		freeList: freeList,
		location: location,
	}
	go d.listenUpdate()
	return d, nil
}

// listenUpdate listens to valChan, and close discard file when channel is closed
func (d *discard) listenUpdate() {
	for {
		select {
		case val, ok := <-d.valChan:
			if !ok {
				if err := d.file.Close(); err != nil {
					log.Fatalf("close discard file err: %v", err)
				}
				return
			}
			d.incrDiscard(val.fid, val.entrySize)
		}
	}
}

func (d *discard) closeChan() {
	d.once.Do(func() {
		close(d.valChan)
	})
}

// remove a discard entry when a logfile is deleted
func (d *discard) removeDiscard(fid uint32) {
	if fid == 0 {
		return
	}

	d.Lock()
	defer d.Unlock()

	offset, ok := d.location[fid]
	if !ok {
		return
	}

	buf := make([]byte, discardRecordSize)
	if _, err := d.file.Write(buf, offset); err != nil {
		log.Fatalf("remove discard err:%v", err)
		return
	}
	delete(d.location, fid)
	d.freeList = append(d.freeList, offset)
}

func (d *discard) incrDiscard(fid uint32, delta int) {
	if delta <= 0 {
		return
	}
	d.Lock()
	defer d.Unlock()
	offset, err := d.alloc(fid)
	if err != nil {
		log.Fatalf("discard file allocate err: %+v", err)
		return
	}
	buf := make([]byte, 4)
	offset += 8
	if _, err := d.file.Read(buf, offset); err != nil {
		log.Fatalf("incr value in discard err: %+v", err)
		return
	}

	v := binary.LittleEndian.Uint32(buf[:4])
	binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	if _, err = d.file.Write(buf, offset); err != nil {
		log.Fatalf("incr value in discard err: %+v", err)
		return
	}
}

func (d *discard) alloc(fid uint32) (int64, error) {
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	if len(d.freeList) == 0 {
		return 0, ErrDiscardNoSpace
	}
	offset := d.freeList[len(d.freeList)-1]
	d.freeList = d.freeList[:len(d.freeList)-1]
	d.location[fid] = offset
	return offset, nil
}

func (d *discard) setTotal(fid, total uint32) {
	d.Lock()
	defer d.Unlock()
	if fid == 0 || total == 0 {
		return
	}

	// totalSize already been set
	if _, ok := d.location[fid]; ok {
		return
	}
	offset, err := d.alloc(fid)
	if err != nil {
		log.Fatalf("discard file allocate err: %+v", err)
		return
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], total)
	if _, err = d.file.Write(buf, offset); err != nil {
		log.Fatalf("set total in discard err: %+v", err)
		return
	}
}

// CCL means compaction cnadidate list.
// iterate and find the file with most discarded data,
// there are 682 records at most, no need to worry about the performance.
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	var offset int64
	ccl := make([]uint32, 0)
	d.Lock()
	defer d.Unlock()
	for {
		if offset == 8172 {
			fmt.Print("")
		}
		buf := make([]byte, discardRecordSize)
		if _, err := d.file.Read(buf, offset); err != nil {
			if err == logfile.ErrLogEndOfFile || err == io.EOF {
				break
			}
			return nil, err
		}
		offset += discardRecordSize

		fid := binary.LittleEndian.Uint32(buf[:4])
		totalSize := binary.LittleEndian.Uint32(buf[4:8])
		v := binary.LittleEndian.Uint32(buf[8:12])
		if fid == activeFid {
			continue
		}
		var curRatio float64
		if totalSize != 0 && fid != 0 {
			curRatio = float64(v) / float64(totalSize)
		}
		if curRatio > ratio {
			ccl = append(ccl, fid)
		}
	}

	// older log file will be merge firstly
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

func (d *discard) sync() error {
	return d.file.Sync()
}

func (d *discard) close() error {
	return d.file.Close()
}

func (d *discard) clear(fid uint32) {
	d.Lock()
	defer d.Unlock()

	// re-initialize
	offset, err := d.alloc(fid)
	if err != nil {
		log.Fatalf("discard file allocate err: %+v", err)
		return
	}
	buf := make([]byte, discardRecordSize)
	if _, err := d.file.Write(buf, offset); err != nil {
		log.Fatalf("incr value in discard err:%v", err)
		return
	}

	// release free space of discard file
	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		delete(d.location, fid)
	}
}
