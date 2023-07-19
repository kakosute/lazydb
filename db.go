package lazydb

import (
	"encoding/binary"
	"errors"
	"io"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	LazyDB struct {
		cfg              *DBConfig
		index            *ds.ConcurrentMap[string]
		strIndex         *strIndex
		hashIndex        *hashIndex
		listIndex        *listIndex
		setIndex         *setIndex
		zSetIndex        *zSetIndex
		discardsMap      map[valueType]*discard
		fidsMap          map[valueType]*MutexFids
		activeLogFileMap map[valueType]*MutexLogFile
		archivedLogFile  map[valueType]*ds.ConcurrentMap[uint32] // [uint32]*MutexLogFile
		mu               sync.RWMutex
	}

	MutexFids struct {
		fids []uint32
		mu   sync.RWMutex
	}

	MutexLogFile struct {
		lf *logfile.LogFile
		mu sync.RWMutex
	}

	valueType uint8

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *ds.AdaptiveRadixTree
	}

	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*ds.AdaptiveRadixTree
	}

	listIndex struct {
		mu    *sync.RWMutex
		trees map[string]*ds.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murHash *util.Murmur128
		trees   map[string]*ds.AdaptiveRadixTree
	}

	zSetIndex struct {
		mu      *sync.RWMutex
		indexes map[string]*ZSetIndex
	}

	Value struct {
		value     []byte
		vType     valueType
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	// 写LogFile之后返回位置信息的结构体
	ValuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}
)

const (
	valueTypeString valueType = iota
	valueTypeList
	valueTypeHash
	valueTypeSet
	valueTypeZSet

	logFileTypeNum = 5

	encodeHeaderSize = 10
	discardFilePath  = "DISCARD"

	initialListSeq = uint32(math.MaxUint32 / 2)
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrLogFileNotExist = errors.New("log file is not exist")
	ErrOpenLogFile     = errors.New("open Log file error")
	ErrWrongIndex      = errors.New("index is out of range")
)

func newStrIndex() *strIndex {
	return &strIndex{idxTree: ds.NewART(), mu: new(sync.RWMutex)}
}

func newHashIndex() *hashIndex {
	return &hashIndex{trees: make(map[string]*ds.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newListIndex() *listIndex {
	return &listIndex{trees: make(map[string]*ds.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newSetIndex() *setIndex {
	return &setIndex{
		mu:      new(sync.RWMutex),
		murHash: util.NewMurmur128(),
		trees:   make(map[string]*ds.AdaptiveRadixTree),
	}
}

func newZSetIndex() *zSetIndex {
	return &zSetIndex{
		mu:      new(sync.RWMutex),
		indexes: make(map[string]*ZSetIndex),
	}
}

func Open(cfg DBConfig) (*LazyDB, error) {
	// create the dir path if not exist
	if !util.PathExist(cfg.DBPath) {
		if err := os.MkdirAll(cfg.DBPath, os.ModePerm); err != nil {
			log.Fatalf("Create db directory in %s error: %v", cfg.DBPath, err)
			return nil, err
		}
	}

	db := &LazyDB{
		cfg:              &cfg,
		index:            ds.NewConcurrentMap(int(cfg.HashIndexShardCount)),
		strIndex:         newStrIndex(),
		hashIndex:        newHashIndex(),
		listIndex:        newListIndex(),
		setIndex:         newSetIndex(),
		zSetIndex:        newZSetIndex(),
		fidsMap:          make(map[valueType]*MutexFids),
		activeLogFileMap: make(map[valueType]*MutexLogFile),
		archivedLogFile:  make(map[valueType]*ds.ConcurrentMap[uint32]),
	}

	for i := 0; i < logFileTypeNum; i++ {
		db.fidsMap[valueType(i)] = &MutexFids{fids: make([]uint32, 0)}
		db.archivedLogFile[valueType(i)] = ds.NewWithCustomShardingFunction[uint32](ds.DefaultShardCount, ds.SimpleSharding)
	}

	if err := db.initDiscard(); err != nil {
		log.Fatalf("Init Discard Files error: %v", err)
		return nil, err
	}

	if err := db.buildLogFiles(); err != nil {
		log.Fatalf("Build Log Files error: %v", err)
		return nil, err
	}

	if err := db.buildIndexFromLogFiles(); err != nil {
		log.Fatalf("Build Index From Log Files error: %v", err)
		return nil, err
	}

	return db, nil
}

// Sync flush the buffer into stable storage.
func (db *LazyDB) Sync() error {
	for _, mlf := range db.activeLogFileMap {
		mlf.mu.Lock()
		if err := mlf.lf.Sync(); err != nil {
			return err
		}
		mlf.mu.Unlock()
	}
	// sync discard files
	for _, dis := range db.discardsMap {
		if err := dis.sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close db
func (db *LazyDB) Close() error {
	for _, mlf := range db.activeLogFileMap {
		mlf.lf.Sync()
		err := mlf.lf.Close()
		if err != nil {
			log.Fatalf("Close log file err: %v", err)
		}
	}
	for typ, mutexFids := range db.fidsMap {
		for _, fid := range mutexFids.fids {
			mlf := db.getArchivedLogFile(typ, fid)
			if mlf == nil {
				continue
			}
			mlf.lf.Sync()
			err := mlf.lf.Close()
			if err != nil {
				log.Fatalf("Close archived log file err: %v", err)
			}
		}
	}

	db.index = nil
	db.fidsMap = nil
	db.activeLogFileMap = nil
	db.archivedLogFile = nil
	// close discard channel
	for _, dis := range db.discardsMap {
		close(dis.valChan)
	}
	return nil
}

func (db *LazyDB) mergeStr(fid uint32, offset int64, ent *logfile.LogEntry) error {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	indexVal := db.strIndex.idxTree.Get(ent.Key)
	if indexVal == nil {
		return nil
	}

	val, _ := indexVal.(*Value)
	// Only update rewriting entry when fid and offset is the same
	// as in index. Otherwise, this entry is updated in other log.
	if val != nil && val.fid == fid && val.offset == offset {
		// rewrite entry
		valuePos, err := db.writeLogEntry(valueTypeString, ent)
		if err != nil {
			return err
		}
		// update index
		db.updateIndexTree(valueTypeString, db.strIndex.idxTree, ent, valuePos, false)
	}
	return nil
}

func (db *LazyDB) mergeHash(fid uint32, offset int64, ent *logfile.LogEntry) error {
	key, _ := decodeKey(ent.Key)
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	idxTree := db.hashIndex.trees[util.ByteToString(key)]

	indexVal := idxTree.Get(ent.Key)
	if indexVal == nil {
		return nil
	}

	val, _ := indexVal.(*Value)
	// Only update rewriting entry when fid and offset is the same
	// as in index. Otherwise, this entry is updated in other log.
	if val != nil && val.fid == fid && val.offset == offset {
		// rewrite entry
		valuePos, err := db.writeLogEntry(valueTypeHash, ent)
		if err != nil {
			return err
		}
		// update index
		db.updateIndexTree(valueTypeHash, idxTree, ent, valuePos, false)
	}
	return nil
}

func (db *LazyDB) mergeZSet(fid uint32, offset int64, ent *logfile.LogEntry) error {
	key, _ := decodeKey(ent.Key)
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()
	idxTree := db.zSetIndex.indexes[util.ByteToString(key)].tree

	indexVal := idxTree.Get(ent.Key)
	if indexVal == nil {
		return nil
	}

	val, _ := indexVal.(*Value)
	// Only update rewriting entry when fid and offset is the same
	// as in index. Otherwise, this entry is updated in other log.
	if val != nil && val.fid == fid && val.offset == offset {
		// rewrite entry
		valuePos, err := db.writeLogEntry(valueTypeZSet, ent)
		if err != nil {
			return err
		}
		// update index
		db.updateIndexTree(valueTypeZSet, idxTree, ent, valuePos, false)
	}
	return nil
}

func (db *LazyDB) mergeList(fid uint32, offset int64, ent *logfile.LogEntry) error {
	key, _ := decodeKey(ent.Key)
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	idxTree := db.listIndex.trees[util.ByteToString(key)]
	indexVal := idxTree.Get(ent.Key)
	if indexVal == nil {
		return nil
	}

	val, _ := indexVal.(*Value)
	// Only update rewriting entry when fid and offset is the same
	// as in index. Otherwise, this entry is updated in other log.
	if val != nil && val.fid == fid && val.offset == offset {
		// rewrite entry
		valuePos, err := db.writeLogEntry(valueTypeList, ent)
		if err != nil {
			return err
		}
		// update index
		db.updateIndexTree(valueTypeList, idxTree, ent, valuePos, false)
	}
	return nil
}

func (db *LazyDB) Merge(typ valueType, targetFid uint32, gcRatio float64) error {

	activeFile := db.getActiveLogFile(typ)

	if err := db.discardsMap[typ].sync(); err != nil {
		return err
	}

	ccl, err := db.discardsMap[typ].getCCL(activeFile.lf.Fid, gcRatio)
	if err != nil {
		return err
	}

	for _, fid := range ccl {
		// only merge specified log file
		if targetFid >= 0 && targetFid != fid {
			continue
		}
		archivedFile := db.getArchivedLogFile(typ, targetFid)
		if archivedFile == nil {
			continue
		}
		var offset int64
		for {
			ent, size, err := archivedFile.lf.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrLogEndOfFile {
					break
				}
				return err
			}
			var off = offset
			offset += int64(size)
			if ent.Stat == logfile.SDelete {
				continue
			}
			ts := time.Now().Unix()
			if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
				continue
			}
			var mergeErr error
			switch typ {
			case valueTypeString:
				mergeErr = db.mergeStr(archivedFile.lf.Fid, off, ent)
			case valueTypeHash:
				mergeErr = db.mergeHash(archivedFile.lf.Fid, off, ent)
			case valueTypeZSet:
				mergeErr = db.mergeZSet(archivedFile.lf.Fid, off, ent)
			case valueTypeList:
				mergeErr = db.mergeList(archivedFile.lf.Fid, off, ent)
			}

			if mergeErr != nil {
				return mergeErr
			}
		}

		// delete older log file
		archivedLogFiles := db.archivedLogFile[typ]
		shard := archivedLogFiles.GetShardByWriting(targetFid)

		val, _ := shard.Get(targetFid)
		mutexLF := val.(*MutexLogFile)

		_ = mutexLF.lf.Delete() // close file and remove local file
		shard.Remove(targetFid) // remove index from memory

		shard.Unlock()

		db.discardsMap[typ].clear(fid)
	}

	return nil
}

// readLogEntry Reads entry from log files by fid and offset.
// Return error if entry does not exist.
func (db *LazyDB) readLogEntry(typ valueType, fid uint32, offset int64) (*logfile.LogEntry, error) {
	var lf *logfile.LogFile
	activelf := db.activeLogFileMap[typ]

	lf = activelf.lf
	if lf == nil {
		return nil, ErrOpenLogFile
	}

	if lf.Fid != fid {
		mlf := db.getArchivedLogFile(typ, fid)
		if mlf == nil || mlf.lf == nil {
			return nil, ErrLogFileNotExist
		}
		lf = mlf.lf
	}
	lf.Mu.RLock()
	defer lf.Mu.RUnlock()
	entry, _, err := lf.ReadLogEntry(offset)
	return entry, err
}

// writeLogEntry writes entry into active log file and returns position.
// Return nil and error if writing fails.
func (db *LazyDB) writeLogEntry(typ valueType, entry *logfile.LogEntry) (*ValuePos, error) {
	activeLogFile := db.getActiveLogFile(typ)
	if activeLogFile == nil {
		return nil, ErrOpenLogFile
	}
	activeLogFile.mu.Lock()
	defer activeLogFile.mu.Unlock()

	lf := activeLogFile.lf
	entBuf, entSize := logfile.EncodeEntry(entry)

	// maxsize exceeded
	if lf.Offset+int64(entSize) > db.cfg.MaxLogFileSize {
		if err := lf.Sync(); err != nil {
			return nil, err
		}

		newFid := lf.Fid + 1
		newActiveLF, err := logfile.Open(db.cfg.DBPath, newFid, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
		if err != nil {
			return nil, err
		}

		// move activeLogFile to archive
		db.archivedLogFile[typ].Set(lf.Fid, &MutexLogFile{lf: lf})

		// insert new fid
		fids := db.fidsMap[typ]
		fids.mu.Lock()
		fids.fids = append(fids.fids, newFid)
		fids.mu.Unlock()

		// update discard of new file
		db.discardsMap[typ].setTotal(newFid, uint32(db.cfg.MaxLogFileSize))

		// update activeLogFile
		activeLogFile.lf = newActiveLF
	}

	lf = activeLogFile.lf
	writeAt := lf.Offset
	if err := lf.Write(entBuf); err != nil {
		return nil, err
	}
	valPos := &ValuePos{
		fid:       lf.Fid,
		offset:    writeAt,
		entrySize: entSize,
	}
	return valPos, nil
}

// buildLogFiles Recover archivedLogFile from disk.
// Only run once when program start running.
func (db *LazyDB) buildLogFiles() error {
	fileInfos, err := os.ReadDir(db.cfg.DBPath)
	if err != nil {
		return err
	}
	for _, file := range fileInfos {
		if !strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			continue
		}
		splitInfo := strings.Split(file.Name(), ".")
		if len(splitInfo) != 3 {
			log.Printf("Invalid log file name: %s", file.Name())
			continue
		}
		typ := valueType(logfile.FileTypesMap[splitInfo[1]])
		fid, err := strconv.Atoi(splitInfo[2])
		if err != nil {
			log.Printf("Invalid log file name: %s", file.Name())
			continue
		}
		fids := db.fidsMap[typ]
		fids.fids = append(fids.fids, uint32(fid))
	}

	build := func(typ valueType) {
		mutexFids := db.fidsMap[typ]
		fids := mutexFids.fids
		if len(fids) == 0 {
			return
		}
		// newly created log file has bigger fid
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		archivedLogFiles := db.archivedLogFile[typ]
		for i, fid := range fids {
			lf, err := logfile.Open(db.cfg.DBPath, fid, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
			if err != nil {
				log.Fatalf("Open Log File error:%v. Type: %v, Fid: %v,", err, typ, fid)
				continue
			}

			// latest one is the active log file
			if i == len(fids)-1 {
				db.activeLogFileMap[typ] = &MutexLogFile{lf: lf}
			} else {
				archivedLogFiles.Set(fid, &MutexLogFile{lf: lf})
			}
		}
	}
	for typ := 0; typ < logFileTypeNum; typ++ {
		build(valueType(typ))
	}
	return nil
}

// getArchivedLogFile Util function for get archivedLogFile from ConcurrentMap.
// Returns nil when target log file does not exist
func (db *LazyDB) getArchivedLogFile(typ valueType, fid uint32) *MutexLogFile {
	lfs := db.archivedLogFile[typ]
	v, ok := lfs.Get(fid)
	if !ok {
		return nil
	}
	lf := v.(*MutexLogFile)
	return lf
}

func (db *LazyDB) getActiveLogFile(typ valueType) *MutexLogFile {
	mutexLf, ok := db.activeLogFileMap[typ]
	if !ok {
		lf, err := logfile.Open(db.cfg.DBPath, 1, db.cfg.MaxLogFileSize, logfile.FType(typ), db.cfg.IOType)
		if err != nil {
			log.Fatalf("Create New Log File error: %v", err)
			return nil
		}
		newMutexLf := &MutexLogFile{lf: lf}
		db.activeLogFileMap[typ] = newMutexLf

		fids := db.fidsMap[typ]
		fids.mu.Lock()
		fids.fids = append(fids.fids, lf.Fid)
		fids.mu.Unlock()

		db.discardsMap[typ].setTotal(lf.Fid, uint32(db.cfg.MaxLogFileSize))

		return newMutexLf
	}
	return mutexLf
}
func (db *LazyDB) initDiscard() error {
	discardPath := path.Join(db.cfg.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discardsMap := make(map[valueType]*discard)
	for i := 0; i < logFileTypeNum; i++ {
		name := logfile.FileNamesMap[logfile.FType(i)] + discardFileName
		d, err := newDiscard(discardPath, name, db.cfg.DiscardBufferSize)
		if err != nil {
			return err
		}
		discardsMap[valueType(i)] = d
	}
	db.discardsMap = discardsMap

	return nil
}

func (db *LazyDB) sendDiscard(oldVal any, updated bool, typ valueType) error {
	if !updated || oldVal == nil {
		return nil
	}
	node, _ := oldVal.(*Value)
	if node == nil || node.entrySize == 0 {
		return nil
	}

	select {
	case db.discardsMap[typ].valChan <- node:
	default:
		log.Fatalf("send discard fail")
	}
	return nil
}

func encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf, header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):index+length], subKey)
		return buf
	}
	return header[:index]
}

// decodeKey returns key and subKey from entry
// key is used to locate the index tree, subKey is used to locate the Value in the indexTree
func decodeKey(encodedKey []byte) ([]byte, []byte) {
	var index int
	keyLen, n := binary.Varint(encodedKey)
	index += n
	_, n = binary.Varint(encodedKey[index:])
	index += n
	return encodedKey[index : index+int(keyLen)], encodedKey[index+int(keyLen):]
}
