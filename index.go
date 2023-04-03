package lazydb

import (
	"io"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

func (db *LazyDB) buildStrIndex(entry *logfile.LogEntry, vPos *ValuePos) {
	if entry.Stat == logfile.SDelete {
		db.strIndex.idxTree.Delete(entry.Key)
		return
	}
	_, size := logfile.EncodeEntry(entry)
	idxNode := &Value{fid: vPos.fid, offset: vPos.offset, entrySize: size}

	// TODO: set expire time

	db.strIndex.idxTree.Put(entry.Key, idxNode)
}

func (db *LazyDB) buildHashIndex(entry *logfile.LogEntry, vPos *ValuePos) {
	key, _ := decodeKey(entry.Key)
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	idxTree := db.hashIndex.trees[util.ByteToString(key)]
	if entry.Stat == logfile.SDelete {
		idxTree.Delete(entry.Key)
		return
	}

	_, size := logfile.EncodeEntry(entry)
	idxNode := &Value{fid: vPos.fid, offset: vPos.offset, entrySize: size}

	// TODO: set expire time

	idxTree.Put(entry.Key, idxNode)
}

func (db *LazyDB) buildIndexByVType(typ valueType, entry *logfile.LogEntry, vPos *ValuePos) {
	switch typ {
	case valueTypeString:
		db.buildStrIndex(entry, vPos)
	case valueTypeHash:
		db.buildHashIndex(entry, vPos)
	}
}

func (db *LazyDB) buildIndexFromLogFiles() error {
	build := func(typ valueType, wg *sync.WaitGroup) {
		defer wg.Done()

		mutexFids := db.fidsMap[typ]
		fids := mutexFids.fids
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFileMap[typ].lf
			} else {
				mlf := db.getArchivedLogFile(typ, fid)
				if mlf == nil {
					log.Fatalf("log file is nil, failed to open db")
				}
				logFile = mlf.lf
			}
			if logFile == nil {
				log.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				entry, entSize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrLogEndOfFile {
						break
					}
					log.Fatalf("read log entry from file err: %v, failed to open db", err)
				}
				vPos := &ValuePos{fid: fid, offset: offset, entrySize: entSize}
				db.buildIndexByVType(typ, entry, vPos)
				offset += int64(entSize)
			}
			// set latest log file`s WriteAt.
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.Offset, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go build(valueType(i), wg)
	}
	wg.Wait()
	return nil
}

func (db *LazyDB) getValue(idxTree *ds.AdaptiveRadixTree, key []byte, typ valueType) ([]byte, error) {
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	val, ok := rawValue.(*Value)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// TODO: check expire time later

	ent, err := db.readLogEntry(typ, val.fid, val.offset)
	if err != nil {
		return nil, err
	}

	// check if key has been deleted
	if ent.Stat == logfile.SDelete {
		return nil, ErrKeyNotFound
	}

	return ent.Value, nil
}

func (db *LazyDB) updateIndexTree(typ valueType, idxTree *ds.AdaptiveRadixTree, entry *logfile.LogEntry, vPos *ValuePos,
	sendDiscard bool) error {

	var size = vPos.entrySize
	if typ == valueTypeString || typ == valueTypeList {
		_, size = logfile.EncodeEntry(entry)
	}
	idxNode := &Value{fid: vPos.fid, offset: vPos.offset, entrySize: size}

	// TODO: set expired time

	_, _ = idxTree.Put(entry.Key, idxNode)

	// TODO: send discard

	return nil
}
