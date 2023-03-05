package lazydb

import (
	"io"
	"lazydb/logfile"
	"lazydb/util"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

func (db *LazyDB) buildIndexByVType(typ valueType, entry *logfile.LogEntry, vPos *ValuePos) {
	value := Value{
		value:     entry.Value,
		vType:     typ,
		fid:       vPos.fid,
		offset:    vPos.offset,
		entrySize: vPos.entrySize,
		expiredAt: 0,
	}
	db.index.Set(util.ByteToString(entry.Key), value)
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

func (db *LazyDB) getValue(key []byte, typ valueType) ([]byte, error) {
	rawValue, ok := db.index.Get(util.ByteToString(key))
	if !ok {
		return nil, ErrKeyNotFound

	}
	val, ok := rawValue.(Value)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// TODO: check expire time later

	ent, err := db.readLogEntry(typ, val.fid, val.offset)
	if err != nil {
		return nil, err
	}

	// TODO: !get key deletion status

	return ent.Value, nil
}
