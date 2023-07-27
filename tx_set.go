package lazydb

import (
	"lazydb/ds"
	"lazydb/logfile"
)

func (tx *Tx) SAdd(key []byte, members ...[]byte) {
	if tx.db.setIndex.trees[string(key)] == nil {
		tx.db.setIndex.trees[string(key)] = ds.NewART()
	}

	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}
		tx.db.setIndex.murHash.Write(mem)

		sum := tx.db.setIndex.murHash.EncodeSum128()
		tx.db.setIndex.murHash.Reset()

		ent := &logfile.LogEntry{Key: key, Value: mem}
		tx.pendingSet = append(tx.pendingSet, &pSet{
			e:   ent,
			sum: sum,
			mem: mem,
		})
	}
}
