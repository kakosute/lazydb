package lazydb

import (
	"lazydb/ds"
	"lazydb/logfile"
	"log"
)

// SAdd add the values the set stored at key.
func (db *LazyDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = ds.NewART()
	}

	idxTree := db.setIndex.trees[string(key)]
	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}
		if err := db.setIndex.murHash.Write(mem); err != nil {
			return err
		}

		sum := db.setIndex.murHash.EncodeSum128()
		db.setIndex.murHash.Reset()

		ent := &logfile.LogEntry{Key: key, Value: mem}
		valPos, err := db.writeLogEntry(valueTypeSet, ent)
		if err != nil {
			return err
		}

		entry := &logfile.LogEntry{Key: sum, Value: mem}
		_, size := logfile.EncodeEntry(ent)
		valPos.entrySize = size

		if err := db.updateIndexTree(valueTypeSet, idxTree, entry, valPos, false); err != nil {
			return err
		}
	}
	return nil
}

// SIsMember returns if the argument is the one value of the set stored at key.
func (db *LazyDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.setIndex.trees[string(key)] == nil {
		return false
	}
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murHash.Write(member); err != nil {
		return false
	}

	sum := db.setIndex.murHash.EncodeSum128()
	db.setIndex.murHash.Reset()
	node := idxTree.Get(sum)

	return node != nil
}

// SMembers returns all the values of the set value stored at key.
func (db *LazyDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.sMembers(key)
}

// Helper for getting all members of the given set key.
func (db *LazyDB) sMembers(key []byte) ([][]byte, error) {
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}

	var values [][]byte
	idxTree := db.setIndex.trees[string(key)]
	iterator := idxTree.Iterator()
	for iterator.HasNext() {
		node, _ := iterator.Next()
		if node == nil {
			continue
		}

		val, err := db.getValue(idxTree, node.Key(), valueTypeSet)
		if err != nil {
			return nil, err
		}

		values = append(values, val)
	}
	return values, nil
}

func (db *LazyDB) sremInternal(key []byte, member []byte) error {
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murHash.Write(member); err != nil {
		return err
	}

	sum := db.setIndex.murHash.EncodeSum128()
	db.setIndex.murHash.Reset()

	val, updated := idxTree.Delete(sum)
	if !updated {
		return nil
	}

	entry := &logfile.LogEntry{Key: key, Value: sum, Stat: logfile.SDelete}
	pos, err := db.writeLogEntry(valueTypeSet, entry)
	if err != nil {
		return err
	}

	// delete invalid entry
	db.sendDiscard(val, updated, valueTypeSet)
	// also merge the delete entry
	_, size := logfile.EncodeEntry(entry)
	node := &Value{fid: pos.fid, entrySize: size}
	select {
	case db.discardsMap[valueTypeSet].valChan <- node:
	default:
		log.Fatal("send discard fail")
	}
	return nil
}

// SPop removes and returns members from the set value store at key.
func (db *LazyDB) SPop(key []byte, num uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.setIndex.trees[string(key)]

	var values [][]byte
	iter := idxTree.Iterator()

	for iter.HasNext() && num > 0 {
		num--
		node, _ := iter.Next()
		if node == nil {
			continue
		}

		val, err := db.getValue(idxTree, node.Key(), valueTypeSet)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}

	for _, val := range values {
		if err := db.sremInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SRem remove the specified members from the set stored at key.
func (db *LazyDB) SRem(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil
	}

	for _, mem := range members {
		if err := db.sremInternal(key, mem); err != nil {
			return err
		}
	}
	return nil
}
