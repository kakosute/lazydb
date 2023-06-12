package lazydb

import (
	"encoding/binary"
	"lazydb/ds"
	"lazydb/logfile"
)

func (db *LazyDB) LPush(key []byte, args ...[]byte) (err error) {
	if len(args) == 0 {
		return nil
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if (db.listIndex.trees[string(key)]) == nil {
		db.listIndex.trees[string(key)] = ds.NewART()
	}
	for _, arg := range args {
		if err := db.push(key, arg, true); err != nil {
			return err
		}
	}
	return nil
}

func (db *LazyDB) LPushX(key []byte, args ...[]byte) (err error) {
	if len(args) == 0 {
		return nil
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if (db.listIndex.trees[string(key)]) == nil {
		return ErrKeyNotFound
	}
	for _, arg := range args {
		if err := db.push(key, arg, true); err != nil {
			return err
		}
	}
	return nil
}

func (db *LazyDB) LPop(key []byte) (value []byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	value, err = db.pop(key, true)
	return value, err
}

func (db *LazyDB) RPush(key []byte, args ...[]byte) (err error) {
	if len(args) == 0 {
		return nil
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if (db.listIndex.trees[string(key)]) == nil {
		db.listIndex.trees[string(key)] = ds.NewART()
	}
	for _, arg := range args {
		if err := db.push(key, arg, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *LazyDB) RPushX(key []byte, args ...[]byte) (err error) {
	if len(args) == 0 {
		return nil
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if (db.listIndex.trees[string(key)]) == nil {
		return ErrKeyNotFound
	}
	for _, arg := range args {
		if err := db.push(key, arg, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *LazyDB) RPop(key []byte) (value []byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	value, err = db.pop(key, false)
	return value, err
}

func (db *LazyDB) LSet(key []byte, index int, value []byte) (err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if (db.listIndex.trees[string(key)]) == nil {
		return ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.lMeta(idxTree, key)
	if err != nil {
		return err
	}
	s, err := db.lSequence(headSeq, tailSeq, index)
	if s <= headSeq || s >= tailSeq {
		return ErrWrongIndex
	}
	encodeKey := db.encodeListKey(key, s)
	entry := &logfile.LogEntry{Key: encodeKey, Value: value}
	pos, err := db.writeLogEntry(valueTypeList, entry)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(valueTypeList, idxTree, entry, pos, false)
	return err
}

func (db *LazyDB) LIndex(key []byte, index int) (value []byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if (db.listIndex.trees[string(key)]) == nil {
		return nil, ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.lMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	s, err := db.lSequence(headSeq, tailSeq, index)
	if s <= headSeq || s >= tailSeq {
		return nil, ErrWrongIndex
	}
	encodeKey := db.encodeListKey(key, s)
	val, err := db.getValue(idxTree, encodeKey, valueTypeList)
	if err != nil {
		return nil, err
	}
	return val, err
}

func (db *LazyDB) LLen(key []byte) (len int) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if (db.listIndex.trees[string(key)]) == nil {
		return 0
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.lMeta(idxTree, key)
	if err != nil {
		return 0
	}
	len = int(tailSeq - headSeq - 1)
	return len
}

func (db *LazyDB) LRange(key []byte, start int, stop int) (value [][]byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	if (db.listIndex.trees[string(key)]) == nil {
		return nil, ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.lMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	startSeq, err := db.lSequence(headSeq, tailSeq, start)
	if err != nil {
		return nil, err
	}
	stopSeq, err := db.lSequence(headSeq, tailSeq, stop)
	if err != nil {
		return nil, err
	}
	if startSeq > stopSeq || startSeq >= tailSeq || stopSeq <= headSeq {
		return nil, ErrWrongIndex
	}
	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}
	if stopSeq >= tailSeq {
		stopSeq = tailSeq - 1
	}
	for seq := startSeq; seq < stopSeq+1; seq++ {
		encodeKey := db.encodeListKey(key, seq)
		val, err := db.getValue(idxTree, encodeKey, valueTypeList)
		if err != nil {
			return nil, err
		}
		value = append(value, val)
	}
	return value, nil
}

func (db *LazyDB) LMove(sourceKey []byte, distKey []byte, sourceIsLeft bool, distIsLeft bool) (val []byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	val, err = db.pop(sourceKey, sourceIsLeft)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if db.listIndex.trees[string(distKey)] == nil {
		db.listIndex.trees[string(distKey)] = ds.NewART()
	}
	err = db.push(distKey, val, distIsLeft)
	if err != nil {
		return nil, err
	}
	return val, err
}

func (db *LazyDB) pop(key []byte, isLeft bool) (value []byte, err error) {
	if (db.listIndex.trees[string(key)]) == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.lMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	if tailSeq-headSeq-1 <= 0 {
		return nil, nil
	}
	var s = headSeq + 1
	if isLeft != true {
		s = tailSeq - 1
	}
	encodeKey := db.encodeListKey(key, s)
	value, err = db.getValue(idxTree, encodeKey, valueTypeList)
	if err != nil {
		return nil, nil
	}
	entry := &logfile.LogEntry{Key: encodeKey, Stat: logfile.SDelete}
	_, err = db.writeLogEntry(valueTypeList, entry)
	if err != nil {
		return nil, nil
	}

	//delVal, updated := idxTree.Delete(encodeKey)

	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.saveLMeta(idxTree, key, headSeq, tailSeq); err != nil {
		return nil, err
	}

	// TODO: send discard

	if tailSeq-headSeq-1 == 0 {
		// reset meta
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveLMeta(idxTree, key, headSeq, tailSeq)
		}
		delete(db.listIndex.trees, string(key))
	}
	return value, nil
}

func (db *LazyDB) push(key []byte, arg []byte, isLeft bool) (err error) {
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.lMeta(idxTree, key)
	if err != nil {
		return err
	}
	var s = headSeq
	if isLeft != true {
		s = tailSeq
	}
	encodeKey := db.encodeListKey(key, s)
	entry := &logfile.LogEntry{Key: encodeKey, Value: arg}
	vPos, err := db.writeLogEntry(valueTypeList, entry)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(valueTypeList, idxTree, entry, vPos, false)
	if err != nil {
		return err
	}
	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}
	err = db.saveLMeta(idxTree, key, headSeq, tailSeq)
	return err
}

func (db *LazyDB) lMeta(idxTree *ds.AdaptiveRadixTree, key []byte) (headSeq uint32, tailSeq uint32, err error) {
	value, err := db.getValue(idxTree, key, valueTypeList)
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}
	headSeq = initialListSeq
	tailSeq = initialListSeq + 1
	if len(value) != 0 {
		headSeq = binary.LittleEndian.Uint32(value[:4])
		tailSeq = binary.LittleEndian.Uint32(value[4:8])
	}
	return headSeq, tailSeq, nil
}

func (db *LazyDB) saveLMeta(idxTree *ds.AdaptiveRadixTree, key []byte, headSeq uint32, tailSeq uint32) (err error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	entry := &logfile.LogEntry{Key: key, Value: buf, Stat: logfile.SListMeta}
	pos, err := db.writeLogEntry(valueTypeList, entry)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(valueTypeList, idxTree, entry, pos, false)
	return err
}

func (db *LazyDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key)
	return buf
}

func (db *LazyDB) decodeListKey(buf []byte) (key []byte, seq uint32) {
	seq = binary.LittleEndian.Uint32(buf[:4])
	key = make([]byte, len(buf[4:]))
	copy(key, buf[4:])
	return key, seq
}

func (db *LazyDB) lSequence(headSeq uint32, tailSeq uint32, index int) (seq uint32, err error) {
	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}
