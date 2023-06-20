package lazydb

import (
	"errors"
	"github.com/gansidui/skiplist"
	"lazydb/ds"
	"lazydb/logfile"
	"lazydb/util"
	"log"
)

var (
	ErrZSetKeyNotExist    = errors.New("zset key not exist")
	ErrZSetMemberNotExist = errors.New("zset member not exist")
)

type ZSetIndex struct {
	tree *ds.AdaptiveRadixTree
	skl  *skiplist.SkipList
}

type Node struct {
	score  float64
	member string
}

func (n *Node) Less(other interface{}) bool {
	if n.score < other.(*Node).score {
		return true
	}
	if n.score == other.(*Node).score && len(n.member) < len(other.(*Node).member) {
		return true
	}
	return false
}

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *LazyDB) ZAdd(key []byte, args ...[]byte) error {
	if len(args)&1 == 1 {
		return ErrInvalidParam
	}
	if len(args) == 0 {
		return nil
	}
	db.zSetIndex.mu.Lock()
	defer db.zSetIndex.mu.Unlock()

	strKey := util.ByteToString(key)
	if db.zSetIndex.indexes[strKey] == nil {
		tree := ds.NewART()
		skl := skiplist.New()
		db.zSetIndex.indexes[strKey] = &ZSetIndex{
			tree: tree,
			skl:  skl,
		}
	}
	tree := db.zSetIndex.indexes[strKey].tree
	skl := db.zSetIndex.indexes[strKey].skl
	for i := 0; i < len(args); i += 2 {
		score, member := args[i], args[i+1]
		zsetKey := encodeKey(key, member)
		entry := &logfile.LogEntry{Key: zsetKey, Value: score}
		valPos, err := db.writeLogEntry(valueTypeZSet, entry)
		if err != nil {
			return err
		}
		if tree.Get(zsetKey) != nil {
			oriScore, err := db.getValue(tree, zsetKey, valueTypeZSet)
			if err != nil {
				return err
			}
			skl.Delete(&Node{score: util.ByteToFloat64(oriScore), member: util.ByteToString(member)})
		}
		err = db.updateIndexTree(valueTypeZSet, tree, entry, valPos, true)
		if err != nil {
			return err
		}
		skl.Insert(&Node{score: util.ByteToFloat64(score), member: util.ByteToString(member)})
	}
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *LazyDB) ZScore(key, member []byte) (score float64, err error) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil {
		return 0, ErrZSetKeyNotExist
	}
	zsetKey := encodeKey(key, member)
	val, err := db.getValue(idx.tree, zsetKey, valueTypeZSet)
	if err != nil {
		return 0, ErrZSetMemberNotExist
	}

	return util.ByteToFloat64(val), nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *LazyDB) ZCard(key []byte) int {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil {
		return 0
	}
	return idx.tree.Size()
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (db *LazyDB) ZRank(key, member []byte) (rank int, err error) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	score, err := db.ZScore(key, member)
	if err != nil {
		return -1, err
	}
	skl := db.zSetIndex.indexes[util.ByteToString(key)].skl
	return skl.GetRank(&Node{score: score, member: util.ByteToString(member)}) - 1, nil
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *LazyDB) ZRevRank(key, member []byte) (rank int, err error) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	score, err := db.ZScore(key, member)
	if err != nil {
		return -1, err
	}
	skl := db.zSetIndex.indexes[util.ByteToString(key)].skl
	return skl.Len() - skl.GetRank(&Node{score: score, member: util.ByteToString(member)}), nil
}

// ZRange returns the specified range of elements in the sorted set stored at <key>.
func (db *LazyDB) ZRange(key []byte, start, stop int) (members [][]byte) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.skl == nil {
		return nil
	}
	if start < 0 {
		start += idx.skl.Len()
	}
	if stop < 0 {
		stop += idx.skl.Len()
	}
	start += 1
	stop += 1
	stop = util.Min(stop, idx.skl.Len())
	e := idx.skl.GetElementByRank(start)
	for i := start; i <= stop; i++ {
		members = append(members, util.StringToByte(e.Value.(*Node).member))
		e = e.Next()
	}
	return members
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at key.
func (db *LazyDB) ZRangeWithScores(key []byte, start, stop int) (members [][]byte, scores []float64) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.skl == nil {
		return nil, nil
	}
	if start < 0 {
		start += idx.skl.Len()
	}
	if stop < 0 {
		stop += idx.skl.Len()
	}
	start += 1
	stop += 1
	stop = util.Min(stop, idx.skl.Len())
	e := idx.skl.GetElementByRank(start)
	for i := start; i <= stop; i++ {
		members = append(members, util.StringToByte(e.Value.(*Node).member))
		scores = append(scores, e.Value.(*Node).score)
		e = e.Next()
	}
	return members, scores
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (db *LazyDB) ZRevRange(key []byte, start, stop int) (members [][]byte) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.skl == nil {
		return nil
	}
	if start < 0 {
		start += idx.skl.Len()
	}
	if stop < 0 {
		stop += idx.skl.Len()
	}
	start += 1
	stop += 1
	stop = util.Min(stop, idx.skl.Len())
	e := idx.skl.GetElementByRank(stop)
	for i := start; i <= stop; i++ {
		members = append(members, util.StringToByte(e.Value.(*Node).member))
		e = e.Prev()
	}
	return members
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (db *LazyDB) ZRevRangeWithScores(key []byte, start, stop int) (members [][]byte, scores []float64) {
	db.zSetIndex.mu.RLock()
	defer db.zSetIndex.mu.RUnlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.skl == nil {
		return nil, nil
	}
	if start < 0 {
		start += idx.skl.Len()
	}
	if stop < 0 {
		stop += idx.skl.Len()
	}
	start += 1
	stop += 1
	stop = util.Min(stop, idx.skl.Len())
	e := idx.skl.GetElementByRank(stop)
	for i := start; i <= stop; i++ {
		members = append(members, util.StringToByte(e.Value.(*Node).member))
		scores = append(scores, e.Value.(*Node).score)
		e = e.Prev()
	}
	return members, scores
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (db *LazyDB) ZIncrBy(key []byte, increment float64, member []byte) (float64, error) {
	score, _ := db.ZScore(key, member)
	err := db.ZAdd(key, util.Float64ToByte(score+increment), member)
	if err != nil {
		return 0, err
	}
	return score + increment, nil
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *LazyDB) ZRem(key []byte, members ...[]byte) (number int, err error) {
	db.zSetIndex.mu.Lock()
	defer db.zSetIndex.mu.Unlock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil {
		return 0, nil
	}
	var count int
	for _, member := range members {
		zSetKey := encodeKey(key, member)
		entry := &logfile.LogEntry{Key: zSetKey, Stat: logfile.SDelete}
		pos, err := db.writeLogEntry(valueTypeZSet, entry)
		if err != nil {
			return count, err
		}
		score, err := db.getValue(idx.tree, zSetKey, valueTypeZSet)
		if err != nil {
			if err != ErrKeyNotFound {
				return 0, err
			}
			continue
		}
		val, updated := idx.tree.Delete(zSetKey)
		idx.skl.Delete(&Node{score: util.ByteToFloat64(score), member: util.ByteToString(member)})
		count++
		// delete invalid entry
		db.sendDiscard(val, updated, valueTypeZSet)
		// also merge the delete entry
		_, size := logfile.EncodeEntry(entry)
		node := &Value{fid: pos.fid, entrySize: size}
		select {
		case db.discardsMap[valueTypeZSet].valChan <- node:
		default:
			log.Fatal("send discard fail")
		}
	}
	return count, nil
}

// ZPopMax Removes and returns up to count members with the highest scores in the sorted set stored at key.
// When left unspecified, the default value for count is 1.
// Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
func (db *LazyDB) ZPopMax(key []byte) ([]byte, float64, error) {
	db.zSetIndex.mu.Lock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil || idx.skl == nil || idx.skl.Len() == 0 {
		return nil, 0, nil
	}
	element := idx.skl.GetElementByRank(idx.skl.Len())
	member := element.Value.(*Node).member
	score := element.Value.(*Node).score

	db.zSetIndex.mu.Unlock()

	_, err := db.ZRem(key, util.StringToByte(member))
	if err != nil {
		return nil, 0, err
	}
	return util.StringToByte(member), score, nil
}

func (db *LazyDB) ZPopMaxWithCount(key []byte, count int) (members [][]byte, scores []float64, err error) {
	db.zSetIndex.mu.Lock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil || idx.skl == nil {
		return nil, nil, nil
	}
	count = util.Min(count, idx.skl.Len())

	db.zSetIndex.mu.Unlock()

	for i := 0; i < count; i++ {
		member, score, err := db.ZPopMax(key)
		if err != nil {
			return nil, nil, err
		}
		members = append(members, member)
		scores = append(scores, score)
	}
	return
}

// ZPopMin Removes and returns up to count members with the lowest scores in the sorted set stored at key.
// When left unspecified, the default value for count is 1.
// Specifying a count value that is higher than the sorted set's cardinality will not produce an error.
func (db *LazyDB) ZPopMin(key []byte) ([]byte, float64, error) {
	db.zSetIndex.mu.Lock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil || idx.skl == nil || idx.skl.Len() == 0 {
		return nil, 0, nil
	}
	element := idx.skl.GetElementByRank(1)
	member := element.Value.(*Node).member
	score := element.Value.(*Node).score

	db.zSetIndex.mu.Unlock()

	_, err := db.ZRem(key, util.StringToByte(member))
	if err != nil {
		return nil, 0, err
	}
	return util.StringToByte(member), score, nil
}

func (db *LazyDB) ZPopMinWithCount(key []byte, count int) (members [][]byte, scores []float64, err error) {
	db.zSetIndex.mu.Lock()

	idx := db.zSetIndex.indexes[util.ByteToString(key)]
	if idx == nil || idx.tree == nil || idx.skl == nil {
		return nil, nil, nil
	}
	count = util.Min(count, idx.skl.Len())

	db.zSetIndex.mu.Unlock()

	for i := 0; i < count; i++ {
		member, score, err := db.ZPopMin(key)
		if err != nil {
			return nil, nil, err
		}
		members = append(members, member)
		scores = append(scores, score)
	}
	return
}
