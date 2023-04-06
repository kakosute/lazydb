package ds

import (
	art "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree art.Tree
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: art.New(),
	}
}

func (t *AdaptiveRadixTree) Get(key []byte) interface{} {
	value, _ := t.tree.Search(key)
	return value
}

func (t *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldVal interface{}, updated bool) {
	return t.tree.Insert(key, value)
}

func (t *AdaptiveRadixTree) Delete(key []byte) (val interface{}, updated bool) {
	return t.tree.Delete(key)
}

func (t *AdaptiveRadixTree) Size() int {
	return t.tree.Size()
}

func (t *AdaptiveRadixTree) Iterator() art.Iterator {
	return t.tree.Iterator()
}

// PrefixScan returns keys start with specific prefix
// Count refers to the maximum number of retrieved keys. No limitation if count is smaller than 0.
func (t *AdaptiveRadixTree) PrefixScan(prefix []byte, count int) (keys [][]byte) {
	cb := func(node art.Node) bool {
		if node.Kind() != art.Leaf {
			return true
		}
		if count == 0 {
			return false
		}
		keys = append(keys, node.Key())
		if count > 0 {
			count--
		}
		return true
	}

	if len(prefix) == 0 {
		t.tree.ForEach(cb)
	} else {
		t.tree.ForEachPrefix(prefix, cb)
	}
	return
}
