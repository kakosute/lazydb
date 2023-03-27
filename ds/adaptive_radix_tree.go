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

func (t *AdaptiveRadixTree) PrefixScan(prefix []byte, count int) (keys [][]byte) {
	cb := func(node art.Node) bool {
		if node.Kind() != art.Leaf {
			return true
		}
		if count <= 0 {
			return false
		}
		keys = append(keys, node.Key())
		count--
		return true
	}

	if len(prefix) == 0 {
		t.tree.ForEach(cb)
	} else {
		t.tree.ForEachPrefix(prefix, cb)
	}
	return
}
