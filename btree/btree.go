package btree

import (
	"sync"
)

type BTree struct {
	Root    Node
	maxKeys int
	mux     sync.RWMutex
}

func NewBTree(maxKeys int) BTree {
	return BTree{
		Root:    &LeafNode{MaxKeys: maxKeys},
		maxKeys: maxKeys,
	}
}

func (tree *BTree) Find(key string) (value string, ok bool) {
	tree.mux.RLock()
	defer tree.mux.RUnlock()
	return tree.Root.Find(key)
}

func (tree *BTree) Upsert(key, value string) (created bool) {
	tree.mux.Lock()
	lockContext := LockContext{}
	lockContext.Add(&tree.mux)
	defer lockContext.UnlockAll()
	result := tree.Root.Upsert(key, value, &lockContext)
	if result.Left != nil {
		tree.Root = &IntermediateNode{
			Keys:     []string{result.SplitKey},
			Children: []Node{result.Left, result.Right},
			MaxKeys:  tree.maxKeys,
			Mux:      sync.RWMutex{},
		}
	}
	return result.Created
}
