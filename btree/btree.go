package btree

import (
	"sync"
)

type BTree struct {
	Root      Node
	maxKeys   int
	mux       sync.RWMutex
	parentMux sync.RWMutex
}

func NewBTree(maxKeys int) BTree {
	return BTree{
		Root:    &LeafNode{MaxKeys: maxKeys},
		maxKeys: maxKeys,
	}
}

func (tree *BTree) GetMux() *sync.RWMutex {
	return &tree.mux
}

func (tree *BTree) Find(key string) (value string, ok bool) {
	tree.mux.RLock()
	return tree.Root.Find(key, &tree.mux)
}

func (tree *BTree) IsStable() bool {
	return true
}

func (tree *BTree) SafeUpsert(key, value string) InsertionResult {
	tree.Root.GetMux().Lock()
	defer tree.Root.GetMux().Unlock()
	return tree.Root.SafeUpsert(key, value)
}

func (tree *BTree) GetStableAncestor(key string) (SafeUpserter, *sync.RWMutex) {
	tree.parentMux.RLock()
	lockContext := LockContext{
		Muxes: []*sync.RWMutex{&tree.parentMux},
	}
	lockContext.Add(tree)
	tree.Root.AcquireLockContext(key, &lockContext)
	return lockContext.Resolve()
}

func (tree *BTree) Upsert(key, value string) (created bool) {
	stableAncestor, parentMux := tree.GetStableAncestor(key)
	stableAncestor.GetMux().Lock()
	if !stableAncestor.IsStable() {
		parentMux.RUnlock()
		stableAncestor.GetMux().Unlock()
		return tree.Upsert(key, value)
	}
	defer parentMux.RUnlock()
	defer stableAncestor.GetMux().Unlock()
	result := stableAncestor.SafeUpsert(key, value)
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
