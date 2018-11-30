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

func (tree *BTree) GetMux() *sync.RWMutex {
	return &tree.mux
}

func (tree *BTree) Find(key string) (value string, ok bool) {
	tree.mux.RLock()
	return tree.Root.Find(key, &tree.mux)
}

func (tree *BTree) AcquireLockContext(key string) LockContext {
	tree.mux.RLock()
	lockContext := LockContext{}
	lockContext.Add(tree)
	tree.Root.AcquireLockContext(key, &lockContext)
	return lockContext
}

func (tree *BTree) CheckAncestor(key string, ancestor LockedUpserter) bool {
	if tree == ancestor {
		return true
	}
	tree.mux.RLock()
	return tree.Root.CheckAncestor(key, ancestor, &tree.mux)
}

func (tree *BTree) LockedUpsert(key, value string) InsertionResult {
	tree.Root.GetMux().Lock()
	defer tree.Root.GetMux().Unlock()
	result := tree.Root.LockedUpsert(key, value)
	if result.Left != nil {
		tree.Root = &IntermediateNode{
			Keys:     []string{result.SplitKey},
			Children: []Node{result.Left, result.Right},
			MaxKeys:  tree.maxKeys,
			Mux:      sync.RWMutex{},
		}
	}
	return result
}

func (tree *BTree) Upsert(key, value string) (created bool) {
	lockContext := tree.AcquireLockContext(key)
	stableAncestor := lockContext.Upgrade()
	for !tree.CheckAncestor(key, stableAncestor) {
		stableAncestor.GetMux().Unlock()
		lockContext := tree.AcquireLockContext(key)
		stableAncestor = lockContext.Upgrade()
	}
	defer stableAncestor.GetMux().Unlock()
	result := stableAncestor.LockedUpsert(key, value)
	return result.Created
}
