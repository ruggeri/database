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

// NED: Pretty sure that if you don't add the tree, but instead have a
// LockContext.New, then this method can just panic, as no one would
// call it. I think it's a pretty dubious method; if someone is asking
// if the "tree is stable" maybe they're making a mistake.
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
	// NED: Feels weird to add the tree as the StableAncestor this way,
	// when we've gone in and set Muxes explicitly. Why not just go all
	// the way and set StableAncestor to tree?
	//
	// Also: I'm afraid you're going to try to acquire the read lock
	// *twice*. That is not safe against deadlock. Put a sleep before
	// `lockContext.Add(tree)` and I think you'll get deadlock.
	//
	// https://golang.org/pkg/sync/#RWMutex explains what you shouldn't
	// do. Why?
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
	// NED: I think you can release the parentMux right after getting the
	// write lock. You don't need to defer.
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
