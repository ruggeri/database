package btree

import (
	"sync"
)

type LeafNode struct {
	Keys    []string
	Values  []string
	Next    *LeafNode
	MaxKeys int
	Mux     sync.RWMutex
}

type IntermediateNode struct {
	Keys     []string
	Children []Node
	MaxKeys  int
	Mux      sync.RWMutex
}

type InsertionResult struct {
	Left     Node
	Right    Node
	SplitKey string
	Created  bool
}

type Node interface {
	Find(findKey string, parentMux *sync.RWMutex) (string, bool)
	LockedUpsert(updateKey, value string) InsertionResult
	Split() (Node, Node, string)
	GetMux() *sync.RWMutex
	AcquireLockContext(updateKey string, lockContext *LockContext)
	CheckAncestor(updateKey string, ancestor LockedUpserter, parentMux *sync.RWMutex) bool
}

type LockedUpserter interface {
	LockedUpsert(updateKey, value string) InsertionResult
	GetMux() *sync.RWMutex
}

type LockContext struct {
	Muxes          []*sync.RWMutex
	StableAncestor LockedUpserter
}

func (ctx *LockContext) UnlockAll() {
	for _, mux := range ctx.Muxes {
		mux.RUnlock()
	}
	ctx.Muxes = ctx.Muxes[:0]

	ctx.StableAncestor = nil
}

func (ctx *LockContext) Add(node LockedUpserter) {
	ctx.Muxes = append(ctx.Muxes, node.GetMux())

	if ctx.StableAncestor == nil {
		ctx.StableAncestor = node
	}
}

func (ctx *LockContext) Upgrade() LockedUpserter {
	for _, mux := range ctx.Muxes {
		mux.RUnlock()
	}
	ctx.StableAncestor.GetMux().Lock()
	return ctx.StableAncestor
}

func (node *IntermediateNode) GetMux() *sync.RWMutex {
	return &node.Mux
}

func (node *LeafNode) GetMux() *sync.RWMutex {
	return &node.Mux
}

func (node *IntermediateNode) Find(findKey string, parentMux *sync.RWMutex) (value string, ok bool) {
	node.Mux.RLock()
	parentMux.RUnlock()
	idx := node.indexContaining(findKey)
	return node.Children[idx].Find(findKey, &node.Mux)
}

func (node *LeafNode) Find(findKey string, parentMux *sync.RWMutex) (value string, ok bool) {
	node.Mux.RLock()
	defer node.Mux.RUnlock()
	parentMux.RUnlock()
	for idx, key := range node.Keys {
		if findKey == key {
			return node.Values[idx], true
		}
	}
	return "", false
}

func (node *IntermediateNode) CheckAncestor(updateKey string, ancestor LockedUpserter, parentMux *sync.RWMutex) bool {
	if node == ancestor {
		parentMux.RUnlock()
		return true
	}

	node.Mux.RLock()
	parentMux.RUnlock()

	idx := node.indexContaining(updateKey)
	return node.Children[idx].CheckAncestor(updateKey, ancestor, &node.Mux)
}

func (node *LeafNode) CheckAncestor(updateKey string, ancestor LockedUpserter, parentMux *sync.RWMutex) bool {
	parentMux.RUnlock()
	return node == ancestor
}

func (node *IntermediateNode) AcquireLockContext(updateKey string, lockContext *LockContext) {
	node.Mux.RLock()
	if len(node.Keys) < node.MaxKeys {
		lockContext.UnlockAll()
	}
	lockContext.Add(node)

	idx := node.indexContaining(updateKey)
	node.Children[idx].AcquireLockContext(updateKey, lockContext)
}

func (node *LeafNode) AcquireLockContext(updateKey string, lockContext *LockContext) {
	node.Mux.RLock()
	if len(node.Keys) < node.MaxKeys {
		lockContext.UnlockAll()
	}
	lockContext.Add(node)
}

func (node *IntermediateNode) LockedUpsert(updateKey, value string) InsertionResult {
	idx := node.indexContaining(updateKey)
	child := node.Children[idx]
	child.GetMux().Lock()
	defer child.GetMux().Unlock()
	result := child.LockedUpsert(updateKey, value)
	if result.Left == nil {
		return result
	}
	node.Keys = insert(node.Keys, idx, result.SplitKey)
	node.Children[idx] = result.Left
	node.Children = insertNode(node.Children, idx+1, result.Right)
	if len(node.Keys) > node.MaxKeys {
		left, right, splitKey := node.Split()
		return InsertionResult{left, right, splitKey, result.Created}
	}
	return InsertionResult{Created: result.Created}
}

func (node *LeafNode) LockedUpsert(updateKey, value string) InsertionResult {
	idx := 0
	for idx < len(node.Keys) && updateKey > node.Keys[idx] {
		idx++
	}
	if idx != len(node.Keys) && updateKey == node.Keys[idx] {
		node.Values[idx] = value
		return InsertionResult{Created: false}
	}
	node.Keys = insert(node.Keys, idx, updateKey)
	node.Values = insert(node.Values, idx, value)
	if len(node.Keys) > node.MaxKeys {
		left, right, splitKey := node.Split()
		return InsertionResult{left, right, splitKey, true}
	}
	return InsertionResult{Created: true}
}

func (node *IntermediateNode) indexContaining(findKey string) int {
	idx := 0
	for idx < len(node.Keys) && findKey >= node.Keys[idx] {
		idx++
	}
	return idx
}

func (node *LeafNode) Split() (Node, Node, string) {
	rightKeys := make([]string, len(node.Keys)-len(node.Keys)/2)
	copy(rightKeys, node.Keys[len(node.Keys)/2:])
	rightValues := make([]string, len(node.Values)-len(node.Values)/2)
	copy(rightValues, node.Values[len(node.Values)/2:])
	right := LeafNode{
		Keys:    rightKeys,
		Values:  rightValues,
		Next:    node.Next,
		MaxKeys: node.MaxKeys,
	}
	left := LeafNode{
		Keys:    node.Keys[:len(node.Keys)/2],
		Values:  node.Values[:len(node.Values)/2],
		Next:    &right,
		MaxKeys: node.MaxKeys,
	}
	return &left, &right, right.Keys[0]
}

func (node *IntermediateNode) Split() (Node, Node, string) {
	medianIndex := len(node.Keys) / 2
	splitKey := node.Keys[medianIndex]
	rightKeys := make([]string, len(node.Keys)-medianIndex-1)
	copy(rightKeys, node.Keys[medianIndex+1:])
	rightChildren := make([]Node, len(node.Children)-medianIndex-1)
	copy(rightChildren, node.Children[medianIndex+1:])
	right := IntermediateNode{
		Keys:     rightKeys,
		Children: rightChildren,
		MaxKeys:  node.MaxKeys,
	}
	left := IntermediateNode{
		Keys:     node.Keys[:medianIndex],
		Children: node.Children[:medianIndex+1],
		MaxKeys:  node.MaxKeys,
	}
	return &left, &right, splitKey
}

func insert(slice []string, idx int, value string) []string {
	slice = append(slice, "")
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = value
	return slice
}

func insertNode(slice []Node, idx int, node Node) []Node {
	slice = append(slice, nil)
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = node
	return slice
}
