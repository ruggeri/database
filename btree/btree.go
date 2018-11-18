package btree

type BTree struct {
	Root    Node
	maxKeys int
}

func NewBTree(maxKeys int) BTree {
	return BTree{
		Root:    &LeafNode{MaxKeys: maxKeys},
		maxKeys: maxKeys,
	}
}

func (tree *BTree) Find(key string) (value string, ok bool) {
	return tree.Root.Find(key)
}

func (tree *BTree) Upsert(key, value string) (created bool) {
	result := tree.Root.Upsert(key, value)
	if result.Left != nil {
		tree.Root = &IntermediateNode{
			Keys:     []string{result.SplitKey},
			Children: []Node{result.Left, result.Right},
			MaxKeys:  tree.maxKeys,
		}
	}
	return result.Created
}
