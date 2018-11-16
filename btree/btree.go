package btree

type BTree struct {
	Root    *Node
	maxKeys int
}

func NewBTree(maxKeys int) BTree {
	return BTree{
		Root:    &Node{MaxKeys: 3},
		maxKeys: maxKeys,
	}
}

func (tree *BTree) Find(key string) (value string, ok bool) {
	return tree.Root.Find(key)
}

func (tree *BTree) Upsert(key, value string) (created bool) {
	result := tree.Root.Upsert(key, value)
	if result.Left != nil {
		tree.Root = &Node{
			Keys:     []string{result.Right.Keys[0]},
			Children: []*Node{result.Left, result.Right},
			MaxKeys:  tree.maxKeys,
		}
	}
	return result.Created
}
