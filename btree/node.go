package btree

// Node for a B-tree
type Node struct {
	Keys     []string
	Children []*Node
	Values   []string
	Next     *Node
}

func (node *Node) Find(findKey string) (value string, ok bool) {
	if node.Children == nil {
		return node.LeafFind(findKey)
	}
	return node.IntermediateFind(findKey)
}

func (node *Node) IntermediateFind(findKey string) (value string, ok bool) {
	index := 0
	for index < len(node.Keys) && findKey >= node.Keys[index] {
		index++
	}
	return node.Children[index].Find(findKey)
}

func (node *Node) LeafFind(findKey string) (value string, ok bool) {
	for i, key := range node.Keys {
		if findKey == key {
			return node.Values[i], true
		}
	}
	return "", false
}
