package btree

// Node for a B-tree
type Node struct {
	keys     []string
	children []*Node
	values   []string
	numKeys  int
}

func (node *Node) find(findKey string) (value string, ok bool) {
	if node.children == nil {
		return node.leafFind(findKey)
	}
	return node.intermediateFind(findKey)
}

func (node *Node) intermediateFind(findKey string) (value string, ok bool) {
	index := 0
	for index < len(node.keys) && findKey < node.keys[index] {
		index++
	}
	return node.children[index].find(findKey)
}

func (node *Node) leafFind(findKey string) (value string, ok bool) {
	for i, key := range node.keys {
		if findKey == key {
			return node.values[i], true
		}
	}
	return "", false
}
