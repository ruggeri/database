package btree

// Node for a B-tree
type Node struct {
	Keys     []string
	Children []*Node
	Values   []string
	Next     *Node
	MaxKeys  int
}

type InsertionResult struct {
	Left    *Node
	Right   *Node
	Created bool
}

func (node *Node) Find(findKey string) (value string, ok bool) {
	if node.Children == nil {
		return node.leafFind(findKey)
	}
	return node.intermediateFind(findKey)
}

func (node *Node) intermediateFind(findKey string) (value string, ok bool) {
	idx := node.indexContaining(findKey)
	return node.Children[idx].Find(findKey)
}

func (node *Node) leafFind(findKey string) (value string, ok bool) {
	for idx, key := range node.Keys {
		if findKey == key {
			return node.Values[idx], true
		}
	}
	return "", false
}

func (node *Node) Upsert(updateKey, value string) InsertionResult {
	if node.Children == nil {
		return node.leafUpsert(updateKey, value)
	}
	return node.intermediateUpsert(updateKey, value)
}

func (node *Node) intermediateUpsert(updateKey, value string) InsertionResult {
	idx := node.indexContaining(updateKey)
	result := node.Children[idx].Upsert(updateKey, value)
	if result.Left == nil {
		return result
	}
	node.Keys = insert(node.Keys, idx, result.Right.Keys[0])
	node.Children[idx] = result.Left
	node.Children = insertNode(node.Children, idx+1, result.Right)
	if len(node.Keys) > node.MaxKeys {
		left, right := node.split()
		return InsertionResult{left, right, result.Created}
	}
	return InsertionResult{nil, nil, result.Created}
}

func (node *Node) leafUpsert(updateKey, value string) InsertionResult {
	idx := 0
	for idx < len(node.Keys) && updateKey > node.Keys[idx] {
		idx++
	}
	if idx != len(node.Keys) && updateKey == node.Keys[idx] {
		node.Values[idx] = value
		return InsertionResult{nil, nil, false}
	}
	node.Keys = insert(node.Keys, idx, updateKey)
	node.Values = insert(node.Values, idx, value)
	if len(node.Keys) > node.MaxKeys {
		left, right := node.split()
		return InsertionResult{left, right, true}
	}
	return InsertionResult{nil, nil, true}
}

func (node *Node) indexContaining(findKey string) int {
	idx := 0
	for idx < len(node.Keys) && findKey >= node.Keys[idx] {
		idx++
	}
	return idx
}

func (node *Node) split() (*Node, *Node) {
	right := Node{
		Keys:     node.Keys[len(node.Keys)/2:],
		Children: node.Children[len(node.Children)/2:],
		Values:   node.Values[len(node.Values)/2:],
		Next:     node.Next,
		MaxKeys:  node.MaxKeys,
	}
	left := Node{
		Keys:     node.Keys[:len(node.Keys)/2],
		Children: node.Children[:len(node.Children)/2],
		Values:   node.Values[:len(node.Values)/2],
		Next:     &right,
		MaxKeys:  node.MaxKeys,
	}
	return &left, &right
}

func insert(slice []string, idx int, value string) []string {
	slice = append(slice, "")
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = value
	return slice
}

func insertNode(slice []*Node, idx int, node *Node) []*Node {
	slice = append(slice, nil)
	copy(slice[idx+1:], slice[idx:])
	slice[idx] = node
	return slice
}
