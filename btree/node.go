package btree

type LeafNode struct {
	Keys    []string
	Values  []string
	Next    *LeafNode
	MaxKeys int
}

type IntermediateNode struct {
	Keys     []string
	Children []Node
	MaxKeys  int
}

type InsertionResult struct {
	Left     Node
	Right    Node
	SplitKey string
	Created  bool
}

type Node interface {
	Find(findKey string) (string, bool)
	Upsert(updateKey, value string) InsertionResult
	Split() (Node, Node, string)
}

func (node *IntermediateNode) Find(findKey string) (value string, ok bool) {
	idx := node.indexContaining(findKey)
	return node.Children[idx].Find(findKey)
}

func (node *LeafNode) Find(findKey string) (value string, ok bool) {
	for idx, key := range node.Keys {
		if findKey == key {
			return node.Values[idx], true
		}
	}
	return "", false
}

func (node *IntermediateNode) Upsert(updateKey, value string) InsertionResult {
	idx := node.indexContaining(updateKey)
	result := node.Children[idx].Upsert(updateKey, value)
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

func (node *LeafNode) Upsert(updateKey, value string) InsertionResult {
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
	right := LeafNode{
		Keys:    node.Keys[len(node.Keys)/2:],
		Values:  node.Values[len(node.Values)/2:],
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
	splitKey := node.Keys[len(node.Keys)/2]
	right := IntermediateNode{
		Keys:     node.Keys[len(node.Keys)/2+1:],
		Children: node.Children[(len(node.Children)+1)/2:],
		MaxKeys:  node.MaxKeys,
	}
	left := IntermediateNode{
		Keys:     node.Keys[:len(node.Keys)/2],
		Children: node.Children[:(len(node.Children)+1)/2],
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
