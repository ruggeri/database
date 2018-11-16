package main

import (
	"fmt"

	"github.com/Ian-MacLeod/database/btree"
)

func main() {
	c := btree.Node{
		Keys:     []string{"ca", "cb"},
		Children: nil,
		Values:   []string{"val5", "val6"},
		Next:     nil,
	}
	b := btree.Node{
		Keys:     []string{"ba", "bb"},
		Children: nil,
		Values:   []string{"val3", "val4"},
		Next:     &c,
	}
	a := btree.Node{
		Keys:     []string{"aa", "ab"},
		Children: nil,
		Values:   []string{"val1", "val2"},
		Next:     &b,
	}
	root := btree.Node{
		Keys:     []string{"ba", "ca"},
		Children: []*btree.Node{&a, &b, &c},
		Values:   nil,
		Next:     nil,
	}
	fmt.Println(root.Find("aa"))
	fmt.Println(root.Find("ab"))
	fmt.Println(root.Find("ba"))
	fmt.Println(root.Find("bb"))
	fmt.Println(root.Find("ca"))
	fmt.Println(root.Find("cb"))
	fmt.Println(root.Find("abc"))

	tree := btree.NewBTree(3)

	tree.Upsert("a", "1")
	fmt.Println(tree.Root.Keys)
	fmt.Println(tree.Root.Children)
	tree.Upsert("b", "2")
	fmt.Println(tree.Root.Keys)
	fmt.Println(tree.Root.Children)
	tree.Upsert("c", "3")
	fmt.Println(tree.Root.Keys)
	fmt.Println(tree.Root.Children)
	tree.Upsert("d", "4")
	fmt.Println(tree.Root.Keys)
	fmt.Println(tree.Root.Children)
	tree.Upsert("e", "5")
	tree.Upsert("f", "6")
	fmt.Println(tree.Find("a"))
	fmt.Println(tree.Find("b"))
	fmt.Println(tree.Find("c"))
	fmt.Println(tree.Find("d"))
	fmt.Println(tree.Find("e"))
	fmt.Println(tree.Find("f"))
	fmt.Println(tree.Find("g"))
}
