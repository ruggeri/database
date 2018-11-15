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
}
