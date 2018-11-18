package main

import (
	"fmt"

	"github.com/Ian-MacLeod/database/btree"
)

func main() {
	tree := btree.NewBTree(3)

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	values := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	for i := 0; i < 10; i++ {
		tree.Upsert(keys[i], values[i])
	}
	for i := 0; i < 10; i++ {
		fmt.Println(tree.Find(keys[i]))
	}
	fmt.Println(tree.Find("ea"))
	fmt.Println(tree.Find("da"))
	fmt.Println(tree.Find(""))
	fmt.Println(tree.Find("k"))
}
