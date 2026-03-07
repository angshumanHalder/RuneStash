package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			root: 0,
			get: func(ptr uint64) []byte {
				node, ok := pages[ptr]
				if !ok {
					return nil
				}
				return node
			},
			new: func(node []byte) uint64 {
				if BNode(node).nBytes() > BTreePageSize {
					panic("new: node is too big")
				}
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				if pages[ptr] != nil {
					panic("new: page already exists")
				}
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				if pages[ptr] == nil {
					panic("del: page does not exist")
				}
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) treeWalk(ptr uint64, extracted map[string]string) {
	node := BNode(c.tree.get(ptr))

	if node.nBytes() > BTreePageSize {
		panic(fmt.Sprintf("Structural Error: Node %d size (%d) exceeds page size!", ptr, node.nBytes()))
	}

	nKeys := node.nKeys()
	for i := uint16(1); i < nKeys; i++ {
		k1 := node.getKey(i - 1)
		k2 := node.getKey(i)
		if bytes.Compare(k1, k2) >= 0 {
			panic(fmt.Sprintf("Structural Error: Keys in node %d are not sorted!", ptr))
		}
	}

	if node.bType() == BNodeLeaf {
		for i := uint16(0); i < nKeys; i++ {
			k := node.getKey(i)
			v := node.getVal(i)

			if len(k) == 0 {
				continue
			}
			extracted[string(k)] = string(v)
		}
	} else if node.bType() == BNodeNode {
		for i := uint16(0); i < nKeys; i++ {
			kidPtr := node.getPtr(i)
			c.treeWalk(kidPtr, extracted)
		}
	} else {
		panic(fmt.Sprintf("Structural Error: Node %d has unknown type!", ptr))
	}
}

func (c *C) Verify() {
	if c.tree.root == 0 {
		if len(c.ref) > 0 {
			panic("Data Mismatch: Tree is empty but reference map has data!")
		}
		return
	}

	extracted := map[string]string{}
	c.treeWalk(c.tree.root, extracted)
	if len(extracted) != len(c.ref) {
		panic(fmt.Sprintf("Data Mismatch: Tree has %d items, Ref has %d items", len(extracted), len(c.ref)))
	}
	for k, v := range c.ref {
		treeVal, exists := extracted[k]
		if !exists {
			panic(fmt.Sprintf("Data Mismatch: Key '%s' is in Ref but missing from Tree", k))
		}
		if treeVal != v {
			panic(fmt.Sprintf("Data Mismatch: For key '%s', Tree='%s', Ref='%s'", k, treeVal, v))
		}
	}
}

func randString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = byte(rand.Intn(94) + 32)
	}
	return string(b)
}

func (c *C) Add(key, val string) {
	err := c.tree.Insert([]byte(key), []byte(val))
	if err != nil {
		panic(err)
	}
	c.ref[key] = val
	c.Verify()
}

func (c *C) Del(key string) {
	_, err := c.tree.Delete([]byte(key))
	if err != nil {
		panic(err)
	}
	delete(c.ref, key)
	c.Verify()
}

func TestBTreeRandomOperations(t *testing.T) {
	c := newC()
	var keys []string
	t.Log("Phase 1: Randomized Insertions (Forcing Splits)")
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("key_%05d_%d", i, rand.Intn(100000))
		valLen := 32
		if i%10 == 0 {
			valLen = 2000
		}
		val := randString(valLen)
		keys = append(keys, key)
		c.Add(key, val)
	}

	t.Log("Phase 2: Updating existing Keys")
	for i := 0; i < 1000; i++ {
		idx := rand.Intn(len(keys))
		key := keys[idx]
		newVal := randString(rand.Intn(1000) + 10)
		c.Add(key, newVal)
	}

	t.Log("Phase 3: Randomized Deletions (Forcing Merges)")
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	for _, key := range keys {
		c.Del(key)
	}
	t.Log("Phase 4: Final Verification")
	if c.tree.root != 0 {
		t.Fatalf("Expected tree root to be 0 after deleting all keys, got %d", c.tree.root)
	}
	if len(c.pages) != 0 {
		t.Fatalf("Expected mock disk to be empty, but %d pages were leaked!", len(c.pages))
	}
}
