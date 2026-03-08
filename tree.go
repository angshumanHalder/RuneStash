package main

import (
	"bytes"
	"errors"
	"sort"
)

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

func (tree *BTree) Insert(key, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err
	}

	if tree.root == 0 {
		root := BNode(make([]byte, BTreePageSize))
		root.setHeader(BNodeLeaf, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return nil
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nSplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nSplit > 1 {
		root := BNode(make([]byte, BTreePageSize))
		root.setHeader(BNodeNode, nSplit)
		for i, kNode := range split[:nSplit] {
			ptr, k := tree.new(kNode), kNode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, k, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}

	return nil
}

func (tree *BTree) Delete(key []byte) (bool, error) {
	if err := checkLimit(key, nil); err != nil {
		return false, err
	}

	if tree.root == 0 {
		return false, nil
	}

	updated := treeDelete(tree, tree.get(tree.root), key)

	if len(updated) == 0 {
		return false, nil
	}

	tree.del(tree.root)

	if updated.bType() == BNodeNode && updated.nKeys() == 1 {
		tree.root = updated.getPtr(0)
	} else if updated.bType() == BNodeLeaf && updated.nKeys() == 1 {
		tree.root = 0
	} else {
		tree.root = tree.new(updated)
	}

	return true, nil
}

func (tree *BTree) Get(key []byte) ([]byte, bool) {
	if tree.root == 0 {
		return nil, false
	}
	return treeGet(tree, tree.get(tree.root), key)
}

func checkLimit(key, val []byte) error {
	if len(key) == 0 {
		return errors.New("key is empty")
	}

	if len(key) > BTreeMaxKeySize {
		return errors.New("key is too long")
	}

	if len(val) > BTreeMaxValSize {
		return errors.New("value is too long")
	}

	return nil
}

func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	newNode := BNode(make([]byte, 2*BTreePageSize))
	idx := nodeLookupLE(node, key)
	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(newNode, node, idx, key, val)
		} else {
			leafInsert(newNode, node, idx+1, key, val)
		}
	case BNodeNode:
		kPtr := node.getPtr(idx)
		kNode := treeInsert(tree, tree.get(kPtr), key, val)
		nSplit, split := nodeSplit3(kNode)
		tree.del(kPtr)
		nodeReplaceKidN(tree, newNode, node, idx, split[:nSplit]...)
	default:
		panic("bad node type:")
	}
	return newNode
}

func nodeReplaceKidN(tree *BTree, newNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	newNode.setHeader(BNodeNode, old.nKeys()+inc-1)
	nodeAppendRange(newNode, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(newNode, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(newNode, old, idx+inc, idx+1, old.nKeys()-(idx+1))
}

func leafInsert(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNodeLeaf, oldNode.nKeys()+1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx, oldNode.nKeys()-idx)
}

func leafUpdate(newNode, oldNode BNode, idx uint16, key, val []byte) {
	newNode.setHeader(BNodeLeaf, oldNode.nKeys())
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, 0, key, val)
	nodeAppendRange(newNode, oldNode, idx+1, idx+1, oldNode.nKeys()-(idx+1))
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	nKeys := node.nKeys()
	found := sort.Search(int(nKeys), func(i int) bool {
		return bytes.Compare(node.getKey(uint16(i)), key) > 0
	})
	if found > 0 {
		return uint16(found - 1)
	}
	return 0
}

func nodeAppendRange(newNode, oldNode BNode, dstNew, srcOld, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		var ptr uint64
		if oldNode.bType() == BNodeNode {
			ptr = oldNode.getPtr(src)
		}
		nodeAppendKV(newNode, dst, ptr, oldNode.getKey(src), oldNode.getVal(src))
	}
}

// Split an oversized node into 2 nodes.
func nodeSplit2(left, right, old BNode) {
	if old.nKeys() < 2 {
		panic("nodeSplit2: cannot split a node with less than 2 keys!")
	}
	nLeft := old.nKeys() / 2
	leftBytes := func() uint16 {
		return HeaderSize + 8*nLeft + 2*nLeft + old.getOffset(nLeft)
	}
	for leftBytes() > BTreePageSize {
		nLeft--
	}
	if nLeft < 1 {
		panic("nodeSplit2: cannot split a node with less than 1 key!")
	}
	rightBytes := func() uint16 {
		return old.nBytes() - leftBytes() + HeaderSize
	}
	for rightBytes() > BTreePageSize {
		nLeft++
	}
	if nLeft >= old.nKeys() {
		panic("nodeSplit2: no split occurred")
	}
	nRight := old.nKeys() - nLeft

	// new nodes
	left.setHeader(old.bType(), nLeft)
	right.setHeader(old.bType(), nRight)
	nodeAppendRange(left, old, 0, 0, nLeft)
	nodeAppendRange(right, old, 0, nLeft, nRight)

	if right.nBytes() > BTreePageSize {
		panic("nodeSplit2: right node is too big!")
	}
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nBytes() <= BTreePageSize {
		old = old[:BTreePageSize]
		return 1, [3]BNode{old} // no split
	}
	left := BNode(make([]byte, 2*BTreePageSize))
	right := BNode(make([]byte, BTreePageSize))
	nodeSplit2(left, right, old)
	if left.nBytes() <= BTreePageSize {
		return 2, [3]BNode{left, right}
	}
	leftLeft := BNode(make([]byte, BTreePageSize))
	middle := BNode(make([]byte, BTreePageSize))
	nodeSplit2(leftLeft, middle, left)
	if leftLeft.nBytes() > BTreePageSize {
		panic("nodeSplit3: leftLeft node is still too big!")
	}
	return 3, [3]BNode{leftLeft, middle, right}
}

func leafDelete(newNode, oldNode BNode, idx uint16) {
	newNode.setHeader(BNodeLeaf, oldNode.nKeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendRange(newNode, oldNode, idx, idx+1, oldNode.nKeys()-(idx+1))
}

func nodeMerge(newNode, left, right BNode) {
	newNode.setHeader(left.bType(), left.nKeys()+right.nKeys())
	nodeAppendRange(newNode, left, 0, 0, left.nKeys())
	nodeAppendRange(newNode, right, left.nKeys(), 0, right.nKeys())
}

func nodeReplace2Kid(newNode, oldNode BNode, idx uint16, ptr uint64, key []byte) {
	newNode.setHeader(BNodeNode, oldNode.nKeys()-1)
	nodeAppendRange(newNode, oldNode, 0, 0, idx)
	nodeAppendKV(newNode, idx, ptr, key, nil)
	nodeAppendRange(newNode, oldNode, idx+1, idx+2, oldNode.nKeys()-(idx+2))
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nBytes() > BTreePageSize/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nBytes() + updated.nBytes() - HeaderSize
		if merged <= BTreePageSize {
			return -1, sibling // left
		}
	}

	if idx+1 < node.nKeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nBytes() + updated.nBytes() - HeaderSize
		if merged <= BTreePageSize {
			return 1, sibling
		}
	}

	return 0, BNode{}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	kPtr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kPtr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kPtr)
	// check for merging
	newNode := BNode(make([]byte, BTreePageSize))
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTreePageSize))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(newNode, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTreePageSize))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(newNode, node, idx, tree.new(merged), merged.getKey(0))
	default:
		if updated.nKeys() == 0 {
			if !(node.nKeys() == 1 && idx == 0) {
				panic("nodeDelete: cannot delete node. No empty child!")
			}
			newNode.setHeader(BNodeNode, 0) // the parent becomes empty too
		} else if updated.nKeys() > 0 {
			nodeReplaceKidN(tree, newNode, node, idx, updated)
		}
	}
	return newNode
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)
	switch node.bType() {
	case BNodeLeaf:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		newNode := BNode(make([]byte, BTreePageSize))
		leafDelete(newNode, node, idx)
		return newNode
	case BNodeNode:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("bad node type")
	}
}

func treeGet(tree *BTree, nodeData []byte, key []byte) ([]byte, bool) {
	node := BNode(nodeData)
	idx := nodeLookupLE(node, key)
	switch node.bType() {
	case BNodeLeaf:
		if bytes.Equal(key, node.getKey(idx)) {
			return node.getVal(idx), true
		}
		return nil, false
	case BNodeNode:
		return treeGet(tree, tree.get(node.getPtr(idx)), key)
	default:
		panic("bad node type")
	}
}
