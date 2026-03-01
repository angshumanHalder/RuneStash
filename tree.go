package main

import (
	"bytes"
	"sort"
)

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
		return bytes.Compare(key, node.getKey(uint16(i))) > 0
	})
	return uint16(found - 1)
}

func nodeAppendRange(newNode, oldNode BNode, dstNew, srcOld, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(newNode, dst, oldNode.getPtr(srcOld), oldNode.getKey(src), oldNode.getVal(src))
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
		return old.nBytes() - leftBytes()
	}
	for rightBytes() > BTreePageSize {
		nLeft++
	}
	if nLeft >= old.nKeys() {
		panic("nodeSplit2: no split occurred")
	}
	nRight := old.nKeys() - nLeft

	// new nodes
	left.setHeader(BNodeNode, nLeft)
	right.setHeader(BNodeNode, nRight)
	nodeAppendRange(left, old, 0, 0, nLeft)
	nodeAppendRange(right, old, 0, nLeft, nRight)

	if right.nBytes() > BTreePageSize {
		panic("nodeSplit2: right node is too big!")
	}
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nKeys() <= BTreePageSize {
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
