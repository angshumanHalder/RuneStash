package main

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

type UpdateReq struct {
	tree    *BTree
	Added   bool
	Updated bool
	Old     []byte
	Key     []byte
	Val     []byte
	Mode    int
}

type BIter struct {
	tree *BTree
	path []BNode
	pos  []uint16
}

type CombinedIter struct {
	top *BIter // KvTX.pending  (writes buffered in this TX)
	bot *BIter // KvTX.snapshot (frozen base state)
}

func NewCombinedIter(top, bot *BIter) *CombinedIter {
	ci := &CombinedIter{top: top, bot: bot}
	ci.skipDeleted()
	return ci
}

func (ci *CombinedIter) topKey() []byte {
	if !ci.top.Valid() {
		return nil
	}
	k, _ := ci.top.Deref()
	return k
}

func (ci *CombinedIter) botKey() []byte {
	if !ci.bot.Valid() {
		return nil
	}
	k, _ := ci.bot.Deref()
	return k
}

func (ci *CombinedIter) compare() int {
	tk := ci.topKey()
	bk := ci.botKey()
	if tk == nil && bk == nil {
		return 0
	}
	if tk == nil {
		return 1
	}
	if bk == nil {
		return -1
	}
	return bytes.Compare(tk, bk)
}

func (ci *CombinedIter) skipDeleted() {
	for ci.top.Valid() {
		_, v := ci.top.Deref()
		if v[0] != FlagDeleted {
			break // top points at a real (non-deleted) entry
		}
		// consume this tombstone; if bot has the same key, skip it too
		if cmp := ci.compare(); cmp == 0 {
			ci.bot.Next()
		}
		ci.top.Next()
	}
}

func (ci *CombinedIter) Valid() bool {
	return ci.top.Valid() || ci.bot.Valid()
}

func (ci *CombinedIter) Deref() ([]byte, []byte) {
	cmp := ci.compare()
	if cmp <= 0 {
		k, v := ci.top.Deref()
		return k, v[1:] // strip FlagUpdated
	}
	return ci.bot.Deref()
}

func (ci *CombinedIter) Next() {
	cmp := ci.compare()
	switch {
	case cmp < 0:
		ci.top.Next()
	case cmp > 0:
		ci.bot.Next()
	default: // same key in both: advance both
		ci.top.Next()
		ci.bot.Next()
	}
	ci.skipDeleted()
}

func (tree *BTree) commitRoot(node BNode) {
	tree.del(tree.root)
	nSplit, split := nodeSplit3(node)
	if nSplit > 1 {
		root := BNode(make([]byte, BTreePageSize))
		root.setHeader(BNodeNode, nSplit)
		for i, kNode := range split[:nSplit] {
			nodeAppendKV(root, uint16(i), tree.new(kNode), kNode.getKey(0), nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
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
	tree.commitRoot(node)
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

func (tree *BTree) Update(req *UpdateReq) (bool, error) {
	if tree.root == 0 {
		if req.Mode == ModeUpdateOnly {
			return false, fmt.Errorf("update error: row does not exists")
		}
		root := BNode(make([]byte, BTreePageSize))
		root.setHeader(BNodeLeaf, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, req.Key, req.Val)
		tree.root = tree.new(root)
		req.Added = true
		return true, nil
	}
	newRoot, err := treeUpdate(tree, tree.get(tree.root), req)
	if err != nil {
		return false, err
	}
	tree.commitRoot(newRoot)
	return true, nil

}

func (iter *BIter) Valid() bool {
	if len(iter.path) == 0 {
		return false
	}
	last := len(iter.path) - 1
	pos := iter.pos[last]
	return pos < iter.path[last].nKeys() && len(iter.path[last].getKey(pos)) > 0
}

func (iter *BIter) Deref() ([]byte, []byte) {
	leaf := iter.path[len(iter.path)-1]
	pos := iter.pos[len(iter.pos)-1]
	return leaf.getKey(pos), leaf.getVal(pos)
}

func (tree *BTree) SeekLE(key []byte) *BIter {
	iter := &BIter{tree: tree}
	for ptr := tree.root; ptr != 0; {
		node := tree.get(ptr)
		idx := nodeLookupLE(node, key)
		iter.path = append(iter.path, node)
		iter.pos = append(iter.pos, idx)
		if BNode(node).bType() == BNodeLeaf {
			break
		}
		ptr = BNode(node).getPtr(idx)
	}
	return iter
}

func (tree *BTree) Seek(key []byte, cmp int) *BIter {
	if tree.root == 0 {
		return &BIter{tree: tree} // empty tree: Valid() == false
	}
	iter := tree.SeekLE(key)
	leaf := iter.path[len(iter.path)-1]
	cur := leaf.getKey(iter.pos[len(iter.pos)-1])
	cmpResult := bytes.Compare(cur, key)
	switch cmp {
	case CmpLT:
		if cmpResult == 0 {
			iter.Prev()
		}
	case CmpGE:
		if cmpResult < 0 {
			iter.Next()
		}
	case CmpGT:
		if cmpResult <= 0 {
			iter.Next()
		}
	}
	return iter
}

func (iter *BIter) Next() {
	iterNext(iter, len(iter.path)-1)
}

func (iter *BIter) Prev() {
	iterPrev(iter, len(iter.path)-1)
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

func treeUpdate(tree *BTree, node BNode, req *UpdateReq) (BNode, error) {
	newNode := BNode(make([]byte, 2*BTreePageSize))
	idx := nodeLookupLE(node, req.Key)
	switch node.bType() {
	case BNodeLeaf:
		exists := false
		if idx < node.nKeys() {
			exists = bytes.Equal(node.getKey(idx), req.Key)
		}
		// Enforce relations rules
		if req.Mode == ModeInsertOnly && exists {
			return nil, fmt.Errorf("duplicate key error: row already exists")
		}
		if req.Mode == ModeUpdateOnly && !exists {
			return nil, fmt.Errorf("update error: row does not exists")
		}
		if !exists {
			req.Added = true
			req.Updated = true
			leafInsert(newNode, node, idx+1, req.Key, req.Val)
		} else {
			req.Added = false
			req.Updated = true
			req.Old = node.getVal(idx)
			leafUpdate(newNode, node, idx, req.Key, req.Val)
		}
	case BNodeNode:
		kPtr := node.getPtr(idx)
		child := tree.get(kPtr)
		newChild, err := treeUpdate(tree, child, req)
		if err != nil {
			return nil, err
		}
		nSplit, split := nodeSplit3(newChild)
		tree.del(kPtr)
		nodeReplaceKidN(tree, newNode, node, idx, split[:nSplit]...)
	default:
		panic("bad node type")
	}

	return newNode, nil
}

func freeSubtree(tree *BTree, ptr uint64) {
	node := BNode(tree.get(ptr))
	if node.bType() == BNodeNode {
		for i := uint16(0); i < node.nKeys(); i++ {
			freeSubtree(tree, node.getPtr(i))
		}
	}
	tree.del(ptr)
}

func leafRangeDelete(newNode, old BNode, lo, hi []byte) {
	count := uint16(0)
	for i := uint16(0); i < old.nKeys(); i++ {
		k := old.getKey(i)
		if len(k) == 0 || bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) > 0 {
			count++
		}
	}
	newNode.setHeader(BNodeLeaf, count)
	dst := uint16(0)
	for i := uint16(0); i < old.nKeys(); i++ {
		k := old.getKey(i)
		if len(k) == 0 || bytes.Compare(k, lo) < 0 || bytes.Compare(k, hi) > 0 {
			nodeAppendKV(newNode, dst, 0, k, old.getVal(i))
			dst++
		}
	}
}

func nodeRangeDelete(tree *BTree, node BNode, lo, hi []byte) BNode {
	left := nodeLookupLE(node, lo)
	right := nodeLookupLE(node, hi)

	for i := left + 1; i < right; i++ {
		freeSubtree(tree, node.getPtr(i))
	}

	lPtr := node.getPtr(left)
	lChild := treeRangeDelete(tree, BNode(tree.get(lPtr)), lo, hi)
	tree.del(lPtr)

	var rChild BNode
	if right > left {
		rPtr := node.getPtr(right)
		rChild = treeRangeDelete(tree, BNode(tree.get(rPtr)), lo, hi)
		tree.del(rPtr)
	}

	var kids []BNode
	if lChild.nKeys() > 0 {
		kids = append(kids, lChild)
	}
	if right > left && rChild.nKeys() > 0 {
		if len(kids) > 0 && lChild.nBytes()+rChild.nBytes()-HeaderSize <= BTreePageSize {
			merged := BNode(make([]byte, BTreePageSize))
			nodeMerge(merged, kids[0], rChild)
			kids[0] = merged
		} else {
			kids = append(kids, rChild)
		}
	}

	nBefore := left
	nAfter := node.nKeys() - right - 1
	nNew := nBefore + uint16(len(kids)) + nAfter

	newNode := BNode(make([]byte, BTreePageSize))
	newNode.setHeader(BNodeNode, nNew)
	nodeAppendRange(newNode, node, 0, 0, nBefore)
	for i, kid := range kids {
		nodeAppendKV(newNode, nBefore+uint16(i), tree.new(kid), kid.getKey(0), nil)
	}
	if nAfter > 0 {
		nodeAppendRange(newNode, node, nBefore+uint16(len(kids)), right+1, nAfter)
	}
	return newNode
}

func treeRangeDelete(tree *BTree, node BNode, lo, hi []byte) BNode {
	switch node.bType() {
	case BNodeLeaf:
		newNode := BNode(make([]byte, BTreePageSize))
		leafRangeDelete(newNode, node, lo, hi)
		return newNode
	case BNodeNode:
		return nodeRangeDelete(tree, node, lo, hi)
	default:
		panic("bad node type")
	}
}

func (tree *BTree) RangeDelete(lo, hi []byte) error {
	if err := checkLimit(lo, nil); err != nil {
		return err
	}
	if err := checkLimit(hi, nil); err != nil {
		return err
	}
	if bytes.Compare(lo, hi) > 0 {
		return nil // empty range, nothing to do
	}
	if tree.root == 0 {
		return nil
	}

	updated := treeRangeDelete(tree, BNode(tree.get(tree.root)), lo, hi)
	tree.del(tree.root)

	switch {
	case updated.nKeys() == 0:
		tree.root = 0
	case updated.bType() == BNodeLeaf && updated.nKeys() == 1:
		tree.root = 0 // only the sentinel remains — tree is empty
	case updated.bType() == BNodeNode && updated.nKeys() == 1:
		tree.root = updated.getPtr(0) // collapse single-child internal node
	default:
		tree.root = tree.new(updated)
	}
	return nil
}

func iterNext(iter *BIter, level int) {
	if iter.pos[level]+1 < iter.path[level].nKeys() {
		iter.pos[level]++
	} else if level > 0 {
		iterNext(iter, level-1)
	} else {
		iter.pos[len(iter.pos)-1]++
		return
	}
	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = 0
	}
}

func iterPrev(iter *BIter, level int) {
	if iter.pos[level] > 0 {
		iter.pos[level]--
	} else if level > 0 {
		iterPrev(iter, level-1)
	} else {
		iter.pos[len(iter.pos)-1]--
		return
	}
	if level+1 < len(iter.pos) {
		node := iter.path[level]
		kid := BNode(iter.tree.get(node.getPtr(iter.pos[level])))
		iter.path[level+1] = kid
		iter.pos[level+1] = kid.nKeys() - 1
	}
}
