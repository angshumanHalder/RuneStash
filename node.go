package main

import "encoding/binary"

/* Node structure
HEADER = type + nKeys
| type | nKeys | pointers | offsets | key-values | unused |
| 2B | 2B | nKeys × 8B | nKeys × 2B | ... | |

KV Pair
| key_size | val_size | key | val |
| 2B | 2B | ... | ... |
*/

type BNode []byte // directly dump to disk

// GETTERS
func (n BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(n[0:2])
}

func (n BNode) nKeys() uint16 {
	return binary.LittleEndian.Uint16(n[2:4])
}

// read child pointers array
func (n BNode) getPtr(idx uint16) uint64 {
	if n.bType() != BNodeNode {
		panic("getPtr: cannot get pointer from Leaf Node!")
	}
	if idx >= n.nKeys() {
		panic("getPtr: index out of range")
	}
	return binary.LittleEndian.Uint64(n[HeaderSize+8*idx:])
}

// read the 'offsets' array
func (n BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := HeaderSize + 8*n.nKeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(n[pos:])
}

func (n BNode) kvPos(idx uint16) uint16 {
	if idx > n.nKeys() {
		panic("kvPos: index out of range")
	}
	return HeaderSize + 8*n.nKeys() + 2*n.nKeys() + n.getOffset(idx)
}

func (n BNode) getKey(idx uint16) []byte {
	if idx >= n.nKeys() {
		panic("getKey: index out of range")
	}
	pos := n.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(n[pos:])
	return n[pos+4:][:kLen]
}

func (n BNode) getVal(idx uint16) []byte {
	if idx >= n.nKeys() {
		panic("getVal: index out of range")
	}
	pos := n.kvPos(idx)
	kLen := binary.LittleEndian.Uint16(n[pos:])
	vLen := binary.LittleEndian.Uint16(n[pos+2:])
	return n[pos+4+kLen:][:vLen]
}

// SETTER
func (n BNode) setHeader(bType, nKeys uint16) {
	binary.LittleEndian.PutUint16(n[0:2], bType)
	binary.LittleEndian.PutUint16(n[2:4], nKeys)
}

func (n BNode) setPtr(idx uint16, val uint64) {
	if n.bType() != BNodeNode {
		panic("setPtr: cannot set pointer from Leaf Node!")
	}
	if idx >= n.nKeys() {
		panic("setPtr: index out of range")
	}
	binary.LittleEndian.PutUint64(n[HeaderSize+8*idx:], val)
}

func (n BNode) setOffset(idx uint16, val uint16) {
	if idx == 0 {
		return
	}

	pos := HeaderSize + 8*n.nKeys() + 2*(idx-1)
	binary.LittleEndian.PutUint16(n[pos:], val)
}

// assumes KVs are added in order
func nodeAppendKV(newNode BNode, idx uint16, ptr uint64, key, val []byte) {
	// ptrs
	if newNode.bType() == BNodeNode {
		newNode.setPtr(idx, ptr)
	}

	// KVs
	pos := newNode.kvPos(idx) // uses the offset value of the previous key

	// 4-byte KV sizes
	binary.LittleEndian.PutUint16(newNode[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(newNode[pos+2:], uint16(len(val)))

	// KV data
	copy(newNode[pos+4:], key)
	copy(newNode[pos+4+uint16(len(key)):], val)

	newNode.setOffset(idx+1, newNode.getOffset(idx)+4+uint16(len(key)+len(val)))
}
