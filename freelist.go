package main

import "encoding/binary"

type LNode []byte

type FreeList struct {
	get func(uint64) []byte
	new func([]byte) uint64
	set func(uint64) []byte

	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
	maxSeq   uint64 //saved "tail sequence" to prevent consuming newly added items
}

func (l LNode) getNext() uint64 {
	return binary.LittleEndian.Uint64(l[0:8])
}
func (l LNode) setNext(next uint64) {
	binary.LittleEndian.PutUint64(l[0:8], next)
}
func (l LNode) getPtr(idx int) uint64 {
	offset := FreeListHeader + idx*8
	return binary.LittleEndian.Uint64(l[offset : offset+8])
}
func (l LNode) setPtr(idx int, ptr uint64) {
	offset := FreeListHeader + idx*8
	binary.LittleEndian.PutUint64(l[offset:offset+8], ptr)
}

// return 0 on failure
func (fl *FreeList) PopHead() uint64 {
	ptr, head := fl.flPop()
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) PushTail(ptr uint64) {
	LNode(fl.set(fl.tailPage)).setPtr(seqToIdx(fl.tailSeq), ptr)
	fl.tailSeq++
	if seqToIdx(fl.tailSeq) == 0 {
		next, head := fl.flPop()
		if next == 0 {
			next = fl.new(make([]byte, BTreePageSize))
		}

		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}
func (fl *FreeList) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

func seqToIdx(s uint64) int {
	return int(s % FreeListCap)
}

func (fl *FreeList) flPop() (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0 // cannot advance
	}

	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seqToIdx(fl.headSeq))
	fl.headSeq++

	if seqToIdx(fl.headSeq) == 0 {
		head, fl.headPage = fl.headPage, node.getNext()
		if fl.headPage == 0 {
			panic("RuneStash freelist corruption: next page pointer cannot be 0")
		}
	}
	return
}
