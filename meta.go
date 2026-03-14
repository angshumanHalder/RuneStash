package main

import "encoding/binary"

type Meta struct {
	Root            uint64
	Flushed         uint64
	FreeListHead    uint64
	FreeListTail    uint64
	FreeListHeadSeq uint64
	FreeListTailSeq uint64
}

func (m *Meta) save() []byte {
	var data [64]byte
	copy(data[:16], DBSig)
	binary.LittleEndian.PutUint64(data[16:], m.Root)
	binary.LittleEndian.PutUint64(data[24:], m.Flushed)
	binary.LittleEndian.PutUint64(data[32:], m.FreeListHead)
	binary.LittleEndian.PutUint64(data[40:], m.FreeListTail)
	binary.LittleEndian.PutUint64(data[48:], m.FreeListHeadSeq)
	binary.LittleEndian.PutUint64(data[56:], m.FreeListTailSeq)
	return data[:]
}

func (m *Meta) load(data []byte) {
	if string(data[:len(DBSig)]) != DBSig {
		panic("loadMeta: invalid signature")
	}
	m.Root = binary.LittleEndian.Uint64(data[16:])
	m.Flushed = binary.LittleEndian.Uint64(data[24:])
	m.FreeListHead = binary.LittleEndian.Uint64(data[32:])
	m.FreeListTail = binary.LittleEndian.Uint64(data[40:])
	m.FreeListHeadSeq = binary.LittleEndian.Uint64(data[48:])
	m.FreeListTailSeq = binary.LittleEndian.Uint64(data[56:])
}
