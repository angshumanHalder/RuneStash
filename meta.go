package main

import "encoding/binary"

type Meta struct {
	Root    uint64
	Flushed uint64
}

func (m *Meta) save() []byte {
	var data [32]byte
	copy(data[:16], DBSig)
	binary.LittleEndian.PutUint64(data[16:], m.Root)
	binary.LittleEndian.PutUint64(data[24:], m.Flushed)
	return data[:]
}

func (m *Meta) load(data []byte) {
	if string(data[:len(DBSig)]) != DBSig {
		panic("loadMeta: invalid signature")
	}
	m.Root = binary.LittleEndian.Uint64(data[16:])
	m.Flushed = binary.LittleEndian.Uint64(data[24:])
}
