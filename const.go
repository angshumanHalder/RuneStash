package main

const (
	BNodeNode = 1 // internal nodes without values
	BNodeLeaf = 2 // leaf nodes with values
)

const HeaderSize = 4
const BTreePageSize = 4096
const BTreeMaxKeySize = 1000
const BTreeMaxValSize = 3000
const DBSig = "RuneStash"
const FreeListHeader = 8
const FreeListCap = (BTreePageSize - FreeListHeader) / 8
