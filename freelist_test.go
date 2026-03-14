package main

import "testing"

type mockPager struct {
	pages map[uint64][]byte
	next  uint64
}

func newMockPager() *mockPager {
	return &mockPager{
		pages: make(map[uint64][]byte),
		next:  1,
	}
}

func (m *mockPager) get(ptr uint64) []byte {
	return m.pages[ptr]
}

func (m *mockPager) alloc(node []byte) uint64 {
	ptr := m.next
	m.next++
	m.pages[ptr] = node
	return ptr
}

func (m *mockPager) set(ptr uint64) []byte {
	return m.pages[ptr]
}

func initEmptyFreeList(fl *FreeList) {
	firstPage := fl.new(make([]byte, BTreePageSize))
	fl.headPage = firstPage
	fl.tailPage = firstPage
	fl.headSeq = 0
	fl.tailSeq = 0
	fl.maxSeq = 0
}

func TestFreeList_BasicFIFO(t *testing.T) {
	mp := newMockPager()
	fl := &FreeList{
		get: mp.get,
		set: mp.set,
		new: mp.alloc,
	}
	initEmptyFreeList(fl)
	fl.PushTail(100)
	fl.PushTail(200)
	fl.PushTail(300)
	fl.SetMaxSeq()
	if fl.tailSeq != 3 {
		t.Errorf("Expected tailSeq to be 3, got %d", fl.tailSeq)
	}

	if got := fl.PopHead(); got != 100 {
		t.Errorf("Expected 100 to be popped, got %d", got)
	}

	if get := fl.PopHead(); get != 200 {
		t.Errorf("Expected 200 to be popped, got %d", get)
	}

	if get := fl.PopHead(); get != 300 {
		t.Errorf("Expected 300 to be popped, got %d", get)
	}

	if got := fl.PopHead(); got != 0 {
		t.Errorf("Expected 0 (empty), got %d", got)
	}
}

func TestFreeList_ExpansionAndRecycling(t *testing.T) {
	mp := newMockPager()
	fl := &FreeList{
		get: mp.get,
		set: mp.set,
		new: mp.alloc,
	}
	initEmptyFreeList(fl)

	pushCount := int(FreeListCap) + 10
	for i := 0; i < pushCount; i++ {
		fl.PushTail(uint64((i + 1) * 10))
	}
	fl.SetMaxSeq()

	if fl.headPage == fl.tailPage {
		t.Fatalf("Expected headPage and tailPage to be different, got %d", fl.headPage)
	}

	poppedCount := 0
	for {
		val := fl.PopHead()
		if val == 0 {
			break
		}
		poppedCount++
	}
	if poppedCount == 0 {
		t.Fatalf("Failed to pop any items after expansion")
	}

	if fl.headSeq != fl.maxSeq {
		t.Errorf("Queue should be drained to maxSeq, but headSeq (%d) != maxSeq (%d)", fl.headSeq, fl.maxSeq)
	}
}
