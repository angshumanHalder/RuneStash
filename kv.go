package main

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"syscall"
)

var ErrConflict = errors.New("transaction conflict: retry")

type KV struct {
	Path    string
	tree    BTree
	pager   *Pager
	meta    Meta
	free    FreeList
	failed  bool
	version uint64   // monotonic version number; persisted in meta page
	ongoing []uint64 // version numbers of concurrent TXs
	history []CommittedTX
	mutex   sync.Mutex
}

type CommittedTX struct {
	version uint64
	writes  []KeyRange // sorted
}

type KeyRange struct {
	start []byte
	stop  []byte
}

type KvTX struct {
	kv       *KV
	snapshot BTree
	pending  BTree
	meta     []byte
	newPages map[uint64]struct{} // pages allocated within this transaction
	recycled []uint64            // tx-local pages freed, available for reuse within the tx
	version  uint64              // based on kv.version
	reads    []KeyRange
}

func (kv *KV) Begin(tx *KvTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	// RO snapshot: frozen view of the tree at Begin time
	tx.snapshot.root = kv.tree.root
	chunks := kv.pager.mmap.chunks
	tx.snapshot.get = func(ptr uint64) []byte { return mmapRead(ptr, chunks) }

	// in-memory pending tree for buffering writes within this TX
	pages := [][]byte(nil)
	tx.pending.get = func(ptr uint64) []byte { return pages[ptr-1] }
	tx.pending.new = func(node []byte) uint64 {
		pages = append(pages, node)
		return uint64(len(pages))
	}
	tx.pending.del = func(uint64) {}

	tx.kv = kv
	tx.meta = kv.meta.save()
	tx.newPages = make(map[uint64]struct{})
	tx.recycled = nil

	// record this reader's version so the free list won't recycle pages it may read
	tx.version = kv.version
	kv.ongoing = append(kv.ongoing, tx.version)

	kv.tree.new = tx.pageNew
	kv.tree.del = tx.pageFree
}

func (kv *KV) Commit(tx *KvTX) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.tree.new = kv.pageAlloc
	kv.tree.del = kv.free.PushTail

	// apply buffered pending writes to the shared tree
	if err := applyPending(&kv.tree, &tx.pending); err != nil {
		kv.sync(tx.meta)
		kv.pager.page.nAppend = 0
		kv.pager.page.updates = map[uint64][]byte{}
		return err
	}

	// return any tx-local recycled pages to the free list
	for _, ptr := range tx.recycled {
		kv.free.PushTail(ptr)
	}

	// conflict detection: abort if any read was overwritten by a concurrent commit
	if detectConflicts(kv, tx) {
		kv.Abort(tx)
		return ErrConflict
	}

	// version management: unregister this reader, then advance version
	kv.removeOngoing(tx.version)
	kv.version++
	kv.free.curVer = kv.version
	kv.free.maxVer = kv.minOngoingVersion()

	if err := updateOrRevert(kv, tx.meta); err != nil {
		return err
	}

	// record this commit in history for future conflict detection
	kv.history = append(kv.history, CommittedTX{
		version: kv.version,
		writes:  collectWrites(&tx.pending),
	})
	// trim history entries no longer needed by any active reader
	minVer := kv.minOngoingVersion()
	for len(kv.history) > 0 && !versionBefore(minVer, kv.history[0].version) {
		kv.history = kv.history[1:]
	}
	return nil
}

func (kv *KV) Abort(tx *KvTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.removeOngoing(tx.version)
	kv.tree.new = kv.pageAlloc
	kv.tree.del = kv.free.PushTail
	kv.sync(tx.meta)
	tx.kv.pager.page.nAppend = 0
	tx.kv.pager.page.updates = map[uint64][]byte{}
}

// removes a version from the ongoing readers list.
func (kv *KV) removeOngoing(ver uint64) {
	for i, v := range kv.ongoing {
		if v == ver {
			kv.ongoing = append(kv.ongoing[:i], kv.ongoing[i+1:]...)
			return
		}
	}
}

// returns the smallest version currently reading, or
// kv.version if no readers are active (meaning all pages are reclaimable).
func (kv *KV) minOngoingVersion() uint64 {
	if len(kv.ongoing) == 0 {
		return kv.version
	}
	minV := kv.ongoing[0]
	for _, v := range kv.ongoing[1:] {
		if v < minV {
			minV = v
		}
	}
	return minV
}

// merges all buffered writes from the pending tree into dst.
// Each value in pending is prefixed with FlagUpdated or FlagDeleted.
func applyPending(dst *BTree, pending *BTree) error {
	if pending.root == 0 {
		return nil
	}
	iter := pending.SeekLE([]byte{})
	iter.Next() // advance past sentinel (the empty key at position 0)
	for iter.Valid() {
		key, val := iter.Deref()
		var err error
		switch val[0] {
		case FlagUpdated:
			err = dst.Insert(key, val[1:])
		case FlagDeleted:
			_, err = dst.Delete(key)
		default:
			panic("applyPending: unknown flag byte")
		}
		if err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}

func (tx *KvTX) pageNew(node []byte) uint64 {
	var ptr uint64
	if len(tx.recycled) > 0 {
		ptr = tx.recycled[len(tx.recycled)-1]
		tx.recycled = tx.recycled[:len(tx.recycled)-1]
		if tx.kv.pager.page.updates == nil {
			tx.kv.pager.page.updates = make(map[uint64][]byte)
		}
		tx.kv.pager.page.updates[ptr] = node
	} else {
		ptr = tx.kv.pageAlloc(node)
	}
	tx.newPages[ptr] = struct{}{}
	return ptr
}

func (tx *KvTX) pageFree(ptr uint64) {
	if _, owned := tx.newPages[ptr]; owned {
		delete(tx.newPages, ptr)
		if ptr < tx.kv.pager.page.flushed {
			delete(tx.kv.pager.page.updates, ptr)
		}
		tx.recycled = append(tx.recycled, ptr)
	} else {
		tx.kv.free.PushTail(ptr)
	}
}

func (tx *KvTX) Seek(key []byte, cmp int) *CombinedIter {
	top := tx.pending.Seek(key, cmp)
	bot := tx.snapshot.Seek(key, cmp)
	return NewCombinedIter(top, bot)
}

func (tx *KvTX) Update(req *UpdateReq) (bool, error) {
	old, exists := tx.Get(req.Key)
	if req.Mode == ModeUpdateOnly && !exists {
		return false, fmt.Errorf("update error: row does not exist")
	}
	if req.Mode == ModeInsertOnly && exists {
		return false, fmt.Errorf("duplicate key error: row already exists")
	}
	req.Old = old
	req.Added = !exists
	req.Updated = true
	tagged := make([]byte, 1+len(req.Val))
	tagged[0] = FlagUpdated
	copy(tagged[1:], req.Val)
	err := tx.pending.Insert(req.Key, tagged)
	return err == nil, err
}

func (tx *KvTX) Del(key []byte) (bool, error) {
	_, exists := tx.Get(key)
	if !exists {
		return false, nil
	}
	err := tx.pending.Insert(key, []byte{FlagDeleted})
	return err == nil, err
}

func (tx *KvTX) Set(key, val []byte) error {
	tagged := make([]byte, 1+len(val))
	tagged[0] = FlagUpdated
	copy(tagged[1:], val)
	return tx.pending.Insert(key, tagged)
}

func (tx *KvTX) Get(key []byte) ([]byte, bool) {
	tx.reads = append(tx.reads, KeyRange{key, key})
	val, ok := tx.pending.Get(key)
	switch {
	case ok && val[0] == FlagUpdated:
		return val[1:], true
	case ok && val[0] == FlagDeleted:
		return nil, false
	case !ok:
		return tx.snapshot.Get(key)
	default:
		panic("unreachable")
	}
}

type kvStore interface {
	Update(req *UpdateReq) (bool, error)
	Del(key []byte) (bool, error)
	Set(key, val []byte) error
}

func (kv *KV) Open() error {
	kv.pager = &Pager{
		fd: -1,
	}

	kv.tree.get = kv.pager.pageRead
	kv.tree.new = kv.pageAlloc
	kv.tree.del = kv.free.PushTail

	kv.free.get = kv.pager.pageRead
	kv.free.new = kv.pager.pageAppend
	kv.free.set = kv.pager.pageWrite

	fd, err := createFileSync(kv.Path)
	if err != nil {
		return err
	}
	kv.pager.fd = fd
	var stat syscall.Stat_t
	if err = syscall.Fstat(kv.pager.fd, &stat); err != nil {
		return fmt.Errorf("stat File: %w", err)
	}
	fileSize := stat.Size
	if err = kv.pager.extendMmap(int(fileSize)); err != nil {
		return err
	}
	if err = readRoot(kv, fileSize); err != nil {
		return err
	}
	return nil
}

func (kv *KV) Close() error {
	var er error
	for _, chunk := range kv.pager.mmap.chunks {
		if err := syscall.Munmap(chunk); err != nil && er == nil {
			er = fmt.Errorf("munmap chunk: %w", err)
		}
	}
	kv.pager.mmap.chunks = nil
	kv.pager.mmap.total = 0
	if kv.pager.fd != -1 {
		if err := syscall.Close(kv.pager.fd); err != nil && er == nil {
			er = fmt.Errorf("close file: %w", err)
		}
		kv.pager.fd = -1
	}
	return er
}

func (kv *KV) Get(key []byte) ([]byte, bool) {
	return kv.tree.Get(key)
}

func (kv *KV) Set(key, val []byte) error {
	data := kv.meta.save()
	err := kv.tree.Insert(key, val)
	if err != nil {
		return err
	}
	return updateOrRevert(kv, data)
}

func (kv *KV) Del(key []byte) (bool, error) {
	data := kv.meta.save()
	deleted, err := kv.tree.Delete(key)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	err = updateOrRevert(kv, data)
	return deleted, err
}

func (kv *KV) Update(req *UpdateReq) (bool, error) {
	meta := kv.meta.save()
	if updated, err := kv.tree.Update(req); !updated {
		return false, err
	}
	err := updateOrRevert(kv, meta)
	return err == nil, err
}

func updateOrRevert(kv *KV, data []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if kv.failed {
		// write and fsync the previous meta page
		if _, err := syscall.Pwrite(kv.pager.fd, data, 0); err != nil {
			return fmt.Errorf("recovery write meta: %w", err)
		}
		if err := syscall.Fsync(kv.pager.fd); err != nil {
			return fmt.Errorf("recovery fsync: %w", err)
		}
		kv.failed = false
	}
	// 2 phase update
	err := updateFile(kv)
	// revert on error
	if err != nil {
		kv.failed = true
		kv.sync(data)

		kv.pager.page.nAppend = 0
		kv.pager.page.updates = nil
	}
	return err
}

func updateFile(kv *KV) error {
	if err := kv.pager.writePages(); err != nil {
		return err
	}
	if err := syscall.Fsync(kv.pager.fd); err != nil {
		return err
	}
	if err := updateRoot(kv); err != nil {
		return err
	}
	kv.free.SetMaxSeq()
	return syscall.Fsync(kv.pager.fd)
}

func readRoot(kv *KV, fileSize int64) error {
	if fileSize == 0 {
		kv.pager.page.flushed = 2
		kv.free.headPage = 1
		kv.free.tailPage = 1
		return nil
	}
	data := kv.pager.mmap.chunks[0]
	kv.sync(data)

	// verify the page
	// 1. check alignment
	if fileSize%int64(BTreePageSize) != 0 {
		return fmt.Errorf("readRoot - db corrupt: invalid file size (%d) is not a multiple of page size", fileSize)
	}
	// 2. Check Boundaries (File must be large enough to hold all flushed pages)
	expectedMinSize := int64(kv.pager.page.flushed) * int64(BTreePageSize)
	if fileSize < expectedMinSize {
		return fmt.Errorf("readRoot - db corrupt: meta claims (%d) pages, but file only holds (%d)", kv.pager.page.flushed, fileSize/int64(BTreePageSize))
	}
	// 3. Check Root pointer
	if kv.tree.root == 0 {
		return fmt.Errorf("readRoot - db corrupt: root pointer is cannot be 0 (Page 0 is reserved)")
	}
	if kv.tree.root >= kv.pager.page.flushed {
		return fmt.Errorf("readRoot - db corrupt: root pointer (%d) is out of bounds (max %d)", kv.tree.root, kv.pager.page.flushed-1)
	}
	return nil
}

func updateRoot(kv *KV) error {
	// update meta with current state before saving
	kv.meta.Root = kv.tree.root
	kv.meta.Flushed = kv.pager.page.flushed
	kv.meta.FreeListHead = kv.free.headPage
	kv.meta.FreeListTail = kv.free.tailPage
	kv.meta.FreeListHeadSeq = kv.free.headSeq
	kv.meta.FreeListTailSeq = kv.free.tailSeq

	if _, err := syscall.Pwrite(kv.pager.fd, kv.meta.save(), 0); err != nil {
		return fmt.Errorf("updateRoot: %w", err)
	}
	return nil
}

func (kv *KV) pageAlloc(node []byte) uint64 {
	if ptr := kv.free.PopHead(); ptr != 0 {
		if kv.pager.page.updates == nil {
			kv.pager.page.updates = make(map[uint64][]byte)
		}
		kv.pager.page.updates[ptr] = node
		return ptr
	}

	return kv.pager.pageAppend(node)
}

func (kv *KV) sync(data []byte) {
	kv.meta.load(data)

	// sync meta to pager and tree
	kv.pager.page.flushed = kv.meta.Flushed
	kv.tree.root = kv.meta.Root

	kv.free.headPage = kv.meta.FreeListHead
	kv.free.tailPage = kv.meta.FreeListTail
	kv.free.headSeq = kv.meta.FreeListHeadSeq
	kv.free.tailSeq = kv.meta.FreeListTailSeq
	kv.free.maxSeq = kv.meta.FreeListTailSeq
}

func versionBefore(a, b uint64) bool {
	return a < b
}

func rangesOverlap(a, b []KeyRange) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case bytes.Compare(a[i].stop, b[j].start) < 0:
			i++
		case bytes.Compare(b[j].stop, a[i].start) < 0:
			j++
		default:
			return true
		}
	}
	return false
}

func collectWrites(pending *BTree) []KeyRange {
	if pending.root == 0 {
		return nil
	}
	var ranges []KeyRange
	iter := pending.SeekLE([]byte{})
	iter.Next()
	for iter.Valid() {
		key, _ := iter.Deref()
		k := make([]byte, len(key))
		copy(k, key)
		ranges = append(ranges, KeyRange{k, k})
		iter.Next()
	}
	return ranges
}

func detectConflicts(kv *KV, tx *KvTX) bool {
	for i := len(kv.history) - 1; i >= 0; i-- {
		if !versionBefore(tx.version, kv.history[i].version) {
			break
		}
		if rangesOverlap(tx.reads, kv.history[i].writes) {
			return true
		}
	}
	return false
}
