package main

import (
	"fmt"
	"syscall"
)

type KV struct {
	Path   string
	tree   BTree
	pager  *Pager
	meta   Meta
	free   FreeList
	failed bool
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
