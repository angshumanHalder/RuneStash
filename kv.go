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

func (db *KV) Open() error {
	db.pager = &Pager{
		fd: -1,
	}

	db.tree.get = db.pager.pageRead
	db.tree.new = db.pageAlloc
	db.tree.del = db.free.PushTail

	db.free.get = db.pager.pageRead
	db.free.new = db.pager.pageAppend
	db.free.set = db.pager.pageWrite

	fd, err := createFileSync(db.Path)
	if err != nil {
		return err
	}
	db.pager.fd = fd
	var stat syscall.Stat_t
	if err = syscall.Fstat(db.pager.fd, &stat); err != nil {
		return fmt.Errorf("stat File: %w", err)
	}
	fileSize := stat.Size
	if err = db.pager.extendMmap(int(fileSize)); err != nil {
		return err
	}
	if err = readRoot(db, fileSize); err != nil {
		return err
	}
	return nil
}

func (db *KV) Close() error {
	var er error
	for _, chunk := range db.pager.mmap.chunks {
		if err := syscall.Munmap(chunk); err != nil && er == nil {
			er = fmt.Errorf("munmap chunk: %w", err)
		}
	}
	db.pager.mmap.chunks = nil
	db.pager.mmap.total = 0
	if db.pager.fd != -1 {
		if err := syscall.Close(db.pager.fd); err != nil && er == nil {
			er = fmt.Errorf("close file: %w", err)
		}
		db.pager.fd = -1
	}
	return er
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key, val []byte) error {
	data := db.meta.save()
	err := db.tree.Insert(key, val)
	if err != nil {
		return err
	}
	return updateOrRevert(db, data)
}

func (db *KV) Del(key []byte) (bool, error) {
	data := db.meta.save()
	deleted, err := db.tree.Delete(key)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	err = updateOrRevert(db, data)
	return deleted, err
}

func updateOrRevert(db *KV, data []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		// write and fsync the previous meta page
		if _, err := syscall.Pwrite(db.pager.fd, data, 0); err != nil {
			return fmt.Errorf("recovery write meta: %w", err)
		}
		if err := syscall.Fsync(db.pager.fd); err != nil {
			return fmt.Errorf("recovery fsync: %w", err)
		}
		db.failed = false
	}
	// 2 phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		db.failed = true
		db.meta.load(data)

		// sync reverted meta back to pager and tree
		db.pager.page.flushed = db.meta.Flushed
		db.tree.root = db.meta.Root

		db.free.headPage = db.meta.FreeListHead
		db.free.tailPage = db.meta.FreeListTail
		db.free.headSeq = db.meta.FreeListHeadSeq
		db.free.tailSeq = db.meta.FreeListTailSeq
		db.free.maxSeq = db.meta.FreeListTailSeq

		db.pager.page.nAppend = 0
		db.pager.page.updates = nil
	}
	return err
}

func updateFile(db *KV) error {
	if err := db.pager.writePages(); err != nil {
		return err
	}
	if err := syscall.Fsync(db.pager.fd); err != nil {
		return err
	}
	if err := updateRoot(db); err != nil {
		return err
	}
	db.free.SetMaxSeq()
	return syscall.Fsync(db.pager.fd)
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.pager.page.flushed = 2
		db.free.headPage = 1
		db.free.tailPage = 1
		return nil
	}
	data := db.pager.mmap.chunks[0]
	db.meta.load(data)

	// sync meta to pager and tree
	db.pager.page.flushed = db.meta.Flushed
	db.tree.root = db.meta.Root

	db.free.headPage = db.meta.FreeListHead
	db.free.tailPage = db.meta.FreeListTail
	db.free.headSeq = db.meta.FreeListHeadSeq
	db.free.tailSeq = db.meta.FreeListTailSeq
	db.free.maxSeq = db.meta.FreeListTailSeq

	// verify the page
	// 1. check alignment
	if fileSize%int64(BTreePageSize) != 0 {
		return fmt.Errorf("readRoot - db corrupt: invalid file size (%d) is not a multiple of page size", fileSize)
	}
	// 2. Check Boundaries (File must be large enough to hold all flushed pages)
	expectedMinSize := int64(db.pager.page.flushed) * int64(BTreePageSize)
	if fileSize < expectedMinSize {
		return fmt.Errorf("readRoot - db corrupt: meta claims (%d) pages, but file only holds (%d)", db.pager.page.flushed, fileSize/int64(BTreePageSize))
	}
	// 3. Check Root pointer
	if db.tree.root == 0 {
		return fmt.Errorf("readRoot - db corrupt: root pointer is cannot be 0 (Page 0 is reserved)")
	}
	if db.tree.root >= db.pager.page.flushed {
		return fmt.Errorf("readRoot - db corrupt: root pointer (%d) is out of bounds (max %d)", db.tree.root, db.pager.page.flushed-1)
	}
	return nil
}

func updateRoot(db *KV) error {
	// update meta with current state before saving
	db.meta.Root = db.tree.root
	db.meta.Flushed = db.pager.page.flushed
	db.meta.FreeListHead = db.free.headPage
	db.meta.FreeListTail = db.free.tailPage
	db.meta.FreeListHeadSeq = db.free.headSeq
	db.meta.FreeListTailSeq = db.free.tailSeq

	if _, err := syscall.Pwrite(db.pager.fd, db.meta.save(), 0); err != nil {
		return fmt.Errorf("updateRoot: %w", err)
	}
	return nil
}

func (db *KV) pageAlloc(node []byte) uint64 {
	if ptr := db.free.PopHead(); ptr != 0 {
		if db.pager.page.updates == nil {
			db.pager.page.updates = make(map[uint64][]byte)
		}
		db.pager.page.updates[ptr] = node
		return ptr
	}

	return db.pager.pageAppend(node)
}
