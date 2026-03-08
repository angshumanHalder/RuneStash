package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
)

type KV struct {
	Path string
	fd   int
	tree BTree
	mmap struct {
		total  int
		chunks [][]byte
	}
	page struct {
		flushed uint64
		temp    [][]byte
	}
	failed bool
}

func (db *KV) Open() error {
	db.tree.get = db.pageRead
	db.tree.new = db.pageAppend
	db.tree.del = func(uint64) {}

	fd, err := createFileSync(db.Path)
	if err != nil {
		return err
	}
	db.fd = fd
	var stat syscall.Stat_t
	if err = syscall.Fstat(db.fd, &stat); err != nil {
		return fmt.Errorf("stat File: %w", err)
	}
	fileSize := stat.Size
	if err = extendMmap(db, int(fileSize)); err != nil {
		return err
	}
	if err = readRoot(db, fileSize); err != nil {
		return err
	}
	return nil
}

func (db *KV) Close() error {
	var er error
	for _, chunk := range db.mmap.chunks {
		if err := syscall.Munmap(chunk); err != nil && er == nil {
			er = fmt.Errorf("munmap chunk: %w", err)
		}
	}
	db.mmap.chunks = nil
	db.mmap.total = 0
	if db.fd != -1 {
		if err := syscall.Close(db.fd); err != nil && er == nil {
			er = fmt.Errorf("close file: %w", err)
		}
		db.fd = -1
	}
	return er
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key, val []byte) error {
	meta := saveMeta(db)
	err := db.tree.Insert(key, val)
	if err != nil {
		return err
	}
	return updateOrRevert(db, meta)
}

func (db *KV) Del(key []byte) (bool, error) {
	meta := saveMeta(db)
	deleted, err := db.tree.Delete(key)
	if err != nil {
		return false, err
	}
	if !deleted {
		return false, nil
	}
	err = updateOrRevert(db, meta)
	return deleted, err
}

func updateOrRevert(db *KV, meta []byte) error {
	// ensure the on-disk meta page matches the in-memory one after an error
	if db.failed {
		// write and fsync the previous meta page
		if _, err := syscall.Pwrite(db.fd, meta, 0); err != nil {
			return fmt.Errorf("recovery write meta: %w", err)
		}
		if err := syscall.Fsync(db.fd); err != nil {
			return fmt.Errorf("recovery fsync: %w", err)
		}
		db.failed = false
	}
	// 2 phase update
	err := updateFile(db)
	// revert on error
	if err != nil {
		db.failed = true
		loadMeta(db, meta)
		db.page.temp = db.page.temp[:0]
	}
	return err
}

func updateFile(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	if err := syscall.Fsync(db.fd); err != nil {
		return err
	}
	if err := updateRoot(db); err != nil {
		return err
	}
	return syscall.Fsync(db.fd)
}

func createFileSync(file string) (fd int, err error) {
	flags := os.O_RDONLY | syscall.O_DIRECTORY
	dirFd, err := unix.Open(path.Dir(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open directory: %w", err)
	}

	defer func() {
		closeErr := unix.Close(dirFd)
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close directory: %w", closeErr)
		}
	}()
	flags = os.O_RDWR | os.O_CREATE
	fd, err = unix.Openat(dirFd, path.Base(file), flags, 0o644)
	if err != nil {
		return -1, fmt.Errorf("open file: %w", err)
	}
	if err = syscall.Fsync(dirFd); err != nil {
		_ = syscall.Close(fd) // may leave empty file
		return -1, fmt.Errorf("fsync directory: %w", err)
	}
	return fd, nil
}

func (db *KV) pageRead(ptr uint64) []byte {
	if ptr >= db.page.flushed {
		idx := ptr - db.page.flushed
		if idx < uint64(len(db.page.temp)) {
			return db.page.temp[idx]
		}
		panic("pageRead: temp page not found")
	}
	start := uint64(0)
	for _, chunk := range db.mmap.chunks {
		end := start + uint64(len(chunk))/BTreePageSize
		if ptr < end {
			offset := BTreePageSize * (ptr - start)
			return chunk[offset : offset+BTreePageSize]
		}
		start = end
	}
	panic("pageRead: page not found")
}

func (db *KV) pageAppend(node []byte) uint64 {
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node)
	return ptr
}

func extendMmap(db *KV, size int) error {
	if size <= db.mmap.total {
		return nil
	}

	alloc := max(db.mmap.total, 64<<20)
	for db.mmap.total+alloc < size {
		alloc *= 2
	}
	chunk, err := syscall.Mmap(db.fd, int64(db.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	db.mmap.total += alloc
	db.mmap.chunks = append(db.mmap.chunks, chunk)
	return nil
}

func writePages(db *KV) error {
	size := (int(db.page.flushed) + len(db.page.temp)) * BTreePageSize
	if err := extendMmap(db, size); err != nil {
		return err
	}
	offset := int64(db.page.flushed * BTreePageSize)
	if _, err := unix.Pwritev(db.fd, db.page.temp, offset); err != nil {
		return err
	}
	db.page.flushed += uint64(len(db.page.temp))
	db.page.temp = db.page.temp[:0]
	return nil
}

func saveMeta(db *KV) []byte {
	var data [32]byte
	copy(data[:16], DBSig)
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	return data[:]
}

func loadMeta(db *KV, data []byte) {
	if string(data[:len(DBSig)]) != DBSig {
		panic("loadMeta: invalid signature")
	}
	db.tree.root = binary.LittleEndian.Uint64(data[16:])
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
}

func readRoot(db *KV, fileSize int64) error {
	if fileSize == 0 {
		db.page.flushed = 1
		return nil
	}
	data := db.mmap.chunks[0]
	loadMeta(db, data)
	// verify the page
	// 1. check alignment
	if fileSize%int64(BTreePageSize) != 0 {
		return fmt.Errorf("readRoot - db corrupt: invalid file size (%d) is not a multiple of page size", fileSize)
	}
	// 2. Check Boundaries (File must be large enough to hold all flushed pages)
	expectedMinSize := int64(db.page.flushed) * int64(BTreePageSize)
	if fileSize < expectedMinSize {
		return fmt.Errorf("readRoot - db corrupt: meta claims (%d) pages, but file only holds (%d)", db.page.flushed, fileSize/int64(BTreePageSize))
	}
	// 3. Check Root pointer
	if db.tree.root == 0 {
		return fmt.Errorf("readRoot - db corrupt: root pointer is cannot be 0 (Page 0 is reserved)")
	}
	if db.tree.root >= db.page.flushed {
		return fmt.Errorf("readRoot - db corrupt: root pointer (%d) is out of bounds (max %d)", db.tree.root, db.page.flushed-1)
	}
	return nil
}

func updateRoot(db *KV) error {
	if _, err := syscall.Pwrite(db.fd, saveMeta(db), 0); err != nil {
		return fmt.Errorf("updateRoot: %w", err)
	}
	return nil
}
