package main

import (
	"fmt"
	"os"
	"path"
	"syscall"

	"golang.org/x/sys/unix"
)

type Pager struct {
	fd   int
	mmap struct {
		total  int
		chunks [][]byte
	}
	page struct {
		flushed uint64
		updates map[uint64][]byte
		temp    [][]byte
	}
}

func (p *Pager) extendMmap(size int) error {
	if size <= p.mmap.total {
		return nil
	}

	alloc := max(p.mmap.total, 64<<20)
	for p.mmap.total+alloc < size {
		alloc *= 2
	}
	chunk, err := syscall.Mmap(p.fd, int64(p.mmap.total), alloc, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}
	p.mmap.total += alloc
	p.mmap.chunks = append(p.mmap.chunks, chunk)
	return nil
}

func (p *Pager) writePages() error {
	for ptr, data := range p.page.updates {
		offset := int64(ptr * BTreePageSize)
		if _, err := syscall.Pwrite(p.fd, data, offset); err != nil {
			return fmt.Errorf("pwrite update: %w", err)
		}
	}

	p.page.updates = nil
	if len(p.page.temp) == 0 {
		return nil
	}

	size := (int(p.page.flushed) + len(p.page.temp)) * BTreePageSize
	if err := p.extendMmap(size); err != nil {
		return err
	}
	offset := int64(p.page.flushed * BTreePageSize)
	if _, err := unix.Pwritev(p.fd, p.page.temp, offset); err != nil {
		return err
	}
	p.page.flushed += uint64(len(p.page.temp))
	p.page.temp = p.page.temp[:0]
	return nil
}

func (p *Pager) pageRead(ptr uint64) []byte {
	if node, ok := p.page.updates[ptr]; ok {
		return node
	}
	return p.pageReadFile(ptr)
}

func (p *Pager) pageReadFile(ptr uint64) []byte {
	if ptr >= p.page.flushed {
		idx := ptr - p.page.flushed
		if idx < uint64(len(p.page.temp)) {
			return p.page.temp[idx]
		}
		panic("pageRead: temp page not found")
	}
	start := uint64(0)
	for _, chunk := range p.mmap.chunks {
		end := start + uint64(len(chunk))/BTreePageSize
		if ptr < end {
			offset := BTreePageSize * (ptr - start)
			return chunk[offset : offset+BTreePageSize]
		}
		start = end
	}
	panic("pageRead: page not found")
}

func (p *Pager) pageWrite(ptr uint64) []byte {
	if ptr >= p.page.flushed {
		idx := ptr - p.page.flushed
		if idx < uint64(len(p.page.temp)) {
			return p.page.temp[idx]
		}
		panic("pageWrite: temp page not found")
	}
	if p.page.updates == nil {
		p.page.updates = make(map[uint64][]byte)
	}
	if node, ok := p.page.updates[ptr]; ok {
		return node
	}
	node := make([]byte, BTreePageSize)
	copy(node, p.pageReadFile(ptr))
	p.page.updates[ptr] = node
	return node
}

func (p *Pager) pageAppend(node []byte) uint64 {
	ptr := p.page.flushed + uint64(len(p.page.temp))
	p.page.temp = append(p.page.temp, node)
	return ptr
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
