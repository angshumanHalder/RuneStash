package main

import (
	"path/filepath"
	"testing"
)

func TestKV_LifeCycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "runestash.db")

	// Initialize KV database
	db := &KV{Path: dbPath}

	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open a new database: %v", err)
	}

	if db.pager.fd == -1 {
		t.Errorf("Expected a valid file descriptor, got -1")
	}

	if db.pager.page.flushed != 2 {
		t.Errorf("Expected page.flushed to be 2 (Page 0 reserved), got %d", db.pager.page.flushed)
	}

	if len(db.pager.mmap.chunks) != 0 {
		t.Errorf("Expected memory map chunks to be 0 for a new file, got %d", len(db.pager.mmap.chunks))
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close the database: %v", err)
	}

	if db.pager.fd != -1 {
		t.Errorf("Expected file descriptor to be -1 after close")
	}

	if len(db.pager.mmap.chunks) != 0 {
		t.Errorf("Expected memory map chunks to be cleared after close")
	}
}

func TestKV_Persistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "runestash.db")

	db := &KV{Path: dbPath}
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open a new database: %v", err)
	}

	defer db.Close()

	dummyNode := make(BNode, BTreePageSize)
	copy(dummyNode, []byte("Hello RuneStash"))

	newRootId := db.pager.pageAppend(dummyNode)
	db.tree.root = newRootId

	if err := db.pager.writePages(); err != nil {
		t.Fatalf("Failed to write pages: %v", err)
	}

	if err := updateRoot(db); err != nil {
		t.Fatalf("Failed to update root meta page: %v", err)
	}

	db2 := &KV{Path: dbPath}
	if err := db2.Open(); err != nil {
		t.Fatalf("Phase 2 Open failed: %v", err)
	}
	defer db2.Close()

	if db2.tree.root != 2 {
		t.Errorf("Expected root to be 1, got %d", db2.tree.root)
	}

	if db2.pager.page.flushed != 3 {
		t.Errorf("Expected 2 pages to be flushed (Meta + Node), got %d", db2.pager.page.flushed)
	}

	readNode := db2.pager.pageRead(2)
	if string(readNode[:15]) != "Hello RuneStash" {
		t.Errorf("Expected node to be 'Hello RuneStash', got %s", readNode[:15])
	}
}
