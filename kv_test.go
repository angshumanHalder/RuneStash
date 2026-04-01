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

func TestKV_GetAndSet(t *testing.T) {
	dir := t.TempDir()
	db := &KV{Path: filepath.Join(dir, "test.db")}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// missing key on empty tree
	_, ok := db.Get([]byte("k1"))
	if ok {
		t.Fatal("expected not found on empty DB")
	}

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	val, ok := db.Get([]byte("k1"))
	if !ok || string(val) != "v1" {
		t.Fatalf("expected 'v1', got %q (ok=%v)", val, ok)
	}

	_, ok = db.Get([]byte("missing"))
	if ok {
		t.Fatal("expected not found for missing key")
	}
}

func TestKV_Update_ModeInsertOnly(t *testing.T) {
	dir := t.TempDir()
	db := &KV{Path: filepath.Join(dir, "test.db")}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	req := &UpdateReq{Key: []byte("k1"), Val: []byte("v1"), Mode: ModeInsertOnly}
	if _, err := db.Update(req); err != nil || !req.Added {
		t.Fatalf("expected successful insert, got added=%v err=%v", req.Added, err)
	}

	// duplicate insert must fail
	req2 := &UpdateReq{Key: []byte("k1"), Val: []byte("v2"), Mode: ModeInsertOnly}
	if _, err := db.Update(req2); err == nil {
		t.Fatal("expected duplicate key error, got nil")
	}

	// original value must be unchanged
	val, ok := db.Get([]byte("k1"))
	if !ok || string(val) != "v1" {
		t.Fatalf("expected 'v1', got %q", val)
	}
}

func TestKV_Update_ModeUpdateOnly(t *testing.T) {
	dir := t.TempDir()
	db := &KV{Path: filepath.Join(dir, "test.db")}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// seed so the tree is not empty
	if err := db.Set([]byte("seed"), []byte("x")); err != nil {
		t.Fatal(err)
	}

	// updating a non-existent key must fail
	ghost := &UpdateReq{Key: []byte("ghost"), Val: []byte("val"), Mode: ModeUpdateOnly}
	if _, err := db.Update(ghost); err == nil {
		t.Fatal("expected error updating non-existent key")
	}

	// set a key, then update it
	if err := db.Set([]byte("k1"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	req := &UpdateReq{Key: []byte("k1"), Val: []byte("new"), Mode: ModeUpdateOnly}
	if _, err := db.Update(req); err != nil || req.Added {
		t.Fatalf("expected update ok with Added=false, got added=%v err=%v", req.Added, err)
	}

	val, _ := db.Get([]byte("k1"))
	if string(val) != "new" {
		t.Fatalf("expected 'new', got %q", val)
	}
}

func TestKV_Update_ModeUpsert(t *testing.T) {
	dir := t.TempDir()
	db := &KV{Path: filepath.Join(dir, "test.db")}
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// upsert on new key → insert, Added=true
	req := &UpdateReq{Key: []byte("k1"), Val: []byte("v1"), Mode: ModeUpsert}
	if _, err := db.Update(req); err != nil || !req.Added {
		t.Fatalf("expected insert on upsert, got added=%v err=%v", req.Added, err)
	}

	// upsert on existing key → update, Added=false
	req2 := &UpdateReq{Key: []byte("k1"), Val: []byte("v2"), Mode: ModeUpsert}
	if _, err := db.Update(req2); err != nil || req2.Added {
		t.Fatalf("expected update on upsert, got added=%v err=%v", req2.Added, err)
	}

	val, _ := db.Get([]byte("k1"))
	if string(val) != "v2" {
		t.Fatalf("expected 'v2', got %q", val)
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
