package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// schema used across all DB tests: int64 primary key + bytes value column
var personTable = &TableDef{
	Name:   "person",
	Prefix: 10,
	Types:  []uint32{TypeInt64, TypeBytes},
	Cols:   []string{"id", "name"},
	PKeys:  1,
}

// openTestDB opens a fresh KV-backed DB and registers cleanup.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	db := &DB{}
	db.kv.Path = filepath.Join(t.TempDir(), "test.db")
	if err := db.kv.Open(); err != nil {
		t.Fatalf("open DB: %v", err)
	}
	t.Cleanup(func() { db.kv.Close() })
	return db
}

// registerTable stores a TableDef into TDefTable so the public API can find it.
func registerTable(t *testing.T, db *DB, tDef *TableDef) {
	t.Helper()
	defBytes, err := json.Marshal(tDef)
	if err != nil {
		t.Fatal(err)
	}
	rec := (&Record{}).AddStr("name", []byte(tDef.Name)).AddStr("def", defBytes)
	if _, err = db.dbUpdate(TDefTable, *rec, ModeUpsert); err != nil {
		t.Fatalf("register table %q: %v", tDef.Name, err)
	}
}

// --- unknown table ---

func TestDB_Get_UnknownTable(t *testing.T) {
	db := openTestDB(t)
	rec := (&Record{}).AddI64("id", 1)
	_, err := db.Get("ghost", rec)
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestDB_Insert_UnknownTable(t *testing.T) {
	db := openTestDB(t)
	rec := (&Record{}).AddI64("id", 1).AddStr("name", []byte("alice"))
	_, err := db.Insert("ghost", *rec)
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestDB_Update_UnknownTable(t *testing.T) {
	db := openTestDB(t)
	rec := (&Record{}).AddI64("id", 1).AddStr("name", []byte("alice"))
	_, err := db.Update("ghost", *rec)
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestDB_Delete_UnknownTable(t *testing.T) {
	db := openTestDB(t)
	rec := (&Record{}).AddI64("id", 1)
	_, err := db.Delete("ghost", *rec)
	if err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

// --- Insert + Get ---

func TestDB_Insert_Get(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	ok, err := db.Insert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("alice")))
	if err != nil || !ok {
		t.Fatalf("Insert: ok=%v err=%v", ok, err)
	}

	query := (&Record{}).AddI64("id", 1)
	ok, err = db.Get("person", query)
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if v := query.Get("name"); v == nil || string(v.Str) != "alice" {
		t.Fatalf("expected name='alice', got %v", v)
	}
}

func TestDB_Get_Missing(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	query := (&Record{}).AddI64("id", 99)
	ok, err := db.Get("person", query)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected not found for missing row")
	}
}

// --- Insert duplicate ---

func TestDB_Insert_Duplicate(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	db.Insert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("alice")))

	_, err := db.Insert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("bob")))
	if err == nil {
		t.Fatal("expected duplicate key error, got nil")
	}

	// original value must still be intact
	query := (&Record{}).AddI64("id", 1)
	db.Get("person", query)
	if v := query.Get("name"); v == nil || string(v.Str) != "alice" {
		t.Fatalf("expected 'alice' after rejected duplicate insert, got %v", v)
	}
}

// --- Update ---

func TestDB_Update(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	db.Insert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("alice")))

	// ok=false means the row was updated (not newly inserted), which is correct
	ok, err := db.Update("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("alice2")))
	if err != nil || ok {
		t.Fatalf("Update: expected ok=false (updated, not inserted), got ok=%v err=%v", ok, err)
	}

	query := (&Record{}).AddI64("id", 1)
	db.Get("person", query)
	if v := query.Get("name"); v == nil || string(v.Str) != "alice2" {
		t.Fatalf("expected 'alice2', got %v", v)
	}
}

func TestDB_Update_Missing(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	// seed so the KV tree is not empty
	db.Insert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("seed")))

	_, err := db.Update("person", *(&Record{}).AddI64("id", 99).AddStr("name", []byte("ghost")))
	if err == nil {
		t.Fatal("expected error updating non-existent row")
	}
}

// --- Upsert ---

func TestDB_Upsert_InsertThenUpdate(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	// first upsert: insert → Added=true → ok=true
	ok, err := db.Upsert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("v1")))
	if err != nil || !ok {
		t.Fatalf("Upsert (insert): ok=%v err=%v", ok, err)
	}

	// second upsert: update → Added=false → ok=false
	ok, err = db.Upsert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("v2")))
	if err != nil || ok {
		t.Fatalf("Upsert (update) should return ok=false: ok=%v err=%v", ok, err)
	}

	query := (&Record{}).AddI64("id", 1)
	db.Get("person", query)
	if v := query.Get("name"); v == nil || string(v.Str) != "v2" {
		t.Fatalf("expected 'v2' after upsert, got %v", v)
	}
}

// --- Delete ---

func TestDB_Delete(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	db.Insert("person", *(&Record{}).AddI64("id", 1).AddStr("name", []byte("alice")))

	ok, err := db.Delete("person", *(&Record{}).AddI64("id", 1))
	if err != nil || !ok {
		t.Fatalf("Delete: ok=%v err=%v", ok, err)
	}

	query := (&Record{}).AddI64("id", 1)
	ok, _ = db.Get("person", query)
	if ok {
		t.Fatal("expected row to be gone after delete")
	}
}

func TestDB_Delete_Missing(t *testing.T) {
	db := openTestDB(t)
	registerTable(t, db, personTable)

	ok, err := db.Delete("person", *(&Record{}).AddI64("id", 99))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected false when deleting non-existent row")
	}
}

// --- checkRecord ---

func TestCheckRecord_MissingColumn(t *testing.T) {
	rec := (&Record{}).AddI64("id", 1) // "name" missing
	_, err := checkRecord(personTable, *rec, len(personTable.Cols))
	if err == nil {
		t.Fatal("expected error for missing column")
	}
}

func TestCheckRecord_TypeMismatch(t *testing.T) {
	// "id" declared as TypeInt64 but provided as TypeBytes
	rec := (&Record{}).AddStr("id", []byte("bad")).AddStr("name", []byte("alice"))
	_, err := checkRecord(personTable, *rec, len(personTable.Cols))
	if err == nil {
		t.Fatal("expected error for type mismatch on 'id'")
	}
}

// --- encodeValues / decodeValues round-trip ---

func TestEncodeDecodeValues_RoundTrip(t *testing.T) {
	vals := []Value{
		{Type: TypeInt64, I64: -42},
		{Type: TypeBytes, Str: []byte("hello")},
		{Type: TypeInt64, I64: 0},
		{Type: TypeBytes, Str: []byte{}},
	}

	encoded := encodeValues(nil, vals)

	out := make([]Value, len(vals))
	for i, v := range vals {
		out[i].Type = v.Type
	}
	decodeValues(encoded, out)

	for i, want := range vals {
		got := out[i]
		switch want.Type {
		case TypeInt64:
			if got.I64 != want.I64 {
				t.Errorf("[%d] I64: got %d, want %d", i, got.I64, want.I64)
			}
		case TypeBytes:
			if string(got.Str) != string(want.Str) {
				t.Errorf("[%d] Str: got %q, want %q", i, got.Str, want.Str)
			}
		}
	}
}
