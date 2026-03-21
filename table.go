package main

import "encoding/json"

// Value table cell
type Value struct {
	Type uint32
	I64  int64
	Str  []byte
}

// Record table row
type Record struct {
	Cols []string
	Vals []Value
}

type TableDef struct {
	Name   string
	Types  []uint32
	Cols   []string
	PKeys  int
	Prefix uint32
}

// TDefTable internal table
var TDefTable = &TableDef{
	Prefix: 2,
	Name:   "@table",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"name", "def"},
	PKeys:  1,
}

var TDefMeta = &TableDef{
	Prefix: 1,
	Name:   "@meta",
	Types:  []uint32{TypeBytes, TypeBytes},
	Cols:   []string{"key", "val"},
	PKeys:  1,
}

func (r *Record) AddStr(col string, val []byte) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{Type: TypeBytes, Str: val})
	return r
}

func (r *Record) AddI64(col string, val int64) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{Type: TypeInt64, I64: val})
	return r
}

func (r *Record) Get(col string) *Value {
	for i, c := range r.Cols {
		if c == col {
			return &r.Vals[i]
		}
	}
	return nil
}

func getTableDef(db *DB, name string) *TableDef {
	rec := (&Record{}).AddStr("name", []byte(name))
	ok, err := db.dbGet(TDefTable, rec)
	if err != nil {
		panic(err)
	}
	if !ok {
		return nil
	}
	tDef := &TableDef{}
	def := rec.Get("def")
	if def == nil {
		panic("table definition not found")
	}
	err = json.Unmarshal(def.Str, tDef)
	if err != nil {
		panic(err)
	}
	return tDef
}
