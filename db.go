package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
)

type DB struct {
	Path string
	kv   KV
}

func (db *DB) TableNew(tDef *TableDef) error {
	if strings.HasPrefix(tDef.Name, "@") {
		return fmt.Errorf("names must not start with @ characters")
	}
	t := getTableDef(db, tDef.Name)
	if t != nil {
		return fmt.Errorf("table %s already exists", tDef.Name)
	}
	if len(tDef.Cols) != len(tDef.Types) {
		return fmt.Errorf("length of columns - %d, does not match length of types - %d", len(tDef.Types), len(tDef.Cols))
	}
	if tDef.PKeys < 1 {
		return fmt.Errorf("pkeys must be greater than zero")
	}
	if tDef.PKeys > len(tDef.Cols) {
		return fmt.Errorf("pkeys must be less than or equal to number of columns")
	}
	seen := make(map[string]struct{}, len(tDef.Cols))
	for _, col := range tDef.Cols {
		if col == "" {
			return fmt.Errorf("column names must not be empty")
		}
		if _, exists := seen[col]; exists {
			return fmt.Errorf("duplicate column name - %s", col)
		}
		seen[col] = struct{}{}
	}

	rec := (&Record{}).AddStr("key", []byte("next_prefix"))
	ok, err := db.dbGet(TDefMeta, rec)
	if err != nil {
		panic(err)
	}
	if !ok {
		return fmt.Errorf("unable to find meta table")
	}
	val := rec.Get("val")
	if val == nil {
		return fmt.Errorf("next_prefix not found in meta table")
	}
	prefix := binary.BigEndian.Uint32(val.Str)
	tDef.Prefix = prefix
	prefix += 1
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	val.Str = buf[:]
	_, err = db.dbUpdate(TDefMeta, *rec, ModeUpdateOnly)
	if err != nil {
		panic(err)
	}
	tableRecord := (&Record{}).AddStr("name", []byte(tDef.Name))
	schemaBytes, err := json.Marshal(&tDef)
	if err != nil {
		panic(err)
	}
	tableRecord.AddStr("def", schemaBytes)
	ok, err = db.dbUpdate(TDefTable, *tableRecord, ModeInsertOnly)
	if err != nil {
		panic(err)
	}
	if !ok {
		return fmt.Errorf("unable to create table %s", tDef.Name)
	}
	return nil
}

func (db *DB) Get(table string, rec *Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbGet(tDef, rec)
}

func (db *DB) Insert(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbUpdate(tDef, rec, ModeInsertOnly)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbUpdate(tDef, rec, ModeUpdateOnly)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbUpdate(tDef, rec, ModeUpsert)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbDelete(tDef, rec)
}

// Point query: get single row by primary key
func (db *DB) dbGet(tDef *TableDef, rec *Record) (bool, error) {
	// 1. reorder the input columns according to the schema
	values, err := checkRecord(tDef, *rec, tDef.PKeys)
	if err != nil {
		return false, err
	}

	// 2. encode the primary key
	key := encodeKey(nil, tDef.Prefix, values[:tDef.PKeys])

	// 3. query the KV store
	val, ok := db.kv.Get(key)
	if !ok {
		return false, nil
	}

	for i := tDef.PKeys; i < len(tDef.Cols); i++ {
		values[i].Type = tDef.Types[i]
	}
	decodeValues(val, values[tDef.PKeys:])
	rec.Cols = tDef.Cols
	rec.Vals = values
	return true, nil
}

func (db *DB) dbUpdate(tDef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tDef, rec, len(tDef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tDef.Prefix, values[:tDef.PKeys])
	val := encodeValues(nil, values[tDef.PKeys:])
	return db.kv.Update(key, val, mode)
}

func (db *DB) dbDelete(tDef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tDef, rec, tDef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tDef.Prefix, values[:tDef.PKeys])
	return db.kv.Del(key)
}

func checkRecord(tDef *TableDef, rec Record, n int) ([]Value, error) {
	values := make([]Value, len(tDef.Cols))

	for i := 0; i < n; i++ {
		colName := tDef.Cols[i]
		val := rec.Get(colName)
		if val == nil {
			return nil, fmt.Errorf("missing column: %s", colName)
		}
		if val.Type != tDef.Types[i] {
			return nil, fmt.Errorf("mismatch column type: %s", colName)
		}
		values[i] = *val
	}
	return values, nil
}

func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

func decodeValues(in []byte, out []Value) {
	offset := 0
	for i := range out {
		switch out[i].Type {
		case TypeInt64:
			u := binary.BigEndian.Uint64(in[offset:])
			out[i].I64 = int64(u - (1 << 63))
			offset += 8
		case TypeBytes:
			var str []byte
			for offset < len(in) {
				b := in[offset]
				offset++
				if b == 0x00 {
					break
				}
				if b == 0x01 {
					str = append(str, in[offset]-0x01)
					offset++
				} else {
					str = append(str, b)
				}
			}
			out[i].Str = str
		}
	}
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		out = encodeValue(out, v)
	}
	return out
}

func encodeValue(out []byte, v Value) []byte {
	switch v.Type {
	case TypeInt64:
		var buf [8]byte
		// flip sign bit so that negative numbers sort before positive numbers
		u := uint64(v.I64) + (1 << 63)
		binary.BigEndian.PutUint64(buf[:], u)
		out = append(out, buf[:]...)
	case TypeBytes:
		// escape 0x00 -> 0x01 0x01, 0x01 -> 0x01 0x02, then null-terminate
		for _, b := range v.Str {
			if b <= 0x01 {
				out = append(out, 0x01, b+0x01)
			} else {
				out = append(out, b)
			}
		}
		out = append(out, 0x00)
	}
	return out
}
