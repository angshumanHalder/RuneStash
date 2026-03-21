package main

import (
	"encoding/binary"
	"fmt"
)

type DB struct {
	Path string
	kv   KV
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
	for _, v := range vals {
		switch v.Type {
		case TypeInt64:
			var ibuf [8]byte
			// db assumes the primary key is always a positive number
			binary.BigEndian.PutUint64(ibuf[:], uint64(v.I64))
			out = append(out, ibuf[:]...)
		case TypeBytes:
			out = append(out, v.Str...)
			out = append(out, 0)
		}
	}
	return out
}

func decodeValues(in []byte, out []Value) {
	offset := 0
	for i := range out {
		switch out[i].Type {
		case TypeInt64:
			out[i].I64 = int64(binary.LittleEndian.Uint64(in[offset:]))
			offset += 8
		case TypeBytes:
			length := int(binary.LittleEndian.Uint32(in[offset:]))
			offset += 4
			out[i].Str = in[offset : offset+length]
			offset += length
		}
	}
}

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TypeInt64:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(v.I64))
			out = append(out, buf[:]...)
		case TypeBytes:
			var buf [4]byte
			binary.LittleEndian.PutUint32(buf[:], uint32(len(v.Str)))
			out = append(out, buf[:]...)
			out = append(out, v.Str...)
		}
	}
	return out
}
