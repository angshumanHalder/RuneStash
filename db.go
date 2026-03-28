package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type DB struct {
	Path string
	kv   KV
}

type DbTX struct {
	kv KvTX
	db *DB
}

type Scanner struct {
	Cmp1 int
	Cmp2 int
	Key1 Record
	Key2 Record

	db     *DB
	tDef   *TableDef
	index  int
	iter   *BIter
	keyEnd []byte
}

func (db *DB) Begin(tx *DbTX) {
	tx.db = db
	tx.kv = KvTX{kv: &db.kv, meta: []byte{}}
	db.kv.Begin(&tx.kv)
}

func (db *DB) Commit(tx *DbTX) error {
	return db.kv.Commit(&tx.kv)
}

func (db *DB) Abort(tx *DbTX) {
	db.kv.Abort(&tx.kv)
}

func (tx *DbTX) Scan(table string, req *Scanner) error {
	return tx.db.Scan(table, req)
}

func (tx *DbTX) Set(table string, rec Record, mode int) (bool, error) {
	tDef := getTableDef(tx.db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return tx.db.dbUpdate(&tx.kv, tDef, rec, mode)
}

func (tx *DbTX) Delete(table string, rec Record) (bool, error) {
	tDef := getTableDef(tx.db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return tx.db.dbDelete(&tx.kv, tDef, rec)
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
		return fmt.Errorf("pKeys must be greater than zero")
	}
	if tDef.PKeys > len(tDef.Cols) {
		return fmt.Errorf("pKeys must be less than or equal to number of columns")
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
	if len(tDef.Indexes) == 0 {
		tDef.Indexes = [][]string{tDef.Cols[:tDef.PKeys]}
	}
	tDef.Prefixes = []uint32{prefix}
	prefix += 1
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	val.Str = buf[:]
	_, err = db.dbUpdate(&db.kv, TDefMeta, *rec, ModeUpdateOnly)
	if err != nil {
		panic(err)
	}
	tableRecord := (&Record{}).AddStr("name", []byte(tDef.Name))
	schemaBytes, err := json.Marshal(&tDef)
	if err != nil {
		panic(err)
	}
	tableRecord.AddStr("def", schemaBytes)
	ok, err = db.dbUpdate(&db.kv, TDefTable, *tableRecord, ModeInsertOnly)
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
	return db.dbUpdate(&db.kv, tDef, rec, ModeInsertOnly)
}

func (db *DB) Update(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbUpdate(&db.kv, tDef, rec, ModeUpdateOnly)
}

func (db *DB) Upsert(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbUpdate(&db.kv, tDef, rec, ModeUpsert)
}

func (db *DB) Delete(table string, rec Record) (bool, error) {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return false, fmt.Errorf("table: %s - not found", table)
	}
	return db.dbDelete(&db.kv, tDef, rec)
}

func (db *DB) Scan(table string, req *Scanner) error {
	tDef := getTableDef(db, table)
	if tDef == nil {
		return fmt.Errorf("table: %s - not found", table)
	}
	return dbScan(db, tDef, req)
}

func (sc *Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}
	key, _ := sc.iter.Deref()
	return cmpOK(key, sc.Cmp2, sc.keyEnd)
}

func (sc *Scanner) Next() {
	if sc.Cmp1 > 0 {
		sc.iter.Next()
	} else {
		sc.iter.Prev()
	}
}

func (sc *Scanner) Deref(rec *Record) {
	tDef := sc.tDef
	indexes := tableIndexes(tDef)
	index := indexes[sc.index]

	key, val := sc.iter.Deref()

	rec.Cols = tDef.Cols
	rec.Vals = make([]Value, len(tDef.Cols))

	if sc.index == 0 {
		pkVals := decodeKey(key, tDef, index)
		for i, col := range index {
			colIdx := slices.Index(tDef.Cols, col)
			rec.Vals[colIdx] = pkVals[i]
		}
		nonPkVals := make([]Value, len(tDef.Cols)-tDef.PKeys)
		for i := tDef.PKeys; i < len(tDef.Cols); i++ {
			nonPkVals[i-tDef.PKeys].Type = tDef.Types[i]
		}
		decodeValues(val, nonPkVals)
		for i := tDef.PKeys; i < len(tDef.Cols); i++ {
			rec.Vals[i] = nonPkVals[i-tDef.PKeys]
		}
	} else {
		pkCols := indexes[0]
		inIndex := make(map[string]bool, len(index))
		for _, col := range index {
			inIndex[col] = true
		}
		fullKeyCols := make([]string, len(index))
		copy(fullKeyCols, index)
		for _, pkCol := range pkCols {
			if !inIndex[pkCol] {
				fullKeyCols = append(fullKeyCols, pkCol)
			}
		}

		allVals := decodeKey(key, tDef, fullKeyCols)
		for i, col := range fullKeyCols {
			colIdx := slices.Index(tDef.Cols, col)
			rec.Vals[colIdx] = allVals[i]
		}

		pkRec := &Record{}
		for _, pkCol := range pkCols {
			colIdx := slices.Index(tDef.Cols, pkCol)
			pkRec.Cols = append(pkRec.Cols, pkCol)
			pkRec.Vals = append(pkRec.Vals, rec.Vals[colIdx])
		}
		ok, err := sc.db.dbGet(tDef, pkRec)
		if err != nil || !ok {
			panic("Deref: secondary index points to missing primary key")
		}
		*rec = *pkRec
	}
}

func (db *DB) dbGet(tDef *TableDef, rec *Record) (bool, error) {
	values, err := checkRecord(tDef, *rec, tDef.PKeys)
	if err != nil {
		return false, err
	}

	key := encodeKey(nil, tDef.Prefixes[0], values[:tDef.PKeys])

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

func (db *DB) dbUpdate(kv kvStore, tDef *TableDef, rec Record, mode int) (bool, error) {
	values, err := checkRecord(tDef, rec, len(tDef.Cols))
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tDef.Prefixes[0], values[:tDef.PKeys])
	val := encodeValues(nil, values[tDef.PKeys:])
	req := &UpdateReq{Key: key, Val: val, Mode: mode}
	if updated, e := kv.Update(req); !updated {
		return updated, e
	}
	indexes := tableIndexes(tDef)
	if req.Updated && !req.Added && len(indexes) > 1 {
		// row was modified: delete old secondary index keys using the old value
		oldNonPk := make([]Value, len(tDef.Cols)-tDef.PKeys)
		for i := tDef.PKeys; i < len(tDef.Cols); i++ {
			oldNonPk[i-tDef.PKeys].Type = tDef.Types[i]
		}
		decodeValues(req.Old, oldNonPk)
		oldValues := make([]Value, len(tDef.Cols))
		copy(oldValues[:tDef.PKeys], values[:tDef.PKeys])
		copy(oldValues[tDef.PKeys:], oldNonPk)
		for i := 1; i < len(indexes); i++ {
			if _, err = kv.Del(encodeIndexKey(tDef, i, oldValues)); err != nil {
				return false, err
			}
		}
	}
	if req.Updated && len(indexes) > 1 {
		// row was added or modified: insert new secondary index keys
		for i := 1; i < len(indexes); i++ {
			if err = kv.Set(encodeIndexKey(tDef, i, values), nil); err != nil {
				return false, err
			}
		}
	}
	return req.Updated, nil
}

func (db *DB) dbDelete(kv kvStore, tDef *TableDef, rec Record) (bool, error) {
	values, err := checkRecord(tDef, rec, tDef.PKeys)
	if err != nil {
		return false, err
	}
	key := encodeKey(nil, tDef.Prefixes[0], values[:tDef.PKeys])
	return kv.Del(key)
}

func dbScan(db *DB, tDef *TableDef, req *Scanner) error {
	indexes := tableIndexes(tDef)

	isCovered := func(index []string) bool {
		key := req.Key1.Cols
		return len(index) >= len(key) && slices.Equal(index[:len(key)], key)
	}
	req.index = slices.IndexFunc(indexes, isCovered)
	if req.index < 0 {
		return fmt.Errorf("no index covers the query columns")
	}

	index := indexes[req.index]
	prefix := tDef.Prefixes[req.index]

	vals1, err := indexVals(tDef, req.Key1, index)
	if err != nil {
		return err
	}
	vals2, err := indexVals(tDef, req.Key2, index)
	if err != nil {
		return err
	}

	keyStart := encodeKeyPartial(nil, prefix, vals1, req.Cmp1)
	req.keyEnd = encodeKeyPartial(nil, prefix, vals2, req.Cmp2)
	req.db = db
	req.tDef = tDef
	req.iter = db.kv.tree.Seek(keyStart, req.Cmp1)
	return nil
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

// encodeIndexKey builds the B+tree key for secondary index idx.
// The key is: prefix + index_cols + pk_cols_not_in_index (for uniqueness).
func encodeIndexKey(tDef *TableDef, idx int, values []Value) []byte {
	indexes := tableIndexes(tDef)
	pkCols := indexes[0]
	indexCols := indexes[idx]
	inIndex := make(map[string]bool, len(indexCols))
	for _, col := range indexCols {
		inIndex[col] = true
	}
	var keyVals []Value
	for _, col := range indexCols {
		keyVals = append(keyVals, values[slices.Index(tDef.Cols, col)])
	}
	for _, pkCol := range pkCols {
		if !inIndex[pkCol] {
			keyVals = append(keyVals, values[slices.Index(tDef.Cols, pkCol)])
		}
	}
	return encodeKey(nil, tDef.Prefixes[idx], keyVals)
}

func tableIndexes(tDef *TableDef) [][]string {
	if len(tDef.Indexes) > 0 {
		return tDef.Indexes
	}
	return [][]string{tDef.Cols[:tDef.PKeys]}
}

func encodeKeyPartial(out []byte, prefix uint32, vals []Value, cmp int) []byte {
	out = encodeKey(out, prefix, vals)
	if cmp == CmpGT || cmp == CmpLE {
		out = append(out, 0xff) // +∞ for missing columns
	}
	return out
}

func cmpOK(key []byte, cmp int, bound []byte) bool {
	r := bytes.Compare(key, bound)
	switch cmp {
	case CmpLE:
		return r <= 0
	case CmpLT:
		return r < 0
	case CmpGE:
		return r >= 0
	case CmpGT:
		return r > 0
	default:
		panic("invalid cmp")
	}
}

func indexVals(tDef *TableDef, rec Record, index []string) ([]Value, error) {
	vals := make([]Value, len(rec.Cols))
	for i := range rec.Cols {
		col := index[i]
		v := rec.Get(col)
		if v == nil {
			return nil, fmt.Errorf("missing column: %s", col)
		}
		colIdx := slices.Index(tDef.Cols, col)
		if colIdx < 0 {
			return nil, fmt.Errorf("unknown column: %s", col)
		}
		if v.Type != tDef.Types[colIdx] {
			return nil, fmt.Errorf("type mismatch for column: %s", col)
		}
		vals[i] = *v
	}
	return vals, nil
}

func decodeKey(key []byte, tDef *TableDef, cols []string) []Value {
	offset := 4
	vals := make([]Value, len(cols))
	for i, col := range cols {
		colIdx := slices.Index(tDef.Cols, col)
		typ := tDef.Types[colIdx]
		vals[i].Type = typ
		offset++ // skip type tag
		switch typ {
		case TypeInt64:
			u := binary.BigEndian.Uint64(key[offset:])
			vals[i].I64 = int64(u - (1 << 63))
			offset += 8
		case TypeBytes:
			var str []byte
			for offset < len(key) {
				b := key[offset]
				offset++
				if b == 0x00 {
					break
				}
				if b == 0x01 {
					str = append(str, key[offset]-0x01)
					offset++
				} else {
					str = append(str, b)
				}
			}
			vals[i].Str = str
		}
	}
	return vals
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
		offset++ // skip type tag
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
	out = append(out, byte(v.Type)) // type tag: ensures no encoded value starts with 0xff
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
