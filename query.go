package main

import "fmt"

// RecordIter is a lazy iterator over decoded rows.
type RecordIter interface {
	Valid() bool
	Next()
	Deref(rec *Record) error
}

// ---- qlScanIter: Scanner + offset/limit/filter ----

type qlScanIter struct {
	req   *QLScan
	sc    Scanner
	total int64 // filter-passing rows seen so far
	done  bool
}

func newQLScanIter(req *QLScan, sc Scanner) *qlScanIter {
	it := &qlScanIter{req: req, sc: sc}
	it.seekValid()
	return it
}

func (it *qlScanIter) passFilter(rec Record) bool {
	if it.req.Filter.Type == 0 {
		return true
	}
	ctx := &QLEvalContext{env: rec}
	qlEval(ctx, it.req.Filter)
	return ctx.err == nil && ctx.out.I64 != 0
}

// seekValid advances sc until positioned at the next row to yield,
// respecting filter, offset, and limit.
func (it *qlScanIter) seekValid() {
	for it.sc.Valid() {
		var rec Record
		it.sc.Deref(&rec)
		if it.passFilter(rec) {
			if it.total < it.req.Offset {
				// within offset window, skip
				it.total++
				it.sc.Next()
				continue
			}
			if it.total-it.req.Offset >= it.req.Limit {
				it.done = true
				return
			}
			// valid yield position
			return
		}
		it.sc.Next()
	}
	it.done = true
}

func (it *qlScanIter) Valid() bool { return !it.done }

func (it *qlScanIter) Next() {
	if it.done {
		return
	}
	it.total++
	it.sc.Next()
	it.seekValid()
}

func (it *qlScanIter) Deref(rec *Record) error {
	it.sc.Deref(rec)
	return nil
}

// ---- qlSelectIter: evaluate SELECT expressions on each row ----

type qlSelectIter struct {
	iter  RecordIter
	names []string
	exprs []QLNode
}

func (it *qlSelectIter) Valid() bool { return it.iter.Valid() }
func (it *qlSelectIter) Next()       { it.iter.Next() }

func (it *qlSelectIter) Deref(rec *Record) error {
	var row Record
	if err := it.iter.Deref(&row); err != nil {
		return err
	}
	vals, err := qlEvalMulti(row, it.exprs)
	if err != nil {
		return err
	}
	*rec = Record{Cols: it.names, Vals: vals}
	return nil
}

// ---- qlScanInit: convert QLScan → Scanner keys + CMP values ----

// qlEvalScanKey extracts (Record, cmp) from a single comparison QLNode.
// The left operand must be a column name; right is a constant expression.
func qlEvalScanKey(node QLNode) (Record, int, error) {
	if node.Type == 0 {
		return Record{}, 0, nil
	}
	if len(node.Kids) != 2 {
		return Record{}, 0, fmt.Errorf("scan key must be a comparison expression")
	}
	if node.Kids[0].Type != QLSym {
		return Record{}, 0, fmt.Errorf("scan key left operand must be a column name")
	}
	col := string(node.Kids[0].Str)
	ctx := &QLEvalContext{}
	qlEval(ctx, node.Kids[1])
	if ctx.err != nil {
		return Record{}, 0, ctx.err
	}
	rec := Record{Cols: []string{col}, Vals: []Value{ctx.out}}
	var cmp int
	switch node.Type {
	case QLEq:
		cmp = CmpGE // overridden for QLEq case in qlScanInit
	case QLLt:
		cmp = CmpLT
	case QLGt:
		cmp = CmpGT
	case QLLe:
		cmp = CmpLE
	case QLGe:
		cmp = CmpGE
	default:
		return Record{}, 0, fmt.Errorf("invalid scan key operator: %d", node.Type)
	}
	return rec, cmp, nil
}

// qlScanInit converts a QLScan's Key1/Key2 QLNodes into Scanner Records + CMP values.
// Handles the 3 INDEX BY forms from chapter 14.
func qlScanInit(req *QLScan, sc *Scanner) error {
	key1, cmp1, err := qlEvalScanKey(req.Key1)
	if err != nil {
		return err
	}
	key2, cmp2, err := qlEvalScanKey(req.Key2)
	if err != nil {
		return err
	}
	sc.Key1 = key1
	sc.Key2 = key2
	sc.Cmp1 = cmp1
	sc.Cmp2 = cmp2

	switch {
	case req.Key1.Type == 0 && req.Key2.Type == 0:
		// no INDEX BY → full table scan
		sc.Cmp1, sc.Cmp2 = CmpGE, CmpLE
	case req.Key1.Type == QLEq && req.Key2.Type == 0:
		// INDEX BY key = val → prefix equality
		sc.Key2 = sc.Key1
		sc.Cmp1, sc.Cmp2 = CmpGE, CmpLE
	case req.Key1.Type != 0 && req.Key2.Type == 0:
		// INDEX BY key > val → open-ended
		// Key2 is empty record (encodes as ±∞ via encodeKeyPartial)
		if sc.Cmp1 > 0 { // forward: CmpGT or CmpGE
			sc.Cmp2 = CmpLE // +∞
		} else { // backward: CmpLT or CmpLE
			sc.Cmp2 = CmpGE // -∞
		}
	}
	return nil
}

// ---- Top-level query executors ----

func qlSelect(db *DB, stmt *QLSelect) (RecordIter, error) {
	tDef := getTableDef(db, stmt.Table)
	if tDef == nil {
		return nil, fmt.Errorf("table not found: %s", stmt.Table)
	}
	sc := Scanner{}
	if err := qlScanInit(&stmt.QLScan, &sc); err != nil {
		return nil, err
	}
	if err := dbScan(db, tDef, &sc); err != nil {
		return nil, err
	}
	scanIter := newQLScanIter(&stmt.QLScan, sc)
	return &qlSelectIter{
		iter:  scanIter,
		names: stmt.Names,
		exprs: stmt.Output,
	}, nil
}

func qlInsert(db *DB, stmt *QLInsert) (int, error) {
	count := 0
	for _, row := range stmt.Values {
		if len(row) != len(stmt.Cols) {
			return count, fmt.Errorf("column count mismatch: got %d values for %d columns", len(row), len(stmt.Cols))
		}
		vals, err := qlEvalMulti(Record{}, row)
		if err != nil {
			return count, err
		}
		rec := Record{Cols: stmt.Cols, Vals: vals}
		ok, err := db.Insert(stmt.Table, rec)
		if err != nil {
			return count, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

func qlUpdate(db *DB, stmt *QLUpdate) (int, error) {
	tDef := getTableDef(db, stmt.Table)
	if tDef == nil {
		return 0, fmt.Errorf("table not found: %s", stmt.Table)
	}
	sc := Scanner{}
	if err := qlScanInit(&stmt.QLScan, &sc); err != nil {
		return 0, err
	}
	if err := dbScan(db, tDef, &sc); err != nil {
		return 0, err
	}
	it := newQLScanIter(&stmt.QLScan, sc)
	count := 0
	for ; it.Valid(); it.Next() {
		var row Record
		it.sc.Deref(&row)
		newVals, err := qlEvalMulti(row, stmt.Values)
		if err != nil {
			return count, err
		}
		updated := Record{
			Cols: make([]string, len(row.Cols)),
			Vals: make([]Value, len(row.Vals)),
		}
		copy(updated.Cols, row.Cols)
		copy(updated.Vals, row.Vals)
		for i, name := range stmt.Names {
			for j, col := range updated.Cols {
				if col == name {
					updated.Vals[j] = newVals[i]
					break
				}
			}
		}
		ok, err := db.Update(stmt.Table, updated)
		if err != nil {
			return count, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

func qlDelete(db *DB, stmt *QLDelete) (int, error) {
	tDef := getTableDef(db, stmt.Table)
	if tDef == nil {
		return 0, fmt.Errorf("table not found: %s", stmt.Table)
	}
	sc := Scanner{}
	if err := qlScanInit(&stmt.QLScan, &sc); err != nil {
		return 0, err
	}
	if err := dbScan(db, tDef, &sc); err != nil {
		return 0, err
	}
	it := newQLScanIter(&stmt.QLScan, sc)
	count := 0
	for ; it.Valid(); it.Next() {
		var row Record
		it.sc.Deref(&row)
		pkRec := Record{}
		for i := 0; i < tDef.PKeys; i++ {
			col := tDef.Cols[i]
			v := row.Get(col)
			if v == nil {
				return count, fmt.Errorf("missing PK column: %s", col)
			}
			pkRec.Cols = append(pkRec.Cols, col)
			pkRec.Vals = append(pkRec.Vals, *v)
		}
		ok, err := db.Delete(stmt.Table, pkRec)
		if err != nil {
			return count, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

func qlCreateTable(db *DB, stmt *QLCreateTable) error {
	return db.TableNew(&stmt.Def)
}

// ExecSQL parses and executes a SQL-like statement.
// For SELECT returns a RecordIter; for others returns nil iter on success.
func ExecSQL(db *DB, input string) (RecordIter, error) {
	stmt, err := Parse(input)
	if err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *QLSelect:
		return qlSelect(db, s)
	case *QLInsert:
		_, err := qlInsert(db, s)
		return nil, err
	case *QLUpdate:
		_, err := qlUpdate(db, s)
		return nil, err
	case *QLDelete:
		_, err := qlDelete(db, s)
		return nil, err
	case *QLCreateTable:
		return nil, qlCreateTable(db, s)
	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}
