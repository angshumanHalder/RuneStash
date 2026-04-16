package main

import (
	"math"
	"testing"
)

// helpers

func mustParse(t *testing.T, input string) interface{} {
	t.Helper()
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse(%q) error: %v", input, err)
	}
	return stmt
}

func mustFail(t *testing.T, input string) {
	t.Helper()
	_, err := Parse(input)
	if err == nil {
		t.Fatalf("Parse(%q) expected error, got nil", input)
	}
}

func assertNode(t *testing.T, node QLNode, typ uint32) {
	t.Helper()
	if node.Type != typ {
		t.Errorf("node type: got %d, want %d", node.Type, typ)
	}
}

// ---- SELECT ----

func TestSelectBasic(t *testing.T) {
	stmt := mustParse(t, "select name from users").(*QLSelect)
	if stmt.Table != "users" {
		t.Errorf("table: got %q, want %q", stmt.Table, "users")
	}
	if len(stmt.Output) != 1 {
		t.Fatalf("output len: got %d, want 1", len(stmt.Output))
	}
	assertNode(t, stmt.Output[0], QLSym)
	if string(stmt.Output[0].Str) != "name" {
		t.Errorf("col: got %q, want %q", stmt.Output[0].Str, "name")
	}
	if stmt.Names[0] != "name" {
		t.Errorf("name: got %q, want %q", stmt.Names[0], "name")
	}
}

func TestSelectMultiCol(t *testing.T) {
	stmt := mustParse(t, "select a, b, c from t").(*QLSelect)
	if len(stmt.Output) != 3 {
		t.Fatalf("output len: got %d, want 3", len(stmt.Output))
	}
}

func TestSelectAlias(t *testing.T) {
	stmt := mustParse(t, "select age as years from users").(*QLSelect)
	if stmt.Names[0] != "years" {
		t.Errorf("alias: got %q, want %q", stmt.Names[0], "years")
	}
}

func TestSelectExpr(t *testing.T) {
	stmt := mustParse(t, "select a + b * 2 from t").(*QLSelect)
	assertNode(t, stmt.Output[0], QLAdd)
}

func TestSelectIndexBy(t *testing.T) {
	stmt := mustParse(t, "select name from users index by age > 18").(*QLSelect)
	assertNode(t, stmt.Key1, QLGt)
	if stmt.Key2.Type != 0 {
		t.Errorf("Key2 should be zero, got type %d", stmt.Key2.Type)
	}
}

func TestSelectIndexByRange(t *testing.T) {
	stmt := mustParse(t, "select name from users index by age > 18 and age < 65").(*QLSelect)
	assertNode(t, stmt.Key1, QLGt)
	assertNode(t, stmt.Key2, QLLt)
}

func TestSelectIndexByEqual(t *testing.T) {
	stmt := mustParse(t, "select name from users index by id = 42").(*QLSelect)
	assertNode(t, stmt.Key1, QLEq)
}

func TestSelectFilter(t *testing.T) {
	stmt := mustParse(t, "select name from users filter active = 1 and age > 18").(*QLSelect)
	assertNode(t, stmt.Filter, QLAnd)
}

func TestSelectLimit(t *testing.T) {
	stmt := mustParse(t, "select name from users limit 10").(*QLSelect)
	if stmt.Limit != 10 {
		t.Errorf("limit: got %d, want 10", stmt.Limit)
	}
	if stmt.Offset != 0 {
		t.Errorf("offset: got %d, want 0", stmt.Offset)
	}
}

func TestSelectLimitOffset(t *testing.T) {
	stmt := mustParse(t, "select name from users limit 20, 10").(*QLSelect)
	if stmt.Offset != 20 {
		t.Errorf("offset: got %d, want 20", stmt.Offset)
	}
	if stmt.Limit != 10 {
		t.Errorf("limit: got %d, want 10", stmt.Limit)
	}
}

func TestSelectDefaultLimit(t *testing.T) {
	stmt := mustParse(t, "select name from users").(*QLSelect)
	if stmt.Limit != math.MaxInt64 {
		t.Errorf("default limit: got %d, want MaxInt64", stmt.Limit)
	}
}

func TestSelectAll(t *testing.T) {
	stmt := mustParse(t, "select a from t index by x > 1 and x < 9 filter y = 2 limit 5, 10").(*QLSelect)
	assertNode(t, stmt.Key1, QLGt)
	assertNode(t, stmt.Key2, QLLt)
	assertNode(t, stmt.Filter, QLEq)
	if stmt.Offset != 5 || stmt.Limit != 10 {
		t.Errorf("limit/offset: got %d/%d, want 5/10", stmt.Offset, stmt.Limit)
	}
}

func TestSelectTrailingSemicolon(t *testing.T) {
	mustParse(t, "select a from t;")
}

// ---- INSERT ----

func TestInsertSingleRow(t *testing.T) {
	stmt := mustParse(t, "insert into users (name, age) values ('alice', 30)").(*QLInsert)
	if stmt.Table != "users" {
		t.Errorf("table: got %q", stmt.Table)
	}
	if len(stmt.Cols) != 2 || stmt.Cols[0] != "name" || stmt.Cols[1] != "age" {
		t.Errorf("cols: got %v", stmt.Cols)
	}
	if len(stmt.Values) != 1 || len(stmt.Values[0]) != 2 {
		t.Fatalf("values shape wrong")
	}
	assertNode(t, stmt.Values[0][0], QLStr)
	assertNode(t, stmt.Values[0][1], QLI64)
}

func TestInsertMultiRow(t *testing.T) {
	stmt := mustParse(t, "insert into t (a) values (1), (2), (3)").(*QLInsert)
	if len(stmt.Values) != 3 {
		t.Errorf("row count: got %d, want 3", len(stmt.Values))
	}
}

func TestInsertNegative(t *testing.T) {
	stmt := mustParse(t, "insert into t (n) values (-5)").(*QLInsert)
	assertNode(t, stmt.Values[0][0], QLNeg)
}

// ---- UPDATE ----

func TestUpdateBasic(t *testing.T) {
	stmt := mustParse(t, "update users set age = 31").(*QLUpdate)
	if stmt.Table != "users" {
		t.Errorf("table: got %q", stmt.Table)
	}
	if len(stmt.Names) != 1 || stmt.Names[0] != "age" {
		t.Errorf("names: got %v", stmt.Names)
	}
	assertNode(t, stmt.Values[0], QLI64)
}

func TestUpdateMultiCol(t *testing.T) {
	stmt := mustParse(t, "update t set a = 1, b = 'x'").(*QLUpdate)
	if len(stmt.Names) != 2 {
		t.Errorf("names len: got %d, want 2", len(stmt.Names))
	}
}

func TestUpdateWithExpr(t *testing.T) {
	stmt := mustParse(t, "update t set score = score + 1").(*QLUpdate)
	assertNode(t, stmt.Values[0], QLAdd)
}

func TestUpdateWithScan(t *testing.T) {
	stmt := mustParse(t, "update users set active = 0 filter age < 18").(*QLUpdate)
	assertNode(t, stmt.Filter, QLLt)
}

// ---- DELETE ----

func TestDeleteBasic(t *testing.T) {
	stmt := mustParse(t, "delete from users").(*QLDelete)
	if stmt.Table != "users" {
		t.Errorf("table: got %q", stmt.Table)
	}
}

func TestDeleteWithIndexBy(t *testing.T) {
	stmt := mustParse(t, "delete from users index by id = 5").(*QLDelete)
	assertNode(t, stmt.Key1, QLEq)
}

func TestDeleteWithFilter(t *testing.T) {
	stmt := mustParse(t, "delete from users filter active = 0").(*QLDelete)
	assertNode(t, stmt.Filter, QLEq)
}

func TestDeleteWithLimit(t *testing.T) {
	stmt := mustParse(t, "delete from t limit 100").(*QLDelete)
	if stmt.Limit != 100 {
		t.Errorf("limit: got %d, want 100", stmt.Limit)
	}
}

// ---- CREATE TABLE ----

func TestCreateTableBasic(t *testing.T) {
	stmt := mustParse(t, `create table users (
		id int,
		name string,
		primary key (id)
	)`).(*QLCreateTable)

	if stmt.Def.Name != "users" {
		t.Errorf("name: got %q", stmt.Def.Name)
	}
	if stmt.Def.PKeys != 1 {
		t.Errorf("PKeys: got %d, want 1", stmt.Def.PKeys)
	}
	if stmt.Def.Cols[0] != "id" {
		t.Errorf("first col should be pk: got %q", stmt.Def.Cols[0])
	}
}

func TestCreateTableCompoundPK(t *testing.T) {
	stmt := mustParse(t, `create table t (
		a int,
		b string,
		v string,
		primary key (a, b)
	)`).(*QLCreateTable)

	if stmt.Def.PKeys != 2 {
		t.Errorf("PKeys: got %d, want 2", stmt.Def.PKeys)
	}
	if stmt.Def.Cols[0] != "a" || stmt.Def.Cols[1] != "b" {
		t.Errorf("PK cols not first: %v", stmt.Def.Cols)
	}
}

func TestCreateTableWithIndex(t *testing.T) {
	stmt := mustParse(t, `create table t (
		id int,
		name string,
		age int,
		index (age),
		primary key (id)
	)`).(*QLCreateTable)

	if len(stmt.Def.Indexes) != 2 {
		t.Errorf("indexes: got %d, want 2", len(stmt.Def.Indexes))
	}
	// first index is primary key
	if stmt.Def.Indexes[0][0] != "id" {
		t.Errorf("first index should be PK: got %v", stmt.Def.Indexes[0])
	}
}

// ---- Expressions ----

func TestExprPrecedence(t *testing.T) {
	// a + b * c should parse as a + (b * c)
	stmt := mustParse(t, "select a + b * c from t").(*QLSelect)
	root := stmt.Output[0]
	assertNode(t, root, QLAdd)
	assertNode(t, root.Kids[1], QLMul)
}

func TestExprUnaryNeg(t *testing.T) {
	stmt := mustParse(t, "select -a from t").(*QLSelect)
	assertNode(t, stmt.Output[0], QLNeg)
}

func TestExprNot(t *testing.T) {
	stmt := mustParse(t, "select a from t filter not a = 1").(*QLSelect)
	assertNode(t, stmt.Filter, QLNot)
}

func TestExprOrAnd(t *testing.T) {
	// a OR b AND c should parse as a OR (b AND c)
	stmt := mustParse(t, "select a from t filter x = 1 or y = 2 and z = 3").(*QLSelect)
	assertNode(t, stmt.Filter, QLOr)
	assertNode(t, stmt.Filter.Kids[1], QLAnd)
}

func TestExprString(t *testing.T) {
	stmt := mustParse(t, "select a from t filter name = 'alice'").(*QLSelect)
	assertNode(t, stmt.Filter, QLEq)
	assertNode(t, stmt.Filter.Kids[1], QLStr)
	if string(stmt.Filter.Kids[1].Str) != "alice" {
		t.Errorf("string: got %q", stmt.Filter.Kids[1].Str)
	}
}

func TestExprStringEscapedQuote(t *testing.T) {
	stmt := mustParse(t, "select a from t filter name = 'o''brien'").(*QLSelect)
	if string(stmt.Filter.Kids[1].Str) != "o'brien" {
		t.Errorf("escaped quote: got %q", stmt.Filter.Kids[1].Str)
	}
}

func TestExprParens(t *testing.T) {
	// (a + b) * c
	stmt := mustParse(t, "select (a + b) * c from t").(*QLSelect)
	assertNode(t, stmt.Output[0], QLMul)
	assertNode(t, stmt.Output[0].Kids[0], QLAdd)
}

func TestExprAllCmpOps(t *testing.T) {
	ops := []struct {
		sql string
		typ uint32
	}{
		{"a = b", QLEq},
		{"a != b", QLNe},
		{"a < b", QLLt},
		{"a > b", QLGt},
		{"a <= b", QLLe},
		{"a >= b", QLGe},
	}
	for _, tc := range ops {
		stmt := mustParse(t, "select x from t filter "+tc.sql).(*QLSelect)
		assertNode(t, stmt.Filter, tc.typ)
	}
}

// ---- Error cases ----

func TestErrMissingFrom(t *testing.T) {
	mustFail(t, "select a users")
}

func TestErrMissingTable(t *testing.T) {
	mustFail(t, "select a from")
}

func TestErrBadInsert(t *testing.T) {
	mustFail(t, "insert users (a) values (1)")
}

func TestErrUnterminatedString(t *testing.T) {
	mustFail(t, "select a from t filter name = 'unclosed")
}

func TestErrUnknownStatement(t *testing.T) {
	mustFail(t, "drop table users")
}

func TestErrTrailingGarbage(t *testing.T) {
	mustFail(t, "select a from t garbage")
}
