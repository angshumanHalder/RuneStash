package main

// QL node types
const (
	QLSym   uint32 = 1  // column name / identifier
	QLI64   uint32 = 2  // int64 literal
	QLStr   uint32 = 3  // string literal
	QLNeg   uint32 = 4  // unary -
	QLNot   uint32 = 5  // logical NOT
	QLOr    uint32 = 6  // OR
	QLAnd   uint32 = 7  // AND
	QLEq    uint32 = 8  // =
	QLNe    uint32 = 9  // !=
	QLLt    uint32 = 10 // <
	QLGt    uint32 = 11 // >
	QLLe    uint32 = 12 // <=
	QLGe    uint32 = 13 // >=
	QLAdd   uint32 = 14 // +
	QLSub   uint32 = 15 // -
	QLMul   uint32 = 16 // *
	QLDiv   uint32 = 17 // /
	QLTuple uint32 = 18 // (a, b, ...)
)

type QLNode struct {
	Type uint32 // tagged union
	I64  int64
	Str  []byte
	Kids []QLNode
}

type QLSelect struct {
	QLScan
	Names  []string
	Output []QLNode
}

type QLUpdate struct {
	QLScan
	Names  []string
	Values []QLNode
}

type QLDelete struct {
	QLScan
}

type QLInsert struct {
	Table  string
	Cols   []string
	Values [][]QLNode // multiple rows
}

type QLCreateTable struct {
	Def TableDef
}

type QLScan struct {
	Table  string // table name
	Key1   QLNode // index key
	Key2   QLNode
	Filter QLNode // filter expression
	Offset int64
	Limit  int64
}
