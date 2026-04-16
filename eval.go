package main

import (
	"bytes"
	"fmt"
)

type QLEvalContext struct {
	env Record
	out Value
	err error
}

func qlErr(ctx *QLEvalContext, format string, args ...any) {
	if ctx.err == nil {
		ctx.err = fmt.Errorf(format, args...)
	}
}

func qlEval(ctx *QLEvalContext, node QLNode) {
	if ctx.err != nil {
		return
	}
	switch node.Type {
	case QLSym:
		v := ctx.env.Get(string(node.Str))
		if v == nil {
			qlErr(ctx, "unknown column: %s", node.Str)
			return
		}
		ctx.out = *v
	case QLI64:
		ctx.out = Value{Type: TypeInt64, I64: node.I64}
	case QLStr:
		ctx.out = Value{Type: TypeBytes, Str: node.Str}
	case QLNeg:
		qlEval(ctx, node.Kids[0])
		if ctx.err != nil || ctx.out.Type != TypeInt64 {
			qlErr(ctx, "NEG: expected int64")
			return
		}
		ctx.out.I64 = -ctx.out.I64
	case QLNot:
		qlEval(ctx, node.Kids[0])
		if ctx.err != nil {
			return
		}
		if ctx.out.I64 == 0 {
			ctx.out = Value{Type: TypeInt64, I64: 1}
		} else {
			ctx.out = Value{Type: TypeInt64, I64: 0}
		}
	case QLAdd, QLSub, QLMul, QLDiv:
		qlEvalArith(ctx, node)
	case QLEq, QLNe, QLLt, QLGt, QLLe, QLGe:
		qlEvalCmp(ctx, node)
	case QLAnd:
		qlEval(ctx, node.Kids[0])
		if ctx.err != nil {
			return
		}
		if ctx.out.I64 == 0 {
			ctx.out = Value{Type: TypeInt64, I64: 0}
			return
		}
		qlEval(ctx, node.Kids[1])
		if ctx.err != nil {
			return
		}
		if ctx.out.I64 != 0 {
			ctx.out = Value{Type: TypeInt64, I64: 1}
		} else {
			ctx.out = Value{Type: TypeInt64, I64: 0}
		}
	case QLOr:
		qlEval(ctx, node.Kids[0])
		if ctx.err != nil {
			return
		}
		if ctx.out.I64 != 0 {
			ctx.out = Value{Type: TypeInt64, I64: 1}
			return
		}
		qlEval(ctx, node.Kids[1])
		if ctx.err != nil {
			return
		}
		if ctx.out.I64 != 0 {
			ctx.out = Value{Type: TypeInt64, I64: 1}
		} else {
			ctx.out = Value{Type: TypeInt64, I64: 0}
		}
	default:
		qlErr(ctx, "unknown node type: %d", node.Type)
	}
}

func qlEvalArith(ctx *QLEvalContext, node QLNode) {
	qlEval(ctx, node.Kids[0])
	if ctx.err != nil {
		return
	}
	left := ctx.out
	qlEval(ctx, node.Kids[1])
	if ctx.err != nil {
		return
	}
	right := ctx.out
	if left.Type != TypeInt64 || right.Type != TypeInt64 {
		qlErr(ctx, "arithmetic requires int64 operands")
		return
	}
	ctx.out = Value{Type: TypeInt64}
	switch node.Type {
	case QLAdd:
		ctx.out.I64 = left.I64 + right.I64
	case QLSub:
		ctx.out.I64 = left.I64 - right.I64
	case QLMul:
		ctx.out.I64 = left.I64 * right.I64
	case QLDiv:
		if right.I64 == 0 {
			qlErr(ctx, "division by zero")
			return
		}
		ctx.out.I64 = left.I64 / right.I64
	}
}

func qlEvalCmp(ctx *QLEvalContext, node QLNode) {
	qlEval(ctx, node.Kids[0])
	if ctx.err != nil {
		return
	}
	left := ctx.out
	qlEval(ctx, node.Kids[1])
	if ctx.err != nil {
		return
	}
	right := ctx.out
	if left.Type != right.Type {
		qlErr(ctx, "comparison type mismatch: %d vs %d", left.Type, right.Type)
		return
	}
	var cmp int
	switch left.Type {
	case TypeInt64:
		if left.I64 < right.I64 {
			cmp = -1
		} else if left.I64 > right.I64 {
			cmp = 1
		}
	case TypeBytes:
		cmp = bytes.Compare(left.Str, right.Str)
	default:
		qlErr(ctx, "unsupported type for comparison: %d", left.Type)
		return
	}
	var result int64
	switch node.Type {
	case QLEq:
		if cmp == 0 {
			result = 1
		}
	case QLNe:
		if cmp != 0 {
			result = 1
		}
	case QLLt:
		if cmp < 0 {
			result = 1
		}
	case QLGt:
		if cmp > 0 {
			result = 1
		}
	case QLLe:
		if cmp <= 0 {
			result = 1
		}
	case QLGe:
		if cmp >= 0 {
			result = 1
		}
	}
	ctx.out = Value{Type: TypeInt64, I64: result}
}

// qlEvalMulti evaluates multiple QLNodes against a row, returning their values.
func qlEvalMulti(env Record, nodes []QLNode) ([]Value, error) {
	ctx := &QLEvalContext{env: env}
	vals := make([]Value, len(nodes))
	for i, node := range nodes {
		qlEval(ctx, node)
		if ctx.err != nil {
			return nil, ctx.err
		}
		vals[i] = ctx.out
		ctx.out = Value{}
	}
	return vals, nil
}
