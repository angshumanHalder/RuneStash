package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Parser struct {
	input string
	pos   int
	err   error
}

func newParser(input string) *Parser {
	return &Parser{input: input}
}

func (p *Parser) skipWS() {
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			p.pos++
		} else {
			break
		}
	}
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isAlNum(c byte) bool {
	return isAlpha(c) || (c >= '0' && c <= '9')
}

// pKeyword matches and consumes one or more keywords (case-insensitive).
// Restores position on failure.
func pKeyword(p *Parser, words ...string) bool {
	if p.err != nil {
		return false
	}
	saved := p.pos
	for _, word := range words {
		p.skipWS()
		end := p.pos + len(word)
		if end > len(p.input) {
			p.pos = saved
			return false
		}
		if !strings.EqualFold(p.input[p.pos:end], word) {
			p.pos = saved
			return false
		}
		// word boundary: must not be followed by alphanumeric/_
		if end < len(p.input) && isAlNum(p.input[end]) {
			p.pos = saved
			return false
		}
		p.pos = end
	}
	return true
}

// pSym parses an identifier.
func pSym(p *Parser) (string, bool) {
	if p.err != nil {
		return "", false
	}
	p.skipWS()
	if p.pos >= len(p.input) || !isAlpha(p.input[p.pos]) {
		return "", false
	}
	start := p.pos
	for p.pos < len(p.input) && isAlNum(p.input[p.pos]) {
		p.pos++
	}
	return p.input[start:p.pos], true
}

func pMustSym(p *Parser) string {
	name, ok := pSym(p)
	if !ok && p.err == nil {
		p.err = fmt.Errorf("expected identifier at pos %d", p.pos)
	}
	return name
}

// pPunct matches and consumes a punctuation token.
func pPunct(p *Parser, sym string) bool {
	if p.err != nil {
		return false
	}
	p.skipWS()
	if p.pos+len(sym) > len(p.input) {
		return false
	}
	if p.input[p.pos:p.pos+len(sym)] == sym {
		p.pos += len(sym)
		return true
	}
	return false
}

// pExpect consumes a keyword or punctuation, sets error on failure.
func pExpect(p *Parser, s string, msg string) {
	if p.err != nil {
		return
	}
	var ok bool
	if len(s) > 0 && isAlpha(s[0]) {
		ok = pKeyword(p, s)
	} else {
		ok = pPunct(p, s)
	}
	if !ok {
		p.err = fmt.Errorf("%s at pos %d", msg, p.pos)
	}
}

// pNum parses a non-negative integer literal.
func pNum(p *Parser) (int64, bool) {
	if p.err != nil {
		return 0, false
	}
	p.skipWS()
	if p.pos >= len(p.input) || p.input[p.pos] < '0' || p.input[p.pos] > '9' {
		return 0, false
	}
	start := p.pos
	for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
		p.pos++
	}
	v, err := strconv.ParseInt(p.input[start:p.pos], 10, 64)
	if err != nil {
		p.pos = start
		return 0, false
	}
	return v, true
}

// pStr parses a single-quoted string literal; ” escapes a literal quote.
func pStr(p *Parser) ([]byte, bool) {
	if p.err != nil {
		return nil, false
	}
	p.skipWS()
	if p.pos >= len(p.input) || p.input[p.pos] != '\'' {
		return nil, false
	}
	p.pos++
	var buf []byte
	for p.pos < len(p.input) {
		c := p.input[p.pos]
		p.pos++
		if c == '\'' {
			if p.pos < len(p.input) && p.input[p.pos] == '\'' {
				buf = append(buf, '\'')
				p.pos++
			} else {
				return buf, true
			}
		} else {
			buf = append(buf, c)
		}
	}
	p.err = fmt.Errorf("unterminated string at pos %d", p.pos)
	return nil, false
}

// ---- Expression parsing (recursive descent by precedence) ----

func pExprAtom(p *Parser, node *QLNode) {
	if p.err != nil {
		return
	}
	// parenthesized expression or tuple
	if pPunct(p, "(") {
		pExprOr(p, node)
		if p.err != nil {
			return
		}
		if pPunct(p, ",") {
			kids := []QLNode{*node}
			var next QLNode
			pExprOr(p, &next)
			kids = append(kids, next)
			for p.err == nil && pPunct(p, ",") {
				var n QLNode
				pExprOr(p, &n)
				kids = append(kids, n)
			}
			*node = QLNode{Type: QLTuple, Kids: kids}
		}
		pExpect(p, ")", "expected )")
		return
	}
	if v, ok := pNum(p); ok {
		*node = QLNode{Type: QLI64, I64: v}
		return
	}
	if s, ok := pStr(p); ok {
		*node = QLNode{Type: QLStr, Str: s}
		return
	}
	name, ok := pSym(p)
	if !ok {
		if p.err == nil {
			p.err = fmt.Errorf("expected expression at pos %d", p.pos)
		}
		return
	}
	*node = QLNode{Type: QLSym, Str: []byte(name)}
}

// pExprUnop handles unary -
func pExprUnop(p *Parser, node *QLNode) {
	if p.err != nil {
		return
	}
	if pPunct(p, "-") {
		var kid QLNode
		pExprAtom(p, &kid)
		*node = QLNode{Type: QLNeg, Kids: []QLNode{kid}}
		return
	}
	pExprAtom(p, node)
}

func pExprMul(p *Parser, node *QLNode) {
	pExprUnop(p, node)
	for p.err == nil {
		var op uint32
		switch {
		case pPunct(p, "*"):
			op = QLMul
		case pPunct(p, "/"):
			op = QLDiv
		default:
			return
		}
		var right QLNode
		pExprUnop(p, &right)
		*node = QLNode{Type: op, Kids: []QLNode{*node, right}}
	}
}

func pExprAdd(p *Parser, node *QLNode) {
	pExprMul(p, node)
	for p.err == nil {
		var op uint32
		switch {
		case pPunct(p, "+"):
			op = QLAdd
		case pPunct(p, "-"):
			op = QLSub
		default:
			return
		}
		var right QLNode
		pExprMul(p, &right)
		*node = QLNode{Type: op, Kids: []QLNode{*node, right}}
	}
}

func pExprCmp(p *Parser, node *QLNode) {
	pExprAdd(p, node)
	if p.err != nil {
		return
	}
	// check 2-char ops before 1-char to avoid partial matches
	var op uint32
	switch {
	case pPunct(p, "!="):
		op = QLNe
	case pPunct(p, "<="):
		op = QLLe
	case pPunct(p, ">="):
		op = QLGe
	case pPunct(p, "<"):
		op = QLLt
	case pPunct(p, ">"):
		op = QLGt
	case pPunct(p, "="):
		op = QLEq
	default:
		return
	}
	var right QLNode
	pExprAdd(p, &right)
	*node = QLNode{Type: op, Kids: []QLNode{*node, right}}
}

func pExprNot(p *Parser, node *QLNode) {
	if p.err != nil {
		return
	}
	if pKeyword(p, "not") {
		var kid QLNode
		pExprCmp(p, &kid)
		*node = QLNode{Type: QLNot, Kids: []QLNode{kid}}
		return
	}
	pExprCmp(p, node)
}

func pExprAnd(p *Parser, node *QLNode) {
	pExprNot(p, node)
	for p.err == nil && pKeyword(p, "and") {
		var right QLNode
		pExprNot(p, &right)
		*node = QLNode{Type: QLAnd, Kids: []QLNode{*node, right}}
	}
}

// pExprOr is the lowest-precedence expression parser.
func pExprOr(p *Parser, node *QLNode) {
	pExprAnd(p, node)
	for p.err == nil && pKeyword(p, "or") {
		var right QLNode
		pExprAnd(p, &right)
		*node = QLNode{Type: QLOr, Kids: []QLNode{*node, right}}
	}
}

// ---- Scan clauses (INDEX BY / FILTER / LIMIT) ----

// pIndexBy parses: cmp1 [AND cmp2]
// Each cmp must be a simple comparison expression.
func pIndexBy(p *Parser, node *QLScan) {
	pExprCmp(p, &node.Key1)
	if p.err != nil {
		return
	}
	if pKeyword(p, "and") {
		pExprCmp(p, &node.Key2)
	}
}

func pLimit(p *Parser, node *QLScan) {
	v1, ok := pNum(p)
	if !ok {
		p.err = fmt.Errorf("expected number in LIMIT at pos %d", p.pos)
		return
	}
	if pPunct(p, ",") {
		v2, ok2 := pNum(p)
		if !ok2 {
			p.err = fmt.Errorf("expected count after , in LIMIT at pos %d", p.pos)
			return
		}
		node.Offset = v1
		node.Limit = v2
	} else {
		node.Offset = 0
		node.Limit = v1
	}
}

func pScan(p *Parser, node *QLScan) {
	if p.err != nil {
		return
	}
	if pKeyword(p, "index", "by") {
		pIndexBy(p, node)
	}
	if p.err == nil && pKeyword(p, "filter") {
		pExprOr(p, &node.Filter)
	}
	node.Offset = 0
	node.Limit = math.MaxInt64
	if p.err == nil && pKeyword(p, "limit") {
		pLimit(p, node)
	}
}

// ---- SELECT ----

func pSelectExpr(p *Parser, stmt *QLSelect) {
	if p.err != nil {
		return
	}
	var node QLNode
	pExprOr(p, &node)
	if p.err != nil {
		return
	}
	name := ""
	if pKeyword(p, "as") {
		name = pMustSym(p)
	} else if node.Type == QLSym {
		name = string(node.Str)
	}
	stmt.Output = append(stmt.Output, node)
	stmt.Names = append(stmt.Names, name)
}

func pSelectExprList(p *Parser, stmt *QLSelect) {
	pSelectExpr(p, stmt)
	for p.err == nil && pPunct(p, ",") {
		pSelectExpr(p, stmt)
	}
}

func pSelect(p *Parser) *QLSelect {
	stmt := &QLSelect{}
	pSelectExprList(p, stmt)
	pExpect(p, "from", "expected FROM")
	stmt.Table = pMustSym(p)
	pScan(p, &stmt.QLScan)
	return stmt
}

// ---- INSERT ----

func pInsert(p *Parser) *QLInsert {
	stmt := &QLInsert{}
	pExpect(p, "into", "expected INTO after INSERT")
	stmt.Table = pMustSym(p)
	pExpect(p, "(", "expected ( for column list")
	stmt.Cols = append(stmt.Cols, pMustSym(p))
	for p.err == nil && pPunct(p, ",") {
		stmt.Cols = append(stmt.Cols, pMustSym(p))
	}
	pExpect(p, ")", "expected ) after column list")
	pExpect(p, "values", "expected VALUES")
	for p.err == nil {
		pExpect(p, "(", "expected ( for value row")
		var row []QLNode
		var v QLNode
		pExprOr(p, &v)
		if p.err != nil {
			break
		}
		row = append(row, v)
		for p.err == nil && pPunct(p, ",") {
			var n QLNode
			pExprOr(p, &n)
			row = append(row, n)
		}
		pExpect(p, ")", "expected ) after value row")
		stmt.Values = append(stmt.Values, row)
		if !pPunct(p, ",") {
			break
		}
	}
	return stmt
}

// ---- UPDATE ----

func pUpdate(p *Parser) *QLUpdate {
	stmt := &QLUpdate{}
	stmt.Table = pMustSym(p)
	pExpect(p, "set", "expected SET")
	name := pMustSym(p)
	pExpect(p, "=", "expected = in SET")
	var val QLNode
	pExprOr(p, &val)
	stmt.Names = append(stmt.Names, name)
	stmt.Values = append(stmt.Values, val)
	for p.err == nil && pPunct(p, ",") {
		n := pMustSym(p)
		pExpect(p, "=", "expected = in SET")
		var v QLNode
		pExprOr(p, &v)
		stmt.Names = append(stmt.Names, n)
		stmt.Values = append(stmt.Values, v)
	}
	pScan(p, &stmt.QLScan)
	return stmt
}

// ---- DELETE ----

func pDelete(p *Parser) *QLDelete {
	stmt := &QLDelete{}
	pExpect(p, "from", "expected FROM after DELETE")
	stmt.Table = pMustSym(p)
	pScan(p, &stmt.QLScan)
	return stmt
}

// ---- CREATE TABLE ----

func pCreateTable(p *Parser) *QLCreateTable {
	stmt := &QLCreateTable{}
	stmt.Def.Name = pMustSym(p)
	pExpect(p, "(", "expected (")
	var primaryKey []string
	for p.err == nil {
		if pKeyword(p, "primary", "key") {
			pExpect(p, "(", "expected ( after PRIMARY KEY")
			primaryKey = append(primaryKey, pMustSym(p))
			for p.err == nil && pPunct(p, ",") {
				primaryKey = append(primaryKey, pMustSym(p))
			}
			pExpect(p, ")", "expected ) after PRIMARY KEY")
		} else if pKeyword(p, "index") {
			pExpect(p, "(", "expected ( after INDEX")
			var cols []string
			cols = append(cols, pMustSym(p))
			for p.err == nil && pPunct(p, ",") {
				cols = append(cols, pMustSym(p))
			}
			pExpect(p, ")", "expected ) after INDEX")
			stmt.Def.Indexes = append(stmt.Def.Indexes, cols)
		} else {
			name, ok := pSym(p)
			if !ok {
				break
			}
			typeName, _ := pSym(p)
			var t uint32
			switch strings.ToLower(typeName) {
			case "int", "int64", "integer":
				t = TypeInt64
			default:
				t = TypeBytes
			}
			stmt.Def.Cols = append(stmt.Def.Cols, name)
			stmt.Def.Types = append(stmt.Def.Types, t)
		}
		if !pPunct(p, ",") {
			break
		}
	}
	pExpect(p, ")", "expected ) to close CREATE TABLE")
	if p.err != nil {
		return stmt
	}
	// primary key becomes first index; reorder Cols so PK cols come first
	if len(primaryKey) > 0 {
		stmt.Def.Indexes = append([][]string{primaryKey}, stmt.Def.Indexes...)
		stmt.Def.PKeys = len(primaryKey)
		inPK := make(map[string]bool, len(primaryKey))
		for _, pk := range primaryKey {
			inPK[pk] = true
		}
		var ordCols []string
		var ordTypes []uint32
		for _, pk := range primaryKey {
			for i, c := range stmt.Def.Cols {
				if c == pk {
					ordCols = append(ordCols, c)
					ordTypes = append(ordTypes, stmt.Def.Types[i])
					break
				}
			}
		}
		for i, c := range stmt.Def.Cols {
			if !inPK[c] {
				ordCols = append(ordCols, c)
				ordTypes = append(ordTypes, stmt.Def.Types[i])
			}
		}
		stmt.Def.Cols = ordCols
		stmt.Def.Types = ordTypes
	}
	return stmt
}

// ---- pStmt: top-level dispatcher ----

func pStmt(p *Parser) interface{} {
	switch {
	case pKeyword(p, "create", "table"):
		return pCreateTable(p)
	case pKeyword(p, "select"):
		return pSelect(p)
	case pKeyword(p, "insert"):
		return pInsert(p)
	case pKeyword(p, "update"):
		return pUpdate(p)
	case pKeyword(p, "delete"):
		return pDelete(p)
	default:
		p.err = fmt.Errorf("unknown statement at pos %d", p.pos)
		return nil
	}
}

// Parse parses a SQL-like statement string, returning the AST node or an error.
func Parse(input string) (interface{}, error) {
	p := newParser(input)
	stmt := pStmt(p)
	if p.err != nil {
		return nil, p.err
	}
	p.skipWS()
	pPunct(p, ";")
	p.skipWS()
	if p.pos < len(p.input) {
		return nil, fmt.Errorf("unexpected input at pos %d: %q", p.pos, p.input[p.pos:])
	}
	return stmt, nil
}
