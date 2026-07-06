// Tokenizer + shape matchers for the eligibility recognizer. Mirrors the
// whole-token discipline of the engine's parser (arcx/src/parse.rs): the SQL is
// lexed into typed tokens and matched as an exact sequence, so no prefix/substring
// can mis-accept (the class of bug the engine's Phase 0 review caught:
// `read_parquet_foo`, `SELECTcount`, junk-before-parens).
//
// Vocabulary is deliberately narrow — only what the two eligible shapes contain.
// Anything outside it makes the whole input unrecognized (→ decline). This is the
// USER-facing shape (bare `FROM measurement`), distinct from the engine's
// `read_parquet(...)` form; the router recognizes here and rebuilds engine SQL
// from the parsed parts.

package arcxrouter

import (
	"strconv"
	"strings"
)

type tokKind int

const (
	tokIdent tokKind = iota // keyword or identifier, incl. dotted db.measurement
	tokStr                  // single-quoted string literal (date_trunc unit, WHERE string literal)
	tokNum                  // integer literal (GROUP BY 1, WHERE numeric literal, optional leading -)
	tokPunct                // one of ( ) * ,
	tokOp                   // comparison operator: = != <> < <= > >= (scan WHERE)
)

type token struct {
	kind tokKind
	// lower is the lowercased text for idents (keyword comparison); orig preserves
	// the original spelling (measurement name, unit literal) where it matters.
	lower string
	orig  string
	str   string // unescaped content for tokStr
	punct byte   // the char for tokPunct
	op    string // normalized comparison operator for tokOp: = != < <= > >=
}

// tokenize lexes sql into the narrow vocabulary. Returns ok=false on any
// character or construct outside it (which means "not our shape" → decline).
// A trailing ';' with only whitespace after is tolerated; anything else after it
// (a second statement) declines.
func tokenize(sql string) ([]token, bool) {
	var toks []token
	b := []byte(sql)
	n := len(b)
	i := 0
	for i < n {
		c := b[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == ';':
			// Only whitespace may follow — else it's multiple statements.
			if strings.TrimSpace(sql[i+1:]) != "" {
				return nil, false
			}
			i = n
		case c == '(' || c == ')' || c == '*' || c == ',':
			toks = append(toks, token{kind: tokPunct, punct: c})
			i++
		case c == '=':
			toks = append(toks, token{kind: tokOp, op: "="})
			i++
		case c == '<':
			// <=, <>, or <. Greedy so `< =` (spaced) never forms an operator.
			if i+1 < n && b[i+1] == '=' {
				toks = append(toks, token{kind: tokOp, op: "<="})
				i += 2
			} else if i+1 < n && b[i+1] == '>' {
				toks = append(toks, token{kind: tokOp, op: "!="}) // <> normalizes to !=
				i += 2
			} else {
				toks = append(toks, token{kind: tokOp, op: "<"})
				i++
			}
		case c == '>':
			if i+1 < n && b[i+1] == '=' {
				toks = append(toks, token{kind: tokOp, op: ">="})
				i += 2
			} else {
				toks = append(toks, token{kind: tokOp, op: ">"})
				i++
			}
		case c == '!':
			if i+1 < n && b[i+1] == '=' {
				toks = append(toks, token{kind: tokOp, op: "!="})
				i += 2
			} else {
				return nil, false // bare `!` is not in the vocabulary
			}
		case c == '-' && i+1 < n && isDigit(b[i+1]):
			// Negative integer literal (e.g. a pre-1970 epoch in a WHERE). The sign
			// is part of the token. A bare `-` (arithmetic) falls to default → decline.
			start := i
			i++ // consume '-'
			for i < n && isDigit(b[i]) {
				i++
			}
			toks = append(toks, token{kind: tokNum, orig: sql[start:i]})
		case isAlpha(c) || c == '_':
			// Identifier: letter/underscore start, then alnum/underscore/dot.
			// A dot is allowed INSIDE an identifier so `mydb.cpu` is one token —
			// the table reference form. Leading/trailing dots are rejected.
			start := i
			i++
			for i < n && (isAlnum(b[i]) || b[i] == '_' || b[i] == '.') {
				i++
			}
			orig := sql[start:i]
			if strings.HasPrefix(orig, ".") || strings.HasSuffix(orig, ".") || strings.Contains(orig, "..") {
				return nil, false
			}
			toks = append(toks, token{kind: tokIdent, lower: strings.ToLower(orig), orig: orig})
		case isDigit(c):
			start := i
			i++
			for i < n && isDigit(b[i]) {
				i++
			}
			toks = append(toks, token{kind: tokNum, orig: sql[start:i]})
		case c == '\'':
			lit, next, ok := lexString(sql, i)
			if !ok {
				return nil, false
			}
			toks = append(toks, token{kind: tokStr, str: lit})
			i = next
		default:
			// Anything else (operators, quotes, unicode, brackets) is outside the
			// vocabulary — decline rather than guess.
			return nil, false
		}
	}
	return toks, true
}

// lexString reads a single-quoted literal starting at start (a '). ” is an
// escaped quote (DuckDB semantics). Returns the content and the index past the
// closing quote, or ok=false if unterminated.
func lexString(sql string, start int) (string, int, bool) {
	b := []byte(sql)
	n := len(b)
	var out strings.Builder
	i := start + 1
	for i < n {
		if b[i] == '\'' {
			if i+1 < n && b[i+1] == '\'' {
				out.WriteByte('\'')
				i += 2
			} else {
				return out.String(), i + 1, true
			}
		} else {
			out.WriteByte(b[i])
			i++
		}
	}
	return "", 0, false
}

func isAlpha(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isAlnum(c byte) bool { return isAlpha(c) || isDigit(c) }

// --- matchers -------------------------------------------------------------

// cursor is a simple forward token reader.
type cursor struct {
	toks []token
	i    int
}

func (c *cursor) next() (token, bool) {
	if c.i >= len(c.toks) {
		return token{}, false
	}
	t := c.toks[c.i]
	c.i++
	return t, true
}

func (c *cursor) atEnd() bool { return c.i >= len(c.toks) }

func (c *cursor) ident(want string) bool {
	t, ok := c.next()
	return ok && t.kind == tokIdent && t.lower == want
}

func (c *cursor) punct(want byte) bool {
	t, ok := c.next()
	return ok && t.kind == tokPunct && t.punct == want
}

// numOne matches the literal `1` (GROUP BY 1 / ORDER BY 1).
func (c *cursor) numOne() bool {
	t, ok := c.next()
	return ok && t.kind == tokNum && t.orig == "1"
}

// matchCountStar matches: select count ( * ) from <measurement-ident>
// with nothing after. Returns the measurement token (as written) and ok.
func matchCountStar(toks []token) (string, bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") || !c.ident("count") || !c.punct('(') || !c.punct('*') || !c.punct(')') || !c.ident("from") {
		return "", false
	}
	t, ok := c.next()
	if !ok || t.kind != tokIdent {
		return "", false
	}
	meas := t.orig
	// Nothing may follow — a WHERE/GROUP BY/alias/JOIN means this isn't the bare
	// count-all shape; the footer path would silently ignore it. Decline.
	if !c.atEnd() {
		return "", false
	}
	return meas, true
}

// matchScalarAgg matches a single-scalar footer aggregate over a bare column:
//
//	select {min|max|count} ( <col> ) from <measurement>
//
// with nothing after. `count(*)` is NOT matched here — that's matchCountStar; the
// arg must be a bare identifier, not `*`. Returns (func, col-as-written,
// measurement, ok). `func` is one of "min", "max", "count".
func matchScalarAgg(toks []token) (fn, col, meas string, ok bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") {
		return "", "", "", false
	}
	f, ok := c.next()
	if !ok || f.kind != tokIdent {
		return "", "", "", false
	}
	switch f.lower {
	case "min", "max", "count":
		fn = f.lower
	default:
		return "", "", "", false
	}
	if !c.punct('(') {
		return "", "", "", false
	}
	// The argument must be a bare column identifier — NOT `*` (count(*) is the
	// existing CountStar shape) and NOT an expression.
	arg, ok := c.next()
	if !ok || arg.kind != tokIdent {
		return "", "", "", false
	}
	col = arg.orig
	if !c.punct(')') || !c.ident("from") {
		return "", "", "", false
	}
	m, ok := c.next()
	if !ok || m.kind != tokIdent {
		return "", "", "", false
	}
	meas = m.orig
	// Nothing may follow — a WHERE/GROUP BY/alias means the footer scalar path
	// would silently ignore it. Decline.
	if !c.atEnd() {
		return "", "", "", false
	}
	return fn, col, meas, true
}

// matchDateTruncCount matches:
//
//	select date_trunc ( '<unit>' , time ) , count ( * ) from <measurement>
//	group by 1  [order by 1]
//
// The bucket column must be exactly "time" (Arc's convention, F1) and the unit
// one of the supported set. Returns (unit-literal-as-written, measurement, ok).
func matchDateTruncCount(toks []token) (string, string, bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") || !c.ident("date_trunc") || !c.punct('(') {
		return "", "", false
	}
	// Unit literal.
	ut, ok := c.next()
	if !ok || ut.kind != tokStr {
		return "", "", false
	}
	unit := ut.str
	if !supportedUnits[strings.ToLower(unit)] {
		return "", "", false
	}
	if !c.punct(',') {
		return "", "", false
	}
	// Bucket column — must be the bare identifier "time".
	col, ok := c.next()
	if !ok || col.kind != tokIdent || col.lower != timeColumn {
		return "", "", false
	}
	if !c.punct(')') || !c.punct(',') || !c.ident("count") || !c.punct('(') || !c.punct('*') || !c.punct(')') || !c.ident("from") {
		return "", "", false
	}
	mt, ok := c.next()
	if !ok || mt.kind != tokIdent {
		return "", "", false
	}
	meas := mt.orig
	if !c.ident("group") || !c.ident("by") || !c.numOne() {
		return "", "", false
	}
	// Optional ORDER BY 1.
	if !c.atEnd() {
		if !c.ident("order") || !c.ident("by") || !c.numOne() {
			return "", "", false
		}
	}
	if !c.atEnd() {
		return "", "", false
	}
	return unit, meas, true
}

// op reads a comparison operator token.
func (c *cursor) op() (string, bool) {
	t, ok := c.next()
	if !ok || t.kind != tokOp {
		return "", false
	}
	return t.op, true
}

// peekIdentLower returns the lowercased next-token ident text without consuming,
// or "" if the next token isn't an ident.
func (c *cursor) peekIdentLower() string {
	if c.i >= len(c.toks) {
		return ""
	}
	t := c.toks[c.i]
	if t.kind != tokIdent {
		return ""
	}
	return t.lower
}

// matchScan matches the Phase 2a general single-table scan (user-facing form):
//
//	select <col> (, <col>)* from <measurement>
//	    [ where <col> <op> <lit> ( and <col> <op> <lit> )* ]
//
// Deliberately narrow, mirroring the engine's 2a grammar and its decline-harder
// posture — the router recognizes only what the engine answers green:
//   - projection is an explicit, non-empty BARE-COLUMN list. `*` is NOT routed
//     (the engine declines SELECT * under schema drift, which the router can't
//     detect ahead of the files — so star scans stay on DuckDB).
//   - WHERE is AND-conjoined `<col> <op> <literal>` only; OR/IN/LIKE/BETWEEN/
//     functions/arithmetic all fall outside the vocabulary → decline.
//   - no ORDER BY / LIMIT (the engine declines them in 2a). Anything trailing
//     the (optional) WHERE declines.
//
// The engine re-validates types, union_by_name, and the sandbox; the router's job
// is only to recognize the shape and hand over the parts. Returns the projected
// columns (as written), the predicates, the measurement, and ok.
func matchScan(toks []token) (cols []string, preds []scanPred, orderBy []scanOrderKey, limit int, meas string, ok bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") {
		return nil, nil, nil, 0, "", false
	}
	fail := func() ([]string, []scanPred, []scanOrderKey, int, string, bool) {
		return nil, nil, nil, 0, "", false
	}

	// Projection: one or more bare columns, comma-separated. A `*`, a function
	// call `col(`, or an alias all fall outside → decline.
	for {
		t, ok := c.next()
		if !ok || t.kind != tokIdent || isScanKeyword(t.lower) {
			return fail()
		}
		// A `(` immediately after would be a function call (expression) — 2b.
		if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
			return fail()
		}
		cols = append(cols, t.orig)
		if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ',' {
			c.i++
			continue
		}
		break
	}
	if !c.ident("from") {
		return fail()
	}
	mt, ok := c.next()
	if !ok || mt.kind != tokIdent {
		return fail()
	}
	meas = mt.orig

	// Optional WHERE (AND-conjoined <col> <op> <literal>).
	if c.peekIdentLower() == "where" {
		c.next()
		for {
			colT, ok := c.next()
			if !ok || colT.kind != tokIdent || isScanKeyword(colT.lower) {
				return fail()
			}
			opStr, ok := c.op()
			if !ok {
				return fail()
			}
			litT, ok := c.next()
			if !ok {
				return fail()
			}
			var p scanPred
			switch litT.kind {
			case tokNum:
				p = scanPred{col: colT.orig, op: opStr, num: litT.orig, isStr: false}
			case tokStr:
				p = scanPred{col: colT.orig, op: opStr, str: litT.str, isStr: true}
			default:
				return fail()
			}
			preds = append(preds, p)
			if c.peekIdentLower() == "and" {
				c.i++
				continue
			}
			break
		}
	}

	// Optional ORDER BY <col> [ASC|DESC] (, <col> [ASC|DESC])*. The engine serves
	// ORDER BY on int/µs columns only and declines strings/floats — but the router
	// recognizes the shape and lets the engine be the type authority (decline →
	// DuckDB). Positional ORDER BY (`ORDER BY 1`) is NOT this shape (numeric key);
	// it belongs to the agg matcher, so a Num here declines.
	if c.peekIdentLower() == "order" {
		c.next()
		if !c.ident("by") {
			return fail()
		}
		for {
			colT, ok := c.next()
			if !ok || colT.kind != tokIdent || isScanKeyword(colT.lower) {
				return fail()
			}
			desc := false
			switch c.peekIdentLower() {
			case "asc":
				c.next()
			case "desc":
				c.next()
				desc = true
			}
			orderBy = append(orderBy, scanOrderKey{col: colT.orig, desc: desc})
			if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ',' {
				c.i++
				continue
			}
			break
		}
	}

	// Optional LIMIT <n>. LIMIT without ORDER BY is nondeterministic — the engine
	// declines it, so the router does too (don't route a shape the engine won't run).
	if c.peekIdentLower() == "limit" {
		c.next()
		if len(orderBy) == 0 {
			return fail() // LIMIT without ORDER BY
		}
		nt, ok := c.next()
		if !ok || nt.kind != tokNum {
			return fail()
		}
		n, err := strconv.Atoi(nt.orig)
		if err != nil || n < 0 {
			return fail()
		}
		limit = n
		if limit == 0 {
			// `LIMIT 0` is a valid but degenerate shape; use a sentinel-free encoding
			// where 0 means "no limit". Route LIMIT 0 to DuckDB rather than conflate.
			return fail()
		}
	}

	if !c.atEnd() {
		return fail()
	}
	return cols, preds, orderBy, limit, meas, true
}

// isScanKeyword reports whether a lowercased ident is a clause keyword that must
// not be treated as a column in the scan grammar (guards `SELECT from FROM ...`).
func isScanKeyword(lower string) bool {
	switch lower {
	case "from", "where", "and", "or", "order", "by", "group", "limit", "having", "as":
		return true
	}
	return false
}
