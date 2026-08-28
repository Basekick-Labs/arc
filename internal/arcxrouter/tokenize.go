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
	tokIdent  tokKind = iota // keyword or identifier, incl. dotted db.measurement
	tokStr                   // single-quoted string literal (date_trunc unit, WHERE string literal)
	tokNum                   // integer literal (GROUP BY 1, WHERE numeric literal, optional leading -)
	tokFloat                 // decimal float literal `digit.digit` (WHERE DOUBLE eq, optional leading -)
	tokPunct                 // one of ( ) * ,
	tokOp                    // comparison operator: = != <> < <= > >= (scan WHERE)
	tokIntDiv                // `//` — DuckDB integer (trunc) division, lexed GREEDILY (agg-3b)
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
		case c == '/' && i+1 < n && b[i+1] == '/':
			// `//` (ADJACENT) is DuckDB's integer-trunc division — one token
			// (agg-3b). A spaced `/ /` is a DuckDB Parser Error (oracle-probed),
			// and whitespace is otherwise discarded here, so greedy lexing is the
			// only place adjacency can be enforced — the same lesson as the 2e
			// `--` comment CRITICAL. The spaced form stays two Puncts and no
			// shape consumes them, so it declines.
			toks = append(toks, token{kind: tokIntDiv})
			i += 2
		case c == '(' || c == ')' || c == '*' || c == ',' || c == '+' || c == '/':
			// `+`/`/` for 2e DOUBLE arith in WHERE (`*` was already here for SELECT *
			// / count(*) AND doubles as arith-multiply by position, same as the engine).
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
		case c == '-' && i+1 < n && b[i+1] == '-':
			// `--` is a SQL LINE COMMENT — arcx has no comment handling and the engine
			// declines it (must NOT read as subtract-of-negative, a wrong answer). Match
			// the engine: decline the whole query.
			return nil, false
		case c == '-' && i+1 < n && isDigit(b[i+1]):
			// Negative integer or `digit.digit` float (e.g. a pre-1970 epoch, or a
			// DOUBLE-eq literal). The sign is part of the token → `value -5` stays a
			// signed literal (declines), NOT a subtract (the engine's mis-parse guard).
			start := i
			i++ // consume '-'
			var tok token
			tok, i = lexNumber(sql, b, start, i, n)
			toks = append(toks, tok)
		case c == '-':
			// A bare `-` (not `--`, not before a digit) is a 2e BINARY subtract in a WHERE
			// atom. The re-serializer interprets it by position.
			toks = append(toks, token{kind: tokPunct, punct: '-'})
			i++
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
			var tok token
			tok, i = lexNumber(sql, b, start, i, n)
			toks = append(toks, tok)
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

// lexNumber consumes the integer digit run beginning at i (start points at the
// literal's first byte, possibly a leading '-'), then — only if a '.' is
// IMMEDIATELY followed by a digit — the fractional part, producing a tokFloat.
// Otherwise it's a tokNum (integer). This mirrors the arcx engine's lexer: only
// `digit.digit` is a float; `5.` and `.5` are not (the '.' is left for the next
// step, which then fails to match → decline). No exponent / inf / nan spelling.
func lexNumber(sql string, b []byte, start, i, n int) (token, int) {
	for i < n && isDigit(b[i]) {
		i++
	}
	if i+1 < n && b[i] == '.' && isDigit(b[i+1]) {
		i++ // consume '.'
		for i < n && isDigit(b[i]) {
			i++
		}
		return token{kind: tokFloat, orig: sql[start:i]}, i
	}
	return token{kind: tokNum, orig: sql[start:i]}, i
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

// isFloatLiteral reports whether s is exactly `digit.digit` (optional leading '-'),
// matching what the tokenizer's tokFloat produces — one '.' with at least one digit
// on each side, no exponent / inf / nan. This is the SQL the engine's lexer accepts.
// Lives here (untagged) so both the recognizer and buildScanSQL can call it.
func isFloatLiteral(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' {
		i = 1
	}
	dot := -1
	digitsBefore, digitsAfter := 0, 0
	for ; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '.':
			if dot != -1 {
				return false // more than one '.'
			}
			dot = i
		case c >= '0' && c <= '9':
			if dot == -1 {
				digitsBefore++
			} else {
				digitsAfter++
			}
		default:
			return false
		}
	}
	return dot != -1 && digitsBefore > 0 && digitsAfter > 0
}

// isZeroFloatLiteral reports whether a `digit.digit` float literal is `±0.0` in
// value (any spelling: 0.0, -0.0, 00.000, …). The engine declines these because
// arrow total_cmp separates +0.0/-0.0 while DuckDB treats them equal, so the router
// must not route them. Assumes s already passed isFloatLiteral.
func isZeroFloatLiteral(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= '1' && c <= '9' {
			return false
		}
	}
	return true
}

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
func matchDateTruncCount(toks []token) (unit, meas, whereText string, ok bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") || !c.ident("date_trunc") || !c.punct('(') {
		return "", "", "", false
	}
	// Unit literal.
	ut, ok := c.next()
	if !ok || ut.kind != tokStr {
		return "", "", "", false
	}
	unit = ut.str
	if !supportedUnits[strings.ToLower(unit)] {
		return "", "", "", false
	}
	if !c.punct(',') {
		return "", "", "", false
	}
	// Bucket column — must be the bare identifier "time".
	col, ok := c.next()
	if !ok || col.kind != tokIdent || col.lower != timeColumn {
		return "", "", "", false
	}
	if !c.punct(')') || !c.punct(',') || !c.ident("count") || !c.punct('(') || !c.punct('*') || !c.punct(')') || !c.ident("from") {
		return "", "", "", false
	}
	mt, ok := c.next()
	if !ok || mt.kind != tokIdent {
		return "", "", "", false
	}
	meas = mt.orig

	// Optional WHERE — only a time-range filter on the `time` column (PR-A). The
	// engine serves `time <range-op> '<ts>'` AND-conjoined; anything else declines.
	if c.peekIdentLower() == "where" {
		c.next()
		wt, ok := reserializeTimeWhere(c)
		if !ok {
			return "", "", "", false
		}
		whereText = wt
	}

	if !c.ident("group") || !c.ident("by") || !c.numOne() {
		return "", "", "", false
	}
	// Optional ORDER BY 1.
	if !c.atEnd() {
		if !c.ident("order") || !c.ident("by") || !c.numOne() {
			return "", "", "", false
		}
	}
	if !c.atEnd() {
		return "", "", "", false
	}
	// Sub-hour units (minute/second) require a time-range WHERE. The WHERE above is
	// optional, so a no-WHERE sub-hour query reaches here with whereText=="". The
	// engine's per-row decode counts only non-null in-range rows, so an unfiltered
	// sub-hour query would miss DuckDB's date_trunc(NULL)=NULL bucket — decline it
	// here (and the engine declines it too). Above-hour units answer unfiltered from
	// footers (NULL bucket included), so they are unaffected.
	if isSubHour(unit) && whereText == "" {
		return "", "", "", false
	}
	return unit, meas, whereText, true
}

// reserializeTimeWhere recognizes the footer-agg WHERE — a time-range filter on the
// `time` column, AND-conjoined — and RE-SERIALIZES it token-by-token into a normalized
// string, mirroring reserializeWhere's safety property (no source-substring slice; every
// token re-emitted from the validated vocabulary, string literals re-escaped). It is
// deliberately NARROWER than reserializeWhere: only `time <op> '<str>'` atoms with `op`
// in {>, >=, <, <=}, joined by AND. `=`/`!=`, OR, parens, a non-time column, IN, BETWEEN,
// a non-string RHS → decline, matching the engine's match_footer_where exactly so the
// router never routes a shape the engine declines. The engine re-lexes this text, checks
// the RFC3339-UTC literal, and is the sole authority; a naive/offset literal declines
// engine-side (the router passes the string through, so it can't over-accept the value).
// Leaves the cursor at the first GROUP token. Caps atoms like the scan path.
func reserializeTimeWhere(c *cursor) (string, bool) {
	var b strings.Builder
	atoms := 0
	for {
		atoms++
		if atoms > maxWhereAtoms {
			return "", false
		}
		// LHS: the bare `time` column (same convention as the bucket column).
		colT, ok := c.next()
		if !ok || colT.kind != tokIdent || colT.lower != timeColumn {
			return "", false
		}
		// Operator: only range ops. `=`/`!=` decline (not a range).
		opT, ok := c.next()
		if !ok || opT.kind != tokOp {
			return "", false
		}
		switch opT.op {
		case ">", ">=", "<", "<=":
		default:
			return "", false
		}
		// RHS: a string literal (the RFC3339-UTC timestamp; the engine validates it).
		rhsT, ok := c.next()
		if !ok || rhsT.kind != tokStr {
			return "", false
		}
		if atoms > 1 {
			b.WriteString(" AND ")
		}
		// Re-emit from validated pieces: constant column name, switch-checked op,
		// re-escaped literal. No input byte reaches the output un-validated.
		b.WriteString(timeColumn)
		b.WriteByte(' ')
		b.WriteString(opT.op)
		b.WriteByte(' ')
		b.WriteByte('\'')
		b.WriteString(escapeStringLiteral(rhsT.str))
		b.WriteByte('\'')

		// Continue only on AND; anything else ends the WHERE (GROUP must follow).
		if c.peekIdentLower() == "and" {
			c.next()
			continue
		}
		break
	}
	return b.String(), true
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
//   - WHERE is AND-conjoined `<col> <op> <literal>` / `<col> IS [NOT] NULL` /
//     `<col> BETWEEN lo AND hi` (desugared to two `>=`/`<=` preds). OR/IN/LIKE/
//     functions/arithmetic fall outside the vocabulary → decline.
//   - no ORDER BY / LIMIT (the engine declines them in 2a). Anything trailing
//     the (optional) WHERE declines.
//
// The engine re-validates types, union_by_name, and the sandbox; the router's job
// is only to recognize the shape and hand over the parts. Returns the projected
// columns (as written), the predicates, the measurement, and ok.
// projFuncs is the computed-projection functions the router recognizes (2f-0: only
// `length`). Matches ProjFn / PROJ_FUNCS in arcx/src/{bind,parse}.rs.
// projFuncs maps a recognized projection function to true. length(<col>) (2f-0),
// substr(<col>, <int>[, <int>]) (2f-1), and starts_with/ends_with/contains(<col>, '<str>')
// (2f-2). Matches PROJ_FUNCS / bind_proj_func in arcx/src/{parse,bind}.rs. The engine
// re-validates everything; the router mirrors the parse-level decline boundary.
var projFuncs = map[string]bool{
	"length":      true,
	"substr":      true,
	"starts_with": true,
	"ends_with":   true,
	"contains":    true,
}

// strPredFuncs are the 2f-2 funcs taking `(col, '<string-literal>')` → BOOLEAN.
var strPredFuncs = map[string]bool{"starts_with": true, "ends_with": true, "contains": true}

// matchProjFunc parses a recognized projection function `<fn>(<args>)`, cursor at the
// `(`. Returns the re-serialized item (`length(host)`, `substr(host, 1, 3)`) or ok=false.
// The first arg is always a bare column; substr then takes 1-2 int literals. Mirrors the
// engine's decline boundary: a nested function, `*`, a non-int/`+`-signed substr arg,
// wrong arity, or an unknown function declines (the engine re-checks + owns offset range).
func matchProjFunc(c *cursor, fnLower string) (string, bool) {
	if !projFuncs[fnLower] {
		return "", false
	}
	if !c.punct('(') {
		return "", false
	}
	// First arg: a bare column (never a nested function / literal / keyword).
	arg, ok := c.next()
	if !ok || arg.kind != tokIdent || isScanKeyword(arg.lower) || !isBareIdent(arg.orig) {
		return "", false
	}
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
		return "", false // nested function (`substr(upper(host), ...)`) → decline
	}
	switch fnLower {
	case "length":
		if !c.punct(')') {
			return "", false // >1 arg → decline
		}
		return "length(" + arg.orig + ")", true
	case "substr":
		// `substr(col, <int> [, <int>])` — 1 or 2 int-literal args after the column.
		var b strings.Builder
		b.WriteString("substr(")
		b.WriteString(arg.orig)
		nargs := 0
		for {
			if !c.punct(',') {
				return "", false // substr needs at least a start arg
			}
			lit, ok := c.next()
			// An int literal only (tokNum; a `+`-signed or float/string arg isn't tokNum
			// → decline, mirroring the engine which declines a Column/non-int start/len).
			if !ok || lit.kind != tokNum || !isIntLiteral(lit.orig) {
				return "", false
			}
			b.WriteString(", ")
			b.WriteString(lit.orig)
			nargs++
			if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ')' {
				break
			}
			if nargs >= 2 {
				return "", false // >2 int args (arity >3) → decline
			}
		}
		if !c.punct(')') {
			return "", false
		}
		b.WriteByte(')')
		return b.String(), true
	default:
		// 2f-2 string predicates: `<fn>(col, '<string-literal>')` → re-serialize with the
		// needle re-escaped. The engine re-validates the string-col type + owns the kernel.
		if strPredFuncs[fnLower] {
			if !c.punct(',') {
				return "", false
			}
			lit, ok := c.next()
			if !ok || lit.kind != tokStr {
				return "", false // non-string needle (int/column/`[`) → decline
			}
			if !c.punct(')') {
				return "", false // wrong arity → decline
			}
			// lit.str is the UNESCAPED content; re-escape (double `'`) for the emitted SQL.
			return fnLower + "(" + arg.orig + ", '" + escapeStringLiteral(lit.str) + "')", true
		}
		return "", false
	}
}

// matchScanAgg matches the agg-1 ungrouped-aggregation shape (Phase 3 slice 1):
//
//	select <agg> [, <agg>]* from <measurement> [where <pred tree>]
//
// where <agg> is `count(*)` or `{count|sum|min|max|avg}(<bare column>)`. Each item
// is re-serialized from its VALIDATED tokens as `fn(colAsWritten)` — the function
// name lowercased (both engines lowercase it in the derived output name anyway),
// the argument's typed spelling preserved (DuckDB and arcx both echo it verbatim
// in the derived name, so canonicalizing the arg would change the client-visible
// column name). The WHERE reuses the scan's boolean-tree re-serialization — the
// same injection-proof construction, the engine is the tree authority. Anything
// after the WHERE (GROUP BY / ORDER BY / LIMIT / aliases) declines: those are
// later slices and the engine declines them too.
//
// Tried AFTER the footer matchers (count(*)/scalar agg get first refusal — the
// engine routes footer-first for those) and BEFORE matchScan.
func matchScanAgg(toks []token) (items []string, whereText string, meas string, ok bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") {
		return nil, "", "", false
	}
	fail := func() ([]string, string, string, bool) { return nil, "", "", false }

	for {
		f, ok := c.next()
		if !ok || f.kind != tokIdent {
			return fail()
		}
		switch f.lower {
		case "count", "sum", "min", "max", "avg":
			if !c.punct('(') {
				return fail()
			}
			// `count(*)` — the only star form. `sum(*)` etc. fall through to the
			// bare-column requirement and decline.
			if f.lower == "count" && c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '*' {
				c.i++
				if !c.punct(')') {
					return fail()
				}
				items = append(items, "count(*)")
			} else {
				arg, ok := c.next()
				if !ok || arg.kind != tokIdent || isScanKeyword(arg.lower) || arg.lower == "distinct" {
					return fail()
				}
				// The immediately-following `)` requirement declines expressions
				// (`sum(a*b)`) and DISTINCT — mirroring the engine's matcher.
				if !c.punct(')') {
					return fail()
				}
				items = append(items, f.lower+"("+arg.orig+")")
			}
		case "arg_max", "arg_min", "max_by", "min_by":
			// agg-4 two-arg by-time aggregates: `fn(payload, orderkey)` — both
			// bare idents, re-serialized `fn(a, b)` (fn lowercased, spellings
			// preserved). The 3-arg top-N form and any other arg-shape fall to
			// the `)` requirement and decline; `arg_max_null`/`argmax` are
			// unknown fn names and decline in the default arm.
			item, ok := parseTwoArgAggItem(c, f.lower)
			if !ok {
				return fail()
			}
			items = append(items, item)
		default:
			return fail()
		}
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

	// Optional WHERE — ALWAYS the boolean-tree re-serialization (a flat AND is a
	// tree too; the agg path has no flat-preds representation to maintain).
	if c.peekIdentLower() == "where" {
		c.next()
		wt, ok := reserializeWhere(c)
		if !ok {
			return fail()
		}
		whereText = wt
	}
	if !c.atEnd() {
		return fail()
	}
	return items, whereText, meas, true
}

// matchGroupedAgg matches the grouped class the engine's perf gate has cleared
// (agg-2c: every no-WHERE single-key grouped bench shape beats DuckDB v1.5.5 on
// the 1.47B-row corpus; mimalloc slice 2026-08-27: the WHERE-bearing shapes —
// selective AND broad, incl. the masked dashboard pair — flipped to wins too,
// so the optional WHERE joined the class):
//
//	select {<agg>|<key>} [, ...]* from <measurement> [where <tree>] group by {<key> | <pos>}
//
// EXACTLY one bare key column, ≥1 aggregate from agg-1's set (`count(*)` or
// `count|sum|min|max|avg(<bare col>)`), any item order, an optional WHERE
// through the same `reserializeWhere` boolean-tree surface the scan/agg shapes
// use (injection-proof: emitted text is rebuilt from validated tokens, never
// sliced from the source). Items are re-serialized from validated tokens (fn
// lowercased, ARG spelling preserved — derived-name parity); the key's spelling
// is preserved.
//
// agg-3 widened the key: EITHER a bare tag column OR one time-bucket item
// `date_trunc('<unit>', <bare col>)` with unit in the engine's fixed-width set
// (second/minute/hour/day — bucketUnit/bucketCol return the validated parts and
// the builder re-emits them, never the source text). An optional trailing
// `ORDER BY <key pos|key name> [ASC]` is accepted (the engine sorts ascending,
// NULLS LAST); DESC/LIMIT/multi-key ORDER decline. Non-UTC sessions never get
// here — the M3 tz-injection gate runs before shape matching (gotcha #3).
func matchGroupedAgg(toks []token) (m groupedMatch, ok bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") {
		return groupedMatch{}, false
	}
	fail := func() (groupedMatch, bool) { return groupedMatch{}, false }
	var items []string
	var bucketText, tagKey, whereText, meas string

	bucketItem := -1
	tagItem := -1
	nAggs := 0
	for {
		t, tok := c.next()
		if !tok || t.kind != tokIdent || isScanKeyword(t.lower) {
			return fail()
		}
		isCall := c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '('
		if isCall && t.lower == "to_timestamp" {
			// The epoch-math time-bucket KEY item (agg-3b) — EXACTLY the Grafana
			// `$__timeGroup` emission, token for token:
			//   to_timestamp((epoch_ns(<col>) // 1000000000 // N) * N) AS <alias>
			// Both N literals must match, N ∈ [1, 31_622_400]; the alias is
			// MANDATORY (Grafana's time-series frame needs a column named `time`;
			// the unaliased form would need DuckDB's mangled derived name
			// reproduced). The builder re-emits from the validated parts.
			if bucketItem >= 0 {
				return fail()
			}
			c.i++ // consume '('
			if !c.punct('(') || !c.ident("epoch_ns") || !c.punct('(') {
				return fail()
			}
			bc, tok := c.next()
			if !tok || bc.kind != tokIdent || isScanKeyword(bc.lower) || !isBareIdent(bc.orig) {
				return fail()
			}
			if !c.punct(')') {
				return fail()
			}
			if c.i >= len(c.toks) || c.toks[c.i].kind != tokIntDiv {
				return fail()
			}
			c.i++
			nt, tok := c.next()
			if !tok || nt.kind != tokNum || nt.orig != "1000000000" {
				return fail()
			}
			if c.i >= len(c.toks) || c.toks[c.i].kind != tokIntDiv {
				return fail()
			}
			c.i++
			wt, tok := c.next()
			if !tok || wt.kind != tokNum {
				return fail()
			}
			secs, err := strconv.Atoi(wt.orig)
			if err != nil || secs < 1 || secs > 31_622_400 {
				return fail()
			}
			if !c.punct(')') {
				return fail()
			}
			if c.i >= len(c.toks) || c.toks[c.i].kind != tokPunct || c.toks[c.i].punct != '*' {
				return fail()
			}
			c.i++
			wt2, tok := c.next()
			if !tok || wt2.kind != tokNum || wt2.orig != wt.orig {
				return fail() // width literals must MATCH — else a different expression
			}
			if !c.punct(')') || !c.ident("as") {
				return fail()
			}
			al, tok := c.next()
			if !tok || al.kind != tokIdent || isScanKeyword(al.lower) || !isBareIdent(al.orig) {
				return fail()
			}
			m.epochWidthSecs = secs
			m.bucketCol = bc.orig
			m.bucketAlias = al.orig
			bucketItem = len(items)
			bucketText = "to_timestamp((epoch_ns(" + bc.orig + ") // 1000000000 // " + wt.orig + ") * " + wt.orig + ") AS " + al.orig
			items = append(items, bucketText)
		} else if isCall && t.lower == "date_trunc" {
			// The time-bucket KEY item (agg-3). One per query; fixed-width units
			// only (the engine declines calendar units).
			if bucketItem >= 0 {
				return fail()
			}
			c.i++ // consume '('
			ut, tok := c.next()
			if !tok || ut.kind != tokStr || !fixedWidthUnits[strings.ToLower(ut.str)] {
				return fail()
			}
			if !c.punct(',') {
				return fail()
			}
			bc, tok := c.next()
			if !tok || bc.kind != tokIdent || isScanKeyword(bc.lower) || !isBareIdent(bc.orig) {
				return fail()
			}
			if !c.punct(')') {
				return fail()
			}
			m.bucketUnit = ut.str
			m.bucketCol = bc.orig
			bucketItem = len(items)
			bucketText = "date_trunc('" + ut.str + "', " + bc.orig + ")"
			items = append(items, bucketText)
		} else if isCall && (t.lower == "arg_max" || t.lower == "arg_min" || t.lower == "max_by" || t.lower == "min_by") {
			// agg-4 two-arg by-time aggregates (see matchScanAgg's arm).
			item, iok := parseTwoArgAggItem(c, t.lower)
			if !iok {
				return fail()
			}
			items = append(items, item)
			nAggs++
		} else if isCall {
			switch t.lower {
			case "count", "sum", "min", "max", "avg":
			default:
				return fail()
			}
			c.i++ // consume '('
			if t.lower == "count" && c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '*' {
				c.i++
				if !c.punct(')') {
					return fail()
				}
				items = append(items, "count(*)")
			} else {
				arg, tok := c.next()
				if !tok || arg.kind != tokIdent || isScanKeyword(arg.lower) {
					return fail()
				}
				if !c.punct(')') {
					return fail() // expression / DISTINCT / arity → decline
				}
				items = append(items, t.lower+"("+arg.orig+")")
			}
			nAggs++
		} else {
			// A bare column — the TAG key. agg-3c allows at most ONE tag key
			// (alongside an optional bucket): a second tag would ride the
			// generic engine path that measured 1.4-2.4x BEHIND at corpus
			// scale — allow-listing it routes a known loss (gotcha #5).
			if tagItem >= 0 {
				return fail()
			}
			tagItem = len(items)
			tagKey = t.orig
			items = append(items, t.orig)
		}
		if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ',' {
			c.i++
			continue
		}
		break
	}
	// At least one key of either kind + at least one aggregate.
	if (bucketItem < 0 && tagItem < 0) || nAggs == 0 {
		return fail()
	}
	if !c.ident("from") {
		return fail()
	}
	mt, tok := c.next()
	if !tok || mt.kind != tokIdent {
		return fail()
	}
	meas = mt.orig

	// Optional WHERE — the same boolean-tree re-serialization the scan and
	// ungrouped-agg shapes route through (one shared surface, one shared review).
	if c.peekIdentLower() == "where" {
		c.next()
		wt, wok := reserializeWhere(c)
		if !wok {
			return fail()
		}
		whereText = wt
	}
	if !c.ident("group") || !c.ident("by") {
		return fail()
	}
	// Key references are ASYMMETRIC (oracle-probed at agg-3b): in GROUP BY,
	// DuckDB binds a bare ident to the RAW COLUMN (so an alias never resolves
	// there — position, or the TAG key's bare name only); in ORDER BY it binds
	// the ALIAS. Each ref resolves to a key ITEM index, or -1.
	groupRef := func(kt token) int {
		switch kt.kind {
		case tokIdent:
			if tagItem >= 0 && strings.EqualFold(kt.orig, tagKey) {
				return tagItem
			}
		case tokNum:
			if bucketItem >= 0 && kt.orig == strconv.Itoa(bucketItem+1) {
				return bucketItem
			}
			if tagItem >= 0 && kt.orig == strconv.Itoa(tagItem+1) {
				return tagItem
			}
		}
		return -1
	}
	orderRef := func(kt token) int {
		if kt.kind == tokIdent && m.bucketAlias != "" && strings.EqualFold(kt.orig, m.bucketAlias) {
			return bucketItem
		}
		return groupRef(kt)
	}
	// GROUP BY: a comma-list of key refs covering EVERY key exactly once
	// (agg-3c: bucket + tag panels say `GROUP BY 1, 2` or `GROUP BY 1, host`).
	seen := map[int]bool{}
	for {
		kt, tok := c.next()
		if !tok {
			return fail()
		}
		ki := groupRef(kt)
		if ki < 0 || seen[ki] {
			return fail()
		}
		seen[ki] = true
		if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ',' {
			c.i++
			continue
		}
		break
	}
	nKeys := 0
	if bucketItem >= 0 {
		nKeys++
	}
	if tagItem >= 0 {
		nKeys++
	}
	if len(seen) != nKeys {
		return fail() // every projected key must be grouped exactly once
	}
	// Optional `ORDER BY <key ref> [ASC]` (agg-3): the engine sorts that single
	// key ascending, NULLS LAST. DESC / LIMIT / multi-key ORDER decline.
	if c.peekIdentLower() == "order" {
		c.next()
		if !c.ident("by") {
			return fail()
		}
		ot, tok := c.next()
		if !tok {
			return fail()
		}
		oi := orderRef(ot)
		if oi < 0 {
			return fail()
		}
		if c.peekIdentLower() == "asc" {
			c.next()
		}
		m.orderByItem = oi + 1 // 1-based select-list position
	}
	if !c.atEnd() {
		return fail()
	}
	m.items = items
	m.bucketText = bucketText
	m.bucketItem = bucketItem
	m.tagKey = tagKey
	m.tagItem = tagItem
	m.whereText = whereText
	m.meas = meas
	return m, true
}

// groupedMatch is matchGroupedAgg's result: the re-serialized select items, the
// key set (agg-3c: at most one BUCKET — date_trunc or epoch-math — and at most
// one bare TAG key, at least one of either), their select-list indices, and the
// validated bucket parts the SQL builder re-emits (never source text).
type groupedMatch struct {
	items      []string
	bucketText string // rebuilt bucket item text ("" = no bucket key)
	bucketItem int    // select-list index of the bucket (-1 = none)
	tagKey     string // the bare tag key's spelling ("" = no tag key)
	tagItem    int    // select-list index of the tag key (-1 = none)
	whereText  string
	meas       string
	bucketUnit string
	bucketCol  string
	// ORDER BY: the ordered KEY's 1-based select-list position (0 = none).
	orderByItem int
	// agg-3b epoch-math bucket: width in whole seconds (0 = not this form) and
	// the MANDATORY alias — validated parts the builder re-emits.
	epochWidthSecs int
	bucketAlias    string
}

// parseTwoArgAggItem consumes `( <bare ident> , <bare ident> )` after an agg-4
// function name and re-serializes it as `fn(a, b)` — validated tokens only,
// never source text. Anything else (star, expression, a third argument) fails.
func parseTwoArgAggItem(c *cursor, fnLower string) (string, bool) {
	if !c.punct('(') {
		return "", false
	}
	a1, ok := c.next()
	if !ok || a1.kind != tokIdent || isScanKeyword(a1.lower) || !isBareIdent(a1.orig) {
		return "", false
	}
	if !c.punct(',') {
		return "", false
	}
	a2, ok := c.next()
	if !ok || a2.kind != tokIdent || isScanKeyword(a2.lower) || !isBareIdent(a2.orig) {
		return "", false
	}
	if !c.punct(')') {
		return "", false
	}
	return fnLower + "(" + a1.orig + ", " + a2.orig + ")", true
}

// fixedWidthUnits is the engine's agg-3 bucket set — pure UTC epoch division
// (day = 86,400s; no DST in UTC). Calendar units (week/month/year) decline;
// count-only month/year stays on the footer date_trunc_count shape.
var fixedWidthUnits = map[string]bool{
	"second": true,
	"minute": true,
	"hour":   true,
	"day":    true,
}

func matchScan(toks []token) (cols []string, preds []scanPred, whereText string, orderBy []scanOrderKey, limit int, meas string, ok bool) {
	c := &cursor{toks: toks}
	if !c.ident("select") {
		return nil, nil, "", nil, 0, "", false
	}
	fail := func() ([]string, []scanPred, string, []scanOrderKey, int, string, bool) {
		return nil, nil, "", nil, 0, "", false
	}

	// Projection: one or more bare columns, comma-separated. A `*`, a function
	// call, or an alias all fall outside — EXCEPT a recognized computed-projection
	// function `length(<col>)` (2f-0), which is re-serialized as an item string.
	for {
		t, ok := c.next()
		if !ok || t.kind != tokIdent || isScanKeyword(t.lower) {
			return fail()
		}
		// A `(` immediately after is a function call. Only `length(<bare-col>)` (2f-0)
		// is recognized; any other function / arg-shape declines (mirrors the engine's
		// parse_like_pattern-style decline boundary). The engine re-lexes the emitted SQL
		// and is the authority; the router just re-serializes what it proved.
		if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
			item, ok := matchProjFunc(c, t.lower)
			if !ok {
				return fail()
			}
			cols = append(cols, item)
		} else {
			cols = append(cols, t.orig)
		}
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

	// Optional WHERE. A flat AND-list is captured as []scanPred (2a/2b-1); a WHERE that
	// contains OR or parens is a boolean TREE that []scanPred can't represent, so it's
	// re-serialized to whereText and the engine owns the tree (2b-2).
	if c.peekIdentLower() == "where" {
		c.next()
		if whereHasOrOrParen(c) {
			wt, ok := reserializeWhere(c)
			if !ok {
				return fail()
			}
			whereText = wt
			// After the WHERE tree, fall through to ORDER BY / LIMIT below.
			preds = nil
			goto afterWhere
		}
		for {
			colT, ok := c.next()
			if !ok || colT.kind != tokIdent || isScanKeyword(colT.lower) {
				return fail()
			}
			// Branch: `IS [NOT] NULL` / `BETWEEN lo AND hi` / `<op> <literal>`.
			if c.peekIdentLower() == "is" {
				c.next() // consume `is`
				negated := false
				if c.peekIdentLower() == "not" {
					c.next()
					negated = true
				}
				if c.peekIdentLower() != "null" {
					return fail()
				}
				c.next() // consume `null`
				preds = append(preds, scanPred{col: colT.orig, isNull: true, negated: negated})
			} else if c.peekIdentLower() == "between" {
				// `col BETWEEN lo AND hi` → desugar to two preds (col >= lo, col <= hi),
				// matching arcx's binder exactly so buildScanSQL and the engine agree. The
				// INNER `and` is consumed EAGERLY here so the outer AND-chain loop never
				// mistakes it for a conjunction. NOT BETWEEN isn't reachable (a leading
				// `not` isn't a valid bare-column token) — it declines upstream.
				c.next() // consume `between`
				loT, ok := c.next()
				if !ok || (loT.kind != tokNum && loT.kind != tokStr && loT.kind != tokFloat) {
					return fail()
				}
				if c.peekIdentLower() != "and" {
					return fail()
				}
				c.next() // consume the INNER `and`
				hiT, ok := c.next()
				if !ok || (hiT.kind != tokNum && hiT.kind != tokStr && hiT.kind != tokFloat) {
					return fail()
				}
				// A `±0.0` float bound SERVES since the int-coercion slice: the
				// engine binds any zero spelling as the normalized compare
				// `(col + 0.0) <op> +0.0` (the 2e machinery), oracle-matched on
				// the full specials matrix — the 2b-4 decline is lifted.
				lo := scanPred{col: colT.orig, op: ">="}
				hi := scanPred{col: colT.orig, op: "<="}
				switch loT.kind {
				case tokStr:
					lo.str, lo.isStr = loT.str, true
				case tokFloat:
					lo.num, lo.isFloat = loT.orig, true
				default:
					lo.num = loT.orig
				}
				switch hiT.kind {
				case tokStr:
					hi.str, hi.isStr = hiT.str, true
				case tokFloat:
					hi.num, hi.isFloat = hiT.orig, true
				default:
					hi.num = hiT.orig
				}
				preds = append(preds, lo, hi)
			} else {
				opStr, ok := c.op()
				if !ok {
					return fail()
				}
				litT, ok := c.next()
				if !ok {
					return fail()
				}
				switch litT.kind {
				case tokNum:
					preds = append(preds, scanPred{col: colT.orig, op: opStr, num: litT.orig, isStr: false})
				case tokFloat:
					// DOUBLE comparison. As of 2b-4 the engine serves all six ops on a
					// finite float (arrow total_cmp == DuckDB ordering). Since the
					// int-coercion slice the `±0.0` literal also serves (the engine
					// normalizes both operands — the signed-zero divergence is gone).
					preds = append(preds, scanPred{col: colT.orig, op: opStr, num: litT.orig, isFloat: true})
				case tokStr:
					preds = append(preds, scanPred{col: colT.orig, op: opStr, str: litT.str, isStr: true})
				default:
					return fail()
				}
			}
			if c.peekIdentLower() == "and" {
				c.i++
				continue
			}
			break
		}
	}
afterWhere:

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
	return cols, preds, whereText, orderBy, limit, meas, true
}

// whereHasOrOrParen peeks (without advancing) whether the WHERE clause ahead must go
// through the boolean-TREE re-serializer rather than the flat []scanPred path: an `or`
// keyword or a `(` (a tree the flat list can't hold), OR a `like` token. LIKE is handled
// ONLY in the tree re-serializer (reserializeAtom → writeLikePattern), so any LIKE-bearing
// WHERE — even a single `col LIKE 'x'` with no OR/paren — is routed there; the flat path
// never sees LIKE. Scans from the cursor to the first ORDER/LIMIT/end.
func whereHasOrOrParen(c *cursor) bool {
	for i := c.i; i < len(c.toks); i++ {
		t := c.toks[i]
		if t.kind == tokIdent && (t.lower == "order" || t.lower == "limit") {
			break
		}
		if t.kind == tokIdent && (t.lower == "or" || t.lower == "like") {
			return true
		}
		// An arith punct (`+`/`-`/`/`, or `*` between a col and a literal) routes to the
		// TREE re-serializer, which handles the 2e `col arith num op num` atom. (`*` also
		// appears in `count(*)`/`SELECT *`, but those aren't in a scan WHERE.)
		if t.kind == tokPunct && (t.punct == '+' || t.punct == '-' || t.punct == '/' || t.punct == '*') {
			return true
		}
		if t.kind == tokPunct && (t.punct == '(' || t.punct == ')') {
			return true
		}
	}
	return false
}

// reserializeWhere recognizes the WHERE as a boolean expression over the STRICT allowed
// vocabulary and RE-SERIALIZES it token-by-token into a normalized string (2b-2). It is
// NOT a source-substring slice (tokens carry no offsets): every token is re-emitted —
// bare-column idents via isBareIdent, comparison ops via isCmpOp, string literals
// RE-ESCAPED via escapeStringLiteral, numbers/floats re-validated, and only the structural
// tokens `(` `)` AND OR BETWEEN IS [NOT] NULL IN [NOT] LIKE. Anything else (a bare NOT-prefix,
// a function-call `ident(`, arithmetic) → decline, so the router never routes a shape the
// engine declines (a served-then-declined shadow mismatch). The engine re-lexes this text
// and is the sole tree authority; the round-trip fidelity is covered by a test.
//
// Grammar (SQL precedence): or := and (OR and)* ; and := unary (AND unary)* ;
// unary := '(' or ')' | atom . It leaves the cursor at the first ORDER/LIMIT/end token.
func reserializeWhere(c *cursor) (string, bool) {
	var b strings.Builder
	depth := 0
	atoms := 0
	if !reserializeOr(c, &b, &depth, &atoms) {
		return "", false
	}
	if depth != 0 {
		return "", false // unbalanced parens
	}
	if atoms == 0 {
		return "", false // empty WHERE / `()`
	}
	return b.String(), true
}

// maxWhereDepth / maxWhereAtoms mirror the engine's parse-time caps (a too-deep or
// too-wide WHERE declines at the router too, matching the engine's decline).
const (
	maxWhereDepth = 32
	maxWhereAtoms = 1024
)

func reserializeOr(c *cursor, b *strings.Builder, depth, atoms *int) bool {
	if !reserializeAnd(c, b, depth, atoms) {
		return false
	}
	for c.peekIdentLower() == "or" {
		c.next()
		b.WriteString(" OR ")
		if !reserializeAnd(c, b, depth, atoms) {
			return false
		}
	}
	return true
}

func reserializeAnd(c *cursor, b *strings.Builder, depth, atoms *int) bool {
	if !reserializeUnary(c, b, depth, atoms) {
		return false
	}
	for c.peekIdentLower() == "and" {
		c.next()
		b.WriteString(" AND ")
		if !reserializeUnary(c, b, depth, atoms) {
			return false
		}
	}
	return true
}

// maxArithNodes mirrors the engine's MAX_ARITH_NODES: cap the arith tree size so a wide/deep
// expr declines at the router too (matching the engine's decline, no served-then-declined shadow).
const maxArithNodes = 64

// arithParse carries the running state of an arith-expr re-serialization: the output builder,
// whether any column leaf was seen (a column-free LHS declines — DuckDB folds it in INT/DECIMAL),
// whether the CURRENT node's subtree is constant-only (for the constant-subexpr decline), whether
// any binary op was seen (a bare column/single-op defers to the existing dispatch), and the node
// count (cap). Mirrors the engine's ArithNode analysis (is_constant / has_constant_binop).
type arithParse struct {
	b        strings.Builder
	hasCol   bool
	hasOp    bool
	sawConst bool // a constant-only sub-expression was found → decline
	nodes    int
	// opCount / colCount / litCount over the whole expr, used to DEFER the single-op
	// single-column shape (`col op lit`) to the engine's proven ArithCompare path — the
	// engine's try_parse_arith_expr_atom does the same via is_single_op_single_col, so the
	// router must too or it routes a shape the engine declines (lit-left `2.0 * value`).
	opCount  int
	colCount int
	litCount int
}

// tryReserializeArithExpr attempts `<arith-expr> <cmp> <lit>` from c, re-emitting verbatim.
// Returns true (and commits) only for a COMPOUND, column-bearing, constant-fold-free expr
// followed by a comparison to a numeric literal — else restores the cursor and returns false
// (defer). Mirrors the engine so the router never routes a shape the engine declines.
func tryReserializeArithExpr(c *cursor, b *strings.Builder, atoms *int) bool {
	start := c.i
	ap := &arithParse{}
	constOnly, ok := arithAddSub(c, ap)
	if !ok || ap.sawConst || !ap.hasCol || !ap.hasOp || constOnly {
		c.i = start
		return false
	}
	// Defer the single-op single-column shape (`col op lit` — exactly 1 op, 1 col, 1 lit,
	// no parens/unary) to the engine's proven ArithCompare path. The engine's
	// is_single_op_single_col does the same; if the router served this, it would route the
	// lit-left form (`2.0 * value`) that the engine declines — a served-then-declined shadow.
	// The existing col-left single-op re-serializer (reserializeAtom) handles the served case.
	if ap.opCount == 1 && ap.colCount == 1 && ap.litCount == 1 && ap.nodes == 3 {
		c.i = start
		return false
	}
	// Must be `<cmp-op> <numeric-literal>` to be an atom.
	opStr, ok := c.op()
	if !ok || !isCmpOp(opStr) {
		c.i = start
		return false
	}
	cmpLit, ok := c.next()
	if !ok || !isNumericTok(cmpLit) {
		c.i = start
		return false
	}
	*atoms++
	if *atoms > maxWhereAtoms {
		c.i = start
		return false
	}
	b.WriteString(ap.b.String())
	b.WriteByte(' ')
	b.WriteString(opStr)
	b.WriteByte(' ')
	return writeLiteralTok(b, cmpLit)
}

// arithAddSub := arithMulDiv ( (+|-) arithMulDiv )* — lowest precedence. Returns
// (subtreeIsConstantOnly, ok). Emits verbatim with single spaces around binary ops.
func arithAddSub(c *cursor, ap *arithParse) (bool, bool) {
	// `lconst` tracks whether the CURRENT left subtree (folded left-assoc) is constant-only.
	lconst, ok := arithMulDiv(c, ap)
	if !ok {
		return false, false
	}
	for c.i < len(c.toks) && c.toks[c.i].kind == tokPunct &&
		(c.toks[c.i].punct == '+' || c.toks[c.i].punct == '-') {
		op := c.toks[c.i].punct
		c.next()
		if !ap.bumpNodes() {
			return false, false
		}
		ap.hasOp = true
		ap.opCount++
		ap.b.WriteByte(' ')
		ap.b.WriteByte(op)
		ap.b.WriteByte(' ')
		rconst, ok := arithMulDiv(c, ap)
		if !ok {
			return false, false
		}
		// This node = (left OP right). Both operands constant-only → constant-fold decline.
		if lconst && rconst {
			ap.sawConst = true
		}
		lconst = lconst && rconst // the new left subtree's constant-ness
	}
	return lconst, true
}

// arithMulDiv := arithFactor ( (*|/) arithFactor )* — tighter than +/-. `//` declines: the
// second `/` fails arithPrimary (not a primary), mirroring the engine.
func arithMulDiv(c *cursor, ap *arithParse) (bool, bool) {
	lconst, ok := arithFactor(c, ap)
	if !ok {
		return false, false
	}
	for c.i < len(c.toks) && c.toks[c.i].kind == tokPunct &&
		(c.toks[c.i].punct == '*' || c.toks[c.i].punct == '/') {
		op := c.toks[c.i].punct
		c.next()
		if !ap.bumpNodes() {
			return false, false
		}
		ap.hasOp = true
		ap.opCount++
		ap.b.WriteByte(' ')
		ap.b.WriteByte(op)
		ap.b.WriteByte(' ')
		rconst, ok := arithFactor(c, ap)
		if !ok {
			return false, false
		}
		if lconst && rconst {
			ap.sawConst = true
		}
		lconst = lconst && rconst
	}
	return lconst, true
}

// arithFactor := `-` arithFactor | arithPrimary — unary minus. Only reaches as Punct('-')
// before an ident/`(` (the tokenizer folds `-<digit>` into a signed literal).
func arithFactor(c *cursor, ap *arithParse) (bool, bool) {
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '-' {
		c.next()
		if !ap.bumpNodes() {
			return false, false
		}
		ap.b.WriteByte('-')
		return arithFactor(c, ap)
	}
	return arithPrimary(c, ap)
}

// arithPrimary := column | number | `(` arithAddSub `)`.
func arithPrimary(c *cursor, ap *arithParse) (bool, bool) {
	if !ap.bumpNodes() {
		return false, false
	}
	if c.i >= len(c.toks) {
		return false, false
	}
	t := c.toks[c.i]
	switch {
	case t.kind == tokPunct && t.punct == '(':
		c.next()
		ap.b.WriteByte('(')
		constInner, ok := arithAddSub(c, ap)
		if !ok {
			return false, false
		}
		if c.i >= len(c.toks) || c.toks[c.i].kind != tokPunct || c.toks[c.i].punct != ')' {
			return false, false
		}
		c.next()
		ap.b.WriteByte(')')
		return constInner, true
	case isNumericTok(t):
		c.next()
		if !writeLiteralTok(&ap.b, t) {
			return false, false
		}
		ap.litCount++
		return true, true // a literal leaf is constant
	case t.kind == tokIdent && !isScanKeyword(t.lower) && isBareIdent(t.orig):
		// A `(` right after the ident is a function call — no functions in arith.
		if c.i+1 < len(c.toks) && c.toks[c.i+1].kind == tokPunct && c.toks[c.i+1].punct == '(' {
			return false, false
		}
		c.next()
		ap.b.WriteString(t.orig)
		ap.hasCol = true
		ap.colCount++
		return false, true // a column leaf is NOT constant
	default:
		return false, false
	}
}

func (ap *arithParse) bumpNodes() bool {
	ap.nodes++
	return ap.nodes <= maxArithNodes
}

func reserializeUnary(c *cursor, b *strings.Builder, depth, atoms *int) bool {
	// 2e-multiterm: try a full-precedence arith-expr atom FIRST (`<arith-expr> <cmp> <lit>`),
	// re-emitting tokens VERBATIM (parens preserved — a dropped paren is a silent wrong answer).
	// Mirrors the engine's try_parse_arith_expr_atom: on a non-arith atom (boolean `(` group,
	// IN/LIKE/BETWEEN/IS NULL, plain/single-op compare) it restores the cursor and defers. The
	// engine re-parses this text and is the sole tree authority; this is the eligibility gate.
	if tryReserializeArithExpr(c, b, atoms) {
		return true
	}

	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
		*depth++
		if *depth > maxWhereDepth {
			return false
		}
		c.next()
		b.WriteByte('(')
		if !reserializeOr(c, b, depth, atoms) {
			return false
		}
		if c.i >= len(c.toks) || c.toks[c.i].kind != tokPunct || c.toks[c.i].punct != ')' {
			return false
		}
		c.next()
		b.WriteByte(')')
		*depth--
		return true
	}
	return reserializeAtom(c, b, atoms)
}

// reserializeAtom emits ONE predicate atom, re-escaping/re-validating every token. Mirrors
// the flat []scanPred atom recognition, but writes normalized SQL instead of a struct.
func reserializeAtom(c *cursor, b *strings.Builder, atoms *int) bool {
	*atoms++
	if *atoms > maxWhereAtoms {
		return false
	}
	colT, ok := c.next()
	if !ok || colT.kind != tokIdent || isScanKeyword(colT.lower) || !isBareIdent(colT.orig) {
		return false
	}
	// A `(` right after the ident is a function call — decline (no expression atoms).
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
		return false
	}
	b.WriteString(colT.orig)

	// 2e: `<col> (+|-|*|/) <num> <op> <num>` — an arith punct right after the column. Re-emit
	// verbatim (both engines parse identically); the engine owns the Float64 type-gate, the
	// signed-zero normalization, AND the `-0.0`-divisor fold. `/` is true division (any divisor
	// incl. zero); `//` (floor-div) declines because its lexed second `/` fails isNumericTok —
	// exactly mirroring the engine's match_arith_literal decline.
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct {
		if ap := c.toks[c.i].punct; ap == '+' || ap == '-' || ap == '*' || ap == '/' {
			c.next() // consume the arith punct
			arithLit, ok := c.next()
			if !ok || !isNumericTok(arithLit) {
				return false // non-numeric / column operand → decline
			}
			opStr, ok := c.op()
			if !ok || !isCmpOp(opStr) {
				return false // second arith op / missing cmp → decline
			}
			cmpLit, ok := c.next()
			if !ok || !isNumericTok(cmpLit) {
				return false
			}
			b.WriteByte(' ')
			b.WriteByte(ap)
			b.WriteByte(' ')
			if !writeLiteralTok(b, arithLit) {
				return false
			}
			b.WriteByte(' ')
			b.WriteString(opStr)
			b.WriteByte(' ')
			return writeLiteralTok(b, cmpLit)
		}
	}

	switch {
	case c.peekIdentLower() == "is":
		c.next()
		b.WriteString(" IS")
		if c.peekIdentLower() == "not" {
			c.next()
			b.WriteString(" NOT")
		}
		if c.peekIdentLower() != "null" {
			return false
		}
		c.next()
		b.WriteString(" NULL")
		return true
	case c.peekIdentLower() == "between":
		c.next()
		lo, ok := c.next()
		if !ok || !isAtomLiteralTok(lo) {
			return false
		}
		if c.peekIdentLower() != "and" {
			return false
		}
		c.next()
		hi, ok := c.next()
		if !ok || !isAtomLiteralTok(hi) {
			return false
		}
		b.WriteString(" BETWEEN ")
		if !writeLiteralTok(b, lo) {
			return false
		}
		b.WriteString(" AND ")
		return writeLiteralTok(b, hi)
	case c.peekIdentLower() == "in":
		// `col IN (lit, …)` — the engine desugars to Or-of-equals (2b-3). Re-emit the
		// list; the atom counter bumps per element (a huge IN declines like a wide OR).
		c.next() // consume `in`
		return reserializeInList(c, b, false, atoms)
	case c.peekIdentLower() == "like":
		// `col LIKE '<pattern>'` (2d). Engine reuses arrow's like kernel, which matches
		// DuckDB on every backslash-free pattern; a `\`-pattern or an ESCAPE clause declines
		// (see writeLikePattern). Re-escape the pattern; never slice source text.
		c.next() // consume `like`
		return writeLikePattern(c, b, false)
	case c.peekIdentLower() == "not":
		// `<col> not …` is valid here for `NOT IN` or `NOT LIKE`. Consume `not`; if the next
		// is neither, decline (matches the engine; `IS NOT NULL` puts `not` after `is`,
		// `NOT BETWEEN` isn't reachable).
		c.next() // consume `not`
		if c.peekIdentLower() == "in" {
			c.next() // consume `in`
			return reserializeInList(c, b, true, atoms)
		}
		if c.peekIdentLower() == "like" {
			c.next() // consume `like`
			return writeLikePattern(c, b, true)
		}
		return false
	default:
		opStr, ok := c.op()
		if !ok || !isCmpOp(opStr) {
			return false
		}
		litT, ok := c.next()
		if !ok || !isAtomLiteralTok(litT) {
			return false
		}
		b.WriteByte(' ')
		b.WriteString(opStr)
		b.WriteByte(' ')
		return writeLiteralTok(b, litT)
	}
}

// writeLikePattern re-emits `[NOT] LIKE '<pattern>'` (2d), mirroring the ENGINE's decline
// boundary exactly (arcx/src/parse.rs parse_like_pattern): the pattern must be a bare string
// literal, must NOT contain a backslash (arrow's kernel escapes `\`, DuckDB treats it literal
// — the one divergence), and must NOT be followed by an ESCAPE clause (arrow's kernel has no
// ESCAPE param). The pattern is re-escaped via escapeStringLiteral (doubled `”`), never
// sliced from source. The `col` and the caller-consumed `[NOT] LIKE` keyword precede this.
func writeLikePattern(c *cursor, b *strings.Builder, negated bool) bool {
	patT, ok := c.next()
	if !ok || patT.kind != tokStr {
		return false // non-literal pattern (ident/number/NULL) declines
	}
	if strings.Contains(patT.str, "\\") {
		return false // backslash pattern → engine declines; mirror it here
	}
	if c.peekIdentLower() == "escape" {
		return false // ESCAPE clause unsupported
	}
	if negated {
		b.WriteString(" NOT LIKE '")
	} else {
		b.WriteString(" LIKE '")
	}
	b.WriteString(escapeStringLiteral(patT.str))
	b.WriteByte('\'')
	return true
}

// reserializeInList re-emits `IN ( lit, … )` / `NOT IN ( … )` (2b-3). The engine desugars
// to Or-of-equals / And-of-not-equals; the router just re-serializes the list verbatim
// (each literal re-escaped/re-validated via writeLiteralTok) into whereText. `col` and the
// `IN`/`NOT IN` keyword are already written by the caller EXCEPT the keyword — write it here.
// Empty list, a NULL/non-literal element, a subquery all decline AT THE ROUTER (strict
// allowlist). Per-element atom bump with the cap check inside the loop (a huge IN declines).
func reserializeInList(c *cursor, b *strings.Builder, negated bool, atoms *int) bool {
	if negated {
		b.WriteString(" NOT IN (")
	} else {
		b.WriteString(" IN (")
	}
	// Require `(`.
	if lp, ok := c.next(); !ok || lp.kind != tokPunct || lp.punct != '(' {
		return false
	}
	// Empty `()` declines (DuckDB parser-errors; the engine declines too).
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ')' {
		return false
	}
	first := true
	for {
		*atoms++
		if *atoms > maxWhereAtoms {
			return false
		}
		lit, ok := c.next()
		if !ok || !isAtomLiteralTok(lit) {
			return false // EOF, a subquery `select`, a bare NULL ident — all decline
		}
		if !first {
			b.WriteString(", ")
		}
		first = false
		if !writeLiteralTok(b, lit) {
			return false
		}
		// Terminator: `,` continues, `)` ends, anything else declines.
		sep, ok := c.next()
		if !ok || sep.kind != tokPunct {
			return false
		}
		if sep.punct == ',' {
			continue
		}
		if sep.punct == ')' {
			break
		}
		return false
	}
	b.WriteByte(')')
	return true
}

// isAtomLiteralTok reports whether a token is an accepted predicate RHS literal.
func isAtomLiteralTok(t token) bool {
	return t.kind == tokNum || t.kind == tokFloat || t.kind == tokStr
}

// isNumericTok reports whether the token is an int or decimal-float literal (a 2e
// arithmetic operand — NOT a string).
func isNumericTok(t token) bool {
	return t.kind == tokNum || t.kind == tokFloat
}

// writeLiteralTok re-emits a literal token with re-escaping/re-validation — the same
// discipline buildScanSQL uses, so a crafted string can't break out of the quote.
func writeLiteralTok(b *strings.Builder, t token) bool {
	switch t.kind {
	case tokStr:
		b.WriteByte('\'')
		b.WriteString(escapeStringLiteral(t.str))
		b.WriteByte('\'')
		return true
	case tokFloat:
		if !isFloatLiteral(t.orig) {
			return false
		}
		b.WriteString(t.orig)
		return true
	case tokNum:
		if !isIntLiteral(t.orig) {
			return false
		}
		b.WriteString(t.orig)
		return true
	default:
		return false
	}
}

// isScanKeyword reports whether a lowercased ident is a clause keyword that must
// not be treated as a column in the scan grammar (guards `SELECT from FROM ...`).
func isScanKeyword(lower string) bool {
	switch lower {
	case "from", "where", "and", "or", "in", "order", "by", "group", "limit", "having", "as":
		return true
	}
	return false
}
