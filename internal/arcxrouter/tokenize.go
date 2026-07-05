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

import "strings"

type tokKind int

const (
	tokIdent tokKind = iota // keyword or identifier, incl. dotted db.measurement
	tokStr                  // single-quoted string literal (date_trunc unit)
	tokNum                  // run of digits (the 1 in GROUP BY 1)
	tokPunct                // one of ( ) * ,
)

type token struct {
	kind tokKind
	// lower is the lowercased text for idents (keyword comparison); orig preserves
	// the original spelling (measurement name, unit literal) where it matters.
	lower string
	orig  string
	str   string // unescaped content for tokStr
	punct byte   // the char for tokPunct
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
