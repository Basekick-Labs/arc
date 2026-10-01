package api

import "strings"

// tablePosition is a token the FROM-clause walker found standing where a
// table reference belongs.
type tablePosition struct {
	// tok is the token text; start and end are its byte span in the walked
	// string.
	tok        string
	start, end int
	// viaComma is true when the token follows a comma that continues an open
	// FROM clause's table list (a SQL-92 cross join: `FROM a, b`) rather than
	// the FROM or JOIN keyword itself.
	viaComma bool
	// introStart is the byte offset of the token that put tok in table
	// position: the FROM/JOIN keyword, or the comma.
	introStart int
}

// walkTablePositions tokenises normalised SQL — string literals masked to
// __STR_n__ / __IDENT_n__ placeholders, comments stripped — and calls visit
// for every token in table position: directly after FROM or JOIN, or after a
// comma that continues an in-progress FROM clause's table list. visit returns
// true to stop the walk early.
//
// It is the one FROM-clause state machine shared by the replacement-scan
// validator (maskedTokenInTablePosition), the storage-path rewriters, the RBAC
// table extractor and the cross-database check, so all of them agree on which
// commas introduce a table (#978). Positions reached through FROM/JOIN are
// reported too; the rewriters and the extractor cover those with the FROM/JOIN
// regex passes and act only on viaComma positions.
//
// FROM-clause state is a STACK keyed by parenthesis depth, not a single
// scalar: a subquery inside a FROM clause (`FROM (SELECT … FROM inner) a, b`)
// opens its OWN nested FROM clause whose end (on the closing paren) must NOT
// clear the outer clause — otherwise the trailing comma cross-join is wrongly
// disarmed (GHSA-w8x2 review, blocker 1). A comma continues the table list
// only while THIS depth's clause is armed, which excludes function-argument and
// projection commas: those sit at a deeper depth, or after a clause terminator
// (WHERE, GROUP, ORDER, SELECT in DuckDB's FROM-first form, …).
func walkTablePositions(normalised string, visit func(p tablePosition) (stop bool)) {
	// fromArmed[d] is true when, at paren depth d, we are inside a FROM clause
	// whose table list is still open. Indexed by depth; grows as needed.
	fromArmed := make([]bool, 1, 8)

	// afterFromJoin / afterComma: the immediately preceding token was FROM or
	// JOIN / an armed comma, so the very next atom is in table position.
	afterFromJoin, afterComma := false, false
	introStart := 0
	depth := 0

	for _, m := range tablePosTokenPattern.FindAllStringIndex(normalised, -1) {
		tok := normalised[m[0]:m[1]]
		switch tok {
		case "(":
			depth++
			if depth >= len(fromArmed) {
				fromArmed = append(fromArmed, false)
			} else {
				fromArmed[depth] = false
			}
			afterFromJoin, afterComma = false, false
			continue
		case ")":
			if depth > 0 {
				fromArmed[depth] = false
				depth--
			}
			// Leaving the paren group does NOT touch fromArmed[depth-1]: the
			// OUTER FROM clause (if any) is still open.
			afterFromJoin, afterComma = false, false
			continue
		case ",":
			// A comma continues the table list only if THIS depth's FROM
			// clause is still armed.
			afterFromJoin = false
			afterComma = fromArmed[depth]
			introStart = m[0]
			continue
		}

		// tok is a placeholder or an identifier/keyword run.
		if afterFromJoin || afterComma {
			if visit(tablePosition{tok: tok, start: m[0], end: m[1], viaComma: afterComma, introStart: introStart}) {
				return
			}
		}
		afterFromJoin, afterComma = false, false

		if strings.HasPrefix(tok, "__STR_") || strings.HasPrefix(tok, "__IDENT_") {
			// A placeholder — a value, a function argument, a quoted table —
			// closes the "immediately after" window but leaves the FROM
			// clause's armed state (for a following cross-join comma) alone.
			continue
		}

		switch lower := strings.ToLower(tok); lower {
		case "from", "join":
			fromArmed[depth] = true
			afterFromJoin = true
			introStart = m[0]
		default:
			// A real table name, alias, ON, USING, etc. keeps the clause armed
			// so a following `, b` cross-join is still a table position.
			// Keywords that close the table list at this depth disarm it:
			// after WHERE/GROUP/…, a top-level comma is a projection/ordering
			// separator, not another cross-join table.
			if fromClauseTerminator(lower) {
				fromArmed[depth] = false
			}
		}
	}
}

// commaJoinRef is a table reference that continues a FROM clause's table list
// after a cross-join comma — `FROM a, b` or `FROM a, db.b` — as located by
// findCommaJoinRefs in normalised SQL.
type commaJoinRef struct {
	// start is the byte offset of the introducing comma and end the offset
	// just past the reference, so sql[start:end] is `, b` or `,  db.b`. The
	// rewriters replace that whole span and re-emit the comma themselves.
	start, end int
	// db is "" for an unqualified reference. Either part may be an __IDENT_n__
	// placeholder for a quoted identifier; callers resolve it exactly as the
	// FROM/JOIN passes do.
	db, table string
}

// findCommaJoinRefs returns, in order of appearance, every table reference
// reached through a cross-join comma in normalised SQL (string literals masked,
// comments stripped — the form the storage-path rewriters and the RBAC
// extractor both work on, so both see the same references).
//
// Not reported, mirroring the guards of the FROM/JOIN passes: a token followed
// by `(` (a table function or a subquery), a dotted name whose dot is not
// adjacent on both sides (isDotOrCallAt leaves those alone too), a qualified
// name followed by `(` (`db.func(...)`), LATERAL (which introduces a subquery
// or a function, never a table), and the clause keywords the walker itself
// interprets, so the finder never consumes a token the state machine treats as
// structure.
func findCommaJoinRefs(sql string) []commaJoinRef {
	var refs []commaJoinRef
	walkTablePositions(sql, func(p tablePosition) bool {
		if !p.viaComma {
			return false
		}
		lower := strings.ToLower(p.tok)
		if lower == "lateral" || lower == "from" || lower == "join" || fromClauseTerminator(lower) {
			return false
		}
		rest := strings.TrimLeft(sql[p.end:], " \t\r\n")
		if len(rest) > 0 && rest[0] == '(' {
			return false
		}
		ref := commaJoinRef{start: p.introStart, end: p.end, table: p.tok}
		if len(rest) > 0 && rest[0] == '.' {
			if p.end >= len(sql) || sql[p.end] != '.' {
				return false
			}
			n := identRunLen(sql[p.end+1:])
			if n == 0 {
				return false
			}
			ref.db = p.tok
			ref.table = sql[p.end+1 : p.end+1+n]
			ref.end = p.end + 1 + n
			if after := strings.TrimLeft(sql[ref.end:], " \t\r\n"); len(after) > 0 && after[0] == '(' {
				return false
			}
		}
		refs = append(refs, ref)
		return false
	})
	return refs
}

// identRunLen returns the length of the run of identifier bytes at the start
// of s.
func identRunLen(s string) int {
	n := 0
	for n < len(s) && isIdentChar(s[n]) {
		n++
	}
	return n
}

// rewriteCommaJoinRefs replaces each comma-join reference in sql with the text
// fn returns for it, or leaves it untouched when fn reports false. The span
// handed to fn starts at the comma, so a replacement must re-emit the
// separator — the rewriters pass "," as the clause keyword to
// buildReadParquetExpr for exactly that. One scan, one rebuild: like
// replaceTableRefs, this never rescans a string it has already changed.
func rewriteCommaJoinRefs(sql string, fn func(ref commaJoinRef) (string, bool)) string {
	refs := findCommaJoinRefs(sql)
	if len(refs) == 0 {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	last := 0
	for _, ref := range refs {
		repl, ok := fn(ref)
		if !ok {
			continue
		}
		b.WriteString(sql[last:ref.start])
		b.WriteString(repl)
		last = ref.end
	}
	b.WriteString(sql[last:])
	return b.String()
}

// fromTableListContinues reports whether the FROM clause whose table name
// starts at or after pos (just past the `from ` keyword in lower-cased SQL)
// names more than the one table the single-table fast path can rewrite: the
// table, or its optional `[AS] alias`, is followed by a comma (a SQL-92 cross
// join, #978) or an opening paren (a table function or a column-alias list).
// A clause keyword in alias position (`FROM t WHERE (…)`) ends the list, so
// that common shape keeps the fast path.
func fromTableListContinues(sqlLower string, pos int) bool {
	skipWS := func() {
		for pos < len(sqlLower) && isWhitespace(sqlLower[pos]) {
			pos++
		}
	}
	readIdent := func() string {
		n := identRunLen(sqlLower[pos:])
		tok := sqlLower[pos : pos+n]
		pos += n
		return tok
	}
	continues := func() bool {
		return pos < len(sqlLower) && (sqlLower[pos] == ',' || sqlLower[pos] == '(')
	}

	skipWS()
	if readIdent() == "" {
		return false
	}
	skipWS()
	if continues() {
		return true
	}
	alias := readIdent()
	if alias == "" || fromClauseTerminator(alias) {
		return false
	}
	if alias == "as" {
		skipWS()
		if readIdent() == "" {
			return false
		}
	}
	skipWS()
	return continues()
}

// containsSQLWord reports whether lower-cased SQL contains word as a whole
// token — not preceded or followed by an identifier byte — whatever
// whitespace or punctuation surrounds it.
func containsSQLWord(sqlLower, word string) bool {
	for pos := 0; ; {
		idx := strings.Index(sqlLower[pos:], word)
		if idx < 0 {
			return false
		}
		idx += pos
		end := idx + len(word)
		if (idx == 0 || !isIdentChar(sqlLower[idx-1])) && (end == len(sqlLower) || !isIdentChar(sqlLower[end])) {
			return true
		}
		pos = idx + 1
	}
}
