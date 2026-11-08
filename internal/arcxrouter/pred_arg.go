package arcxrouter

import (
	"regexp"
	"strconv"
	"strings"
)

// agg-6b” (arcX feat/agg-6b2-predicate): predicate arguments of the four Boolean
// aggregates — `count_if(status >= 500)`, `bool_and(cpu_user < 90)`. The grammar
// below mirrors the engine's parse_pred_arg production for production, so a shape
// accepted here is one the engine serves; the engine re-parses the emitted text
// and renders DuckDB 1.5.6's derived column name itself.
//
//	or   := and ( OR and )*
//	and  := prim ( AND prim )*
//	prim := '(' or ')' | atom
//	atom := col cmp lit | col IS [NOT] NULL | col BETWEEN lit AND lit
//	      | col [NOT] LIKE 'pattern'
//
// Declined, as the engine declines them: IN / NOT IN, NOT, arithmetic, timestamp
// literals, non-canonical numbers (`007`, `00.5`, `-0`, `-0.0`), LIKE patterns
// with a backslash, map subscripts, qualified names, bare reserved keywords.
var predArgAggFns = map[string]bool{
	"count_if": true, "countif": true, "bool_and": true, "bool_or": true,
}

// duckdbNonColumnKeywords: DuckDB 1.5.6's reserved and type_function keywords
// (110 names, from duckdb_keywords()) minus the five 1.5.6 accepts as columns
// anyway (columns, generated, map, struct, try_cast). A bare one is not a column
// reference in DuckDB (`sum(end)` is a parser error), and the engine declines it
// in every column position; so does the router.
var duckdbNonColumnKeywords = map[string]bool{
	"all": true, "analyse": true, "analyze": true, "and": true, "anti": true, "any": true,
	"array": true, "as": true, "asc": true, "asof": true, "asymmetric": true, "at": true,
	"authorization": true, "binary": true, "both": true, "by": true, "case": true, "cast": true,
	"check": true, "collate": true, "collation": true, "column": true, "concurrently": true, "constraint": true, "create": true, "cross": true, "default": true,
	"deferrable": true, "desc": true, "describe": true, "distinct": true, "do": true,
	"else": true, "end": true, "except": true, "false": true, "fetch": true, "for": true,
	"foreign": true, "freeze": true, "from": true, "full": true, "glob": true,
	"group": true, "having": true, "ilike": true, "in": true, "initially": true, "inner": true,
	"intersect": true, "into": true, "is": true, "isnull": true, "join": true, "lambda": true,
	"lateral": true, "leading": true, "left": true, "like": true, "limit": true, "natural": true, "not": true, "notnull": true, "null": true, "offset": true, "on": true,
	"only": true, "or": true, "order": true, "outer": true, "overlaps": true, "pivot": true,
	"pivot_longer": true, "pivot_wider": true, "placing": true, "positional": true,
	"primary": true, "qualify": true, "references": true, "returning": true, "right": true,
	"select": true, "semi": true, "show": true, "similar": true, "some": true, "summarize": true, "symmetric": true, "table": true, "tablesample": true, "then": true,
	"to": true, "trailing": true, "true": true, "union": true, "unique": true,
	"unpack": true, "unpivot": true, "using": true, "variadic": true, "verbose": true,
	"when": true, "where": true, "window": true, "with": true,
}

const (
	maxPredArgAtoms = 64 // the engine's MAX_PRED_ARG_ATOMS
	maxPredArgDepth = 32 // the engine's MAX_PRED_DEPTH
)

var (
	canonicalInt     = regexp.MustCompile(`^(0|-?[1-9][0-9]*)$`)
	canonicalDecimal = regexp.MustCompile(`^-?(0|[1-9][0-9]*)\.[0-9]+$`)
	// The engine lexer's strict RFC3339-UTC timestamp literal: such a string is a
	// timestamp there (declined in a predicate argument), not a VARCHAR.
	engineTimestampLit = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$`)
)

// predArgFollows reports whether the aggregate argument at the cursor is a
// predicate rather than a bare column closed by `)` (the agg-6b' form).
func predArgFollows(c *cursor) bool {
	i := c.i
	bare := i+1 < len(c.toks) && c.toks[i].kind == tokIdent &&
		c.toks[i+1].kind == tokPunct && c.toks[i+1].punct == ')'
	return !bare
}

// reserializePredArg consumes a predicate argument (up to, not including, the
// call's closing paren) and re-emits it from validated tokens.
func reserializePredArg(c *cursor) (string, bool) {
	var b strings.Builder
	atoms := 0
	if !predArgOr(c, &b, 0, &atoms) {
		return "", false
	}
	return b.String(), true
}

func predArgOr(c *cursor, b *strings.Builder, depth int, atoms *int) bool {
	if !predArgAnd(c, b, depth, atoms) {
		return false
	}
	for c.peekIdentLower() == "or" {
		c.next()
		b.WriteString(" OR ")
		if !predArgAnd(c, b, depth, atoms) {
			return false
		}
	}
	return true
}

func predArgAnd(c *cursor, b *strings.Builder, depth int, atoms *int) bool {
	if !predArgPrim(c, b, depth, atoms) {
		return false
	}
	for c.peekIdentLower() == "and" {
		c.next()
		b.WriteString(" AND ")
		if !predArgPrim(c, b, depth, atoms) {
			return false
		}
	}
	return true
}

func predArgPrim(c *cursor, b *strings.Builder, depth int, atoms *int) bool {
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
		if depth+1 > maxPredArgDepth {
			return false
		}
		c.i++
		b.WriteByte('(')
		if !predArgOr(c, b, depth+1, atoms) || !c.punct(')') {
			return false
		}
		b.WriteByte(')')
		return true
	}
	*atoms++
	if *atoms > maxPredArgAtoms {
		return false
	}
	col, ok := c.next()
	// No qualifier: the router lexes `t.x` as one identifier, the engine's lexer
	// has no `.` at all.
	if !ok || col.kind != tokIdent || !isBareIdent(col.orig) || strings.Contains(col.orig, ".") ||
		isScanKeyword(col.lower) || duckdbNonColumnKeywords[col.lower] {
		return false
	}
	b.WriteString(col.orig)
	switch c.peekIdentLower() {
	case "is":
		c.next()
		b.WriteString(" IS ")
		if c.peekIdentLower() == "not" {
			c.next()
			b.WriteString("NOT ")
		}
		if c.peekIdentLower() != "null" {
			return false
		}
		c.next()
		b.WriteString("NULL")
		return true
	case "between":
		c.next()
		b.WriteString(" BETWEEN ")
		if !predArgLiteral(c, b) || c.peekIdentLower() != "and" {
			return false
		}
		c.next()
		b.WriteString(" AND ")
		return predArgLiteral(c, b)
	case "like", "not":
		if c.peekIdentLower() == "not" {
			c.next()
			if c.peekIdentLower() != "like" {
				return false
			}
			b.WriteString(" NOT")
		}
		c.next()
		pat, ok := c.next()
		// A backslash pattern: arrow treats `\` as an escape, DuckDB (no ESCAPE)
		// as a literal — the engine declines it.
		if !ok || pat.kind != tokStr || strings.ContainsRune(pat.str, '\\') {
			return false
		}
		b.WriteString(" LIKE '")
		b.WriteString(escapeStringLiteral(pat.str))
		b.WriteByte('\'')
		return true
	}
	op, ok := c.op()
	if !ok || !isCmpOp(op) {
		return false
	}
	b.WriteByte(' ')
	b.WriteString(op)
	b.WriteByte(' ')
	return predArgLiteral(c, b)
}

// predArgLiteral accepts the engine's canonical literal spellings only.
func predArgLiteral(c *cursor, b *strings.Builder) bool {
	t, ok := c.next()
	if !ok {
		return false
	}
	switch t.kind {
	case tokStr:
		if engineTimestampLit.MatchString(t.str) {
			return false
		}
	case tokNum:
		if !canonicalInt.MatchString(t.orig) {
			return false
		}
		if _, err := strconv.ParseInt(t.orig, 10, 64); err != nil {
			return false
		}
	case tokFloat:
		if !canonicalDecimal.MatchString(t.orig) || strings.Trim(t.orig, "-0.") == "" && strings.HasPrefix(t.orig, "-") {
			return false
		}
	default:
		return false
	}
	return writeLiteralTok(b, t)
}

// isPredArgItem re-validates a re-serialized `fn(<predicate>)` item at the SQL
// boundary: it must re-parse, completely, to exactly itself.
func isPredArgItem(item string) bool {
	open := strings.IndexByte(item, '(')
	if open < 0 || !strings.HasSuffix(item, ")") || !predArgAggFns[item[:open]] {
		return false
	}
	inner := item[open+1 : len(item)-1]
	toks, ok := tokenize(inner)
	if !ok {
		return false
	}
	c := &cursor{toks: toks}
	text, ok := reserializePredArg(c)
	return ok && c.atEnd() && text == inner
}
