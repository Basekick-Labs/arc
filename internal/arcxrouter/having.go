package arcxrouter

import (
	"strings"
)

// agg-5a HAVING recognition and re-serialization.
//
// The grammar is deliberately the SAME narrow one the engine accepts, so a shape
// the router sends can never be a shape the engine declines (a served-then-declined
// query is a wasted DuckDB re-execution plus a shadow decline, per gotcha #4):
//
//	having     := hAnd (OR hAnd)*
//	hAnd       := hAtom (AND hAtom)*
//	hAtom      := '(' having ')' | hRef <cmp op> <literal>
//	hRef       := <aggregate item re-spelled> | <tag key> | <bucket alias>
//
// Every emitted byte comes from a VALIDATED part: an aggregate reference is matched
// against the already-re-serialized select items and the ITEM TEXT is what gets
// written, never the user's spelling; a key reference is re-emitted from the
// validated bare identifier; a literal is re-emitted from its token. Nothing is
// copied out of the source string.
//
// Binding precedence matches the engine and the oracle: KEY first, then the bucket
// alias. `count(*) AS host … HAVING host = 'a'` binds the KEY column on v1.5.5.

// havingCtx is what a HAVING reference may resolve against: the re-serialized
// select items plus the names of the two possible key items.
type havingCtx struct {
	items       []string // re-serialized select items, in select order
	tagKey      string   // "" when the shape has no bare tag key
	bucketAlias string   // "" when the bucket is unaliased or absent
}

// reserializeHaving parses a HAVING clause and rebuilds it from validated parts.
// Returns ("", false) to decline — the caller then declines the whole shape.
func reserializeHaving(c *cursor, hc havingCtx) (string, bool) {
	var b strings.Builder
	depth := 0
	atoms := 0
	if !havingOr(c, &b, hc, &depth, &atoms) {
		return "", false
	}
	if depth != 0 || atoms == 0 {
		return "", false
	}
	return b.String(), true
}

func havingOr(c *cursor, b *strings.Builder, hc havingCtx, depth, atoms *int) bool {
	if !havingAnd(c, b, hc, depth, atoms) {
		return false
	}
	for c.peekIdentLower() == "or" {
		c.next()
		b.WriteString(" OR ")
		if !havingAnd(c, b, hc, depth, atoms) {
			return false
		}
	}
	return true
}

func havingAnd(c *cursor, b *strings.Builder, hc havingCtx, depth, atoms *int) bool {
	if !havingAtomOrParen(c, b, hc, depth, atoms) {
		return false
	}
	for c.peekIdentLower() == "and" {
		c.next()
		b.WriteString(" AND ")
		if !havingAtomOrParen(c, b, hc, depth, atoms) {
			return false
		}
	}
	return true
}

func havingAtomOrParen(c *cursor, b *strings.Builder, hc havingCtx, depth, atoms *int) bool {
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
		*depth++
		// Same cap as the WHERE tree: untrusted text must not be able to recurse the
		// engine's parser to death across the in-process FFI boundary (gotcha #1).
		if *depth > maxWhereDepth {
			return false
		}
		c.next()
		b.WriteString("(")
		if !havingOr(c, b, hc, depth, atoms) {
			return false
		}
		if !c.punct(')') {
			return false
		}
		b.WriteString(")")
		*depth--
		return true
	}
	return havingAtom(c, b, hc, atoms)
}

// havingAtom: `<ref> <cmp op> <literal>`.
func havingAtom(c *cursor, b *strings.Builder, hc havingCtx, atoms *int) bool {
	*atoms++
	if *atoms > maxWhereAtoms {
		return false
	}
	refText, ok := havingRef(c, hc)
	if !ok {
		return false
	}
	opT, tok := c.next()
	// `op` carries the NORMALISED comparison operator for tokOp (`orig` is not set
	// for operators); `isCmpOp` is the same gate the WHERE path applies.
	if !tok || opT.kind != tokOp || !isCmpOp(opT.op) {
		return false
	}
	litT, tok := c.next()
	if !tok {
		return false
	}
	var litText string
	switch litT.kind {
	case tokNum:
		litText = litT.orig
	case tokStr:
		// Re-emit from the UNESCAPED content with SQL-standard quote doubling, the
		// same way the WHERE path does — never by copying source bytes.
		litText = "'" + strings.ReplaceAll(litT.str, "'", "''") + "'"
	default:
		return false
	}
	b.WriteString(refText)
	b.WriteString(" ")
	b.WriteString(opT.op)
	b.WriteString(" ")
	b.WriteString(litText)
	return true
}

// havingRef resolves one reference and returns the TEXT to emit for it.
func havingRef(c *cursor, hc havingCtx) (string, bool) {
	t, tok := c.next()
	if !tok || t.kind != tokIdent {
		return "", false
	}
	// An aggregate call: re-serialize it with the SAME item parsers the select list
	// used, then match the result against the select items. Matching on
	// re-serialized text (not the user's spelling) is why `count( * )` and
	// `count(*)` behave identically, and why a function the select list spells
	// differently (`arg_max` vs `max_by`) does not half-match.
	isCall := c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '('
	if isCall {
		var itemText string
		switch t.lower {
		case "count", "sum", "min", "max", "avg":
			// Mirrors the select-list arm in matchGroupedAgg exactly (same accepted
			// forms, same emitted text); kept inline rather than refactoring that
			// proven arm into a shared helper.
			c.i++ // consume '('
			if t.lower == "count" && c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '*' {
				c.i++
				if !c.punct(')') {
					return "", false
				}
				itemText = "count(*)"
			} else {
				arg, tok := c.next()
				if !tok || arg.kind != tokIdent || isScanKeyword(arg.lower) {
					return "", false
				}
				if !c.punct(')') {
					return "", false // expression / DISTINCT / arity -> decline
				}
				itemText = t.lower + "(" + arg.orig + ")"
			}
		case "arg_max", "arg_min", "max_by", "min_by":
			var iok bool
			itemText, iok = parseTwoArgAggItem(c, t.lower)
			if !iok {
				return "", false
			}
		default:
			return "", false
		}
		for _, it := range hc.items {
			if it == itemText {
				return itemText, true
			}
		}
		// Legal DuckDB (an aggregate not in the select list) — declines here.
		return "", false
	}
	// A bare identifier: KEY first, then the bucket alias.
	if hc.tagKey != "" && strings.EqualFold(t.orig, hc.tagKey) {
		return hc.tagKey, true
	}
	if hc.bucketAlias != "" && strings.EqualFold(t.orig, hc.bucketAlias) {
		return hc.bucketAlias, true
	}
	return "", false
}
