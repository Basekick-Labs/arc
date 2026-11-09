package arcxrouter

import (
	"strings"
)

// agg-5a HAVING recognition and re-serialization; res-3 (arcX
// feat/having-all-aggs) widens it to every served aggregate.
//
// The grammar is deliberately the SAME narrow one the engine accepts, so a shape
// the router sends can never be a shape the engine declines (a served-then-declined
// query is a wasted DuckDB re-execution plus a shadow decline, per gotcha #4):
//
//	having     := hAnd (OR hAnd)*
//	hAnd       := hAtom (AND hAtom)*
//	hAtom      := '(' having ')' | hRef <cmp op> <literal> | hRef <eq op> TRUE|FALSE | hRef
//	hRef       := <aggregate item re-spelled> | <tag key> | <bucket alias> | <aggregate alias>
//
// Every emitted byte comes from a VALIDATED part: an aggregate reference is matched
// against the already-re-serialized select items and the ITEM TEXT is what gets
// written, never the user's spelling — an aggregate ALIAS is emitted as its item
// text too (res-3 review M5), so a routed query never depends on the engine's alias
// resolution; a key reference is re-emitted from the validated bare identifier; a
// literal is re-emitted from its token. Nothing is copied out of the source string.
//
// Binding precedence matches the engine and the oracle: KEY first, then the bucket
// alias, then an aggregate alias. `count(*) AS host … HAVING host = 'a'` binds the
// KEY column on v1.5.5 (res-1 declines that collision anyway); an aggregate alias
// beats an UNGROUPED table column of the same name on 1.5.6.
//
// Literal typing the router can see (res-3 review M6), so the engine is never
// handed a compare it declines: TRUE/FALSE and the bare form only against a
// bool_and/bool_or item and only with = / <> / !=; a numeric literal never against
// a Boolean item nor against the bucket alias (a timestamp).

// havingCtx is what a HAVING reference may resolve against: the re-serialized
// select items plus the names of the possible key items and the aggregate aliases.
type havingCtx struct {
	items       []string // re-serialized select items, in select order
	tagKey      string   // "" when the shape has no bare tag key
	bucketAlias string   // "" when the bucket is unaliased or absent
	aliases     []string // res-3: per-item aggregate alias ("" = none), len == len(items)
}

// HAVING reference kinds — what a literal may be compared against.
type havingRefKind int

const (
	refAny  havingRefKind = iota // tag key or a non-Boolean aggregate: engine type-checks
	refBool                      // bool_and / bool_or item
	refTime                      // the bucket (timestamp): string literals only
)

// havingState accumulates per-tree facts while parsing.
type havingState struct {
	depth, atoms int
	// unstable is the shadow class of the most permissive unstable item the tree
	// references ("" none, "arg", "oracle"): a HAVING over an item whose DuckDB value
	// legitimately moves (arg ties, Kahan sums, cancellation-prone bivariates, shape
	// statistics) can flip a group in or out of the result. See
	// HavingUnstable on Decision.
	unstable string
}

// reserializeHaving parses a HAVING clause and rebuilds it from validated parts.
// Returns ok=false to decline — the caller then declines the whole shape.
func reserializeHaving(c *cursor, hc havingCtx) (text string, unstable string, ok bool) {
	var b strings.Builder
	var st havingState
	if !havingOr(c, &b, hc, &st) {
		return "", "", false
	}
	if st.depth != 0 || st.atoms == 0 {
		return "", "", false
	}
	return b.String(), st.unstable, true
}

func havingOr(c *cursor, b *strings.Builder, hc havingCtx, st *havingState) bool {
	if !havingAnd(c, b, hc, st) {
		return false
	}
	for c.peekIdentLower() == "or" {
		c.next()
		b.WriteString(" OR ")
		if !havingAnd(c, b, hc, st) {
			return false
		}
	}
	return true
}

func havingAnd(c *cursor, b *strings.Builder, hc havingCtx, st *havingState) bool {
	if !havingAtomOrParen(c, b, hc, st) {
		return false
	}
	for c.peekIdentLower() == "and" {
		c.next()
		b.WriteString(" AND ")
		if !havingAtomOrParen(c, b, hc, st) {
			return false
		}
	}
	return true
}

func havingAtomOrParen(c *cursor, b *strings.Builder, hc havingCtx, st *havingState) bool {
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' {
		st.depth++
		// Same cap as the WHERE tree: untrusted text must not be able to recurse the
		// engine's parser to death across the in-process FFI boundary (gotcha #1).
		if st.depth > maxWhereDepth {
			return false
		}
		c.next()
		b.WriteString("(")
		if !havingOr(c, b, hc, st) {
			return false
		}
		if !c.punct(')') {
			return false
		}
		b.WriteString(")")
		st.depth--
		return true
	}
	return havingAtom(c, b, hc, st)
}

// havingAtomEnds: the token after a reference ends a BARE atom (`HAVING
// bool_and(b)`) — the same set the engine's parse_having_atom checks.
func havingAtomEnds(c *cursor) bool {
	if c.i >= len(c.toks) {
		return true
	}
	t := c.toks[c.i]
	if t.kind == tokPunct && t.punct == ')' {
		return true
	}
	if t.kind == tokIdent {
		switch t.lower {
		case "and", "or", "order", "limit":
			return true
		}
	}
	return false
}

// havingAtom: `<ref> <cmp op> <literal>`, `<bool ref> <=|<>|!=> TRUE|FALSE`, or a
// bare `<bool ref>`.
func havingAtom(c *cursor, b *strings.Builder, hc havingCtx, st *havingState) bool {
	st.atoms++
	if st.atoms > maxWhereAtoms {
		return false
	}
	refText, kind, ok := havingRef(c, hc, st)
	if !ok {
		return false
	}
	if havingAtomEnds(c) {
		if kind != refBool {
			return false
		}
		b.WriteString(refText)
		return true
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
	switch {
	case litT.kind == tokIdent && (litT.lower == "true" || litT.lower == "false"):
		// res-3: Boolean literals only against a Boolean item, and only equality —
		// DuckDB's boolean ordering (`> false`) is not mirrored by the engine.
		if kind != refBool || (opT.op != "=" && opT.op != "<>" && opT.op != "!=") {
			return false
		}
		litText = strings.ToUpper(litT.lower)
	case kind == refBool:
		return false // `= 1` / `= 'true'` go through DuckDB casts the engine declines
	case litT.kind == tokNum || litT.kind == tokFloat:
		if kind == refTime {
			return false // a number against the timestamp bucket: the engine declines
		}
		// tokFloat: the tokenizer already refused a decimal wider than 15
		// significant digits (decimalTooWide) — the bound that makes the engine's
		// integer-vs-decimal rewrite exact (arcX res-3).
		litText = litT.orig
	case litT.kind == tokStr:
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

// havingItemKind classifies a resolved aggregate item for literal typing, and
// records its shadow-stability class on st.
func havingItemKind(item string, st *havingState) havingRefKind {
	switch {
	case havingArgItem(item):
		if st.unstable == "" {
			st.unstable = "arg"
		}
	case orderTargetRefused(item):
		st.unstable = "oracle"
	}
	if strings.HasPrefix(item, "bool_and(") || strings.HasPrefix(item, "bool_or(") {
		return refBool
	}
	return refAny
}

// havingArgItem: the agg-4 by-time items (DuckDB's tie choice is nondeterministic).
func havingArgItem(item string) bool {
	for _, p := range []string{"arg_max(", "arg_min(", "max_by(", "min_by("} {
		if strings.HasPrefix(item, p) {
			return true
		}
	}
	return false
}

// havingRef resolves one reference and returns the TEXT to emit for it and what
// kind of literal it may be compared against.
func havingRef(c *cursor, hc havingCtx, st *havingState) (string, havingRefKind, bool) {
	t, tok := c.next()
	if !tok || t.kind != tokIdent {
		return "", refAny, false
	}
	// An aggregate call: re-serialize it with the SAME item parsers the select list
	// used, then match the result against the select items. Matching on
	// re-serialized text (not the user's spelling) is why `count( * )` and
	// `count(*)` behave identically, and why a function the select list spells
	// differently (`arg_max` vs `max_by`) does not half-match.
	isCall := c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '('
	if isCall {
		var itemText string
		switch {
		case t.lower == "mode":
			// agg-7: grouped `mode` is never routed (matchGroupedAgg), so it can
			// never be a select item here.
			return "", refAny, false
		case quantileAggFns[t.lower]:
			// res-3: the select list's own parser (scalar fractions only — a LIST
			// quantile never lexes as an item, so it cannot be referenced).
			var qok bool
			itemText, qok = parseQuantileAggItem(c, t.lower)
			if !qok {
				return "", refAny, false
			}
		case unaryAggFns[t.lower]:
			// Mirrors the select-list arm in matchGroupedAgg exactly (same accepted
			// forms, same emitted text); kept inline rather than refactoring that
			// proven arm into a shared helper.
			c.i++ // consume '('
			if t.lower == "count" && c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '*' {
				c.i++
				if !c.punct(')') {
					return "", refAny, false
				}
				itemText = "count(*)"
			} else if predArgAggFns[t.lower] && predArgFollows(c) {
				// res-3 (review M4): a predicate argument, re-serialized by the select
				// list's own canonicaliser — a text match here implies the engine's
				// structural match (the reverse can miss, which only declines).
				text, ptok := reserializePredArg(c)
				if !ptok || !c.punct(')') {
					return "", refAny, false
				}
				itemText = t.lower + "(" + text + ")"
			} else {
				arg, tok := c.next()
				if !tok || arg.kind != tokIdent || isScanKeyword(arg.lower) || duckdbNonColumnKeywords[arg.lower] {
					return "", refAny, false
				}
				if !c.punct(')') {
					return "", refAny, false // expression / DISTINCT / arity -> decline
				}
				itemText = t.lower + "(" + arg.orig + ")"
			}
		case twoArgAggFns[t.lower] || binaryMomentFns[t.lower]:
			var iok bool
			itemText, iok = parseTwoArgAggItem(c, t.lower)
			if !iok {
				return "", refAny, false
			}
		default:
			return "", refAny, false
		}
		for _, it := range hc.items {
			if it == itemText {
				return itemText, havingItemKind(itemText, st), true
			}
		}
		// Legal DuckDB (an aggregate not in the select list) — declines here.
		return "", refAny, false
	}
	// A bare identifier: KEY first, then the bucket alias, then an aggregate alias.
	if hc.tagKey != "" && strings.EqualFold(t.orig, hc.tagKey) {
		return hc.tagKey, refAny, true
	}
	if hc.bucketAlias != "" && strings.EqualFold(t.orig, hc.bucketAlias) {
		return hc.bucketAlias, refTime, true
	}
	for i, a := range hc.aliases {
		if a != "" && strings.EqualFold(a, t.orig) && i < len(hc.items) {
			return hc.items[i], havingItemKind(hc.items[i], st), true
		}
	}
	return "", refAny, false
}
