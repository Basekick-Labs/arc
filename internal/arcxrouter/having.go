package arcxrouter

import (
	"strings"
)

// agg-5a HAVING recognition and re-serialization; res-3 (arcX
// feat/having-all-aggs) widens it to every served aggregate.
//
// The grammar is deliberately the SAME narrow one the engine accepts, so the router
// sends no FORM the engine's parser declines (a served-then-declined query is a
// wasted DuckDB re-execution plus a shadow decline, per gotcha #4). Column TYPES are
// invisible here, so a literal of the wrong type for its target (a decimal against a
// VARCHAR key or `max(time)`, a string against `count(*)`, a timestamp written without
// the `T…Z` form) still reaches the engine and declines at bind:
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
// Literal typing the router CAN see (res-3 review M6) is enforced: TRUE/FALSE and
// the bare form only against a bool_and/bool_or item and only with = / <> / !=; a
// numeric literal never against a Boolean item nor against the bucket alias (a
// timestamp). Everything else is type-checked by the engine (see above).

// havingCtx is what a HAVING reference may resolve against: the re-serialized
// select items plus the names of the possible key items and the aggregate aliases.
type havingCtx struct {
	items       []string // re-serialized select items, in select order
	tagKey      string   // "" when the shape has no bare tag key
	bucketAlias string   // "" when the bucket is unaliased or absent
	aliases     []string // res-3: per-item aggregate alias ("" = none), len == len(items)
	bucketCol   string   // res-3b: the bucket's source column ("" = no bucket)
}

// refCmpClass: what a reference may be compared WITH in a res-3b reference-vs-
// reference atom. The engine compares only ORDER-EXACT items (arcX res-3b review
// B1: DuckDB 1.5.6's float accumulators are thread-count dependent) and only
// compatible types; the router mirrors that with what it can see.
type refCmpClass int

const (
	cmpNone refCmpClass = iota // tag key (type unknown), or a float-accumulated / unstable item
	cmpNum                     // count / count_if / regr_count / bit_* / min / max / quantile_disc
	cmpBool                    // bool_and / bool_or
	cmpTime                    // the bucket alias, or min/max over the bucket's source column
)

// havingRefInfo is what havingRef learned about one reference.
type havingRefInfo struct {
	kind havingRefKind
	cmp  refCmpClass
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
	// res-3b: hidden aggregates (HAVING references not in the select list), as item
	// text in first-reference order, and whether any of them is a bucket-gated
	// family (moment / order statistic / binary — the tag-key gate in
	// matchGroupedAgg applies to them as to select items).
	hidden       []string
	hiddenMoment bool
}

// reserializeHaving parses a HAVING clause and rebuilds it from validated parts.
// Returns ok=false to decline — the caller then declines the whole shape.
func reserializeHaving(c *cursor, hc havingCtx) (text string, st havingState, ok bool) {
	var b strings.Builder
	if !havingOr(c, &b, hc, &st) {
		return "", st, false
	}
	if st.depth != 0 || st.atoms == 0 {
		return "", st, false
	}
	return b.String(), st, true
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
	// res-3b: prefix NOT (any depth) — the engine pushes it down at parse; it counts
	// toward the same depth cap there.
	if c.peekIdentLower() == "not" {
		st.depth++
		if st.depth > maxWhereDepth {
			return false
		}
		c.next()
		b.WriteString("NOT ")
		if !havingAtomOrParen(c, b, hc, st) {
			return false
		}
		st.depth--
		return true
	}
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

// havingLiteral validates and re-emits one literal compared against a reference of
// the given kind (with op, "" for IN/BETWEEN elements). Returns ok=false for a
// literal the engine would decline against it.
func havingLiteral(litT token, kind havingRefKind, op string) (string, bool) {
	switch {
	case litT.kind == tokIdent && (litT.lower == "true" || litT.lower == "false"):
		// res-3: Boolean literals only against a Boolean item, and only equality —
		// DuckDB's boolean ordering (`> false`) is not mirrored by the engine.
		if kind != refBool || (op != "=" && op != "<>" && op != "!=") {
			return "", false
		}
		return strings.ToUpper(litT.lower), true
	case kind == refBool:
		return "", false // `= 1` / `= 'true'` go through DuckDB casts the engine declines
	case litT.kind == tokNum || litT.kind == tokFloat:
		if kind == refTime {
			return "", false // a number against the timestamp bucket: the engine declines
		}
		// tokFloat: the tokenizer already refused a decimal wider than 15
		// significant digits (decimalTooWide) — the bound that makes the engine's
		// integer-vs-decimal rewrite exact (arcX res-3).
		return litT.orig, true
	case litT.kind == tokStr:
		// Re-emit from the UNESCAPED content with SQL-standard quote doubling, the
		// same way the WHERE path does — never by copying source bytes.
		return "'" + strings.ReplaceAll(litT.str, "'", "''") + "'", true
	}
	return "", false
}

// havingIsLiteral: the token starts a literal (not a reference).
func havingIsLiteral(t token) bool {
	return t.kind == tokNum || t.kind == tokFloat || t.kind == tokStr ||
		(t.kind == tokIdent && (t.lower == "true" || t.lower == "false"))
}

func (st *havingState) bumpAtoms(n int) bool {
	st.atoms += n
	return st.atoms <= maxWhereAtoms
}

// refsComparable: may two references meet in `<ref> <op> <ref>`? Both order-exact
// and of compatible kinds (see refCmpClass).
func refsComparable(a, b havingRefInfo) bool {
	return a.cmp != cmpNone && a.cmp == b.cmp
}

// havingAtom (res-3b grammar, mirroring the engine's parse_having_atom):
//
//	<ref> <cmp> <literal> | <literal> <cmp> <ref> | <ref> <cmp> <ref>
//	<ref> IS [NOT] NULL | <ref> [NOT] BETWEEN <lit> AND <lit> | <ref> [NOT] IN (<lit>, …)
//	<ref>                                    -- bare: a Boolean item
//
// Atom accounting matches the engine's: one per atom, plus one for a BETWEEN's
// second bound and one per extra IN element.
func havingAtom(c *cursor, b *strings.Builder, hc havingCtx, st *havingState) bool {
	if !st.bumpAtoms(1) {
		return false
	}
	// Literal on the left: `5 < count(*)`.
	if c.i < len(c.toks) && havingIsLiteral(c.toks[c.i]) {
		litT, _ := c.next()
		opT, tok := c.next()
		if !tok || opT.kind != tokOp || !isCmpOp(opT.op) {
			return false
		}
		if c.i < len(c.toks) && havingIsLiteral(c.toks[c.i]) {
			return false // literal vs literal
		}
		refText, info, ok := havingRef(c, hc, st)
		if !ok {
			return false
		}
		litText, lok := havingLiteral(litT, info.kind, opT.op)
		if !lok {
			return false
		}
		b.WriteString(litText + " " + opT.op + " " + refText)
		return true
	}
	refText, info, ok := havingRef(c, hc, st)
	if !ok {
		return false
	}
	kind := info.kind
	if havingAtomEnds(c) {
		if kind != refBool {
			return false
		}
		b.WriteString(refText)
		return true
	}
	// IS [NOT] NULL — any reference.
	if c.peekIdentLower() == "is" {
		c.next()
		not := ""
		if c.peekIdentLower() == "not" {
			c.next()
			not = "NOT "
		}
		if !c.ident("null") {
			return false
		}
		b.WriteString(refText + " IS " + not + "NULL")
		return true
	}
	not := ""
	if c.peekIdentLower() == "not" {
		c.next()
		not = "NOT "
	}
	switch c.peekIdentLower() {
	case "between":
		c.next()
		if kind == refBool {
			return false
		}
		lo, tok := c.next()
		if !tok {
			return false
		}
		loText, lok := havingLiteral(lo, kind, "")
		if !lok || !c.ident("and") {
			return false
		}
		hi, tok := c.next()
		if !tok {
			return false
		}
		hiText, hok := havingLiteral(hi, kind, "")
		if !hok || !st.bumpAtoms(1) {
			return false
		}
		b.WriteString(refText + " " + not + "BETWEEN " + loText + " AND " + hiText)
		return true
	case "in":
		c.next()
		if kind == refBool || !c.punct('(') {
			return false
		}
		var elems []string
		for {
			lt, tok := c.next()
			// A NULL / column / TRUE in the list is an identifier: refused, as the
			// engine declines it (`NOT IN (…, NULL)` is never TRUE in DuckDB).
			if !tok || lt.kind == tokIdent {
				return false
			}
			et, eok := havingLiteral(lt, kind, "")
			if !eok {
				return false
			}
			elems = append(elems, et)
			if len(elems) > 1 && !st.bumpAtoms(1) {
				return false
			}
			// `c.punct` consumes even on a miss: peek before choosing.
			if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == ',' {
				c.i++
				continue
			}
			if !c.punct(')') {
				return false
			}
			break
		}
		b.WriteString(refText + " " + not + "IN (" + strings.Join(elems, ", ") + ")")
		return true
	}
	if not != "" {
		return false
	}
	opT, tok := c.next()
	// `op` carries the NORMALISED comparison operator for tokOp (`orig` is not set
	// for operators); `isCmpOp` is the same gate the WHERE path applies.
	if !tok || opT.kind != tokOp || !isCmpOp(opT.op) {
		return false
	}
	if c.i < len(c.toks) && havingIsLiteral(c.toks[c.i]) {
		litT, _ := c.next()
		litText, lok := havingLiteral(litT, kind, opT.op)
		if !lok {
			return false
		}
		b.WriteString(refText + " " + opT.op + " " + litText)
		return true
	}
	// res-3b: reference vs reference — order-exact, compatible kinds only.
	rhsText, rhs, rok := havingRef(c, hc, st)
	if !rok || !refsComparable(info, rhs) {
		return false
	}
	b.WriteString(refText + " " + opT.op + " " + rhsText)
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

// havingItemInfo: literal kind + reference-comparison class of an aggregate item.
func havingItemInfo(item string, hc havingCtx, st *havingState) havingRefInfo {
	info := havingRefInfo{kind: havingItemKind(item, st)}
	open := strings.IndexByte(item, '(')
	if open < 0 {
		return info
	}
	fn, arg := item[:open], strings.TrimSuffix(item[open+1:], ")")
	switch fn {
	case "bool_and", "bool_or":
		info.cmp = cmpBool
	case "count", "count_if", "countif", "regr_count", "bit_and", "bit_or", "bit_xor":
		info.cmp = cmpNum
	case "min", "max":
		if hc.bucketCol != "" && strings.EqualFold(arg, hc.bucketCol) {
			info.cmp = cmpTime
		} else {
			info.cmp = cmpNum
		}
	case "quantile_disc", "quantile":
		info.cmp = cmpNum
	}
	// sum (the router cannot tell an integer sum from a float one), avg, the
	// moments, the bivariates, median/quantile_cont/mad, the Kahan sums, shape
	// statistics and arg_* stay cmpNone.
	return info
}

// sqlBareValueFunctions: SQL value functions DuckDB 1.5.6 evaluates when written
// bare (`user` is current_user). Mirrors the engine's is_bare_value_function.
var sqlBareValueFunctions = map[string]bool{
	"current_date": true, "current_time": true, "current_timestamp": true,
	"localtime": true, "localtimestamp": true, "current_user": true, "session_user": true,
	"current_role": true, "current_schema": true, "current_catalog": true, "user": true,
}

// aliasRefBindable: may a bare HAVING / ORDER BY name resolve to a select alias?
// Not when DuckDB 1.5.6 binds it to a keyword or a value function instead (swept
// over every duckdb_keywords() name: exactly duckdbNonColumnKeywords plus `user`,
// and the value functions above).
func aliasRefBindable(lower string) bool {
	return !duckdbNonColumnKeywords[lower] && !sqlBareValueFunctions[lower]
}

// maxGroupedItems mirrors the engine's MAX_AGG_ITEMS: select items plus hidden
// HAVING aggregates.
const maxGroupedItems = 64

// havingArgItem: the agg-4 by-time items (DuckDB's tie choice is nondeterministic);
// the same prefixes as isArgItem, which lives in a tagged file.
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
func havingRef(c *cursor, hc havingCtx, st *havingState) (string, havingRefInfo, bool) {
	t, tok := c.next()
	if !tok || t.kind != tokIdent {
		return "", havingRefInfo{}, false
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
			return "", havingRefInfo{}, false
		case quantileAggFns[t.lower]:
			// res-3: the select list's own parser (scalar fractions only — a LIST
			// quantile never lexes as an item, so it cannot be referenced).
			var qok bool
			itemText, qok = parseQuantileAggItem(c, t.lower)
			if !qok {
				return "", havingRefInfo{}, false
			}
		case unaryAggFns[t.lower]:
			// Mirrors the select-list arm in matchGroupedAgg exactly (same accepted
			// forms, same emitted text); kept inline rather than refactoring that
			// proven arm into a shared helper.
			c.i++ // consume '('
			if t.lower == "count" && c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '*' {
				c.i++
				if !c.punct(')') {
					return "", havingRefInfo{}, false
				}
				itemText = "count(*)"
			} else if predArgAggFns[t.lower] && predArgFollows(c) {
				// res-3 (review M4): a predicate argument, re-serialized by the select
				// list's own canonicaliser — a text match here implies the engine's
				// structural match (the reverse can miss, which only declines).
				text, ptok := reserializePredArg(c)
				if !ptok || !c.punct(')') {
					return "", havingRefInfo{}, false
				}
				itemText = t.lower + "(" + text + ")"
			} else {
				arg, tok := c.next()
				if !tok || arg.kind != tokIdent || isScanKeyword(arg.lower) || duckdbNonColumnKeywords[arg.lower] {
					return "", havingRefInfo{}, false
				}
				if !c.punct(')') {
					return "", havingRefInfo{}, false // expression / DISTINCT / arity -> decline
				}
				itemText = t.lower + "(" + arg.orig + ")"
			}
		case twoArgAggFns[t.lower] || binaryMomentFns[t.lower]:
			var iok bool
			itemText, iok = parseTwoArgAggItem(c, t.lower)
			if !iok {
				return "", havingRefInfo{}, false
			}
		default:
			return "", havingRefInfo{}, false
		}
		for _, it := range hc.items {
			if it == itemText {
				return itemText, havingItemInfo(itemText, hc, st), true
			}
		}
		// res-3b: an aggregate not in the select list is a HIDDEN item — the engine
		// computes it, filters on it and projects it away. It was parsed by the
		// select list's own item parsers above (so it is a routable item), and the
		// bucket-gated families are recorded for matchGroupedAgg's tag-key gate.
		found := false
		for _, h := range st.hidden {
			if h == itemText {
				found = true
				break
			}
		}
		if !found {
			if len(hc.items)+len(st.hidden) >= maxGroupedItems {
				return "", havingRefInfo{}, false
			}
			st.hidden = append(st.hidden, itemText)
			if momentAggFns[t.lower] || quantileAggFns[t.lower] || binaryMomentFns[t.lower] {
				st.hiddenMoment = true
			}
		}
		return itemText, havingItemInfo(itemText, hc, st), true
	}
	// res-3 post-impl review B1: a name DuckDB binds to something else — a keyword
	// (`true`, `null` are literals) or a bare SQL value function (`current_timestamp`
	// is now()) — never resolves to a key or an alias in DuckDB. An alias is emitted
	// as its ITEM TEXT, so the engine could not catch it: refuse every bare form here
	// (the engine declines the same set).
	if !aliasRefBindable(t.lower) {
		return "", havingRefInfo{}, false
	}
	// A bare identifier: KEY first, then the bucket alias, then an aggregate alias.
	if hc.tagKey != "" && strings.EqualFold(t.orig, hc.tagKey) {
		return hc.tagKey, havingRefInfo{kind: refAny, cmp: cmpNone}, true
	}
	if hc.bucketAlias != "" && strings.EqualFold(t.orig, hc.bucketAlias) {
		return hc.bucketAlias, havingRefInfo{kind: refTime, cmp: cmpTime}, true
	}
	for i, a := range hc.aliases {
		if a != "" && strings.EqualFold(a, t.orig) && i < len(hc.items) {
			return hc.items[i], havingItemInfo(hc.items[i], hc, st), true
		}
	}
	return "", havingRefInfo{}, false
}
