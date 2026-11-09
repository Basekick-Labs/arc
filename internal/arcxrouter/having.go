package arcxrouter

import (
	"math"
	"strconv"
	"strings"
)

// agg-5a HAVING recognition and re-serialization; res-3 (arcX
// feat/having-all-aggs) widens it to every served aggregate, res-3b (arcX
// feat/having-grammar) to the grammar below.
//
// The grammar is deliberately the SAME one the engine accepts, so the router sends
// no FORM the engine's parser declines (a served-then-declined query is a wasted
// DuckDB re-execution plus a shadow decline, per gotcha #4). Column TYPES are
// invisible here, so a literal or reference of the wrong type for its target (a
// decimal against a VARCHAR key, a string against `count(*)`, a timestamp written
// without the `T…Z` form, `max(<string or time column>)` against a number) still
// reaches the engine and declines at bind:
//
//	having     := hAnd (OR hAnd)*
//	hAnd       := hNot (AND hNot)*
//	hNot       := NOT hNot | '(' having ')' | hAtom
//	hAtom      := hRef <cmp> <literal> | <literal> <cmp> hRef | hRef <cmp> hRef
//	            | hRef IS [NOT] NULL | hRef [NOT] BETWEEN <lit> AND <lit>
//	            | hRef [NOT] IN (<lit>, …) | hRef <eq op> TRUE|FALSE | hRef
//	hRef       := <aggregate item re-spelled> | <tag key> | <bucket alias> | <aggregate alias>
//
// An aggregate that is not in the select list is a HIDDEN item (res-3b): parsed
// with the select list's own item parsers, emitted as item text, computed by the
// engine and projected away.
//
// Every emitted byte comes from a VALIDATED part: an aggregate reference is matched
// against the already-re-serialized select items (or becomes a hidden item) and the
// ITEM TEXT is what gets written, never the user's spelling — an aggregate ALIAS is
// emitted as its item
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
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '(' && !havingParenIsOperand(c) {
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

// res-3c operands. hNode is one node of a parsed HAVING operand: a reference, a
// literal, unary minus, a parenthesised operand or a binary `+ - * /`. Its text is
// what gets emitted — built from validated parts, tokens separated by spaces (a
// `-` next to another `-` would otherwise form a `--` comment), literals in their
// original spelling (review L4).
type hNodeKind int

const (
	hRef hNodeKind = iota
	hLit
	hNeg
	hParen
	hBin
)

type hNode struct {
	kind hNodeKind
	op   byte   // hBin: '+', '-', '*', '/'
	l, r *hNode // hBin children; l is the operand of hNeg / hParen
	text string // the emitted text of the whole node
	info havingRefInfo
	tag  bool  // hRef: the bare tag key
	tok  token // hLit
}

func (n *hNode) hasRef() bool {
	switch n.kind {
	case hRef:
		return true
	case hLit:
		return false
	case hBin:
		return n.l.hasRef() || n.r.hasRef()
	}
	return n.l.hasRef()
}

// contains: does the subtree reference the item emitted as text?
func (n *hNode) contains(text string) bool {
	switch n.kind {
	case hRef:
		return n.text == text
	case hLit:
		return false
	case hBin:
		return n.l.contains(text) || n.r.contains(text)
	}
	return n.l.contains(text)
}

func (n *hNode) refs(out []*hNode) []*hNode {
	switch n.kind {
	case hRef:
		return append(out, n)
	case hLit:
		return out
	case hBin:
		return n.r.refs(n.l.refs(out))
	}
	return n.l.refs(out)
}

// operandCx is the per-comparison operand state: one node budget shared by both
// sides, the engine's MAX_ARITH_NODES.
type operandCx struct{ nodes int }

const maxHavingNodes = 64

func (cx *operandCx) bump() bool {
	cx.nodes++
	return cx.nodes <= maxHavingNodes
}

// signedNumber: a numeric literal token carrying a leading '-'. After an operand
// DuckDB (and the engine) read it as a BINARY minus of the positive term with full
// precedence: `min(x) -2 * min(x)` is `min(x) - (2 * min(x))` (arcX res-3c review M2).
func signedNumber(c *cursor) bool {
	if c.i >= len(c.toks) {
		return false
	}
	t := c.toks[c.i]
	return (t.kind == tokNum || t.kind == tokFloat || t.kind == tokSci) && strings.HasPrefix(t.orig, "-")
}

func litNode(t token) *hNode {
	text := t.orig
	switch {
	case t.kind == tokStr:
		// Re-emitted from the UNESCAPED content with quote doubling, as the WHERE path.
		text = "'" + strings.ReplaceAll(t.str, "'", "''") + "'"
	case t.kind == tokIdent:
		text = strings.ToUpper(t.lower) // TRUE / FALSE
	}
	return &hNode{kind: hLit, tok: t, text: text}
}

// havingSum: `term (('+' | '-') term | <signed number> term-rest)*`.
func havingSum(c *cursor, hc havingCtx, st *havingState, cx *operandCx) (*hNode, bool) {
	lhs, ok := havingTerm(c, hc, st, cx, nil)
	if !ok {
		return nil, false
	}
	for c.i < len(c.toks) {
		t := c.toks[c.i]
		var op byte
		var first *hNode
		switch {
		case t.kind == tokPunct && (t.punct == '+' || t.punct == '-'):
			c.i++
			op = t.punct
		case signedNumber(c):
			c.i++
			pos := t
			pos.orig = t.orig[1:]
			if !cx.bump() {
				return nil, false
			}
			op, first = '-', litNode(pos)
		default:
			return lhs, true
		}
		rhs, ok := havingTerm(c, hc, st, cx, first)
		if !ok || !cx.bump() {
			return nil, false
		}
		lhs = &hNode{kind: hBin, op: op, l: lhs, r: rhs, text: lhs.text + " " + string(op) + " " + rhs.text}
	}
	return lhs, true
}

// havingTerm: `unary (('*' | '/') unary)*`; `//` refuses (the engine declines it).
func havingTerm(c *cursor, hc havingCtx, st *havingState, cx *operandCx, first *hNode) (*hNode, bool) {
	lhs := first
	if lhs == nil {
		var ok bool
		if lhs, ok = havingUnary(c, hc, st, cx); !ok {
			return nil, false
		}
	}
	for c.i < len(c.toks) {
		t := c.toks[c.i]
		if t.kind == tokIntDiv {
			return nil, false
		}
		if t.kind != tokPunct || (t.punct != '*' && t.punct != '/') {
			break
		}
		c.i++
		rhs, ok := havingUnary(c, hc, st, cx)
		if !ok || !cx.bump() {
			return nil, false
		}
		lhs = &hNode{kind: hBin, op: t.punct, l: lhs, r: rhs, text: lhs.text + " " + string(t.punct) + " " + rhs.text}
	}
	return lhs, true
}

// havingUnary: `'-' unary | primary`.
func havingUnary(c *cursor, hc havingCtx, st *havingState, cx *operandCx) (*hNode, bool) {
	if c.i < len(c.toks) && c.toks[c.i].kind == tokPunct && c.toks[c.i].punct == '-' {
		c.i++
		if !cx.bump() {
			return nil, false
		}
		inner, ok := havingUnary(c, hc, st, cx)
		if !ok {
			return nil, false
		}
		return &hNode{kind: hNeg, l: inner, text: "- " + inner.text}, true
	}
	return havingPrimary(c, hc, st, cx)
}

// havingPrimary: `literal | reference | '(' sum ')'`. A parenthesis counts toward
// the predicate depth cap (arcX res-3c review M4); NULL inside an operand refuses
// (review L3).
func havingPrimary(c *cursor, hc havingCtx, st *havingState, cx *operandCx) (*hNode, bool) {
	if !cx.bump() || c.i >= len(c.toks) {
		return nil, false
	}
	t := c.toks[c.i]
	switch {
	case t.kind == tokPunct && t.punct == '(':
		st.depth++
		if st.depth > maxWhereDepth {
			return nil, false
		}
		c.i++
		inner, ok := havingSum(c, hc, st, cx)
		if !ok || !c.punct(')') {
			return nil, false
		}
		st.depth--
		return &hNode{kind: hParen, l: inner, text: "(" + inner.text + ")"}, true
	case t.kind == tokNum || t.kind == tokFloat || t.kind == tokSci || t.kind == tokStr:
		c.i++
		return litNode(t), true
	case t.kind == tokIdent && (t.lower == "true" || t.lower == "false"):
		c.i++
		return litNode(t), true
	case t.kind == tokIdent && t.lower == "null":
		return nil, false
	}
	text, info, ok := havingRef(c, hc, st)
	if !ok {
		return nil, false
	}
	return &hNode{kind: hRef, text: text, info: info, tag: hc.tagKey != "" && text == hc.tagKey}, true
}

// arithLiteralOK: may this literal leaf sit in a derived comparison? Integers in
// int64 range except int64's minimum (arcX res-3c review H1: DuckDB's
// MoveConstantsRule answers FALSE for every operator when `i64::MIN / -1` does not
// fit); decimals (the tokenizer already capped them at 15 digits); finite scientific
// literals that are not negative zero (review M1: DuckDB merges −0.0 and +0.0
// constants within a query).
func arithLiteralOK(t token) bool {
	switch t.kind {
	case tokNum:
		v, err := strconv.ParseInt(t.orig, 10, 64)
		return err == nil && v != math.MinInt64
	case tokFloat:
		return true
	case tokSci:
		f, err := strconv.ParseFloat(t.orig, 64)
		return err == nil && !math.IsInf(f, 0) && !(f == 0 && math.Signbit(f))
	}
	return false // strings, TRUE/FALSE: the engine declines them beside arithmetic
}

// isZeroSci: a scientific literal equal to zero, through parentheses (unary minus of
// it is the negative-zero literal the engine folds and declines).
func isZeroSci(n *hNode) bool {
	for n.kind == hParen {
		n = n.l
	}
	if n.kind != hLit || n.tok.kind != tokSci {
		return false
	}
	f, err := strconv.ParseFloat(n.tok.orig, 64)
	return err == nil && f == 0
}

// arithNodeOK checks every node of a derived-comparison operand: references the
// engine can type in arithmetic (an aggregate item other than bool_*, regr_count
// and the time-typed min/max; never the tag key — Arc tags are VARCHAR — nor the
// bucket alias), servable literals, no constant-only binary node (DuckDB folds it
// in INTEGER/DECIMAL), no negative-zero DOUBLE.
func arithNodeOK(n *hNode) bool {
	switch n.kind {
	case hRef:
		return !n.tag && n.info.kind == refAny && !strings.HasPrefix(n.text, "regr_count(")
	case hLit:
		return arithLiteralOK(n.tok)
	case hNeg:
		return !isZeroSci(n.l) && arithNodeOK(n.l)
	case hParen:
		return arithNodeOK(n.l)
	}
	return (n.l.hasRef() || n.r.hasRef()) && arithNodeOK(n.l) && arithNodeOK(n.r)
}

// additiveMix: does a `+`/`-` node combine the subtree holding item `x` with another
// item-bearing subtree?
func additiveMix(n *hNode, x string) bool {
	switch n.kind {
	case hRef, hLit:
		return false
	case hBin:
		here := (n.op == '+' || n.op == '-') &&
			((n.l.contains(x) && n.r.hasRef()) || (n.r.contains(x) && n.l.hasRef()))
		return here || additiveMix(n.l, x) || additiveMix(n.r, x)
	}
	return additiveMix(n.l, x)
}

// derivedOK: the engine's rules for a derived comparison (or `<expr> IS NULL`) over
// these sides. Besides the node checks, arcX res-3c review H3: a float-accumulated
// item (anything not order-exact — the router cannot tell an integer `sum` from a
// float one, so `sum` counts as one) may appear at most once per comparison, may meet
// another item-bearing subtree only through `*` or `/`, and not at all when both
// sides carry items.
func derivedOK(sides ...*hNode) bool {
	anyRef := false
	for _, s := range sides {
		if !arithNodeOK(s) {
			return false
		}
		anyRef = anyRef || s.hasRef()
	}
	if !anyRef {
		return false // literal-only comparison
	}
	inexact := ""
	for _, s := range sides {
		for _, r := range s.refs(nil) {
			if r.info.cmp == cmpNum {
				continue
			}
			if inexact != "" && inexact != r.text {
				return false
			}
			inexact = r.text
		}
	}
	if inexact == "" {
		return true
	}
	if len(sides) == 2 && sides[0].hasRef() && sides[1].hasRef() {
		return false
	}
	for _, s := range sides {
		if additiveMix(s, inexact) {
			return false
		}
	}
	return true
}

// plainLiteral: a literal leaf the res-3 atom path takes (not a scientific one).
func plainLiteral(n *hNode) bool {
	return n.kind == hLit && n.tok.kind != tokSci
}

// havingCompare validates `<l> <op> <r>` and returns its text. A bare reference
// against a plain literal or another bare reference keeps the res-3 / res-3b rules;
// anything else is a derived comparison (derivedOK).
func havingCompare(l *hNode, op string, r *hNode) (string, bool) {
	text := l.text + " " + op + " " + r.text
	switch {
	case l.kind == hRef && r.kind == hRef:
		return text, refsComparable(l.info, r.info)
	case l.kind == hRef && plainLiteral(r):
		_, ok := havingLiteral(r.tok, l.info.kind, op)
		return text, ok
	case r.kind == hRef && plainLiteral(l):
		_, ok := havingLiteral(l.tok, r.info.kind, op)
		return text, ok
	}
	return text, derivedOK(l, r)
}

// havingAtom (res-3b grammar, res-3c operands; mirrors the engine's parse_having_atom):
//
//	<operand> <cmp> <operand>
//	<operand> IS [NOT] NULL | <operand> IS [NOT] DISTINCT FROM <operand|NULL>
//	<bool item> IS [NOT] TRUE|FALSE | <tag key> [NOT] LIKE '<pattern>'
//	<operand> [NOT] BETWEEN <lit> AND <lit> | <operand> [NOT] IN (<lit>, …)
//	<ref>                                    -- bare: a Boolean item
//
// Atom accounting matches the engine's: one per comparison, plus one for a
// BETWEEN's second bound and one per extra IN element.
func havingAtom(c *cursor, b *strings.Builder, hc havingCtx, st *havingState) bool {
	if !st.bumpAtoms(1) {
		return false
	}
	cx := &operandCx{}
	lhs, ok := havingSum(c, hc, st, cx)
	if !ok {
		return false
	}
	if havingAtomEnds(c) {
		if lhs.kind != hRef || lhs.info.kind != refBool {
			return false
		}
		b.WriteString(lhs.text)
		return true
	}
	if c.peekIdentLower() == "is" {
		c.next()
		not := ""
		if c.peekIdentLower() == "not" {
			c.next()
			not = "NOT "
		}
		switch c.peekIdentLower() {
		case "null":
			c.next()
			if lhs.kind != hRef && !derivedOK(lhs) {
				return false
			}
			b.WriteString(lhs.text + " IS " + not + "NULL")
			return true
		case "true", "false":
			// IS [NOT] TRUE|FALSE on a Boolean item only (DuckDB casts anything else).
			v, _ := c.next()
			if lhs.kind != hRef || lhs.info.kind != refBool {
				return false
			}
			b.WriteString(lhs.text + " IS " + not + strings.ToUpper(v.lower))
			return true
		}
		if !c.ident("distinct") || !c.ident("from") {
			return false
		}
		if c.peekIdentLower() == "null" {
			c.next()
			if lhs.kind != hRef && !derivedOK(lhs) {
				return false
			}
			b.WriteString(lhs.text + " IS " + not + "DISTINCT FROM NULL")
			return true
		}
		rhs, ok := havingSum(c, hc, st, cx)
		if !ok {
			return false
		}
		// The engine desugars a bare reference against a plain literal onto the
		// `IS [NOT] NULL` and `=` / `<>` atoms; havingCompare checks it as `=`.
		if _, ok := havingCompare(lhs, "=", rhs); !ok {
			return false
		}
		b.WriteString(lhs.text + " IS " + not + "DISTINCT FROM " + rhs.text)
		return true
	}
	not := ""
	if c.peekIdentLower() == "not" {
		c.next()
		not = "NOT "
	}
	switch c.peekIdentLower() {
	case "between", "in":
		kw, _ := c.next()
		var bounds []*hNode
		if kw.lower == "between" {
			lo, tok := c.next()
			if !tok || !c.ident("and") {
				return false
			}
			hi, tok := c.next()
			if !tok || !st.bumpAtoms(1) {
				return false
			}
			bounds = []*hNode{litNode(lo), litNode(hi)}
		} else {
			if !c.punct('(') {
				return false
			}
			for {
				lt, tok := c.next()
				if !tok {
					return false
				}
				bounds = append(bounds, litNode(lt))
				if len(bounds) > 1 && !st.bumpAtoms(1) {
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
		}
		// Bounds and elements are literal tokens: a NULL / column / TRUE in the list
		// is an identifier and refuses (`NOT IN (…, NULL)` is never TRUE in DuckDB).
		sci := false
		texts := make([]string, len(bounds))
		for i, bd := range bounds {
			t := bd.tok
			if t.kind != tokNum && t.kind != tokFloat && t.kind != tokSci && t.kind != tokStr {
				return false
			}
			sci = sci || t.kind == tokSci
			texts[i] = bd.text
		}
		if lhs.kind == hRef && !sci {
			if lhs.info.kind == refBool {
				return false
			}
			for _, bd := range bounds {
				if _, ok := havingLiteral(bd.tok, lhs.info.kind, ""); !ok {
					return false
				}
			}
		} else {
			// A DOUBLE bound makes the common type DOUBLE (arcX res-3c review H2):
			// a derived comparison even over a bare reference.
			for _, bd := range bounds {
				if !arithLiteralOK(bd.tok) {
					return false
				}
			}
			if !derivedOK(lhs) {
				return false
			}
		}
		if kw.lower == "between" {
			b.WriteString(lhs.text + " " + not + "BETWEEN " + texts[0] + " AND " + texts[1])
		} else {
			b.WriteString(lhs.text + " " + not + "IN (" + strings.Join(texts, ", ") + ")")
		}
		return true
	case "like":
		c.next()
		// `<tag key> [NOT] LIKE '<pattern>'` — the one string output the engine serves
		// (min/max over strings is outside its aggregate matrix, so LIKE on an aggregate
		// would only cost a DuckDB re-run). The WHERE pattern rules: a string literal,
		// no backslash, no ESCAPE.
		pt, tok := c.next()
		if !tok || pt.kind != tokStr || strings.Contains(pt.str, "\\") || c.peekIdentLower() == "escape" {
			return false
		}
		if lhs.kind != hRef || !lhs.tag {
			return false
		}
		b.WriteString(lhs.text + " " + not + "LIKE " + litNode(pt).text)
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
	rhs, ok := havingSum(c, hc, st, cx)
	if !ok {
		return false
	}
	text, ok := havingCompare(lhs, opT.op, rhs)
	if !ok {
		return false
	}
	b.WriteString(text)
	return true
}

// havingParenIsOperand: is the `(` at the cursor an operand's parenthesis rather than
// a predicate group? The token after its matching `)` decides, as in the engine: a
// comparison or arithmetic operator, `//`, a signed number (a binary minus), or IS /
// BETWEEN / IN / LIKE / NOT.
func havingParenIsOperand(c *cursor) bool {
	open := 0
	j := c.i
	for ; j < len(c.toks); j++ {
		t := c.toks[j]
		if t.kind != tokPunct {
			continue
		}
		if t.punct == '(' {
			open++
		} else if t.punct == ')' {
			open--
			if open == 0 {
				break
			}
		}
	}
	if open != 0 || j+1 >= len(c.toks) {
		return false
	}
	t := c.toks[j+1]
	switch t.kind {
	case tokOp, tokIntDiv:
		return true
	case tokPunct:
		return t.punct == '+' || t.punct == '-' || t.punct == '*' || t.punct == '/'
	case tokIdent:
		switch t.lower {
		case "is", "between", "in", "like", "not":
			return true
		}
	case tokNum, tokFloat, tokSci:
		return strings.HasPrefix(t.orig, "-")
	}
	return false
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
			// A timestamp: string literals only, like the bucket alias (review M2).
			info.kind, info.cmp = refTime, cmpTime
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
