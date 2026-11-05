package api

import (
	"context"
	"errors"
	"regexp"
	"strings"
)

// Deferred table-reference expansion (#1482, #1483).
//
// The storage-path rewrite runs several passes over the SQL: the comma-join
// pass, then the FROM and JOIN patterns. Each used to splice in its finished
// read_parquet() expression, about 30 times the length of a short reference,
// so every later pass scanned the expanded text: tens of MB from a 1 MiB
// query. Each pass now writes refSentinel in place of the expression and
// keeps the expression in a list, in the order the sentinels appear. One pass
// at the end puts the expressions in. Every pass therefore scans text about
// as long as the query.
//
// A later pass sees refSentinel where it used to see the first byte of an
// emitted expression: ',', 'F' of FROM, or a join keyword. The lookahead
// checks (isDotOrCallAt, keepsOwnName) read both as "not an identifier, not a
// '.' or '('", no table pattern matches either, and a regexp \b holds at an
// identifier/NUL edge as at an identifier/',' edge.
//
// Deferring also lets the alias decision see the whole query: a reference is
// named after its table when another rewritten reference shares its FROM
// scope (#1483), which is not known until every pass has run.

// refSentinel stands in for a rewritten table reference. A NUL cannot come
// from the query: string literals are masked before the passes run, and
// deferredRefs.refuseNUL refuses one anywhere else.
const refSentinel = '\x00'

// errSQLContainsNUL refuses a query with a NUL byte outside a string literal.
// DuckDB reads the statement as a C string and would stop at it, so the query
// would run truncated; here it would also be read as a rewritten reference.
var errSQLContainsNUL = errors.New("SQL contains a NUL byte outside a string literal")

// cancelCheckInterval is how many references a pass rewrites between checks
// of the request context.
const cancelCheckInterval = 1024

type deferredRef struct {
	expr string
	// name is the table's name, given as an alias at expansion when another
	// rewritten reference shares this one's scope. Empty when the reference
	// must never be given one (see deferredRefs.candidate).
	name string
	// aliased is true when the expression already carries its alias: the
	// query qualifies columns by the table's name (#1447, aliasTableRef).
	aliased bool
}

type deferredRefs struct {
	ctx   context.Context
	refs  []deferredRef
	calls int
	// done is set once the request context has ended; nothing is rewritten
	// after it.
	done bool
	// cteNames and otherAliases are names a reference must not be given: a
	// CTE's, and those the query gives another relation (#1450). Either
	// would be a second relation of that name in the scope.
	cteNames     map[string]bool
	otherAliases map[string]bool
	qualified    map[string]bool
}

func newDeferredRefs(ctx context.Context, cteNames, qualified, otherAliases map[string]bool) *deferredRefs {
	return &deferredRefs{ctx: ctx, cteNames: cteNames, qualified: qualified, otherAliases: otherAliases}
}

// refuseNUL reports whether sql, masked and comment-stripped, holds a NUL,
// recording errSQLContainsNUL on the request when it does.
func (d *deferredRefs) refuseNUL(sql string) bool {
	if strings.IndexByte(sql, refSentinel) < 0 {
		return false
	}
	recordStoragePathFailure(d.ctx, errSQLContainsNUL)
	return true
}

// stopped reports whether the request is done. It reads the context once
// every cancelCheckInterval references and records its error, so the caller
// refuses the partly rewritten SQL rather than run or cache it: a reference
// left unrewritten may keep a quoted, path-shaped name, which DuckDB would
// read as a replacement scan.
func (d *deferredRefs) stopped() bool {
	if d.done {
		return true
	}
	d.calls++
	if d.calls%cancelCheckInterval != 0 {
		return false
	}
	return d.cancelled()
}

// cancelled reads the request context now and records its error.
func (d *deferredRefs) cancelled() bool {
	if d.done {
		return true
	}
	err := d.ctx.Err()
	if err == nil {
		return false
	}
	d.done = true
	recordStoragePathFailure(d.ctx, err)
	return true
}

// ref builds the deferred form of expr, the rewritten reference to table
// name ending at end in sql. ok is false for a name the identifier resolver
// rejected. The #1447 alias is applied now, as aliasTableRef always did; the
// #1483 one waits for expansion.
func (d *deferredRefs) ref(expr, sql string, end int, name string, ok bool) deferredRef {
	if aliased := aliasTableRef(expr, sql, end, name, ok, d.qualified); aliased != expr {
		return deferredRef{expr: aliased, aliased: true}
	}
	r := deferredRef{expr: expr}
	if d.candidate(expr, sql, end, name, ok) {
		r.name = name
	}
	return r
}

// candidate reports whether the reference may be named after its table at
// expansion: the same conditions as aliasTableRef apart from the qualifier,
// and the name is no CTE's or other relation's.
func (d *deferredRefs) candidate(expr, sql string, end int, name string, ok bool) bool {
	if expr == "" || !ok || containsMaskPlaceholder(name) || keepsOwnName(sql, end) {
		return false
	}
	lower := strings.ToLower(name)
	return !d.cteNames[lower] && !d.otherAliases[lower]
}

// replaceComma runs the comma-join pass. It must be the first pass: the
// references it returns are appended as they come, which is in sentinel order
// only while sql holds no sentinel yet.
func (d *deferredRefs) replaceComma(sql string, fn func(ref commaJoinRef) (deferredRef, bool)) string {
	if len(d.refs) != 0 {
		panic("deferredRefs: the comma pass must run first")
	}
	return rewriteCommaJoinRefs(sql, func(ref commaJoinRef) (string, bool) {
		if d.stopped() {
			return "", false
		}
		r, ok := fn(ref)
		if !ok {
			return "", false
		}
		d.refs = append(d.refs, r)
		return string(refSentinel), true
	})
}

// replace runs one table-pattern pass. fn returns the deferred reference for
// a match, or false to leave the match as it is. The list stays in sentinel
// order: before each new reference go the existing ones whose sentinels the
// copied text holds.
//
// fn receives the submatch strings and the byte offset just past the match in
// the string being scanned. The offset must come from that string, the
// CURRENT one: the "is this identifier followed by '.' or '('?" lookahead
// once recovered it with strings.Index on a lower-cased copy taken before the
// earlier passes ran, and the stale offset landed inside an emitted
// read_parquet('…'), whose '(' tripped the function-call guard and left a
// real table unrewritten (`FROM a JOIN cross`). parts for groups that did not
// participate are empty strings.
func (d *deferredRefs) replace(sql string, re *regexp.Regexp, fn func(parts []string, end int) (deferredRef, bool)) string {
	matches := re.FindAllStringSubmatchIndex(sql, -1)
	if len(matches) == 0 {
		return sql
	}
	merged := make([]deferredRef, 0, len(d.refs)+len(matches))
	old := 0
	carry := func(segment string) {
		n := strings.Count(segment, string(refSentinel))
		merged = append(merged, d.refs[old:old+n]...)
		old += n
	}

	var b strings.Builder
	b.Grow(len(sql))
	last := 0
	for _, m := range matches {
		matched := sql[m[0]:m[1]]
		// No table pattern matches a NUL; a match holding one is left alone
		// so the sentinel inside it keeps its place.
		if strings.IndexByte(matched, refSentinel) >= 0 || d.stopped() {
			continue
		}
		parts := make([]string, len(m)/2)
		for i := range parts {
			if m[2*i] >= 0 {
				parts[i] = sql[m[2*i]:m[2*i+1]]
			}
		}
		r, ok := fn(parts, m[1])
		if !ok {
			continue
		}
		carry(sql[last:m[0]])
		merged = append(merged, r)
		b.WriteString(sql[last:m[0]])
		b.WriteByte(refSentinel)
		last = m[1]
	}
	carry(sql[last:])
	b.WriteString(sql[last:])
	d.refs = merged
	return b.String()
}

// expand puts each reference's expression in place of its sentinel. A
// reference that is a naming candidate gets ` AS "<name>"` when its scope
// holds another rewritten reference (#1483): DuckDB names every unaliased
// read_parquet() relation "read_parquet", so two of them in one FROM leave
// every unqualified column and * ambiguous.
func (d *deferredRefs) expand(sql string) string {
	if len(d.refs) == 0 {
		return sql
	}
	scopes := sentinelScopes(sql, len(d.refs))
	perScope := make(map[int]int, len(d.refs))
	for _, s := range scopes {
		perScope[s]++
	}

	size := len(sql)
	for _, r := range d.refs {
		size += len(r.expr) + len(r.name) + 6
	}
	var b strings.Builder
	b.Grow(size)
	i, last := 0, 0
	for pos := 0; pos < len(sql); pos++ {
		if sql[pos] != refSentinel {
			continue
		}
		r := d.refs[i]
		b.WriteString(sql[last:pos])
		b.WriteString(r.expr)
		if !r.aliased && r.name != "" && perScope[scopes[i]] > 1 {
			b.WriteString(` AS "`)
			b.WriteString(strings.ReplaceAll(r.name, `"`, `""`))
			b.WriteByte('"')
		}
		i++
		last = pos + 1
	}
	b.WriteString(sql[last:])
	return b.String()
}

// sentinelScopes returns the scope of each of the n sentinels in sql, in
// order. A scope is the part of a statement one SELECT reads from: it starts
// at an opening parenthesis and at each SELECT at the current depth, so a
// subquery, and each arm of a UNION, is a scope of its own. sql is masked and
// comment-stripped, so every parenthesis and word in it is SQL.
func sentinelScopes(sql string, n int) []int {
	scopes := make([]int, 0, n)
	next := 0
	stack := []int{next}
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == refSentinel:
			scopes = append(scopes, stack[len(stack)-1])
			i++
		case c == '(':
			next++
			stack = append(stack, next)
			i++
		case c == ')':
			if len(stack) > 1 {
				stack = stack[:len(stack)-1]
			}
			i++
		case isIdentChar(c):
			start := i
			for i < len(sql) && isIdentChar(sql[i]) {
				i++
			}
			if i-start == 6 && strings.EqualFold(sql[start:i], "select") {
				next++
				stack[len(stack)-1] = next
			}
		default:
			i++
		}
	}
	return scopes
}
