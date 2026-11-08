// res-2 shadow comparison for a grouped query with ORDER BY [LIMIT]: the row
// ORDER is part of the answer, so neither side is re-sorted. The rows are cut
// into joint tie classes; within a class rows are matched by group key and
// compared with the shared cell rules (groupedCmp.rowCells). Diffs stay
// value-free.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"fmt"
	"math"
	"math/big"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
)

// ordVal is one ORDER BY value of one row, read from a key part or a cell.
type ordVal struct {
	null bool
	kind byte // 'i' integer, 'f' float, 's' string, 'b' boolean
	i    *big.Int
	f    float64
	s    string
	b    bool
}

// ordCol locates an ORDER BY column in a groupedRow: a key part or a cell.
type ordCol struct {
	key       bool
	idx       int
	desc      bool
	nullFirst bool
	tolerant  bool
}

func (oc ordCol) val(r *groupedRow) ordVal {
	if oc.key {
		p := r.keys[oc.idx]
		switch {
		case p.isNull:
			return ordVal{null: true}
		case p.isI:
			return ordVal{kind: 'i', i: big.NewInt(p.i)}
		default:
			return ordVal{kind: 's', s: p.s}
		}
	}
	c := r.cells[oc.idx]
	switch {
	case c.isNull:
		return ordVal{null: true}
	case c.isInt:
		return ordVal{kind: 'i', i: c.i}
	case c.isBool:
		return ordVal{kind: 'b', b: c.b}
	case c.isStr:
		return ordVal{kind: 's', s: c.s}
	default:
		return ordVal{kind: 'f', f: c.f}
	}
}

// ordCmp orders two values of ONE side the way DuckDB does: NULL placement per
// the key (independent of direction), NaN above every number, -0 == +0, then
// the direction. ok=false on a kind mismatch (never ordered; the caller alarms).
func (oc ordCol) ordCmp(a, b ordVal) (int, bool) {
	if a.null || b.null {
		switch {
		case a.null && b.null:
			return 0, true
		case a.null == oc.nullFirst:
			return -1, true
		default:
			return 1, true
		}
	}
	if a.kind != b.kind {
		return 0, false
	}
	var c int
	switch a.kind {
	case 'i':
		c = a.i.Cmp(b.i)
	case 'f':
		an, bn := math.IsNaN(a.f), math.IsNaN(b.f)
		switch {
		case an && bn:
		case an:
			c = 1
		case bn:
			c = -1
		case a.f < b.f:
			c = -1
		case a.f > b.f:
			c = 1
		}
	case 's':
		switch {
		case a.s < b.s:
			c = -1
		case a.s > b.s:
			c = 1
		}
	case 'b':
		switch {
		case a.b == b.b:
		case !a.b:
			c = -1
		default:
			c = 1
		}
	}
	if oc.desc {
		c = -c
	}
	return c, true
}

// clearlyDiffers: the two values cannot be a tie under any legitimate
// difference between the engines — exact for every non-tolerant column, beyond
// twice the documented tolerance (each side may sit 1e-9 off the true value)
// for the one tolerant float key.
func (oc ordCol) clearlyDiffers(a, b ordVal) bool {
	c, ok := oc.ordCmp(a, b)
	if !ok {
		return true
	}
	if c == 0 {
		return false
	}
	if !oc.tolerant || a.null || b.null || a.kind != 'f' {
		return true
	}
	if math.IsNaN(a.f) || math.IsNaN(b.f) || math.IsInf(a.f, 0) || math.IsInf(b.f, 0) {
		return true
	}
	scale := math.Max(math.Abs(a.f), math.Abs(b.f))
	return math.Abs(a.f-b.f) > 2*aggFloatRelTol*math.Max(scale, 1.0)
}

func tupleCmp(cols []ordCol, a, b *groupedRow) (int, bool) {
	for _, oc := range cols {
		c, ok := oc.ordCmp(oc.val(a), oc.val(b))
		if !ok {
			return 0, false
		}
		if c != 0 {
			return c, true
		}
	}
	return 0, true
}

func tupleClearlyDiffers(cols []ordCol, a, b *groupedRow) bool {
	for _, oc := range cols {
		if oc.clearlyDiffers(oc.val(a), oc.val(b)) {
			return true
		}
	}
	return false
}

// ordCols maps the ORDER BY keys (1-based select positions) to row locations.
func ordCols(items []string, keyCols []int, keys []OrderKey) ([]ordCol, bool) {
	out := make([]ordCol, 0, len(keys))
	for _, k := range keys {
		p := k.Pos - 1
		if p < 0 || p >= len(items) {
			return nil, false
		}
		oc := ordCol{desc: k.Desc, nullFirst: k.NullsFirst}
		for i := 0; i < p; i++ {
			if isKeyCol(keyCols, i) == isKeyCol(keyCols, p) {
				oc.idx++
			}
		}
		oc.key = isKeyCol(keyCols, p)
		oc.tolerant = !oc.key && aggItemTolerant(items[p])
		out = append(out, oc)
	}
	return out, true
}

// compareGroupedOrdered: both result sets in the order each engine returned.
//
//  1. Each side must be monotone under the ORDER BY (DuckDB semantics, exact on
//     its own values) — an arcx row out of order is a mismatch.
//  2. Row i starts a new joint tie class only where BOTH sides' rows i-1 and i
//     clearly differ; tied rows may legitimately come in different orders.
//  3. Within a class the rows are matched by group key and their cells compared
//     with the shared rules.
//  4. With LIMIT, when the result is exactly the limit long, the LAST class may
//     straddle the cut: a row present on one side only, tied with that side's
//     last row, is a `tiediff:` CANDIDATE — DuckDB picks among tied groups
//     arbitrarily, arcx by group key — provided both sides' last rows are tied
//     with each other. The caller (verifyTopNTie) confirms it against a
//     deterministic oracle before filing it. Anywhere else a one-sided row
//     alarms.
func compareGroupedOrdered(rec arrow.Record, oracle []groupedRow, items []string, keyCols []int,
	keys []OrderKey, hasLimit bool, limit int) string {
	return compareGroupedOrderedMode(rec, oracle, items, keyCols, keys, hasLimit, limit, false)
}

// compareGroupedOrderedMode: strict is the verification pass against an oracle
// that applied the engine's own tie-break — there an exact tie is ordered the
// same on both sides, so a one-sided row is tolerated only when the last ORDER
// key is a tolerant float (a near-tie the engines may round apart).
func compareGroupedOrderedMode(rec arrow.Record, oracle []groupedRow, items []string, keyCols []int,
	keys []OrderKey, hasLimit bool, limit int, strict bool) string {
	got, err := groupedFromArcx(rec, keyCols)
	if err != nil {
		return skipPrefix + "arcx grouped decode error: " + err.Error()
	}
	if len(got) != len(oracle) {
		return fmt.Sprintf("grouped row count differs (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	// Shape check before any index into keys/cells (review H2): a wrong column
	// count must alarm, never panic in the ordering loop.
	nCells := len(items) - len(keyCols)
	for i := range got {
		if len(got[i].keys) != len(keyCols) || len(got[i].cells) != nCells {
			return fmt.Sprintf("grouped row %d differs (arcx column arity)", i)
		}
		if len(oracle[i].keys) != len(keyCols) || len(oracle[i].cells) != nCells {
			return skipPrefix + "duckdb grouped result has an unexpected column arity"
		}
	}
	cols, ok := ordCols(items, keyCols, keys)
	if !ok {
		return skipPrefix + "ORDER BY position out of range"
	}
	normalizeNullParts(got)
	normalizeNullParts(oracle)
	n := len(got)
	for i := 1; i < n; i++ {
		if c, ok := tupleCmp(cols, &got[i-1], &got[i]); !ok || c > 0 {
			return fmt.Sprintf("grouped rows %d..%d of %d not in ORDER BY order on arcx (values withheld)", i-1, i, n)
		}
		// DuckDB out of the comparator's own model of DuckDB's order (a setting,
		// a collation): says nothing about arcx — skip, never the alarm (M4).
		if c, ok := tupleCmp(cols, &oracle[i-1], &oracle[i]); !ok || c > 0 {
			return skipPrefix + "duckdb rows not in the modelled ORDER BY order"
		}
	}
	gc := newGroupedCmp(items, keyCols, n)
	// The cut can only fall inside a tie when both sides' last rows are tied.
	truncated := hasLimit && n == limit && n > 0 && !tupleClearlyDiffers(cols, &got[n-1], &oracle[n-1])
	tieDiff := ""
	ai := make([]int, 0, n)
	di := make([]int, 0, n)
	for start := 0; start < n; {
		end := start + 1
		for end < n && !(tupleClearlyDiffers(cols, &got[end-1], &got[end]) &&
			tupleClearlyDiffers(cols, &oracle[end-1], &oracle[end])) {
			end++
		}
		cut := end == n && truncated
		// Match the class by group key: sort both index sets by key and merge —
		// O(k log k) per class (review H3; a bucket-ordered composite result is
		// often ONE class of every row).
		ai, di = ai[:0], di[:0]
		for i := start; i < end; i++ {
			ai = append(ai, i)
			di = append(di, i)
		}
		sort.Slice(ai, func(x, y int) bool { return groupedLess(&got[ai[x]], &got[ai[y]]) })
		sort.Slice(di, func(x, y int) bool { return groupedLess(&oracle[di[x]], &oracle[di[y]]) })
		// oneSided: a row on one side only. Inside the cut's tie class and tied
		// with its own side's last row it is a tiediff candidate (verified by the
		// caller against a deterministic oracle); anywhere else it alarms.
		oneSided := func(r *groupedRow, side []groupedRow, idx int) string {
			if cut && !tupleClearlyDiffers(cols, r, &side[n-1]) && (!strict || cols[len(cols)-1].tolerant) {
				if tieDiff == "" {
					tieDiff = fmt.Sprintf("grouped row %d of %d: a different group tied at the LIMIT cut (values withheld)", idx, n)
				}
				return ""
			}
			return fmt.Sprintf("grouped row %d of %d differs (key, values withheld)", idx, n)
		}
		x, y := 0, 0
		for x < len(ai) || y < len(di) {
			switch {
			case y == len(di) || (x < len(ai) && groupedLess(&got[ai[x]], &oracle[di[y]])):
				if msg := oneSided(&got[ai[x]], got, ai[x]); msg != "" {
					return msg
				}
				x++
			case x == len(ai) || groupedLess(&oracle[di[y]], &got[ai[x]]):
				if msg := oneSided(&oracle[di[y]], oracle, di[y]); msg != "" {
					return msg
				}
				y++
			default:
				if msg := gc.rowCells(ai[x], &got[ai[x]], &oracle[di[y]]); msg != "" {
					return msg
				}
				x++
				y++
			}
		}
		start = end
	}
	// A tie candidate goes first, ahead of the WARN-class cell diffs: its
	// one-sided rows were never checked, and only the caller's verification pass
	// (which re-runs the cell rules against a deterministic oracle) can.
	if tieDiff != "" {
		return "tiediff:" + tieDiff
	}
	return gc.deferred()
}
