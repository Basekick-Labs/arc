// Shadow comparison for the allow-listed grouped class (agg-2b/2c → agg-3c
// multi-key): N rows of (key part(s), agg-1 aggregate columns). Both sides sort
// by the TYPED key-part TUPLE (plan F5: never a joined string; parts compare
// element-wise in key-column order, NULL parts ordered first), then compare
// row-wise with the per-item agg policy: keys and counts/min/max exact (float
// min/max ±0.0-equal, two NaNs equal), float sum/avg within the documented
// 1e-9 tolerance, integer sums via big.Int. Timestamp key parts (agg-3 buckets)
// compare as typed epoch-µs on both sides. Diffs stay value-free.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// keyPart is one typed component of a group key tuple: a Utf8 tag value or an
// Int64/epoch-µs bucket. NULL is distinct from ”/0 (isNull); NULL parts are
// normalized to isI=false on BOTH sides before comparison (the DuckDB side
// cannot observe a NULL's column type).
type keyPart struct {
	isNull bool
	isI    bool
	s      string
	i      int64
}

// groupedRow is one comparable row: the key parts in KEY-COLUMN order plus the
// aggregate cells in column order (reusing compare_agg's aggCell + policies).
type groupedRow struct {
	keys  []keyPart
	cells []aggCell
}

func keyPartLess(a, b *keyPart) bool {
	if a.isNull != b.isNull {
		return a.isNull
	}
	if a.isNull {
		return false
	}
	if a.isI {
		return a.i < b.i
	}
	return a.s < b.s
}

func keyPartEq(a, b *keyPart) bool {
	if a.isNull != b.isNull || a.isI != b.isI {
		return false
	}
	if a.isNull {
		return true
	}
	if a.isI {
		return a.i == b.i
	}
	return a.s == b.s
}

func groupedLess(a, b *groupedRow) bool {
	for k := range a.keys {
		if k >= len(b.keys) {
			return false
		}
		if !keyPartEq(&a.keys[k], &b.keys[k]) {
			return keyPartLess(&a.keys[k], &b.keys[k])
		}
	}
	return false
}

func groupedKeyEq(a, b *groupedRow) bool {
	if len(a.keys) != len(b.keys) {
		return false
	}
	for k := range a.keys {
		if !keyPartEq(&a.keys[k], &b.keys[k]) {
			return false
		}
	}
	return true
}

func isKeyCol(keyCols []int, c int) bool {
	for _, k := range keyCols {
		if k == c {
			return true
		}
	}
	return false
}

func groupedFromArcx(rec arrow.Record, keyCols []int) ([]groupedRow, error) {
	ncols := int(rec.NumCols())
	for _, k := range keyCols {
		if k >= ncols {
			return nil, fmt.Errorf("arcx grouped: key col %d out of %d", k, ncols)
		}
	}
	out := make([]groupedRow, rec.NumRows())
	for r := range out {
		row := &out[r]
		for c := 0; c < ncols; c++ {
			if isKeyCol(keyCols, c) {
				var p keyPart
				switch col := rec.Column(c).(type) {
				case *array.String:
					if col.IsNull(r) {
						p.isNull = true
					} else {
						p.s = col.Value(r)
					}
				case *array.Int64:
					p.isI = true
					if col.IsNull(r) {
						p.isNull = true
					} else {
						p.i = col.Value(r)
					}
				// agg-3 bucket key: compare as epoch µs (typed, never a string).
				case *array.Timestamp:
					p.isI = true
					if col.IsNull(r) {
						p.isNull = true
					} else {
						unit := col.DataType().(*arrow.TimestampType).Unit
						p.i = toMicros(int64(col.Value(r)), unit)
					}
				default:
					return nil, fmt.Errorf("arcx grouped key: unexpected type %T", rec.Column(c))
				}
				row.keys = append(row.keys, p)
				continue
			}
			switch col := rec.Column(c).(type) {
			case *array.Int64:
				if col.IsNull(r) {
					row.cells = append(row.cells, aggCell{isNull: true})
				} else {
					row.cells = append(row.cells, aggCell{isInt: true, i: big.NewInt(col.Value(r))})
				}
			case *array.Float64:
				if col.IsNull(r) {
					row.cells = append(row.cells, aggCell{isNull: true})
				} else {
					row.cells = append(row.cells, aggCell{f: col.Value(r)})
				}
			case *array.Uint32: // agg-6b regr_count
				if col.IsNull(r) {
					row.cells = append(row.cells, aggCell{isNull: true})
				} else {
					row.cells = append(row.cells, aggCell{isInt: true, i: big.NewInt(int64(col.Value(r)))})
				}
			case *array.Boolean: // agg-6b' bool_and / bool_or
				if col.IsNull(r) {
					row.cells = append(row.cells, aggCell{isNull: true})
				} else {
					row.cells = append(row.cells, aggCell{isBool: true, b: col.Value(r)})
				}
			case *array.String: // agg-7 mode over VARCHAR
				if col.IsNull(r) {
					row.cells = append(row.cells, aggCell{isNull: true})
				} else {
					row.cells = append(row.cells, aggCell{isStr: true, s: col.Value(r)})
				}
			case *array.Timestamp:
				if col.IsNull(r) {
					row.cells = append(row.cells, aggCell{isNull: true})
				} else {
					unit := col.DataType().(*arrow.TimestampType).Unit
					row.cells = append(row.cells, aggCell{
						isInt: true,
						i:     big.NewInt(toMicros(int64(col.Value(r)), unit)),
					})
				}
			default:
				return nil, fmt.Errorf("arcx grouped agg col %d: unexpected type %T", c, rec.Column(c))
			}
		}
	}
	return out, nil
}

func groupedFromRows(rows *sql.Rows, keyCols []int) ([]groupedRow, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []groupedRow
	for rows.Next() {
		raw := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		var row groupedRow
		for c, v := range raw {
			if isKeyCol(keyCols, c) {
				var p keyPart
				switch x := v.(type) {
				case nil:
					p.isNull = true
				case string:
					p.s = x
				case []byte:
					p.s = string(x)
				case int64:
					p.isI = true
					p.i = x
				case time.Time:
					p.isI = true
					p.i = x.UnixMicro()
				default:
					return nil, fmt.Errorf("duckdb grouped key: unexpected type %T", v)
				}
				row.keys = append(row.keys, p)
				continue
			}
			switch x := v.(type) {
			case nil:
				row.cells = append(row.cells, aggCell{isNull: true})
			case int64:
				row.cells = append(row.cells, aggCell{isInt: true, i: big.NewInt(x)})
			case uint32: // agg-6b regr_count (UINTEGER)
				row.cells = append(row.cells, aggCell{isInt: true, i: big.NewInt(int64(x))})
			case bool: // agg-6b' bool_and / bool_or
				row.cells = append(row.cells, aggCell{isBool: true, b: x})
			case string: // agg-7 mode over VARCHAR
				row.cells = append(row.cells, aggCell{isStr: true, s: x})
			case *big.Int:
				row.cells = append(row.cells, aggCell{isInt: true, i: x})
			case float64:
				row.cells = append(row.cells, aggCell{f: x})
			case time.Time:
				row.cells = append(row.cells, aggCell{isInt: true, i: big.NewInt(x.UnixMicro())})
			default:
				return nil, fmt.Errorf("duckdb grouped agg col %d: unexpected type %T", c, v)
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// normalizeNullParts forces every NULL key part's isI to false on BOTH sides so
// keyPartEq/Less never see a type mismatch on NULLs (the DuckDB side cannot
// observe a NULL's column type; the arcx side can — align them).
func normalizeNullParts(rows []groupedRow) {
	for i := range rows {
		for k := range rows[i].keys {
			if rows[i].keys[k].isNull {
				rows[i].keys[k].isI = false
			}
		}
	}
}

// compareGrouped returns "" on match, else a short VALUE-FREE diff. `items` is
// the select list in column order (keyCols mark the key columns; the rest pick
// the per-cell policy — tolerance only for float sum/avg). oneSided is
// Decision.HavingUnstable (res-3): "" compares row sets strictly.
func compareGrouped(rec arrow.Record, oracle []groupedRow, items []string, keyCols []int, oneSided string) string {
	got, err := groupedFromArcx(rec, keyCols)
	if err != nil {
		return skipPrefix + "arcx grouped decode error: " + err.Error()
	}
	return compareGroupedRows(got, oracle, items, keyCols, oneSided)
}

// compareGroupedRows is compareGrouped after decoding. With oneSided set (a HAVING
// over an unstable item, res-3 review H3), the two sides are MERGED on the group
// key instead of required to be the same length: a group present on one side only
// is the legitimate effect of DuckDB's value moving across the HAVING literal and is
// recorded as a deferred argdiff/oraclediff; a group on both sides compares with the
// normal cell rules (every non-unstable cell strict). A duplicate group key on
// either side still alarms — a merge would otherwise file it as one-sided.
func compareGroupedRows(got, oracle []groupedRow, items []string, keyCols []int, oneSided string) string {
	if oneSided == "" && len(got) != len(oracle) {
		return fmt.Sprintf("grouped row count differs (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	gc := newGroupedCmp(items, keyCols, max(len(got), len(oracle)))
	normalizeNullParts(got)
	normalizeNullParts(oracle)
	sort.Slice(got, func(i, j int) bool { return groupedLess(&got[i], &got[j]) })
	sort.Slice(oracle, func(i, j int) bool { return groupedLess(&oracle[i], &oracle[j]) })
	if oneSided != "" {
		return gc.mergeOneSided(got, oracle, oneSided)
	}
	for i := range got {
		a, d := &got[i], &oracle[i]
		if !groupedKeyEq(a, d) {
			return fmt.Sprintf("grouped row %d of %d differs (key, values withheld)", i, len(got))
		}
		if msg := gc.rowCells(i, a, d); msg != "" {
			return msg
		}
	}
	return gc.deferred()
}

// mergeOneSided walks two key-sorted row sets in step (see compareGroupedRows).
func (gc *groupedCmp) mergeOneSided(got, oracle []groupedRow, oneSided string) string {
	for _, side := range [][]groupedRow{got, oracle} {
		for i := 1; i < len(side); i++ {
			if groupedKeyEq(&side[i-1], &side[i]) {
				return fmt.Sprintf("grouped rows %d..%d share a group key (values withheld)", i-1, i)
			}
		}
	}
	i, j, oneSidedRows, matched := 0, 0, 0, 0
	for i < len(got) || j < len(oracle) {
		switch {
		case j >= len(oracle) || (i < len(got) && groupedLess(&got[i], &oracle[j])):
			oneSidedRows++
			i++
		case i >= len(got) || groupedLess(&oracle[j], &got[i]):
			oneSidedRows++
			j++
		default:
			if !groupedKeyEq(&got[i], &oracle[j]) {
				return fmt.Sprintf("grouped row %d differs (key, values withheld)", i)
			}
			if msg := gc.rowCells(i, &got[i], &oracle[j]); msg != "" {
				return msg
			}
			matched++
			i++
			j++
		}
	}
	// Post-impl review M1: an unstable value near the literal moves SOME groups; a
	// result with no group in common (one side empty, or disjoint keys — a shifted
	// bucket, a wrong key, a filter that dropped everything) is a whole-result
	// defect, not oracle noise, and alarms.
	if oneSidedRows > 0 && matched == 0 {
		return fmt.Sprintf("no group in common under a HAVING over an unstable aggregate (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	if oneSidedRows > 0 {
		msg := fmt.Sprintf("%d group(s) on one side only under a HAVING over an unstable aggregate (arcx=%d duckdb=%d, values withheld)",
			oneSidedRows, len(got), len(oracle))
		if oneSided == "arg" {
			if gc.argDiff == "" {
				gc.argDiff = msg
			}
		} else if gc.oracleDiff == "" {
			gc.oracleDiff = msg
		}
	}
	return gc.deferred()
}

// groupedCmp carries the per-cell policies of one grouped comparison and the
// deferred (WARN-class) diffs it has seen. `rowCells` is the single source of the
// cell rules, shared by the unordered and the ordered (res-2) comparisons.
type groupedCmp struct {
	tolerant   []bool
	cellItems  []string
	total      int
	argDiff    string
	oracleDiff string
}

func newGroupedCmp(items []string, keyCols []int, total int) *groupedCmp {
	gc := &groupedCmp{total: total}
	// Cell policies in CELL order (items minus the keys, order preserved).
	// cellItems carries the item text per cell for the agg-4 arg-tie deferral.
	for i, item := range items {
		if isKeyCol(keyCols, i) {
			continue
		}
		gc.tolerant = append(gc.tolerant, aggItemTolerant(item))
		gc.cellItems = append(gc.cellItems, item)
	}
	return gc
}

// deferArg: agg-4 tie policy — VALUE (and NaN-ness) diffs on arg cells are
// deferred, surfaced tagged "argdiff:" only when nothing else differs. Structural
// diffs (row count, keys, null-ness, kind) always alarm — with ONE exception
// (res-3): under Decision.HavingUnstable a group present on one side only is
// deferred by mergeOneSided, because the HAVING itself filters on the unstable
// value. Null-ness, kind and duplicate keys still alarm there.
func (gc *groupedCmp) deferArg(c int, msg string) bool {
	if c < len(gc.cellItems) && isArgItem(gc.cellItems[c]) {
		if gc.argDiff == "" {
			gc.argDiff = msg
		}
		return true
	}
	return false
}

// deferred returns the WARN-class diff, if any ("" = full match).
func (gc *groupedCmp) deferred() string {
	if gc.argDiff != "" {
		return "argdiff:" + gc.argDiff
	}
	if gc.oracleDiff != "" {
		return "oraclediff:" + gc.oracleDiff
	}
	return ""
}

// rowCells compares the aggregate cells of two rows whose keys already match;
// returns an alarm message, or "" (deferred diffs are recorded on gc).
func (gc *groupedCmp) rowCells(i int, a, d *groupedRow) string {
	tolerant, cellItems := gc.tolerant, gc.cellItems
	if len(a.cells) != len(d.cells) || len(a.cells) != len(tolerant) {
		return fmt.Sprintf("grouped row %d differs (agg-column arity)", i)
	}
	for c := range a.cells {
		ac, dc := &a.cells[c], &d.cells[c]
		// agg-8: any shape-statistic difference is an oracle defect (WARN).
		if c < len(cellItems) && isShapeItem(cellItems[c]) {
			if shapeKindDiffers(*ac, *dc) {
				return fmt.Sprintf("grouped row %d differs (agg col %d kind on a shape statistic)", i, c)
			}
			if shapeCellDiffers(*ac, *dc) && gc.oracleDiff == "" {
				gc.oracleDiff = fmt.Sprintf("grouped row %d of %d differs (agg col %d shape statistic vs DuckDB 1.5.6, values withheld)", i, gc.total, c)
			}
			continue
		}
		if ac.isNull != dc.isNull {
			return fmt.Sprintf("grouped row %d differs (agg col %d null-ness)", i, c)
		}
		if ac.isNull {
			continue
		}
		if ac.isBool != dc.isBool {
			return fmt.Sprintf("grouped row %d differs (agg col %d kind)", i, c)
		}
		if ac.isBool {
			if ac.b != dc.b {
				return fmt.Sprintf("grouped row %d of %d differs (agg col %d boolean, values withheld)", i, gc.total, c)
			}
			continue
		}
		if ac.isStr != dc.isStr {
			return fmt.Sprintf("grouped row %d differs (agg col %d kind)", i, c)
		}
		if ac.isStr {
			if ac.s != dc.s {
				return fmt.Sprintf("grouped row %d of %d differs (agg col %d string, values withheld)", i, gc.total, c)
			}
			continue
		}
		if ac.isInt != dc.isInt {
			return fmt.Sprintf("grouped row %d differs (agg col %d kind)", i, c)
		}
		if ac.isInt {
			if ac.i.Cmp(dc.i) != 0 {
				msg := fmt.Sprintf("grouped row %d of %d differs (agg col %d, values withheld)", i, gc.total, c)
				if gc.deferArg(c, msg) {
					continue
				}
				return msg
			}
			continue
		}
		if math.IsNaN(ac.f) || math.IsNaN(dc.f) {
			if math.IsNaN(ac.f) && math.IsNaN(dc.f) {
				continue
			}
			msg := fmt.Sprintf("grouped row %d differs (agg col %d NaN-ness)", i, c)
			if math.IsNaN(ac.f) && math.IsInf(dc.f, 0) && c < len(cellItems) && isPinnedNaNItem(cellItems[c]) {
				if gc.oracleDiff == "" {
					gc.oracleDiff = msg
				}
				continue
			}
			if gc.deferArg(c, msg) {
				continue
			}
			return msg
		}
		// ±Inf compares exactly — the tolerance test cannot see it (see compareAgg).
		if math.IsInf(ac.f, 0) || math.IsInf(dc.f, 0) {
			if ac.f == dc.f {
				continue
			}
			msg := fmt.Sprintf("grouped row %d differs (agg col %d Inf-ness)", i, c)
			if gc.deferArg(c, msg) {
				continue
			}
			return msg
		}
		if tolerant[c] {
			scale := math.Max(math.Abs(ac.f), math.Abs(dc.f))
			if math.Abs(ac.f-dc.f) > aggFloatRelTol*math.Max(scale, 1.0) {
				msg := fmt.Sprintf("grouped row %d differs (agg col %d beyond 1e-9 tolerance, values withheld)", i, c)
				if c < len(cellItems) && isOracleUnstableItem(cellItems[c]) {
					if gc.oracleDiff == "" {
						gc.oracleDiff = msg
					}
					continue
				}
				return msg
			}
		} else if ac.f != dc.f {
			msg := fmt.Sprintf("grouped row %d of %d differs (agg col %d float, values withheld)", i, gc.total, c)
			if gc.deferArg(c, msg) {
				continue
			}
			return msg
		}
	}
	return ""
}
