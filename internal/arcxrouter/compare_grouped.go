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
// the per-cell policy — tolerance only for float sum/avg).
func compareGrouped(rec arrow.Record, oracle []groupedRow, items []string, keyCols []int) string {
	got, err := groupedFromArcx(rec, keyCols)
	if err != nil {
		return "arcx grouped decode error: " + err.Error()
	}
	if len(got) != len(oracle) {
		return fmt.Sprintf("grouped row count differs (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	// Cell policies in CELL order (items minus the keys, order preserved).
	var tolerant []bool
	for i, item := range items {
		if isKeyCol(keyCols, i) {
			continue
		}
		tolerant = append(tolerant, aggItemTolerant(item))
	}
	normalizeNullParts(got)
	normalizeNullParts(oracle)
	sort.Slice(got, func(i, j int) bool { return groupedLess(&got[i], &got[j]) })
	sort.Slice(oracle, func(i, j int) bool { return groupedLess(&oracle[i], &oracle[j]) })
	for i := range got {
		a, d := &got[i], &oracle[i]
		if !groupedKeyEq(a, d) {
			return fmt.Sprintf("grouped row %d of %d differs (key, values withheld)", i, len(got))
		}
		if len(a.cells) != len(d.cells) || len(a.cells) != len(tolerant) {
			return fmt.Sprintf("grouped row %d differs (agg-column arity)", i)
		}
		for c := range a.cells {
			ac, dc := &a.cells[c], &d.cells[c]
			if ac.isNull != dc.isNull {
				return fmt.Sprintf("grouped row %d differs (agg col %d null-ness)", i, c)
			}
			if ac.isNull {
				continue
			}
			if ac.isInt != dc.isInt {
				return fmt.Sprintf("grouped row %d differs (agg col %d kind)", i, c)
			}
			if ac.isInt {
				if ac.i.Cmp(dc.i) != 0 {
					return fmt.Sprintf("grouped row %d of %d differs (agg col %d, values withheld)", i, len(got), c)
				}
				continue
			}
			if math.IsNaN(ac.f) || math.IsNaN(dc.f) {
				if math.IsNaN(ac.f) && math.IsNaN(dc.f) {
					continue
				}
				return fmt.Sprintf("grouped row %d differs (agg col %d NaN-ness)", i, c)
			}
			if tolerant[c] {
				scale := math.Max(math.Abs(ac.f), math.Abs(dc.f))
				if math.Abs(ac.f-dc.f) > aggFloatRelTol*math.Max(scale, 1.0) {
					return fmt.Sprintf("grouped row %d differs (agg col %d beyond 1e-9 tolerance, values withheld)", i, c)
				}
			} else if ac.f != dc.f {
				return fmt.Sprintf("grouped row %d of %d differs (agg col %d float, values withheld)", i, len(got), c)
			}
		}
	}
	return ""
}
