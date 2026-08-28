// Shadow comparison for the allow-listed grouped class (agg-2b/2c): N rows of
// (single group key, agg-1 aggregate columns). Both sides sort by the TYPED key
// part (plan F5: never a joined string; a single key makes this one part — Utf8
// or Int64, NULL ordered first), then compare row-wise with the per-item agg
// policy: keys and counts/min/max exact (float min/max ±0.0-equal, two NaNs
// equal), float sum/avg within the documented 1e-9 tolerance, integer sums via
// big.Int. Diffs stay value-free.

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

// groupedRow is one comparable row: the key part (typed) plus the aggregate
// cells in column order (reusing compare_agg's aggCell + policies).
type groupedRow struct {
	keyNull bool
	keyStr  string
	keyInt  int64
	keyIsI  bool
	cells   []aggCell
}

func groupedLess(a, b *groupedRow) bool {
	if a.keyNull != b.keyNull {
		return a.keyNull
	}
	if a.keyIsI {
		return a.keyInt < b.keyInt
	}
	return a.keyStr < b.keyStr
}

func groupedKeyEq(a, b *groupedRow) bool {
	if a.keyNull != b.keyNull || a.keyIsI != b.keyIsI {
		return false
	}
	if a.keyNull {
		return true
	}
	if a.keyIsI {
		return a.keyInt == b.keyInt
	}
	return a.keyStr == b.keyStr
}

func groupedFromArcx(rec arrow.Record, keyCol int) ([]groupedRow, error) {
	ncols := int(rec.NumCols())
	if keyCol >= ncols {
		return nil, fmt.Errorf("arcx grouped: key col %d out of %d", keyCol, ncols)
	}
	out := make([]groupedRow, rec.NumRows())
	for r := range out {
		row := &out[r]
		for c := 0; c < ncols; c++ {
			if c == keyCol {
				switch col := rec.Column(c).(type) {
				case *array.String:
					if col.IsNull(r) {
						row.keyNull = true
					} else {
						row.keyStr = col.Value(r)
					}
				case *array.Int64:
					row.keyIsI = true
					if col.IsNull(r) {
						row.keyNull = true
					} else {
						row.keyInt = col.Value(r)
					}
				// agg-3 bucket key: compare as epoch µs (typed, never a string).
				case *array.Timestamp:
					row.keyIsI = true
					if col.IsNull(r) {
						row.keyNull = true
					} else {
						unit := col.DataType().(*arrow.TimestampType).Unit
						row.keyInt = toMicros(int64(col.Value(r)), unit)
					}
				default:
					return nil, fmt.Errorf("arcx grouped key: unexpected type %T", rec.Column(c))
				}
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

func groupedFromRows(rows *sql.Rows, keyCol int) ([]groupedRow, error) {
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
			if c == keyCol {
				switch x := v.(type) {
				case nil:
					row.keyNull = true
				case string:
					row.keyStr = x
				case []byte:
					row.keyStr = string(x)
				case int64:
					row.keyIsI = true
					row.keyInt = x
				case time.Time:
					row.keyIsI = true
					row.keyInt = x.UnixMicro()
				default:
					return nil, fmt.Errorf("duckdb grouped key: unexpected type %T", v)
				}
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

// compareGrouped returns "" on match, else a short VALUE-FREE diff. `items` is
// the select list in column order (the key item marks the key column; the rest
// pick the per-cell policy — tolerance only for float sum/avg).
func compareGrouped(rec arrow.Record, oracle []groupedRow, items []string, keyCol int) string {
	got, err := groupedFromArcx(rec, keyCol)
	if err != nil {
		return "arcx grouped decode error: " + err.Error()
	}
	if len(got) != len(oracle) {
		return fmt.Sprintf("grouped row count differs (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	// Cell policies in CELL order (items minus the key, order preserved).
	var tolerant []bool
	for i, item := range items {
		if i == keyCol {
			continue
		}
		tolerant = append(tolerant, aggItemTolerant(item))
	}
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
