// Shadow comparison for the allow-listed grouped shape (agg-2b): N rows of
// (single group key, count(*) columns). Both sides sort by the TYPED key part
// (plan F5: never a joined string; a single key makes this one part — Utf8 or
// Int64 — with NULL ordered first), then compare row-wise: keys and counts
// EXACT. Diffs stay value-free.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// groupedRow is one comparable grouped-count row: the key part (typed) and the
// remaining cells as int64 counts in column order.
type groupedRow struct {
	keyNull bool
	keyStr  string
	keyInt  int64
	keyIsI  bool
	counts  []int64
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

// groupedFromArcx extracts rows from arcx's record. `keyCol` is the key's
// column index within the select list (from Decision.AggItems order).
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
				default:
					return nil, fmt.Errorf("arcx grouped key: unexpected type %T", rec.Column(c))
				}
				continue
			}
			col, ok := rec.Column(c).(*array.Int64)
			if !ok || col.IsNull(r) {
				return nil, fmt.Errorf("arcx grouped count col %d: unexpected type/null", c)
			}
			row.counts = append(row.counts, col.Value(r))
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
					return nil, fmt.Errorf("duckdb grouped key: unexpected timestamp")
				default:
					return nil, fmt.Errorf("duckdb grouped key: unexpected type %T", v)
				}
				continue
			}
			x, ok := v.(int64)
			if !ok {
				return nil, fmt.Errorf("duckdb grouped count col %d: unexpected type %T", c, v)
			}
			row.counts = append(row.counts, x)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// compareGroupedCount returns "" on match, else a short VALUE-FREE diff.
func compareGroupedCount(rec arrow.Record, oracle []groupedRow, keyCol int) string {
	got, err := groupedFromArcx(rec, keyCol)
	if err != nil {
		return "arcx grouped decode error: " + err.Error()
	}
	if len(got) != len(oracle) {
		return fmt.Sprintf("grouped row count differs (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	sort.Slice(got, func(i, j int) bool { return groupedLess(&got[i], &got[j]) })
	sort.Slice(oracle, func(i, j int) bool { return groupedLess(&oracle[i], &oracle[j]) })
	for i := range got {
		a, d := &got[i], &oracle[i]
		if a.keyNull != d.keyNull || a.keyIsI != d.keyIsI ||
			(!a.keyNull && a.keyIsI && a.keyInt != d.keyInt) ||
			(!a.keyNull && !a.keyIsI && a.keyStr != d.keyStr) {
			return fmt.Sprintf("grouped row %d of %d differs (key, values withheld)", i, len(got))
		}
		if len(a.counts) != len(d.counts) {
			return fmt.Sprintf("grouped row %d differs (count-column arity)", i)
		}
		for c := range a.counts {
			if a.counts[c] != d.counts[c] {
				return fmt.Sprintf("grouped row %d of %d differs (count col %d, values withheld)", i, len(got), c)
			}
		}
	}
	return ""
}
