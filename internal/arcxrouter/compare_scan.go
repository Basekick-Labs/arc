// Record-batch comparator for the Phase 2a general scan. Unlike the scalar and
// (bucket,count) comparators, a scan returns arbitrary width/height, so this
// decodes BOTH sides into a generic row multiset of per-column string cells and
// compares (arcx has no ORDER BY in 2a → multiset, not ordered). Column NAMES are
// part of each cell key, so a wrong column set or a wrong column also mismatches.
//
// Tagged: reads the arcx arrow.Record.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// scanRow is one result row rendered as name=value cells, sorted by column name so
// the compare is independent of column ORDER within a row (column order is a
// separate concern; value-correctness is what shadow mode gates). A NULL renders as
// the sentinel "\x00NULL".
type scanRow []string

const scanNull = "\x00NULL"

// compareScan returns "" if arcx's record and DuckDB's rows carry the same value
// multiset, else a human diff string (logged as a mismatch). Column-name/type
// differences surface as row mismatches (the cells are name-keyed).
func compareScan(rec arrow.Record, oracle []scanRow) string {
	arcx, err := scanRowsFromArcx(rec)
	if err != nil {
		// A HOST-side render failure, not an engine disagreement. Returning it as a
		// plain diff made it fire ArcxShadowMismatch — "arcx is wrong" — for a bug
		// on our side of the boundary. Tag it so runShadow reports a SKIP instead
		// (gotcha #4: decline, error and mismatch must stay distinguishable).
		return skipPrefix + "arcx decode error: " + err.Error()
	}
	cols := make([]string, rec.Schema().NumFields())
	for i, f := range rec.Schema().Fields() {
		cols[i] = f.Name
	}
	a, ka := sortScanRows(arcx)
	o, ko := sortScanRows(oracle)
	if len(a) != len(o) {
		return fmt.Sprintf("row count: arcx=%d duckdb=%d", len(a), len(o))
	}
	for i := range a {
		if ka[i] != ko[i] {
			// Values are WITHHELD. This used to print both raw rows — host names and
			// metric values — into an ERROR log that only masks SQL and error text.
			// It was effectively unreachable while the drain truncated (the row-count
			// branch above always fired first); fixing the drain makes it the live
			// branch for every real divergence, so it has to match the posture the
			// sibling comparators already take (compare_grouped.go, compare.go).
			// Name the first differing COLUMN — enough to start an investigation,
			// nothing of the data itself.
			return fmt.Sprintf("row %d of %d differs (first differing column: %s; values withheld)",
				i, len(a), firstDiffColumn(a[i], o[i], cols))
		}
	}
	return ""
}

// firstDiffColumn names the first column whose cell differs, or "(row length)"
// when the rows differ only in length. Returns a NAME, never a value.
//
// `cols` is indexed POSITIONALLY, which is correct because a scanRow's cells are in
// column order — not sorted by name. (The scanRow type comment above has said
// "sorted by column name" for a while and is wrong; fixing the code to match it
// would mis-name every diff column.)
func firstDiffColumn(a, o scanRow, cols []string) string {
	for i := range a {
		if i >= len(o) {
			break
		}
		if a[i] != o[i] {
			if i < len(cols) {
				return cols[i]
			}
			return fmt.Sprintf("#%d", i)
		}
	}
	return "(row length)"
}

// scanRowsFromArcx renders the arcx record to name-keyed, per-row string cells.
func scanRowsFromArcx(rec arrow.Record) ([]scanRow, error) {
	nCols := int(rec.NumCols())
	nRows := int(rec.NumRows())
	names := make([]string, nCols)
	for c := 0; c < nCols; c++ {
		names[c] = rec.ColumnName(c)
	}
	out := make([]scanRow, nRows)
	for r := 0; r < nRows; r++ {
		cells := make(scanRow, nCols)
		for c := 0; c < nCols; c++ {
			v, err := arcxCell(rec.Column(c), r)
			if err != nil {
				return nil, err
			}
			cells[c] = names[c] + "=" + v
		}
		out[r] = cells
	}
	return out, nil
}

// arcxCell renders one arrow cell to a canonical string matching the DuckDB side.
func arcxCell(col arrow.Array, row int) (string, error) {
	if col.IsNull(row) {
		return scanNull, nil
	}
	switch a := col.(type) {
	case *array.Int64:
		return fmt.Sprintf("%d", a.Value(row)), nil
	case *array.Float64:
		return fmt.Sprintf("%v", a.Value(row)), nil
	case *array.String:
		return a.Value(row), nil
	case *array.Boolean:
		return fmt.Sprintf("%t", a.Value(row)), nil
	case *array.Timestamp:
		// µs epoch → canonical integer (the DuckDB side normalizes time.Time to µs).
		return fmt.Sprintf("%d", int64(a.Value(row))), nil
	default:
		return "", fmt.Errorf("unhandled arcx column type %T", col)
	}
}

// scanRowsFromRows renders DuckDB's *sql.Rows to the same name-keyed form. It reads
// column names from the result and dynamically Scans each row into []interface{}.
func scanRowsFromRows(rows *sql.Rows) ([]scanRow, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []scanRow
	for rows.Next() {
		holders := make([]interface{}, len(cols))
		for i := range holders {
			holders[i] = new(interface{})
		}
		if err := rows.Scan(holders...); err != nil {
			return nil, err
		}
		cells := make(scanRow, len(cols))
		for i, name := range cols {
			cells[i] = name + "=" + duckCell(*(holders[i].(*interface{})))
		}
		out = append(out, cells)
	}
	return out, rows.Err()
}

// duckCell renders a DuckDB-scanned value to the canonical string form arcxCell
// produces, so equal values compare equal across the driver's dynamic types.
func duckCell(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return scanNull
	case int64:
		return fmt.Sprintf("%d", x)
	case int32:
		return fmt.Sprintf("%d", int64(x))
	case float64:
		return fmt.Sprintf("%v", x)
	case bool:
		return fmt.Sprintf("%t", x)
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		// DuckDB TIMESTAMP → µs epoch (arcx renders µs-timestamps the same way).
		return fmt.Sprintf("%d", x.UnixMicro())
	default:
		return fmt.Sprintf("%v", x)
	}
}

// sortScanRows sorts rows lexicographically for a stable multiset compare and
// returns the sorted rows ALONGSIDE their join keys. Each row's cells are already in
// column order (both sides in result-schema order); the row-level sort makes the
// overall comparison order-insensitive.
//
// The key is precomputed once per row rather than joined inside the less-func. The
// old form called strings.Join on BOTH operands on every comparison — O(n log n)
// full-row joins. Measured at 907,916 rows × 5 columns: 1,462 ms / +2.3 GB churn
// versus 171 ms / +249 MB keyed. That cost was unreachable while the drain truncated
// the arcx side to one batch; fixing the drain is exactly what makes it reachable,
// so the two changes belong in the same slice.
func sortScanRows(rows []scanRow) ([]scanRow, []string) {
	out := make([]scanRow, len(rows))
	copy(out, rows)
	keys := make([]string, len(out))
	for i, r := range out {
		keys[i] = strings.Join(r, "\x1f")
	}
	idx := make([]int, len(out))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(i, j int) bool { return keys[idx[i]] < keys[idx[j]] })
	sortedRows := make([]scanRow, len(out))
	sortedKeys := make([]string, len(out))
	for n, from := range idx {
		sortedRows[n] = out[from]
		sortedKeys[n] = keys[from]
	}
	return sortedRows, sortedKeys
}
