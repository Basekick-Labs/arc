// Value comparison between arcx's Arrow result and DuckDB's rows, normalized to a
// common canonical form and compared as a multiset (GROUP BY order isn't
// guaranteed; Phase 1 declines ORDER BY so multiset is correct). Tagged: it reads
// the arcx arrow.Record.
//
// arcx result shapes (from arcx/src/lib.rs):
//   - count(*): one Int64 column "count_star()", one row.
//   - agg:      Timestamp(µs, "UTC") bucket + Int64 "count_star()".
// DuckDB via *sql.Rows scans the bucket as time.Time and the count as int64. The
// canonical form reduces both to (bucketMicros int64, count int64); count(*) uses
// a sentinel bucket so the two shapes share one comparator.

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

// noBucket is the sentinel bucket key for the scalar count(*) shape (which has no
// time bucket). A real bucket is a µs epoch; math.MinInt64 never collides.
const noBucket int64 = -1 << 62

// canonicalRow is one comparable (bucket, count) pair.
type canonicalRow struct {
	bucketMicros int64
	count        int64
}

// canonicalResult is the multiset of rows plus a flag for which shape produced it
// (so a shape/column-count mismatch is caught before value comparison).
type canonicalResult struct {
	rows     []canonicalRow
	isScalar bool // true for count(*): exactly one row, noBucket key
	numCols  int
}

// canonicalFromArcx reduces the arcx arrow.Record to canonical form.
func canonicalFromArcx(rec arrow.Record) (canonicalResult, error) {
	nCols := int(rec.NumCols())
	switch nCols {
	case 1:
		// count(*): single Int64 column, single row.
		col, ok := rec.Column(0).(*array.Int64)
		if !ok {
			return canonicalResult{}, fmt.Errorf("arcx count col not Int64: %T", rec.Column(0))
		}
		if col.Len() != 1 {
			return canonicalResult{}, fmt.Errorf("arcx count expected 1 row, got %d", col.Len())
		}
		return canonicalResult{
			rows:     []canonicalRow{{bucketMicros: noBucket, count: col.Value(0)}},
			isScalar: true,
			numCols:  1,
		}, nil
	case 2:
		// agg: Timestamp(µs) bucket + Int64 count.
		ts, ok := rec.Column(0).(*array.Timestamp)
		if !ok {
			return canonicalResult{}, fmt.Errorf("arcx bucket col not Timestamp: %T", rec.Column(0))
		}
		cnt, ok := rec.Column(1).(*array.Int64)
		if !ok {
			return canonicalResult{}, fmt.Errorf("arcx count col not Int64: %T", rec.Column(1))
		}
		unit := ts.DataType().(*arrow.TimestampType).Unit
		n := ts.Len()
		rows := make([]canonicalRow, 0, n)
		for i := 0; i < n; i++ {
			if ts.IsNull(i) {
				return canonicalResult{}, fmt.Errorf("arcx bucket[%d] is null", i)
			}
			rows = append(rows, canonicalRow{
				bucketMicros: toMicros(int64(ts.Value(i)), unit),
				count:        cnt.Value(i),
			})
		}
		return canonicalResult{rows: rows, numCols: 2}, nil
	default:
		return canonicalResult{}, fmt.Errorf("arcx result has %d cols, expected 1 or 2", nCols)
	}
}

// canonicalFromRows reduces DuckDB *sql.Rows to canonical form. It reads the
// column count to pick the shape, mirroring canonicalFromArcx.
func canonicalFromRows(rows *sql.Rows) (canonicalResult, error) {
	cols, err := rows.Columns()
	if err != nil {
		return canonicalResult{}, err
	}
	switch len(cols) {
	case 1:
		var out canonicalResult
		out.isScalar = true
		out.numCols = 1
		seen := 0
		for rows.Next() {
			var c int64
			if err := rows.Scan(&c); err != nil {
				return canonicalResult{}, err
			}
			out.rows = append(out.rows, canonicalRow{bucketMicros: noBucket, count: c})
			seen++
		}
		if err := rows.Err(); err != nil {
			return canonicalResult{}, err
		}
		if seen != 1 {
			return canonicalResult{}, fmt.Errorf("duckdb count expected 1 row, got %d", seen)
		}
		return out, nil
	case 2:
		var out canonicalResult
		out.numCols = 2
		for rows.Next() {
			var bucket time.Time
			var c int64
			if err := rows.Scan(&bucket, &c); err != nil {
				return canonicalResult{}, err
			}
			out.rows = append(out.rows, canonicalRow{
				bucketMicros: bucket.UnixMicro(),
				count:        c,
			})
		}
		if err := rows.Err(); err != nil {
			return canonicalResult{}, err
		}
		return out, nil
	default:
		return canonicalResult{}, fmt.Errorf("duckdb result has %d cols, expected 1 or 2", len(cols))
	}
}

// compareResults returns "" if arcx (rec) and the DuckDB oracle match as a
// multiset, else a short human-readable diff for the alarm log. A shape mismatch
// (column count) or any scan error is itself a mismatch worth alarming.
func compareResults(rec arrow.Record, oracle canonicalResult) string {
	got, err := canonicalFromArcx(rec)
	if err != nil {
		return "arcx result decode error: " + err.Error()
	}
	if got.numCols != oracle.numCols {
		return fmt.Sprintf("column-count mismatch: arcx=%d duckdb=%d", got.numCols, oracle.numCols)
	}
	if len(got.rows) != len(oracle.rows) {
		return fmt.Sprintf("row-count mismatch: arcx=%d duckdb=%d", len(got.rows), len(oracle.rows))
	}
	a := append([]canonicalRow(nil), got.rows...)
	b := append([]canonicalRow(nil), oracle.rows...)
	sortRows(a)
	sortRows(b)
	for i := range a {
		if a[i] != b[i] {
			return fmt.Sprintf("row %d differs: arcx=(bucket=%d,count=%d) duckdb=(bucket=%d,count=%d)",
				i, a[i].bucketMicros, a[i].count, b[i].bucketMicros, b[i].count)
		}
	}
	return ""
}

func sortRows(r []canonicalRow) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].bucketMicros != r[j].bucketMicros {
			return r[i].bucketMicros < r[j].bucketMicros
		}
		return r[i].count < r[j].count
	})
}

// toMicros normalizes an Arrow timestamp value to microseconds. arcx emits µs
// (L3), but normalize defensively so a future unit change can't read as a value
// mismatch.
func toMicros(v int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return v * 1_000_000
	case arrow.Millisecond:
		return v * 1_000
	case arrow.Microsecond:
		return v
	case arrow.Nanosecond:
		return v / 1_000
	default:
		return v
	}
}

// --- Phase 1b scalar comparison (min/max/count(col)) ---------------------------
//
// These shapes are a SINGLE row, SINGLE column. Unlike count(*)/agg they can be a
// Timestamp scalar (min/max(time)) or NULL (all-null min/max), so they get their
// own comparison rather than the (bucket, count) canonical form. The value is
// reduced to a nullable int64 (count and int-min/max are already int64;
// timestamps become µs epoch; NULL is represented explicitly).

// scalarValue is one comparable cell: an int64 (or µs epoch for a timestamp), or
// NULL. Everything the scalar shapes produce reduces to this.
type scalarValue struct {
	isNull bool
	v      int64
}

func (s scalarValue) String() string {
	if s.isNull {
		return "NULL"
	}
	return fmt.Sprintf("%d", s.v)
}

// scalarFromArcx extracts the single cell from arcx's 1x1 result. Accepts Int64
// (count(col), int min/max) or Timestamp (min/max(time)); NULL is honored.
func scalarFromArcx(rec arrow.Record) (scalarValue, error) {
	if rec.NumCols() != 1 {
		return scalarValue{}, fmt.Errorf("arcx scalar expected 1 col, got %d", rec.NumCols())
	}
	if rec.NumRows() != 1 {
		return scalarValue{}, fmt.Errorf("arcx scalar expected 1 row, got %d", rec.NumRows())
	}
	switch col := rec.Column(0).(type) {
	case *array.Int64:
		if col.IsNull(0) {
			return scalarValue{isNull: true}, nil
		}
		return scalarValue{v: col.Value(0)}, nil
	case *array.Timestamp:
		if col.IsNull(0) {
			return scalarValue{isNull: true}, nil
		}
		unit := col.DataType().(*arrow.TimestampType).Unit
		return scalarValue{v: toMicros(int64(col.Value(0)), unit)}, nil
	default:
		return scalarValue{}, fmt.Errorf("arcx scalar unexpected col type %T", rec.Column(0))
	}
}

// scalarFromRows extracts the single cell from DuckDB's 1x1 result. DuckDB scans a
// count/int as int64 and a timestamp as time.Time; NULL scans as a nil pointer.
// We scan into *int64 and *time.Time via a sql.Null-style two-attempt: the column
// type tells us which. Simplest robust path: scan into an interface{} and type-switch.
func scalarFromRows(rows *sql.Rows) (scalarValue, error) {
	cols, err := rows.Columns()
	if err != nil {
		return scalarValue{}, err
	}
	if len(cols) != 1 {
		return scalarValue{}, fmt.Errorf("duckdb scalar expected 1 col, got %d", len(cols))
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return scalarValue{}, err
		}
		return scalarValue{}, fmt.Errorf("duckdb scalar returned no rows")
	}
	var raw interface{}
	if err := rows.Scan(&raw); err != nil {
		return scalarValue{}, err
	}
	// Guard against >1 row (a real scalar aggregate never returns more).
	if rows.Next() {
		return scalarValue{}, fmt.Errorf("duckdb scalar returned more than one row")
	}
	if err := rows.Err(); err != nil {
		return scalarValue{}, err
	}
	switch x := raw.(type) {
	case nil:
		return scalarValue{isNull: true}, nil
	case int64:
		return scalarValue{v: x}, nil
	case time.Time:
		return scalarValue{v: x.UnixMicro()}, nil
	default:
		return scalarValue{}, fmt.Errorf("duckdb scalar unexpected type %T", raw)
	}
}

// compareScalar returns "" if arcx's scalar matches DuckDB's, else a short diff.
func compareScalar(rec arrow.Record, oracle scalarValue) string {
	got, err := scalarFromArcx(rec)
	if err != nil {
		return "arcx scalar decode error: " + err.Error()
	}
	if got.isNull != oracle.isNull || (!got.isNull && got.v != oracle.v) {
		return fmt.Sprintf("scalar differs: arcx=%s duckdb=%s", got, oracle)
	}
	return ""
}

// isScalarShape reports whether a shape produces a single-cell result compared via
// compareScalar rather than the (bucket, count) canonical form.
func isScalarShape(shape string) bool {
	switch shape {
	case ShapeMinCol, ShapeMaxCol, ShapeCountCol:
		return true
	default:
		return false
	}
}
