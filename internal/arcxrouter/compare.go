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
