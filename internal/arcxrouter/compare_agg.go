// Agg-1 (Phase 3 slice 1) shadow comparison: one row, N aggregate columns.
//
// Policy (mirrors the engine's differential harness, arcx plan
// 2026-08-26-phase3-agg1 F2/F6/F7):
//   - count / count(*): exact int64.
//   - min / max: exact numeric — float compares with == (which already treats
//     -0.0 == 0.0, the DuckDB first-encountered-wins ±0.0 case that is not
//     oracle-able), and two NaNs compare EQUAL (NaN is a legitimate shared
//     answer, and NaN != NaN would false-alarm every NaN-bearing column).
//   - sum / avg on floats: relative tolerance 1e-9 (absolute near zero). arcx's
//     deterministic work-list-order combine and DuckDB's parallel reduction are
//     different association orders; bit-matching is not a defined target.
//   - sum on integers: DuckDB returns HUGEINT (the driver scans *big.Int); arcx
//     returns int64 (Arc's decimal→int64 client contract). Compared exactly as
//     big integers — a HUGEINT beyond int64 can't appear here, because the arcx
//     side errors before a record exists.
//
// Diffs are VALUE-FREE (class only), same as every other comparator in this
// package — mismatch logs must never leak query results.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const aggFloatRelTol = 1e-9

// aggCell is one oracle cell reduced to a comparable form. Exactly one of
// (i, f, isNull) is meaningful per kind.
type aggCell struct {
	isNull bool
	isInt  bool
	i      *big.Int // isInt: count, integer sum, timestamp µs
	f      float64  // !isInt: float sum/avg/min/max
}

// aggFromRows scans DuckDB's single aggregate row. The driver hands back int64
// (count, int min/max), float64 (double aggs), time.Time (timestamp min/max),
// *big.Int (HUGEINT integer sums), or nil (NULL aggregates over an empty set).
func aggFromRows(rows *sql.Rows) ([]aggCell, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("duckdb agg returned no rows")
	}
	raw := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	if rows.Next() {
		return nil, fmt.Errorf("duckdb agg returned more than one row")
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]aggCell, len(cols))
	for i, v := range raw {
		switch x := v.(type) {
		case nil:
			out[i] = aggCell{isNull: true}
		case int64:
			out[i] = aggCell{isInt: true, i: big.NewInt(x)}
		case *big.Int:
			out[i] = aggCell{isInt: true, i: x}
		case float64:
			out[i] = aggCell{f: x}
		case time.Time:
			out[i] = aggCell{isInt: true, i: big.NewInt(x.UnixMicro())}
		default:
			return nil, fmt.Errorf("duckdb agg col %d: unexpected type %T", i, v)
		}
	}
	return out, nil
}

// aggFromArcx reduces arcx's 1×N record to the same cell form.
func aggFromArcx(rec arrow.Record) ([]aggCell, error) {
	if rec.NumRows() != 1 {
		return nil, fmt.Errorf("arcx agg expected 1 row, got %d", rec.NumRows())
	}
	out := make([]aggCell, rec.NumCols())
	for c := 0; c < int(rec.NumCols()); c++ {
		switch col := rec.Column(c).(type) {
		case *array.Int64:
			if col.IsNull(0) {
				out[c] = aggCell{isNull: true}
			} else {
				out[c] = aggCell{isInt: true, i: big.NewInt(col.Value(0))}
			}
		case *array.Float64:
			if col.IsNull(0) {
				out[c] = aggCell{isNull: true}
			} else {
				out[c] = aggCell{f: col.Value(0)}
			}
		case *array.Timestamp:
			if col.IsNull(0) {
				out[c] = aggCell{isNull: true}
			} else {
				unit := col.DataType().(*arrow.TimestampType).Unit
				out[c] = aggCell{isInt: true, i: big.NewInt(toMicros(int64(col.Value(0)), unit))}
			}
		default:
			return nil, fmt.Errorf("arcx agg col %d: unexpected type %T", c, rec.Column(c))
		}
	}
	return out, nil
}

// aggItemTolerant reports whether the item's float result carries the documented
// tolerance (sum/avg — order-dependent reductions). min/max/count stay exact.
func aggItemTolerant(item string) bool {
	return len(item) > 4 && (item[:4] == "sum(" || item[:4] == "avg(")
}

// compareAgg returns "" on match, else a short VALUE-FREE diff.
func compareAgg(rec arrow.Record, oracle []aggCell, items []string) string {
	got, err := aggFromArcx(rec)
	if err != nil {
		return "arcx agg decode error: " + err.Error()
	}
	if len(got) != len(oracle) {
		return fmt.Sprintf("agg column count differs (arcx=%d duckdb=%d)", len(got), len(oracle))
	}
	// agg-4 tie policy: a VALUE diff on an arg_max/arg_min cell is deferred —
	// only if NO non-arg cell differs does it surface, tagged "argdiff:" so
	// the caller WARNs instead of alarming (DuckDB's tie pick is
	// nondeterministic; arcx's is pinned). Structural diffs (null-ness/kind)
	// on arg cells still alarm: ties can't change a cell's type.
	argDiff := ""
	deferArg := func(c int, msg string) bool {
		if c < len(items) && isArgItem(items[c]) {
			if argDiff == "" {
				argDiff = msg
			}
			return true
		}
		return false
	}
	for c := range got {
		a, d := got[c], oracle[c]
		if a.isNull != d.isNull {
			return fmt.Sprintf("agg col %d differs (null-ness: arcx_null=%t duckdb_null=%t)", c, a.isNull, d.isNull)
		}
		if a.isNull {
			continue
		}
		if a.isInt != d.isInt {
			// One side numeric-int, the other float — a type divergence (e.g. a
			// shape mapping bug), not a value difference.
			return fmt.Sprintf("agg col %d differs (kind: arcx_int=%t duckdb_int=%t)", c, a.isInt, d.isInt)
		}
		if a.isInt {
			if a.i.Cmp(d.i) != 0 {
				msg := fmt.Sprintf("agg col %d differs (int values withheld from log)", c)
				if deferArg(c, msg) {
					continue
				}
				return msg
			}
			continue
		}
		// Float cell. Two NaNs agree; one-sided NaN is a real divergence —
		// EXCEPT on an arg cell, where a tie between a NaN payload and a real
		// one legitimately diverges (defer like a value diff).
		if math.IsNaN(a.f) || math.IsNaN(d.f) {
			if math.IsNaN(a.f) && math.IsNaN(d.f) {
				continue
			}
			msg := fmt.Sprintf("agg col %d differs (NaN-ness: arcx_nan=%t duckdb_nan=%t)", c, math.IsNaN(a.f), math.IsNaN(d.f))
			if deferArg(c, msg) {
				continue
			}
			return msg
		}
		tolerant := c < len(items) && aggItemTolerant(items[c])
		if tolerant {
			scale := math.Max(math.Abs(a.f), math.Abs(d.f))
			if math.Abs(a.f-d.f) > aggFloatRelTol*math.Max(scale, 1.0) {
				return fmt.Sprintf("agg col %d differs (float beyond 1e-9 tolerance, values withheld)", c)
			}
		} else if a.f != d.f {
			msg := fmt.Sprintf("agg col %d differs (float values withheld from log)", c)
			if deferArg(c, msg) {
				continue
			}
			return msg
		}
	}
	if argDiff != "" {
		return "argdiff:" + argDiff
	}
	return ""
}
