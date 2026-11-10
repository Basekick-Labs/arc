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
	"strings"
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
	isBool bool     // agg-6b' bool_and / bool_or (exact compare)
	i      *big.Int // isInt: count, integer sum, timestamp µs
	f      float64  // !isInt && !isBool: float sum/avg/min/max
	b      bool     // isBool
	isStr  bool     // agg-7 mode over VARCHAR (exact compare)
	s      string   // isStr
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
		case uint32: // agg-6b regr_count (UINTEGER)
			out[i] = aggCell{isInt: true, i: big.NewInt(int64(x))}
		case bool: // agg-6b' bool_and / bool_or
			out[i] = aggCell{isBool: true, b: x}
		case string: // agg-7 mode over VARCHAR
			out[i] = aggCell{isStr: true, s: x}
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
		case *array.Uint32: // agg-6b regr_count
			if col.IsNull(0) {
				out[c] = aggCell{isNull: true}
			} else {
				out[c] = aggCell{isInt: true, i: big.NewInt(int64(col.Value(0)))}
			}
		case *array.Boolean: // agg-6b' bool_and / bool_or
			if col.IsNull(0) {
				out[c] = aggCell{isNull: true}
			} else {
				out[c] = aggCell{isBool: true, b: col.Value(0)}
			}
		case *array.String: // agg-7 mode over VARCHAR
			if col.IsNull(0) {
				out[c] = aggCell{isNull: true}
			} else {
				out[c] = aggCell{isStr: true, s: col.Value(0)}
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

// isShapeItem reports an agg-8 shape-statistic item. DuckDB 1.5.6 computes these
// from raw power sums: on constant columns and high-offset data its result is
// architecture-dependent garbage, NULL or NaN where the true value exists (arcX
// agg-8 adversarial B2), while arcx computes central moments exactly. So ANY
// difference on these cells — value, NULL-ness, NaN-ness — is filed as
// `oraclediff:` (WARN), never a mismatch alarm; their correctness is pinned by
// the engine's exact-reference fixtures, and the shadow comparison is
// informational.
var shapeItemPrefixes = []string{"skewness(", "kurtosis(", "kurtosis_pop("}

func isShapeItem(item string) bool {
	for _, p := range shapeItemPrefixes {
		if strings.HasPrefix(item, p) {
			return true
		}
	}
	return false
}

// shapeKindDiffers: a kind difference (int/bool/string vs float) on a shape cell
// is a structural engine bug — oracle garbage can't change a value's type — so it
// still alarms (review M1).
func shapeKindDiffers(a, d aggCell) bool {
	if a.isNull || d.isNull {
		return false
	}
	return a.isInt != d.isInt || a.isBool != d.isBool || a.isStr != d.isStr
}

// shapeCellDiffers: true when two shape cells differ beyond the tolerance in any
// way (NULL-ness, NaN-ness, Inf-ness, value).
func shapeCellDiffers(a, d aggCell) bool {
	if a.isNull || d.isNull {
		return a.isNull != d.isNull
	}
	if math.IsNaN(a.f) || math.IsNaN(d.f) {
		return !(math.IsNaN(a.f) && math.IsNaN(d.f))
	}
	if math.IsInf(a.f, 0) || math.IsInf(d.f, 0) {
		return a.f != d.f
	}
	scale := math.Max(math.Abs(a.f), math.Abs(d.f))
	return math.Abs(a.f-d.f) > aggFloatRelTol*math.Max(scale, 1.0)
}

// compareAgg returns "" on match, else a short VALUE-FREE diff.
func compareAgg(rec arrow.Record, oracle []aggCell, items []string) string {
	got, err := aggFromArcx(rec)
	if err != nil {
		return skipPrefix + "arcx agg decode error: " + err.Error()
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
	oracleDiff := ""
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
		if c < len(items) && isShapeItem(items[c]) {
			if shapeKindDiffers(a, d) {
				return fmt.Sprintf("agg col %d differs (kind on a shape statistic)", c)
			}
			if shapeCellDiffers(a, d) && oracleDiff == "" {
				oracleDiff = fmt.Sprintf("agg col %d differs (shape statistic vs DuckDB 1.5.6 raw sums, values withheld)", c)
			}
			continue
		}
		if a.isNull != d.isNull {
			return fmt.Sprintf("agg col %d differs (null-ness: arcx_null=%t duckdb_null=%t)", c, a.isNull, d.isNull)
		}
		if a.isNull {
			continue
		}
		if a.isBool != d.isBool {
			return fmt.Sprintf("agg col %d differs (kind: arcx_bool=%t duckdb_bool=%t)", c, a.isBool, d.isBool)
		}
		if a.isBool {
			if a.b != d.b {
				return fmt.Sprintf("agg col %d differs (boolean value withheld from log)", c)
			}
			continue
		}
		if a.isStr != d.isStr {
			return fmt.Sprintf("agg col %d differs (kind: arcx_str=%t duckdb_str=%t)", c, a.isStr, d.isStr)
		}
		if a.isStr {
			if a.s != d.s {
				return fmt.Sprintf("agg col %d differs (string value withheld from log)", c)
			}
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
		// ±Inf on either side compares exactly. The tolerance test below cannot
		// see it: |finite − Inf| = Inf and 1e-9·max(Inf, 1) = Inf, and Inf > Inf
		// is false — a finite-vs-Inf (or +Inf-vs-−Inf) divergence passed as a
		// match (found by the agg-6b' plan review).
		if math.IsInf(a.f, 0) || math.IsInf(d.f, 0) {
			if a.f == d.f {
				continue
			}
			msg := fmt.Sprintf("agg col %d differs (Inf-ness, values withheld)", c)
			if deferArg(c, msg) {
				continue
			}
			return msg
		}
		tolerant := c < len(items) && aggItemTolerant(items[c])
		if tolerant {
			scale := math.Max(math.Abs(a.f), math.Abs(d.f))
			if math.Abs(a.f-d.f) > aggFloatRelTol*math.Max(scale, 1.0) {
				msg := fmt.Sprintf("agg col %d differs (float beyond 1e-9 tolerance, values withheld)", c)
				if c < len(items) && isOracleUnstableItem(items[c]) {
					if oracleDiff == "" {
						oracleDiff = msg
					}
					continue
				}
				return msg
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
	if oracleDiff != "" {
		return "oraclediff:" + oracleDiff
	}
	return ""
}
