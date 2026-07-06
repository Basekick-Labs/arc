// Package arcxrouter decides whether a query is eligible for the standalone arcx
// engine and, when built with the engine linked in, runs it (shadow or serve).
//
// This file is the pure, cgo-free eligibility recognizer — it has NO dependency
// on the arcx FFI bridge, compiles in every build (tagged or not), and is the
// cheap Go-side pre-filter that runs on EVERY query. It must reject the common
// (non-eligible) case on a string-only tokenizer pass before any handler state,
// metadata, or storage is touched.
//
// The recognizer mirrors the engine's own parser discipline (arcx/src/parse.rs):
// whole-token matching, never substring — so `read_parquet_foo`, `SELECTcount`,
// and junk-before-parens can never mis-accept. It deliberately recognizes the
// USER-facing shape (`FROM <measurement>`), not the rewritten `read_parquet(...)`
// form the engine consumes; the router builds the engine SQL itself from the
// parsed components (see Decision). The engine re-validates everything and is the
// authority for anything needing file paths (the hour-over-daily-file decline).
//
// Two shapes only, matching exactly what the engine answers green on the corpus:
//   - bare  SELECT count(*) FROM <measurement>
//   - agg   SELECT date_trunc('<unit>', time), count(*) FROM <measurement> GROUP BY 1
//           (optional ORDER BY 1), unit in {year,month,day,hour}, column == "time"

package arcxrouter

import "strings"

// Shape fingerprints, used as the arcx result Shape and as the metric label.
const (
	ShapeCountStar     = "count_star"
	ShapeDateTruncCent = "date_trunc_count"
	// Phase 1b scalar footer aggregates over a bare column.
	ShapeMinCol   = "min_col"
	ShapeMaxCol   = "max_col"
	ShapeCountCol = "count_col"
	// Phase 2a general single-table scan: SELECT <cols> FROM m [WHERE <preds>].
	ShapeScan = "scan"
)

// scanPred is one WHERE predicate `<col> <op> <literal>` from a scan. Exactly one
// of (num, str) is meaningful per isStr. The engine re-type-checks (col,op,lit);
// the router just carries the parsed parts.
type scanPred struct {
	col   string
	op    string // = != < <= > >=
	num   string // integer literal text (isStr == false)
	str   string // string literal content (isStr == true)
	isStr bool
}

// scanOrderKey is one ORDER BY key `<col> [ASC|DESC]` from a scan.
type scanOrderKey struct {
	col  string
	desc bool
}

// timeColumn is Arc's hardcoded time-column convention (F1): every measurement
// ingests an int64-µs column literally named "time". There is no per-measurement
// registry, so the agg shape's bucket column must be exactly this or we decline.
const timeColumn = "time"

// supportedUnits is the date_trunc units the engine buckets from the partition
// path. Matches Unit::from_str in arcx/src/partition.rs.
var supportedUnits = map[string]bool{
	"year":  true,
	"month": true,
	"day":   true,
	"hour":  true,
}

// tzInjectionTokens are lowercased substrings whose presence forces a decline:
// arcx buckets to UTC partition boundaries only, so any explicit timezone
// manipulation makes the answer potentially non-UTC and thus ineligible (M3).
// Arc's pooled DuckDB connection is UTC-pinned and pre-rewrites time functions to
// UTC epoch math, so the common case never contains these; this is the guard
// against a query that smuggles a session-TZ change in the SQL body.
var tzInjectionTokens = []string{"time zone", "timezone", "at time zone"}

// matchResult is what eligibleShape resolves a recognized query into. `unit` is
// set only for the date_trunc agg; `col` only for the scalar column aggregates
// (min/max/count(col)). measurement is the bare FROM token as written.
type matchResult struct {
	shape       string
	unit        string     // date_trunc agg only
	col         string     // min/max/count(col) only
	measurement string
	cols        []string       // scan only: projected columns (as written)
	preds       []scanPred     // scan only: AND-conjoined WHERE predicates
	orderBy     []scanOrderKey // scan only: ORDER BY keys
	limit       int            // scan only: LIMIT n (0 = none)
}

// eligibleShape recognizes the arcx shapes on the raw user SQL. ok=false means
// "not one of our shapes" — decline silently, the overwhelmingly common path, so
// it must be cheap (a lowercased Contains guard + one tokenize pass).
func eligibleShape(sql string) (matchResult, bool) {
	// Cheap TZ-injection guard first — a lowercased Contains, before tokenizing.
	// Correctness gate (M3): never let a non-UTC session reach the UTC fast path.
	// This matters for min/max(time) too — a non-UTC session renders timestamps
	// differently, so the scalar timestamp shapes inherit the exclusion.
	low := strings.ToLower(sql)
	for _, t := range tzInjectionTokens {
		if strings.Contains(low, t) {
			return matchResult{}, false
		}
	}

	toks, ok := tokenize(sql)
	if !ok {
		return matchResult{}, false
	}
	if u, meas, ok := matchDateTruncCount(toks); ok {
		return matchResult{shape: ShapeDateTruncCent, unit: u, measurement: meas}, true
	}
	if meas, ok := matchCountStar(toks); ok {
		return matchResult{shape: ShapeCountStar, measurement: meas}, true
	}
	if fn, col, meas, ok := matchScalarAgg(toks); ok {
		shape := map[string]string{"min": ShapeMinCol, "max": ShapeMaxCol, "count": ShapeCountCol}[fn]
		return matchResult{shape: shape, col: col, measurement: meas}, true
	}
	// Scan is tried LAST: it's the broadest shape (a bare column list matches many
	// SELECTs), so the specific aggregate shapes get first refusal.
	if cols, preds, orderBy, limit, meas, ok := matchScan(toks); ok {
		return matchResult{
			shape:       ShapeScan,
			cols:        cols,
			preds:       preds,
			orderBy:     orderBy,
			limit:       limit,
			measurement: meas,
		}, true
	}
	return matchResult{}, false
}
