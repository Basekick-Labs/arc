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
)

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

// eligibleShape recognizes the two arcx shapes on the raw user SQL and returns
// (shape, unit, measurementToken, ok). measurementToken is the bare table
// reference as written (e.g. "cpu" or "mydb.cpu"); the caller resolves it to a
// (database, measurement) pair via Arc's extractTableReferences + headerDB fold.
// unit is "" for the count(*) shape. ok=false means "not one of our shapes" —
// decline silently, this is the overwhelmingly common path and must be cheap.
func eligibleShape(sql string) (shape, unit, measurement string, ok bool) {
	// Cheap TZ-injection guard first — a lowercased Contains, before tokenizing.
	// Correctness gate (M3): never let a non-UTC session reach the UTC fast path.
	low := strings.ToLower(sql)
	for _, t := range tzInjectionTokens {
		if strings.Contains(low, t) {
			return "", "", "", false
		}
	}

	toks, ok := tokenize(sql)
	if !ok {
		return "", "", "", false
	}
	if u, meas, ok := matchDateTruncCount(toks); ok {
		return ShapeDateTruncCent, u, meas, true
	}
	if meas, ok := matchCountStar(toks); ok {
		return ShapeCountStar, "", meas, true
	}
	return "", "", "", false
}
