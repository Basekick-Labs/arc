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
	// Phase 3 agg-1 ungrouped aggregation: SELECT <agg list> FROM m [WHERE ...].
	ShapeScanAgg = "scan_agg"
	// Phase 3 agg-2b/2c: the grouped class past the perf gate — single key,
	// agg-1's aggregate set, NO WHERE. WHERE-bearing grouped shapes stay
	// ineligible (blocked by the broad-predicate bench; they share a shape).
	ShapeScanAggGrouped = "scan_agg_grouped"
)

// scanPred is one WHERE predicate `<col> <op> <literal>` from a scan. Exactly one
// of (num, str) is meaningful per isStr. The engine re-type-checks (col,op,lit);
// the router just carries the parsed parts.
type scanPred struct {
	col   string
	op    string // = != < <= > >=  (comparison predicates)
	num   string // integer OR float literal text (isStr == false; float when isFloat)
	str   string // string literal content (isStr == true)
	isStr bool
	// isFloat marks a DOUBLE-eq literal (2b-1b): num holds `digit.digit` text, op is
	// `=`/`!=`, and it's never `±0.0` (declined at match time). Emitted verbatim.
	isFloat bool
	// Null-check predicates (col IS [NOT] NULL). When isNull is true, op/num/str are
	// unused; negated distinguishes IS NOT NULL (true) from IS NULL (false).
	isNull  bool
	negated bool
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

// supportedUnits is the date_trunc units the engine buckets. Matches
// Unit::from_str in arcx/src/partition.rs. year/month/day/hour derive the bucket
// from the partition PATH; minute/second are sub-hour (the path granularity is at
// most the hour) so the engine decodes and buckets per-row — and requires a
// time-range WHERE (see isSubHour / the decline in matchDateTruncCount).
var supportedUnits = map[string]bool{
	"year":   true,
	"month":  true,
	"day":    true,
	"hour":   true,
	"minute": true,
	"second": true,
}

// isSubHour reports whether a date_trunc unit buckets below the partition path's
// finest granularity (the hour). Sub-hour units require a time-range WHERE: the
// engine's per-row decode counts only non-null in-range rows, so an UNFILTERED
// sub-hour query would miss DuckDB's date_trunc(NULL)=NULL bucket (silent wrong
// answer). A WHERE drops NULL-time rows by 3-valued logic, closing that gap. The
// engine enforces this too (defense in depth); declining here keeps a wrong shape
// off the arcx path entirely.
func isSubHour(unit string) bool {
	switch strings.ToLower(unit) {
	case "minute", "second":
		return true
	default:
		return false
	}
}

// tzInjectionTokens are lowercased substrings whose presence forces a decline:
// arcx buckets to UTC partition boundaries only, so any explicit timezone
// manipulation makes the answer potentially non-UTC and thus ineligible (M3).
// Arc's pooled DuckDB connection is UTC-pinned and pre-rewrites time functions to
// UTC epoch math, so the common case never contains these; this is the guard
// against a query that smuggles a session-TZ change in the SQL body.
var tzInjectionTokens = []string{"time zone", "timezone", "at time zone"}

// collationTokens force a decline for the same class of reason as tzInjectionTokens,
// but for string ordering (2b-4). arcx compares strings byte-wise, which equals DuckDB
// ONLY under the default BINARY collation. A `COLLATE` clause or an in-query
// `default_collation` change makes DuckDB order differently (e.g. NOCASE/ICU), so any
// string inequality/BETWEEN answer would silently diverge. Decline whenever the SQL body
// mentions collation. NOTE: this guards only the in-query case — an out-of-band
// `SET default_collation` on the pooled DuckDB connection is invisible here and must be
// upheld by Arc keeping its pooled session at default BINARY collation (2b-4 review H1).
var collationTokens = []string{"collate", "default_collation"}

// nullOrderTokens force a decline for the footer-agg NULL bucket: arcx hard-codes the
// NULL bucket LAST under ORDER BY 1 (DuckDB's default `NULLS_LAST`). An in-query
// `SET default_null_order = 'nulls_first'` would make DuckDB order the NULL bucket
// FIRST, so arcx's fixed ordering would silently diverge (values identical, ROW ORDER
// differs). Decline whenever the SQL body mentions null-order. Same out-of-band caveat
// as collation: a `SET default_null_order` on the pooled DuckDB connection is invisible
// here and must be upheld by Arc keeping the pooled session at NULLS_LAST. Note the
// filtered footer-agg shape never emits a NULL bucket (a time filter drops NULL rows),
// so this only bites the unfiltered date_trunc agg — but the guard is cheap and
// covers both.
var nullOrderTokens = []string{"default_null_order", "null_order"}

// matchResult is what eligibleShape resolves a recognized query into. `unit` is
// set only for the date_trunc agg; `col` only for the scalar column aggregates
// (min/max/count(col)). measurement is the bare FROM token as written.
type matchResult struct {
	shape       string
	unit        string // date_trunc agg only
	col         string // min/max/count(col) only
	measurement string
	cols        []string   // scan only: projected columns (as written)
	preds       []scanPred // scan only: AND-conjoined WHERE predicates (flat case)
	whereText   string     // re-serialized WHERE: boolean tree (scan, 2b-2) OR time-range (date_trunc agg, PR-A)
	// (OR / parens). Mutually exclusive with preds — set only when the flat AND-list
	// can't represent the WHERE (2b-2). buildScanSQL emits it verbatim; the engine is
	// the tree authority.
	orderBy []scanOrderKey // scan only: ORDER BY keys
	limit   int            // scan only: LIMIT n (0 = none)
	// scan_agg only: re-serialized aggregate items ("count(*)", "sum(colAsWritten)"),
	// in select-list order — validated token-by-token in matchScanAgg.
	aggItems []string
	// scan_agg_grouped_count only: the single group-key column (as written).
	// aggItems then holds the select list ("count(*)" or the key), in order.
	groupKey string
	// agg-3 bucket key: the validated date_trunc parts (empty = a bare tag key).
	// The SQL builder re-emits from THESE, never from groupKey's text.
	bucketUnit string
	bucketCol  string
	// agg-3: emit `ORDER BY <key position>` (ascending) in the engine SQL.
	orderByKey bool
	// agg-3b epoch-math bucket: width secs (0 = not this form) + mandatory alias.
	epochWidthSecs int
	bucketAlias    string
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
	// Collation guard (2b-4): arcx's byte-wise string comparison equals DuckDB only under
	// the default BINARY collation. Any in-query collation mention makes string ordering
	// potentially non-BINARY and thus ineligible.
	for _, t := range collationTokens {
		if strings.Contains(low, t) {
			return matchResult{}, false
		}
	}
	// Null-order guard: arcx's fixed NULLS_LAST footer-agg output diverges from DuckDB
	// under an in-query `SET default_null_order = 'nulls_first'`.
	for _, t := range nullOrderTokens {
		if strings.Contains(low, t) {
			return matchResult{}, false
		}
	}

	toks, ok := tokenize(sql)
	if !ok {
		return matchResult{}, false
	}
	if u, meas, whereText, ok := matchDateTruncCount(toks); ok {
		return matchResult{shape: ShapeDateTruncCent, unit: u, measurement: meas, whereText: whereText}, true
	}
	if meas, ok := matchCountStar(toks); ok {
		return matchResult{shape: ShapeCountStar, measurement: meas}, true
	}
	if fn, col, meas, ok := matchScalarAgg(toks); ok {
		shape := map[string]string{"min": ShapeMinCol, "max": ShapeMaxCol, "count": ShapeCountCol}[fn]
		return matchResult{shape: shape, col: col, measurement: meas}, true
	}
	// Agg-1 ungrouped aggregation: tried after the footer shapes (they keep first
	// refusal — the engine itself routes footer-first) and before the scan. Catches
	// the shapes the footer matchers just declined (a WHERE, a multi-agg list,
	// sum/avg) — the engine's parse-level fall-through, mirrored.
	if items, whereText, meas, ok := matchScanAgg(toks); ok {
		return matchResult{shape: ShapeScanAgg, aggItems: items, whereText: whereText, measurement: meas}, true
	}
	// The allow-listed grouped shape (agg-2b → agg-2c full agg set → mimalloc
	// slice optional WHERE). Tried before the scan (a bare-column-led grouped
	// select would fail the scan anyway; count-led ones fall through the agg
	// matchers above).
	if gm, ok := matchGroupedAgg(toks); ok {
		return matchResult{
			shape:          ShapeScanAggGrouped,
			aggItems:       gm.items,
			groupKey:       gm.key,
			bucketUnit:     gm.bucketUnit,
			bucketCol:      gm.bucketCol,
			epochWidthSecs: gm.epochWidthSecs,
			bucketAlias:    gm.bucketAlias,
			orderByKey:     gm.orderByKey,
			whereText:      gm.whereText,
			measurement:    gm.meas,
		}, true
	}
	// Scan is tried LAST: it's the broadest shape (a bare column list matches many
	// SELECTs), so the specific aggregate shapes get first refusal.
	if cols, preds, whereText, orderBy, limit, meas, ok := matchScan(toks); ok {
		return matchResult{
			shape:       ShapeScan,
			cols:        cols,
			preds:       preds,
			whereText:   whereText,
			orderBy:     orderBy,
			limit:       limit,
			measurement: meas,
		}, true
	}
	return matchResult{}, false
}
