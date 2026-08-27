// Tagged router: the real Decide/Run that drives the arcx engine. Built only with
// `arcx_engine` (and cgo). Without the tag, router_stub.go's no-ops take over and
// stock Arc is byte-identical.
//
// Shape of the integration (see arcx/docs/2026-07-05-router-phase1.md):
//   - Decide (cheap, cgo-free logic reused from eligibility.go) runs on every
//     query; the eligibility recognizer short-circuits the common non-eligible
//     case on a string pass.
//   - Run, in shadow mode (default), runs arcx AND a second cheap DuckDB oracle
//     execution, compares them, alarms on mismatch, and returns handled=false so
//     the caller's existing DuckDB dispatch serves the response untouched.
//   - Run, in serve mode, streams arcx's result for green shapes and returns
//     handled=true; declines/errors fall back to DuckDB.
//
// The router pulls what it needs (storage, db, logger, metadata) via an explicit
// Deps struct passed from the handleQuery hook — NOT via exported accessors on
// QueryHandler, so query.go gains no exported surface and the hot-path file stays
// otherwise untouched.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"context"
	"database/sql"
	"errors"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/arcxengine"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Deps is everything the tagged router needs from the query handler, passed
// explicitly so query.go exposes no new exported methods.
type Deps struct {
	Storage storage.Backend
	DB      OracleDB
	Logger  zerolog.Logger
	Metrics Metrics
	Mode    Mode
	// ConvertedSQL is the DuckDB-correct rewrite the caller already computed for
	// this query (the oracle SQL for shadow compare). date_trunc intact per DuckDB.
	ConvertedSQL string
	// ServeStream writes an arcx result to the response in the request's wire
	// format (JSON or msgpack), reusing Arc's existing Arrow→wire streamers. It
	// lives on the api side because those streamers are package-private there;
	// the router hands back the streaming RECORD READER (un-concatenated batches —
	// the single-batch concat is the serial tail this slice removes). The api side
	// owns the governance cap/timeout, the metrics/onComplete callbacks, and the
	// runtime.KeepAlive that holds the FFI-backed reader alive across the async wire
	// writer (its Release is a no-op; the arcx buffers free on a GC finalizer). `start`
	// is when the query began (BEFORE the engine ran) so execution_time_ms covers the
	// engine work. Returns true if it served. nil in shadow-only wiring.
	ServeStream func(c *fiber.Ctx, reader array.RecordReader, start time.Time) bool
	// AllowedDirs is DuckDB's sandbox allowlist, passed straight through to the
	// arcx engine's per-query Context. arcx does NOT inherit DuckDB's
	// allowed_directories, so this is the ONLY thing stopping arcx from being a
	// filesystem-sandbox bypass around Arc's CVE fix. Populated by the hook from
	// (*database.DuckDB).AllowedDirectories(); empty ⇒ engine denies every path.
	AllowedDirs []string
}

// Handler is the concrete dependency bundle in the tagged build.
type Handler = Deps

// OracleDB is the subset of *database.DuckDB the compare oracle uses: a plain
// row-returning query (tag-independent; deliberately not the duckdb_arrow path).
type OracleDB interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// Metrics is the observability surface. Kept as an interface so the handler wires
// its Prometheus collectors without the router importing them.
type Metrics interface {
	ArcxShadowMatch(shape string)
	ArcxShadowMismatch(shape string)
	ArcxShadowError(shape string)
	// ArcxShadowSkipped: a shadow sample was deliberately not taken (at the concurrency
	// cap, or the result exceeded ShadowMaxRows). NOT an error and NOT a mismatch —
	// conflating a skipped sample with a wrong answer would train operators to ignore
	// shadow alarms, which is the one thing shadow mode exists to provide.
	ArcxShadowSkipped(shape string)
	ArcxShadowDeclined(shape string)
	ArcxLatency(engine, shape string, micros int64)
}

// Decision is the router's verdict for a query. Carries structured components,
// not SQL — Run builds the arcx engine SQL from Unit + the expanded path array.
type Decision struct {
	Eligible  bool
	Ctx       arcxengine.Context
	Shape     string
	Unit      string         // date_trunc agg only
	Col       string         // min/max/count(col) only — the bare column, user's spelling
	Cols      []string       // scan only: projected columns (as written)
	Preds     []scanPred     // scan only: AND-conjoined WHERE predicates (flat case)
	WhereText string         // re-serialized WHERE: boolean tree for scan (2b-2), or a time-range filter for date_trunc agg (PR-A)
	OrderBy   []scanOrderKey // scan only: ORDER BY keys
	Limit     int            // scan only: LIMIT n (0 = none)
	AggItems  []string       // scan_agg only: re-serialized aggregate items, select-list order
	GroupKey  string         // scan_agg_grouped_count only: the single group-key column
}

// Decide is the cheap per-query pre-filter. It never calls the engine; it only
// recognizes the shape (string pass) and resolves the single measurement.
func Decide(sql, headerDB string, h Handler) Decision {
	// Mode gate first: off is a true zero-cost kill switch (H2).
	if h.Mode == ModeOff {
		return Decision{}
	}
	m, ok := eligibleShape(sql)
	if !ok {
		return Decision{}
	}
	// The recognizer already captured the single FROM token; split it into
	// (database, measurement) and fold headerDB for the bare form — mirroring
	// checkQueryPermissions' resolution (query.go:1220), self-contained so we
	// don't need Arc's unexported extractTableReferences.
	database, measurement, ok := resolveMeasurementToken(m.measurement, headerDB)
	if !ok {
		return Decision{}
	}
	return Decision{
		Eligible: true,
		Ctx: arcxengine.Context{
			Database:    database,
			Measurement: measurement,
			TimeColumn:  timeColumn,
			// Thread DuckDB's sandbox allowlist into the engine — without it every
			// arcx path-open is denied (fail-closed), and WITH the wrong list arcx
			// would be a bypass around Arc's CVE fix.
			AllowedDirs: h.AllowedDirs,
		},
		Shape:     m.shape,
		Unit:      m.unit,
		Col:       m.col,
		Cols:      m.cols,
		Preds:     m.preds,
		WhereText: m.whereText,
		OrderBy:   m.orderBy,
		Limit:     m.limit,
		AggItems:  m.aggItems,
		GroupKey:  m.groupKey,
	}
}

// Run executes the decision. In shadow it returns false (caller serves DuckDB);
// in serve it returns true when it streamed an arcx result.
func Run(c *fiber.Ctx, d Decision, h Handler, mode Mode, start time.Time) (handled bool) {
	if !d.Eligible || mode == ModeOff {
		return false
	}
	ctx := c.UserContext()

	engineSQL, ok := h.buildEngineSQL(ctx, d)
	if !ok {
		// No files / expansion failed — let DuckDB's normal no-files path handle it.
		return false
	}

	switch mode {
	case ModeShadow:
		h.runShadowAsync(ctx, d, engineSQL)
		return false // DuckDB always serves in shadow
	case ModeServe:
		return h.runServe(c, ctx, d, engineSQL, start)
	default:
		return false
	}
}

// RunArrow is the router entry for the raw Arrow-IPC endpoint, which streams
// arcx's record itself (IPC writing lives in the api package) rather than going
// through ServeStream. It returns the arcx record ONLY in serve mode on a green
// shape; the caller owns it and MUST Release it after streaming. In shadow mode it
// runs the compare and returns (nil,false); on decline/error/off it returns
// (nil,false) so the caller falls back to DuckDB. `ctx` is the caller's (already
// context.Background-derived, not the pooled Fiber ctx) execution context.
func RunArrow(ctx context.Context, d Decision, h Handler, mode Mode) (reader array.RecordReader, served bool) {
	if !d.Eligible || mode == ModeOff {
		return nil, false
	}
	engineSQL, ok := h.buildEngineSQL(ctx, d)
	if !ok {
		return nil, false
	}
	switch mode {
	case ModeShadow:
		h.runShadowAsync(ctx, d, engineSQL)
		return nil, false // DuckDB serves in shadow
	case ModeServe:
		r, err := arcxengine.QueryStream(engineSQL, d.Ctx)
		if err != nil {
			if _, unsupported := err.(arcxengine.ErrUnsupported); !unsupported {
				h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", sqlutil.ForLog(engineSQL)).
					Msg("arcx serve (arrow): engine ERROR; falling back to DuckDB")
				h.Metrics.ArcxShadowError(d.Shape)
			}
			return nil, false
		}
		// caller owns r: it drains the reader to the IPC wire and MUST
		// runtime.KeepAlive(r) past the last batch (the FFI-backed batches free on a
		// GC finalizer; Release is a no-op) — the FFI use-after-free cliff.
		return r, true
	default:
		return nil, false
	}
}

// buildEngineSQL expands the measurement to a concrete .parquet path array (F5 —
// the engine declines globs) and constructs canonical arcx SQL from the parsed
// parts. Returns ok=false if the backend isn't local (the engine has no
// object-store reader yet — Phase 4) or no parquet files exist.
func (h Deps) buildEngineSQL(ctx context.Context, d Decision) (string, bool) {
	// Local-FS only: the engine opens local files directly and has no S3/Azure
	// reader. A non-local backend must decline (→ DuckDB), not be handed paths it
	// can't open. This is a correctness gate, not an optimization.
	local, ok := h.Storage.(*storage.LocalBackend)
	if !ok {
		return "", false
	}
	basePath := local.GetBasePath()

	prefix := d.Ctx.Database + "/" + d.Ctx.Measurement + "/"
	entries, err := h.Storage.List(ctx, prefix)
	if err != nil {
		h.Logger.Warn().Err(err).Str("prefix", prefix).Msg("arcx: storage list failed; declining")
		return "", false
	}
	var arr strings.Builder
	arr.WriteByte('[')
	n := 0
	for _, e := range entries {
		if !strings.HasSuffix(e, ".parquet") {
			continue
		}
		if n > 0 {
			arr.WriteString(", ")
		}
		// List returns backend-relative paths; the engine needs absolute paths it
		// can open (and that still carry the db/measurement/Y/M/D[/H] structure
		// its partition parser anchors on). Prepend the base path.
		arr.WriteString(quotePath(basePath + "/" + e))
		n++
	}
	arr.WriteByte(']')
	if n == 0 {
		return "", false
	}

	switch d.Shape {
	case ShapeCountStar:
		return "SELECT count(*) FROM read_parquet(" + arr.String() + ")", true
	case ShapeDateTruncCent:
		// Unit spelling preserved verbatim so the engine reproduces DuckDB's
		// derived column name. Column is the "time" convention (F1). An optional
		// WHERE carries a re-serialized time-range filter (PR-A); the engine
		// re-lexes it, validates the RFC3339-UTC literal, and classifies path-first.
		var b strings.Builder
		b.WriteString("SELECT date_trunc('")
		b.WriteString(escapeStringLiteral(d.Unit))
		b.WriteString("', time), count(*) FROM read_parquet(")
		b.WriteString(arr.String())
		b.WriteByte(')')
		if d.WhereText != "" {
			b.WriteString(" WHERE ")
			b.WriteString(d.WhereText)
		}
		b.WriteString(" GROUP BY 1")
		return b.String(), true
	case ShapeMinCol, ShapeMaxCol, ShapeCountCol:
		// The engine's parser expects the column as a BARE identifier. d.Col came
		// from the tokenizer as an identifier ([A-Za-z_][A-Za-z0-9_.]*), so it's
		// injection-safe by construction; guard defensively anyway.
		if !isBareIdent(d.Col) {
			return "", false
		}
		fn := map[string]string{ShapeMinCol: "min", ShapeMaxCol: "max", ShapeCountCol: "count"}[d.Shape]
		return "SELECT " + fn + "(" + d.Col + ") FROM read_parquet(" + arr.String() + ")", true
	case ShapeScan:
		return buildScanSQL(d, arr.String())
	case ShapeScanAgg:
		return buildScanAggSQL(d, arr.String())
	case ShapeScanAggGroupedCount:
		return buildGroupedCountSQL(d, arr.String())
	default:
		return "", false
	}
}

// buildScanAggSQL constructs the engine SQL for an ungrouped aggregation (agg-1):
//
//	SELECT <agg items> FROM read_parquet([<paths>]) [WHERE <tree>]
//
// Every item came from matchScanAgg's token-validated re-serialization; re-validate
// the exact form here anyway (defense in depth vs a hand-built Decision) — decline
// rather than emit unsafe SQL. The WHERE text was built by reserializeWhere (every
// token re-validated, strings re-escaped), same as the scan's tree path.
func buildScanAggSQL(d Decision, pathArray string) (string, bool) {
	if len(d.AggItems) == 0 {
		return "", false
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, item := range d.AggItems {
		if !isAggItem(item) {
			return "", false
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(item)
	}
	b.WriteString(" FROM read_parquet(")
	b.WriteString(pathArray)
	b.WriteByte(')')
	if d.WhereText != "" {
		b.WriteString(" WHERE ")
		b.WriteString(d.WhereText)
	}
	return b.String(), true
}

// buildGroupedCountSQL constructs the engine SQL for the allow-listed grouped
// shape: `SELECT <items> FROM read_parquet([...]) GROUP BY <key>`. Items are
// re-validated (each is `count(*)` or exactly the bare key); decline rather
// than emit unsafe SQL — same defense-in-depth as the other builders.
func buildGroupedCountSQL(d Decision, pathArray string) (string, bool) {
	if len(d.AggItems) < 2 || !isBareIdent(d.GroupKey) {
		return "", false
	}
	sawKey, sawCount := false, false
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, item := range d.AggItems {
		switch {
		case item == "count(*)":
			sawCount = true
		case item == d.GroupKey && !sawKey:
			sawKey = true
		default:
			return "", false
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(item)
	}
	if !sawKey || !sawCount {
		return "", false
	}
	b.WriteString(" FROM read_parquet(")
	b.WriteString(pathArray)
	b.WriteString(") GROUP BY ")
	b.WriteString(d.GroupKey)
	return b.String(), true
}

// isAggItem re-validates one re-serialized aggregate item: `count(*)`, or
// `{count|sum|min|max|avg}(<bare ident>)` with the fn lowercased — exactly what
// matchScanAgg emits.
func isAggItem(item string) bool {
	if item == "count(*)" {
		return true
	}
	open := strings.IndexByte(item, '(')
	if open < 0 || !strings.HasSuffix(item, ")") {
		return false
	}
	switch item[:open] {
	case "count", "sum", "min", "max", "avg":
	default:
		return false
	}
	return isBareIdent(item[open+1 : len(item)-1])
}

// buildScanSQL constructs the engine SQL for a general single-table scan:
//
//	SELECT <cols> FROM read_parquet([<paths>]) [WHERE <col> <op> <lit> AND ...]
//
// Every column (projection AND predicate) must be a bare identifier — they came
// from the tokenizer as identifiers, so injection-safe by construction, but we
// guard defensively. String literals are single-quote escaped; numeric literals
// are validated as integers. Declines (ok=false) on any non-bare column or
// malformed literal rather than emit unsafe SQL.
func buildScanSQL(d Decision, pathArray string) (string, bool) {
	if len(d.Cols) == 0 {
		return "", false
	}
	var b strings.Builder
	b.WriteString("SELECT ")
	for i, col := range d.Cols {
		// A projection item is either a bare column or a recognized computed function
		// re-serialized by matchProjFunc (`length(col)`, 2f-0). Re-validate both here
		// (defense in depth vs a hand-built Decision) — a bare ident, or a proj-func
		// item whose form re-parses cleanly.
		if !isBareIdent(col) && !isProjFuncItem(col) {
			return "", false
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
	}
	b.WriteString(" FROM read_parquet(")
	b.WriteString(pathArray)
	b.WriteByte(')')

	// A boolean-tree WHERE (2b-2) is emitted from its pre-validated, re-serialized text
	// (built by reserializeWhere, which already re-escaped strings + re-validated every
	// token). Mutually exclusive with the flat Preds path.
	if d.WhereText != "" {
		b.WriteString(" WHERE ")
		b.WriteString(d.WhereText)
	} else if len(d.Preds) > 0 {
		b.WriteString(" WHERE ")
		for i, p := range d.Preds {
			if !isBareIdent(p.col) {
				return "", false
			}
			if i > 0 {
				b.WriteString(" AND ")
			}
			b.WriteString(p.col)
			if p.isNull {
				if p.negated {
					b.WriteString(" IS NOT NULL")
				} else {
					b.WriteString(" IS NULL")
				}
				continue
			}
			if !isCmpOp(p.op) {
				return "", false
			}
			b.WriteByte(' ')
			b.WriteString(p.op)
			b.WriteByte(' ')
			if p.isStr {
				b.WriteByte('\'')
				b.WriteString(escapeStringLiteral(p.str))
				b.WriteByte('\'')
			} else if p.isFloat {
				// DOUBLE comparison literal — validate the `digit.digit` shape and reject
				// `±0.0` again at emit time (defense in depth vs a hand-built Decision).
				// As of 2b-4 all six ops are allowed (arrow total_cmp == DuckDB); only the
				// ±0.0 literal stays rejected (signed-zero divergence, all ops).
				if !isFloatLiteral(p.num) || isZeroFloatLiteral(p.num) {
					return "", false
				}
				b.WriteString(p.num)
			} else {
				if !isIntLiteral(p.num) {
					return "", false
				}
				b.WriteString(p.num)
			}
		}
	}

	if len(d.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, k := range d.OrderBy {
			if !isBareIdent(k.col) {
				return "", false
			}
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(k.col)
			if k.desc {
				b.WriteString(" DESC")
			} else {
				b.WriteString(" ASC")
			}
		}
	}

	if d.Limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(strconv.Itoa(d.Limit))
	}

	return b.String(), true
}

// drainReaderToRecord pulls every batch from a streaming reader and concatenates them into
// ONE arrow.Record (the shadow comparators take a single Record). Each batch is Retained on
// extraction because the reader auto-releases the previous batch on the next Next() (the
// arrow-go C-stream reader contract). The caller MUST runtime.KeepAlive(reader) until after
// this returns — the batches are FFI-backed and free on the reader's GC finalizer. Returns
// an empty-but-schema'd record for a zero-batch result.
// `maxRows` bounds the drain: shadow materializes the WHOLE result into one record, and
// an eligible shape need carry no LIMIT (`SELECT host FROM cpu` is eligible), so an
// unbounded drain on a large measurement is an OOM in Arc's own address space. Stopping
// early is safe here — shadow never serves, and a truncated compare is reported as a
// skip, never as a mismatch (a false mismatch alarm would be worse than no signal).
func drainReaderToRecord(reader array.RecordReader, maxRows int) (arrow.Record, error) {
	schema := reader.Schema()
	var batches []arrow.Record
	rows := 0
	for reader.Next() {
		b := reader.Record()
		if b == nil {
			break
		}
		if maxRows > 0 && rows+int(b.NumRows()) > maxRows {
			for _, x := range batches {
				x.Release()
			}
			return nil, errShadowTruncated
		}
		rows += int(b.NumRows())
		b.Retain()
		batches = append(batches, b)
	}
	if err := reader.Err(); err != nil {
		for _, b := range batches {
			b.Release()
		}
		return nil, err
	}
	if len(batches) == 0 {
		return emptyRecord(schema), nil
	}
	if len(batches) == 1 {
		return batches[0], nil // caller releases
	}
	// Concatenate via a Table → single record. NewTableFromRecords retains the batches;
	// release our refs after.
	tbl := array.NewTableFromRecords(schema, batches)
	defer tbl.Release()
	for _, b := range batches {
		b.Release()
	}
	tr := array.NewTableReader(tbl, tbl.NumRows())
	defer tr.Release()
	if !tr.Next() {
		return emptyRecord(schema), nil
	}
	rec := tr.Record()
	rec.Retain() // outlive the table reader
	return rec, nil
}

// emptyRecord builds a zero-row record carrying `schema` (for a fully-filtered result).
// Each column is built via NewBuilder(f.Type) — its empty array's type is EXACTLY the
// schema field's type, so NewRecord's internal validate() cannot panic on a type mismatch
// (the invariant that keeps this off the "no panics in the query path" list). arcx's scan
// result schema is plain columns (dict encoding is reconciled to Utf8 before export), so
// there is no dictionary/extension type here for which an empty builder could disagree.
func emptyRecord(schema *arrow.Schema) arrow.Record {
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		b := array.NewBuilder(memory.DefaultAllocator, f.Type)
		cols[i] = b.NewArray()
		b.Release()
	}
	rec := array.NewRecord(schema, cols, 0)
	for _, col := range cols {
		col.Release()
	}
	return rec
}

// errShadowTruncated marks a shadow run abandoned because the result exceeded
// `ShadowMaxRows`. Reported as a SKIP, never a mismatch — see drainReaderToRecord.
var errShadowTruncated = errors.New("arcx shadow: result exceeded the shadow row cap")

// ShadowMaxRows bounds what shadow will materialize. Shadow holds the entire result in
// memory at once, so this is a memory ceiling, not a fairness knob.
const ShadowMaxRows = 1_000_000

// shadowSlots bounds CONCURRENT shadow runs. Shadow costs a second full DuckDB query on
// the same bounded `*sql.DB` pool plus a full arcx run, so unbounded concurrency doubles
// pool pressure exactly when the server is busiest. A shadow run that cannot get a slot is
// DROPPED (non-blocking send): shadow is a sampling signal, and shedding it under load is
// strictly better than adding latency to real queries.
var shadowSlots = make(chan struct{}, 2)

// runShadow runs arcx and a cheap DuckDB oracle, compares, and records metrics.
// It never serves — the caller's DuckDB dispatch does. Synchronous for the first
// cut (immediate, deterministic correctness signal).
// runShadowAsync launches runShadow OFF the request path. It was synchronous, which put a
// second full DuckDB query plus a full arcx run inside every request in the DEFAULT mode —
// doubling latency and pool pressure on a path that never serves. The context is DETACHED
// (context.WithoutCancel) because the request's ctx is cancelled the moment the handler
// returns, which would cancel the oracle query we just launched.
func (h Deps) runShadowAsync(ctx context.Context, d Decision, engineSQL string) {
	select {
	case shadowSlots <- struct{}{}:
	default:
		h.Metrics.ArcxShadowSkipped(d.Shape)
		return // at capacity: shed this sample rather than slow the request
	}
	detached := context.WithoutCancel(ctx)
	go func() {
		defer func() { <-shadowSlots }()
		// Shadow must never take Arc down: it runs off-request, so a panic here would
		// otherwise be an unrecovered goroutine panic = process exit.
		defer func() {
			if r := recover(); r != nil {
				h.Logger.Error().Interface("panic", r).Msg("arcx shadow: recovered panic")
			}
		}()
		h.runShadow(detached, d, engineSQL)
	}()
}

func (h Deps) runShadow(ctx context.Context, d Decision, engineSQL string) {
	arcxStart := time.Now()
	// Drive the STREAMING path in shadow (the default mode) so the FFI-import + reader
	// machinery is exercised on every shadow query — otherwise the streaming serve path
	// would first run in prod only at the serve-flip (its riskiest moment). Drain the
	// reader into ONE record for the existing comparators (which take a single Record).
	reader, err := arcxengine.QueryStream(engineSQL, d.Ctx)
	if err != nil {
		if _, unsupported := err.(arcxengine.ErrUnsupported); unsupported {
			// Expected engine decline (e.g. hour-over-daily, F2). Not an alarm —
			// eligibility over-claimed; a tightening signal.
			h.Logger.Warn().Str("shape", d.Shape).Str("sql", sqlutil.ForLog(engineSQL)).
				Msg("arcx shadow: engine declined an eligible shape")
			h.Metrics.ArcxShadowDeclined(d.Shape)
			return
		}
		// Real engine error — alarm.
		h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", sqlutil.ForLog(engineSQL)).
			Msg("arcx shadow: engine ERROR")
		h.Metrics.ArcxShadowError(d.Shape)
		return
	}
	rec, err := drainReaderToRecord(reader, ShadowMaxRows)
	// KeepAlive: the reader (holding FFI-backed arcx buffers) must stay reachable until
	// AFTER the drain reads the last batch — Release is a no-op; the stream frees on a GC
	// finalizer, so an early finalize is a use-after-free.
	runtime.KeepAlive(reader)
	if errors.Is(err, errShadowTruncated) {
		// Too big to compare in memory. A SKIP, not an error and not a mismatch — arcx
		// may well be correct here; we simply declined to hold the result. Alarming would
		// train the operator to ignore shadow alarms.
		h.Logger.Debug().Str("shape", d.Shape).Int("cap", ShadowMaxRows).
			Msg("arcx shadow: skipped, result exceeds the shadow row cap")
		h.Metrics.ArcxShadowSkipped(d.Shape)
		return
	}
	if err != nil {
		h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", sqlutil.ForLog(engineSQL)).
			Msg("arcx shadow: stream drain ERROR")
		h.Metrics.ArcxShadowError(d.Shape)
		return
	}
	arcxMicros := time.Since(arcxStart).Microseconds()
	defer rec.Release()
	h.Metrics.ArcxLatency("arcx", d.Shape, arcxMicros)

	oracleStart := time.Now()
	diff, err := h.compareToOracle(ctx, d, rec)
	oracleMicros := time.Since(oracleStart).Microseconds()
	if err != nil {
		// The oracle itself failed — can't compare. Log, don't alarm arcx for it.
		h.Logger.Warn().Err(err).Str("shape", d.Shape).Msg("arcx shadow: oracle query failed; skipping compare")
		return
	}
	h.Metrics.ArcxLatency("duckdb", d.Shape, oracleMicros)

	if diff != "" {
		h.Logger.Error().
			Str("shape", d.Shape).
			Str("engine_sql", sqlutil.ForLog(engineSQL)).
			Str("oracle_sql", sqlutil.ForLog(h.ConvertedSQL)).
			Str("diff", diff).
			Msg("arcx shadow: MISMATCH vs DuckDB")
		h.Metrics.ArcxShadowMismatch(d.Shape)
		return
	}
	h.Metrics.ArcxShadowMatch(d.Shape)
}

// compareToOracle runs the DuckDB oracle for this query and compares its result to
// arcx's `rec`, returning the diff string ("" == match) or an oracle error. Scalar
// shapes (min/max/count(col)) use the single-cell comparison; count(*)/agg use the
// (bucket, count) canonical form.
func (h Deps) compareToOracle(ctx context.Context, d Decision, rec arrow.Record) (string, error) {
	rows, err := h.DB.QueryContext(ctx, h.ConvertedSQL)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if d.Shape == ShapeScan {
		oracle, err := scanRowsFromRows(rows)
		if err != nil {
			return "", err
		}
		return compareScan(rec, oracle), nil
	}
	if d.Shape == ShapeScanAgg {
		oracle, err := aggFromRows(rows)
		if err != nil {
			return "", err
		}
		return compareAgg(rec, oracle, d.AggItems), nil
	}
	if d.Shape == ShapeScanAggGroupedCount {
		keyCol := 0
		for i, item := range d.AggItems {
			if item != "count(*)" {
				keyCol = i
				break
			}
		}
		oracle, err := groupedFromRows(rows, keyCol)
		if err != nil {
			return "", err
		}
		return compareGroupedCount(rec, oracle, keyCol), nil
	}
	if isScalarShape(d.Shape) {
		oracle, err := scalarFromRows(rows)
		if err != nil {
			return "", err
		}
		return compareScalar(rec, oracle), nil
	}
	oracle, err := canonicalFromRows(rows)
	if err != nil {
		return "", err
	}
	return compareResults(rec, oracle), nil
}

// runServe streams arcx's result for a green shape. Falls back (handled=false) on
// decline; on a real error it alarms and falls back too (Phase 1 keeps DuckDB the
// safety net rather than surfacing arcx errors to users).
func (h Deps) runServe(c *fiber.Ctx, ctx context.Context, d Decision, engineSQL string, start time.Time) bool {
	reader, err := arcxengine.QueryStream(engineSQL, d.Ctx)
	if err != nil {
		if _, unsupported := err.(arcxengine.ErrUnsupported); unsupported {
			return false // silent fallback — normal router contract
		}
		h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", sqlutil.ForLog(engineSQL)).
			Msg("arcx serve: engine ERROR; falling back to DuckDB")
		h.Metrics.ArcxShadowError(d.Shape)
		return false
	}
	if h.ServeStream == nil {
		// No streamer wired (shadow-only build/config) — release the reader (its
		// Release is a no-op; the stream frees on GC finalizer) and fall back.
		reader.Release()
		return false
	}
	// ServeStream owns the reader across the async wire writer: it must
	// runtime.KeepAlive(reader) until AFTER the last batch is encoded, because the
	// imported reader's Release is a no-op and the FFI-backed batches free on a GC
	// finalizer — a reader that goes unreachable mid-encode is the design doc's FFI
	// use-after-free cliff. The router does NOT defer-release here; the reader's
	// lifetime is the streamer's, held for the in-flight response.
	return h.ServeStream(c, reader, start)
}
