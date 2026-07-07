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
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
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
	// the router just hands back the record. `start` is when the query began
	// (captured BEFORE the engine ran) so the response's execution_time_ms covers
	// the engine work, not just serialization. Returns true if it served the
	// response. nil in shadow-only wiring (serve mode then falls back to DuckDB).
	ServeStream func(c *fiber.Ctx, rec arrow.Record, start time.Time) bool
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
	WhereText string         // scan only: re-serialized boolean WHERE (OR/parens, 2b-2)
	OrderBy   []scanOrderKey // scan only: ORDER BY keys
	Limit     int            // scan only: LIMIT n (0 = none)
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
		h.runShadow(ctx, d, engineSQL)
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
func RunArrow(ctx context.Context, d Decision, h Handler, mode Mode) (rec arrow.Record, served bool) {
	if !d.Eligible || mode == ModeOff {
		return nil, false
	}
	engineSQL, ok := h.buildEngineSQL(ctx, d)
	if !ok {
		return nil, false
	}
	switch mode {
	case ModeShadow:
		h.runShadow(ctx, d, engineSQL)
		return nil, false // DuckDB serves in shadow
	case ModeServe:
		r, err := arcxengine.Query(engineSQL, d.Ctx)
		if err != nil {
			if _, unsupported := err.(arcxengine.ErrUnsupported); !unsupported {
				h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", engineSQL).
					Msg("arcx serve (arrow): engine ERROR; falling back to DuckDB")
				h.Metrics.ArcxShadowError(d.Shape)
			}
			return nil, false
		}
		return r, true // caller owns r, must Release after streaming
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
		// derived column name. Column is the "time" convention (F1).
		return "SELECT date_trunc('" + escapeStringLiteral(d.Unit) + "', time), count(*) FROM read_parquet(" +
			arr.String() + ") GROUP BY 1", true
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
	default:
		return "", false
	}
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
		if !isBareIdent(col) {
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
				// DOUBLE-eq literal — validate the `digit.digit` shape and reject
				// `±0.0` again at emit time (defense in depth vs a hand-built
				// Decision); the op was constrained to `=`/`!=` at match time.
				if !isFloatLiteral(p.num) || isZeroFloatLiteral(p.num) {
					return "", false
				}
				if p.op != "=" && p.op != "!=" {
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

// runShadow runs arcx and a cheap DuckDB oracle, compares, and records metrics.
// It never serves — the caller's DuckDB dispatch does. Synchronous for the first
// cut (immediate, deterministic correctness signal).
func (h Deps) runShadow(ctx context.Context, d Decision, engineSQL string) {
	arcxStart := time.Now()
	rec, err := arcxengine.Query(engineSQL, d.Ctx)
	arcxMicros := time.Since(arcxStart).Microseconds()
	if err != nil {
		if _, unsupported := err.(arcxengine.ErrUnsupported); unsupported {
			// Expected engine decline (e.g. hour-over-daily, F2). Not an alarm —
			// eligibility over-claimed; a tightening signal.
			h.Logger.Warn().Str("shape", d.Shape).Str("sql", engineSQL).
				Msg("arcx shadow: engine declined an eligible shape")
			h.Metrics.ArcxShadowDeclined(d.Shape)
			return
		}
		// Real engine error — alarm.
		h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", engineSQL).
			Msg("arcx shadow: engine ERROR")
		h.Metrics.ArcxShadowError(d.Shape)
		return
	}
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
			Str("engine_sql", engineSQL).
			Str("oracle_sql", h.ConvertedSQL).
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
	rec, err := arcxengine.Query(engineSQL, d.Ctx)
	if err != nil {
		if _, unsupported := err.(arcxengine.ErrUnsupported); unsupported {
			return false // silent fallback — normal router contract
		}
		h.Logger.Error().Err(err).Str("shape", d.Shape).Str("sql", engineSQL).
			Msg("arcx serve: engine ERROR; falling back to DuckDB")
		h.Metrics.ArcxShadowError(d.Shape)
		return false
	}
	defer rec.Release()
	if h.ServeStream == nil {
		// No streamer wired (shadow-only build/config) — fall back to DuckDB.
		return false
	}
	// ServeStream streams the record to the response. It MUST Retain the record
	// if it defers work past return (the async body-stream writer does), because
	// this defer releases our reference as soon as Run returns. Owning the
	// buffer lifetime across the async boundary is the memory-safety cliff the
	// design doc warns about — the streamer, not the router, holds it while the
	// response is in flight.
	return h.ServeStream(c, rec, start)
}
