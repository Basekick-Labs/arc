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
	"strings"
	"time"

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
	Eligible bool
	Ctx      arcxengine.Context
	Shape    string
	Unit     string // "" for count_star
}

// Decide is the cheap per-query pre-filter. It never calls the engine; it only
// recognizes the shape (string pass) and resolves the single measurement.
func Decide(sql, headerDB string, h Handler) Decision {
	// Mode gate first: off is a true zero-cost kill switch (H2).
	if h.Mode == ModeOff {
		return Decision{}
	}
	shape, unit, measToken, ok := eligibleShape(sql)
	if !ok {
		return Decision{}
	}
	// The recognizer already captured the single FROM token; split it into
	// (database, measurement) and fold headerDB for the bare form — mirroring
	// checkQueryPermissions' resolution (query.go:1220), self-contained so we
	// don't need Arc's unexported extractTableReferences.
	database, measurement, ok := resolveMeasurementToken(measToken, headerDB)
	if !ok {
		return Decision{}
	}
	return Decision{
		Eligible: true,
		Ctx: arcxengine.Context{
			Database:    database,
			Measurement: measurement,
			TimeColumn:  timeColumn,
		},
		Shape: shape,
		Unit:  unit,
	}
}

// Run executes the decision. In shadow it returns false (caller serves DuckDB);
// in serve it returns true when it streamed an arcx result.
func Run(c *fiber.Ctx, d Decision, h Handler, mode Mode) (handled bool) {
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
		return h.runServe(c, ctx, d, engineSQL)
	default:
		return false
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
	default:
		return "", false
	}
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
	oracle, err := h.fetchOracle(ctx)
	oracleMicros := time.Since(oracleStart).Microseconds()
	if err != nil {
		// The oracle itself failed — can't compare. Log, don't alarm arcx for it.
		h.Logger.Warn().Err(err).Str("shape", d.Shape).Msg("arcx shadow: oracle query failed; skipping compare")
		return
	}
	h.Metrics.ArcxLatency("duckdb", d.Shape, oracleMicros)

	if diff := compareResults(rec, oracle); diff != "" {
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

// runServe streams arcx's result for a green shape. Falls back (handled=false) on
// decline; on a real error it alarms and falls back too (Phase 1 keeps DuckDB the
// safety net rather than surfacing arcx errors to users).
func (h Deps) runServe(c *fiber.Ctx, ctx context.Context, d Decision, engineSQL string) bool {
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
	return streamArcxResult(c, rec)
}

// fetchOracle runs the DuckDB-correct converted SQL and drains it into a
// canonical result for comparison. Tiny shapes → cheap full scan.
func (h Deps) fetchOracle(ctx context.Context) (canonicalResult, error) {
	rows, err := h.DB.QueryContext(ctx, h.ConvertedSQL)
	if err != nil {
		return canonicalResult{}, err
	}
	defer rows.Close()
	return canonicalFromRows(rows)
}
