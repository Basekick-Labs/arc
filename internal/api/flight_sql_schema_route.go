//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"errors"
	"github.com/basekick-labs/arc/internal/readgate"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/gofiber/fiber/v2"
)

// registerFlightSQLSchemaRoute registers flightSQLSchemaPath on the Flight SQL
// bridge, behind the door the query routes have (read auth on the handler's
// lifetime, then the catch-up gate). It is never registered on the main app:
// RegisterRoutes does not know it.
//
// Binds are limited to a quarter of the DuckDB pool, at least one, across the
// process: a client that prepares in a loop must not take every connection
// the queries need.
func (h *QueryHandler) registerFlightSQLSchemaRoute(bridge *fiber.App) {
	binds := make(chan struct{}, flightSQLBindSlots(h.db))
	readAuth := h.bindQueryLifetime(withAnyResourceReadAuth(h.authManager, h.rbacManager))
	bridge.Post(flightSQLSchemaPath, readAuth, h.checkReplicationReady, func(c *fiber.Ctx) error {
		return h.describeFlightSQLSchema(c, binds)
	})
}

// flightSQLBindSlots is how many prepare binds may run at once:
// database.max_connections / 4, at least one.
func flightSQLBindSlots(db *database.DuckDB) int {
	if db == nil {
		return 1
	}
	return max(1, db.Stats().MaxOpenConnections/4)
}

// describeFlightSQLSchema answers the result schema of a statement a Flight
// SQL client prepares, as a schema-only Arrow IPC stream, without running it.
//
// It makes executeQueryArrow's checks up to execution, in the same order, by
// calling the same helpers: the forward decision, SQL validation, the
// x-arc-database header, RBAC on every table, governance, the deadline, the
// transform, RBAC on the tables the transform emitted. Governance is asked
// without counting (Manager.Peek): the execute that follows is the query, and
// it is counted then. Nothing here is a query either: no registry entry, no
// query metrics, no slow-query log; the bind has its own metrics.
func (h *QueryHandler) describeFlightSQLSchema(c *fiber.Ctx, binds chan struct{}) error {
	c.Locals(audit.DetailLocalsKey, map[string]string{"operation": "flight_sql.prepare"})

	// A node that does not serve queries forwards them, which a bind cannot
	// be: 501, so the client runs the statement unprepared and it is
	// forwarded. CreatePreparedStatement refuses before getting here.
	if QueryForwardDecision(h.router, c) != ForwardLocal {
		return flightSQLSchemaRefusal(c, fiber.StatusNotImplemented, "prepared statements are not served by a node that does not run queries")
	}

	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
	}
	if err := ValidateSQLRequest(req.SQL); err != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusBadRequest, err.Error())
	}
	headerDB := c.Get(flightSQLDatabaseHeader)
	if err := validateHeaderDatabase(headerDB); err != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusBadRequest, "invalid x-arc-database header: "+err.Error())
	}
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		return flightSQLSchemaRefusal(c, fiber.StatusBadRequest, "Cross-database queries (db.table syntax) not allowed when x-arc-database header is set")
	}
	// Arc answers SHOW itself, with a schema CreatePreparedStatement knows.
	if isFlightShowSQL(req.SQL) {
		return flightSQLSchemaRefusal(c, fiber.StatusBadRequest, "SHOW statements are not described by binding")
	}
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusForbidden, err.Error())
	}
	governanceTimeout, governanceRejection := h.peekQueryGovernance(c)
	if governanceRejection != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusTooManyRequests, governanceRejection.reason)
	}

	effectiveTimeout := h.queryTimeout
	if governanceTimeout > 0 {
		effectiveTimeout = governanceTimeout
	}
	deadline := requestDeadline(effectiveTimeout)
	// The bridge's base context: shutdown cancels it.
	parent := c.UserContext()
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline.IsZero() {
		ctx, cancel = context.WithCancel(parent)
	} else {
		ctx, cancel = context.WithDeadline(parent, deadline)
	}
	// Stopped if this node loses its cluster read lease (readgate.Bind).
	// Bound before the read enters (below, after its slot): the epoch now
	// is at or before the one it enters in.
	cancel = h.reads.Bind(readgate.Entry{Epoch: h.reads.Epoch()}, ctx, cancel)
	defer cancel()
	// Always on, whatever query.cancel_on_client_disconnect says: the client
	// is the Flight side of the bridge, which closes the request when its RPC
	// ends. Started before the wait for a slot, so a client that gives up
	// waiting frees its place in the queue.
	watchClientDisconnect(ctx, c.Context().Conn(), cancel)

	// Counted from here, so a prepare that ends waiting for a slot or in the
	// transform is in the bind metrics too, with the time it spent. The
	// slow-bind log leaves the wait out: a queued prepare is not a slow one.
	waitStart := time.Now()
	select {
	case binds <- struct{}{}:
		defer func() { <-binds }()
	case <-ctx.Done():
		result, code, message := flightSQLContextEnded(parent, ctx)
		h.recordFlightSQLBind(result, time.Since(waitStart), 0, req.SQL)
		return flightSQLSchemaRefusal(c, code, message)
	}
	waited := time.Since(waitStart)
	start := time.Now()

	// A read in flight until its statement is bound (internal/readgate).
	defer h.reads.Enter()()
	convertedSQL, _, emittedRefs, err := h.getTransformedSQLWithRefs(ctx, req.SQL, headerDB)
	if err != nil {
		if errors.Is(err, errQueryTimedOut) {
			h.logTransformTimeout(req.SQL, effectiveTimeout)
		}
		code := transformErrorStatus(err)
		result := flightSQLTransformResult(code)
		if ctx.Err() != nil {
			result, _, _ = flightSQLContextEnded(parent, ctx)
		}
		h.recordFlightSQLBind(result, waited, time.Since(start), req.SQL)
		return flightSQLSchemaRefusal(c, code, err.Error())
	}
	if err := h.checkEmittedQueryPermissions(c, "read", emittedRefs); err != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusForbidden, err.Error())
	}

	schema, err := h.db.DescribeArrowSchema(ctx, convertedSQL)
	elapsed := time.Since(start)
	if err != nil && ctx.Err() == nil {
		// As executeQueryArrow (classifyNoFiles): a measurement with no
		// data yet has an empty result, whose schema has no columns.
		switch h.classifyNoFiles(err) {
		case noFilesEmpty:
			schema, err = arrow.NewSchema(nil, nil), nil
		case noFilesVanished:
			h.recordFlightSQLBind(metrics.FlightSQLBindFailed, waited, elapsed, req.SQL)
			c.Set(fiber.HeaderRetryAfter, "1")
			return flightSQLSchemaRefusal(c, fiber.StatusServiceUnavailable, vanishedPartitionMessage)
		}
	}
	if err != nil {
		result, code, message := h.flightSQLBindFailure(parent, ctx, err)
		h.recordFlightSQLBind(result, waited, elapsed, req.SQL)
		return flightSQLSchemaRefusal(c, code, message)
	}
	h.recordFlightSQLBind(metrics.FlightSQLBindOK, waited, elapsed, req.SQL)

	// As the Arrow route streams it (DoGet), decimals included.
	if castInfo := normalizeDecimalSchema(schema); castInfo != nil {
		schema = castInfo.schema
	}
	var buf bytes.Buffer
	if err := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(memory.DefaultAllocator)).Close(); err != nil {
		return flightSQLSchemaRefusal(c, fiber.StatusInternalServerError, "encode result schema: "+err.Error())
	}
	c.Set(fiber.HeaderContentType, "application/vnd.apache.arrow.stream")
	return c.Send(buf.Bytes())
}

// flightSQLBindStorageMessage answers a bind that could not read storage.
const flightSQLBindStorageMessage = "the data could not be read from storage while preparing the statement; retry"

// flightSQLBindMemoryMessage answers a bind that ran out of memory.
const flightSQLBindMemoryMessage = "Arc ran out of memory while preparing the statement; retry"

// flightSQLBindFailure maps a DescribeArrowSchema failure to its bind result,
// status and message. A statement that bound but cannot be described is 501,
// never 400: ADBC runs the statement unprepared only after Unimplemented.
// A bind that could not read storage, or ran out of memory, is 503; only a
// failure nothing classifies is 500 (a damaged Parquet file among them), and
// its message is generic, as DuckDB's text names storage paths.
func (h *QueryHandler) flightSQLBindFailure(parent, ctx context.Context, err error) (result string, code int, message string) {
	if ctx.Err() != nil {
		return flightSQLContextEnded(parent, ctx)
	}
	switch {
	case errors.Is(err, database.ErrSchemaParameters):
		return metrics.FlightSQLBindInvalid, fiber.StatusBadRequest, "parameters not supported: Arc prepared statements run as written"
	case errors.Is(err, database.ErrSchemaBind):
		return metrics.FlightSQLBindInvalid, fiber.StatusBadRequest, err.Error()
	case errors.Is(err, database.ErrSchemaStorage):
		// Not the statement's fault, so not 400: ADBC would tell the user
		// their SQL is wrong. DuckDB's text names storage paths, so it is
		// logged, not returned.
		h.logger.Warn().Err(err).Msg("Flight SQL prepare: the bind could not read storage")
		return metrics.FlightSQLBindFailed, fiber.StatusServiceUnavailable, flightSQLBindStorageMessage
	case errors.Is(err, database.ErrSchemaMemory):
		h.logger.Warn().Err(err).Msg("Flight SQL prepare: out of memory during the bind")
		return metrics.FlightSQLBindFailed, fiber.StatusServiceUnavailable, flightSQLBindMemoryMessage
	case errors.Is(err, database.ErrSchemaUnsupported):
		return metrics.FlightSQLBindUnimplemented, fiber.StatusNotImplemented, err.Error()
	default:
		h.logger.Error().Err(err).Msg("Flight SQL prepare: the bind failed")
		return metrics.FlightSQLBindFailed, fiber.StatusInternalServerError, "the statement's result schema could not be read"
	}
}

// flightSQLTransformResult is the bind result of a prepare whose SQL
// transform failed with status code (transformErrorStatus).
func flightSQLTransformResult(code int) string {
	switch code {
	case fiber.StatusBadRequest:
		return metrics.FlightSQLBindInvalid
	case fiber.StatusGatewayTimeout:
		return metrics.FlightSQLBindTimeout
	default:
		return metrics.FlightSQLBindFailed
	}
}

// flightSQLContextEnded is the bind result, status and message of a request
// whose context ended: its deadline (504, the query timeout's answer),
// shutdown (parent cancelled), or the client going away.
func flightSQLContextEnded(parent, ctx context.Context) (result string, code int, message string) {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return metrics.FlightSQLBindTimeout, fiber.StatusGatewayTimeout, "Query timed out"
	case parent.Err() != nil:
		return metrics.FlightSQLBindCancelled, fiber.StatusServiceUnavailable, "Server shutting down"
	default:
		return metrics.FlightSQLBindCancelled, fiber.StatusServiceUnavailable, "Query cancelled"
	}
}

// recordFlightSQLBind counts a prepare and its duration, the wait for a bind
// slot included, and logs one whose work (the SQL transform and the bind,
// without the wait) took query.slow_query_threshold_ms or more.
func (h *QueryHandler) recordFlightSQLBind(result string, waited, worked time.Duration, sql string) {
	metrics.Get().ObserveFlightSQLBind(result, waited+worked)
	if h.slowQueryThreshold > 0 && worked >= h.slowQueryThreshold {
		h.logger.Warn().
			Str("sql", sqlutil.ForLog(sql)).
			Int64("bind_time_ms", worked.Milliseconds()).
			Int64("wait_ms", waited.Milliseconds()).
			Str("result", result).
			Msg("Slow Flight SQL prepare bind")
	}
}

// peekQueryGovernance is checkQueryGovernance without counting the request:
// whether the token's next query would be admitted, and its policy timeout.
// A refusal is not counted or logged as a refused query either; the query it
// stands for has not been sent.
func (h *QueryHandler) peekQueryGovernance(c *fiber.Ctx) (timeout time.Duration, rejection *governanceRejection) {
	if !queryGovernanceLicensed(h) {
		return 0, nil
	}
	tokenInfo := auth.GetTokenInfo(c)
	if tokenInfo == nil {
		return 0, nil
	}
	result := h.governanceManager.Peek(tokenInfo.ID, tokenInfo.Fingerprint)
	if !result.Allowed {
		c.Set(fiber.HeaderRetryAfter, strconv.Itoa(result.RetryAfterSec))
		return 0, &governanceRejection{reason: result.Reason}
	}
	return result.MaxDuration, nil
}

func flightSQLSchemaRefusal(c *fiber.Ctx, code int, message string) error {
	return c.Status(code).JSON(fiber.Map{"success": false, "error": message})
}
