//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Prepared statements without parameters (#1645), against the real query
// handler behind Flight SQL.

type preparedRigOptions struct {
	rbac           RBACChecker
	maxConnections int
	// policy, when set, is the token's governance policy; its TokenID is
	// filled in.
	policy *governance.Policy
	router *cluster.Router
	// storedDecimals adds sample.dec, one row of a DECIMAL(38,0) "big", a
	// DECIMAL(18,0) "small" and a DECIMAL(10,2) "frac", as a measurement
	// written with ingest.decimal_columns stores them (#1784).
	storedDecimals bool
}

type preparedRig struct {
	client     *flightsql.Client
	ctx        context.Context
	handler    *QueryHandler
	governance *governance.Manager
	tokenID    int64
	registry   *queryregistry.Registry
	bridge     *bridgeRecorder
}

// bridgeRecorder records what reaches the bridge's routes, with the audit
// detail the handler set: it runs where cmd/arc puts the audit middleware.
type bridgeRecorder struct {
	mu       sync.Mutex
	requests []bridgeRequest
}

type bridgeRequest struct {
	path   string
	detail map[string]string
	// decimals is every x-arc-decimals value the request carried.
	decimals []string
}

func (r *bridgeRecorder) middleware(c *fiber.Ctx) error {
	err := c.Next()
	detail, _ := c.Locals(audit.DetailLocalsKey).(map[string]string)
	r.mu.Lock()
	var decimals []string
	for _, v := range c.Request().Header.PeekAll(decimalsHeader) {
		decimals = append(decimals, string(v))
	}
	r.requests = append(r.requests, bridgeRequest{path: utils.CopyString(c.Path()), detail: detail, decimals: decimals})
	r.mu.Unlock()
	return err
}

func (r *bridgeRecorder) count(path string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, req := range r.requests {
		if req.path == path {
			n++
		}
	}
	return n
}

func (r *bridgeRecorder) all() []bridgeRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bridgeRequest(nil), r.requests...)
}

// newPreparedRig serves one measurement, sample.cpu, through Flight SQL and
// returns a client authenticated with a read token.
func newPreparedRig(t *testing.T, opts preparedRigOptions) *preparedRig {
	t.Helper()
	logger := zerolog.Nop()
	storageRoot := t.TempDir()
	backend, err := storage.NewLocalBackend(storageRoot, logger)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buffer.Close() })
	require.NoError(t, buffer.WriteTypedColumnarDirect(context.Background(), "sample", "cpu", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{time.Now().UTC().UnixMicro()},
			"value": []float64{42},
		},
	}, 1))
	require.NoError(t, buffer.FlushAllAndWait(context.Background()))

	maxConnections := opts.maxConnections
	if maxConnections == 0 {
		maxConnections = 4
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: maxConnections, LocalStorageRoot: storageRoot}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	if opts.storedDecimals {
		dir := filepath.Join(storageRoot, "sample", "dec", "2026", "11", "26", "10")
		require.NoError(t, os.MkdirAll(dir, 0o700))
		_, err := duck.Exec("COPY (SELECT TIMESTAMP '2026-11-26 10:00:00' AS time, " +
			"100000000000000000000::DECIMAL(38,0) AS big, 5::DECIMAL(18,0) AS small, 1.5::DECIMAL(10,2) AS frac) TO '" +
			strings.ReplaceAll(filepath.Join(dir, "dec.parquet"), "'", "''") + "' (FORMAT PARQUET)")
		require.NoError(t, err)
	}
	authManager, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, logger)
	require.NoError(t, err)
	t.Cleanup(func() { authManager.Close() })
	token, err := authManager.CreateToken(context.Background(), "flight-prepared", "", "read", nil)
	require.NoError(t, err)
	tokenInfo := authManager.VerifyToken(token)
	require.NotNil(t, tokenInfo)

	queryHandler := NewQueryHandler(duck, backend, logger, 0, 0)
	queryHandler.SetAuthAndRBAC(authManager, opts.rbac)
	registry := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 32}, logger)
	queryHandler.SetQueryRegistry(registry)
	rig := &preparedRig{handler: queryHandler, tokenID: tokenInfo.ID, registry: registry, bridge: &bridgeRecorder{}}
	if opts.policy != nil {
		policy := *opts.policy
		policy.TokenID = tokenInfo.ID
		rig.governance = newGovernanceTestManager(t, &policy)
		queryHandler.governanceManager = rig.governance
		origLicensed := queryGovernanceLicensed
		queryGovernanceLicensed = func(*QueryHandler) bool { return true }
		t.Cleanup(func() { queryGovernanceLicensed = origLicensed })
	}
	if opts.router != nil {
		queryHandler.SetRouter(opts.router)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	server, err := NewFlightSQLServer(queryHandler, FlightSQLServerConfig{
		Host: "127.0.0.1", Port: port, Middleware: []fiber.Handler{rig.bridge.middleware},
	}, logger)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})

	rig.client, err = flightsql.NewClient(fmt.Sprintf("127.0.0.1:%d", port), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { rig.client.Client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	rig.ctx, err = rig.client.Client.AuthenticateBasicToken(ctx, "", token)
	require.NoError(t, err)
	return rig
}

func TestFlightSQLDoGetMapsStatementErrorToInvalidArgument(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	info, err := rig.client.Execute(rig.ctx, `SELECT missing_column FROM "sample"."cpu"`)
	require.NoError(t, err)
	require.NotEmpty(t, info.Endpoint)

	_, err = rig.client.DoGet(rig.ctx, info.Endpoint[0].Ticket)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Contains(t, err.Error(), "Binder Error")
}

// prepareAndRun prepares sql, executes it and reads the whole result,
// returning the prepared schema, the result's schema and its row count.
func (r *preparedRig) prepareAndRun(t *testing.T, sql string) (prepared, result *arrow.Schema, rows int64) {
	t.Helper()
	ps, err := r.client.Prepare(r.ctx, sql)
	require.NoError(t, err)
	defer func() { require.NoError(t, ps.Close(r.ctx)) }()
	isUpdate, ok := ps.IsUpdate()
	require.True(t, ok, "the server must say whether the statement is an update")
	require.False(t, isUpdate)
	info, err := ps.Execute(r.ctx)
	require.NoError(t, err)
	reader, err := r.client.DoGet(r.ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	require.NoError(t, reader.Err())
	return ps.DatasetSchema(), reader.Schema(), rows
}

// One execute of a prepared statement is one Arc query: one Arrow query
// request on the bridge (the data.query audit record), one registry entry,
// one governance admission. The prepare's bind is audited under its own
// operation and counts as none of them.
func TestFlightSQLPreparedStatementIsOneQuery(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{policy: &governance.Policy{RateLimitPerMinute: 100}})

	// A prepare alone moves no query metric.
	queryMetrics := func() map[string]interface{} {
		snapshot := metrics.Get().Snapshot()
		out := map[string]interface{}{}
		for _, key := range []string{"query_requests_total", "query_success_total", "query_errors_total", "query_rows_total"} {
			out[key] = snapshot[key]
		}
		return out
	}
	before := queryMetrics()
	ps, err := rig.client.Prepare(rig.ctx, `SELECT time, value FROM "sample"."cpu"`)
	require.NoError(t, err)
	require.NoError(t, ps.Close(rig.ctx))
	require.Equal(t, before, queryMetrics())
	require.Zero(t, rig.registry.HistoryLen())
	require.Zero(t, rig.bridge.count("/api/v1/query/arrow"))

	prepared, result, rows := rig.prepareAndRun(t, `SELECT time, value FROM "sample"."cpu"`)
	require.EqualValues(t, 1, rows)
	require.Equal(t, result.String(), prepared.String())

	require.Equal(t, 1, rig.bridge.count("/api/v1/query/arrow"), "Arrow queries on the bridge")
	require.Equal(t, 2, rig.bridge.count(flightSQLSchemaPath), "binds on the bridge: the bare prepare's and this one's")
	for _, req := range rig.bridge.all() {
		switch req.path {
		case flightSQLSchemaPath:
			require.Equal(t, map[string]string{"operation": "flight_sql.prepare"}, req.detail)
		default:
			require.Nil(t, req.detail, "%s carries the prepare's audit detail", req.path)
		}
	}
	require.Eventually(t, func() bool { return rig.registry.HistoryLen() == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, 0, rig.registry.ActiveCount())
	require.Equal(t, 99, rig.governance.GetTokenUsage(rig.tokenID).RateLimitRemainingPerMin, "governance admissions")
}

func TestFlightSQLPreparedExactDecimalModeIsStable(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	ctx := metadata.AppendToOutgoingContext(rig.ctx, decimalsHeader, decimalModeExact)
	ps, err := rig.client.Prepare(ctx, "SELECT 100000000000000000000::HUGEINT AS v")
	require.NoError(t, err)
	defer func() { require.NoError(t, ps.Close(ctx)) }()
	preparedSchema := ps.DatasetSchema()
	require.Equal(t, &arrow.Decimal128Type{Precision: 38, Scale: 0}, preparedSchema.Field(0).Type)

	info, err := ps.Execute(ctx)
	require.NoError(t, err)
	reader, err := rig.client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	require.Equal(t, preparedSchema.String(), reader.Schema().String())
	_, ok := reader.RecordBatch().Column(0).(*array.Decimal128)
	require.True(t, ok)
	require.NoError(t, reader.Err())

	_, err = ps.Execute(rig.ctx)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "executing a prepared exact statement in default mode must be refused: %v", err)
}

// The prepared schema is the schema DoGet streams: Arc's decimal
// normalisation, duplicate names, nested types, JSON and the NULL type
// included, and a type only DESCRIBE can spell.
func TestFlightSQLPreparedSchemaMatchesTheResult(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	for _, sql := range []string{
		`SELECT * FROM "sample"."cpu"`,
		`SELECT count(*) AS n, sum(1) AS s, avg(value) AS mean, 1.5::DECIMAL(10,2) AS d FROM "sample"."cpu"`,
		`SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, max(value) AS v FROM "sample"."cpu" GROUP BY 1`,
		`SELECT 1 AS a, 'x' AS a, [1, 2] AS l, {'k': 'v'} AS s, '{}'::JSON AS j, NULL AS n, TIMESTAMPTZ '2024-01-01 00:00:00+00' AS ts`,
		`SELECT union_value(k := 1) AS u`,
		`SELECT value FROM "sample"."cpu"; `,
		"SELECT value FROM \"sample\".\"cpu\" -- trailing comment",
	} {
		t.Run(sql, func(t *testing.T) {
			prepared, result, _ := rig.prepareAndRun(t, sql)
			require.Equal(t, result.String(), prepared.String())
			ps, err := rig.client.Prepare(rig.ctx, sql)
			require.NoError(t, err)
			defer ps.Close(rig.ctx)
			schema, err := ps.GetSchema(rig.ctx)
			require.NoError(t, err)
			got, err := flight.DeserializeSchema(schema.GetSchema(), nil)
			require.NoError(t, err)
			require.Equal(t, result.String(), got.String(), "GetSchema")
		})
	}
}

// A measurement with no data yet has an empty result, whose schema has no
// columns; it is still a query.
func TestFlightSQLPreparedEmptyMeasurement(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	prepared, result, rows := rig.prepareAndRun(t, `SELECT * FROM "sample"."nothing_here"`)
	require.Zero(t, rows)
	require.Zero(t, prepared.NumFields())
	require.Equal(t, result.String(), prepared.String())
}

// SHOW has a schema Arc knows: prepared without a bind, executed as a SHOW.
func TestFlightSQLPreparedShowStatements(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	for _, sql := range []string{"SHOW DATABASES", `SHOW TABLES FROM "sample"`} {
		t.Run(sql, func(t *testing.T) {
			prepared, result, rows := rig.prepareAndRun(t, sql)
			require.NotZero(t, rows)
			require.Equal(t, result.String(), prepared.String())
		})
	}
	require.Zero(t, rig.bridge.count(flightSQLSchemaPath), "SHOW was bound")
}

func TestFlightSQLPreparedStatementRefusals(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	for _, tc := range []struct {
		name, sql string
		code      codes.Code
	}{
		{"statement-leading PIVOT", `PIVOT "sample"."cpu" ON value USING count(*)`, codes.Unimplemented},
		{"PIVOT in a FROM clause", `SELECT * FROM (PIVOT "sample"."cpu" ON value USING count(*)) p`, codes.Unimplemented},
		{"PIVOT glued to a quoted name", `WITH "c" AS (SELECT * FROM "sample"."cpu") SELECT * FROM (PIVOT"c" ON value USING count(*))`, codes.Unimplemented},
		{"unnamed struct", `SELECT (1, 2) AS s`, codes.Unimplemented},
		{"unnamed struct from row()", `SELECT row(1, 'a') AS s`, codes.Unimplemented},
		{"parameters", `SELECT value FROM "sample"."cpu" WHERE value > ?`, codes.InvalidArgument},
		{"an unknown column", `SELECT no_such_column FROM "sample"."cpu"`, codes.InvalidArgument},
		{"a write", `DELETE FROM "sample"."cpu"`, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := rig.client.Prepare(rig.ctx, tc.sql)
			require.Equal(t, tc.code, status.Code(err), "%v", err)
		})
	}
	// None of them reached the query path.
	require.Zero(t, rig.bridge.count("/api/v1/query/arrow"))
	require.Zero(t, rig.registry.HistoryLen())

	// The PIVOT refusal names a form a JDBC client, which cannot run a
	// statement unprepared, can send.
	_, err := rig.client.Prepare(rig.ctx, `PIVOT "sample"."cpu" ON value USING count(*)`)
	require.Contains(t, status.Convert(err).Message(), "FILTER (WHERE")
}

// A bind is 400 only when the statement is at fault. Storage that could not
// be read is 503, so a client does not report the user's SQL as wrong, with
// a message that names no storage path; a failure nothing classifies is 500.
func TestFlightSQLBindFailureStatus(t *testing.T) {
	h := NewQueryHandler(nil, nil, zerolog.Nop(), 0, 0)
	ctx := context.Background()
	const path = "s3://bucket/sample/cpu/2026/11/20/10/x.parquet"
	for _, tc := range []struct {
		name string
		err  error
		code int
	}{
		{"the statement", fmt.Errorf("%w: Binder Error: no column", database.ErrSchemaBind), http.StatusBadRequest},
		{"parameters", database.ErrSchemaParameters, http.StatusBadRequest},
		{"storage", fmt.Errorf("%w: HTTP Error: reading '%s' (HTTP 503)", database.ErrSchemaStorage, path), http.StatusServiceUnavailable},
		{"memory", fmt.Errorf("%w: Out of Memory Error: x", database.ErrSchemaMemory), http.StatusServiceUnavailable},
		{"cannot be described", fmt.Errorf("%w: x", database.ErrSchemaUnsupported), http.StatusNotImplemented},
		{"a damaged file", fmt.Errorf("bind: Invalid Input Error: No magic bytes found at end of file '%s'", path), http.StatusInternalServerError},
		{"unclassified", fmt.Errorf("bind: Internal Error: x"), http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, code, message := h.flightSQLBindFailure(ctx, ctx, tc.err)
			require.Equal(t, tc.code, code)
			require.NotContains(t, message, path)
		})
	}
}

// A token that may not read a table gets no schema for it.
func TestFlightSQLPreparedStatementIsCheckedByRBAC(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{rbac: &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"default": true}, deniedReason: "no read permission"}})
	ps, err := rig.client.Prepare(rig.ctx, `SELECT * FROM "sample"."cpu"`)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Nil(t, ps)
}

// The prepare asks governance without counting: it succeeds with one query
// left and leaves it for the execute, and is refused once the token's queries
// would be.
func TestFlightSQLPreparedStatementGovernance(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{policy: &governance.Policy{RateLimitPerMinute: 1}})
	const sql = `SELECT value FROM "sample"."cpu"`

	ps, err := rig.client.Prepare(rig.ctx, sql)
	require.NoError(t, err)
	require.Equal(t, 1, rig.governance.GetTokenUsage(rig.tokenID).RateLimitRemainingPerMin, "the prepare spent the query")
	info, err := ps.Execute(rig.ctx)
	require.NoError(t, err)
	reader, err := rig.client.DoGet(rig.ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	for reader.Next() {
	}
	require.NoError(t, reader.Err())
	reader.Release()
	require.NoError(t, ps.Close(rig.ctx))
	require.Zero(t, rig.governance.GetTokenUsage(rig.tokenID).RateLimitRemainingPerMin)

	_, err = rig.client.Prepare(rig.ctx, sql)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "%v", err)
}

// The bind and the schema query share one pool connection: a pool of one
// prepares and executes.
func TestFlightSQLPreparedStatementOnASingleConnectionPool(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{maxConnections: 1})
	for _, sql := range []string{`SELECT * FROM "sample"."cpu"`, `SELECT union_value(k := 1) AS u`} {
		prepared, result, _ := rig.prepareAndRun(t, sql)
		require.Equal(t, result.String(), prepared.String())
	}
	_, err := rig.client.Prepare(rig.ctx, `SELECT (1, 2) AS s`)
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
}

// A node that does not run queries prepares nothing, SHOW included: the
// client then runs the statement unprepared, and it is forwarded.
func TestFlightSQLPreparedStatementOnANodeThatDoesNotServeQueries(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{router: arrowForwardRouter(t, "http://127.0.0.1:1", cluster.RoleCompactor, true)})
	for _, sql := range []string{`SELECT 1`, "SHOW DATABASES"} {
		_, err := rig.client.Prepare(rig.ctx, sql)
		require.Equal(t, codes.Unimplemented, status.Code(err), "%s: %v", sql, err)
	}
	require.Zero(t, rig.bridge.count(flightSQLSchemaPath))
}

// The schema route exists on the bridge only.
func TestFlightSQLSchemaRouteIsNotOnThePublicApp(t *testing.T) {
	// A real database, so that the route, were it registered here, would
	// answer the request rather than panic: the failure is the status code.
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: t.TempDir()}, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	qh := NewQueryHandler(duck, nil, zerolog.Nop(), 0, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	qh.RegisterRoutes(app)
	req := httptest.NewRequest(http.MethodPost, flightSQLSchemaPath, strings.NewReader(`{"sql":"SELECT 1"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	s := newBridgeTestServer(t, nil)
	var onBridge bool
	for _, route := range s.bridge.GetRoutes() {
		onBridge = onBridge || (route.Method == http.MethodPost && route.Path == flightSQLSchemaPath)
	}
	require.True(t, onBridge, "the schema route is not on the bridge")
}

type preparedHandle []byte

func (h preparedHandle) GetPreparedStatementHandle() []byte { return h }

// The handle is the client's to send back: it is checked like a ticket, and
// executing it is executing a statement.
func TestFlightSQLPreparedStatementHandles(t *testing.T) {
	s := newBridgeTestServer(t, nil).service
	ctx := context.Background()

	for _, handle := range []string{`not json`, `{"v":2,"sql":"SELECT 1"}`, `{"sql":"SELECT 1"}`, `{"v":1,"sql":"SELECT 1","database":"../x"}`} {
		_, err := s.GetFlightInfoPreparedStatement(ctx, preparedHandle(handle), nil)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%s: %v", handle, err)
		_, _, err = s.DoGetPreparedStatement(ctx, preparedHandle(handle))
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%s: %v", handle, err)
	}
	_, err := s.GetFlightInfoPreparedStatement(ctx, preparedHandle(`{"v":1,"sql":"DROP TABLE x"}`), nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	info, err := s.GetFlightInfoPreparedStatement(ctx, preparedHandle(`{"v":1,"sql":"SELECT 1","database":"sample"}`), nil)
	require.NoError(t, err)
	ticket, err := flightsql.GetStatementQueryTicket(info.Endpoint[0].Ticket)
	require.NoError(t, err)
	var statement flightStatement
	require.NoError(t, json.Unmarshal(ticket.GetStatementHandle(), &statement))
	require.Equal(t, flightStatement{SQL: "SELECT 1", Database: "sample"}, statement)

	other := metadata.NewIncomingContext(ctx, metadata.Pairs(flightSQLDatabaseHeader, "other"))
	_, err = s.GetFlightInfoPreparedStatement(other, preparedHandle(`{"v":1,"sql":"SELECT 1","database":"sample"}`), nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	require.NoError(t, s.ClosePreparedStatement(ctx, preparedHandle(`{"v":1,"sql":"SELECT 1"}`)))
	_, err = s.DoPutPreparedStatementQuery(ctx, preparedHandle(`{"v":1,"sql":"SELECT 1"}`), nil, nil)
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
	_, err = s.DoPutPreparedStatementUpdate(ctx, preparedHandle(`{"v":1,"sql":"SELECT 1"}`), nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	_, err = s.CreatePreparedStatement(ctx, preparedRequest{query: "SELECT 1", transaction: []byte("tx")})
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
}

type preparedRequest struct {
	query       string
	transaction []byte
}

func (r preparedRequest) GetQuery() string         { return r.query }
func (r preparedRequest) GetTransactionId() []byte { return r.transaction }

func TestHasFlightPivot(t *testing.T) {
	for sql, want := range map[string]bool{
		`PIVOT cpu ON host USING sum(value)`:                      true,
		`pivot_wider cpu ON host`:                                 true,
		`SELECT * FROM (PIVOT cpu ON host) p`:                     true,
		`WITH p AS (PIVOT cpu ON host) SELECT * FROM p`:           true,
		`SELECT * FROM cpu PIVOT (sum(v) FOR host IN ('a', 'b'))`: true,
		`UNPIVOT cpu ON a, b INTO NAME n VALUE v`:                 false,
		`SELECT * FROM cpu UNPIVOT (v FOR n IN (a, b))`:           false,
		`PIVOT_LONGER cpu ON a, b INTO NAME n VALUE v`:            false,
		`SELECT 'PIVOT' AS x`:                                     false,
		`SELECT "pivot" FROM cpu`:                                 false,
		`SELECT 1 -- PIVOT`:                                       false,
		`SELECT /* PIVOT */ 1`:                                    false,
		`SELECT pivots FROM cpu`:                                  false,
		// Glued to a quoted identifier, a literal or a comment, as DuckDB
		// still parses them.
		`WITH "c" AS (SELECT 1) SELECT * FROM (PIVOT"c" ON host USING sum(i))`: true,
		`SELECT * FROM "x"PIVOT (sum(v) FOR host IN ('a'))`:                    true,
		`SELECT * FROM cpu PIVOT('x')`:                                         true,
		`PIVOT/**/cpu ON host`:                                                 true,
		`SELECT "PIVOT"x FROM cpu`:                                             false,
		`SELECT 'a'PIVOTS FROM cpu`:                                            false,
	} {
		require.Equal(t, want, hasFlightPivot(sql), sql)
	}
}
