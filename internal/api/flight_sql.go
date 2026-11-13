package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/fips"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type FlightSQLServerConfig struct {
	Host        string
	Port        int
	TLSEnabled  bool
	TLSCertFile string
	TLSKeyFile  string
	// Version is the Arc version GetSqlInfo reports as the server version
	// (FLIGHT_SQL_SERVER_VERSION). Left out of the answer when empty.
	Version string
	// Middleware runs on the internal bridge after authentication, in order,
	// as it does on the main app: cmd/arc/main.go passes the audit
	// middleware here when audit logging is on, so Flight queries are
	// audited like HTTP ones.
	Middleware []fiber.Handler
}

// flightSQLPeerHeader carries the Flight client's address across the bridge
// hop. The bridge reads it as its ProxyHeader, so c.IP() in the query
// registry, the audit log and the request log is the client's address, not
// the loopback one. Only a request holding the internal key gets that far.
const flightSQLPeerHeader = "X-Arc-Flight-Peer"

// flightSQLInternalKeyHeader authenticates the gRPC side to the bridge.
const flightSQLInternalKeyHeader = "X-Arc-Flight-Internal-Key"

// Shutdown stages. The shutdown coordinator gives every hook and component
// one context and skips everything left once it expires, including the
// ingest buffer and WAL flushes, so this hook keeps its own deadline well
// inside it: in-flight queries get flightSQLShutdownGrace to finish and are
// then cancelled; connections still open flightSQLShutdownForce later (a
// client that stopped reading a stream) are closed, and the server gets
// flightSQLShutdownForce more to stop; the bridge then drains for up to
// flightSQLBridgeDrain. Together at most 11 s; Shutdown scales every stage
// down when half of its context's remaining time is less.
// Variables so a test can shorten them.
var (
	flightSQLShutdownGrace = 5 * time.Second
	flightSQLShutdownForce = 2 * time.Second
	flightSQLBridgeDrain   = 2 * time.Second
)

type FlightSQLServer struct {
	bridge     *fiber.App
	bridgeLn   net.Listener
	flight     flight.Server
	flightLn   *flightSQLConnTracker
	service    *flightSQLService
	config     FlightSQLServerConfig
	logger     zerolog.Logger
	serveDone  chan error
	cancelBase context.CancelFunc

	startMu           sync.Mutex
	listening         bool
	serving           bool
	shutdownMu        sync.Mutex
	shutdownDone      chan struct{}
	shutdownErr       error
	shutdownRequested bool
}

type flightSQLService struct {
	flightsql.BaseServer
	queryHandler *QueryHandler
	internalKey  string
	baseURL      string
	client       *http.Client
	// baseCtx is cancelled during shutdown: every bridge request ends with
	// it, which ends the RPCs waiting on them.
	baseCtx context.Context
	// sqlInfo holds the SqlInfo IDs registered with the base server.
	sqlInfo map[uint32]struct{}
}

type flightSQLIdentity struct {
	token string
}

type flightSQLAuth struct {
	manager *auth.AuthManager
}

type flightStatement struct {
	SQL string `json:"sql"`
}

type flightQueryResponse struct {
	Success bool                `json:"success"`
	Columns []string            `json:"columns"`
	Data    [][]json.RawMessage `json:"data"`
	Error   string              `json:"error"`
}

func NewFlightSQLServer(queryHandler *QueryHandler, config FlightSQLServerConfig, logger zerolog.Logger) (*FlightSQLServer, error) {
	if queryHandler == nil {
		return nil, fmt.Errorf("Flight SQL requires a query handler")
	}
	if !database.ArrowEnabled {
		return nil, fmt.Errorf("Flight SQL requires a build with the Arrow query path (-tags=duckdb_arrow)")
	}
	if config.Port < 1 || config.Port > 65535 {
		return nil, fmt.Errorf("Flight SQL port must be between 1 and 65535")
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate internal Flight SQL credential: %w", err)
	}
	internalKey := hex.EncodeToString(secret)
	baseCtx, cancelBase := context.WithCancel(context.Background())

	// The bridge serves the query handler's routes to the gRPC side only.
	// Its middleware mirrors the main app's (internal/api/server.go,
	// cmd/arc/main.go) for what a query needs: recover first, so a handler
	// panic is a failed RPC and not a dead process; then the key; then
	// request logging, auth and the configured middleware (audit). All of
	// it is registered before the routes: Fiber runs a Use added after a
	// route that answers without calling Next for nothing.
	bridge := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ProxyHeader:           flightSQLPeerHeader,
		// A query body is one JSON-escaped SQL string; escaping can grow
		// it several times over (Fiber's default is 4 MiB).
		BodyLimit: 64 << 20,
	})
	bridge.Use(recover.New(recover.Config{EnableStackTrace: true}))
	bridge.Use(func(c *fiber.Ctx) error {
		if subtle.ConstantTimeCompare([]byte(c.Get(flightSQLInternalKeyHeader)), []byte(internalKey)) != 1 {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		// Forwarding copies every request header to a cluster peer
		// (routing.go); the key never leaves this process.
		c.Request().Header.Del(flightSQLInternalKeyHeader)
		return c.Next()
	})
	bridge.Use(requestLogger(logger))
	bridge.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: queryHandler.authManager}))
	// Handlers hand c.UserContext() to DuckDB. On the bridge it is the
	// server's base context, so shutdown cancels the query itself and not
	// only the loopback request carrying it. Not cancelled per request: the
	// Arrow path streams after the handler returns.
	bridge.Use(func(c *fiber.Ctx) error {
		c.SetUserContext(baseCtx)
		return c.Next()
	})
	for _, m := range config.Middleware {
		if m != nil {
			bridge.Use(m)
		}
	}
	queryHandler.RegisterRoutes(bridge)

	transport := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 64}
	service := &flightSQLService{
		// Set here, not left to the base server: its SqlInfo handlers
		// assign a nil Alloc on every call, a data race between two calls.
		BaseServer:   flightsql.BaseServer{Alloc: memory.DefaultAllocator},
		queryHandler: queryHandler,
		internalKey:  internalKey,
		client:       &http.Client{Transport: transport},
		baseCtx:      baseCtx,
	}
	if err := service.registerSqlInfo(flightSQLInfo(config.Version)); err != nil {
		cancelBase()
		return nil, err
	}

	var options []grpc.ServerOption
	if config.TLSEnabled {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			cancelBase()
			return nil, fmt.Errorf("load Flight SQL TLS keypair: %w", err)
		}
		tlsConfig := fips.HardenTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2"},
		})
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	var middleware []flight.ServerMiddleware
	if queryHandler.authManager != nil {
		middleware = append(middleware, flight.CreateServerBasicAuthMiddleware(&flightSQLAuth{manager: queryHandler.authManager}))
	}
	flightServer := flight.NewServerWithMiddleware(middleware, options...)
	flightServer.RegisterFlightService(flightsql.NewFlightServer(service))

	return &FlightSQLServer{
		bridge:     bridge,
		flight:     flightServer,
		service:    service,
		config:     config,
		logger:     logger,
		serveDone:  make(chan error, 1),
		cancelBase: cancelBase,
	}, nil
}

// Listen binds the bridge and the Flight SQL port without serving them, so
// cmd/arc can bind before the node reports ready (a port conflict exits
// first) and serve once the HTTP server is up.
func (s *FlightSQLServer) Listen() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.listening {
		return fmt.Errorf("Flight SQL server is already listening")
	}
	if s.shutdownRequested {
		return fmt.Errorf("Flight SQL server is shutting down")
	}

	bridgeLn, err := listenLoopback()
	if err != nil {
		return fmt.Errorf("listen for internal Flight SQL query bridge: %w", err)
	}
	_, addr := ListenAddr(s.config.Host, s.config.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		_ = bridgeLn.Close()
		return fmt.Errorf("listen for Flight SQL on %s: %w", addr, err)
	}
	s.bridgeLn = bridgeLn
	s.service.baseURL = "http://" + bridgeLn.Addr().String()
	s.flightLn = newFlightSQLConnTracker(ln)
	s.flight.InitListener(s.flightLn)
	s.listening = true
	return nil
}

// listenLoopback listens on IPv4 loopback, or IPv6 loopback on a host
// without IPv4.
func listenLoopback() (net.Listener, error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err == nil {
		return ln, nil
	}
	if ln6, err6 := net.Listen("tcp6", "[::1]:0"); err6 == nil {
		return ln6, nil
	}
	return nil, err
}

// Serve starts serving what Listen bound.
func (s *FlightSQLServer) Serve() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if !s.listening {
		return fmt.Errorf("Flight SQL server is not listening")
	}
	if s.serving {
		return fmt.Errorf("Flight SQL server is already started")
	}
	if s.shutdownRequested {
		return fmt.Errorf("Flight SQL server is shutting down")
	}
	s.serving = true
	bridgeLn := s.bridgeLn
	go func() {
		if err := s.bridge.Listener(bridgeLn); err != nil && !errors.Is(err, net.ErrClosed) {
			s.logger.Error().Err(err).Msg("Internal Flight SQL query bridge stopped")
		}
	}()
	go func() {
		err := s.flight.Serve()
		s.serveDone <- err
		close(s.serveDone)
	}()

	protocol := "grpc"
	if s.config.TLSEnabled {
		protocol = "grpcs"
	}
	s.logger.Info().Str("address", s.flightLn.Addr().String()).Str("protocol", protocol).Msg("Arrow Flight SQL server started")
	return nil
}

// Start is Listen then Serve.
func (s *FlightSQLServer) Start() error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve()
}

// Shutdown stops the server within its own bound (see flightSQLShutdownGrace),
// which it shortens to half of ctx's remaining time when that is less, so the
// hooks and components after it still run.
func (s *FlightSQLServer) Shutdown(ctx context.Context) error {
	s.startMu.Lock()
	s.shutdownRequested = true
	listening, serving := s.listening, s.serving
	s.startMu.Unlock()
	if !listening {
		s.cancelBase()
		return nil
	}

	st := flightSQLShutdownStages{grace: flightSQLShutdownGrace, force: flightSQLShutdownForce, drain: flightSQLBridgeDrain}
	if deadline, ok := ctx.Deadline(); ok {
		st = st.within(time.Until(deadline) / 2)
	}

	s.shutdownMu.Lock()
	if s.shutdownDone == nil {
		s.shutdownDone = make(chan struct{})
		go s.finishShutdown(serving, st)
	}
	done := s.shutdownDone
	s.shutdownMu.Unlock()

	select {
	case <-done:
		s.shutdownMu.Lock()
		err := s.shutdownErr
		s.shutdownMu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flightSQLShutdownStages are the waits of one shutdown. Their worst case is
// grace + 2*force + drain.
type flightSQLShutdownStages struct {
	grace, force, drain time.Duration
}

func (st flightSQLShutdownStages) total() time.Duration {
	return st.grace + 2*st.force + st.drain
}

// within scales every stage so the worst case fits in budget.
func (st flightSQLShutdownStages) within(budget time.Duration) flightSQLShutdownStages {
	total := st.total()
	if budget >= total {
		return st
	}
	if budget < 0 {
		budget = 0
	}
	scale := func(d time.Duration) time.Duration { return time.Duration(int64(d) * int64(budget) / int64(total)) }
	return flightSQLShutdownStages{grace: scale(st.grace), force: scale(st.force), drain: scale(st.drain)}
}

func (s *FlightSQLServer) finishShutdown(serving bool, st flightSQLShutdownStages) {
	grace, force := st.grace, st.force
	var shutdownErr error
	if serving {
		stopped := make(chan struct{})
		go func() {
			s.flight.Shutdown() // GracefulStop: no new RPCs, waits for the rest
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(grace):
			// In-flight queries end with their bridge requests.
			s.cancelBase()
			select {
			case <-stopped:
			case <-time.After(force):
				// A stream blocked on a client that stopped reading does
				// not see a cancelled context: close its connection.
				n := s.flightLn.closeAll()
				s.logger.Warn().Int("connections", n).Msg("Flight SQL shutdown: closed connections still open after in-flight queries were cancelled")
				select {
				case <-stopped:
				case <-time.After(force):
					shutdownErr = errors.Join(shutdownErr, errors.New("Flight SQL server did not stop after its connections were closed"))
				}
			}
		}
	} else if s.flightLn != nil {
		_ = s.flightLn.Close()
	}
	s.cancelBase()
	if err := s.bridge.ShutdownWithTimeout(st.drain); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown internal Flight SQL query bridge: %w", err))
	}
	if s.bridgeLn != nil {
		_ = s.bridgeLn.Close()
	}
	if transport, ok := s.service.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	if serving && shutdownErr == nil {
		if err := <-s.serveDone; err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("Flight SQL server stopped: %w", err))
		}
	}

	s.shutdownMu.Lock()
	s.shutdownErr = shutdownErr
	close(s.shutdownDone)
	s.shutdownMu.Unlock()
}

// flightSQLConnTracker records the connections it accepts, so shutdown can
// close the ones a graceful stop would otherwise wait on forever.
type flightSQLConnTracker struct {
	net.Listener
	mu    sync.Mutex
	conns map[*flightSQLTrackedConn]struct{}
}

type flightSQLTrackedConn struct {
	net.Conn
	tracker *flightSQLConnTracker
	once    sync.Once
}

func newFlightSQLConnTracker(ln net.Listener) *flightSQLConnTracker {
	return &flightSQLConnTracker{Listener: ln, conns: map[*flightSQLTrackedConn]struct{}{}}
}

func (t *flightSQLConnTracker) Accept() (net.Conn, error) {
	c, err := t.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tc := &flightSQLTrackedConn{Conn: c, tracker: t}
	t.mu.Lock()
	t.conns[tc] = struct{}{}
	t.mu.Unlock()
	return tc, nil
}

func (c *flightSQLTrackedConn) Close() error {
	c.once.Do(func() {
		c.tracker.mu.Lock()
		delete(c.tracker.conns, c)
		c.tracker.mu.Unlock()
	})
	return c.Conn.Close()
}

// closeAll closes every connection still open and reports how many.
func (t *flightSQLConnTracker) closeAll() int {
	t.mu.Lock()
	conns := make([]*flightSQLTrackedConn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return len(conns)
}

func (a *flightSQLAuth) Validate(username, password string) (string, error) {
	token := password
	if token == "" {
		token = username
	}
	if token == "" || a.manager.VerifyToken(token) == nil {
		return "", status.Error(codes.Unauthenticated, "invalid Arc token")
	}
	return token, nil
}

func (a *flightSQLAuth) IsValid(token string) (interface{}, error) {
	if token == "" || a.manager.VerifyToken(token) == nil {
		return nil, status.Error(codes.Unauthenticated, "invalid Arc token")
	}
	return &flightSQLIdentity{token: token}, nil
}

func (s *flightSQLService) token(ctx context.Context) string {
	identity, _ := flight.AuthFromContext(ctx).(*flightSQLIdentity)
	if identity == nil {
		return ""
	}
	return identity.token
}

func (s *flightSQLService) GetFlightInfoStatement(_ context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(query.GetTransactionId()) != 0 {
		return nil, status.Error(codes.Unimplemented, "Flight SQL transactions are not supported")
	}
	if err := ValidateSQLRequest(query.GetQuery()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	handle, err := json.Marshal(flightStatement{SQL: query.GetQuery()})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode statement ticket: %v", err)
	}
	ticket, err := flightsql.CreateStatementQueryTicket(handle)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode statement ticket: %v", err)
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *flightSQLService) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var statement flightStatement
	if err := json.Unmarshal(ticket.GetStatementHandle(), &statement); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid statement ticket")
	}
	if err := ValidateSQLRequest(statement.SQL); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}

	path := "/api/v1/query/arrow"
	if isFlightShowSQL(statement.SQL) {
		path = "/api/v1/query"
	}
	resp, err := s.query(ctx, path, statement.SQL, s.token(ctx))
	if err != nil {
		return nil, nil, err
	}
	if path == "/api/v1/query" {
		defer resp.Body.Close()
		return s.showResponse(ctx, resp)
	}

	reader, err := ipc.NewReader(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		return nil, nil, status.Errorf(codes.Internal, "read Arrow query response: %v", err)
	}
	schema := reader.Schema()
	stream := make(chan flight.StreamChunk)
	go func() {
		flight.StreamChunksFromReader(ctx, &flightSQLResponseReader{RecordReader: reader, ctx: ctx, response: resp}, stream)
		_ = resp.Body.Close()
	}()
	return schema, stream, nil
}

// flightSQLResponseReader preserves the HTTP completion signal when adapting
// IPC to Flight. An IPC end marker can precede the HTTP trailers; a clean IPC
// stream alone does not mean the query was uncapped.
type flightSQLResponseReader struct {
	array.RecordReader
	ctx           context.Context
	response      *http.Response
	once          sync.Once
	completionErr error
}

func (r *flightSQLResponseReader) Err() error {
	// Decoder errors take precedence over any completion metadata.
	if err := r.RecordReader.Err(); err != nil {
		return err
	}
	r.once.Do(func() {
		r.completionErr = flightSQLResponseCompletion(r.ctx, r.response)
	})
	return r.completionErr
}

func flightSQLResponseCompletion(ctx context.Context, response *http.Response) error {
	// Only HTTP framing/trailers should remain after IPC's end marker. Bound
	// both bytes and time: a broken internal bridge must not hang DoGet forever.
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	stopClose := context.AfterFunc(drainCtx, func() { _ = response.Body.Close() })
	defer func() { stopClose(); cancel() }()
	n, err := io.CopyN(io.Discard, response.Body, 1)
	if ctxErr := drainCtx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	if n != 0 {
		return status.Error(codes.Internal, "unexpected data after Arc Arrow IPC result")
	}
	if !errors.Is(err, io.EOF) {
		return status.Error(codes.Internal, "could not read Arc query completion trailers")
	}
	if response.Trailer.Get(arrowStreamTruncatedTrailer) != "" {
		return status.Error(codes.Internal, "Arc query stream was truncated")
	}
	if value := response.Trailer.Get(arrowRowsCappedTrailer); value != "" {
		rowCap, err := strconv.ParseUint(value, 10, 64)
		if err != nil || rowCap == 0 {
			return status.Error(codes.Internal, "invalid Arc query row-cap trailer")
		}
		// Standard Flight clients observe terminal RPC errors via reader.Err();
		// custom application metadata is not universally consumed by SQL clients.
		return status.Errorf(codes.ResourceExhausted, "Arc query reached its governance row cap (%d); result may be incomplete", rowCap)
	}
	return nil
}

func (s *flightSQLService) showResponse(ctx context.Context, resp *http.Response) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var response flightQueryResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&response); err != nil {
		return nil, nil, status.Errorf(codes.Internal, "decode SHOW response: %v", err)
	}
	if !response.Success {
		return nil, nil, status.Error(codes.Internal, response.Error)
	}
	builder, err := showRecordBuilder(response.Columns)
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	defer builder.Release()
	for _, row := range response.Data {
		if err := appendShowRow(builder, response.Columns, row); err != nil {
			return nil, nil, status.Errorf(codes.Internal, "convert SHOW result: %v", err)
		}
	}
	batch := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
	batch.Release()
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "build SHOW result: %v", err)
	}
	schema := reader.Schema()
	stream := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(ctx, reader, stream)
	return schema, stream, nil
}

func showRecordBuilder(columns []string) (*array.RecordBuilder, error) {
	switch strings.Join(columns, ",") {
	case "database":
		return array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "database", Type: arrow.BinaryTypes.String}}, nil)), nil
	case "database,tier":
		return array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
			{Name: "database", Type: arrow.BinaryTypes.String},
			{Name: "tier", Type: arrow.BinaryTypes.String},
		}, nil)), nil
	case "database,table_name,storage_path,file_count,total_size_mb":
		return array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
			{Name: "database", Type: arrow.BinaryTypes.String},
			{Name: "table_name", Type: arrow.BinaryTypes.String},
			{Name: "storage_path", Type: arrow.BinaryTypes.String},
			{Name: "file_count", Type: arrow.PrimitiveTypes.Int64},
			{Name: "total_size_mb", Type: arrow.PrimitiveTypes.Float64},
		}, nil)), nil
	default:
		return nil, fmt.Errorf("unsupported SHOW result columns: %s", strings.Join(columns, ","))
	}
}

func appendShowRow(builder *array.RecordBuilder, columns []string, row []json.RawMessage) error {
	if len(row) != len(columns) {
		return fmt.Errorf("got %d values for %d columns", len(row), len(columns))
	}
	for i, column := range columns {
		switch column {
		case "database", "tier", "table_name", "storage_path":
			var value string
			if err := json.Unmarshal(row[i], &value); err != nil {
				return err
			}
			builder.Field(i).(*array.StringBuilder).Append(value)
		case "file_count":
			var value int64
			if err := json.Unmarshal(row[i], &value); err != nil {
				return err
			}
			builder.Field(i).(*array.Int64Builder).Append(value)
		case "total_size_mb":
			var value float64
			if err := json.Unmarshal(row[i], &value); err != nil {
				return err
			}
			builder.Field(i).(*array.Float64Builder).Append(value)
		default:
			return fmt.Errorf("unsupported SHOW column %q", column)
		}
	}
	return nil
}

func isFlightShowSQL(sql string) bool {
	normalised := normalizeSQLForShow(sql)
	return showDatabasesPattern.MatchString(normalised) || showTablesPattern.MatchString(normalised)
}

func (s *flightSQLService) query(ctx context.Context, path, sql, token string) (*http.Response, error) {
	body, err := json.Marshal(QueryRequest{SQL: sql})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode query: %v", err)
	}
	// The request ends with the RPC or with shutdown, whichever is first.
	base := s.baseCtx
	if base == nil {
		base = context.Background()
	}
	reqCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(base, cancel)
	release := func() { stop(); cancel() }
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.baseURL+path, bytes.NewReader(body))
	if err != nil {
		release()
		return nil, status.Errorf(codes.Internal, "prepare query: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(flightSQLInternalKeyHeader, s.internalKey)
	if token != "" {
		req.Header.Set(fiber.HeaderAuthorization, "Bearer "+token)
	}
	// Who is asking, for the query registry, the audit log and forwarding.
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		host := p.Addr.String()
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		req.Header.Set(flightSQLPeerHeader, host)
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ua := md.Get("user-agent"); len(ua) > 0 {
			req.Header.Set(fiber.HeaderUserAgent, ua[0])
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		release()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, status.FromContextError(ctxErr).Err()
		}
		if base.Err() != nil {
			return nil, status.Error(codes.Unavailable, "Arc is shutting down")
		}
		s.queryHandler.logger.Warn().Err(err).Str("path", path).Msg("Flight SQL: the internal query bridge request failed")
		return nil, status.Error(codes.Unavailable, "Arc query request failed")
	}
	if resp.StatusCode != http.StatusOK {
		defer release()
		defer resp.Body.Close()
		var message struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&message)
		if message.Error == "" {
			message.Error = http.StatusText(resp.StatusCode)
		}
		return nil, status.Error(flightCodeForHTTP(resp.StatusCode), message.Error)
	}
	resp.Body = &flightSQLReleasingBody{ReadCloser: resp.Body, release: release}
	return resp, nil
}

// flightSQLReleasingBody releases a bridge request's cancellation when its
// body is closed.
type flightSQLReleasingBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (b *flightSQLReleasingBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

func flightCodeForHTTP(code int) codes.Code {
	switch code {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}

// flightSQLInfo is what GetSqlInfo answers: who the server is, that it is
// read-only, how identifiers are quoted and search patterns escaped, and which
// kinds of statement it takes. The JDBC driver answers DatabaseMetaData from
// it, and ADBC's GetInfo reads the server name and versions from it.
func flightSQLInfo(version string) map[flightsql.SqlInfo]interface{} {
	info := map[flightsql.SqlInfo]interface{}{
		flightsql.SqlInfoFlightSqlServerName:          "Arc",
		flightsql.SqlInfoFlightSqlServerArrowVersion:  arrow.PkgVersion,
		flightsql.SqlInfoFlightSqlServerReadOnly:      true,
		flightsql.SqlInfoFlightSqlServerSql:           true,
		flightsql.SqlInfoFlightSqlServerSubstrait:     false,
		flightsql.SqlInfoFlightSqlServerTransaction:   int32(flightsql.SqlTransactionNone),
		flightsql.SqlInfoFlightSqlServerCancel:        false,
		flightsql.SqlInfoFlightSqlServerBulkIngestion: false,
		flightsql.SqlInfoDDLCatalog:                   false,
		flightsql.SqlInfoDDLSchema:                    false,
		flightsql.SqlInfoDDLTable:                     false,
		flightsql.SqlInfoIdentifierQuoteChar:          `"`,
		// The escape compileFlightPattern applies to metadata patterns.
		flightsql.SqlInfoSearchStringEscape: `\`,
		flightsql.SqlInfoCatalogTerm:        "database",
	}
	if version != "" {
		info[flightsql.SqlInfoFlightSqlServerVersion] = version
	}
	return info
}

func (s *flightSQLService) registerSqlInfo(info map[flightsql.SqlInfo]interface{}) error {
	s.sqlInfo = make(map[uint32]struct{}, len(info))
	for id, value := range info {
		if err := s.RegisterSqlInfo(id, value); err != nil {
			return fmt.Errorf("register Flight SQL info %d: %w", id, err)
		}
		s.sqlInfo[uint32(id)] = struct{}{}
	}
	return nil
}

// DoGetSqlInfo answers the requested IDs Arc has a value for and leaves the
// others out, where the base server fails the whole request on the first one
// it lacks. Clients ask for IDs a server may not have: ADBC's GetInfo asks for
// the Substrait version range, which a server without Substrait has no value
// for, and must not fail for it. An empty request is answered in full.
func (s *flightSQLService) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	requested := cmd.GetInfo()
	if len(requested) == 0 {
		return s.BaseServer.DoGetSqlInfo(ctx, cmd)
	}
	known := make(flightSQLInfoRequest, 0, len(requested))
	for _, id := range requested {
		if _, ok := s.sqlInfo[id]; ok {
			known = append(known, id)
		}
	}
	if len(known) == 0 {
		// Not handed to the base server: an empty request means every ID.
		builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.SqlInfo)
		defer builder.Release()
		return flightRecordStream(ctx, schema_ref.SqlInfo, builder)
	}
	return s.BaseServer.DoGetSqlInfo(ctx, known)
}

// flightSQLInfoRequest is a GetSqlInfo request for the given IDs.
type flightSQLInfoRequest []uint32

func (r flightSQLInfoRequest) GetInfo() []uint32 { return r }

func (s *flightSQLService) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfo(desc, schema_ref.Catalogs), nil
}

func (s *flightSQLService) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	response, err := s.listDatabases(ctx, s.token(ctx))
	if err != nil {
		return nil, nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.Catalogs)
	defer builder.Release()
	for _, database := range response {
		builder.Field(0).(*array.StringBuilder).Append(database)
	}
	return flightRecordStream(ctx, schema_ref.Catalogs, builder)
}

func (s *flightSQLService) GetFlightInfoSchemas(_ context.Context, _ flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfo(desc, schema_ref.DBSchemas), nil
}

func (s *flightSQLService) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	token := s.token(ctx)
	catalogPattern := compileFlightPattern(cmd.GetCatalog())
	dbSchemaPattern := compileFlightPattern(cmd.GetDBSchemaFilterPattern())
	var databases []string
	if catalog := cmd.GetCatalog(); catalog != nil {
		if _, err := s.listMeasurements(ctx, *catalog, token); err != nil {
			return nil, nil, err
		}
		databases = []string{*catalog}
	} else {
		var err error
		databases, err = s.listDatabases(ctx, token)
		if err != nil {
			return nil, nil, err
		}
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.DBSchemas)
	defer builder.Release()
	for _, database := range databases {
		if !matchesFlightPattern(database, catalogPattern) || !matchesFlightPattern("", dbSchemaPattern) {
			continue
		}
		builder.Field(0).(*array.StringBuilder).Append(database)
		builder.Field(1).(*array.StringBuilder).Append("")
	}
	return flightRecordStream(ctx, schema_ref.DBSchemas, builder)
}

func (s *flightSQLService) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfo(desc, schema), nil
}

func (s *flightSQLService) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	token := s.token(ctx)
	catalogPattern := compileFlightPattern(cmd.GetCatalog())
	dbSchemaPattern := compileFlightPattern(cmd.GetDBSchemaFilterPattern())
	tableNamePattern := compileFlightPattern(cmd.GetTableNameFilterPattern())
	var databases []string
	if catalog := cmd.GetCatalog(); catalog != nil {
		databases = []string{*catalog}
	} else {
		var err error
		databases, err = s.listDatabases(ctx, token)
		if err != nil {
			return nil, nil, err
		}
	}
	tableSchema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		tableSchema = schema_ref.TablesWithIncludedSchema
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, tableSchema)
	defer builder.Release()
	for _, database := range databases {
		if !matchesFlightPattern(database, catalogPattern) || !matchesFlightPattern("", dbSchemaPattern) {
			continue
		}
		measurements, err := s.listMeasurements(ctx, database, token)
		if err != nil {
			return nil, nil, err
		}
		for _, measurement := range measurements {
			if !matchesFlightPattern(measurement, tableNamePattern) || !flightTableTypeMatches(cmd.GetTableTypes()) {
				continue
			}
			// The schema first: a row is all five columns or none.
			var schemaBytes []byte
			if cmd.GetIncludeSchema() {
				var err error
				schemaBytes, err = s.measurementSchema(ctx, database, measurement, token)
				if err != nil {
					if skipFlightTableSchemaError(ctx, err) {
						continue
					}
					return nil, nil, err
				}
			}
			builder.Field(0).(*array.StringBuilder).Append(database)
			builder.Field(1).(*array.StringBuilder).Append("")
			builder.Field(2).(*array.StringBuilder).Append(measurement)
			builder.Field(3).(*array.StringBuilder).Append("TABLE")
			if cmd.GetIncludeSchema() {
				builder.Field(4).(*array.BinaryBuilder).Append(schemaBytes)
			}
		}
	}
	return flightRecordStream(ctx, tableSchema, builder)
}

// skipFlightTableSchemaError reports whether a measurement whose schema query
// failed is left out of a GetTables listing rather than failing it: only one
// the token may not read (SHOW TABLES lists every measurement; RBAC applies
// per measurement) or one that is gone. Anything else fails the listing, so
// a BI tool is never handed a quietly partial catalogue: a governance limit
// or timeout, an expired token, a query error, shutdown, or the RPC ending.
func skipFlightTableSchemaError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	switch status.Code(err) {
	case codes.PermissionDenied, codes.NotFound:
		return true
	default:
		return false
	}
}

func (s *flightSQLService) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return s.flightInfo(desc, schema_ref.TableTypes), nil
}

func (s *flightSQLService) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema_ref.TableTypes)
	builder.Field(0).(*array.StringBuilder).Append("TABLE")
	defer builder.Release()
	return flightRecordStream(ctx, schema_ref.TableTypes, builder)
}

func (s *flightSQLService) flightInfo(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	var ticket []byte
	if desc != nil {
		ticket = desc.Cmd
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, memory.DefaultAllocator),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func flightRecordStream(ctx context.Context, schema *arrow.Schema, builder *array.RecordBuilder) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	batch := builder.NewRecordBatch()
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	batch.Release()
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "build Flight SQL result: %v", err)
	}
	stream := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(ctx, reader, stream)
	return schema, stream, nil
}

func (s *flightSQLService) listDatabases(ctx context.Context, token string) ([]string, error) {
	resp, err := s.query(ctx, "/api/v1/query", "SHOW DATABASES", token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result flightQueryResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, status.Errorf(codes.Internal, "decode database listing: %v", err)
	}
	if !result.Success {
		return nil, status.Error(codes.PermissionDenied, result.Error)
	}
	var databases []string
	for _, row := range result.Data {
		if len(row) == 0 {
			continue
		}
		var database string
		if err := json.Unmarshal(row[0], &database); err != nil {
			return nil, status.Errorf(codes.Internal, "decode database name: %v", err)
		}
		databases = append(databases, database)
	}
	return databases, nil
}

func (s *flightSQLService) listMeasurements(ctx context.Context, database, token string) ([]string, error) {
	if err := validateIdentifier(database); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid catalog")
	}
	sql := "SHOW TABLES FROM " + quoteFlightIdentifier(database)
	resp, err := s.query(ctx, "/api/v1/query", sql, token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result flightQueryResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&result); err != nil {
		return nil, status.Errorf(codes.Internal, "decode measurement listing: %v", err)
	}
	if !result.Success {
		return nil, status.Errorf(codes.PermissionDenied, "cannot list measurements in catalog %q: %s", database, result.Error)
	}
	var measurements []string
	for _, row := range result.Data {
		if len(row) < 2 {
			continue
		}
		var measurement string
		if err := json.Unmarshal(row[1], &measurement); err != nil {
			return nil, status.Errorf(codes.Internal, "decode measurement name: %v", err)
		}
		measurements = append(measurements, measurement)
	}
	return measurements, nil
}

func (s *flightSQLService) measurementSchema(ctx context.Context, database, measurement, token string) ([]byte, error) {
	if err := validateIdentifier(database); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid catalog")
	}
	sql := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0", quoteFlightIdentifier(database), quoteFlightIdentifier(measurement))
	resp, err := s.query(ctx, "/api/v1/query/arrow", sql, token)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	reader, err := ipc.NewReader(resp.Body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read measurement schema: %v", err)
	}
	defer reader.Release()
	return flight.SerializeSchema(reader.Schema(), memory.DefaultAllocator), nil
}

func quoteFlightIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func compileFlightPattern(pattern *string) *regexp.Regexp {
	if pattern == nil {
		return nil
	}
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(*pattern); i++ {
		switch (*pattern)[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '\\':
			i++
			if i == len(*pattern) {
				b.WriteString(`\\`)
			} else {
				b.WriteString(regexp.QuoteMeta((*pattern)[i : i+1]))
			}
		default:
			b.WriteString(regexp.QuoteMeta((*pattern)[i : i+1]))
		}
	}
	b.WriteByte('$')
	return regexp.MustCompile(b.String())
}

func matchesFlightPattern(value string, pattern *regexp.Regexp) bool {
	if pattern == nil {
		return true
	}
	return pattern.MatchString(value)
}

func flightTableTypeMatches(types []string) bool {
	if len(types) == 0 {
		return true
	}
	for _, tableType := range types {
		if strings.EqualFold(tableType, "TABLE") || strings.EqualFold(tableType, "BASE TABLE") {
			return true
		}
	}
	return false
}
