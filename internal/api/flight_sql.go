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
	"math/bits"
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
	"github.com/valyala/fasthttp"
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

// flightSQLDatabaseHeader is the one client call header a statement carries
// across the bridge: the database unqualified table names resolve in, as the
// HTTP header of the same name. ADBC sets it with the connection option
// adbc.flight.sql.rpc.call_header.x-arc-database. Of the client's metadata
// only this, the bearer token and the User-Agent cross; the peer header is
// the bridge's proxy header (the audit log's client address), and the other
// x-arc-* headers change what the Arrow route encodes or whether it forwards.
const flightSQLDatabaseHeader = "x-arc-database"

// Shutdown stages. The shutdown hooks share one deadline, their part of
// server.shutdown_timeout: a hook that overruns it leaves the hooks after it
// an expired context, and once the whole timeout has passed the coordinator
// skips everything left, including the ingest buffer and WAL flushes. So this
// hook keeps its own deadline well inside it: in-flight queries get flightSQLShutdownGrace to finish and are
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
	bridge   *fiber.App
	bridgeLn net.Listener
	// bridgeConns follows the bridge's connections, so the drain knows
	// which still carry a request (see drainBridge).
	bridgeConns *flightSQLBridgeConns
	flight      flight.Server
	flightLn    *flightSQLConnTracker
	service     *flightSQLService
	config      FlightSQLServerConfig
	logger      zerolog.Logger
	serveDone   chan error
	cancelBase  context.CancelFunc

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
	// Database is the x-arc-database call header GetFlightInfo saw, so a
	// client that sends it only there still runs in that database.
	Database    string `json:"database,omitempty"`
	DecimalMode string `json:"decimal_mode,omitempty"`
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
	queryHandler.registerFlightSQLSchemaRoute(bridge)

	// No gzip on the loopback hop: it only costs CPU on both ends.
	transport := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 64, DisableCompression: true}
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
		middleware = append(middleware, flightSQLAuthMiddleware(&flightSQLAuth{manager: queryHandler.authManager}))
	}
	flightServer := flight.NewServerWithMiddleware(middleware, options...)
	flightServer.RegisterFlightService(flightsql.NewFlightServer(newFlightSQLMetadataModeService(service, logger)))

	// Fiber never sets ConnState, and bridge.Server() is the server its
	// Listener serves.
	bridgeConns := newFlightSQLBridgeConns()
	bridge.Server().ConnState = bridgeConns.connState

	return &FlightSQLServer{
		bridge:      bridge,
		bridgeConns: bridgeConns,
		flight:      flightServer,
		service:     service,
		config:      config,
		logger:      logger,
		serveDone:   make(chan error, 1),
		cancelBase:  cancelBase,
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

// within scales every stage so the worst case fits in budget: each becomes
// stage*budget/total, rounded down, so the scaled stages never add up to more
// than budget and none is negative.
func (st flightSQLShutdownStages) within(budget time.Duration) flightSQLShutdownStages {
	// A negative stage or budget counts as zero: no wait.
	st = flightSQLShutdownStages{grace: max(st.grace, 0), force: max(st.force, 0), drain: max(st.drain, 0)}
	budget = max(budget, 0)
	total := st.total()
	if budget >= total {
		return st
	}
	// Here 0 <= budget < total and 0 <= d <= total, so d*budget/total is at
	// most d and Div64's precondition (hi < total) holds. The product is
	// taken in 128 bits: in int64 nanoseconds it overflows for ordinary
	// values (5 s * 6 s is 3e19, past 9.2e18) and wrapped, so at the default
	// server.shutdown_timeout every stage came out negative and none waited.
	scale := func(d time.Duration) time.Duration {
		hi, lo := bits.Mul64(uint64(d), uint64(budget))
		q, _ := bits.Div64(hi, lo, uint64(total))
		return time.Duration(q)
	}
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
	if err := s.drainBridge(st.drain); err != nil {
		shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown internal Flight SQL query bridge: %w", err))
	}
	if s.bridgeLn != nil {
		_ = s.bridgeLn.Close()
	}
	if transport, ok := s.service.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	if serving && shutdownErr == nil {
		// ErrServerStopped is Serve finding the server already stopped: a
		// shutdown that came before Serve had started. That is a stop.
		if err := <-s.serveDone; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("Flight SQL server stopped: %w", err))
		}
	}

	s.shutdownMu.Lock()
	s.shutdownErr = shutdownErr
	close(s.shutdownDone)
	s.shutdownMu.Unlock()
}

// flightSQLBridgeDrainPoll is how often drainBridge looks for requests still
// in flight.
const flightSQLBridgeDrainPoll = 2 * time.Millisecond

// drainBridge stops the bridge and waits up to drain for the requests still
// in flight on it. It fails only when a request is still running at the end:
// a handler that did not return, or a response still being streamed. Their
// queries were cancelled before this is called.
//
// The wait is on Arc's own count of requests in flight, not on the result of
// fasthttp's shutdown. That shutdown waits for every connection to be gone
// and looks again only every 100 ms, so it timed out with nothing running:
// whenever the drain was shorter than that (#1834), and at any drain when a
// connection had been opened and not yet used, which it does not treat as
// idle for 5 s. A connection without a request is closed here, not waited on.
func (s *FlightSQLServer) drainBridge(drain time.Duration) error {
	// The bridge's only client: its idle connections first, so their server
	// ends are closing before the wait starts.
	if transport, ok := s.service.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	ctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	// fasthttp's shutdown runs for the whole wait: it closes the listener,
	// and while it runs a connection closes once its request is answered.
	done := make(chan error, 1)
	go func() { done <- s.bridge.ShutdownWithContext(ctx) }()
	tick := time.NewTicker(flightSQLBridgeDrainPoll)
	defer tick.Stop()
	var err error
	running := -1
wait:
	for {
		select {
		case err = <-done:
			break wait
		case <-tick.C:
			if s.bridgeConns.active() == 0 {
				// Nothing in flight: that is the result. It is not read
				// again after the join below, where a request that was
				// sent before the queries were cancelled could still
				// show up, and be reported as outliving a drain that
				// never ran its length.
				running = 0
				cancel()
				err = <-done
				break wait
			}
		}
	}
	if running < 0 {
		running = s.bridgeConns.active()
	}
	// Whatever is left is closed: fasthttp's shutdown has returned and no
	// longer closes a connection when its request ends.
	s.bridgeConns.closeAll()
	if running > 0 {
		s.logger.Warn().Int("requests", running).Dur("drain", drain).Msg("Flight SQL shutdown: requests still running on the bridge after its drain, their connections were closed")
		return fmt.Errorf("requests still running after the %s drain: %d", drain, running)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// flightSQLBridgeConns follows the bridge's connections through fasthttp's
// ConnState hook: which are open, and which carry a request. A connection is
// active from the first byte of a request until its response is written in
// full, a streamed body included.
type flightSQLBridgeConns struct {
	mu    sync.Mutex
	conns map[net.Conn]bool // true while a request is in flight on it
}

func newFlightSQLBridgeConns() *flightSQLBridgeConns {
	return &flightSQLBridgeConns{conns: map[net.Conn]bool{}}
}

func (t *flightSQLBridgeConns) connState(c net.Conn, state fasthttp.ConnState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch state {
	case fasthttp.StateNew, fasthttp.StateIdle:
		t.conns[c] = false
	case fasthttp.StateActive:
		t.conns[c] = true
	case fasthttp.StateClosed, fasthttp.StateHijacked:
		delete(t.conns, c)
	}
}

// active is the number of connections with a request in flight.
func (t *flightSQLBridgeConns) active() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, busy := range t.conns {
		if busy {
			n++
		}
	}
	return n
}

// open is the number of connections, with or without a request.
func (t *flightSQLBridgeConns) open() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

// closeAll closes every connection still open. fasthttp closes them again
// when their serve loops end; a second Close is harmless.
func (t *flightSQLBridgeConns) closeAll() {
	t.mu.Lock()
	conns := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
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

// flightHandshakeMethod is the gRPC method of a Flight Handshake.
const flightHandshakeMethod = "/arrow.flight.protocol.FlightService/Handshake"

// flightSQLAuthMiddleware is arrow-go's Basic auth middleware, plus a
// Handshake that carries an Arc token as a bearer token. The Arrow Flight SQL
// JDBC driver opens a connection with that handshake when given its `token`
// property, and sends the same header on every call after it; arrow-go only
// accepts Basic credentials on a handshake. A valid token answers with the
// same bearer trailer a Basic handshake does. Every other call, and a
// handshake without a bearer token, goes through arrow-go's middleware.
func flightSQLAuthMiddleware(validator *flightSQLAuth) flight.ServerMiddleware {
	middleware := flight.CreateServerBasicAuthMiddleware(validator)
	basicStream := middleware.Stream
	middleware.Stream = func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == flightHandshakeMethod {
			if token, ok := flightBearerToken(stream.Context()); ok {
				if _, err := validator.IsValid(token); err != nil {
					return err
				}
				stream.SetTrailer(metadata.Pairs("authorization", "Bearer "+token))
				return handler(srv, stream)
			}
		}
		return basicStream(srv, stream, info, handler)
	}
	return middleware
}

// flightBearerToken returns the token of an `authorization: Bearer <token>`
// header, parsed as arrow-go parses it for the calls after a handshake.
func flightBearerToken(ctx context.Context) (string, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", false
	}
	scheme, credential, _ := strings.Cut(strings.TrimSpace(values[0]), " ")
	credential = strings.TrimLeft(credential, " ")
	if !strings.EqualFold(scheme, "Bearer") || credential == "" {
		return "", false
	}
	return credential, true
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

// flightDatabase returns the statement's x-arc-database call header, "" when
// it is absent or empty. Sent more than once, or not a valid database name,
// it is the client's error.
func flightDatabase(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get(flightSQLDatabaseHeader)
	if len(values) == 0 {
		return "", nil
	}
	if len(values) > 1 {
		return "", status.Error(codes.InvalidArgument, "the x-arc-database header was sent more than once")
	}
	if err := validateHeaderDatabase(values[0]); err != nil {
		return "", status.Error(codes.InvalidArgument, "invalid x-arc-database header: "+err.Error())
	}
	return values[0], nil
}

// flightStatementDatabase is the database a statement planned in stored (a
// ticket's or a prepared statement handle's, "" for none) runs in: the call's
// x-arc-database header when only it is set, stored otherwise. Both set, they
// must match.
func flightStatementDatabase(ctx context.Context, stored string) (string, error) {
	database, err := flightDatabase(ctx)
	if err != nil {
		return "", err
	}
	switch {
	case database == "":
		return stored, nil
	case stored != "" && database != stored:
		return "", status.Error(codes.InvalidArgument, "the x-arc-database header differs from the database the statement was prepared in")
	}
	return database, nil
}

func (s *flightSQLService) GetFlightInfoStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(query.GetTransactionId()) != 0 {
		return nil, status.Error(codes.Unimplemented, "Flight SQL transactions are not supported")
	}
	if err := ValidateSQLRequest(query.GetQuery()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	database, err := flightDatabase(ctx)
	if err != nil {
		return nil, err
	}
	decimalMode, _, err := flightSQLDecimalMode(ctx)
	if err != nil {
		return nil, err
	}
	return flightStatementInfo(desc, query.GetQuery(), database, decimalMode)
}

// flightStatementInfo answers a statement ticket for sql, to run in database.
func flightStatementInfo(desc *flight.FlightDescriptor, sql, database string, decimalModes ...string) (*flight.FlightInfo, error) {
	decimalMode := ""
	if len(decimalModes) > 0 && decimalModes[0] == decimalModeExact {
		decimalMode = decimalModeExact
	}
	handle, err := json.Marshal(flightStatement{SQL: sql, Database: database, DecimalMode: decimalMode})
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

// flightSQLBenchDirect is set only by flight_sql_bench.go, which builds with
// the flightbench tag and never into a release: a benchmark arm that serves a
// statement from DuckDB in-process, without the bridge (scripts/flightbench).
// It returns handled=false unless the caller asked for it.
var flightSQLBenchDirect func(ctx context.Context, s *flightSQLService, sql, database string) (schema *arrow.Schema, stream <-chan flight.StreamChunk, handled bool, err error)

func (s *flightSQLService) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var statement flightStatement
	if err := json.Unmarshal(ticket.GetStatementHandle(), &statement); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid statement ticket")
	}
	if err := ValidateSQLRequest(statement.SQL); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// The ticket is the client's to send back, so its database is checked
	// like the header; the route then applies the same checks to both.
	if err := validateHeaderDatabase(statement.Database); err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid database in statement ticket")
	}
	if _, err := flightStatementDecimalMode(ctx, statement.DecimalMode); err != nil {
		return nil, nil, err
	}
	database, err := flightStatementDatabase(ctx, statement.Database)
	if err != nil {
		return nil, nil, err
	}
	if flightSQLBenchDirect != nil {
		if schema, stream, handled, err := flightSQLBenchDirect(ctx, s, statement.SQL, database); handled {
			return schema, stream, err
		}
	}

	path := "/api/v1/query/arrow"
	if isFlightShowSQL(statement.SQL) {
		path = "/api/v1/query"
	}
	resp, err := s.query(ctx, path, statement.SQL, s.token(ctx), database)
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
	decodeErr := r.RecordReader.Err()
	r.once.Do(func() {
		r.completionErr = flightSQLResponseCompletion(r.ctx, r.response)
	})
	if decodeErr != nil {
		// Arc ends a stream it had to cut short with a marker no decoder
		// reads (poisonArrowStream), then names the cut in a trailer. Report
		// the cut, not the decoder's complaint about the marker. Any other
		// decoder error takes precedence over the completion metadata.
		if reason := r.response.Trailer.Get(arrowStreamTruncatedTrailer); reason != "" {
			return flightTruncationError(reason)
		}
		return decodeErr
	}
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
	if reason := response.Trailer.Get(arrowStreamTruncatedTrailer); reason != "" {
		return flightTruncationError(reason)
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
	schema, err := flightShowSchema(columns)
	if err != nil {
		return nil, err
	}
	return array.NewRecordBuilder(memory.DefaultAllocator, schema), nil
}

// flightShowSchema is the Flight schema of a SHOW result with these columns.
func flightShowSchema(columns []string) (*arrow.Schema, error) {
	switch strings.Join(columns, ",") {
	case "database":
		return arrow.NewSchema([]arrow.Field{{Name: "database", Type: arrow.BinaryTypes.String}}, nil), nil
	case "database,tier":
		return arrow.NewSchema([]arrow.Field{
			{Name: "database", Type: arrow.BinaryTypes.String},
			{Name: "tier", Type: arrow.BinaryTypes.String},
		}, nil), nil
	case "database,table_name,storage_path,file_count,total_size_mb":
		return arrow.NewSchema([]arrow.Field{
			{Name: "database", Type: arrow.BinaryTypes.String},
			{Name: "table_name", Type: arrow.BinaryTypes.String},
			{Name: "storage_path", Type: arrow.BinaryTypes.String},
			{Name: "file_count", Type: arrow.PrimitiveTypes.Int64},
			{Name: "total_size_mb", Type: arrow.PrimitiveTypes.Float64},
		}, nil), nil
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

// query sends one statement across the bridge. database, when set, becomes
// the x-arc-database header; the catalog helpers pass "" because their SQL
// qualifies every table, which the route refuses alongside the header.
func (s *flightSQLService) query(ctx context.Context, path, sql, token, database string) (*http.Response, error) {
	decimalMode := ""
	decimalModePresent := false
	if path == "/api/v1/query/arrow" || path == flightSQLSchemaPath {
		var err error
		decimalMode, decimalModePresent, err = flightSQLDecimalMode(ctx)
		if err != nil {
			return nil, err
		}
	}
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
	if database != "" {
		req.Header.Set(flightSQLDatabaseHeader, database)
	}
	if decimalModePresent {
		req.Header.Set(flightSQLDecimalsHeader, decimalMode)
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
	case http.StatusNotImplemented:
		return codes.Unimplemented
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	case http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}

// flightSQLInfo is what GetSqlInfo answers: who the server is, that it is
// read-only, how identifiers are quoted and search patterns escaped, which
// kinds of statement it takes and which query grammar it supports. The JDBC driver answers DatabaseMetaData from
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
		// database.measurement: the catalog comes first.
		flightsql.SqlInfoCatalogAtStart: true,

		// What the query grammar supports, every one checked through Flight
		// against Arc's db.table rewrite. A client that is not told reads
		// these as false: the JDBC driver (19.0.0) answers an ID the server
		// leaves out with false, 0 or "", so leaving one out claims that Arc
		// has no GROUP BY, UNION, outer join or subqueries.
		flightsql.SqlInfoSupportsColumnAliasing:        true,
		flightsql.SqlInfoNullPlusNullIsNull:            true,
		flightsql.SqlInfoSupportsTableCorrelationNames: true,
		flightsql.SqlInfoSupportsExpressionsInOrderBy:  true,
		flightsql.SqlInfoSupportsOrderByUnrelated:      true,
		flightsql.SqlInfoSupportsLikeEscapeClause:      true,
		flightsql.SqlInfoCorrelatedSubqueriesSupported: true,
		// Bitmasks: bit n is the enum value n.
		flightsql.SqlInfoSupportedGroupBy:    int32(flightSQLGroupByUnrelated | flightSQLGroupByBeyondSelect),
		flightsql.SqlInfoSupportedSubqueries: int32(flightSQLSubqueriesInComparisons | flightSQLSubqueriesInExists | flightSQLSubqueriesInIns | flightSQLSubqueriesInQuantifieds),
		flightsql.SqlInfoSupportedUnions:     int32(flightSQLUnion | flightSQLUnionAll),
		// Results stream forward only; there are no transactions, so the only
		// isolation level is none.
		flightsql.SqlInfoSupportedResultSetTypes:              int32(flightSQLResultSetForwardOnly),
		flightsql.SqlInfoSupportedTransactionsIsolationlevels: int32(flightSQLTransactionIsolationNone),
		// An ordinal, not a bitmask: full outer joins.
		flightsql.SqlInfoOuterJoinsSupportLevel: int32(flightSQLFullOuterJoins),
		// The ODBC minimum grammar, whose writes a read-only source may leave
		// out (ODBC reference, appendix C). Not the core or extended grammar,
		// and no ANSI-92 level: those are full levels including DDL and DML.
		flightsql.SqlInfoSupportedGrammar: int32(flightSQLMinimumGrammar),
	}
	if version != "" {
		info[flightsql.SqlInfoFlightSqlServerVersion] = version
	}
	return info
}

// SqlInfo enum values from the Flight SQL protocol (FlightSql.proto); the
// bitmask ones are 1 << the enum value. arrow-go exports no Go names for them.
const (
	flightSQLGroupByUnrelated    = 1 << 0 // SQL_GROUP_BY_UNRELATED
	flightSQLGroupByBeyondSelect = 1 << 1 // SQL_GROUP_BY_BEYOND_SELECT

	flightSQLSubqueriesInComparisons = 1 << 0 // SQL_SUBQUERIES_IN_COMPARISONS
	flightSQLSubqueriesInExists      = 1 << 1 // SQL_SUBQUERIES_IN_EXISTS
	flightSQLSubqueriesInIns         = 1 << 2 // SQL_SUBQUERIES_IN_INS
	flightSQLSubqueriesInQuantifieds = 1 << 3 // SQL_SUBQUERIES_IN_QUANTIFIEDS

	flightSQLUnion    = 1 << 0 // SQL_UNION
	flightSQLUnionAll = 1 << 1 // SQL_UNION_ALL

	flightSQLResultSetForwardOnly     = 1 << 1 // SQL_RESULT_SET_TYPE_FORWARD_ONLY
	flightSQLTransactionIsolationNone = 1 << 0 // SQL_TRANSACTION_NONE

	flightSQLFullOuterJoins = 2 // SqlOuterJoinsSupportLevel.SQL_FULL_OUTER_JOINS (ordinal)

	flightSQLMinimumGrammar = 1 << 0 // SQL_MINIMUM_GRAMMAR
)

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
	resp, err := s.query(ctx, "/api/v1/query", "SHOW DATABASES", token, "")
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
	resp, err := s.query(ctx, "/api/v1/query", sql, token, "")
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
	resp, err := s.query(ctx, "/api/v1/query/arrow", sql, token, "")
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
