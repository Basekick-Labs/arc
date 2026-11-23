//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// The internal bridge between the gRPC side and the query handler: the
// middleware it runs, what crosses the hop, and shutdown. The routes the tests
// add come after every middleware, as the query handler's do.

func newBridgeTestServer(t *testing.T, authManager *auth.AuthManager, middleware ...fiber.Handler) *FlightSQLServer {
	t.Helper()
	qh := NewQueryHandler(nil, nil, zerolog.Nop(), 0, 0)
	if authManager != nil {
		qh.SetAuthAndRBAC(authManager, nil)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	s, err := NewFlightSQLServer(qh, FlightSQLServerConfig{Host: "127.0.0.1", Port: port, Middleware: middleware}, zerolog.Nop())
	require.NoError(t, err)
	return s
}

func startBridgeTestServer(t *testing.T, s *FlightSQLServer) {
	t.Helper()
	require.NoError(t, s.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
}

// A handler panic is a failed RPC, not a dead process (the main app has
// recover; so must the bridge).
func TestFlightSQLBridgeRecoversFromAHandlerPanic(t *testing.T) {
	s := newBridgeTestServer(t, nil)
	s.bridge.Post("/test/panic", func(*fiber.Ctx) error { panic("boom") })
	s.bridge.Post("/test/ok", func(c *fiber.Ctx) error { return c.SendString("{}") })
	startBridgeTestServer(t, s)

	_, err := s.service.query(context.Background(), "/test/panic", "SELECT 1", "", "")
	require.Error(t, err)
	resp, err := s.service.query(context.Background(), "/test/ok", "SELECT 1", "", "")
	require.NoError(t, err, "the bridge must keep serving after a panic")
	require.NoError(t, resp.Body.Close())
}

// The internal key stays in this process; the client's address and
// User-Agent reach the handler, so the query registry, audit and forwarding
// see the Flight client rather than loopback.
func TestFlightSQLBridgeStripsTheKeyAndCarriesTheClient(t *testing.T) {
	s := newBridgeTestServer(t, nil)
	s.bridge.Post("/test/echo", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"key": c.Get(flightSQLInternalKeyHeader), "ip": c.IP(), "ua": c.Get(fiber.HeaderUserAgent)})
	})
	startBridgeTestServer(t, s)

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 5555}})
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("user-agent", "ADBC Flight SQL Driver"))
	resp, err := s.service.query(ctx, "/test/echo", "SELECT 1", "", "")
	require.NoError(t, err)
	defer resp.Body.Close()
	var got map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Empty(t, got["key"], "the internal key must not reach handlers (forwarding copies headers to peers)")
	require.Equal(t, "203.0.113.7", got["ip"])
	require.Equal(t, "ADBC Flight SQL Driver", got["ua"])
}

// Configured middleware (audit, from cmd/arc) runs after auth, and not for a
// request without the internal key.
func TestFlightSQLBridgeRunsConfiguredMiddlewareAfterAuth(t *testing.T) {
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.Nop())
	require.NoError(t, err)
	defer am.Close()
	token, err := am.CreateToken(context.Background(), "flight-bridge", "", "read", nil)
	require.NoError(t, err)

	var ran, sawToken atomic.Int32
	mw := func(c *fiber.Ctx) error {
		ran.Add(1)
		if auth.GetTokenInfo(c) != nil {
			sawToken.Add(1)
		}
		return c.Next()
	}
	s := newBridgeTestServer(t, am, mw)
	s.bridge.Post("/test/ok", func(c *fiber.Ctx) error { return c.SendString("{}") })
	startBridgeTestServer(t, s)

	resp, err := s.service.query(context.Background(), "/test/ok", "SELECT 1", token, "")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int32(1), ran.Load())
	require.Equal(t, int32(1), sawToken.Load(), "configured middleware must run after auth")

	// Without the key the bridge refuses before any of it runs.
	keyless := *s.service
	keyless.internalKey = "wrong"
	_, err = keyless.query(context.Background(), "/test/ok", "SELECT 1", token, "")
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Equal(t, int32(1), ran.Load())
}

// A connection that never finishes its handshake keeps a graceful stop
// waiting (gRPC's handshake timeout is 120 s). Shutdown closes it after its
// own bound and returns, so the hooks and components after it still run.
func TestFlightSQLShutdownIsBoundedByAStalledConnection(t *testing.T) {
	prevGrace, prevForce := flightSQLShutdownGrace, flightSQLShutdownForce
	flightSQLShutdownGrace, flightSQLShutdownForce = 200*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { flightSQLShutdownGrace, flightSQLShutdownForce = prevGrace, prevForce })

	s := newBridgeTestServer(t, nil)
	require.NoError(t, s.Start())
	conn, err := net.Dial("tcp", s.flightLn.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.Eventually(t, func() bool {
		s.flightLn.mu.Lock()
		defer s.flightLn.mu.Unlock()
		return len(s.flightLn.conns) == 1
	}, 2*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, s.Shutdown(ctx))
	require.Less(t, time.Since(start), 5*time.Second, "shutdown must not wait for the stalled connection")
	// The server writes its settings frame at accept; after that the next
	// thing the client sees must be the close, not a read timeout.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.Copy(io.Discard, conn)
	var ne net.Error
	require.False(t, errors.As(err, &ne) && ne.Timeout(), "the stalled connection must have been closed, got a read timeout")
}

// A bridge request in flight when shutdown starts is cancelled by it, so the
// RPC waiting on it can finish. The request here is sent without a Flight
// call, so nothing holds the server through the grace period and the cancel
// comes at once; TestFlightSQLShutdownGivesAnInFlightCallItsGrace has the
// grace.
func TestFlightSQLShutdownCancelsInFlightBridgeRequests(t *testing.T) {
	prevGrace := flightSQLShutdownGrace
	flightSQLShutdownGrace = 200 * time.Millisecond
	t.Cleanup(func() { flightSQLShutdownGrace = prevGrace })

	s := newBridgeTestServer(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.bridge.Post("/test/hang", func(c *fiber.Ctx) error {
		close(entered)
		<-release
		return c.SendString("{}")
	})
	defer close(release)
	require.NoError(t, s.Start())

	errc := make(chan error, 1)
	go func() {
		_, err := s.service.query(context.Background(), "/test/hang", "SELECT 1", "", "")
		errc <- err
	}()
	<-entered
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()
	select {
	case err := <-errc:
		require.Equal(t, codes.Unavailable, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("an in-flight bridge request was not cancelled by shutdown")
	}
}

// A schema query refused for one measurement leaves that measurement out of
// a GetTables listing; a refusal about the caller fails the listing.
func TestSkipFlightTableSchemaError(t *testing.T) {
	ctx := context.Background()
	for code, skip := range map[codes.Code]bool{
		codes.PermissionDenied:  true,
		codes.NotFound:          true,
		codes.Internal:          false,
		codes.InvalidArgument:   false,
		codes.DeadlineExceeded:  false,
		codes.ResourceExhausted: false,
		codes.Unauthenticated:   false,
		codes.Unavailable:       false,
		codes.Canceled:          false,
	} {
		require.Equal(t, skip, skipFlightTableSchemaError(ctx, status.Error(code, "x")), code.String())
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.False(t, skipFlightTableSchemaError(cancelled, status.Error(codes.PermissionDenied, "x")), "a cancelled RPC aborts")
	require.False(t, skipFlightTableSchemaError(ctx, errors.New("plain")))
}

// Every stage of a shutdown is scaled in proportion and fits in the budget
// Shutdown is given. The scaled values are checked themselves, not only their
// sum: the int64 product used to wrap, and a negative stage still "fits".
func TestFlightSQLShutdownStagesFitTheBudget(t *testing.T) {
	st := flightSQLShutdownStages{grace: 5 * time.Second, force: 2 * time.Second, drain: 2 * time.Second}
	require.Equal(t, 11*time.Second, st.total())
	require.Equal(t, st, st.within(20*time.Second))
	require.Equal(t, st, st.within(11*time.Second))

	// Half of the hooks' share at the default server.shutdown_timeout: 6 s
	// on a standalone node, 3 s in a cluster after the ready drain.
	require.Equal(t, flightSQLShutdownStages{grace: 2727272727, force: 1090909090, drain: 1090909090}, st.within(6*time.Second))
	require.Equal(t, flightSQLShutdownStages{grace: 1363636363, force: 545454545, drain: 545454545}, st.within(3*time.Second))

	check := func(st flightSQLShutdownStages, budget time.Duration, prev flightSQLShutdownStages) flightSQLShutdownStages {
		got := st.within(budget)
		for name, pair := range map[string][3]time.Duration{
			"grace": {got.grace, st.grace, prev.grace},
			"force": {got.force, st.force, prev.force},
			"drain": {got.drain, st.drain, prev.drain},
		} {
			require.GreaterOrEqual(t, pair[0], time.Duration(0), "%s at %s", name, budget)
			require.LessOrEqual(t, pair[0], pair[1], "%s at %s is longer than unscaled", name, budget)
			require.GreaterOrEqual(t, pair[0], pair[2], "%s at %s is shorter than at a smaller budget", name, budget)
		}
		require.LessOrEqual(t, got.total(), max(budget, 0), budget.String())
		if budget >= 0 && budget < st.total() {
			// Four roundings down, one per term of total().
			require.Less(t, budget-got.total(), 4*time.Nanosecond, "%s is not used in full", budget)
		}
		return got
	}
	var prev flightSQLShutdownStages
	for budget := -time.Second; budget <= 12*time.Second; budget += time.Millisecond {
		prev = check(st, budget, prev)
	}
	// Stages far longer than any shipped: still no overflow.
	long := flightSQLShutdownStages{grace: 1000 * time.Hour, force: 500 * time.Hour, drain: 500 * time.Hour}
	prev = flightSQLShutdownStages{}
	for budget := time.Duration(0); budget <= long.total(); budget += long.total() / 997 {
		prev = check(long, budget, prev)
	}

	// Stages a test or a caller may set: zero, and negative (no wait).
	require.Equal(t, flightSQLShutdownStages{}, flightSQLShutdownStages{}.within(-time.Second))
	require.Equal(t, flightSQLShutdownStages{}, flightSQLShutdownStages{}.within(time.Second))
	require.Equal(t, flightSQLShutdownStages{}, st.within(-time.Second))
	odd := flightSQLShutdownStages{grace: 5 * time.Second, force: -2 * time.Second, drain: -2 * time.Second}
	require.Equal(t, flightSQLShutdownStages{grace: time.Second}, odd.within(time.Second))
}

// runFlightTestStatement runs one statement to the end of its DoGet, which is
// the call that crosses the bridge. Its outcome is the test server's business.
func runFlightTestStatement(client *flightsql.Client) {
	info, err := client.Execute(context.Background(), "SELECT 1")
	if err != nil || len(info.Endpoint) == 0 {
		return
	}
	reader, err := client.DoGet(context.Background(), info.Endpoint[0].Ticket)
	if err != nil {
		return
	}
	defer reader.Release()
	for reader.Next() {
	}
}

// A Flight call in flight when shutdown starts keeps running through the
// grace period. With a 20 s context the grace is about 4.5 s; it used to come
// out negative, so the query was cancelled the moment shutdown began.
func TestFlightSQLShutdownGivesAnInFlightCallItsGrace(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	cancelled := make(chan struct{})
	var once sync.Once
	// Every bridge request of the call stops here, under the base context
	// the handlers get, until the test releases it or shutdown cancels it.
	s := newBridgeTestServer(t, nil, func(c *fiber.Ctx) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-c.UserContext().Done():
			select {
			case <-cancelled:
			default:
				close(cancelled)
			}
		}
		return c.Status(fiber.StatusServiceUnavailable).SendString(`{"error":"test"}`)
	})
	require.NoError(t, s.Start())
	client, err := flightsql.NewClient(s.flightLn.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()
	rpcDone := make(chan struct{})
	go func() {
		defer close(rpcDone)
		runFlightTestStatement(client)
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the Flight call did not reach the bridge")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- s.Shutdown(ctx) }()
	require.Eventually(t, func() bool {
		s.shutdownMu.Lock()
		defer s.shutdownMu.Unlock()
		return s.shutdownDone != nil
	}, 5*time.Second, 5*time.Millisecond)

	select {
	case <-cancelled:
		t.Fatal("shutdown cancelled an in-flight call before its grace period")
	case err := <-shutdownErr:
		t.Fatalf("shutdown returned while a call was in flight: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	select {
	case <-rpcDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the released call did not finish")
	}
	require.NoError(t, <-shutdownErr, "the call finished inside the grace period: a clean stop")
}

// An idle server with a client still connected stops cleanly inside the
// hooks' share of the default server.shutdown_timeout (12 s, so a 6 s
// budget). Every stage used to be negative there: the waits all expired at
// once and Shutdown reported that the server did not stop, which made the
// process exit 1.
func TestFlightSQLShutdownOfAnIdleServerIsClean(t *testing.T) {
	s := newBridgeTestServer(t, nil, func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusServiceUnavailable).SendString(`{"error":"test"}`)
	})
	require.NoError(t, s.Start())
	client, err := flightsql.NewClient(s.flightLn.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()
	// One statement, so the client's connection and a bridge connection exist.
	runFlightTestStatement(client)

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
}

// With a short context, a stalled connection and a hung bridge handler,
// Shutdown still returns before the context ends, so the coordinator runs
// the hooks and components after it.
func TestFlightSQLShutdownReturnsWithinAShortContext(t *testing.T) {
	s := newBridgeTestServer(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	s.bridge.Post("/test/hang", func(c *fiber.Ctx) error {
		close(entered)
		<-release
		return c.SendString("{}")
	})
	defer close(release)
	require.NoError(t, s.Start())
	conn, err := net.Dial("tcp", s.flightLn.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	go func() { _, _ = s.service.query(context.Background(), "/test/hang", "SELECT 1", "", "") }()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	err = s.Shutdown(ctx)
	require.NoError(t, ctx.Err(), "Shutdown must return before its context ends")
	_ = err // the bridge drain may report the hung handler; returning in time is what counts
}

// Bridge handlers run under the server's base context, so shutdown cancels
// the query itself.
func TestFlightSQLBridgeHandlersRunUnderTheBaseContext(t *testing.T) {
	s := newBridgeTestServer(t, nil)
	done := make(chan error, 1)
	s.bridge.Post("/test/ctx", func(c *fiber.Ctx) error {
		select {
		case <-c.UserContext().Done():
			done <- c.UserContext().Err()
		case <-time.After(5 * time.Second):
			done <- errors.New("handler context was not cancelled")
		}
		return nil
	})
	require.NoError(t, s.Start())
	go func() { _, _ = s.service.query(context.Background(), "/test/ctx", "SELECT 1", "", "") }()
	time.Sleep(100 * time.Millisecond)
	s.cancelBase()
	require.ErrorIs(t, <-done, context.Canceled)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
}
