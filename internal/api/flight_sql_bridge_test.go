//go:build duckdb_arrow

package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
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

	_, err := s.service.query(context.Background(), "/test/panic", "SELECT 1", "")
	require.Error(t, err)
	resp, err := s.service.query(context.Background(), "/test/ok", "SELECT 1", "")
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
	resp, err := s.service.query(ctx, "/test/echo", "SELECT 1", "")
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

	resp, err := s.service.query(context.Background(), "/test/ok", "SELECT 1", token)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, int32(1), ran.Load())
	require.Equal(t, int32(1), sawToken.Load(), "configured middleware must run after auth")

	// Without the key the bridge refuses before any of it runs.
	keyless := *s.service
	keyless.internalKey = "wrong"
	_, err = keyless.query(context.Background(), "/test/ok", "SELECT 1", token)
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

// A bridge request in flight when shutdown starts is cancelled once the grace
// period ends, so the RPC waiting on it can finish.
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
		_, err := s.service.query(context.Background(), "/test/hang", "SELECT 1", "")
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
		codes.Internal:          true,
		codes.InvalidArgument:   true,
		codes.ResourceExhausted: false,
		codes.Unauthenticated:   false,
		codes.Unavailable:       false,
		codes.Canceled:          false,
	} {
		require.Equal(t, skip, skipFlightTableSchemaError(ctx, status.Error(code, "x")), code.String())
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.False(t, skipFlightTableSchemaError(cancelled, status.Error(codes.Internal, "x")), "a cancelled RPC aborts")
	require.False(t, skipFlightTableSchemaError(ctx, errors.New("plain")))
}
