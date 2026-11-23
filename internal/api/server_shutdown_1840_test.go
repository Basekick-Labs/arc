//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// #1840: what the HTTP drain does with a connection that was accepted and
// never sent a request.

// lockedBuffer is a log sink the test can read while the server writes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

const (
	logCancelledQueries = "cancelled the queries still running"
	logClosedStillOpen  = "closed connections still open"
	logClosedUnused     = "closed connections that never sent a request"
)

func newLoggedShutdownTestServer(t *testing.T) (*Server, *lockedBuffer) {
	t.Helper()
	logs := &lockedBuffer{}
	return NewServer(DefaultServerConfig(), zerolog.New(logs)), logs
}

func setShutdownNewConnAllowance(t *testing.T, d time.Duration) {
	t.Helper()
	prev := shutdownNewConnAllowance
	shutdownNewConnAllowance = d
	t.Cleanup(func() { shutdownNewConnAllowance = prev })
}

// dialTracked opens a connection and waits until the server has accepted it.
func dialTracked(t *testing.T, s *Server, addr string) net.Conn {
	t.Helper()
	before := trackedConns(s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for trackedConns(s) <= before {
		if time.Now().After(deadline) {
			t.Fatal("the server did not accept the connection")
		}
		time.Sleep(time.Millisecond)
	}
	return conn
}

func trackedConns(s *Server) int {
	s.conns.mu.Lock()
	defer s.conns.mu.Unlock()
	return len(s.conns.conns)
}

// closedByServer reads conn to its end and reports whether the server closed
// it (an EOF or a reset) before the deadline.
func closedByServer(conn net.Conn, within time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(within))
	_, err := io.Copy(io.Discard, conn)
	var ne net.Error
	return !(errors.As(err, &ne) && ne.Timeout())
}

func requireNoLog(t *testing.T, logs *lockedBuffer, unwanted ...string) {
	t.Helper()
	out := logs.String()
	for _, u := range unwanted {
		if strings.Contains(out, u) {
			t.Fatalf("unexpected log %q in:\n%s", u, out)
		}
	}
}

// A connection that never sent a request is closed once it is the allowance
// old, counted from accept, and the drain ends there: not at the end of the
// grace, and with no word about queries.
func TestShutdownClosesAConnectionThatNeverSentARequest(t *testing.T) {
	setShutdownNewConnAllowance(t, 300*time.Millisecond)
	s, logs := newLoggedShutdownTestServer(t)
	addr := serve1644(t, s)
	conn := dialTracked(t, s, addr)

	// Budget 10 s: grace 6 s. fasthttp alone would close this connection
	// after its own 5 s.
	err, took := shutdownWithin(s, 10*time.Second)
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if took > 2*time.Second {
		t.Fatalf("drain took %v, want about the 300ms allowance", took)
	}
	if !closedByServer(conn, 2*time.Second) {
		t.Fatal("the unused connection was not closed")
	}
	requireNoLog(t, logs, logCancelledQueries, logClosedStillOpen)
	if !strings.Contains(logs.String(), logClosedUnused) {
		t.Fatalf("no log of the closed connection:\n%s", logs.String())
	}
}

// With TLS, a connection that never finishes its handshake is closed the
// same way. fasthttp starts its own 5 s only after the handshake, so it
// never closed this one and the drain ran its whole grace, then reported
// queries cancelled and connections closed.
func TestShutdownClosesAConnectionThatNeverHandshakes(t *testing.T) {
	setShutdownNewConnAllowance(t, 300*time.Millisecond)
	s, logs := newLoggedShutdownTestServer(t)
	certFile, keyFile, _ := createFlightSQLTestTLS(t)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = s.app.Listener(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}))
	}()
	conn := dialTracked(t, s, ln.Addr().String()) // plain TCP: no ClientHello

	err, took := shutdownWithin(s, 10*time.Second)
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if took > 2*time.Second {
		t.Fatalf("drain took %v, want about the 300ms allowance", took)
	}
	if !closedByServer(conn, 2*time.Second) {
		t.Fatal("the connection was not closed")
	}
	requireNoLog(t, logs, logCancelledQueries, logClosedStillOpen)
}

// The allowance protects a client whose first request is still on its way:
// a connection accepted before the drain that sends its request inside the
// allowance is answered.
func TestShutdownAnswersAFirstRequestInsideTheAllowance(t *testing.T) {
	s, _ := newLoggedShutdownTestServer(t) // the shipped 5 s allowance
	s.app.Get("/ok", func(c *fiber.Ctx) error { return c.SendString("ok") })
	addr := serve1644(t, s)
	conn := dialTracked(t, s, addr)

	done := make(chan error, 1)
	go func() {
		err, _ := shutdownWithin(s, 10*time.Second)
		done <- err
	}()
	time.Sleep(300 * time.Millisecond) // several sweeps into the drain
	if _, err := conn.Write([]byte("GET /ok HTTP/1.1\r\nHost: x\r\n\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("the request was not answered: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not return after the request was answered")
	}
}

// When the grace is shorter than the allowance, the connection is closed at
// the end of the grace, and the drain does not report queries cancelled or
// connections closed under them: there were none.
func TestShutdownWithAGraceShorterThanTheAllowance(t *testing.T) {
	s, logs := newLoggedShutdownTestServer(t) // the shipped 5 s allowance
	addr := serve1644(t, s)
	conn := dialTracked(t, s, addr)

	// Budget 2 s: grace 1.2 s, each force stage 400 ms.
	err, took := shutdownWithin(s, 2*time.Second)
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if took < time.Second || took > 1900*time.Millisecond {
		t.Fatalf("drain took %v, want the 1.2s grace and one check after it", took)
	}
	if !closedByServer(conn, 2*time.Second) {
		t.Fatal("the unused connection was not closed")
	}
	requireNoLog(t, logs, logCancelledQueries, logClosedStillOpen)
	if !strings.Contains(logs.String(), logClosedUnused) {
		t.Fatalf("no log of the closed connection:\n%s", logs.String())
	}
}

// A request that is running when the grace ends is still reported, with the
// unused connection beside it not counted.
func TestShutdownStillReportsQueriesItCancelled(t *testing.T) {
	s, logs := newLoggedShutdownTestServer(t)
	started := make(chan struct{})
	s.app.Get("/hang", func(c *fiber.Ctx) error {
		c.SetUserContext(s.QueryLifetime())
		close(started)
		<-c.UserContext().Done()
		return c.SendStatus(fiber.StatusServiceUnavailable)
	})
	addr := serve1644(t, s)
	_ = dialTracked(t, s, addr) // never used
	go func() { _, _ = http.Get("http://" + addr + "/hang") }()
	<-started

	if err, _ := shutdownWithin(s, 2*time.Second); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, logCancelledQueries) || !strings.Contains(out, `"connections":1,"message":"HTTP shutdown: cancelled`) {
		t.Fatalf("want the cancelled-queries warning for 1 connection:\n%s", out)
	}
}

// The tracker's view of a connection: unused until the first byte of a
// request, and counted once by the calls that close it.
func TestConnTrackerUnusedConnections(t *testing.T) {
	tr := newConnTracker()
	a, aPeer := net.Pipe()
	b, bPeer := net.Pipe()
	defer aPeer.Close()
	defer bPeer.Close()
	tr.connState(a, fasthttp.StateNew)
	tr.connState(b, fasthttp.StateNew)
	tr.connState(b, fasthttp.StateActive)
	tr.connState(b, fasthttp.StateIdle)

	if n := tr.closeUnused(time.Hour); n != 0 {
		t.Fatalf("closed %d connections younger than the allowance", n)
	}
	if n := tr.usedOpen(); n != 1 {
		t.Fatalf("usedOpen = %d, want 1", n)
	}
	if n := tr.closeUnused(0); n != 1 {
		t.Fatalf("closeUnused = %d, want 1 (a only)", n)
	}
	if n := tr.closeUnused(0); n != 0 {
		t.Fatalf("closeUnused closed %d connections a second time", n)
	}
	// a is closing but fasthttp has not reported it: closeAll counts b only.
	if n := tr.closeAll(); n != 1 {
		t.Fatalf("closeAll = %d, want 1", n)
	}
	if n := tr.usedOpen(); n != 0 {
		t.Fatalf("usedOpen = %d after closeAll, want 0", n)
	}
	tr.connState(a, fasthttp.StateClosed)
	tr.connState(b, fasthttp.StateClosed)
	if len(tr.conns) != 0 {
		t.Fatalf("%d connections left after their close was reported", len(tr.conns))
	}
	var nilTracker *connTracker
	if nilTracker.closeAll() != 0 || nilTracker.closeUnused(0) != 0 || nilTracker.usedOpen() != 0 {
		t.Fatal("a nil tracker must report nothing")
	}
}
