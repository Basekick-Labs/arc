package api

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// serve1644 starts s on a loopback port and returns its address.
func serve1644(t *testing.T, s *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.app.Listener(ln) }()
	return ln.Addr().String()
}

func newShutdownTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := DefaultServerConfig()
	return NewServer(cfg, zerolog.Nop())
}

// shutdownWithin runs ShutdownContext with a deadline budget from now and
// returns its error and how long it took.
func shutdownWithin(s *Server, budget time.Duration) (error, time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	err := s.ShutdownContext(ctx, time.Minute)
	return err, time.Since(start)
}

// #1644: a request waiting on the query lifetime (what a running DuckDB
// query does) ends when the grace is over; the drain returns nil, within the
// budget, not after the request's own length.
func TestShutdownCancelsQueriesAfterTheGrace(t *testing.T) {
	s := newShutdownTestServer(t)
	started := make(chan struct{})
	var sawCancel atomic.Bool
	s.app.Get("/hang", func(c *fiber.Ctx) error {
		c.SetUserContext(s.QueryLifetime())
		close(started)
		select {
		case <-c.UserContext().Done():
			sawCancel.Store(true)
			return c.SendStatus(fiber.StatusServiceUnavailable)
		case <-time.After(time.Minute):
			return c.SendStatus(fiber.StatusOK)
		}
	})
	addr := serve1644(t, s)
	go func() { _, _ = http.Get("http://" + addr + "/hang") }()
	<-started

	err, took := shutdownWithin(s, 2*time.Second)
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if !sawCancel.Load() {
		t.Fatal("the request's context was not cancelled")
	}
	// grace = 2s - 2*400ms = 1.2s; the request ends right after it.
	if took < time.Second || took > 2200*time.Millisecond {
		t.Fatalf("drain took %v, want about the 1.2s grace", took)
	}
}

// A response stream blocked on a client that stopped reading sees no
// context: the drain closes its connection, and still returns nil.
func TestShutdownClosesConnectionsAStreamIsBlockedOn(t *testing.T) {
	s := newShutdownTestServer(t)
	started := make(chan struct{}, 1)
	s.app.Get("/stream", func(c *fiber.Ctx) error {
		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			select {
			case started <- struct{}{}:
			default:
			}
			chunk := []byte(strings.Repeat("x", 64<<10))
			for {
				if _, err := w.Write(chunk); err != nil {
					return
				}
				if err := w.Flush(); err != nil {
					return
				}
			}
		})
		return nil
	})
	addr := serve1644(t, s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	<-started // and never read: the writer blocks once the buffers fill

	err, took := shutdownWithin(s, 2*time.Second)
	if err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if took > 2500*time.Millisecond {
		t.Fatalf("drain took %v", took)
	}
}

// A handler that waits on neither its context nor its connection cannot be
// ended: the drain says so, after its stages, instead of hanging.
func TestShutdownReportsARequestItCannotEnd(t *testing.T) {
	s := newShutdownTestServer(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	started := make(chan struct{})
	s.app.Get("/stuck", func(c *fiber.Ctx) error {
		close(started)
		<-release
		return nil
	})
	addr := serve1644(t, s)
	go func() { _, _ = http.Get("http://" + addr + "/stuck") }()
	<-started

	err, took := shutdownWithin(s, time.Second)
	if err == nil {
		t.Fatal("want an error for a request still running")
	}
	if took > 1500*time.Millisecond {
		t.Fatalf("drain took %v", took)
	}
}

// An idle server stops at once, and a spent budget still ends a query: the
// force stages have a floor.
func TestShutdownWithASpentBudget(t *testing.T) {
	s := newShutdownTestServer(t)
	started := make(chan struct{})
	s.app.Get("/hang", func(c *fiber.Ctx) error {
		c.SetUserContext(s.QueryLifetime())
		close(started)
		<-c.UserContext().Done()
		return nil
	})
	addr := serve1644(t, s)
	go func() { _, _ = http.Get("http://" + addr + "/hang") }()
	<-started
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := s.ShutdownContext(ctx, time.Minute); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
}

func TestHTTPShutdownStages(t *testing.T) {
	for _, tc := range []struct {
		budget       time.Duration
		grace, force time.Duration
	}{
		{30 * time.Second, 26 * time.Second, 2 * time.Second},
		{2 * time.Second, 1200 * time.Millisecond, 400 * time.Millisecond},
		{500 * time.Millisecond, 300 * time.Millisecond, 100 * time.Millisecond},
		{150 * time.Millisecond, 0, 100 * time.Millisecond},
		{-time.Second, 0, 100 * time.Millisecond},
	} {
		st := httpShutdownStagesFor(tc.budget)
		if st.grace != tc.grace || st.force != tc.force {
			t.Errorf("budget %v: got %+v, want grace %v force %v", tc.budget, st, tc.grace, tc.force)
		}
	}
}
