package api

// #1629: a forward must not hold the pooled fasthttp request state past its
// handler. net/http dials, and writes a request body, from goroutines that can
// outlive the router call; the forward's context therefore never reaches the
// *fasthttp.RequestCtx (whose Value reads the request's user values, i.e. fiber
// Locals), and its body is cut off from fasthttp's buffer once the forward
// returns.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// forwardProbeKey is a fiber Local (a fasthttp user value) the test apps set
// on every request. A forward's context that still reaches the RequestCtx
// returns it from Value.
const forwardProbeKey = "forward-probe"

func probeMiddleware(c *fiber.Ctx) error {
	c.Locals(forwardProbeKey, "x")
	return c.Next()
}

// dialProbe records, for every connection a forward opens, what the dial's
// context returns for forwardProbeKey.
type dialProbe struct {
	mu   sync.Mutex
	seen []any
}

func (p *dialProbe) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	p.mu.Lock()
	p.seen = append(p.seen, ctx.Value(forwardProbeKey))
	p.mu.Unlock()
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// requireClean asserts the forward dialed (a reused connection would make
// the probe vacuous) and that no dial's context reached the request's values.
func (p *dialProbe) requireClean(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NotEmpty(t, p.seen, "the forward never dialed: the probe saw nothing")
	for _, v := range p.seen {
		require.Nil(t, v, "a forward's dial context reads the pooled fasthttp request's values")
	}
}

// probePeer answers every request with status after reading the body, and
// records the framing the body arrived with.
type probePeer struct {
	status int
	mu     sync.Mutex
	got    []string // "<content-length> <transfer-encoding> <bytes read>"
}

func (p *probePeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n, _ := io.Copy(io.Discard, r.Body)
	p.mu.Lock()
	p.got = append(p.got, fmt.Sprintf("%s %v %d", r.Header.Get("Content-Length"), r.TransferEncoding, n))
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(p.status)
	_, _ = io.WriteString(w, `{}`)
}

// probeRouter is a router on a node of role local whose one peer, of role
// peerRole, is srv. Every forward dials through probe.
func probeRouter(t *testing.T, local, peerRole cluster.NodeRole, srv *httptest.Server, probe *dialProbe, standby bool) *cluster.Router {
	t.Helper()
	localNode := cluster.NewNode("local", "Local", local, "c")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: localNode, Logger: zerolog.Nop()})
	peer := cluster.NewNode("peer", "Peer", peerRole, "c")
	peer.UpdateState(cluster.StateHealthy)
	peer.APIAddress = strings.TrimPrefix(srv.URL, "http://")
	require.NoError(t, registry.Register(peer))
	cfg := &cluster.RouterConfig{
		Timeout:            5 * time.Second,
		Registry:           registry,
		LocalNode:          localNode,
		Logger:             zerolog.Nop(),
		Transport:          &http.Transport{DialContext: probe.dial},
		WriteDialContext:   probe.dial,
		PeerReadQueries:    func(string) bool { return true },
		PeerDatabaseCreate: func(string) bool { return true },
	}
	if standby {
		cfg.StandbyWriteTarget = func() (*cluster.Node, bool) { return peer, true }
	}
	return cluster.NewRouter(cfg)
}

func probeBuffer(t *testing.T) *ingest.ArrowBuffer {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	return buf
}

// writeApp is a forwarding app with every write-class route: line protocol,
// MessagePack, TLE, imports and database creation.
func writeApp(t *testing.T, router *cluster.Router) *fiber.App {
	t.Helper()
	buf := probeBuffer(t)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(probeMiddleware)
	lp := NewLineProtocolHandler(buf, zerolog.Nop())
	lp.SetRouter(router)
	lp.RegisterRoutes(app)
	mp := NewMsgPackHandler(zerolog.Nop(), buf, 10<<20)
	mp.SetRouter(router)
	mp.RegisterRoutes(app)
	tle := NewTLEHandler(buf, zerolog.Nop())
	tle.SetRouter(router)
	tle.RegisterRoutes(app)
	imp := NewImportHandler(zerolog.Nop())
	imp.SetRouter(router)
	imp.RegisterRoutes(app)
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	dbs := NewDatabasesHandler(backend, &config.DeleteConfig{}, nil, zerolog.Nop())
	dbs.SetRouter(router)
	dbs.RegisterRoutes(app)
	return app
}

var forwardedWrites = []struct{ path, body string }{
	{"/api/v1/write/line-protocol", "cpu,host=a value=1 1700000000000000000\n"},
	{"/write?db=db", "cpu,host=a value=1 1700000000000000000\n"},
	{"/api/v1/write/msgpack", "\x81\xa1m\xa3cpu"},
	{"/api/v1/write/tle", "ISS\n1 25544U\n2 25544\n"},
	{"/api/v1/import/lp", "cpu value=1\n"},
	{"/api/v1/import/csv", "a,b\n1,2\n"},
	{"/api/v1/databases", `{"name":"probe"}`},
	// Around forwardBodyCopyMax: copied at the limit, read through a
	// forwardBody above it.
	{"/api/v1/write/line-protocol?n=at", lpOfLen(forwardBodyCopyMax)},
	{"/api/v1/write/line-protocol?n=above", lpOfLen(forwardBodyCopyMax + 1)},
	{"/api/v1/write/line-protocol?n=large", lpOfLen(1 << 20)},
	{"/api/v1/import/lp?n=large", lpOfLen(1 << 20)},
}

// lpOfLen is line protocol of exactly n bytes.
func lpOfLen(n int) string {
	line := "cpu,host=a value=1 1700000000000000000\n"
	s := strings.Repeat(line, n/len(line)+1)[:n-1]
	return s + "\n"
}

// probeRequest is a POST of body to path; an import's body is wrapped in a
// multipart upload. n is the length sent.
func probeRequest(t *testing.T, path, body string) (req *http.Request, n int) {
	t.Helper()
	raw, contentType := []byte(body), "application/json"
	switch {
	case strings.HasPrefix(path, "/api/v1/import/"):
		raw, contentType = multipartUpload(t, raw)
	case strings.Contains(path, "msgpack"):
		contentType = "application/msgpack"
	}
	req = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("x-arc-database", "db")
	req.Header.Set("Content-Type", contentType)
	return req, len(raw)
}

func mustProbeRequest(t *testing.T, path, body string) *http.Request {
	req, _ := probeRequest(t, path, body)
	return req
}

// Every write-class forward (a reader sending to the writer) dials on a
// context that does not reach the request's fasthttp user values, and sends
// the body as received with its Content-Length (not chunked).
func TestForwardedWritesDoNotReachTheRequestContext(t *testing.T) {
	for _, w := range forwardedWrites {
		t.Run(w.path, func(t *testing.T) {
			peer := &probePeer{status: http.StatusOK}
			srv := httptest.NewServer(peer)
			defer srv.Close()
			probe := &dialProbe{}
			app := writeApp(t, probeRouter(t, cluster.RoleReader, cluster.RoleWriter, srv, probe, false))

			req, sent := probeRequest(t, w.path, w.body)
			resp, err := app.Test(req, 10000)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			probe.requireClean(t)
			peer.mu.Lock()
			defer peer.mu.Unlock()
			n := strconv.Itoa(sent)
			require.Equal(t, []string{n + " [] " + n}, peer.got, "the peer got another framing or body")
		})
	}
}

// A body of at most forwardBodyCopyMax is copied: the request does not alias
// fasthttp's buffer, which is reused once the handler returns.
func TestSmallForwardBodyIsACopy(t *testing.T) {
	for _, n := range []int{1, forwardBodyCopyMax} {
		app := fiber.New()
		var got []byte
		app.Post("/w", func(c *fiber.Ctx) error {
			req, release, err := BuildHTTPRequest(c, context.Background())
			if err != nil {
				return err
			}
			defer release()
			raw := c.Request().Body()
			for i := range raw {
				raw[i] = 'X' // what a later request reusing the buffer would do
			}
			got, err = io.ReadAll(req.Body)
			return err
		})
		body := strings.Repeat("a", n)
		resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/w", strings.NewReader(body)), 10000)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, body, string(got), "the forwarded body aliases fasthttp's buffer (n=%d)", n)
	}
}

// The standby writer's forward to the primary (#1270), write and import.
func TestStandbyForwardDoesNotReachTheRequestContext(t *testing.T) {
	for _, path := range []string{"/api/v1/write/line-protocol", "/api/v1/import/lp"} {
		t.Run(path, func(t *testing.T) {
			peer := &probePeer{status: http.StatusOK}
			srv := httptest.NewServer(peer)
			defer srv.Close()
			probe := &dialProbe{}
			app := writeApp(t, probeRouter(t, cluster.RoleWriter, cluster.RoleWriter, srv, probe, true))
			body := "cpu,host=a value=1 1700000000000000000\n"
			resp, err := app.Test(mustProbeRequest(t, path, body), 10000)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			probe.requireClean(t)
			peer.mu.Lock()
			defer peer.mu.Unlock()
			require.Len(t, peer.got, 1)
		})
	}
}

// A forwarded query (a compactor sending /api/v1/query to a reader).
func TestForwardedQueryDoesNotReachTheRequestContext(t *testing.T) {
	peer := &probePeer{status: http.StatusOK}
	srv := httptest.NewServer(peer)
	defer srv.Close()
	probe := &dialProbe{}
	qh := NewQueryHandler(nil, nil, zerolog.Nop(), 0, 0)
	qh.SetRouter(probeRouter(t, cluster.RoleCompactor, cluster.RoleReader, srv, probe, false))
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Use(probeMiddleware)
	qh.RegisterRoutes(app)

	body := `{"sql":"SELECT 1"}`
	resp, err := app.Test(mustProbeRequest(t, "/api/v1/query", body), 10000)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	probe.requireClean(t)
}

// A request served here never builds the forward's context (it is set up
// only behind the forward decision): a standalone node (no router) and a
// writer that ingests itself. A forwarded write builds exactly one.
func TestForwardSetupOnlyBehindTheDecision(t *testing.T) {
	lpBody := "cpu,host=a value=1 1700000000000000000\n"
	local := func(t *testing.T, router *cluster.Router) {
		before := forwardContextsBuilt.Load()
		app := writeApp(t, router)
		qh := NewQueryHandler(nil, nil, zerolog.Nop(), 0, 0)
		if router != nil {
			qh.SetRouter(router)
		}
		qh.RegisterRoutes(app)
		for _, path := range []string{"/api/v1/write/line-protocol", "/api/v1/import/lp", "/api/v1/query"} {
			resp, err := app.Test(mustProbeRequest(t, path, lpBody), 10000)
			require.NoError(t, err)
			_ = resp.Body.Close()
		}
		require.Equal(t, before, forwardContextsBuilt.Load(), "a request served here built a forward context")
	}
	t.Run("standalone", func(t *testing.T) { local(t, nil) })
	t.Run("writer", func(t *testing.T) {
		srv := httptest.NewServer(&probePeer{status: http.StatusOK})
		defer srv.Close()
		local(t, probeRouter(t, cluster.RoleWriter, cluster.RoleWriter, srv, &dialProbe{}, false))
	})
	t.Run("forwarded", func(t *testing.T) {
		srv := httptest.NewServer(&probePeer{status: http.StatusOK})
		defer srv.Close()
		app := writeApp(t, probeRouter(t, cluster.RoleReader, cluster.RoleWriter, srv, &dialProbe{}, false))
		before := forwardContextsBuilt.Load()
		resp, err := app.Test(mustProbeRequest(t, "/api/v1/write/line-protocol", lpBody), 10000)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, before+1, forwardContextsBuilt.Load())
	})
}

// lateWriterConn is the forwarding node's connection to a writer that
// answers 413 as soon as it has the request headers, before the body. Writes
// that start past gateAt wait for open, so the transport is still sending the
// body when the forward returns. Writes keep succeeding after Close (the
// transport closes a connection whose answer came first, but its write loop
// may still be inside a Write and go on reading the body): what the transport
// reads from the body after the handler returned is what is counted.
type lateWriterConn struct {
	gateAt  int
	open    chan struct{}
	resp    *io.PipeReader
	respW   *io.PipeWriter
	mu      sync.Mutex
	head    []byte
	body    int // bytes after the request headers
	headEnd bool
	full    chan struct{} // closed once body reaches want
	want    int
}

func newLateWriterConn(gateAt, want int, open chan struct{}) *lateWriterConn {
	r, w := io.Pipe()
	return &lateWriterConn{gateAt: gateAt, open: open, resp: r, respW: w, want: want, full: make(chan struct{})}
}

func (c *lateWriterConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	sent := len(c.head) + c.body
	c.mu.Unlock()
	if sent >= c.gateAt {
		<-c.open
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.headEnd {
		c.head = append(c.head, p...)
		if i := bytes.Index(c.head, []byte("\r\n\r\n")); i >= 0 {
			c.headEnd = true
			c.body = len(c.head) - i - 4
			c.head = c.head[:i+4]
			go func() {
				_, _ = io.WriteString(c.respW, "HTTP/1.1 413 Request Entity Too Large\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")
			}()
		}
	} else {
		c.body += len(p)
	}
	if c.headEnd && c.body >= c.want {
		select {
		case <-c.full:
		default:
			close(c.full)
		}
	}
	return len(p), nil
}

func (c *lateWriterConn) Read(p []byte) (int, error)       { return c.resp.Read(p) }
func (c *lateWriterConn) Close() error                     { _ = c.respW.Close(); return nil }
func (c *lateWriterConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *lateWriterConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *lateWriterConn) SetDeadline(time.Time) error      { return nil }
func (c *lateWriterConn) SetReadDeadline(time.Time) error  { return nil }
func (c *lateWriterConn) SetWriteDeadline(time.Time) error { return nil }

// A writer that answers before reading the whole body (a 413, a 401): the
// transport is still writing the body when the forward returns, and fasthttp
// reuses that buffer once the handler returns. Nothing more of it may be read
// after the handler returned.
func TestForwardedWriteBodyIsReleasedWithTheForward(t *testing.T) {
	body := strings.Repeat("cpu,host=a value=1 1700000000000000000\n", (4<<20)/40)
	open := make(chan struct{})
	conn := newLateWriterConn(64<<10, len(body), open)
	srv := &httptest.Server{URL: "http://127.0.0.1:1"}
	router := cluster.NewRouter(&cluster.RouterConfig{
		Registry:  routerRegistry(t, srv),
		LocalNode: cluster.NewNode("local", "Local", cluster.RoleReader, "c"),
		Logger:    zerolog.Nop(),
		WriteDialContext: func(context.Context, string, string) (net.Conn, error) {
			return conn, nil
		},
		WriteTimeout: 10 * time.Second,
	})
	app := writeApp(t, router)

	resp, err := app.Test(mustProbeRequest(t, "/api/v1/write/line-protocol", body), 10000)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	conn.mu.Lock()
	require.True(t, conn.headEnd, "the forward never sent its headers")
	require.Less(t, conn.body, len(body), "the body was sent before the answer: the probe saw nothing")
	conn.mu.Unlock()
	// The handler has returned (app.Test waits for it): let the transport go on.
	close(open)
	select {
	case <-conn.full:
		t.Fatal("the transport went on reading the request body after the forward returned")
	case <-time.After(2 * time.Second):
	}
}

// routerRegistry is a registry on a reader whose one writer is srv.
func routerRegistry(t *testing.T, srv *httptest.Server) *cluster.Registry {
	t.Helper()
	local := cluster.NewNode("local", "Local", cluster.RoleReader, "c")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	w := cluster.NewNode("writer", "Writer", cluster.RoleWriter, "c")
	w.UpdateState(cluster.StateHealthy)
	w.APIAddress = strings.TrimPrefix(srv.URL, "http://")
	require.NoError(t, registry.Register(w))
	return registry
}

// StopForwards (wired to the HTTP server's shutdown start) ends a forward in
// flight, as fasthttp's request context ending at shutdown used to.
func TestStopForwardsEndsAForwardInFlight(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		once.Do(func() { close(entered) })
		<-r.Context().Done()
	}))
	defer srv.Close()
	router := probeRouter(t, cluster.RoleReader, cluster.RoleWriter, srv, &dialProbe{}, false)
	app := writeApp(t, router)

	s := &Server{app: fiber.New(fiber.Config{DisableStartupMessage: true}), logger: zerolog.Nop()}
	s.OnShutdownStart(router.StopForwards)
	done := make(chan int, 1)
	go func() {
		resp, err := app.Test(mustProbeRequest(t, "/api/v1/write/line-protocol", "cpu value=1\n"), -1)
		if err != nil {
			done <- -1
			return
		}
		_ = resp.Body.Close()
		done <- resp.StatusCode
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the forward never reached the writer")
	}
	require.NoError(t, s.Shutdown(time.Second))
	select {
	case status := <-done:
		// Sent, then cut: ambiguous, 504.
		require.Equal(t, http.StatusGatewayTimeout, status)
	case <-time.After(5 * time.Second):
		t.Fatal("StopForwards did not end the forward in flight")
	}
}
