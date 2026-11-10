//go:build duckdb_arrow

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// A compactor forwards every read it cannot serve (#1567 and its follow-up),
// to peers that accept them; with peers but none capable it answers itself;
// with no peer at all it answers 503.

// readPeer records what it was asked and answers a marker document.
type readPeer struct {
	mu    sync.Mutex
	calls []string
	hits  atomic.Int32
}

func (p *readPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.hits.Add(1)
	p.mu.Lock()
	p.calls = append(p.calls, r.Method+" "+r.URL.Path)
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"from":"peer"}`))
}

// readRouter routes from a compactor; withPeer false leaves the cluster with
// no node to serve a read.
func readRouter(t *testing.T, peerURL string, capable, withPeer bool, timeout ...time.Duration) *cluster.Router {
	t.Helper()
	requestTimeout := time.Second
	if len(timeout) > 0 {
		requestTimeout = timeout[0]
	}
	local := cluster.NewNode("local", "Local", cluster.RoleCompactor, "test-cluster")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	if withPeer {
		reader := cluster.NewNode("reader-1", "Reader", cluster.RoleReader, "test-cluster")
		reader.UpdateState(cluster.StateHealthy)
		reader.APIAddress = strings.TrimPrefix(peerURL, "http://")
		require.NoError(t, registry.Register(reader))
	}
	return cluster.NewRouter(&cluster.RouterConfig{
		Timeout:         requestTimeout,
		Registry:        registry,
		LocalNode:       local,
		Logger:          zerolog.Nop(),
		PeerReadQueries: func(string) bool { return capable },
	})
}

var forwardedReads = []struct{ method, path, body string }{
	{"POST", "/api/v1/query/estimate", `{"sql":"SELECT 1"}`},
	{"GET", "/api/v1/query/cpu", ""},
	{"GET", "/api/v1/measurements", ""},
	{"GET", "/api/v1/databases/tenanta/measurements/cpu/schema", ""},
	{"GET", "/api/v1/databases", ""},
	{"GET", "/api/v1/databases/tenanta", ""},
	{"GET", "/api/v1/databases/tenanta/measurements", ""},
}

func (r *doorRig) read(t *testing.T, method, path, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return r.do(t, "coarse-no-team", req)
}

func TestCompactorForwardsEveryRead(t *testing.T) {
	for _, rd := range forwardedReads {
		t.Run(rd.method+" "+rd.path, func(t *testing.T) {
			r := newDoorRig(t)
			peer := &readPeer{}
			srv := httptest.NewServer(peer)
			defer srv.Close()

			// Capable peer: forwarded, the peer's answer relayed.
			router := readRouter(t, srv.URL, true, true)
			r.qh.SetRouter(router)
			r.dbs.SetRouter(router)
			status, body := r.read(t, rd.method, rd.path, rd.body)
			require.Equal(t, http.StatusOK, status, body)
			require.Contains(t, body, `"from":"peer"`)
			require.Equal(t, []string{rd.method + " " + rd.path}, peer.calls)

			// Peers, none capable (rolling upgrade): answered here.
			router = readRouter(t, srv.URL, false, true)
			r.qh.SetRouter(router)
			r.dbs.SetRouter(router)
			_, body = r.read(t, rd.method, rd.path, rd.body)
			require.NotContains(t, body, `"from":"peer"`)
			require.Equal(t, int32(1), peer.hits.Load(), "forwarded to a peer without the capability")

			// No peer at all: 503, not a local answer.
			router = readRouter(t, srv.URL, true, false)
			r.qh.SetRouter(router)
			r.dbs.SetRouter(router)
			status, body = r.read(t, rd.method, rd.path, rd.body)
			require.Equal(t, http.StatusServiceUnavailable, status, body)
		})
	}
}

// Every route the app registers is forwardable exactly when it is meant to
// be: a new route under a forwardable shape cannot silently accept a
// forwarded identity.
func TestOnlyTheIntendedRoutesAreForwardable(t *testing.T) {
	r := newDoorRig(t)
	want := map[string]bool{
		"POST /api/v1/query": true, "POST /api/v1/query/msgpack": true, "POST /api/v1/query/arrow": true,
		"POST /api/v1/query/estimate": true, "GET /api/v1/query/:measurement": true,
		"GET /api/v1/measurements": true, "GET /api/v1/databases/:database/measurements/:measurement/schema": true,
		"GET /api/v1/databases": true, "GET /api/v1/databases/:name": true, "GET /api/v1/databases/:name/measurements": true,
		"POST /write": true, "POST /api/v2/write": true, "POST /api/v1/write/line-protocol": true,
		"POST /api/v1/write/msgpack": true, "POST /api/v1/write/tle": true,
	}
	seen := 0
	for _, rt := range r.app.GetRoutes(true) {
		if rt.Method == "USE" || strings.Contains(rt.Path, "*") {
			continue
		}
		concrete := rt.Path
		for _, seg := range strings.Split(rt.Path, "/") {
			if strings.HasPrefix(seg, ":") {
				concrete = strings.Replace(concrete, seg, "x", 1)
			}
		}
		key := rt.Method + " " + rt.Path
		// HEAD is served by the GET handler, so it follows its GET route.
		expect := want[key]
		if rt.Method == "HEAD" {
			expect = want["GET "+rt.Path]
		}
		if got := forwardableRoute(rt.Method, concrete); got != expect {
			t.Errorf("%s: forwardable = %v, want %v", key, got, expect)
		}
		if want[key] {
			seen++
		}
	}
	require.Equal(t, len(want), seen, "an intended forwardable route is not registered by the rig")
}

// A forward is registered in the compactor's query registry, so
// /api/v1/queries lists it (with its target) and can cancel it; cancelling
// closes the connection to the peer.
func TestForwardedQueryIsListedAndCancellable(t *testing.T) {
	r := newDoorRig(t)
	// Authenticate before starting the registry/cancellation deadline: a cold
	// PBKDF2 verification can exceed it under race instrumentation and CPU load.
	// The request still passes through the real auth middleware and its cache.
	require.NotNil(t, r.am.VerifyToken(r.tokens["coarse-no-team"]))
	reg := queryregistry.NewRegistry(nil, zerolog.Nop())
	r.qh.queryRegistry = reg
	peerCancelled := make(chan struct{})
	peerStarted := make(chan struct{})
	releasePeer := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Read the body first, as a real peer does before running the query:
		// net/http notices a dropped connection only after that.
		_, _ = io.ReadAll(req.Body)
		close(peerStarted)
		select {
		case <-req.Context().Done(): // a long query; ends when the forward is cut
			close(peerCancelled)
		case <-releasePeer: // release the handler if an assertion fails
		}
	}))
	defer srv.Close()
	defer close(releasePeer) // runs before Close, including on test failure
	r.qh.SetRouter(readRouter(t, srv.URL, true, true, 30*time.Second))

	done := make(chan int, 1)
	go func() {
		status, _ := r.read(t, "POST", "/api/v1/query", `{"sql":"SELECT 42"}`)
		done <- status
	}()

	var entry *queryregistry.TrackedQuery
	require.Eventually(t, func() bool {
		for _, q := range reg.GetActive() {
			if q.SQL == "SELECT 42" && q.ForwardedTo != "" {
				entry = q
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the forward is not listed with its target")
	require.Equal(t, "reader-1", entry.ForwardedTo)

	// The registry target is set before dialing. Wait for an established
	// peer request before asserting that cancelling closes that connection.
	select {
	case <-peerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarded request did not reach the peer")
	}
	require.True(t, reg.Cancel(entry.ID))
	select {
	case <-peerCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling the forward did not reach the peer")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the forwarded request did not end after cancel")
	}
	require.Empty(t, reg.GetActive())
}

// A read that falls back to this node (no capable peer) leaves no phantom
// "running" entry: the forward's registration is discarded. (/query itself
// is not capability-gated, so an estimate exercises the fallback.)
func TestForwardFallbackLeavesNoPhantomEntry(t *testing.T) {
	r := newDoorRig(t)
	reg := queryregistry.NewRegistry(nil, zerolog.Nop())
	r.qh.queryRegistry = reg
	srv := httptest.NewServer(&readPeer{})
	defer srv.Close()
	r.qh.SetRouter(readRouter(t, srv.URL, false, true))

	_, body := r.read(t, "POST", "/api/v1/query/estimate", `{"sql":"SELECT 7"}`)
	require.NotContains(t, body, `"from":"peer"`)
	require.Empty(t, reg.GetActive(), "a discarded forward registration is still active")
	for _, q := range reg.GetHistory(10) {
		require.Empty(t, q.ForwardedTo, "a local answer recorded as forwarded: %+v", q)
	}
}

// The X-Arc-Query-ID a client gets from a forwarded query is this node's, so
// DELETE /api/v1/queries/<it> here finds it; the answering node's ID goes in
// X-Arc-Forwarded-Query-ID.
func TestForwardedQueryCarriesThisNodesQueryID(t *testing.T) {
	r := newDoorRig(t)
	reg := queryregistry.NewRegistry(nil, zerolog.Nop())
	r.qh.queryRegistry = reg
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.ReadAll(req.Body)
		w.Header().Set("X-Arc-Query-ID", "peer-id-123")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()
	r.qh.SetRouter(readRouter(t, srv.URL, true, true))

	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(`{"sql":"SELECT 9"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.tokens["coarse-no-team"])
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	hist := reg.GetHistory(1)
	require.Len(t, hist, 1)
	require.Equal(t, hist[0].ID, resp.Header.Get("X-Arc-Query-ID"))
	require.Equal(t, "peer-id-123", resp.Header.Get("X-Arc-Forwarded-Query-ID"))
}

// A forwarded listing is bounded as a local one is: a hung peer gets 504.
func TestForwardedListingIsBounded(t *testing.T) {
	r := newDoorRig(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	r.qh.SetRouter(readRouter(t, srv.URL, true, true))
	old := listingForwardTimeout
	listingForwardTimeout = 200 * time.Millisecond
	defer func() { listingForwardTimeout = old }()

	start := time.Now()
	status, _ := r.read(t, "GET", "/api/v1/measurements", "")
	require.Equal(t, http.StatusGatewayTimeout, status)
	require.Less(t, time.Since(start), 5*time.Second)
}

// The header timer answers 504 (a timeout), not the 502 a cancelled context
// would map to.
func TestForwardedQueryHeaderTimeoutIs504(t *testing.T) {
	r := newDoorRig(t)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.ReadAll(req.Body)
		<-release
	}))
	defer srv.Close()
	defer close(release)
	r.qh.SetRouter(readRouter(t, srv.URL, true, true))
	r.qh.queryTimeout = 100 * time.Millisecond
	// queryForwardHeaderBound adds 30 s of slack: estimate's bound is then
	// too long for a test, so drive the timer through the Arrow-independent
	// helper with a zero-slack bound.
	o := r.qh.readForwardOpts(r.qh.router.RouteReadQuery, "")
	o.headerBound = 150 * time.Millisecond
	r.app.Get("/zz-test-forward", func(c *fiber.Ctx) error {
		_, err := forwardRead(c, r.qh.router, o)
		return err
	})
	status, body := r.read(t, "GET", "/zz-test-forward", "")
	require.Equal(t, http.StatusGatewayTimeout, status, body)
}
