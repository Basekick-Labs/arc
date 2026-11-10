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
func readRouter(t *testing.T, peerURL string, capable, withPeer bool) *cluster.Router {
	t.Helper()
	local := cluster.NewNode("local", "Local", cluster.RoleCompactor, "test-cluster")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	if withPeer {
		reader := cluster.NewNode("reader-1", "Reader", cluster.RoleReader, "test-cluster")
		reader.UpdateState(cluster.StateHealthy)
		reader.APIAddress = strings.TrimPrefix(peerURL, "http://")
		require.NoError(t, registry.Register(reader))
	}
	return cluster.NewRouter(&cluster.RouterConfig{
		Timeout:         time.Second,
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
		if got := forwardableRoute(rt.Method, concrete); got != want[key] {
			t.Errorf("%s: forwardable = %v, want %v", key, got, want[key])
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
	reg := queryregistry.NewRegistry(nil, zerolog.Nop())
	r.qh.queryRegistry = reg
	peerCancelled := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Read the body first, as a real peer does before running the query:
		// net/http notices a dropped connection only after that.
		_, _ = io.ReadAll(req.Body)
		<-req.Context().Done() // a long query; ends when the forward is cut
		close(peerCancelled)
	}))
	defer srv.Close()
	r.qh.SetRouter(readRouter(t, srv.URL, true, true))

	done := make(chan int)
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
