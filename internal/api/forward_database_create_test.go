//go:build duckdb_arrow

package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// A node that does not ingest (a compactor or reader) sends
// POST /api/v1/databases to the writer, when the writer accepts it; with an
// older writer it creates the database itself, as before; with no writer it
// answers 503.

type createPeer struct {
	mu    sync.Mutex
	calls []string
}

func (p *createPeer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	p.calls = append(p.calls, r.Method+" "+r.URL.Path+" "+string(body))
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"from":"writer"}`))
}

func createRouter(t *testing.T, role cluster.NodeRole, peerURL string, capable, withWriter bool) *cluster.Router {
	t.Helper()
	local := cluster.NewNode("local", "Local", role, "test-cluster")
	registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	if withWriter {
		w := cluster.NewNode("writer-1", "Writer", cluster.RoleWriter, "test-cluster")
		w.UpdateState(cluster.StateHealthy)
		w.APIAddress = strings.TrimPrefix(peerURL, "http://")
		require.NoError(t, registry.Register(w))
	}
	return cluster.NewRouter(&cluster.RouterConfig{
		Timeout:            time.Second,
		WriteTimeout:       time.Second,
		Registry:           registry,
		LocalNode:          local,
		Logger:             zerolog.Nop(),
		PeerDatabaseCreate: func(string) bool { return capable },
	})
}

func TestNonIngestingNodeForwardsDatabaseCreate(t *testing.T) {
	for _, role := range []cluster.NodeRole{cluster.RoleCompactor, cluster.RoleReader} {
		t.Run(string(role), func(t *testing.T) {
			r := newDoorRig(t)
			tok, err := r.am.CreateToken(context.Background(), "admin", "admin", "admin", nil)
			require.NoError(t, err)
			r.tokens["admin"] = tok
			peer := &createPeer{}
			srv := httptest.NewServer(peer)
			defer srv.Close()
			create := func(name string) (int, string) {
				req := httptest.NewRequest(http.MethodPost, "/api/v1/databases", strings.NewReader(`{"name":"`+name+`"}`))
				req.Header.Set("Content-Type", "application/json")
				return r.do(t, "admin", req)
			}

			// Capable writer: forwarded with its body, the writer's answer relayed.
			r.dbs.SetRouter(createRouter(t, role, srv.URL, true, true))
			status, body := create("viawriter")
			require.Equal(t, http.StatusCreated, status, body)
			require.Contains(t, body, `"from":"writer"`)
			require.Equal(t, []string{`POST /api/v1/databases {"name":"viawriter"}`}, peer.calls)
			exists, err := r.dbs.databaseExists(context.Background(), "viawriter")
			require.NoError(t, err)
			require.False(t, exists, "a forwarded create also created the database locally")

			// Writer too old to accept it (rolling upgrade): created here.
			r.dbs.SetRouter(createRouter(t, role, srv.URL, false, true))
			status, body = create("locally")
			require.Equal(t, http.StatusCreated, status, body)
			require.NotContains(t, body, `"from":"writer"`)
			require.Len(t, peer.calls, 1, "forwarded to a writer without the capability")
			exists, err = r.dbs.databaseExists(context.Background(), "locally")
			require.NoError(t, err)
			require.True(t, exists)

			// No writer at all: 503, nothing created here.
			r.dbs.SetRouter(createRouter(t, role, srv.URL, true, false))
			status, body = create("nowriter")
			require.Equal(t, http.StatusServiceUnavailable, status, body)
			exists, err = r.dbs.databaseExists(context.Background(), "nowriter")
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

// A create already marked as forwarded, arriving at a node that does not
// ingest, is a loop or a spoofed header: 508, nothing created. A writer that
// cannot be reached: 503 in a create's words, nothing created.
func TestDatabaseCreateForwardFailures(t *testing.T) {
	r := newDoorRig(t)
	tok, err := r.am.CreateToken(context.Background(), "admin", "admin", "admin", nil)
	require.NoError(t, err)
	r.tokens["admin"] = tok
	create := func(name string, marked bool) (int, string) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/databases", strings.NewReader(`{"name":"`+name+`"}`))
		req.Header.Set("Content-Type", "application/json")
		if marked {
			req.Header.Set(ForwardedByHeader, "other-node")
		}
		return r.do(t, "admin", req)
	}

	r.dbs.SetRouter(createRouter(t, cluster.RoleCompactor, "http://127.0.0.1:1", true, true))
	status, body := create("looped", true)
	require.Equal(t, http.StatusLoopDetected, status, body)

	// Nothing listens on the writer's address.
	status, body = create("unreached", false)
	require.Equal(t, http.StatusServiceUnavailable, status, body)
	require.Contains(t, body, "Database not created")

	for _, name := range []string{"looped", "unreached"} {
		exists, err := r.dbs.databaseExists(context.Background(), name)
		require.NoError(t, err)
		require.False(t, exists, name)
	}
}

// A writer creates the database itself, whatever the router says.
func TestWriterCreatesDatabaseLocally(t *testing.T) {
	r := newDoorRig(t)
	tok, err := r.am.CreateToken(context.Background(), "admin", "admin", "admin", nil)
	require.NoError(t, err)
	r.tokens["admin"] = tok
	peer := &createPeer{}
	srv := httptest.NewServer(peer)
	defer srv.Close()
	r.dbs.SetRouter(createRouter(t, cluster.RoleWriter, srv.URL, true, true))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/databases", strings.NewReader(`{"name":"here"}`))
	req.Header.Set("Content-Type", "application/json")
	status, body := r.do(t, "admin", req)
	require.Equal(t, http.StatusCreated, status, body)
	require.Empty(t, peer.calls)
}
