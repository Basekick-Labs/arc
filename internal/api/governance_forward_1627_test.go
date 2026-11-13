//go:build duckdb_arrow

package api

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1627, rule 2: a query is governed once. A node that forwards a query (a
// compactor) runs no governance for it; the node that answers does. Over the
// four governed routes, the forwarding node's counters stay at zero, and the
// same requests answered locally count one each (so zero means something).
func TestForwardedQueriesAreGovernedOnce(t *testing.T) {
	r := newDoorRig(t)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	m, err := governance.NewManager(&governance.ManagerConfig{DB: db, Logger: zerolog.Nop(),
		Config: &config.GovernanceConfig{DefaultMaxQueriesPerHour: 1000, DefaultMaxRowsPerQuery: 1000}})
	require.NoError(t, err)
	orig := queryGovernanceLicensed
	queryGovernanceLicensed = func(*QueryHandler) bool { return true }
	t.Cleanup(func() { queryGovernanceLicensed = orig })
	r.qh.SetGovernance(m, nil)
	tokenID := r.am.VerifyToken(r.tokens["coarse-no-team"]).ID
	queries := func() int { return m.GetTokenUsage(tokenID).QueriesThisHour }

	routes := []struct{ method, path, body string }{
		{"POST", "/api/v1/query", `{"sql":"SELECT * FROM tenanta.cpu"}`},
		{"POST", "/api/v1/query/arrow", `{"sql":"SELECT * FROM tenanta.cpu"}`},
		{"POST", "/api/v1/query/estimate", `{"sql":"SELECT * FROM tenanta.cpu"}`},
		{"GET", "/api/v1/query/cpu?database=tenanta", ""},
	}
	send := func(method, path, body string) (int, string) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		return r.do(t, "coarse-no-team", req)
	}

	peer := &readPeer{}
	srv := httptest.NewServer(peer)
	defer srv.Close()
	r.qh.SetRouter(readRouter(t, srv.URL, true, true))
	for _, rt := range routes {
		status, body := send(rt.method, rt.path, rt.body)
		require.Equal(t, http.StatusOK, status, rt.path+": "+body)
		require.Contains(t, body, `"from":"peer"`, rt.path)
	}
	require.Equal(t, int32(len(routes)), peer.hits.Load())
	require.Zero(t, queries(), "the forwarding node counted a query it forwarded")

	r.qh.SetRouter(nil) // no cluster: answered here
	for i, rt := range routes {
		status, body := send(rt.method, rt.path, rt.body)
		require.NotContains(t, body, `"from":"peer"`, rt.path)
		require.Less(t, status, 500, rt.path+": "+body)
		require.Equal(t, i+1, queries(), "a query answered here was not governed once: "+rt.path)
	}
}
