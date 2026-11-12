package api

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/governance"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// stubGovernanceCluster is a cluster that owns the governance policies.
type stubGovernanceCluster struct {
	seeded bool
	setErr error
}

func (s *stubGovernanceCluster) Seeded() bool                         { return s.seeded }
func (s *stubGovernanceCluster) Collecting() bool                     { return false }
func (s *stubGovernanceCluster) Get(int64) (*governance.Policy, bool) { return nil, false }
func (s *stubGovernanceCluster) Set(context.Context, *governance.Policy, bool, string) error {
	return s.setErr
}
func (s *stubGovernanceCluster) Delete(context.Context, int64, string) error { return s.setErr }

type stubGovernanceView struct{ seeded bool }

func (v stubGovernanceView) GovernanceView() (bool, []raft.GovernanceViewEntry) { return v.seeded, nil }

// #1627: the API answers the cluster-wide policies' errors distinctly: a
// token that is not a cluster token is a 404 that says so, a node that has
// not caught up is a 503 to retry, and a refused policy is a 400.
func TestGovernanceRoutesInACluster(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	m, err := governance.NewManager(&governance.ManagerConfig{DB: db, Config: &config.GovernanceConfig{}, Logger: zerolog.Nop(), Cluster: true})
	if err != nil {
		t.Fatal(err)
	}
	src := &stubGovernanceCluster{seeded: true, setErr: governance.ErrTokenNotCluster}
	m.SetClusterSource(src)
	m.SyncClusterView(stubGovernanceView{seeded: true})
	h := NewGovernanceHandler(m, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/policies", h.CreatePolicy)
	app.Get("/policies", h.ListPolicies)
	app.Delete("/policies/:token_id", h.DeletePolicy)

	status, body := sendGovernance(t, app, "POST", "/policies", `{"token_id":7,"max_rows_per_query":10}`)
	if status != fiber.StatusNotFound || body["error"] != "token 7 is not a cluster token; governance policies apply to cluster tokens only" {
		t.Fatalf("not a cluster token: %d %v", status, body)
	}
	src.setErr = governance.ErrInvalidPolicy
	if status, body := sendGovernance(t, app, "DELETE", "/policies/7", ""); status != fiber.StatusBadRequest {
		t.Fatalf("refused: %d %v", status, body)
	}
	src.setErr = governance.ErrClusterUnavailable
	if status, body := sendGovernance(t, app, "POST", "/policies", `{"token_id":7}`); status != fiber.StatusServiceUnavailable {
		t.Fatalf("cluster unavailable: %d %v", status, body)
	}

	// Owned, not caught up (a restart before the cluster state replayed).
	src.seeded = false
	if _, err := db.Exec(`INSERT INTO governance_cluster_state (name, value) VALUES ('owned', '1')`); err != nil {
		t.Fatal(err)
	}
	m2, err := governance.NewManager(&governance.ManagerConfig{DB: db, Config: &config.GovernanceConfig{}, Logger: zerolog.Nop(), Cluster: true})
	if err != nil {
		t.Fatal(err)
	}
	m2.SetClusterSource(src)
	m2.SyncClusterView(stubGovernanceView{})
	h2 := NewGovernanceHandler(m2, nil, nil, zerolog.Nop())
	app2 := fiber.New()
	app2.Post("/policies", h2.CreatePolicy)
	app2.Get("/policies", h2.ListPolicies)
	for _, r := range []struct{ method, body string }{{"POST", `{"token_id":7}`}, {"GET", ""}} {
		if status, body := sendGovernance(t, app2, r.method, "/policies", r.body); status != fiber.StatusServiceUnavailable {
			t.Fatalf("%s before catching up: %d %v", r.method, status, body)
		}
	}
}

type entriesView []raft.GovernanceViewEntry

func (v entriesView) GovernanceView() (bool, []raft.GovernanceViewEntry) { return true, v }

// Once the cluster owns the policies, a token's policy is found by its ID and
// its stored hash: the query path passes the request token's fingerprint,
// and the policy routes resolve the token's on this node.
func TestGovernanceUsesTheTokensFingerprint(t *testing.T) {
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	tok, err := am.CreateToken(context.Background(), "gov", "", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(tok)
	var hash string
	if err := am.GetDB().QueryRow(`SELECT token_hash FROM api_tokens WHERE id = ?`, info.ID).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	m, err := governance.NewManager(&governance.ManagerConfig{DB: db, Config: &config.GovernanceConfig{}, Logger: zerolog.Nop(), Cluster: true})
	if err != nil {
		t.Fatal(err)
	}
	m.SetClusterSource(&stubGovernanceCluster{seeded: true})
	m.SyncClusterView(entriesView{{GovernancePolicyEntry: raft.GovernancePolicyEntry{ID: 3, TokenID: info.ID, MaxRowsPerQuery: 42}, TokenHash: hash}})

	h := NewGovernanceHandler(m, am, nil, zerolog.Nop())
	app := fiber.New()
	app.Get("/policies/:token_id", h.GetPolicy)
	status, body := sendGovernance(t, app, "GET", "/policies/"+strconv.FormatInt(info.ID, 10), "")
	policy, _ := body["policy"].(map[string]any)
	if status != fiber.StatusOK || policy["max_rows_per_query"] != float64(42) {
		t.Fatalf("GET of the cluster token's policy: %d %v", status, body)
	}

	var gotMaxRows int
	qapp := newGovernanceTestApp(t, newGovernanceTestHandler(m, time.Minute), info, &gotMaxRows, nil)
	req := httptest.NewRequest("GET", "/api/v1/query/cpu?database=db", nil)
	resp, err := qapp.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotMaxRows != 42 {
		t.Fatalf("the query ran with max rows %d, want the cluster policy's 42", gotMaxRows)
	}
}
