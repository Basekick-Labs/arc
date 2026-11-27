package api

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
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
	if status != fiber.StatusOK || policy["max_rows_per_query"] != float64(42) || body["scope"] != "cluster" {
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

// A node where governance does not run answers the adoption from its table,
// and once adopted keeps that table equal to the cluster's, marked as the
// cluster's backup copy.
func TestGovernanceTableListerAndMirror(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := governance.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO governance_policies (token_id, max_rows_per_query) VALUES (3, 30), (4, 40)`); err != nil {
		t.Fatal(err)
	}
	raw, err := GovernanceTableLister(nil, path)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"token_id":3`) || !strings.Contains(string(raw), `"token":"unknown"`) {
		t.Fatalf("lister answered %s", raw)
	}
	if raw, err := GovernanceTableLister(nil, filepath.Join(t.TempDir(), "none.db"))(context.Background()); err != nil || !strings.Contains(string(raw), `"policies":[]`) {
		t.Fatalf("no database: %s %v", raw, err)
	}

	mirror := GovernanceTableMirror(nil, path, entriesView{{GovernancePolicyEntry: raft.GovernancePolicyEntry{ID: 9, TokenID: 3, MaxRowsPerQuery: 33}, TokenHash: "h"}}, zerolog.Nop())
	if err := mirror(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows, err := governance.ReadLocalRows(context.Background(), db)
	if err != nil || len(rows.Policies) != 1 || rows.Policies[0].ID != 9 || rows.Policies[0].MaxRowsPerQuery != 33 {
		t.Fatalf("after the mirror %+v %v", rows, err)
	}
	var owned int
	db.QueryRow(`SELECT COUNT(*) FROM governance_cluster_state WHERE name = 'owned'`).Scan(&owned)
	if owned != 1 {
		t.Fatal("the mirrored table is not marked as the cluster's")
	}
	// This node's own rows the cluster did not adopt as they are are kept,
	// dormant: token 3 (the cluster holds it with other limits) and token 4.
	type dormantRow struct{ token, rows int64 }
	var dormant []dormantRow
	drows, err := db.Query(`SELECT token_id, max_rows_per_query FROM governance_policies_dormant ORDER BY token_id`)
	if err != nil {
		t.Fatal(err)
	}
	for drows.Next() {
		var d dormantRow
		if err := drows.Scan(&d.token, &d.rows); err != nil {
			t.Fatal(err)
		}
		dormant = append(dormant, d)
	}
	drows.Close()
	if len(dormant) != 2 || dormant[0] != (dormantRow{3, 30}) || dormant[1] != (dormantRow{4, 40}) {
		t.Fatalf("dormant rows %+v, want token 3 at 30 and token 4 at 40", dormant)
	}
	if err := GovernanceTableMirror(nil, path, stubGovernanceView{}, zerolog.Nop())(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// The policy list, get and usage answers say whose policies this node
// enforces: "node" standalone or before it caught up with the cluster that
// owns them, "cluster" once it has. Nothing else in them changes.
func TestGovernanceAnswersCarryTheScope(t *testing.T) {
	routes := func(m *governance.Manager) *fiber.App {
		h := NewGovernanceHandler(m, nil, nil, zerolog.Nop())
		app := fiber.New()
		app.Get("/policies", h.ListPolicies)
		app.Get("/policies/:token_id", h.GetPolicy)
		app.Get("/usage/:token_id", h.GetUsage)
		return app
	}
	check := func(t *testing.T, app *fiber.App, path, want string) map[string]any {
		t.Helper()
		status, body := sendGovernance(t, app, "GET", path, "")
		if status != fiber.StatusOK || body["scope"] != want {
			t.Fatalf("GET %s: %d %v, want scope %q", path, status, body, want)
		}
		return body
	}

	t.Run("standalone", func(t *testing.T) {
		app := routes(newGovernanceTestManager(t, &governance.Policy{TokenID: 7, MaxRowsPerQuery: 9}))
		if body := check(t, app, "/policies", "node"); body["count"] != float64(1) {
			t.Fatalf("list %v", body)
		}
		check(t, app, "/policies/7", "node")
		check(t, app, "/usage/7", "node")
	})

	newClusterManager := func(t *testing.T, db *sql.DB) *governance.Manager {
		m, err := governance.NewManager(&governance.ManagerConfig{DB: db, Config: &config.GovernanceConfig{DefaultMaxRowsPerQuery: 3}, Logger: zerolog.Nop(), Cluster: true})
		if err != nil {
			t.Fatal(err)
		}
		m.SetClusterSource(&stubGovernanceCluster{seeded: true})
		return m
	}
	t.Run("cluster", func(t *testing.T) {
		db, _ := sql.Open("sqlite3", ":memory:")
		t.Cleanup(func() { db.Close() })
		m := newClusterManager(t, db)
		m.SyncClusterView(entriesView{{GovernancePolicyEntry: raft.GovernancePolicyEntry{ID: 4, TokenID: 7, MaxRowsPerQuery: 9}, TokenHash: "h"}})
		app := routes(m)
		if body := check(t, app, "/policies", "cluster"); body["count"] != float64(1) {
			t.Fatalf("list %v", body)
		}
		// No auth here: the token's fingerprint is unknown, so GET finds no
		// policy for it and usage shows the defaults (id 0).
		body := check(t, app, "/usage/7", "cluster")
		if p, _ := body["policy"].(map[string]any); p["id"] != float64(0) || p["max_rows_per_query"] != float64(3) {
			t.Fatalf("usage policy %v, want the defaults with id 0", body["policy"])
		}
	})

	t.Run("not caught up", func(t *testing.T) {
		db, _ := sql.Open("sqlite3", ":memory:")
		t.Cleanup(func() { db.Close() })
		if err := governance.EnsureSchema(db); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO governance_cluster_state (name, value) VALUES ('owned', '1')`); err != nil {
			t.Fatal(err)
		}
		m := newClusterManager(t, db)
		m.SyncClusterView(stubGovernanceView{})
		app := routes(m)
		if status, _ := sendGovernance(t, app, "GET", "/policies", ""); status != fiber.StatusServiceUnavailable {
			t.Fatalf("list before catching up: %d", status)
		}
		body := check(t, app, "/usage/7", "node")
		if p, _ := body["policy"].(map[string]any); p["id"] != float64(0) || p["max_rows_per_query"] != float64(3) {
			t.Fatalf("usage policy %v, want the defaults with id 0", body["policy"])
		}
	})
}
