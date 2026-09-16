package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #876: the compactor lease could only be moved by making its holder
// unhealthy — i.e. restarting a node that is also taking ingest.
// TriggerManualFailover existed and was unit-tested, but nothing routed to
// it: no endpoint, no CLI verb, no config.
//
// Unlike the writer hand-over (#872), the target here is explicit and
// required. The automatic choice landing in the wrong place IS the defect, so
// "move it, you pick" would reproduce what the operator is overriding.

func assignCompactorTestApp(t *testing.T) *fiber.App {
	t.Helper()
	// A nil coordinator is the OSS/standalone shape: it exercises routing,
	// body validation and the not-enabled response without a Raft cluster.
	// The lease semantics themselves are tested in internal/cluster, where a
	// real single-node Raft is available.
	h := NewClusterHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	return app
}

func postAssign(t *testing.T, app *fiber.App, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/cluster/compactor/assign", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	rec := httptest.NewRecorder()
	rec.Code = resp.StatusCode
	_, _ = rec.Body.ReadFrom(resp.Body)
	return rec
}

// The route has to exist and be a POST. A typo means the operator lever #876
// asked for still does not exist.
func TestAssignCompactorRouteIsRegistered(t *testing.T) {
	rec := postAssign(t, assignCompactorTestApp(t), `{"node_id":"compactor-1"}`)
	if rec.Code == fiber.StatusNotFound || rec.Code == fiber.StatusMethodNotAllowed {
		t.Fatalf("POST /api/v1/cluster/compactor/assign is not routed (status %d)", rec.Code)
	}
}

// It must not collide with /api/v1/cluster/nodes/:id or any other existing
// route, and must not be reachable by GET.
func TestAssignCompactorRejectsGet(t *testing.T) {
	app := assignCompactorTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/cluster/compactor/assign", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusMethodNotAllowed && resp.StatusCode != fiber.StatusNotFound {
		t.Errorf("GET on the assign route returned %d; it must not be reachable by GET", resp.StatusCode)
	}
}

// Without clustering it must refuse with a non-2xx. Deliberately NOT the
// read endpoints' 200 + enabled=false: a caller checking the status code
// would read that as an assignment that happened.
func TestAssignCompactorWithoutClustering(t *testing.T) {
	rec := postAssign(t, assignCompactorTestApp(t), `{"node_id":"compactor-1"}`)
	if rec.Code == fiber.StatusOK {
		t.Error("a node with no coordinator reported a successful lease assignment")
	}
	if rec.Code != fiber.StatusConflict {
		t.Errorf("status %d, want 409", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if ok, _ := body["success"].(bool); ok {
		t.Error("success=true on a node with no coordinator")
	}
}

// The target is required. An empty or missing node_id must be a 400, not a
// server-side pick — choosing for the operator is the behaviour being fixed.
func TestAssignCompactorRequiresATarget(t *testing.T) {
	for _, body := range []string{`{}`, `{"node_id":""}`, `{"node_id":null}`} {
		rec := postAssign(t, assignCompactorTestApp(t), body)
		if rec.Code != fiber.StatusBadRequest {
			t.Errorf("body %s: status %d, want 400", body, rec.Code)
		}
	}
}

func TestAssignCompactorRejectsAMalformedBody(t *testing.T) {
	rec := postAssign(t, assignCompactorTestApp(t), `not json`)
	if rec.Code != fiber.StatusBadRequest {
		t.Errorf("status %d, want 400 for a non-JSON body", rec.Code)
	}
}

// An over-long node ID is rejected before it reaches Raft.
func TestAssignCompactorRejectsAnOverlongID(t *testing.T) {
	rec := postAssign(t, assignCompactorTestApp(t), `{"node_id":"`+strings.Repeat("a", maxNodeIDLength+10)+`"}`)
	if rec.Code != fiber.StatusBadRequest {
		t.Errorf("status %d, want 400 for an over-long node ID", rec.Code)
	}
}

// The endpoint mutates cluster-wide state, so it must be admin-gated. Nothing
// else in this file asserts that: every other case builds the handler with a
// nil authManager, which disables the middleware entirely.
//
// This is the one property whose absence would be a security bug rather than
// a correctness one, so it gets a real AuthManager rather than a nil one.
func TestAssignCompactorRequiresAuth(t *testing.T) {
	am, err := auth.NewAuthManager(t.TempDir()+"/auth.db", time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h := NewClusterHandler(nil, am, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)

	req := httptest.NewRequest("POST", "/api/v1/cluster/compactor/assign", strings.NewReader(`{"node_id":"compactor-1"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("an unauthenticated assign returned %d, want 401 — this endpoint moves cluster-wide state", resp.StatusCode)
	}
}
