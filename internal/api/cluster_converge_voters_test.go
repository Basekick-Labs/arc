package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #880: the operator lever that converges an existing cluster's Raft voter set
// onto the role-based rule #862 introduced at join time. Demote-only, dry-run
// by default.

func convergeTestApp(t *testing.T) *fiber.App {
	t.Helper()
	h := NewClusterHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	return app
}

func postConverge(t *testing.T, app *fiber.App, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/api/v1/cluster/voters/converge", strings.NewReader(body))
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

func TestConvergeVotersRouteIsRegistered(t *testing.T) {
	rec := postConverge(t, convergeTestApp(t), `{"dry_run":true}`)
	if rec.Code == fiber.StatusNotFound || rec.Code == fiber.StatusMethodNotAllowed {
		t.Fatalf("POST /api/v1/cluster/voters/converge is not routed (status %d)", rec.Code)
	}
}

// It must not collide with any existing /api/v1/cluster/* route, and must not
// be reachable by GET — this changes Raft membership.
func TestConvergeVotersRejectsGet(t *testing.T) {
	app := convergeTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/cluster/voters/converge", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusMethodNotAllowed && resp.StatusCode != fiber.StatusNotFound {
		t.Errorf("GET returned %d; this endpoint must not be reachable by GET", resp.StatusCode)
	}
}

// Without clustering it must refuse with a non-2xx, not answer 200 with
// enabled=false the way the read endpoints do.
func TestConvergeVotersWithoutClustering(t *testing.T) {
	rec := postConverge(t, convergeTestApp(t), `{"dry_run":true}`)
	if rec.Code != fiber.StatusConflict {
		t.Fatalf("status %d, want 409", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if ok, _ := body["success"].(bool); ok {
		t.Error("success=true on a node with no coordinator")
	}
}

// The endpoint changes Raft membership, and an unintended demotion can leave a
// cluster unable to elect a leader with no recovery path. Admin auth is not
// optional, and nothing else in this file asserts it — every other case builds
// the handler with a nil auth manager, which disables the middleware.
func TestConvergeVotersRequiresAuth(t *testing.T) {
	am, err := auth.NewAuthManager(t.TempDir()+"/auth.db", time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	h := NewClusterHandler(nil, am, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)

	rec := postConverge(t, app, `{"dry_run":false}`)
	if rec.Code != fiber.StatusUnauthorized {
		t.Fatalf("an unauthenticated converge returned %d, want 401", rec.Code)
	}
}

// dry_run defaults to TRUE. Tested against the parser directly: a handler
// test with no coordinator refuses before the parse result is ever used, so it
// asserts nothing — which is exactly how the first version of this test came
// to pass regardless of the default.
func TestConvergeVotersDefaultsToDryRun(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"absent body", "", true},
		{"empty object", "{}", true},
		{"malformed", "not json", true},
		{"only allow_single_voter", `{"allow_single_voter":true}`, true},
		{"explicit true", `{"dry_run":true}`, true},
		{"explicit false is the ONLY way to act", `{"dry_run":false}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := parseConvergeRequest([]byte(tc.body))
			if got != tc.want {
				t.Errorf("parseConvergeRequest(%q) dry_run = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

// allow_single_voter is carried through, and is false unless asked for.
func TestConvergeVotersAllowSingleVoterIsOptIn(t *testing.T) {
	if _, allow := parseConvergeRequest([]byte(`{"dry_run":false}`)); allow {
		t.Error("allow_single_voter defaulted to true")
	}
	if _, allow := parseConvergeRequest([]byte(`{"dry_run":false,"allow_single_voter":true}`)); !allow {
		t.Error("allow_single_voter was not carried through")
	}
	// A malformed body must not smuggle it in either.
	if _, allow := parseConvergeRequest([]byte(`{"allow_single_voter":true,`)); allow {
		t.Error("a malformed body enabled allow_single_voter")
	}
}

// A converge that stopped part-way must not be reported as a success. It stops
// at the first failed revocation, so a populated failure map means the voter
// set is between where it was and where it was asked to be.
func TestPartialConvergeIsNotASuccess(t *testing.T) {
	if convergeIsPartial(&cluster.VoterConvergeResult{Demoted: []string{"a"}}) {
		t.Error("a clean run was reported as partial")
	}
	if !convergeIsPartial(&cluster.VoterConvergeResult{
		Demoted: []string{"a"},
		Failed:  map[string]string{"b": "no longer the leader"},
	}) {
		t.Error("a run that stopped at a failed revocation was reported as a success")
	}
	if convergeIsPartial(nil) {
		t.Error("nil result treated as partial")
	}
}
