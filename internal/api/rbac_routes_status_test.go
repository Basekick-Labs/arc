package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Pins the HTTP status contract for the RBAC handlers (issue #549).
//
// Client-supplied bad input must never surface as 5xx: a 500 drives client
// retries, alerting, and error budgets, and gives the caller no way to tell
// "the server broke" from "pick a different name". Before #549 a duplicate
// name returned 500 and updateRole returned 500 for invalid patterns and
// permissions.
//
// The license middleware is deliberately not mounted — these tests exercise
// the handlers' error-to-status mapping, not the license gate (which is
// covered by requireRBACLicense and returns 403 before any handler body).
func setupRBACStatusTest(t *testing.T) (*fiber.App, *auth.RBACManager) {
	t.Helper()

	dir, err := os.MkdirTemp("", "arc-rbac-status-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	lg := zerolog.Nop()
	am, err := auth.NewAuthManager(filepath.Join(dir, "auth.db"), 5*time.Minute, 100, lg)
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() { am.Close() })

	rm := auth.NewRBACManager(&auth.RBACManagerConfig{DB: am.GetDB(), Logger: lg})
	h := NewRBACHandler(am, rm, lg)

	app := fiber.New()
	app.Post("/orgs", h.createOrganization)
	app.Patch("/orgs/:id", h.updateOrganization)
	app.Post("/orgs/:org_id/teams", h.createTeam)
	app.Patch("/teams/:id", h.updateTeam)
	app.Post("/teams/:team_id/roles", h.createRole)
	app.Patch("/roles/:id", h.updateRole)
	app.Post("/roles/:role_id/measurements", h.createMeasurementPermission)
	app.Delete("/orgs/:id", h.deleteOrganization)
	app.Delete("/teams/:id", h.deleteTeam)
	app.Delete("/roles/:id", h.deleteRole)
	app.Delete("/measurement-permissions/:id", h.deleteMeasurementPermission)

	return app, rm
}

// doRBACRequest issues a request and returns the status code plus the decoded error field.
func doRBACRequest(t *testing.T, app *fiber.App, method, path, body string) (int, string) {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 5000)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	var payload struct {
		Error string `json:"error"`
	}
	// A body that does not decode is not fatal; the status is the assertion.
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	return resp.StatusCode, payload.Error
}

// TestRBACDuplicateNameReturnsConflict covers finding 1 of #549: name
// collisions returned 500 on all four org/team create+update handlers.
func TestRBACDuplicateNameReturnsConflict(t *testing.T) {
	app, rm := setupRBACStatusTest(t)
	ctx := t.Context()

	orgA, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "org-alpha"})
	if err != nil {
		t.Fatalf("seed orgA: %v", err)
	}
	orgB, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "org-bravo"})
	if err != nil {
		t.Fatalf("seed orgB: %v", err)
	}
	if _, err := rm.CreateTeam(ctx, orgA.ID, &auth.CreateTeamRequest{Name: "team-alpha"}); err != nil {
		t.Fatalf("seed teamA: %v", err)
	}
	teamB, err := rm.CreateTeam(ctx, orgA.ID, &auth.CreateTeamRequest{Name: "team-bravo"})
	if err != nil {
		t.Fatalf("seed teamB: %v", err)
	}

	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"create org duplicate", "POST", "/orgs", `{"name":"org-alpha"}`},
		{"update org duplicate", "PATCH", fmt.Sprintf("/orgs/%d", orgB.ID), `{"name":"org-alpha"}`},
		{"create team duplicate in org", "POST", fmt.Sprintf("/orgs/%d/teams", orgA.ID), `{"name":"team-alpha"}`},
		{"update team duplicate in org", "PATCH", fmt.Sprintf("/teams/%d", teamB.ID), `{"name":"team-alpha"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, errMsg := doRBACRequest(t, app, tc.method, tc.path, tc.body)
			if status != http.StatusConflict {
				t.Errorf("status = %d, want %d (409 Conflict) — err=%q", status, http.StatusConflict, errMsg)
			}
			if !strings.Contains(errMsg, "already exists") {
				t.Errorf("error message should explain the collision, got %q", errMsg)
			}
		})
	}
}

// TestRBACInvalidNameReturnsBadRequest guards the #324 behavior against
// regression now that detection moved from substring matching to errors.Is.
func TestRBACInvalidNameReturnsBadRequest(t *testing.T) {
	app, rm := setupRBACStatusTest(t)
	ctx := t.Context()

	org, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "org-valid"})
	if err != nil {
		t.Fatalf("seed org: %v", err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "team-valid"})
	if err != nil {
		t.Fatalf("seed team: %v", err)
	}

	orgPath := fmt.Sprintf("/orgs/%d", org.ID)
	teamPath := fmt.Sprintf("/teams/%d", team.ID)
	teamsPath := fmt.Sprintf("/orgs/%d/teams", org.ID)
	tooLong := "a" + strings.Repeat("b", 64) // 65 chars

	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"create org empty", "POST", "/orgs", `{"name":""}`},
		{"create org leading digit", "POST", "/orgs", `{"name":"9bad"}`},
		{"create org spaces", "POST", "/orgs", `{"name":"has spaces"}`},
		{"create org too long", "POST", "/orgs", `{"name":"` + tooLong + `"}`},
		{"update org empty", "PATCH", orgPath, `{"name":""}`},
		{"update org leading digit", "PATCH", orgPath, `{"name":"9bad"}`},
		{"update org spaces", "PATCH", orgPath, `{"name":"has spaces"}`},
		{"create team spaces", "POST", teamsPath, `{"name":"has spaces"}`},
		{"update team leading digit", "PATCH", teamPath, `{"name":"9bad"}`},
		{"update team empty", "PATCH", teamPath, `{"name":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if status, errMsg := doRBACRequest(t, app, tc.method, tc.path, tc.body); status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
			}
		})
	}
}

// TestUpdateRoleInvalidInputReturnsBadRequest covers finding 2 of #549:
// updateRole had no 400 mapping, so manager-layer validation errors for
// database_pattern and permissions surfaced as 500.
func TestUpdateRoleInvalidInputReturnsBadRequest(t *testing.T) {
	app, rm := setupRBACStatusTest(t)
	ctx := t.Context()

	org, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "role-org"})
	if err != nil {
		t.Fatalf("seed org: %v", err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "role-team"})
	if err != nil {
		t.Fatalf("seed team: %v", err)
	}
	role, err := rm.CreateRole(ctx, team.ID, &auth.CreateRoleRequest{
		DatabasePattern: "metrics",
		Permissions:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("seed role: %v", err)
	}

	rolePath := fmt.Sprintf("/roles/%d", role.ID)

	t.Run("invalid permission", func(t *testing.T) {
		if status, errMsg := doRBACRequest(t, app, "PATCH", rolePath, `{"permissions":["not-a-permission"]}`); status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
		}
	})

	t.Run("invalid database pattern", func(t *testing.T) {
		if status, errMsg := doRBACRequest(t, app, "PATCH", rolePath, `{"database_pattern":"../etc"}`); status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
		}
	})

	t.Run("invalid permission on create still 400", func(t *testing.T) {
		body := `{"database_pattern":"metrics","permissions":["not-a-permission"]}`
		if status, errMsg := doRBACRequest(t, app, "POST", fmt.Sprintf("/teams/%d/roles", team.ID), body); status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
		}
	})

	t.Run("valid update still succeeds", func(t *testing.T) {
		if status, errMsg := doRBACRequest(t, app, "PATCH", rolePath, `{"permissions":["read","write"]}`); status != http.StatusOK {
			t.Errorf("status = %d, want 200 — err=%q", status, errMsg)
		}
	})

	// createMeasurementPermission validates a measurement pattern the same
	// way createRole validates a database pattern. Both are client input;
	// neither may surface as 5xx.
	measPath := fmt.Sprintf("/roles/%d/measurements", role.ID)

	t.Run("invalid measurement pattern", func(t *testing.T) {
		body := `{"measurement_pattern":"../etc","permissions":["read"]}`
		if status, errMsg := doRBACRequest(t, app, "POST", measPath, body); status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
		}
	})

	t.Run("invalid measurement permission", func(t *testing.T) {
		body := `{"measurement_pattern":"cpu","permissions":["not-a-permission"]}`
		if status, errMsg := doRBACRequest(t, app, "POST", measPath, body); status != http.StatusBadRequest {
			t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
		}
	})
}

// TestRBACErrorMessagesUnchanged pins the exact client-visible error text.
//
// Status-code classification uses sentinel errors attached via tagError,
// which deliberately does NOT alter the message — an earlier iteration used
// fmt.Errorf("%w: ...") and silently prefixed every message ("name already
// exists: organization with name 'x' already exists"), a client-visible API
// change. The release notes state messages are unchanged; this test is what
// makes that claim enforceable.
func TestRBACErrorMessagesUnchanged(t *testing.T) {
	app, rm := setupRBACStatusTest(t)
	ctx := t.Context()

	if _, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "msg-org"}); err != nil {
		t.Fatalf("seed org: %v", err)
	}

	for _, tc := range []struct {
		name, method, path, body, want string
	}{
		{
			name: "duplicate org", method: "POST", path: "/orgs",
			body: `{"name":"msg-org"}`,
			want: "organization with name 'msg-org' already exists",
		},
		{
			name: "empty org name", method: "POST", path: "/orgs",
			body: `{"name":""}`,
			want: "organization name is required",
		},
		{
			name: "invalid org name", method: "POST", path: "/orgs",
			body: `{"name":"9bad"}`,
			want: "invalid organization name: name must start with a letter, contain only alphanumeric characters, underscores, or hyphens, and be at most 64 characters",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, errMsg := doRBACRequest(t, app, tc.method, tc.path, tc.body)
			if errMsg != tc.want {
				t.Errorf("error message drifted:\n got: %q\nwant: %q", errMsg, tc.want)
			}
		})
	}
}

// TestRBACNotFoundStillReturns404 proves the new client-error mapping does
// not shadow the pre-existing not-found handling, and that an invalid name
// on a missing ID cannot be used to probe whether that ID exists.
func TestRBACNotFoundStillReturns404(t *testing.T) {
	app, _ := setupRBACStatusTest(t)

	t.Run("update org missing id, valid name", func(t *testing.T) {
		if status, errMsg := doRBACRequest(t, app, "PATCH", "/orgs/99999", `{"name":"fine-name"}`); status != http.StatusNotFound {
			t.Errorf("status = %d, want 404 — err=%q", status, errMsg)
		}
	})

	t.Run("update team missing id, valid name", func(t *testing.T) {
		if status, errMsg := doRBACRequest(t, app, "PATCH", "/teams/99999", `{"name":"fine-name"}`); status != http.StatusNotFound {
			t.Errorf("status = %d, want 404 — err=%q", status, errMsg)
		}
	})

	t.Run("update role missing id, valid input", func(t *testing.T) {
		if status, errMsg := doRBACRequest(t, app, "PATCH", "/roles/99999", `{"permissions":["read"]}`); status != http.StatusNotFound {
			t.Errorf("status = %d, want 404 — err=%q", status, errMsg)
		}
	})

	// Validation runs before the existence lookup, so an invalid name is a
	// 400 whether or not the ID exists. That ordering is deliberate: it
	// means the status code cannot be used to probe for existence.
	t.Run("invalid name does not leak existence", func(t *testing.T) {
		missing, _ := doRBACRequest(t, app, "PATCH", "/orgs/99999", `{"name":"9bad"}`)
		if missing != http.StatusBadRequest {
			t.Errorf("missing-id invalid-name status = %d, want 400", missing)
		}
	})
}
