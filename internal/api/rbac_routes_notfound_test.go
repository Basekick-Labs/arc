package api

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
)

// Characterization tests for the "not found" and "required field" status
// contract across every RBAC endpoint.
//
// These pin a deliberate asymmetry that is easy to destroy in a refactor:
//
//   - When the missing entity is the TARGET of the request (its ID is the
//     last path segment: GET/PATCH/DELETE), the endpoint returns 404.
//   - When the missing entity is the PARENT of a create (its ID is an
//     earlier path segment: POST .../:parent_id/children), the endpoint
//     returns 400, not 404.
//
// Both cases surface the same error string from RBACManager ("organization
// not found", "team not found", "role not found"), so the distinction lives
// entirely in the handlers. Any change that maps those errors centrally —
// for example a single ErrNotFound sentinel — must preserve it, or three
// create endpoints silently change from 400 to 404.
//
// Whether 400 is the right code for a missing parent is a separate design
// question (404 is arguably better). These tests assert what Arc does today
// so a refactor is provably behavior-preserving; changing the contract
// should be its own deliberate, documented change.
func TestRBACNotFoundStatusContract(t *testing.T) {
	app, rm := setupRBACStatusTest(t)
	ctx := t.Context()

	org, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "nf-org"})
	if err != nil {
		t.Fatalf("seed org: %v", err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "nf-team"})
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
	_ = role

	const missing = 99999

	t.Run("missing target returns 404", func(t *testing.T) {
		for _, tc := range []struct {
			name, method, path, body string
		}{
			{"update org", "PATCH", fmt.Sprintf("/orgs/%d", missing), `{"name":"fine-name"}`},
			{"delete org", "DELETE", fmt.Sprintf("/orgs/%d", missing), ``},
			{"update team", "PATCH", fmt.Sprintf("/teams/%d", missing), `{"name":"fine-name"}`},
			{"delete team", "DELETE", fmt.Sprintf("/teams/%d", missing), ``},
			{"update role", "PATCH", fmt.Sprintf("/roles/%d", missing), `{"permissions":["read"]}`},
			{"delete role", "DELETE", fmt.Sprintf("/roles/%d", missing), ``},
			{"delete measurement permission", "DELETE", fmt.Sprintf("/measurement-permissions/%d", missing), ``},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if status, errMsg := doRBACRequest(t, app, tc.method, tc.path, tc.body); status != http.StatusNotFound {
					t.Errorf("status = %d, want 404 — err=%q", status, errMsg)
				}
			})
		}
	})

	// The missing entity is the create's parent, not its target. Arc
	// returns 400 here, not 404. Pinned so a central not-found mapping
	// cannot silently flip these.
	t.Run("missing parent on create returns 400", func(t *testing.T) {
		for _, tc := range []struct {
			name, method, path, body string
		}{
			{
				"create team under missing org", "POST",
				fmt.Sprintf("/orgs/%d/teams", missing),
				`{"name":"orphan-team"}`,
			},
			{
				"create role under missing team", "POST",
				fmt.Sprintf("/teams/%d/roles", missing),
				`{"database_pattern":"metrics","permissions":["read"]}`,
			},
			{
				"create measurement permission under missing role", "POST",
				fmt.Sprintf("/roles/%d/measurements", missing),
				`{"measurement_pattern":"cpu","permissions":["read"]}`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if status, errMsg := doRBACRequest(t, app, tc.method, tc.path, tc.body); status != http.StatusBadRequest {
					t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
				}
			})
		}
	})

	// Missing required fields on create are client errors (400), reported
	// by RBACManager before it ever touches the database.
	t.Run("missing required field returns 400", func(t *testing.T) {
		for _, tc := range []struct {
			name, method, path, body string
		}{
			{
				"role without database pattern", "POST",
				fmt.Sprintf("/teams/%d/roles", team.ID),
				`{"permissions":["read"]}`,
			},
			{
				"role without permissions", "POST",
				fmt.Sprintf("/teams/%d/roles", team.ID),
				`{"database_pattern":"metrics"}`,
			},
			{
				"measurement permission without pattern", "POST",
				fmt.Sprintf("/roles/%d/measurements", role.ID),
				`{"permissions":["read"]}`,
			},
			{
				"measurement permission without permissions", "POST",
				fmt.Sprintf("/roles/%d/measurements", role.ID),
				`{"measurement_pattern":"cpu"}`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if status, errMsg := doRBACRequest(t, app, tc.method, tc.path, tc.body); status != http.StatusBadRequest {
					t.Errorf("status = %d, want 400 — err=%q", status, errMsg)
				}
			})
		}
	})
}
