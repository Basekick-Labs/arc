package auth

import (
	"context"
	"testing"
)

// scopedRig builds a team whose role covers database "db1" and whose only
// measurement grant is "cpu:read", then returns a token in that team with the
// given coarse permissions.
//
// Note the license: setupTestRBACManager wires LicenseClient: nil, so
// IsRBACEnabled() is false throughout. That is deliberate — enforcement must
// not consult the license (a lapsed trial would otherwise widen every tenant
// token to full read), so these tests double as the regression test for that
// property. See the RBAC ENFORCEMENT MODEL note in rbac_manager.go.
func scopedRig(t *testing.T, coarse string) (*RBACManager, *TokenInfo, func()) {
	t.Helper()
	rm, am, cleanup := setupTestRBACManager(t)
	ctx := context.Background()

	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "tenant1"})
	if err != nil {
		t.Fatal(err)
	}
	role, err := rm.CreateRole(ctx, team.ID, &CreateRoleRequest{
		DatabasePattern: "db1", Permissions: []string{"read"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rm.CreateMeasurementPermission(ctx, role.ID, &CreateMeasurementPermissionRequest{
		MeasurementPattern: "cpu", Permissions: []string{"read"},
	}); err != nil {
		t.Fatal(err)
	}

	raw, err := am.CreateToken(ctx, "tenant1-tok", "scoped", coarse, nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(raw)
	if info == nil {
		t.Fatalf("VerifyToken returned nil for coarse=%q", coarse)
	}
	if _, err := rm.AddTokenToTeam(ctx, info.ID, team.ID); err != nil {
		t.Fatal(err)
	}
	rm.InvalidateTokenCache(info.ID)
	return rm, info, cleanup
}

func check(rm *RBACManager, info *TokenInfo, db, meas string) *PermissionCheckResult {
	return rm.CheckPermission(&PermissionCheckRequest{
		TokenInfo: info, Database: db, Measurement: meas, Permission: "read",
	})
}

// An RBAC denial is final: the token's coarse permissions no longer override
// it. Before this, checkPermissionUncached fell back to checkOSSPermission on
// denial, so a token holding the coarse "read" bit — which RequireRead demands
// of every caller — read every database regardless of its grants, and no
// read path in Arc could be narrowed by RBAC at all.
func TestRBACDenialIsFinal_CoarseReadDoesNotOverride(t *testing.T) {
	rm, info, cleanup := scopedRig(t, "read")
	defer cleanup()

	if got := check(rm, info, "db1", "cpu"); !got.Allowed || got.Source != "rbac" {
		t.Errorf("the granted measurement must be allowed by RBAC: allowed=%v source=%q", got.Allowed, got.Source)
	}
	for _, tc := range []struct{ db, meas, why string }{
		{"db1", "secrets", "an ungranted measurement inside the granted database"},
		{"otherdb", "cpu", "an entirely ungranted database"},
		{"*", "*", "the wildcard both grants fail to cover"},
	} {
		got := check(rm, info, tc.db, tc.meas)
		if got.Allowed {
			t.Errorf("%s must be denied, got allowed with source=%q", tc.why, got.Source)
		}
	}
}

// Case 1: an admin token bypasses RBAC. Without this an operator could lock
// themselves out of their own tooling by adding an admin token to a team.
func TestRBACAdminBreakGlass(t *testing.T) {
	rm, info, cleanup := scopedRig(t, "read,write,delete,admin")
	defer cleanup()

	for _, tc := range [][2]string{{"otherdb", "secrets"}, {"*", "*"}} {
		got := check(rm, info, tc[0], tc[1])
		if !got.Allowed || got.Source != "token" {
			t.Errorf("admin must bypass RBAC for %s/%s: allowed=%v source=%q",
				tc[0], tc[1], got.Allowed, got.Source)
		}
	}
}

// Case 3: a token with no team memberships keeps resolving to its coarse
// permissions. This is the "backward compatible with OSS tokens" guarantee
// the original RBAC work made, and removing the denial fallback must not
// change it.
func TestRBACNoMembershipsUsesCoarsePermissions(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()
	ctx := context.Background()

	raw, err := am.CreateToken(ctx, "plain", "no teams", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(raw)
	if info == nil {
		t.Fatal("VerifyToken returned nil")
	}
	got := check(rm, info, "anydb", "anymeasurement")
	if !got.Allowed || got.Source != "token" {
		t.Errorf("a token with no memberships must use coarse permissions: allowed=%v source=%q",
			got.Allowed, got.Source)
	}
}

// An RBAC-only token (PermissionsNone) is the class the whole feature exists
// for — see the PermissionsNone doc comment, "whose access comes solely from
// team/role grants". It must be granted exactly its grants.
func TestRBACOnlyTokenGetsExactlyItsGrants(t *testing.T) {
	rm, info, cleanup := scopedRig(t, PermissionsNone)
	defer cleanup()

	if len(info.Permissions) != 0 {
		t.Fatalf("expected no coarse permissions, got %v", info.Permissions)
	}
	if got := check(rm, info, "db1", "cpu"); !got.Allowed || got.Source != "rbac" {
		t.Errorf("the granted measurement must be allowed: allowed=%v source=%q", got.Allowed, got.Source)
	}
	if got := check(rm, info, "db1", "secrets"); got.Allowed {
		t.Errorf("an ungranted measurement must be denied, got source=%q", got.Source)
	}
}

// The batch path carries its own copy of the decision and had its own copy of
// the fallback, so it needs its own regression test.
func TestRBACDenialIsFinal_BatchPath(t *testing.T) {
	rm, info, cleanup := scopedRig(t, "read")
	defer cleanup()

	reqs := []*PermissionCheckRequest{
		{TokenInfo: info, Database: "db1", Measurement: "cpu", Permission: "read"},
		{TokenInfo: info, Database: "db1", Measurement: "secrets", Permission: "read"},
		{TokenInfo: info, Database: "otherdb", Measurement: "cpu", Permission: "read"},
	}
	results := rm.CheckPermissionsBatch(reqs)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if !results[0].Allowed || results[0].Source != "rbac" {
		t.Errorf("granted: allowed=%v source=%q", results[0].Allowed, results[0].Source)
	}
	if results[1].Allowed {
		t.Errorf("ungranted measurement allowed in batch, source=%q", results[1].Source)
	}
	if results[2].Allowed {
		t.Errorf("ungranted database allowed in batch, source=%q", results[2].Source)
	}
}

// A failure to load the token's RBAC data denies rather than falling through
// to the coarse permissions: the membership tables are created unconditionally
// by the auth schema, so an error means a broken database, and we cannot tell
// whether RBAC restricts this token.
func TestRBACLoadFailureFailsClosed(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()
	ctx := context.Background()

	raw, err := am.CreateToken(ctx, "tok", "d", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(raw)
	if info == nil {
		t.Fatal("VerifyToken returned nil")
	}
	// Break the store underneath the manager. Close the *sql.DB directly
	// rather than the AuthManager, whose Close the rig's cleanup also calls.
	if err := am.GetDB().Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	rm.InvalidateTokenCache(info.ID)

	got := check(rm, info, "db1", "cpu")
	if got.Allowed {
		t.Errorf("a broken permission store must deny, got allowed with source=%q", got.Source)
	}
}
