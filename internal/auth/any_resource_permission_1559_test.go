package auth

import (
	"context"
	"testing"
)

// #1559: HasPermissionAnywhere is the door question for routes whose
// resources are in the body. Same rules as CanAccessAnythingIn with every
// database matching.
func TestHasPermissionAnywhere(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()
	ctx := context.Background()
	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "o"})
	if err != nil {
		t.Fatal(err)
	}
	team := func(name string, rolePerms, measPerms []string) int64 {
		t.Helper()
		tm, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		role, err := rm.CreateRole(ctx, tm.ID, &CreateRoleRequest{DatabasePattern: "tenanta", Permissions: rolePerms})
		if err != nil {
			t.Fatal(err)
		}
		if measPerms != nil {
			if _, err := rm.CreateMeasurementPermission(ctx, role.ID, &CreateMeasurementPermissionRequest{
				MeasurementPattern: "cpu", Permissions: measPerms,
			}); err != nil {
				t.Fatal(err)
			}
		}
		return tm.ID
	}
	token := func(name, perms string, teamID int64) *TokenInfo {
		t.Helper()
		tok, err := am.CreateToken(ctx, name, name, perms, nil)
		if err != nil {
			t.Fatal(err)
		}
		info := am.VerifyToken(tok)
		if info == nil {
			t.Fatal("verify")
		}
		if teamID != 0 {
			if _, err := rm.AddTokenToTeam(ctx, info.ID, teamID); err != nil {
				t.Fatal(err)
			}
		}
		return info
	}

	readOnly := token("read-grant", PermissionsNone, team("r", []string{"read"}, nil))
	measRead := token("meas-read", PermissionsNone, team("m", []string{"read", "write"}, []string{"read"}))
	disabledTeamID := team("off", []string{"read", "write"}, nil)
	inDisabledTeam := token("disabled-team", PermissionsNone, disabledTeamID)
	off := false
	if err := rm.UpdateTeam(ctx, disabledTeamID, &UpdateTeamRequest{Enabled: &off}); err != nil {
		t.Fatal(err)
	}
	coarseRead := token("coarse-read", "read", 0)
	none := token("none", PermissionsNone, 0)
	admin := token("admin", "admin", 0)
	coarseWithNoGrant := token("coarse-no-grant", "read,write", team("d", []string{"delete"}, nil))
	disabledToken := *readOnly
	disabledToken.Enabled = false
	rm.InvalidateAllCache()

	for _, c := range []struct {
		name       string
		tok        *TokenInfo
		permission string
		want       bool
	}{
		{"nil token", nil, "read", false},
		{"RBAC read grant, read", readOnly, "read", true},
		{"RBAC read grant, write is not implied", readOnly, "write", false},
		{"measurement grant read, read", measRead, "read", true},
		// Role-level write does not count once the role has measurement grants.
		{"measurement grant read, write", measRead, "write", false},
		{"only team disabled", inDisabledTeam, "read", false},
		{"disabled token", &disabledToken, "read", false},
		{"no teams, coarse read", coarseRead, "read", true},
		{"no teams, coarse read, write", coarseRead, "write", false},
		{"no teams, no permissions", none, "read", false},
		{"coarse admin", admin, "write", true},
		// RBAC is authoritative for a team member: the coarse bit is not consulted.
		{"coarse read,write, team grants neither", coarseWithNoGrant, "read", false},
	} {
		if got := rm.HasPermissionAnywhere(c.tok, c.permission); got != c.want {
			t.Errorf("%s: HasPermissionAnywhere(%q) = %v, want %v", c.name, c.permission, got, c.want)
		}
	}

	// A store that cannot be read fails closed, except for coarse admin,
	// which is decided before the store is asked.
	rm.InvalidateAllCache()
	if err := am.GetDB().Close(); err != nil {
		t.Fatal(err)
	}
	if rm.HasPermissionAnywhere(readOnly, "read") {
		t.Error("RBAC data load failure: allowed, want denied")
	}
	if !rm.HasPermissionAnywhere(admin, "read") {
		t.Error("coarse admin refused while the store is unreadable")
	}
}
