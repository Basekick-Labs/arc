package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// setupTestRBACManager creates a test RBACManager with a temporary database
func setupTestRBACManager(t *testing.T) (*RBACManager, *AuthManager, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-rbac-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "auth.db")
	logger := zerolog.Nop()

	am, err := NewAuthManager(dbPath, 5*time.Minute, 100, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create AuthManager: %v", err)
	}

	// Create RBACManager without license client (RBAC disabled)
	rm := NewRBACManager(&RBACManagerConfig{
		DB:            am.GetDB(),
		LicenseClient: nil, // No license client - RBAC is disabled
		Logger:        logger,
	})

	cleanup := func() {
		am.Close()
		os.RemoveAll(tmpDir)
	}

	return rm, am, cleanup
}

// =============================================================================
// Organization CRUD Tests
// =============================================================================

func TestCreateOrganization(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	t.Run("basic creation", func(t *testing.T) {
		org, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
			Name:        "acme-corp",
			Description: "Test organization",
		})
		if err != nil {
			t.Fatalf("CreateOrganization failed: %v", err)
		}
		if org == nil {
			t.Fatal("Organization should not be nil")
		}
		if org.ID == 0 {
			t.Error("Organization ID should not be 0")
		}
		if org.Name != "acme-corp" {
			t.Errorf("Name = %s, want %s", org.Name, "acme-corp")
		}
		if !org.Enabled {
			t.Error("Organization should be enabled by default")
		}
	})

	t.Run("empty name", func(t *testing.T) {
		_, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
			Name: "",
		})
		if err == nil {
			t.Error("Expected error for empty name")
		}
	})

	t.Run("duplicate name", func(t *testing.T) {
		// First creation should succeed
		_, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
			Name: "duplicate-org",
		})
		if err != nil {
			t.Fatalf("First CreateOrganization failed: %v", err)
		}

		// Second creation should fail
		_, err = rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
			Name: "duplicate-org",
		})
		if err == nil {
			t.Error("Expected error for duplicate name")
		}
	})
}

func TestGetOrganization(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	t.Run("existing organization", func(t *testing.T) {
		created, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
			Name:        "get-test-org",
			Description: "Test description",
		})
		if err != nil {
			t.Fatalf("CreateOrganization failed: %v", err)
		}

		org, err := rm.GetOrganization(created.ID)
		if err != nil {
			t.Fatalf("GetOrganization failed: %v", err)
		}
		if org == nil {
			t.Fatal("Organization should not be nil")
		}
		if org.Name != "get-test-org" {
			t.Errorf("Name = %s, want %s", org.Name, "get-test-org")
		}
	})

	t.Run("non-existent organization", func(t *testing.T) {
		org, err := rm.GetOrganization(99999)
		if err != nil {
			t.Fatalf("GetOrganization failed: %v", err)
		}
		if org != nil {
			t.Error("Organization should be nil for non-existent ID")
		}
	})
}

func TestListOrganizations(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	// Create some organizations
	rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "org-a"})
	rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "org-b"})
	rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "org-c"})

	orgs, err := rm.ListOrganizations()
	if err != nil {
		t.Fatalf("ListOrganizations failed: %v", err)
	}
	if len(orgs) != 3 {
		t.Errorf("Expected 3 organizations, got %d", len(orgs))
	}
}

func TestUpdateOrganization(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
		Name:        "update-test-org",
		Description: "Original description",
	})

	t.Run("update name", func(t *testing.T) {
		newName := "updated-org-name"
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Name: &newName,
		})
		if err != nil {
			t.Fatalf("UpdateOrganization failed: %v", err)
		}

		updated, _ := rm.GetOrganization(org.ID)
		if updated.Name != "updated-org-name" {
			t.Errorf("Name = %s, want %s", updated.Name, "updated-org-name")
		}
	})

	t.Run("disable organization", func(t *testing.T) {
		enabled := false
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Enabled: &enabled,
		})
		if err != nil {
			t.Fatalf("UpdateOrganization failed: %v", err)
		}

		updated, _ := rm.GetOrganization(org.ID)
		if updated.Enabled {
			t.Error("Organization should be disabled")
		}
	})

	t.Run("non-existent organization", func(t *testing.T) {
		newName := "new-name"
		err := rm.UpdateOrganization(context.Background(), 99999, &UpdateOrganizationRequest{
			Name: &newName,
		})
		if err == nil {
			t.Error("Expected error for non-existent organization")
		}
	})

	t.Run("invalid name rejected", func(t *testing.T) {
		badName := "123-invalid"
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Name: &badName,
		})
		if err == nil {
			t.Error("Expected error for invalid organization name")
		}
	})

	t.Run("empty name rejected", func(t *testing.T) {
		empty := ""
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Name: &empty,
		})
		if err == nil {
			t.Error("Expected error for empty organization name")
		}
	})

	t.Run("name too long rejected", func(t *testing.T) {
		longName := "a" + strings.Repeat("b", 64) // 65 chars
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Name: &longName,
		})
		if err == nil {
			t.Error("Expected error for name exceeding 64 characters")
		}
	})

	t.Run("name with spaces rejected", func(t *testing.T) {
		spaceName := "valid start but invalid"
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Name: &spaceName,
		})
		if err == nil {
			t.Error("Expected error for name with spaces")
		}
	})

	t.Run("update description only skips name validation", func(t *testing.T) {
		desc := "new description"
		err := rm.UpdateOrganization(context.Background(), org.ID, &UpdateOrganizationRequest{
			Description: &desc,
		})
		if err != nil {
			t.Fatalf("UpdateOrganization with description only should succeed: %v", err)
		}
	})
}

func TestDeleteOrganization(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{
		Name: "delete-test-org",
	})

	t.Run("delete existing", func(t *testing.T) {
		err := rm.DeleteOrganization(context.Background(), org.ID)
		if err != nil {
			t.Fatalf("DeleteOrganization failed: %v", err)
		}

		deleted, _ := rm.GetOrganization(org.ID)
		if deleted != nil {
			t.Error("Organization should be deleted")
		}
	})

	t.Run("delete non-existent", func(t *testing.T) {
		err := rm.DeleteOrganization(context.Background(), 99999)
		if err == nil {
			t.Error("Expected error for non-existent organization")
		}
	})
}

// =============================================================================
// Team CRUD Tests
// =============================================================================

func TestCreateTeam(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "team-test-org"})

	t.Run("basic creation", func(t *testing.T) {
		team, err := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{
			Name:        "Engineering",
			Description: "Engineering team",
		})
		if err != nil {
			t.Fatalf("CreateTeam failed: %v", err)
		}
		if team == nil {
			t.Fatal("Team should not be nil")
		}
		if team.OrganizationID != org.ID {
			t.Errorf("OrganizationID = %d, want %d", team.OrganizationID, org.ID)
		}
	})

	t.Run("empty name", func(t *testing.T) {
		_, err := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{Name: ""})
		if err == nil {
			t.Error("Expected error for empty name")
		}
	})

	t.Run("non-existent organization", func(t *testing.T) {
		_, err := rm.CreateTeam(context.Background(), 99999, &CreateTeamRequest{Name: "Test Team"})
		if err == nil {
			t.Error("Expected error for non-existent organization")
		}
	})

	t.Run("duplicate name in organization", func(t *testing.T) {
		rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{Name: "Duplicate Team"})
		_, err := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{Name: "Duplicate Team"})
		if err == nil {
			t.Error("Expected error for duplicate team name in same org")
		}
	})
}

func TestUpdateTeam(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "update-team-org"})
	team, _ := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{
		Name:        "original-team",
		Description: "Original description",
	})

	t.Run("update name", func(t *testing.T) {
		newName := "renamed-team"
		err := rm.UpdateTeam(context.Background(), team.ID, &UpdateTeamRequest{
			Name: &newName,
		})
		if err != nil {
			t.Fatalf("UpdateTeam failed: %v", err)
		}
		updated, _ := rm.GetTeam(team.ID)
		if updated.Name != "renamed-team" {
			t.Errorf("Name = %s, want %s", updated.Name, "renamed-team")
		}
	})

	t.Run("invalid name rejected", func(t *testing.T) {
		badName := "123-invalid"
		err := rm.UpdateTeam(context.Background(), team.ID, &UpdateTeamRequest{
			Name: &badName,
		})
		if err == nil {
			t.Error("Expected error for invalid team name")
		}
	})

	t.Run("empty name rejected", func(t *testing.T) {
		empty := ""
		err := rm.UpdateTeam(context.Background(), team.ID, &UpdateTeamRequest{
			Name: &empty,
		})
		if err == nil {
			t.Error("Expected error for empty team name")
		}
	})

	t.Run("non-existent team", func(t *testing.T) {
		newName := "new-name"
		err := rm.UpdateTeam(context.Background(), 99999, &UpdateTeamRequest{
			Name: &newName,
		})
		if err == nil {
			t.Error("Expected error for non-existent team")
		}
	})

	t.Run("name too long rejected", func(t *testing.T) {
		longName := "a" + strings.Repeat("b", 64) // 65 chars
		err := rm.UpdateTeam(context.Background(), team.ID, &UpdateTeamRequest{
			Name: &longName,
		})
		if err == nil {
			t.Error("Expected error for name exceeding 64 characters")
		}
	})

	t.Run("name with spaces rejected", func(t *testing.T) {
		spaceName := "valid start but invalid"
		err := rm.UpdateTeam(context.Background(), team.ID, &UpdateTeamRequest{
			Name: &spaceName,
		})
		if err == nil {
			t.Error("Expected error for name with spaces")
		}
	})

	t.Run("update description only skips name validation", func(t *testing.T) {
		desc := "new description"
		err := rm.UpdateTeam(context.Background(), team.ID, &UpdateTeamRequest{
			Description: &desc,
		})
		if err != nil {
			t.Fatalf("UpdateTeam with description only should succeed: %v", err)
		}
	})
}

func TestTeamCascadeDelete(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "cascade-test-org"})
	team, _ := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{Name: "cascade-test-team"})

	// Create role and verify it exists
	role, _ := rm.CreateRole(context.Background(), team.ID, &CreateRoleRequest{
		DatabasePattern: "production",
		Permissions:     []string{"read", "write"},
	})

	// Delete organization should cascade delete team and role
	err := rm.DeleteOrganization(context.Background(), org.ID)
	if err != nil {
		t.Fatalf("DeleteOrganization failed: %v", err)
	}

	// Team should be deleted
	deletedTeam, _ := rm.GetTeam(team.ID)
	if deletedTeam != nil {
		t.Error("Team should be deleted")
	}

	// Role should be deleted
	deletedRole, _ := rm.GetRole(role.ID)
	if deletedRole != nil {
		t.Error("Role should be deleted")
	}
}

// =============================================================================
// Role CRUD Tests
// =============================================================================

func TestCreateRole(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "role-test-org"})
	team, _ := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{Name: "role-test-team"})

	t.Run("basic creation", func(t *testing.T) {
		role, err := rm.CreateRole(context.Background(), team.ID, &CreateRoleRequest{
			DatabasePattern: "production",
			Permissions:     []string{"read", "write"},
		})
		if err != nil {
			t.Fatalf("CreateRole failed: %v", err)
		}
		if role.DatabasePattern != "production" {
			t.Errorf("DatabasePattern = %s, want %s", role.DatabasePattern, "production")
		}
		if len(role.Permissions) != 2 {
			t.Errorf("Permissions count = %d, want 2", len(role.Permissions))
		}
	})

	t.Run("wildcard pattern", func(t *testing.T) {
		role, err := rm.CreateRole(context.Background(), team.ID, &CreateRoleRequest{
			DatabasePattern: "*",
			Permissions:     []string{"read"},
		})
		if err != nil {
			t.Fatalf("CreateRole with wildcard failed: %v", err)
		}
		if role.DatabasePattern != "*" {
			t.Errorf("DatabasePattern = %s, want *", role.DatabasePattern)
		}
	})

	t.Run("empty database pattern", func(t *testing.T) {
		_, err := rm.CreateRole(context.Background(), team.ID, &CreateRoleRequest{
			DatabasePattern: "",
			Permissions:     []string{"read"},
		})
		if err == nil {
			t.Error("Expected error for empty database pattern")
		}
	})

	t.Run("empty permissions", func(t *testing.T) {
		_, err := rm.CreateRole(context.Background(), team.ID, &CreateRoleRequest{
			DatabasePattern: "test",
			Permissions:     []string{},
		})
		if err == nil {
			t.Error("Expected error for empty permissions")
		}
	})

	t.Run("invalid permission", func(t *testing.T) {
		_, err := rm.CreateRole(context.Background(), team.ID, &CreateRoleRequest{
			DatabasePattern: "test",
			Permissions:     []string{"read", "invalid_perm"},
		})
		if err == nil {
			t.Error("Expected error for invalid permission")
		}
	})
}

// =============================================================================
// Token Membership Tests
// =============================================================================

func TestTokenMembership(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()

	// Create organization and team
	org, _ := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "membership-test-org"})
	team, _ := rm.CreateTeam(context.Background(), org.ID, &CreateTeamRequest{Name: "membership-test-team"})

	// Create a token
	token, _ := am.CreateToken(context.Background(), "membership-test", "Test token", "read,write", nil)
	tokenInfo := am.VerifyToken(token)

	t.Run("add token to team", func(t *testing.T) {
		membership, err := rm.AddTokenToTeam(context.Background(), tokenInfo.ID, team.ID)
		if err != nil {
			t.Fatalf("AddTokenToTeam failed: %v", err)
		}
		if membership.TokenID != tokenInfo.ID {
			t.Errorf("TokenID = %d, want %d", membership.TokenID, tokenInfo.ID)
		}
		if membership.TeamID != team.ID {
			t.Errorf("TeamID = %d, want %d", membership.TeamID, team.ID)
		}
	})

	t.Run("duplicate membership", func(t *testing.T) {
		_, err := rm.AddTokenToTeam(context.Background(), tokenInfo.ID, team.ID)
		if err == nil {
			t.Error("Expected error for duplicate membership")
		}
	})

	t.Run("get token teams", func(t *testing.T) {
		teams, err := rm.GetTokenTeams(tokenInfo.ID)
		if err != nil {
			t.Fatalf("GetTokenTeams failed: %v", err)
		}
		if len(teams) != 1 {
			t.Errorf("Expected 1 team, got %d", len(teams))
		}
		if teams[0].ID != team.ID {
			t.Errorf("Team ID = %d, want %d", teams[0].ID, team.ID)
		}
	})

	t.Run("remove token from team", func(t *testing.T) {
		err := rm.RemoveTokenFromTeam(context.Background(), tokenInfo.ID, team.ID)
		if err != nil {
			t.Fatalf("RemoveTokenFromTeam failed: %v", err)
		}

		teams, _ := rm.GetTokenTeams(tokenInfo.ID)
		if len(teams) != 0 {
			t.Errorf("Expected 0 teams after removal, got %d", len(teams))
		}
	})

	t.Run("remove non-existent membership", func(t *testing.T) {
		err := rm.RemoveTokenFromTeam(context.Background(), tokenInfo.ID, team.ID)
		if err == nil {
			t.Error("Expected error for non-existent membership")
		}
	})
}

// =============================================================================
// Pattern Matching Tests
// =============================================================================

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern  string
		value    string
		expected bool
	}{
		// Exact matches
		{"production", "production", true},
		{"production", "staging", false},
		{"production", "prod", false},

		// Universal wildcard
		{"*", "anything", true},
		{"*", "production", true},
		{"*", "", true},

		// Prefix wildcards with underscore
		{"prod_*", "prod_us", true},
		{"prod_*", "prod_eu", true},
		{"prod_*", "production", false},
		{"prod_*", "staging", false},

		// Suffix wildcards with underscore
		{"*_metrics", "cpu_metrics", true},
		{"*_metrics", "memory_metrics", true},
		{"*_metrics", "metrics_data", false},

		// General prefix wildcards
		{"prod*", "production", true},
		{"prod*", "prod_us", true},
		{"prod*", "staging", false},

		// Edge cases
		{"", "", true},
		{"test", "", false},
		{"", "test", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_vs_"+tt.value, func(t *testing.T) {
			result := matchPattern(tt.pattern, tt.value)
			if result != tt.expected {
				t.Errorf("matchPattern(%q, %q) = %v, want %v", tt.pattern, tt.value, result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Permission Checking Tests
// =============================================================================

func TestCheckPermission(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()

	// Create token with read,write permissions
	token, _ := am.CreateToken(context.Background(), "perm-test", "Test token", "read,write", nil)
	tokenInfo := am.VerifyToken(token)

	// Without RBAC enabled (no license), should use OSS permissions
	t.Run("OSS permission check - allowed", func(t *testing.T) {
		result := rm.CheckPermission(&PermissionCheckRequest{
			TokenInfo:  tokenInfo,
			Database:   "production",
			Permission: "read",
		})
		if !result.Allowed {
			t.Error("Expected permission to be allowed")
		}
		if result.Source != "token" {
			t.Errorf("Source = %s, want token", result.Source)
		}
	})

	t.Run("OSS permission check - denied", func(t *testing.T) {
		result := rm.CheckPermission(&PermissionCheckRequest{
			TokenInfo:  tokenInfo,
			Database:   "production",
			Permission: "delete",
		})
		if result.Allowed {
			t.Error("Expected permission to be denied")
		}
		if result.Source != "denied" {
			t.Errorf("Source = %s, want denied", result.Source)
		}
	})

	t.Run("nil token", func(t *testing.T) {
		result := rm.CheckPermission(&PermissionCheckRequest{
			TokenInfo:  nil,
			Database:   "production",
			Permission: "read",
		})
		if result.Allowed {
			t.Error("Expected permission to be denied for nil token")
		}
	})
}

func TestCheckPermissionWithAdmin(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()

	// Create admin token
	token, _ := am.CreateToken(context.Background(), "admin-test", "Admin token", "admin", nil)
	tokenInfo := am.VerifyToken(token)

	// Admin should have all permissions
	permissions := []string{"read", "write", "delete", "admin"}
	for _, perm := range permissions {
		t.Run("admin_has_"+perm, func(t *testing.T) {
			result := rm.CheckPermission(&PermissionCheckRequest{
				TokenInfo:  tokenInfo,
				Database:   "any_database",
				Permission: perm,
			})
			if !result.Allowed {
				t.Errorf("Admin should have %s permission", perm)
			}
		})
	}
}

// =============================================================================
// Effective Permissions Tests
// =============================================================================

func TestGetEffectivePermissions(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()

	// Create token
	token, _ := am.CreateToken(context.Background(), "eff-perm-test", "Test token", "read,write", nil)
	tokenInfo := am.VerifyToken(token)

	t.Run("OSS permissions only", func(t *testing.T) {
		perms, err := rm.GetEffectivePermissions(tokenInfo.ID, tokenInfo)
		if err != nil {
			t.Fatalf("GetEffectivePermissions failed: %v", err)
		}
		if len(perms) != 1 {
			t.Errorf("Expected 1 effective permission, got %d", len(perms))
		}
		if perms[0].Source != "token" {
			t.Errorf("Source = %s, want token", perms[0].Source)
		}
		if perms[0].Database != "*" {
			t.Errorf("Database = %s, want *", perms[0].Database)
		}
	})
}

// =============================================================================
// IsRBACEnabled Tests
// =============================================================================

func TestIsRBACEnabled(t *testing.T) {
	rm, _, cleanup := setupTestRBACManager(t)
	defer cleanup()

	// Without license client, RBAC should be disabled
	if rm.IsRBACEnabled() {
		t.Error("RBAC should be disabled without license client")
	}
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestIsValidPermission(t *testing.T) {
	validPerms := []string{"read", "write", "delete", "admin"}
	for _, p := range validPerms {
		if !IsValidPermission(p) {
			t.Errorf("IsValidPermission(%q) = false, want true", p)
		}
	}

	invalidPerms := []string{"", "invalid", "READ", "ADMIN", "create", "execute"}
	for _, p := range invalidPerms {
		if IsValidPermission(p) {
			t.Errorf("IsValidPermission(%q) = true, want false", p)
		}
	}
}

func TestContainsPermission(t *testing.T) {
	tests := []struct {
		perms    []string
		target   string
		expected bool
	}{
		{[]string{"read", "write"}, "read", true},
		{[]string{"read", "write"}, "delete", false},
		{[]string{"admin"}, "read", true},  // admin grants all
		{[]string{"admin"}, "write", true}, // admin grants all
		{[]string{}, "read", false},
		{[]string{"read"}, "", false},
	}

	for _, tt := range tests {
		result := containsPermission(tt.perms, tt.target)
		if result != tt.expected {
			t.Errorf("containsPermission(%v, %q) = %v, want %v", tt.perms, tt.target, result, tt.expected)
		}
	}
}
