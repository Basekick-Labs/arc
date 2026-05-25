package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// Phase A.1: Cluster Auth Convergence (RBAC) — FSM-level tests.
//
// These tests mirror fsm_token_test.go's shape: in-memory only, no
// SQLite, no real Raft. They pin the applier-side invariants for the
// 13 RBAC CommandTypes:
//   - ID is stamped from the Raft log index
//   - Validators reject malformed payloads + bump rejectedRBAC
//   - Secondary indices stay in sync with primary maps
//   - Cascade-on-delete clears every descendant under one log entry
//   - Snapshot round-trip preserves all 5 maps + rebuilds every index
//
// Materialisation callbacks (FSM → RBACManager → SQLite) are exercised
// in internal/auth/cluster_proposer_test.go.

// makeOrgEntry constructs a CreateOrganization-shaped OrganizationEntry.
func makeOrgEntry(name string) OrganizationEntry {
	return OrganizationEntry{
		Name:              name,
		Description:       name + " desc",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		UpdatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
}

func makeTeamEntry(orgID int64, name string) TeamEntry {
	return TeamEntry{
		OrganizationID:    orgID,
		Name:              name,
		Description:       name + " desc",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		UpdatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
}

func makeRoleEntry(teamID int64, pattern string) RoleEntry {
	return RoleEntry{
		TeamID:            teamID,
		DatabasePattern:   pattern,
		Permissions:       "read,write",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
	}
}

func makeMPermEntry(roleID int64, pattern string) MeasurementPermissionEntry {
	return MeasurementPermissionEntry{
		RoleID:             roleID,
		MeasurementPattern: pattern,
		Permissions:        "read",
		CreatedAtUnixNano:  1_700_000_000_000_000_000,
	}
}

func makeMembership(tokenID, teamID int64) TokenMembershipEntry {
	return TokenMembershipEntry{
		TokenID:           tokenID,
		TeamID:            teamID,
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
	}
}

// applyOrCreate is a thin helper that proposes a CreateOrganization and
// returns the resulting ID (stamped from log.Index). Used by cascade
// tests to build up state quickly.
func applyOrCreate(t *testing.T, fsm *ClusterFSM, name string, logIndex uint64) int64 {
	t.Helper()
	cmd := makeCommand(t, CommandCreateOrganization, CreateOrganizationPayload{Organization: makeOrgEntry(name)})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: logIndex}); res != nil {
		t.Fatalf("CreateOrganization %q failed: %v", name, res)
	}
	return int64(logIndex)
}

func applyTeamCreate(t *testing.T, fsm *ClusterFSM, orgID int64, name string, logIndex uint64) int64 {
	t.Helper()
	cmd := makeCommand(t, CommandCreateTeam, CreateTeamPayload{Team: makeTeamEntry(orgID, name)})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: logIndex}); res != nil {
		t.Fatalf("CreateTeam %q failed: %v", name, res)
	}
	return int64(logIndex)
}

func applyRoleCreate(t *testing.T, fsm *ClusterFSM, teamID int64, pattern string, logIndex uint64) int64 {
	t.Helper()
	cmd := makeCommand(t, CommandCreateRole, CreateRolePayload{Role: makeRoleEntry(teamID, pattern)})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: logIndex}); res != nil {
		t.Fatalf("CreateRole %q failed: %v", pattern, res)
	}
	return int64(logIndex)
}

func applyMPermCreate(t *testing.T, fsm *ClusterFSM, roleID int64, pattern string, logIndex uint64) int64 {
	t.Helper()
	cmd := makeCommand(t, CommandCreateMeasurementPermission, CreateMeasurementPermissionPayload{MeasurementPermission: makeMPermEntry(roleID, pattern)})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: logIndex}); res != nil {
		t.Fatalf("CreateMeasurementPermission %q failed: %v", pattern, res)
	}
	return int64(logIndex)
}

func applyMembershipCreate(t *testing.T, fsm *ClusterFSM, tokenID, teamID int64, logIndex uint64) int64 {
	t.Helper()
	cmd := makeCommand(t, CommandAddTokenToTeam, AddTokenToTeamPayload{Membership: makeMembership(tokenID, teamID)})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: logIndex}); res != nil {
		t.Fatalf("AddTokenToTeam(t=%d,team=%d) failed: %v", tokenID, teamID, res)
	}
	return int64(logIndex)
}

// -----------------------------------------------------------------------------
// Organization tests.
// -----------------------------------------------------------------------------

func TestApplyCreateOrganization_Success(t *testing.T) {
	fsm := newTestFSM()
	id := applyOrCreate(t, fsm, "acme", 17)
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.organizations) != 1 {
		t.Fatalf("expected 1 org, got %d", len(fsm.organizations))
	}
	got := fsm.organizations[id]
	if got == nil {
		t.Fatalf("expected org at id=17")
	}
	if got.ID != 17 || got.LSN != 17 {
		t.Errorf("ID/LSN should be stamped from log index 17: got ID=%d LSN=%d", got.ID, got.LSN)
	}
	if got.Name != "acme" {
		t.Errorf("Name mismatch: got %q", got.Name)
	}
	if !got.Enabled {
		t.Errorf("Enabled should default true")
	}
	if fsm.organizationsByName["acme"] != id {
		t.Errorf("organizationsByName[acme] = %d, want %d", fsm.organizationsByName["acme"], id)
	}
}

func TestApplyCreateOrganization_RejectsEmptyName(t *testing.T) {
	fsm := newTestFSM()
	entry := makeOrgEntry("")
	cmd := makeCommand(t, CommandCreateOrganization, CreateOrganizationPayload{Organization: entry})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 17}); res == nil {
		t.Fatal("expected rejection for empty name")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
	if len(fsm.organizations) != 0 {
		t.Errorf("FSM should not hold the rejected entry")
	}
}

func TestApplyCreateOrganization_RejectsZeroCreatedAt(t *testing.T) {
	fsm := newTestFSM()
	entry := makeOrgEntry("acme")
	entry.CreatedAtUnixNano = 0
	cmd := makeCommand(t, CommandCreateOrganization, CreateOrganizationPayload{Organization: entry})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 17}); res == nil {
		t.Fatal("expected rejection for zero CreatedAt")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyCreateOrganization_RejectsDuplicateName(t *testing.T) {
	fsm := newTestFSM()
	applyOrCreate(t, fsm, "acme", 17)
	// Second create with the same name must be rejected by the
	// organizationsByName UNIQUE check.
	cmd := makeCommand(t, CommandCreateOrganization, CreateOrganizationPayload{Organization: makeOrgEntry("acme")})
	res := fsm.Apply(&raft.Log{Data: cmd, Index: 18})
	if res == nil {
		t.Fatal("expected duplicate-name rejection")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
	if len(fsm.organizations) != 1 {
		t.Errorf("FSM should hold exactly 1 org, got %d", len(fsm.organizations))
	}
}

func TestApplyUpdateOrganization_Success(t *testing.T) {
	fsm := newTestFSM()
	id := applyOrCreate(t, fsm, "acme", 17)
	newName := "acme-renamed"
	updateP := UpdateOrganizationPayload{
		ID:                id,
		Name:              newName,
		UpdatedAtUnixNano: 1_800_000_000_000_000_000,
		ChangedFields:     []string{"name"},
	}
	cmd := makeCommand(t, CommandUpdateOrganization, updateP)
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 18}); res != nil {
		t.Fatalf("UpdateOrganization failed: %v", res)
	}
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.organizations[id]
	if got.Name != newName {
		t.Errorf("Name not updated: got %q, want %q", got.Name, newName)
	}
	if got.LSN != 18 {
		t.Errorf("LSN should bump to 18 on update, got %d", got.LSN)
	}
	if _, exists := fsm.organizationsByName["acme"]; exists {
		t.Errorf("old name index entry should be removed")
	}
	if fsm.organizationsByName[newName] != id {
		t.Errorf("new name index entry missing")
	}
}

func TestApplyUpdateOrganization_RejectsDuplicateName(t *testing.T) {
	fsm := newTestFSM()
	idA := applyOrCreate(t, fsm, "acme", 17)
	applyOrCreate(t, fsm, "globex", 18)
	// Rename acme to "globex" — should fail.
	cmd := makeCommand(t, CommandUpdateOrganization, UpdateOrganizationPayload{
		ID:            idA,
		Name:          "globex",
		ChangedFields: []string{"name"},
	})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 19}); res == nil {
		t.Fatal("expected duplicate-name rejection on update")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
	// acme should still be acme.
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if fsm.organizations[idA].Name != "acme" {
		t.Errorf("acme should be unchanged after failed rename, got %q", fsm.organizations[idA].Name)
	}
}

func TestApplyUpdateOrganization_NotFound(t *testing.T) {
	fsm := newTestFSM()
	cmd := makeCommand(t, CommandUpdateOrganization, UpdateOrganizationPayload{
		ID:            999,
		Name:          "x",
		ChangedFields: []string{"name"},
	})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 19}); res == nil {
		t.Fatal("expected not-found rejection")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

// TestApplyUpdateOrganization_DuplicateNameInChangedFieldsKeepsIndexConsistent
// pins Gemini PR #458 round 5 G20: a malformed payload that lists
// "name" twice in ChangedFields must not corrupt organizationsByName.
// Pre-round-5 behaviour: the first iteration of the loop mutated the
// secondary index, the second iteration found the new name "already
// exists", and the function rejected the entire update — leaving the
// primary map untouched but the secondary index pointing the NEW name
// at this ID. The fix stages the index mutation and applies it only
// after the loop succeeds, so a duplicate "name" entry is now a no-op
// rather than a corruption.
func TestApplyUpdateOrganization_DuplicateNameInChangedFieldsKeepsIndexConsistent(t *testing.T) {
	fsm := newTestFSM()
	id := applyOrCreate(t, fsm, "acme", 17)

	// Duplicate "name" in ChangedFields with a non-conflicting rename.
	// Pre-fix: rejects with "name already exists" + leaves the index
	// pointing the new name at id while primary still has the old name.
	// Post-fix: rename applies cleanly; both index and primary agree.
	updateP := UpdateOrganizationPayload{
		ID:            id,
		Name:          "acme-renamed",
		ChangedFields: []string{"name", "name"}, // duplicate intentionally
	}
	cmd := makeCommand(t, CommandUpdateOrganization, updateP)
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 18}); res != nil {
		t.Fatalf("duplicate-name ChangedFields should be a no-op rename, got %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.organizations[id]
	if got.Name != "acme-renamed" {
		t.Errorf("primary map not updated: got %q, want %q", got.Name, "acme-renamed")
	}
	// Index must agree with primary.
	if fsm.organizationsByName["acme-renamed"] != id {
		t.Errorf("secondary index missing new name; got %v", fsm.organizationsByName)
	}
	if _, stale := fsm.organizationsByName["acme"]; stale {
		t.Errorf("secondary index still has old name 'acme'")
	}
}

// -----------------------------------------------------------------------------
// Team tests.
// -----------------------------------------------------------------------------

func TestApplyCreateTeam_Success(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.teams[teamID]
	if got == nil {
		t.Fatalf("expected team at id=18")
	}
	if got.OrganizationID != orgID {
		t.Errorf("OrganizationID mismatch")
	}
	if fsm.teamsByOrg[orgID]["platform"] != teamID {
		t.Errorf("teamsByOrg[%d][platform] = %d, want %d", orgID, fsm.teamsByOrg[orgID]["platform"], teamID)
	}
}

func TestApplyCreateTeam_RejectsUnknownOrg(t *testing.T) {
	fsm := newTestFSM()
	cmd := makeCommand(t, CommandCreateTeam, CreateTeamPayload{Team: makeTeamEntry(999, "platform")})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 17}); res == nil {
		t.Fatal("expected rejection for unknown org")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyCreateTeam_RejectsDuplicateNameInSameOrg(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	applyTeamCreate(t, fsm, orgID, "platform", 18)
	cmd := makeCommand(t, CommandCreateTeam, CreateTeamPayload{Team: makeTeamEntry(orgID, "platform")})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 19}); res == nil {
		t.Fatal("expected duplicate-name rejection")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyCreateTeam_AllowsDuplicateNameAcrossOrgs(t *testing.T) {
	fsm := newTestFSM()
	orgA := applyOrCreate(t, fsm, "acme", 17)
	orgB := applyOrCreate(t, fsm, "globex", 18)
	applyTeamCreate(t, fsm, orgA, "platform", 19)
	applyTeamCreate(t, fsm, orgB, "platform", 20)

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.teams) != 2 {
		t.Errorf("expected 2 teams (one per org), got %d", len(fsm.teams))
	}
}

// TestApplyUpdateTeam_DuplicateNameInChangedFieldsKeepsIndexConsistent
// pins Gemini PR #458 round 5 G21: same FSM-corruption shape as the
// org-level G20 test above, but for applyUpdateTeam's teamsByOrg
// nested index. A malformed payload listing "name" twice in
// ChangedFields must not desync the index from f.teams.
func TestApplyUpdateTeam_DuplicateNameInChangedFieldsKeepsIndexConsistent(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)

	updateP := UpdateTeamPayload{
		ID:            teamID,
		Name:          "platform-renamed",
		ChangedFields: []string{"name", "name"}, // duplicate intentionally
	}
	cmd := makeCommand(t, CommandUpdateTeam, updateP)
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 19}); res != nil {
		t.Fatalf("duplicate-name ChangedFields should be a no-op rename, got %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.teams[teamID]
	if got.Name != "platform-renamed" {
		t.Errorf("primary map not updated: got %q, want %q", got.Name, "platform-renamed")
	}
	// Index must agree with primary.
	orgTeams := fsm.teamsByOrg[orgID]
	if orgTeams["platform-renamed"] != teamID {
		t.Errorf("secondary index missing new name; got %v", orgTeams)
	}
	if _, stale := orgTeams["platform"]; stale {
		t.Errorf("secondary index still has old name 'platform'")
	}
}

func TestApplyDeleteTeam_CascadesRolesAndMemberships(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	roleID := applyRoleCreate(t, fsm, teamID, "production", 19)
	applyMPermCreate(t, fsm, roleID, "metrics_*", 20)
	// Seed a token first so the membership applies don't reject.
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	if res := fsm.Apply(&raft.Log{Data: tokCmd, Index: 21}); res != nil {
		t.Fatalf("seed token: %v", res)
	}
	applyMembershipCreate(t, fsm, 21, teamID, 22)

	// Delete the team.
	delCmd := makeCommand(t, CommandDeleteTeam, DeleteTeamPayload{ID: teamID})
	if res := fsm.Apply(&raft.Log{Data: delCmd, Index: 23}); res != nil {
		t.Fatalf("DeleteTeam: %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if _, ok := fsm.teams[teamID]; ok {
		t.Errorf("team should be deleted")
	}
	if len(fsm.roles) != 0 {
		t.Errorf("roles should cascade-deleted; got %d remaining", len(fsm.roles))
	}
	if len(fsm.measurementPermissions) != 0 {
		t.Errorf("measurement_permissions should cascade-deleted; got %d remaining", len(fsm.measurementPermissions))
	}
	if len(fsm.tokenMemberships) != 0 {
		t.Errorf("memberships should cascade-deleted; got %d remaining", len(fsm.tokenMemberships))
	}
	// Token row itself should survive (we only cascaded from the team side).
	if _, ok := fsm.tokens[21]; !ok {
		t.Errorf("token should NOT be cascade-deleted when its team is deleted")
	}
}

// -----------------------------------------------------------------------------
// Role tests.
// -----------------------------------------------------------------------------

func TestApplyCreateRole_Success(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	roleID := applyRoleCreate(t, fsm, teamID, "production", 19)

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.roles[roleID]
	if got == nil {
		t.Fatalf("expected role at id=19")
	}
	if got.TeamID != teamID {
		t.Errorf("TeamID mismatch")
	}
	if _, ok := fsm.rolesByTeam[teamID][roleID]; !ok {
		t.Errorf("rolesByTeam should contain the new role")
	}
}

func TestApplyCreateRole_RejectsInvalidPermission(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	entry := makeRoleEntry(teamID, "production")
	entry.Permissions = "read,write,nuke" // invalid verb
	cmd := makeCommand(t, CommandCreateRole, CreateRolePayload{Role: entry})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 19}); res == nil {
		t.Fatal("expected rejection for invalid permission")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyCreateRole_RejectsUnknownTeam(t *testing.T) {
	fsm := newTestFSM()
	cmd := makeCommand(t, CommandCreateRole, CreateRolePayload{Role: makeRoleEntry(999, "production")})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 17}); res == nil {
		t.Fatal("expected rejection for unknown team")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyDeleteRole_CascadesMeasurementPermissions(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	roleID := applyRoleCreate(t, fsm, teamID, "production", 19)
	applyMPermCreate(t, fsm, roleID, "metrics_*", 20)
	applyMPermCreate(t, fsm, roleID, "events_*", 21)

	delCmd := makeCommand(t, CommandDeleteRole, DeleteRolePayload{ID: roleID})
	if res := fsm.Apply(&raft.Log{Data: delCmd, Index: 22}); res != nil {
		t.Fatalf("DeleteRole: %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if _, ok := fsm.roles[roleID]; ok {
		t.Errorf("role should be deleted")
	}
	if len(fsm.measurementPermissions) != 0 {
		t.Errorf("measurement_permissions should cascade-deleted; got %d remaining", len(fsm.measurementPermissions))
	}
	if _, ok := fsm.rolesByTeam[teamID][roleID]; ok {
		t.Errorf("rolesByTeam should not contain the deleted role")
	}
}

// -----------------------------------------------------------------------------
// MeasurementPermission tests.
// -----------------------------------------------------------------------------

func TestApplyCreateMeasurementPermission_Success(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	roleID := applyRoleCreate(t, fsm, teamID, "production", 19)
	mpermID := applyMPermCreate(t, fsm, roleID, "metrics_*", 20)

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.measurementPermissions[mpermID]
	if got == nil {
		t.Fatalf("expected measurement_permission at id=20")
	}
	if got.MeasurementPattern != "metrics_*" {
		t.Errorf("MeasurementPattern mismatch")
	}
}

func TestApplyDeleteMeasurementPermission_Success(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	roleID := applyRoleCreate(t, fsm, teamID, "production", 19)
	mpermID := applyMPermCreate(t, fsm, roleID, "metrics_*", 20)

	delCmd := makeCommand(t, CommandDeleteMeasurementPermission, DeleteMeasurementPermissionPayload{ID: mpermID})
	if res := fsm.Apply(&raft.Log{Data: delCmd, Index: 21}); res != nil {
		t.Fatalf("DeleteMeasurementPermission: %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if _, ok := fsm.measurementPermissions[mpermID]; ok {
		t.Errorf("measurement_permission should be deleted")
	}
}

// -----------------------------------------------------------------------------
// TokenMembership tests.
// -----------------------------------------------------------------------------

func TestApplyAddTokenToTeam_Success(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	if res := fsm.Apply(&raft.Log{Data: tokCmd, Index: 19}); res != nil {
		t.Fatalf("seed token: %v", res)
	}
	tokenID := int64(19)
	memID := applyMembershipCreate(t, fsm, tokenID, teamID, 20)

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	got := fsm.tokenMemberships[memID]
	if got == nil {
		t.Fatalf("expected membership at id=20")
	}
	if got.TokenID != tokenID || got.TeamID != teamID {
		t.Errorf("TokenID/TeamID mismatch")
	}
	if fsm.tokenMembershipsByPair[tokenID][teamID] != memID {
		t.Errorf("byPair index broken")
	}
	if _, ok := fsm.tokenMembershipsByToken[tokenID][memID]; !ok {
		t.Errorf("byToken index broken")
	}
	if _, ok := fsm.tokenMembershipsByTeam[teamID][memID]; !ok {
		t.Errorf("byTeam index broken")
	}
}

func TestApplyAddTokenToTeam_RejectsDuplicatePair(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	fsm.Apply(&raft.Log{Data: tokCmd, Index: 19})
	tokenID := int64(19)
	applyMembershipCreate(t, fsm, tokenID, teamID, 20)

	// Duplicate (tokenID, teamID) → reject.
	dupCmd := makeCommand(t, CommandAddTokenToTeam, AddTokenToTeamPayload{Membership: makeMembership(tokenID, teamID)})
	if res := fsm.Apply(&raft.Log{Data: dupCmd, Index: 21}); res == nil {
		t.Fatal("expected duplicate-pair rejection")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyAddTokenToTeam_RejectsUnknownToken(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	cmd := makeCommand(t, CommandAddTokenToTeam, AddTokenToTeamPayload{Membership: makeMembership(999, teamID)})
	if res := fsm.Apply(&raft.Log{Data: cmd, Index: 19}); res == nil {
		t.Fatal("expected unknown-token rejection")
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("rejectedRBAC should be 1, got %d", fsm.RejectedRBACCount())
	}
}

func TestApplyRemoveTokenFromTeam_Success(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	fsm.Apply(&raft.Log{Data: tokCmd, Index: 19})
	tokenID := int64(19)
	applyMembershipCreate(t, fsm, tokenID, teamID, 20)

	remCmd := makeCommand(t, CommandRemoveTokenFromTeam, RemoveTokenFromTeamPayload{TokenID: tokenID, TeamID: teamID})
	if res := fsm.Apply(&raft.Log{Data: remCmd, Index: 21}); res != nil {
		t.Fatalf("RemoveTokenFromTeam: %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.tokenMemberships) != 0 {
		t.Errorf("membership should be deleted; got %d remaining", len(fsm.tokenMemberships))
	}
	if _, ok := fsm.tokenMembershipsByPair[tokenID]; ok {
		t.Errorf("byPair index entry for token should be cleaned up")
	}
}

// TestApplyDeleteToken_CascadesMemberships pins the Phase A.1 extension
// to Phase A's applyDeleteToken: when a token is hard-deleted, every
// membership it held must also be removed from the FSM. This mirrors
// the SQLite FK ON DELETE CASCADE on rbac_token_memberships.token_id.
func TestApplyDeleteToken_CascadesMemberships(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamA := applyTeamCreate(t, fsm, orgID, "platform", 18)
	teamB := applyTeamCreate(t, fsm, orgID, "data", 19)
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	fsm.Apply(&raft.Log{Data: tokCmd, Index: 20})
	tokenID := int64(20)
	applyMembershipCreate(t, fsm, tokenID, teamA, 21)
	applyMembershipCreate(t, fsm, tokenID, teamB, 22)

	delCmd := makeCommand(t, CommandDeleteToken, DeleteTokenPayload{ID: tokenID})
	if res := fsm.Apply(&raft.Log{Data: delCmd, Index: 23}); res != nil {
		t.Fatalf("DeleteToken: %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if _, ok := fsm.tokens[tokenID]; ok {
		t.Errorf("token should be deleted")
	}
	if len(fsm.tokenMemberships) != 0 {
		t.Errorf("memberships should cascade-deleted; got %d remaining", len(fsm.tokenMemberships))
	}
	if _, ok := fsm.tokenMembershipsByToken[tokenID]; ok {
		t.Errorf("byToken index entry should be cleaned up")
	}
	if _, ok := fsm.tokenMembershipsByTeam[teamA]; ok {
		t.Errorf("teamA's byTeam entry should be cleared if it held only this token's membership")
	}
}

// -----------------------------------------------------------------------------
// Full-cascade test (org → teams → roles → measurement_permissions → memberships).
// -----------------------------------------------------------------------------

func TestApplyDeleteOrganization_CascadesAllLevels(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	// 2 teams, each with 2 roles, each role with 2 measurement_perms.
	teamA := applyTeamCreate(t, fsm, orgID, "platform", 18)
	teamB := applyTeamCreate(t, fsm, orgID, "data", 19)
	roleA1 := applyRoleCreate(t, fsm, teamA, "production", 20)
	roleA2 := applyRoleCreate(t, fsm, teamA, "staging", 21)
	roleB1 := applyRoleCreate(t, fsm, teamB, "analytics", 22)
	roleB2 := applyRoleCreate(t, fsm, teamB, "warehouse", 23)
	applyMPermCreate(t, fsm, roleA1, "metrics_*", 24)
	applyMPermCreate(t, fsm, roleA1, "events_*", 25)
	applyMPermCreate(t, fsm, roleA2, "metrics_*", 26)
	applyMPermCreate(t, fsm, roleA2, "events_*", 27)
	applyMPermCreate(t, fsm, roleB1, "warehouse_*", 28)
	applyMPermCreate(t, fsm, roleB1, "logs_*", 29)
	applyMPermCreate(t, fsm, roleB2, "warehouse_*", 30)
	applyMPermCreate(t, fsm, roleB2, "logs_*", 31)
	// One token with memberships in both teamA and teamB.
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	fsm.Apply(&raft.Log{Data: tokCmd, Index: 32})
	applyMembershipCreate(t, fsm, 32, teamA, 33)
	applyMembershipCreate(t, fsm, 32, teamB, 34)

	// Sanity-check the seeded state.
	fsm.mu.RLock()
	if len(fsm.organizations) != 1 ||
		len(fsm.teams) != 2 ||
		len(fsm.roles) != 4 ||
		len(fsm.measurementPermissions) != 8 ||
		len(fsm.tokenMemberships) != 2 {
		fsm.mu.RUnlock()
		t.Fatalf("seed state wrong: orgs=%d teams=%d roles=%d mperms=%d memberships=%d",
			len(fsm.organizations), len(fsm.teams), len(fsm.roles),
			len(fsm.measurementPermissions), len(fsm.tokenMemberships))
	}
	_ = roleA1
	_ = roleA2
	_ = roleB1
	_ = roleB2
	fsm.mu.RUnlock()

	// Delete the org — should cascade through every level.
	delCmd := makeCommand(t, CommandDeleteOrganization, DeleteOrganizationPayload{ID: orgID})
	if res := fsm.Apply(&raft.Log{Data: delCmd, Index: 35}); res != nil {
		t.Fatalf("DeleteOrganization: %v", res)
	}

	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.organizations) != 0 {
		t.Errorf("organizations not cleared: %d", len(fsm.organizations))
	}
	if len(fsm.organizationsByName) != 0 {
		t.Errorf("organizationsByName not cleared: %d", len(fsm.organizationsByName))
	}
	if len(fsm.teams) != 0 {
		t.Errorf("teams not cleared: %d", len(fsm.teams))
	}
	if len(fsm.teamsByOrg) != 0 {
		t.Errorf("teamsByOrg not cleared: %d", len(fsm.teamsByOrg))
	}
	if len(fsm.roles) != 0 {
		t.Errorf("roles not cleared: %d", len(fsm.roles))
	}
	if len(fsm.rolesByTeam) != 0 {
		t.Errorf("rolesByTeam not cleared: %d", len(fsm.rolesByTeam))
	}
	if len(fsm.measurementPermissions) != 0 {
		t.Errorf("measurementPermissions not cleared: %d", len(fsm.measurementPermissions))
	}
	if len(fsm.measurementPermsByRole) != 0 {
		t.Errorf("measurementPermsByRole not cleared: %d", len(fsm.measurementPermsByRole))
	}
	if len(fsm.tokenMemberships) != 0 {
		t.Errorf("tokenMemberships not cleared: %d", len(fsm.tokenMemberships))
	}
	// Token row survives — only the org cascade was triggered.
	if _, ok := fsm.tokens[32]; !ok {
		t.Errorf("token should survive an organization delete")
	}
}

// -----------------------------------------------------------------------------
// Snapshot round-trip.
// -----------------------------------------------------------------------------

func TestFSMSnapshot_RoundTripsRBAC(t *testing.T) {
	fsm := newTestFSM()
	orgID := applyOrCreate(t, fsm, "acme", 17)
	teamID := applyTeamCreate(t, fsm, orgID, "platform", 18)
	roleID := applyRoleCreate(t, fsm, teamID, "production", 19)
	applyMPermCreate(t, fsm, roleID, "metrics_*", 20)
	tokTok := TokenEntry{
		Name:              "t1",
		TokenHash:         "h",
		TokenPrefix:       "p",
		Permissions:       "read",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		Enabled:           true,
	}
	tokCmd := makeCommand(t, CommandCreateToken, CreateTokenPayload{Token: tokTok})
	fsm.Apply(&raft.Log{Data: tokCmd, Index: 21})
	applyMembershipCreate(t, fsm, 21, teamID, 22)

	// Snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var buf bytes.Buffer
	sink := &testSnapshotSink{Writer: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	// Restore into a fresh FSM.
	fsm2 := newTestFSM()
	if err := fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	fsm2.mu.RLock()
	defer fsm2.mu.RUnlock()
	if len(fsm2.organizations) != 1 {
		t.Errorf("orgs not restored: %d", len(fsm2.organizations))
	}
	if fsm2.organizationsByName["acme"] != orgID {
		t.Errorf("organizationsByName not rebuilt")
	}
	if len(fsm2.teams) != 1 {
		t.Errorf("teams not restored: %d", len(fsm2.teams))
	}
	if fsm2.teamsByOrg[orgID]["platform"] != teamID {
		t.Errorf("teamsByOrg not rebuilt")
	}
	if len(fsm2.roles) != 1 {
		t.Errorf("roles not restored: %d", len(fsm2.roles))
	}
	if _, ok := fsm2.rolesByTeam[teamID][roleID]; !ok {
		t.Errorf("rolesByTeam not rebuilt")
	}
	if len(fsm2.measurementPermissions) != 1 {
		t.Errorf("measurement_permissions not restored: %d", len(fsm2.measurementPermissions))
	}
	if _, ok := fsm2.measurementPermsByRole[roleID]; !ok {
		t.Errorf("measurementPermsByRole not rebuilt")
	}
	if len(fsm2.tokenMemberships) != 1 {
		t.Errorf("memberships not restored: %d", len(fsm2.tokenMemberships))
	}
	if fsm2.tokenMembershipsByPair[21][teamID] == 0 {
		t.Errorf("tokenMembershipsByPair not rebuilt")
	}
	if _, ok := fsm2.tokenMembershipsByToken[21]; !ok {
		t.Errorf("tokenMembershipsByToken not rebuilt")
	}
	if _, ok := fsm2.tokenMembershipsByTeam[teamID]; !ok {
		t.Errorf("tokenMembershipsByTeam not rebuilt")
	}
}

func TestFSMSnapshot_RestoreQuarantinesMalformedRBAC(t *testing.T) {
	// Hand-craft a snapshot with a malformed organization (empty name).
	// Restore should skip it, increment rejectedRBAC, and continue.
	malformed := FSMSnapshot{
		Nodes: map[string]*NodeInfo{},
		Organizations: map[int64]*OrganizationEntry{
			1: {ID: 1, Name: "", CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1, Enabled: true}, // bad
			2: {ID: 2, Name: "good", CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1, Enabled: true},
		},
	}
	data, err := json.Marshal(malformed)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	fsm := newTestFSM()
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("expected 1 rejected RBAC entry, got %d", fsm.RejectedRBACCount())
	}
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.organizations) != 1 {
		t.Errorf("expected 1 good org restored, got %d", len(fsm.organizations))
	}
	if _, ok := fsm.organizations[2]; !ok {
		t.Errorf("good organization should be restored")
	}
}

func TestFSMSnapshot_RestoreSkipsOrphanedRBAC(t *testing.T) {
	// Team referencing non-existent org → quarantined.
	malformed := FSMSnapshot{
		Nodes: map[string]*NodeInfo{},
		Organizations: map[int64]*OrganizationEntry{
			1: {ID: 1, Name: "acme", CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1, Enabled: true},
		},
		Teams: map[int64]*TeamEntry{
			2: {ID: 2, OrganizationID: 1, Name: "platform", CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1, Enabled: true},
			3: {ID: 3, OrganizationID: 999, Name: "ghost", CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1, Enabled: true}, // orphan
		},
	}
	data, err := json.Marshal(malformed)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	fsm := newTestFSM()
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if fsm.RejectedRBACCount() != 1 {
		t.Errorf("expected 1 rejected RBAC entry (the orphan), got %d", fsm.RejectedRBACCount())
	}
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	if len(fsm.teams) != 1 {
		t.Errorf("expected 1 surviving team, got %d", len(fsm.teams))
	}
}
