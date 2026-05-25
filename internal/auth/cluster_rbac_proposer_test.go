package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Phase A.1: Cluster Auth Convergence (RBAC) — proposer round-trip tests.
//
// Mirrors cluster_proposer_test.go (Phase A tokens). A fakeRBACProposer
// simulates a Raft leader by deterministically assigning monotonic IDs
// and invoking RBACManager.Apply<X> directly, collapsing the FSM apply
// + materialise loop into a single synchronous call site so tests can
// assert against local SQLite state immediately.
//
// What these tests pin:
//   1. Each of the 13 RBAC writes routes through the proposer and lands
//      in local SQLite as a materialised row.
//   2. Wire-format JSON tags (clusterXxxEntryWire) round-trip without
//      field drift — the proposer marshals, the fake unmarshals,
//      content survives.
//   3. UNIQUE / parent-FK rejections surface as proposer errors that
//      the RBACManager translates into the same user-facing error
//      messages as the OSS path.
//   4. ApplyCreateXxx's SELECT-first divergence detection accepts
//      identical re-applies (log replay) and rejects content drift.

// fakeRBACProposer is the RBAC-side equivalent of fakeProposer. It
// shares the same IsLeader policy (always leader in single-node tests)
// and uses logIdx to mint Raft-style IDs.
type fakeRBACProposer struct {
	mu     sync.Mutex
	logIdx atomic.Int64
	rm     *RBACManager
	// memberships tracked here to enforce UNIQUE(token_id, team_id)
	// at the fake's layer, mirroring the FSM's tokenMembershipsByPair.
	memberships map[int64]map[int64]int64 // tokenID → teamID → membershipID
	// orgs / teams names tracked for UNIQUE enforcement at the fake
	// (mirroring organizationsByName / teamsByOrg in the FSM).
	orgsByName        map[string]int64
	teamsByOrgAndName map[int64]map[string]int64
	// Parent-FK checks: which orgs / teams / roles exist.
	orgs  map[int64]struct{}
	teams map[int64]struct{}
	roles map[int64]struct{}
}

func newFakeRBACProposer() *fakeRBACProposer {
	return &fakeRBACProposer{
		memberships:       make(map[int64]map[int64]int64),
		orgsByName:        make(map[string]int64),
		teamsByOrgAndName: make(map[int64]map[string]int64),
		orgs:              make(map[int64]struct{}),
		teams:             make(map[int64]struct{}),
		roles:             make(map[int64]struct{}),
	}
}

func (f *fakeRBACProposer) setRBACManager(rm *RBACManager) { f.rm = rm }

func (f *fakeRBACProposer) IsLeader() bool { return true }

func (f *fakeRBACProposer) Propose(ctx context.Context, cmdType uint8, payload []byte, timeout time.Duration) error {
	if f.rm == nil {
		return fmt.Errorf("fakeRBACProposer: RBACManager not wired")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	idx := f.logIdx.Add(1)
	switch cmdType {
	case ProposalCommandCreateOrganization:
		var p createOrganizationPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if _, exists := f.orgsByName[p.Organization.Name]; exists {
			return errors.New("organization name already exists")
		}
		entry := ClusterOrganizationEntry{
			ID:                idx,
			Name:              p.Organization.Name,
			Description:       p.Organization.Description,
			CreatedAtUnixNano: p.Organization.CreatedAtUnixNano,
			UpdatedAtUnixNano: p.Organization.UpdatedAtUnixNano,
			Enabled:           true,
			LSN:               uint64(idx),
		}
		if err := f.rm.ApplyCreateOrganization(entry); err != nil {
			return err
		}
		f.orgsByName[entry.Name] = idx
		f.orgs[idx] = struct{}{}
		return nil

	case ProposalCommandUpdateOrganization:
		var p updateOrganizationPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if _, ok := f.orgs[p.ID]; !ok {
			return fmt.Errorf("organization %d not found", p.ID)
		}
		// Load existing fields from SQLite to construct full entry.
		existing, err := f.rm.GetOrganization(p.ID)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("organization %d not found", p.ID)
		}
		entry := ClusterOrganizationEntry{
			ID:                p.ID,
			Name:              existing.Name,
			Description:       existing.Description,
			CreatedAtUnixNano: existing.CreatedAt.UnixNano(),
			UpdatedAtUnixNano: p.UpdatedAtUnixNano,
			Enabled:           existing.Enabled,
			LSN:               uint64(idx),
		}
		changed := map[string]bool{}
		for _, c := range p.ChangedFields {
			changed[c] = true
		}
		if changed["name"] {
			if existingID, exists := f.orgsByName[p.Name]; exists && existingID != p.ID {
				return errors.New("organization name already exists")
			}
			delete(f.orgsByName, entry.Name)
			f.orgsByName[p.Name] = p.ID
			entry.Name = p.Name
		}
		if changed["description"] {
			entry.Description = p.Description
		}
		if changed["enabled"] {
			entry.Enabled = p.Enabled
		}
		return f.rm.ApplyUpdateOrganization(entry)

	case ProposalCommandDeleteOrganization:
		var p deleteOrganizationPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		existing, _ := f.rm.GetOrganization(p.ID)
		if existing != nil {
			delete(f.orgsByName, existing.Name)
		}
		delete(f.orgs, p.ID)
		// Cascade in the fake's parent-FK maps too (SQLite ON DELETE CASCADE
		// fires inside ApplyDeleteOrganization on the materialise side).
		delete(f.teamsByOrgAndName, p.ID)
		return f.rm.ApplyDeleteOrganization(p.ID)

	case ProposalCommandCreateTeam:
		var p createTeamPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if _, ok := f.orgs[p.Team.OrganizationID]; !ok {
			return fmt.Errorf("organization %d not found", p.Team.OrganizationID)
		}
		if names, ok := f.teamsByOrgAndName[p.Team.OrganizationID]; ok {
			if _, exists := names[p.Team.Name]; exists {
				return errors.New("team name already exists in this organization")
			}
		}
		entry := ClusterTeamEntry{
			ID:                idx,
			OrganizationID:    p.Team.OrganizationID,
			Name:              p.Team.Name,
			Description:       p.Team.Description,
			CreatedAtUnixNano: p.Team.CreatedAtUnixNano,
			UpdatedAtUnixNano: p.Team.UpdatedAtUnixNano,
			Enabled:           true,
			LSN:               uint64(idx),
		}
		if err := f.rm.ApplyCreateTeam(entry); err != nil {
			return err
		}
		names := f.teamsByOrgAndName[p.Team.OrganizationID]
		if names == nil {
			names = make(map[string]int64)
			f.teamsByOrgAndName[p.Team.OrganizationID] = names
		}
		names[p.Team.Name] = idx
		f.teams[idx] = struct{}{}
		return nil

	case ProposalCommandUpdateTeam:
		var p updateTeamPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		existing, err := f.rm.GetTeam(p.ID)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("team %d not found", p.ID)
		}
		entry := ClusterTeamEntry{
			ID:                p.ID,
			OrganizationID:    existing.OrganizationID,
			Name:              existing.Name,
			Description:       existing.Description,
			CreatedAtUnixNano: existing.CreatedAt.UnixNano(),
			UpdatedAtUnixNano: p.UpdatedAtUnixNano,
			Enabled:           existing.Enabled,
			LSN:               uint64(idx),
		}
		changed := map[string]bool{}
		for _, c := range p.ChangedFields {
			changed[c] = true
		}
		if changed["name"] {
			entry.Name = p.Name
		}
		if changed["description"] {
			entry.Description = p.Description
		}
		if changed["enabled"] {
			entry.Enabled = p.Enabled
		}
		return f.rm.ApplyUpdateTeam(entry)

	case ProposalCommandDeleteTeam:
		var p deleteTeamPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		delete(f.teams, p.ID)
		return f.rm.ApplyDeleteTeam(p.ID)

	case ProposalCommandCreateRole:
		var p createRolePayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if _, ok := f.teams[p.Role.TeamID]; !ok {
			return fmt.Errorf("team %d not found", p.Role.TeamID)
		}
		entry := ClusterRoleEntry{
			ID:                idx,
			TeamID:            p.Role.TeamID,
			DatabasePattern:   p.Role.DatabasePattern,
			Permissions:       p.Role.Permissions,
			CreatedAtUnixNano: p.Role.CreatedAtUnixNano,
			LSN:               uint64(idx),
		}
		if err := f.rm.ApplyCreateRole(entry); err != nil {
			return err
		}
		f.roles[idx] = struct{}{}
		return nil

	case ProposalCommandUpdateRole:
		var p updateRolePayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		existing, err := f.rm.GetRole(p.ID)
		if err != nil {
			return err
		}
		if existing == nil {
			return fmt.Errorf("role %d not found", p.ID)
		}
		entry := ClusterRoleEntry{
			ID:                p.ID,
			TeamID:            existing.TeamID,
			DatabasePattern:   existing.DatabasePattern,
			Permissions:       joinCSV(existing.Permissions),
			CreatedAtUnixNano: existing.CreatedAt.UnixNano(),
			LSN:               uint64(idx),
		}
		changed := map[string]bool{}
		for _, c := range p.ChangedFields {
			changed[c] = true
		}
		if changed["database_pattern"] {
			entry.DatabasePattern = p.DatabasePattern
		}
		if changed["permissions"] {
			entry.Permissions = p.Permissions
		}
		return f.rm.ApplyUpdateRole(entry)

	case ProposalCommandDeleteRole:
		var p deleteRolePayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		delete(f.roles, p.ID)
		return f.rm.ApplyDeleteRole(p.ID)

	case ProposalCommandCreateMeasurementPermission:
		var p createMeasurementPermissionPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if _, ok := f.roles[p.MeasurementPermission.RoleID]; !ok {
			return fmt.Errorf("role %d not found", p.MeasurementPermission.RoleID)
		}
		entry := ClusterMeasurementPermissionEntry{
			ID:                 idx,
			RoleID:             p.MeasurementPermission.RoleID,
			MeasurementPattern: p.MeasurementPermission.MeasurementPattern,
			Permissions:        p.MeasurementPermission.Permissions,
			CreatedAtUnixNano:  p.MeasurementPermission.CreatedAtUnixNano,
			LSN:                uint64(idx),
		}
		return f.rm.ApplyCreateMeasurementPermission(entry)

	case ProposalCommandDeleteMeasurementPermission:
		var p deleteMeasurementPermissionPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		return f.rm.ApplyDeleteMeasurementPermission(p.ID)

	case ProposalCommandAddTokenToTeam:
		var p addTokenToTeamPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if _, ok := f.teams[p.Membership.TeamID]; !ok {
			return fmt.Errorf("team %d not found", p.Membership.TeamID)
		}
		// UNIQUE(token_id, team_id) check.
		if pairs, ok := f.memberships[p.Membership.TokenID]; ok {
			if _, exists := pairs[p.Membership.TeamID]; exists {
				return errors.New("token is already a member of this team")
			}
		}
		entry := ClusterTokenMembershipEntry{
			ID:                idx,
			TokenID:           p.Membership.TokenID,
			TeamID:            p.Membership.TeamID,
			CreatedAtUnixNano: p.Membership.CreatedAtUnixNano,
			LSN:               uint64(idx),
		}
		if err := f.rm.ApplyAddTokenToTeam(entry); err != nil {
			return err
		}
		pairs := f.memberships[p.Membership.TokenID]
		if pairs == nil {
			pairs = make(map[int64]int64)
			f.memberships[p.Membership.TokenID] = pairs
		}
		pairs[p.Membership.TeamID] = idx
		return nil

	case ProposalCommandRemoveTokenFromTeam:
		var p removeTokenFromTeamPayloadWire
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		if pairs, ok := f.memberships[p.TokenID]; ok {
			delete(pairs, p.TeamID)
			if len(pairs) == 0 {
				delete(f.memberships, p.TokenID)
			}
		}
		return f.rm.ApplyRemoveTokenFromTeam(p.TokenID, p.TeamID)

	default:
		return fmt.Errorf("fakeRBACProposer: unknown command type %d", cmdType)
	}
}

// joinCSV joins a []string into a comma-separated string, mirroring the
// way RBACManager stores permissions in SQLite.
func joinCSV(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

// newRBACTestManager builds an RBACManager backed by a temp SQLite DB
// via an AuthManager so the schema migrations run. Returns the wired
// fake proposer for tests to drive.
func newRBACTestManager(t *testing.T) (*RBACManager, *fakeRBACProposer) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "rbac-test.db")
	am, err := NewAuthManager(dbPath, 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() { am.Close() })

	rm := NewRBACManager(&RBACManagerConfig{
		DB:     am.GetDB(),
		Logger: zerolog.Nop(),
	})
	t.Cleanup(func() { rm.Close() })

	prop := newFakeRBACProposer()
	prop.setRBACManager(rm)
	rm.SetRaftProposer(prop)
	return rm, prop
}

// -----------------------------------------------------------------------------
// Round-trip tests.
// -----------------------------------------------------------------------------

func TestProposer_CreateOrganization_RoundTrip(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme", Description: "test"})
	if err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}
	if org.ID == 0 {
		t.Errorf("expected non-zero ID stamped from fake logIdx")
	}
	if org.Name != "acme" {
		t.Errorf("Name mismatch: %q", org.Name)
	}
	if !org.Enabled {
		t.Errorf("Enabled should default true")
	}
	// SQLite should hold the row.
	got, err := rm.GetOrganization(org.ID)
	if err != nil {
		t.Fatalf("GetOrganization: %v", err)
	}
	if got == nil || got.Name != "acme" {
		t.Errorf("SQLite materialise failed")
	}
}

func TestProposer_CreateOrganization_DuplicateName(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	if _, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
	// Surface the production user-facing string.
	if got := err.Error(); !contains(got, "already exists") {
		t.Errorf("error string mismatch: %q", got)
	}
}

func TestProposer_UpdateOrganization_RoundTrip(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	newName := "acme-renamed"
	if err := rm.UpdateOrganization(ctx, org.ID, &UpdateOrganizationRequest{Name: &newName}); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ := rm.GetOrganization(org.ID)
	if got == nil || got.Name != newName {
		t.Errorf("rename did not materialise, got=%v", got)
	}
}

func TestProposer_DeleteOrganization_CascadesInSQLite(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	team, _ := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})
	role, _ := rm.CreateRole(ctx, team.ID, &CreateRoleRequest{
		DatabasePattern: "production",
		Permissions:     []string{"read", "write"},
	})

	if err := rm.DeleteOrganization(ctx, org.ID); err != nil {
		t.Fatalf("delete org: %v", err)
	}

	// SQLite cascade: org gone, team gone, role gone.
	if got, _ := rm.GetOrganization(org.ID); got != nil {
		t.Errorf("organization should be deleted")
	}
	if got, _ := rm.GetTeam(team.ID); got != nil {
		t.Errorf("team should be cascade-deleted in SQLite via FK")
	}
	if got, _ := rm.GetRole(role.ID); got != nil {
		t.Errorf("role should be cascade-deleted in SQLite via FK")
	}
}

// TestProposer_DeleteNonExistent_ReturnsNotFound pins Gemini PR #458
// round 8 G27: cluster-mode Delete of a non-existent entity must
// return "not found" (not nil/200 OK) so the cluster path matches
// the OSS path's behaviour. Covers all 5 Delete entry points
// (org, team, role, mperm, membership).
func TestProposer_DeleteNonExistent_ReturnsNotFound(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()

	if err := rm.DeleteOrganization(ctx, 99999); err == nil ||
		!strings.Contains(err.Error(), "organization not found") {
		t.Errorf("DeleteOrganization(non-existent) should return 'organization not found', got: %v", err)
	}
	if err := rm.DeleteTeam(ctx, 99999); err == nil ||
		!strings.Contains(err.Error(), "team not found") {
		t.Errorf("DeleteTeam(non-existent) should return 'team not found', got: %v", err)
	}
	if err := rm.DeleteRole(ctx, 99999); err == nil ||
		!strings.Contains(err.Error(), "role not found") {
		t.Errorf("DeleteRole(non-existent) should return 'role not found', got: %v", err)
	}
	if err := rm.DeleteMeasurementPermission(ctx, 99999); err == nil ||
		!strings.Contains(err.Error(), "measurement permission not found") {
		t.Errorf("DeleteMeasurementPermission(non-existent) should return 'measurement permission not found', got: %v", err)
	}
	if err := rm.RemoveTokenFromTeam(ctx, 99999, 99999); err == nil ||
		!strings.Contains(err.Error(), "token membership not found") {
		t.Errorf("RemoveTokenFromTeam(non-existent) should return 'token membership not found', got: %v", err)
	}
}

func TestProposer_CreateTeam_DuplicateInSameOrg(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	if _, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"}); err != nil {
		t.Fatalf("first team: %v", err)
	}
	_, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
}

func TestProposer_CreateTeam_AllowedAcrossOrgs(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	orgA, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	orgB, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "globex"})
	if _, err := rm.CreateTeam(ctx, orgA.ID, &CreateTeamRequest{Name: "platform"}); err != nil {
		t.Fatalf("teamA: %v", err)
	}
	if _, err := rm.CreateTeam(ctx, orgB.ID, &CreateTeamRequest{Name: "platform"}); err != nil {
		t.Fatalf("teamB: %v", err)
	}
}

func TestProposer_CreateRole_RoundTrip(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	team, _ := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})
	role, err := rm.CreateRole(ctx, team.ID, &CreateRoleRequest{
		DatabasePattern: "production",
		Permissions:     []string{"read", "write"},
	})
	if err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	got, err := rm.GetRole(role.ID)
	if err != nil {
		t.Fatalf("GetRole: %v", err)
	}
	if got == nil || got.DatabasePattern != "production" {
		t.Errorf("role not materialised")
	}
	if len(got.Permissions) != 2 {
		t.Errorf("permissions mismatch: %v", got.Permissions)
	}
}

func TestProposer_CreateRole_RejectsUnknownTeam(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	_, err := rm.CreateRole(ctx, 9999, &CreateRoleRequest{
		DatabasePattern: "production",
		Permissions:     []string{"read"},
	})
	if err == nil {
		t.Fatal("expected error for unknown team")
	}
}

func TestProposer_CreateMeasurementPermission_RoundTrip(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	team, _ := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})
	role, _ := rm.CreateRole(ctx, team.ID, &CreateRoleRequest{
		DatabasePattern: "production",
		Permissions:     []string{"read", "write"},
	})
	mp, err := rm.CreateMeasurementPermission(ctx, role.ID, &CreateMeasurementPermissionRequest{
		MeasurementPattern: "metrics_*",
		Permissions:        []string{"read"},
	})
	if err != nil {
		t.Fatalf("CreateMeasurementPermission: %v", err)
	}
	if mp.MeasurementPattern != "metrics_*" {
		t.Errorf("MeasurementPattern mismatch: %q", mp.MeasurementPattern)
	}
}

func TestProposer_AddTokenToTeam_RoundTrip(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	team, _ := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})

	// Seed a real api_tokens row so the FK is satisfied in SQLite.
	tokenID := seedTokenRow(t, rm)

	mem, err := rm.AddTokenToTeam(ctx, tokenID, team.ID)
	if err != nil {
		t.Fatalf("AddTokenToTeam: %v", err)
	}
	if mem.TokenID != tokenID || mem.TeamID != team.ID {
		t.Errorf("membership fields mismatch: %+v", mem)
	}
}

func TestProposer_AddTokenToTeam_DuplicatePair(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	team, _ := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})
	tokenID := seedTokenRow(t, rm)
	if _, err := rm.AddTokenToTeam(ctx, tokenID, team.ID); err != nil {
		t.Fatalf("first AddTokenToTeam: %v", err)
	}
	_, err := rm.AddTokenToTeam(ctx, tokenID, team.ID)
	if err == nil {
		t.Fatal("expected duplicate-membership error")
	}
}

func TestProposer_RemoveTokenFromTeam_RoundTrip(t *testing.T) {
	rm, _ := newRBACTestManager(t)
	ctx := context.Background()
	org, _ := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme"})
	team, _ := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "platform"})
	tokenID := seedTokenRow(t, rm)
	if _, err := rm.AddTokenToTeam(ctx, tokenID, team.ID); err != nil {
		t.Fatalf("AddTokenToTeam: %v", err)
	}
	if err := rm.RemoveTokenFromTeam(ctx, tokenID, team.ID); err != nil {
		t.Fatalf("RemoveTokenFromTeam: %v", err)
	}
	// Subsequent add should now succeed (pair is gone).
	if _, err := rm.AddTokenToTeam(ctx, tokenID, team.ID); err != nil {
		t.Fatalf("re-add after remove: %v", err)
	}
}

func TestProposer_NilProposer_FallsThroughToDirectSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rbac-nil-proposer.db")
	am, err := NewAuthManager(dbPath, 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()
	rm := NewRBACManager(&RBACManagerConfig{
		DB:     am.GetDB(),
		Logger: zerolog.Nop(),
	})
	defer rm.Close()
	// No SetRaftProposer — OSS path.
	ctx := context.Background()
	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "acme-oss"})
	if err != nil {
		t.Fatalf("CreateOrganization OSS path: %v", err)
	}
	if org.Name != "acme-oss" || org.ID == 0 {
		t.Errorf("OSS-path org row malformed: %+v", org)
	}
}

// -----------------------------------------------------------------------------
// Helpers.
// -----------------------------------------------------------------------------

// seedTokenRow inserts an api_tokens row directly so RBAC membership
// inserts can satisfy the FK. We don't go through CreateToken here
// because that would route through the (token) proposer which is not
// wired in these RBAC-only tests.
func seedTokenRow(t *testing.T, rm *RBACManager) int64 {
	t.Helper()
	result, err := rm.db.Exec(`
		INSERT INTO api_tokens (name, token_hash, token_prefix, description, permissions, enabled)
		VALUES (?, ?, ?, '', 'read,write', 1)
	`, "test-token", "fakehash", "fakeprefix")
	if err != nil {
		t.Fatalf("seedTokenRow: %v", err)
	}
	id, _ := result.LastInsertId()
	return id
}

// -----------------------------------------------------------------------------
// readBackAfterPropose regression tests (Gemini PR #458 rounds 2/4
// introduced the helper + the ctx-cancel arm; internal review round 2
// flagged that neither is pinned). Use a deferred-apply proposer so the
// FSM apply completes AFTER Propose returns — simulating the
// follower-apply lag readBackAfterPropose is supposed to absorb.
// -----------------------------------------------------------------------------

// deferredApplyProposer simulates the follower behaviour where Propose
// returns when the leader has committed the entry but the local apply
// has not yet fired. After `applyDelay`, it kicks the apply on a
// goroutine. Only handles CreateOrganization — that's enough surface
// to pin the retry helper.
type deferredApplyProposer struct {
	rm         *RBACManager
	logIdx     atomic.Int64
	applyDelay time.Duration
}

func (f *deferredApplyProposer) IsLeader() bool { return true }

func (f *deferredApplyProposer) Propose(ctx context.Context, cmdType uint8, payload []byte, timeout time.Duration) error {
	if cmdType != ProposalCommandCreateOrganization {
		return fmt.Errorf("deferredApplyProposer: only supports CreateOrganization, got %d", cmdType)
	}
	var p createOrganizationPayloadWire
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	idx := f.logIdx.Add(1)
	entry := ClusterOrganizationEntry{
		ID:                idx,
		Name:              p.Organization.Name,
		Description:       p.Organization.Description,
		CreatedAtUnixNano: p.Organization.CreatedAtUnixNano,
		UpdatedAtUnixNano: p.Organization.UpdatedAtUnixNano,
		Enabled:           true,
		LSN:               uint64(idx),
	}
	// Schedule the apply to fire AFTER Propose returns. This is the
	// production follower scenario: the leader committed, but our local
	// runFSM goroutine hasn't picked it up yet.
	go func() {
		time.Sleep(f.applyDelay)
		_ = f.rm.ApplyCreateOrganization(entry)
	}()
	return nil
}

// TestReadBackAfterPropose_RetriesOnFollowerApplyLag pins the round-2
// G3-G7 read-back retry: Propose returns before the apply lands, the
// first scan hits sql.ErrNoRows, the helper retries within its backoff
// window, and the second scan succeeds.
func TestReadBackAfterPropose_RetriesOnFollowerApplyLag(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rbac-followerlag.db")
	am, err := NewAuthManager(dbPath, 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()
	rm := NewRBACManager(&RBACManagerConfig{DB: am.GetDB(), Logger: zerolog.Nop()})
	defer rm.Close()

	// Delay the apply by 40ms — past the first three retry intervals
	// (0, 10, 25 = 35ms cumulative) and into the 50ms retry. The helper
	// should hit ErrNoRows on attempts 1-3 then succeed on attempt 4.
	prop := &deferredApplyProposer{rm: rm, applyDelay: 40 * time.Millisecond}
	rm.SetRaftProposer(prop)

	org, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "lag-org"})
	if err != nil {
		t.Fatalf("CreateOrganization with follower-lag: %v", err)
	}
	if org.Name != "lag-org" {
		t.Errorf("expected name 'lag-org' after retry, got %q", org.Name)
	}
	if org.ID == 0 {
		t.Errorf("expected non-zero ID after retry-succeeded read-back")
	}
}

// TestReadBackAfterPropose_HonoursCtxCancel pins the round-4 G14
// ctx-cancel arm: a ctx cancelled mid-retry causes the helper to bail
// with ctx.Err() rather than waiting through the full backoff window.
func TestReadBackAfterPropose_HonoursCtxCancel(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rbac-ctxcancel.db")
	am, err := NewAuthManager(dbPath, 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()
	rm := NewRBACManager(&RBACManagerConfig{DB: am.GetDB(), Logger: zerolog.Nop()})
	defer rm.Close()

	// Use a long delay so the apply never lands during the test window;
	// the helper should ride the backoff ladder and we cancel ctx
	// before it finishes. Total readBackAfterPropose budget is ~785ms;
	// we cancel at ~30ms which is during the 50ms wait.
	prop := &deferredApplyProposer{rm: rm, applyDelay: 5 * time.Second}
	rm.SetRaftProposer(prop)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel from a goroutine so the cancel races the retry loop.
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err = rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "cancelled-org"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected ctx-cancel error, got nil (the apply never landed in 5s, retry should have given up earlier)")
	}
	// We expect the error to be the read-back error wrapping context.Canceled.
	if !strings.Contains(err.Error(), "context canceled") && !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in error chain, got %v", err)
	}
	// Sanity: should have returned well before the full 785ms budget.
	// Allow a generous 300ms ceiling to absorb test-runner jitter and
	// the time.Sleep(40) the cancel goroutine introduces.
	if elapsed > 300*time.Millisecond {
		t.Errorf("readBackAfterPropose ignored ctx.Done: took %v (expected ≤300ms)", elapsed)
	}
}

// TestReadBackAfterPropose_PreCancelledCtxNoScans pins Gemini PR #458
// round 6 G22: a ctx that is ALREADY cancelled before
// readBackAfterPropose is called must not run even one Scan attempt.
// Pre-round-6 the first iteration had d==0 and skipped the select arm
// entirely, so one wasted Scan would still run.
//
// We can't directly observe "Scan didn't run" through the helper's
// public surface, but we can prove it via a side-channel: the
// deferredApplyProposer schedules the apply 100ms in the future, so
// if any Scan runs, it'll see sql.ErrNoRows AND the helper's
// total runtime will include at least one Scan latency (~sub-ms).
// More importantly, the helper must return immediately with
// context.Canceled, not bail after the first failed Scan with
// sql.ErrNoRows wrapped. Returning ctx.Err() proves the
// top-of-loop check ran BEFORE the scan.
func TestReadBackAfterPropose_PreCancelledCtxNoScans(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "rbac-precancel.db")
	am, err := NewAuthManager(dbPath, 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()
	rm := NewRBACManager(&RBACManagerConfig{DB: am.GetDB(), Logger: zerolog.Nop()})
	defer rm.Close()

	prop := &deferredApplyProposer{rm: rm, applyDelay: 100 * time.Millisecond}
	rm.SetRaftProposer(prop)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already-cancelled BEFORE the call

	start := time.Now()
	_, err = rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "precancel-org"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error on pre-cancelled ctx, got nil")
	}
	// The error chain must include context.Canceled — proves the
	// top-of-loop ctx.Err() check fired before any Scan.
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context.Canceled in error chain, got: %v", err)
	}
	// Pre-fix: would have done one Scan (~sub-ms) + returned
	// sql.ErrNoRows wrapped. Post-fix: helper returns immediately
	// without scanning. Either way the test runs very fast; the
	// observable signal is the error type, not the elapsed time.
	// But assert under 50ms anyway to catch any future regression
	// that adds an unexpected wait.
	if elapsed > 50*time.Millisecond {
		t.Errorf("pre-cancelled ctx should bail immediately, took %v", elapsed)
	}
}
