package auth

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1336: a row is the cluster's only when it carries raft_materialised = 1.

func openAuthAt(t *testing.T, dbPath string) *AuthManager {
	t.Helper()
	am, err := NewAuthManager(dbPath, time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	return am
}

func tokenMarked(t *testing.T, am *AuthManager, id int64) bool {
	t.Helper()
	var marked int
	if err := am.db.QueryRow(`SELECT raft_materialised FROM api_tokens WHERE id = ?`, id).Scan(&marked); err != nil {
		t.Fatalf("read marker of %d: %v", id, err)
	}
	return marked == 1
}

func localTokenID(t *testing.T, am *AuthManager, name string) int64 {
	t.Helper()
	if _, err := am.CreateToken(context.Background(), name, "", "read,write,delete,admin", nil); err != nil {
		t.Fatalf("CreateToken %s: %v", name, err)
	}
	var id int64
	if err := am.db.QueryRow(`SELECT id FROM api_tokens WHERE name = ?`, name).Scan(&id); err != nil {
		t.Fatalf("id of %s: %v", name, err)
	}
	return id
}

func clusterEntry(id int64, name, hash string) ClusterTokenEntry {
	return ClusterTokenEntry{
		ID: id, Name: name, Permissions: "read", TokenHash: hash, TokenPrefix: "p-" + name,
		CreatedAtUnixNano: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC).UnixNano(), Enabled: true,
	}
}

// The boot backfill marks exactly the rows a cluster apply wrote: their
// created_at carries go-sqlite3's UTC offset, a local row's CURRENT_TIMESTAMP
// never does.
func TestBackfillMarksOnlyClusterWrittenTokens(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "auth.db")
	am := openAuthAt(t, dbPath)
	localID := localTokenID(t, am, "local")
	if err := am.ApplyCreateToken(clusterEntry(5000, "replicated", "h-replicated")); err != nil {
		t.Fatalf("ApplyCreateToken: %v", err)
	}
	// Both rows as a pre-#1336 binary left them.
	if _, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 0`); err != nil {
		t.Fatal(err)
	}
	am.Close()

	am = openAuthAt(t, dbPath)
	defer am.Close()
	if !tokenMarked(t, am, 5000) {
		t.Error("the cluster-written row was not marked by the backfill")
	}
	if tokenMarked(t, am, localID) {
		t.Error("a local CURRENT_TIMESTAMP row was marked as the cluster's")
	}
}

// An id-keyed cluster apply must not change a local row that only shares the
// id: a rotate would hand the cluster token's new secret the local row's name
// and admin permissions.
func TestIDKeyedApplyLeavesALocalRowAlone(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	id := localTokenID(t, am, "legacy")
	var hashBefore string
	if err := am.db.QueryRow(`SELECT token_hash FROM api_tokens WHERE id = ?`, id).Scan(&hashBefore); err != nil {
		t.Fatal(err)
	}

	if err := am.ApplyRotateToken(id, "h-cluster", "p-cluster"); err == nil {
		t.Error("ApplyRotateToken on a local row: want the divergence error")
	}
	if err := am.ApplyRevokeToken(id); err == nil {
		t.Error("ApplyRevokeToken on a local row: want the divergence error")
	}
	upd := clusterEntry(id, "renamed", "")
	if err := am.ApplyUpdateToken(upd); err == nil {
		t.Error("ApplyUpdateToken on a local row: want the divergence error")
	}
	if err := am.ApplyDeleteToken(id); err != nil {
		t.Errorf("ApplyDeleteToken: %v", err)
	}

	var name, hash string
	var enabled int
	if err := am.db.QueryRow(`SELECT name, token_hash, enabled FROM api_tokens WHERE id = ?`, id).Scan(&name, &hash, &enabled); err != nil {
		t.Fatalf("the local row is gone: %v", err)
	}
	if name != "legacy" || hash != hashBefore || enabled != 1 {
		t.Errorf("local row changed: name=%q hash changed=%v enabled=%d", name, hash != hashBefore, enabled)
	}
}

// A create replayed onto an identical row adopts it, so later applies reach it.
func TestCreateApplyAdoptsAnIdenticalRow(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	e := clusterEntry(7000, "same", "h-same")
	if err := am.ApplyCreateToken(e); err != nil {
		t.Fatal(err)
	}
	if _, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 0 WHERE id = 7000`); err != nil {
		t.Fatal(err)
	}
	if err := am.ApplyCreateToken(e); err != nil {
		t.Fatalf("replayed create: %v", err)
	}
	if !tokenMarked(t, am, 7000) {
		t.Fatal("replayed create did not adopt the identical row")
	}
	if err := am.ApplyRevokeToken(7000); err != nil {
		t.Fatalf("revoke after adoption: %v", err)
	}
}

// RBAC: once the node has reconciled, an org it created locally is not the
// cluster's to delete.
func TestRBACIDKeyedApplyLeavesALocalRowAlone(t *testing.T) {
	rm, _ := newRBACTestManagerOSS(t)
	org, err := rm.CreateOrganization(context.Background(), &CreateOrganizationRequest{Name: "local-org"})
	if err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}
	if _, err := rm.db.Exec(`INSERT INTO cluster_auth_reconcile_state (id, raft_term, applied_index) VALUES (1, 1, 10)`); err != nil {
		t.Fatal(err)
	}
	if err := rm.ApplyDeleteOrganization(org.ID); err != nil {
		t.Fatalf("ApplyDeleteOrganization: %v", err)
	}
	if err := rm.ApplyUpdateOrganization(ClusterOrganizationEntry{ID: org.ID, Name: "renamed", Enabled: true}); err == nil {
		t.Error("ApplyUpdateOrganization on a local row: want the divergence error")
	}
	var name string
	if err := rm.db.QueryRow(`SELECT name FROM rbac_organizations WHERE id = ?`, org.ID).Scan(&name); err != nil {
		t.Fatalf("local org is gone: %v", err)
	}
	if name != "local-org" {
		t.Errorf("local org renamed to %q", name)
	}
}

// Before a node's first reconcile its RBAC rows from before the marker are all
// unmarked; cluster removes, deletes and renames must still reach them, as
// they did before #1336, and the first reconcile must then adopt them.
func TestRBACAppliesBeforeTheFirstReconcileStillLand(t *testing.T) {
	rm, am := newRBACTestManagerOSS(t)
	mustApplyCreate(t, am, clusterEntry(100, "tok", "h"))
	org := ClusterOrganizationEntry{ID: 200, Name: "o", Enabled: true, CreatedAtUnixNano: 11, UpdatedAtUnixNano: 11}
	team := ClusterTeamEntry{ID: 201, OrganizationID: 200, Name: "t1", Enabled: true, CreatedAtUnixNano: 12, UpdatedAtUnixNano: 12}
	role := ClusterRoleEntry{ID: 202, TeamID: 201, DatabasePattern: "db*", Permissions: "read", CreatedAtUnixNano: 13}
	member := ClusterTokenMembershipEntry{ID: 203, TokenID: 100, TeamID: 201, CreatedAtUnixNano: 14}
	for _, err := range []error{rm.ApplyCreateOrganization(org), rm.ApplyCreateTeam(team), rm.ApplyCreateRole(role), rm.ApplyAddTokenToTeam(member)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, tbl := range []string{"rbac_organizations", "rbac_teams", "rbac_roles", "rbac_token_memberships"} {
		if _, err := am.db.Exec(`UPDATE ` + tbl + ` SET raft_materialised = 0`); err != nil {
			t.Fatal(err)
		}
	}

	if err := rm.ApplyRemoveTokenFromTeam(100, 201); err != nil {
		t.Fatal(err)
	}
	if err := rm.ApplyDeleteRole(202); err != nil {
		t.Fatal(err)
	}
	team.Name = "t2"
	if err := rm.ApplyUpdateTeam(team); err != nil {
		t.Fatalf("rename before the first reconcile: %v", err)
	}
	var n int
	am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships`).Scan(&n)
	if n != 0 {
		t.Error("a membership removed before the first reconcile still grants access")
	}
	am.db.QueryRow(`SELECT COUNT(*) FROM rbac_roles`).Scan(&n)
	if n != 0 {
		t.Error("a role deleted before the first reconcile is still present")
	}

	rep, err := ReconcileClusterState(context.Background(), am.db, ClusterAuthState{
		Tokens:        []ClusterTokenEntry{clusterEntry(100, "tok", "h")},
		Organizations: []ClusterOrganizationEntry{org},
		Teams:         []ClusterTeamEntry{team},
		IncludeRBAC:   true, AppliedIndex: 1000, Term: 1,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{TableOrganizations, TableTeams} {
		if tr := rep.Tables[tbl]; tr == nil || tr.Adopted != 1 || tr.Collisions != 0 || tr.Unreplicated != 0 {
			t.Errorf("%s: %+v, want the pre-marker row adopted", tbl, tr)
		}
	}
}

// A team the cluster renamed before this node had the marker is still that
// team: it shares the creation instant every cluster apply wrote.
func TestReconcileAdoptsAnRBACRowRenamedBeforeTheMarker(t *testing.T) {
	rm, am := newRBACTestManagerOSS(t)
	org := ClusterOrganizationEntry{ID: 200, Name: "o", Enabled: true, CreatedAtUnixNano: 11, UpdatedAtUnixNano: 11}
	team := ClusterTeamEntry{ID: 201, OrganizationID: 200, Name: "old", Enabled: true, CreatedAtUnixNano: 12, UpdatedAtUnixNano: 12}
	if err := rm.ApplyCreateOrganization(org); err != nil {
		t.Fatal(err)
	}
	if err := rm.ApplyCreateTeam(team); err != nil {
		t.Fatal(err)
	}
	am.db.Exec(`UPDATE rbac_teams SET raft_materialised = 0`)
	team.Name = "new"
	if _, err := ReconcileClusterState(context.Background(), am.db, ClusterAuthState{
		Organizations: []ClusterOrganizationEntry{org}, Teams: []ClusterTeamEntry{team},
		IncludeRBAC: true, AppliedIndex: 1000, Term: 1,
	}, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}
	var name string
	var marked int
	if err := am.db.QueryRow(`SELECT name, raft_materialised FROM rbac_teams WHERE id = 201`).Scan(&name, &marked); err != nil {
		t.Fatal(err)
	}
	if name != "new" || marked != 1 {
		t.Errorf("team = %q marked=%d, want adopted and renamed", name, marked)
	}
}
