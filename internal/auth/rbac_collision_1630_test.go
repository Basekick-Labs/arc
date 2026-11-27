package auth

import (
	"context"
	"testing"
)

// #1630 PR 2: on a cluster node, a token this node keeps at ID X (created
// before v26.06.1, unmarked) and the cluster's token with the same ID X are
// different tokens, but memberships are keyed by token_id alone. The cluster
// token's memberships, materialised here with raft_materialised = 1, must not
// grant the node's own token anything; and a cluster removal for the pair
// must not delete the node token's own grant.

// rbacCollisionFixture: a node-local token "legacy" with WIDE coarse
// permissions (read,write) restricted to read on db "own" by its own
// (unmarked) membership, and a marked membership at the same
// token ID in a team with write on db "secret", as the cluster token's grant
// lands on this node.
func rbacCollisionFixture(t *testing.T) (*RBACManager, *AuthManager, int64, int64, int64, *TokenInfo) {
	t.Helper()
	rm, am, cleanup := setupTestRBACManager(t)
	t.Cleanup(cleanup)
	ctx := context.Background()

	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "org"})
	if err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}
	own, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "own"})
	if err != nil {
		t.Fatalf("CreateTeam own: %v", err)
	}
	clusterTeam, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "cluster-team"})
	if err != nil {
		t.Fatalf("CreateTeam cluster: %v", err)
	}
	if _, err := rm.CreateRole(ctx, own.ID, &CreateRoleRequest{DatabasePattern: "own", Permissions: []string{"read"}}); err != nil {
		t.Fatalf("CreateRole own: %v", err)
	}
	if _, err := rm.CreateRole(ctx, clusterTeam.ID, &CreateRoleRequest{DatabasePattern: "secret", Permissions: []string{"read", "write"}}); err != nil {
		t.Fatalf("CreateRole cluster: %v", err)
	}

	secret, err := am.CreateToken(ctx, "legacy", "", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	info := am.VerifyToken(secret)
	if info == nil {
		t.Fatal("legacy token does not authenticate")
	}
	if markerOf(t, am, info.ID) != 0 {
		t.Fatal("fixture: the legacy token row is marked")
	}
	if _, err := rm.AddTokenToTeam(ctx, info.ID, own.ID); err != nil {
		t.Fatalf("AddTokenToTeam own: %v", err)
	}
	// The cluster token with the same ID was added to cluster-team; the apply
	// wrote the membership here keyed by token_id only.
	if _, err := am.db.Exec(`INSERT INTO rbac_token_memberships (token_id, team_id, raft_materialised) VALUES (?, ?, 1)`, info.ID, clusterTeam.ID); err != nil {
		t.Fatal(err)
	}
	rm.InvalidateAllCache()
	return rm, am, info.ID, own.ID, clusterTeam.ID, info
}

func TestNodeLocalTokenDoesNotInheritClusterTokenTeams(t *testing.T) {
	rm, _, id, ownTeam, _, info := rbacCollisionFixture(t)

	teams, err := rm.GetTokenTeams(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(teams) != 1 || teams[0].ID != ownTeam {
		t.Errorf("GetTokenTeams = %+v, want only the token's own team", teams)
	}

	if r := rm.CheckPermission(&PermissionCheckRequest{TokenInfo: info, Database: "secret", Measurement: "m", Permission: "write"}); r.Allowed {
		t.Errorf("the node-local token can write db secret through the cluster token's team: %+v", r)
	}
	if r := rm.CheckPermission(&PermissionCheckRequest{TokenInfo: info, Database: "own", Measurement: "m", Permission: "read"}); !r.Allowed {
		t.Errorf("the token lost its own grant: %+v", r)
	}
	// Its memberships govern it: the wide coarse permissions do not apply.
	if r := rm.CheckPermission(&PermissionCheckRequest{TokenInfo: info, Database: "unrelated", Measurement: "m", Permission: "read"}); r.Allowed {
		t.Errorf("the token fell back to its coarse permissions: %+v", r)
	}
}

// The cluster token's own rows, both marked, keep working.
func TestClusterTokenKeepsItsMarkedMemberships(t *testing.T) {
	rm, am, id, _, clusterTeam, info := rbacCollisionFixture(t)
	if _, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 1 WHERE id = ?`, id); err != nil {
		t.Fatal(err)
	}
	rm.InvalidateAllCache()
	teams, err := rm.GetTokenTeams(id)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tm := range teams {
		found = found || tm.ID == clusterTeam
	}
	if !found {
		t.Errorf("a marked token lost its marked membership: %+v", teams)
	}
	if r := rm.CheckPermission(&PermissionCheckRequest{TokenInfo: info, Database: "secret", Measurement: "m", Permission: "write"}); !r.Allowed {
		t.Errorf("the cluster token lost its grant: %+v", r)
	}
}

// A cluster removal of the pair must not delete the node token's own,
// unmarked grant once this node has reconciled; before its first reconcile
// (rows written before the marker existed are still unmarked) it acts as
// before.
func TestApplyRemoveTokenFromTeamLeavesTheNodeTokensGrant(t *testing.T) {
	t.Run("after the first reconcile", func(t *testing.T) {
		rm, am, id, ownTeam, _, _ := rbacCollisionFixture(t)
		if _, err := am.db.Exec(`INSERT INTO cluster_auth_reconcile_state (id, raft_term, applied_index) VALUES (1, 1, 100)`); err != nil {
			t.Fatal(err)
		}
		if err := rm.ApplyRemoveTokenFromTeam(id, ownTeam); err != nil {
			t.Fatal(err)
		}
		var n int
		_ = am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?`, id, ownTeam).Scan(&n)
		if n != 1 {
			t.Error("the cluster removal deleted the node-local token's own membership")
		}
	})
	t.Run("before the first reconcile", func(t *testing.T) {
		rm, am, id, ownTeam, _, _ := rbacCollisionFixture(t)
		if err := rm.ApplyRemoveTokenFromTeam(id, ownTeam); err != nil {
			t.Fatal(err)
		}
		var n int
		_ = am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?`, id, ownTeam).Scan(&n)
		if n != 0 {
			t.Error("before the first reconcile an unmarked row is still the cluster's to remove")
		}
	})
	t.Run("a marked membership is removed", func(t *testing.T) {
		rm, am, id, _, clusterTeam, _ := rbacCollisionFixture(t)
		if _, err := am.db.Exec(`INSERT INTO cluster_auth_reconcile_state (id, raft_term, applied_index) VALUES (1, 1, 100)`); err != nil {
			t.Fatal(err)
		}
		if err := rm.ApplyRemoveTokenFromTeam(id, clusterTeam); err != nil {
			t.Fatal(err)
		}
		var n int
		_ = am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?`, id, clusterTeam).Scan(&n)
		if n != 0 {
			t.Error("the cluster removal left the cluster membership")
		}
	})
}

// An unmarked membership is a grant made on this node (standalone, or while
// its cluster was down) to the row this node holds at that ID. It keeps
// counting when that row is the cluster's token: only the cluster's own
// memberships need a cluster token row.
func TestUnmarkedMembershipStillGrantsAClusterToken(t *testing.T) {
	rm, am, id, ownTeam, _, info := rbacCollisionFixture(t)
	if _, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 1 WHERE id = ?`, id); err != nil {
		t.Fatal(err)
	}
	rm.InvalidateAllCache()
	teams, err := rm.GetTokenTeams(id)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tm := range teams {
		found = found || tm.ID == ownTeam
	}
	if !found {
		t.Errorf("a marked token lost a grant made on this node: %+v", teams)
	}
	if r := rm.CheckPermission(&PermissionCheckRequest{TokenInfo: info, Database: "own", Measurement: "m", Permission: "read"}); !r.Allowed {
		t.Errorf("the grant made on this node no longer applies: %+v", r)
	}
}

// adoptionFixture: the cluster's org, team T (id 201) and role (read on db
// "own") applied on this node; a node-local token "legacy" at ID X with WIDE
// coarse permissions (read,write) and its own, unmarked membership of T. The
// cluster holds a different token at ID X, also in T.
func adoptionFixture(t *testing.T, legacyMembershipID int64) (*RBACManager, *AuthManager, int64, *TokenInfo, ClusterAuthState) {
	t.Helper()
	rm, am := newRBACTestManagerOSS(t)
	ctx := context.Background()
	org := ClusterOrganizationEntry{ID: 200, Name: "o1", Enabled: true, CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1}
	team := ClusterTeamEntry{ID: 201, OrganizationID: 200, Name: "t1", Enabled: true, CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1}
	role := ClusterRoleEntry{ID: 202, TeamID: 201, DatabasePattern: "own", Permissions: "read", CreatedAtUnixNano: 1}
	for _, err := range []error{rm.ApplyCreateOrganization(org), rm.ApplyCreateTeam(team), rm.ApplyCreateRole(role)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	secret, err := am.CreateToken(ctx, "legacy", "", "read,write", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(secret)
	if info == nil {
		t.Fatal("legacy token does not authenticate")
	}
	mustExec(t, am, `INSERT INTO rbac_token_memberships (id, token_id, team_id) VALUES (?, ?, 201)`, legacyMembershipID, info.ID)
	rm.InvalidateAllCache()
	st := ClusterAuthState{
		Tokens:        []ClusterTokenEntry{clusterEntry(info.ID, "svc", "h-svc")},
		Organizations: []ClusterOrganizationEntry{org},
		Teams:         []ClusterTeamEntry{team},
		Roles:         []ClusterRoleEntry{role},
		Memberships:   []ClusterTokenMembershipEntry{{ID: 300, TokenID: info.ID, TeamID: 201, CreatedAtUnixNano: 1}},
		IncludeRBAC:   true, AppliedIndex: 1000, Term: 1,
	}
	return rm, am, info.ID, info, st
}

// requireOnlyOwnGrant: the legacy token reads db "own" and nothing else, and
// its membership row is still its own (unmarked).
func requireOnlyOwnGrant(t *testing.T, rm *RBACManager, am *AuthManager, id int64, info *TokenInfo) {
	t.Helper()
	rm.InvalidateAllCache()
	var marked int
	if err := am.db.QueryRow(`SELECT raft_materialised FROM rbac_token_memberships WHERE token_id = ? AND team_id = 201`, id).Scan(&marked); err != nil {
		t.Fatalf("the legacy token's membership is gone: %v", err)
	}
	if marked != 0 {
		t.Error("the legacy token's own membership was marked as the cluster token's")
	}
	for _, c := range []struct {
		db, perm string
		want     bool
	}{{"own", "read", true}, {"own", "write", false}, {"unrelated", "read", false}, {"unrelated", "write", false}} {
		r := rm.CheckPermission(&PermissionCheckRequest{TokenInfo: info, Database: c.db, Measurement: "m", Permission: c.perm})
		if r.Allowed != c.want {
			t.Errorf("%s on %s: allowed=%v (%s), want %v", c.perm, c.db, r.Allowed, r.Source, c.want)
		}
	}
}

// The reconcile must not adopt the legacy token's own membership for the
// colliding cluster token (by key, and by id).
func TestReconcileLeavesTheLegacyTokensMembership(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int64
	}{{"by key", 7}, {"by id", 300}} {
		t.Run(tc.name, func(t *testing.T) {
			rm, am, id, info, st := adoptionFixture(t, tc.id)
			requireOnlyOwnGrant(t, rm, am, id, info)
			rep := reconcile(t, am, st)
			if rep.table(TableMemberships).Collisions != 1 {
				t.Errorf("membership collisions = %+v, want 1", rep.table(TableMemberships))
			}
			requireOnlyOwnGrant(t, rm, am, id, info)
			// After the reconcile, a cluster removal of the pair leaves it.
			if err := rm.ApplyRemoveTokenFromTeam(id, 201); err != nil {
				t.Fatal(err)
			}
			requireOnlyOwnGrant(t, rm, am, id, info)
		})
	}
}

// The live apply's replay path must not mark it either.
func TestApplyAddTokenToTeamLeavesTheLegacyTokensMembership(t *testing.T) {
	rm, am, id, info, _ := adoptionFixture(t, 300)
	// A deliberate refusal: no error (nothing failed), the row stays unmarked.
	if err := rm.ApplyAddTokenToTeam(ClusterTokenMembershipEntry{ID: 300, TokenID: id, TeamID: 201, CreatedAtUnixNano: 1}); err != nil {
		t.Errorf("the refusal returned an error: %v", err)
	}
	var marked int
	_ = am.db.QueryRow(`SELECT raft_materialised FROM rbac_token_memberships WHERE id = 300`).Scan(&marked)
	if marked != 0 {
		t.Error("the apply marked a membership of the token this node keeps at that ID")
	}
	requireOnlyOwnGrant(t, rm, am, id, info)
	// Another id for the same pair: refused by UNIQUE(token_id, team_id).
	if err := rm.ApplyAddTokenToTeam(ClusterTokenMembershipEntry{ID: 301, TokenID: id, TeamID: 201, CreatedAtUnixNano: 1}); err == nil {
		t.Error("the apply inserted a second membership for the pair")
	}
	requireOnlyOwnGrant(t, rm, am, id, info)
}

// A cluster token's own replayed membership is still marked.
func TestApplyAddTokenToTeamMarksAClusterTokensMembership(t *testing.T) {
	rm, am, id, _, _ := adoptionFixture(t, 300)
	mustExec(t, am, `UPDATE api_tokens SET raft_materialised = 1 WHERE id = ?`, id)
	if err := rm.ApplyAddTokenToTeam(ClusterTokenMembershipEntry{ID: 300, TokenID: id, TeamID: 201, CreatedAtUnixNano: 1}); err != nil {
		t.Fatal(err)
	}
	var marked int
	_ = am.db.QueryRow(`SELECT raft_materialised FROM rbac_token_memberships WHERE id = 300`).Scan(&marked)
	if marked != 1 {
		t.Error("a cluster token's replayed membership was not marked")
	}
}

// RBAC's per-token cache is dropped when this node writes the token's row:
// GetTokenTeams reads the row's marker.
func TestTokenRowWritesDropTheRBACTokenCache(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()
	am.SetTokenRowChangedHook(rm.InvalidateTokenCache)
	ctx := context.Background()
	cached := func(id int64) bool {
		rm.tokenCacheMu.RLock()
		defer rm.tokenCacheMu.RUnlock()
		_, ok := rm.tokenCache[id]
		return ok
	}
	prime := func(t *testing.T, id int64) {
		t.Helper()
		if _, err := rm.getTokenRBACData(id); err != nil || !cached(id) {
			t.Fatalf("prime cache for %d: %v", id, err)
		}
	}
	mk := func(t *testing.T, name string) int64 {
		t.Helper()
		if _, err := am.CreateToken(ctx, name, "", "read", nil); err != nil {
			t.Fatal(err)
		}
		return tokenIDByName(t, am, name)
	}

	for _, tc := range []struct {
		name string
		op   func(t *testing.T, id int64) error
	}{
		{"apply create (replay mark)", func(t *testing.T, id int64) error {
			var name, hash string
			_ = am.db.QueryRow(`SELECT name, token_hash FROM api_tokens WHERE id = ?`, id).Scan(&name, &hash)
			return am.ApplyCreateToken(ClusterTokenEntry{ID: id, Name: name, TokenHash: hash, Permissions: "read", CreatedAtUnixNano: 1, Enabled: true})
		}},
		{"apply revoke", func(t *testing.T, id int64) error { return am.ApplyRevokeToken(id) }},
		{"apply rotate", func(t *testing.T, id int64) error { return am.ApplyRotateToken(id, "h2", "p2") }},
		{"apply delete", func(t *testing.T, id int64) error { return am.ApplyDeleteToken(id) }},
		{"standalone revoke", func(t *testing.T, id int64) error { return am.RevokeToken(ctx, id) }},
		{"standalone delete", func(t *testing.T, id int64) error {
			_, err := am.DeleteTokenScoped(ctx, id, TokenWriteOptions{})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := mk(t, "tok-"+tc.name)
			prime(t, id)
			_ = tc.op(t, id)
			if cached(id) {
				t.Error("the RBAC cache still holds the token after a write of its row")
			}
		})
	}

	t.Run("node-local revoke and classify adoption", func(t *testing.T) {
		legacy := mk(t, "legacy-node")
		adopt := applyClusterTokenMarkerless(t, am, mk(t, "svc-unadopted"))
		prop := newViewProposer()
		var name, hash string
		_ = am.db.QueryRow(`SELECT name, token_hash FROM api_tokens WHERE id = ?`, adopt).Scan(&name, &hash)
		prop.tokens[adopt] = ClusterTokenEntry{ID: adopt, Name: name, TokenHash: hash, CreatedAtUnixNano: 1}
		am.SetRaftProposer(prop)
		defer am.SetRaftProposer(nil)

		prime(t, legacy)
		if _, err := am.RevokeTokenScoped(ctx, legacy, here); err != nil {
			t.Fatal(err)
		}
		if cached(legacy) {
			t.Error("node-local revoke left the RBAC cache")
		}
		prime(t, adopt)
		_, _ = am.RevokeTokenScoped(ctx, adopt, TokenWriteOptions{}) // classify adopts the row first
		if markerOf(t, am, adopt) != 1 {
			t.Fatal("fixture: the row was not adopted")
		}
		if cached(adopt) {
			t.Error("the adoption left the RBAC cache")
		}
	})
}

// applyClusterTokenMarkerless returns id: a row a cluster apply wrote before
// the marker existed (unmarked, but the FSM's entry).
func applyClusterTokenMarkerless(t *testing.T, am *AuthManager, id int64) int64 {
	t.Helper()
	mustExec(t, am, `UPDATE api_tokens SET raft_materialised = 0 WHERE id = ?`, id)
	return id
}
