package auth

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// #1336: ReconcileClusterState rewrites the local tables from a copy of the
// cluster FSM without ever touching a row the cluster does not own.

type tokenRow struct {
	name, hash, perms string
	enabled, marked   bool
}

func readToken(t *testing.T, am *AuthManager, id int64) (tokenRow, bool) {
	t.Helper()
	var r tokenRow
	var enabled, marked int
	err := am.db.QueryRow(`SELECT name, token_hash, permissions, enabled, raft_materialised FROM api_tokens WHERE id = ?`, id).
		Scan(&r.name, &r.hash, &r.perms, &enabled, &marked)
	if err != nil {
		return r, false
	}
	r.enabled, r.marked = enabled == 1, marked == 1
	return r, true
}

func mustApplyCreate(t *testing.T, am *AuthManager, e ClusterTokenEntry) {
	t.Helper()
	if err := am.ApplyCreateToken(e); err != nil {
		t.Fatalf("ApplyCreateToken %d: %v", e.ID, err)
	}
}

func mustExec(t *testing.T, am *AuthManager, q string, args ...any) {
	t.Helper()
	if _, err := am.db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func reconcile(t *testing.T, am *AuthManager, st ClusterAuthState) *ReconcileReport {
	t.Helper()
	rep, err := ReconcileClusterState(context.Background(), am.db, st, zerolog.Nop())
	if err != nil {
		t.Fatalf("ReconcileClusterState: %v", err)
	}
	return rep
}

func TestReconcileTokens(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	// Local rows first, as production made them: pre-26.06.1 AUTOINCREMENT
	// ids below the log indexes the cluster stamps later.
	legacyID := localTokenID(t, am, "legacy-e")
	adminID := localTokenID(t, am, "admin") // a pre-26.06.1 env bootstrap row

	a := clusterEntry(100, "a", "ha")
	b := clusterEntry(101, "b", "hb")
	d := clusterEntry(103, "d", "hd")
	h := clusterEntry(106, "h1", "hh")
	i := clusterEntry(107, "h2", "hi")
	k := clusterEntry(108, "k", "hk")
	for _, e := range []ClusterTokenEntry{a, b, d, h, i, k} {
		mustApplyCreate(t, am, e)
	}
	mustExec(t, am, `UPDATE api_tokens SET enabled = 0 WHERE id = 108`) // revoked here while standalone
	// F: a local row at an id the cluster later used for another token.
	mustExec(t, am, `INSERT INTO api_tokens (id, name, token_hash, token_prefix, permissions) VALUES (104, 'f-local', 'hf-local', 'pf', 'read')`)
	// G: a row a cluster apply wrote before the marker existed.
	mustExec(t, am, `INSERT INTO api_tokens (id, name, token_hash, token_prefix, permissions) VALUES (105, 'g', 'hg', 'p-g', 'read')`)

	a.Enabled = false                 // revoked while away
	b.TokenHash = "hb2"               // rotated while away
	b.TokenPrefix = "p-b2"            //
	c := clusterEntry(102, "c", "hc") // created while away
	f := clusterEntry(104, "f", "hf")
	g := clusterEntry(105, "g", "hg")
	g.Permissions = "read,write"
	h.Name, i.Name = "h2", "h1" // swapped names
	admin := clusterEntry(109, "admin", "h-admin")
	// d: deleted while away (absent).

	rep := reconcile(t, am, ClusterAuthState{
		Tokens:       []ClusterTokenEntry{a, b, c, f, g, h, i, k, admin},
		AppliedIndex: 1000, Term: 1,
	})

	if r, _ := readToken(t, am, 100); r.enabled {
		t.Error("A: a token revoked while away still authenticates")
	}
	if r, _ := readToken(t, am, 101); r.hash != "hb2" {
		t.Errorf("B: rotated token keeps hash %q", r.hash)
	}
	if r, ok := readToken(t, am, 102); !ok || !r.marked || !r.enabled {
		t.Errorf("C: token created while away = %+v present=%v", r, ok)
	}
	if _, ok := readToken(t, am, 103); ok {
		t.Error("D: a token deleted while away is still present")
	}
	if r, ok := readToken(t, am, legacyID); !ok || !r.enabled || r.marked {
		t.Errorf("E: legacy local token changed: %+v present=%v", r, ok)
	}
	if r, _ := readToken(t, am, 104); r.name != "f-local" || r.hash != "hf-local" || r.marked {
		t.Errorf("F: a local row sharing the id was taken over: %+v", r)
	}
	if r, _ := readToken(t, am, 105); !r.marked || r.perms != "read,write" {
		t.Errorf("G: pre-marker cluster row not adopted and updated: %+v", r)
	}
	if r, _ := readToken(t, am, 106); r.name != "h2" {
		t.Errorf("H: name %q, want h2", r.name)
	}
	if r, _ := readToken(t, am, 107); r.name != "h1" {
		t.Errorf("I: name %q, want h1", r.name)
	}
	if r, _ := readToken(t, am, 108); r.enabled {
		t.Error("K: a token revoked on this node was re-enabled by the cluster copy")
	}
	if r, ok := readToken(t, am, adminID); !ok || !r.enabled {
		t.Error("legacy admin row lost: the administrator would be locked out")
	}
	if _, ok := readToken(t, am, 109); ok {
		t.Error("cluster admin inserted alongside the legacy admin of the same name")
	}

	tr := rep.Tables[TableTokens]
	if tr.Deleted != 1 || tr.Inserted != 1 || tr.Adopted != 1 || tr.KeptDisabled != 1 || tr.Unreplicated != 2 {
		t.Errorf("report = %+v", *tr)
	}
	if tr.Collisions != 2 { // F and the cluster admin
		t.Errorf("collisions = %d, want 2", tr.Collisions)
	}
}

// Rows the FSM lacks but which must not be deleted.
func TestReconcileKeepsRowsItCannotAccountFor(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	for id := int64(10); id < 20; id++ {
		mustApplyCreate(t, am, clusterEntry(id, "t"+string(rune('a'+id-10)), "h"))
	}
	mustApplyCreate(t, am, clusterEntry(5000, "future", "h"))
	keep := []ClusterTokenEntry{}
	for id := int64(10); id < 19; id++ {
		keep = append(keep, clusterEntry(id, "t"+string(rune('a'+id-10)), "h"))
	}
	// 19 is quarantined, 5000 is above the applied index.
	rep := reconcile(t, am, ClusterAuthState{
		Tokens:       keep,
		Quarantined:  map[string]map[int64]bool{TableTokens: {19: true}},
		AppliedIndex: 1000, Term: 1,
	})
	for _, id := range []int64{19, 5000} {
		if _, ok := readToken(t, am, id); !ok {
			t.Errorf("token %d deleted", id)
		}
	}
	if got := rep.Tables[TableTokens].Protected; got != 2 {
		t.Errorf("protected = %d, want 2", got)
	}
}

func TestReconcileBreakerRefusesMassDelete(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	for id := int64(10); id < 14; id++ {
		mustApplyCreate(t, am, clusterEntry(id, "t"+string(rune('a'+id-10)), "h"))
	}
	rep := reconcile(t, am, ClusterAuthState{
		Tokens:       []ClusterTokenEntry{clusterEntry(10, "ta", "h")},
		AppliedIndex: 1000, Term: 1,
	})
	if len(rep.BreakerTripped) != 1 {
		t.Fatalf("breaker did not trip: %+v", rep)
	}
	for id := int64(11); id < 14; id++ {
		if _, ok := readToken(t, am, id); !ok {
			t.Errorf("token %d deleted despite the breaker", id)
		}
	}
}

// A Raft wiped and re-bootstrapped restarts its term and index; the node's
// cluster rows then come from a history the new FSM never had. Nothing is
// deleted, the rows become local, and the new cluster's admin collides by
// name instead of replacing the old one.
func TestReconcileOnARebuiltHistoryDeletesNothing(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	old := []ClusterTokenEntry{clusterEntry(400, "admin", "h-old"), clusterEntry(401, "ci", "h-ci")}
	for _, e := range old {
		mustApplyCreate(t, am, e)
	}
	reconcile(t, am, ClusterAuthState{Tokens: old, AppliedIndex: 900, Term: 4})

	rep := reconcile(t, am, ClusterAuthState{
		Tokens:       []ClusterTokenEntry{clusterEntry(3, "admin", "h-new")},
		AppliedIndex: 12, Term: 1,
	})
	if !rep.HistoryRebuilt {
		t.Fatal("rebuilt history not detected")
	}
	for _, id := range []int64{400, 401} {
		r, ok := readToken(t, am, id)
		if !ok || !r.enabled {
			t.Errorf("token %d lost on a rebuilt history", id)
		}
		if r.marked {
			t.Errorf("token %d still marked as the cluster's", id)
		}
	}
	// The new history is recorded: a later run does not see a regression.
	if rep := reconcile(t, am, ClusterAuthState{Tokens: []ClusterTokenEntry{clusterEntry(3, "admin", "h-new")}, AppliedIndex: 20, Term: 1}); rep.HistoryRebuilt {
		t.Error("history reported rebuilt again")
	}
}

// The first transaction (revokes) commits even when the second cannot begin.
func TestReconcileRevokesSurviveAFailedSecondTransaction(t *testing.T) {
	am := openAuthAt(t, filepath.Join(t.TempDir(), "auth.db"))
	defer am.Close()
	a := clusterEntry(100, "a", "ha")
	mustApplyCreate(t, am, a)
	a.Enabled = false

	ctx, cancel := context.WithCancel(context.Background())
	reconcileBetweenTransactions = cancel
	defer func() { reconcileBetweenTransactions = nil }()
	_, err := ReconcileClusterState(ctx, am.db, ClusterAuthState{
		Tokens:       []ClusterTokenEntry{a, clusterEntry(102, "c", "hc")},
		AppliedIndex: 1000, Term: 1,
	}, zerolog.Nop())
	if err == nil {
		t.Fatal("want the second transaction's error")
	}
	if r, _ := readToken(t, am, 100); r.enabled {
		t.Error("the revoke was rolled back with the failed second transaction")
	}
}

func TestReconcileRBAC(t *testing.T) {
	rm, am := newRBACTestManagerOSS(t)
	ctx := context.Background()

	mustApplyCreate(t, am, clusterEntry(100, "tok-a", "ha"))
	mustApplyCreate(t, am, clusterEntry(101, "tok-b", "hb"))
	org := ClusterOrganizationEntry{ID: 200, Name: "o1", Enabled: true, CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1}
	team := ClusterTeamEntry{ID: 201, OrganizationID: 200, Name: "t1", Enabled: true, CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1}
	role := ClusterRoleEntry{ID: 202, TeamID: 201, DatabasePattern: "db*", Permissions: "read,write", CreatedAtUnixNano: 1}
	perm := ClusterMeasurementPermissionEntry{ID: 203, RoleID: 202, MeasurementPattern: "m*", Permissions: "read", CreatedAtUnixNano: 1}
	memberB := ClusterTokenMembershipEntry{ID: 204, TokenID: 101, TeamID: 201, CreatedAtUnixNano: 1}
	for _, err := range []error{
		rm.ApplyCreateOrganization(org), rm.ApplyCreateTeam(team), rm.ApplyCreateRole(role),
		rm.ApplyCreateMeasurementPermission(perm), rm.ApplyAddTokenToTeam(memberB),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	// A pre-26.06.1 org of the name the cluster now holds at another id.
	legacy, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "shared-name"})
	if err != nil {
		t.Fatal(err)
	}
	// tok-a's membership as a pre-marker apply left it, at another id.
	mustExec(t, am, `INSERT INTO rbac_token_memberships (id, token_id, team_id) VALUES (7, 100, 201)`)

	role.Permissions = "read"                                                                        // narrowed while away
	memberA := ClusterTokenMembershipEntry{ID: 206, TokenID: 100, TeamID: 201, CreatedAtUnixNano: 1} // same pair as the local row 7
	shared := ClusterOrganizationEntry{ID: 205, Name: "shared-name", Enabled: true, CreatedAtUnixNano: 1, UpdatedAtUnixNano: 1}
	// perm deleted and tok-b removed from the team while away.

	rep, err := ReconcileClusterState(ctx, am.db, ClusterAuthState{
		Tokens:        []ClusterTokenEntry{clusterEntry(100, "tok-a", "ha"), clusterEntry(101, "tok-b", "hb")},
		Organizations: []ClusterOrganizationEntry{org, shared},
		Teams:         []ClusterTeamEntry{team},
		Roles:         []ClusterRoleEntry{role},
		Memberships:   []ClusterTokenMembershipEntry{memberA},
		IncludeRBAC:   true, AppliedIndex: 1000, Term: 1,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	var perms string
	if err := am.db.QueryRow(`SELECT permissions FROM rbac_roles WHERE id = 202`).Scan(&perms); err != nil || perms != "read" {
		t.Errorf("role permissions = %q (%v), want narrowed to read", perms, err)
	}
	var n int
	am.db.QueryRow(`SELECT COUNT(*) FROM rbac_measurement_permissions WHERE id = 203`).Scan(&n)
	if n != 0 {
		t.Error("a measurement permission deleted while away still grants access")
	}
	am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships WHERE token_id = 101`).Scan(&n)
	if n != 0 {
		t.Error("a membership removed while away still grants the team's roles")
	}
	am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships WHERE token_id = 100 AND team_id = 201 AND raft_materialised = 1`).Scan(&n)
	if n != 1 {
		t.Errorf("tok-a's membership: %d marked rows, want the local one adopted", n)
	}
	var sharedID int64
	if err := am.db.QueryRow(`SELECT id FROM rbac_organizations WHERE name = 'shared-name'`).Scan(&sharedID); err != nil || sharedID != 205 {
		t.Errorf("org realign: shared-name at id %d (%v), want 205 (legacy was %d)", sharedID, err, legacy.ID)
	}
	if rep.Errors() != 0 {
		t.Errorf("errors: %+v", rep.Tables)
	}
}
