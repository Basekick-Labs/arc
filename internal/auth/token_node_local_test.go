package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// viewProposer is fsmLikeTokenProposer plus the cluster state's read side,
// as the production proposer has it (#1630). It records the order of its
// reads so a test can pin that the applied index is read before the map.
type viewProposer struct {
	fsmLikeTokenProposer
	vmu      sync.Mutex
	nodeID   string
	tokens   map[int64]ClusterTokenEntry
	applied  uint64
	reads    []string
	onLookup func() // runs inside ClusterToken
}

func (v *viewProposer) ClusterToken(id int64) (ClusterTokenEntry, bool) {
	v.vmu.Lock()
	v.reads = append(v.reads, "token")
	e, ok := v.tokens[id]
	hook := v.onLookup
	v.vmu.Unlock()
	if hook != nil {
		hook()
	}
	return e, ok
}

func (v *viewProposer) ClusterTokenIDByName(name string) (int64, bool) {
	v.vmu.Lock()
	defer v.vmu.Unlock()
	for id, e := range v.tokens {
		if e.Name == name {
			return id, true
		}
	}
	return 0, false
}

func (v *viewProposer) AppliedDone() uint64 {
	v.vmu.Lock()
	defer v.vmu.Unlock()
	v.reads = append(v.reads, "applied")
	return v.applied
}

func (v *viewProposer) LocalNodeID() string { return v.nodeID }

func newViewProposer() *viewProposer {
	return &viewProposer{nodeID: "n2", tokens: map[int64]ClusterTokenEntry{}, applied: 1000}
}

var here = TokenWriteOptions{Node: "n2", NodeSet: true}

// legacyToken creates a token the way a pre-v26.06.1 node did: through the
// local path, before any proposer is wired, so the row is unmarked and its
// created_at is CURRENT_TIMESTAMP. Returns its ID and secret.
func legacyToken(t *testing.T, am *AuthManager, name string) (int64, string) {
	t.Helper()
	secret, err := am.CreateToken(context.Background(), name, "", "read", nil)
	if err != nil {
		t.Fatalf("CreateToken(%s): %v", name, err)
	}
	return tokenIDByName(t, am, name), secret
}

func requireScopeCode(t *testing.T, what string, err error, code string) {
	t.Helper()
	var se *TokenScopeError
	if !errors.As(err, &se) || se.Code != code {
		t.Fatalf("%s: err = %v, want TokenScopeError %q", what, err, code)
	}
}

func markerOf(t *testing.T, am *AuthManager, id int64) int {
	t.Helper()
	var m int
	if err := am.db.QueryRow(`SELECT raft_materialised FROM api_tokens WHERE id = ?`, id).Scan(&m); err != nil {
		t.Fatalf("read marker of %d: %v", id, err)
	}
	return m
}

// The #1630 bug: on a cluster node, a revoke of a token the cluster does not
// manage was proposed, dropped by the FSM, and the token kept working.
func TestNodeLocalTokenRevokeTakesEffect(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "leaked")
	if am.VerifyToken(secret) == nil {
		t.Fatal("the legacy token does not authenticate before the revoke")
	}
	prop := newViewProposer()
	am.SetRaftProposer(prop)
	ctx := context.Background()

	// Without ?node: refused, nothing changed.
	_, err := am.RevokeTokenScoped(ctx, id, TokenWriteOptions{})
	requireScopeCode(t, "revoke without node", err, TokenCodeNodeLocal)
	if am.VerifyToken(secret) == nil {
		t.Fatal("a refused revoke disabled the token")
	}

	res, err := am.RevokeTokenScoped(ctx, id, here)
	if err != nil {
		t.Fatalf("revoke with node: %v", err)
	}
	if !res.NodeLocal || res.NodeID != "n2" || res.Name != "leaked" || res.Fingerprint == "" {
		t.Errorf("result = %+v, want node-local on n2 naming the token", res)
	}
	if got := prop.proposals(); len(got) != 0 {
		t.Errorf("proposed %v for a node-local token, want nothing", got)
	}
	// The cache was warm: the very next verify must already refuse it.
	if am.VerifyToken(secret) != nil {
		t.Fatal("the revoked node-local token still authenticates")
	}
	// Idempotent, like every revoke.
	if _, err := am.RevokeTokenScoped(ctx, id, here); err != nil {
		t.Errorf("second revoke: %v", err)
	}
}

func TestNodeLocalTokenRotateUpdateDelete(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "old")
	legacyToken(t, am, "other")
	if am.VerifyToken(secret) == nil {
		t.Fatal("legacy token does not authenticate")
	}
	prop := newViewProposer()
	prop.tokens[99] = ClusterTokenEntry{ID: 99, Name: "cluster-name", TokenHash: "h", CreatedAtUnixNano: 1}
	am.SetRaftProposer(prop)
	ctx := context.Background()

	value, res, err := am.RotateTokenScoped(ctx, id, here)
	if err != nil || value == "" || !res.NodeLocal {
		t.Fatalf("rotate: value %q, res %+v, err %v", value, res, err)
	}
	if am.VerifyToken(secret) != nil {
		t.Error("the old value still authenticates after a node-local rotate")
	}
	if info := am.VerifyToken(value); info == nil || info.ID != id {
		t.Error("the new value does not authenticate")
	}
	if markerOf(t, am, id) != 0 {
		t.Error("the node-local rotate marked the row as the cluster's")
	}

	// A rename to a name a cluster token holds is refused.
	taken := "cluster-name"
	_, err = am.UpdateTokenScoped(ctx, id, &taken, nil, nil, nil, here)
	requireScopeCode(t, "rename to a cluster name", err, TokenCodeNameHeldByCluster)

	renamed, desc := "old-renamed", "d"
	if res, err := am.UpdateTokenScoped(ctx, id, &renamed, &desc, nil, nil, here); err != nil || !res.NodeLocal {
		t.Fatalf("update: res %+v, err %v", res, err)
	}
	if info, _ := am.GetTokenByID(id); info == nil || info.Name != renamed || info.Description != desc {
		t.Errorf("update not visible: %+v", info)
	}
	// Rename to a name another local token holds is the usual conflict.
	other := "other"
	_, err = am.UpdateTokenScoped(ctx, id, &other, nil, nil, nil, here)
	requireNameConflict(t, "local rename", err, "other")

	if res, err := am.DeleteTokenScoped(ctx, id, here); err != nil || !res.NodeLocal {
		t.Fatalf("delete: res %+v, err %v", res, err)
	}
	if info, _ := am.GetTokenByID(id); info != nil {
		t.Error("the node-local token is still there after delete")
	}
	if am.VerifyToken(value) != nil {
		t.Error("the deleted token still authenticates")
	}
	if got := prop.proposals(); len(got) != 0 {
		t.Errorf("proposed %v, want nothing", got)
	}
}

func TestNodeParamInterlock(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "legacy")
	prop := newViewProposer()
	clusterID := applyClusterToken(t, am, 500, "svc")
	prop.tokens[clusterID] = ClusterTokenEntry{ID: clusterID, Name: "svc", TokenHash: "$2a$10$svcfakebcrypthash", CreatedAtUnixNano: 1_700_000_000_000_000_000 + clusterID}
	am.SetRaftProposer(prop)
	ctx := context.Background()

	// Wrong node is decided before the lookup: even an unknown ID answers
	// wrong_node, never 404 about another node's tokens.
	for _, tid := range []int64{id, clusterID, 4242} {
		_, err := am.RevokeTokenScoped(ctx, tid, TokenWriteOptions{Node: "n1", NodeSet: true})
		requireScopeCode(t, "revoke on the wrong node", err, TokenCodeWrongNode)
	}
	_, err := am.RevokeTokenScoped(ctx, id, TokenWriteOptions{NodeSet: true})
	requireScopeCode(t, "empty node", err, TokenCodeInvalidNode)

	// A cluster token named with ?node is refused, not changed locally.
	_, err = am.RevokeTokenScoped(ctx, clusterID, here)
	requireScopeCode(t, "cluster token with node", err, TokenCodeClusterToken)
	_, _, err = am.RotateTokenScoped(ctx, clusterID, here)
	requireScopeCode(t, "rotate cluster token with node", err, TokenCodeClusterToken)

	// A cluster token without ?node is proposed as before.
	res, err := am.RevokeTokenScoped(ctx, clusterID, TokenWriteOptions{})
	if err != nil || res.NodeLocal {
		t.Fatalf("cluster revoke: res %+v, err %v", res, err)
	}
	if got := prop.proposals(); len(got) != 1 || got[0] != ProposalCommandRevokeToken {
		t.Errorf("proposals = %v, want one revoke", got)
	}
	if am.VerifyToken(secret) == nil {
		t.Error("the refused requests changed the legacy token")
	}
}

// An unmarked row at an ID the cluster state holds for another token: the
// row this node shows is the one the operator named. On main the revoke was
// proposed, and revoked the cluster token on every node.
func TestNodeLocalTokenIDCollision(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "legacy")
	prop := newViewProposer()
	prop.tokens[id] = ClusterTokenEntry{ID: id, Name: "replacement", TokenHash: "other", CreatedAtUnixNano: 42}
	am.SetRaftProposer(prop)

	_, err := am.RevokeTokenScoped(context.Background(), id, TokenWriteOptions{})
	requireScopeCode(t, "revoke of a colliding legacy row", err, TokenCodeNodeLocal)
	if _, err := am.RevokeTokenScoped(context.Background(), id, here); err != nil {
		t.Fatalf("revoke with node: %v", err)
	}
	if got := prop.proposals(); len(got) != 0 {
		t.Errorf("proposed %v: the cluster token at the same ID would be revoked", got)
	}
	if am.VerifyToken(secret) != nil {
		t.Error("the legacy token still authenticates")
	}
}

// An unmarked row that is the cluster's own (identical to the FSM entry) is
// adopted and proposed, not changed locally.
func TestUnadoptedClusterRowIsAdopted(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, _ := legacyToken(t, am, "svc")
	var hash string
	if err := am.db.QueryRow(`SELECT token_hash FROM api_tokens WHERE id = ?`, id).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	prop := newViewProposer()
	prop.tokens[id] = ClusterTokenEntry{ID: id, Name: "svc", TokenHash: hash, CreatedAtUnixNano: 1}
	am.SetRaftProposer(prop)

	res, err := am.RevokeTokenScoped(context.Background(), id, TokenWriteOptions{})
	if err != nil || res.NodeLocal {
		t.Fatalf("revoke: res %+v, err %v", res, err)
	}
	if got := prop.proposals(); len(got) != 1 {
		t.Errorf("proposals = %v, want the revoke", got)
	}
	if markerOf(t, am, id) != 1 {
		t.Error("the row was not adopted")
	}
}

// A marked row the cluster state does not hold.
func TestMarkedRowNotInClusterState(t *testing.T) {
	ctx := context.Background()

	t.Run("caught up: revoke and delete local, rotate and update refused", func(t *testing.T) {
		am := newTokenWriteTestManager(t)
		id := applyClusterToken(t, am, 7, "gone")
		prop := newViewProposer()
		prop.applied = 10
		am.SetRaftProposer(prop)

		_, err := am.RevokeTokenScoped(ctx, id, TokenWriteOptions{})
		requireScopeCode(t, "revoke without node", err, TokenCodeNotInClusterState)
		_, _, err = am.RotateTokenScoped(ctx, id, here)
		requireScopeCode(t, "rotate", err, TokenCodeRevokeOrDelete)
		d := "x"
		_, err = am.UpdateTokenScoped(ctx, id, nil, &d, nil, nil, here)
		requireScopeCode(t, "update", err, TokenCodeRevokeOrDelete)
		if res, err := am.RevokeTokenScoped(ctx, id, here); err != nil || !res.NodeLocal {
			t.Fatalf("revoke: res %+v, err %v", res, err)
		}
		if info, _ := am.GetTokenByID(id); info == nil || info.Enabled {
			t.Error("the revoke did not disable the row")
		}
		if markerOf(t, am, id) != 1 {
			t.Error("the revoke changed the marker")
		}
		if got := prop.proposals(); len(got) != 0 {
			t.Errorf("proposed %v, want nothing", got)
		}
	})

	t.Run("not caught up: retry, nothing written", func(t *testing.T) {
		am := newTokenWriteTestManager(t)
		id := applyClusterToken(t, am, 7, "pending")
		prop := newViewProposer()
		prop.applied = 6
		am.SetRaftProposer(prop)
		_, err := am.RevokeTokenScoped(ctx, id, here)
		requireScopeCode(t, "revoke while behind", err, TokenCodeNotCaughtUp)
		if info, _ := am.GetTokenByID(id); info == nil || !info.Enabled {
			t.Error("a revoke while behind changed the row")
		}
		// The applied index is read before the map: an apply between the
		// two reads must not make the ID look absent at a caught-up index.
		prop.vmu.Lock()
		reads := append([]string(nil), prop.reads...)
		prop.vmu.Unlock()
		if len(reads) < 2 || reads[0] != "applied" || reads[1] != "token" {
			t.Errorf("view reads = %v, want applied before token", reads)
		}
	})
}

// A marked row from another Raft history: the cluster state holds a
// different token at its ID. It must never be proposed.
func TestMarkedRowFromAnotherHistory(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id := applyClusterToken(t, am, 7, "old-history")
	prop := newViewProposer()
	prop.tokens[id] = ClusterTokenEntry{ID: id, Name: "new-history", TokenHash: "new", CreatedAtUnixNano: 5}
	am.SetRaftProposer(prop)
	ctx := context.Background()

	_, err := am.RevokeTokenScoped(ctx, id, TokenWriteOptions{})
	requireScopeCode(t, "revoke", err, TokenCodeHistoryMismatch)
	// Only a delete: the reconcile rewrites a marked row to the token the
	// state holds at its ID, so a revoke would end as that cluster token,
	// disabled on this node for good.
	_, err = am.RevokeTokenScoped(ctx, id, here)
	requireScopeCode(t, "revoke with node", err, TokenCodeDeleteOnly)
	if r, _ := readToken(t, am, id); !r.enabled {
		t.Fatal("the refused revoke disabled the row")
	}
	_, _, err = am.RotateTokenScoped(ctx, id, here)
	requireScopeCode(t, "rotate", err, TokenCodeDeleteOnly)
	desc := "x"
	_, err = am.UpdateTokenScoped(ctx, id, nil, &desc, nil, nil, here)
	requireScopeCode(t, "update", err, TokenCodeDeleteOnly)
	if _, err := am.DeleteTokenScoped(ctx, id, here); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := prop.proposals(); len(got) != 0 {
		t.Errorf("proposed %v, want nothing", got)
	}
}

// A proposer without the view (as some fakes): the marker alone decides.
func TestNodeLocalFallbackWithoutView(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "legacy")
	clusterID := applyClusterToken(t, am, 500, "svc")
	prop := &fsmLikeTokenProposer{}
	am.SetRaftProposer(prop)
	ctx := context.Background()

	_, err := am.RevokeTokenScoped(ctx, id, TokenWriteOptions{})
	requireScopeCode(t, "revoke without node", err, TokenCodeNodeLocal)
	// Without its own ID the node cannot confirm ?node names it: fail closed.
	_, err = am.RevokeTokenScoped(ctx, id, TokenWriteOptions{Node: "anything", NodeSet: true})
	requireScopeCode(t, "revoke with node", err, TokenCodeNodeIDUnknown)
	if am.VerifyToken(secret) == nil {
		t.Error("a refused revoke changed the token")
	}
	if err := am.RevokeToken(ctx, clusterID); err != nil {
		t.Fatalf("cluster revoke: %v", err)
	}
	if got := prop.proposals(); len(got) != 1 {
		t.Errorf("proposals = %v, want one", got)
	}
}

// The row changes between the check and the write: nothing is written.
func TestNodeLocalWriteRowChangedUnderneath(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "legacy")
	prop := newViewProposer()
	prop.onLookup = func() {
		// An apply adopts the row while the write is being decided.
		if _, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 1 WHERE id = ?`, id); err != nil {
			t.Errorf("adopt: %v", err)
		}
	}
	am.SetRaftProposer(prop)
	_, err := am.RevokeTokenScoped(context.Background(), id, here)
	requireScopeCode(t, "revoke of a row adopted underneath", err, TokenCodeChanged)
	if am.VerifyToken(secret) == nil {
		t.Error("the refused write changed the token")
	}
}

func TestNodeLocalTokenMemberships(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()
	ctx := context.Background()
	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "org"})
	if err != nil {
		t.Fatalf("CreateOrganization: %v", err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "team"})
	if err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	team2, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "team2"})
	if err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	id, _ := legacyToken(t, am, "legacy")
	if _, err := rm.AddTokenToTeam(ctx, id, team.ID); err != nil {
		t.Fatalf("AddTokenToTeam: %v", err)
	}
	// A cluster membership materialised at the same token ID (a collision).
	if _, err := am.db.Exec(`INSERT INTO rbac_token_memberships (token_id, team_id, raft_materialised) VALUES (?, ?, 1)`, id, team2.ID); err != nil {
		t.Fatal(err)
	}
	prop := newViewProposer()
	am.SetRaftProposer(prop)

	_, err = am.ResolveTokenMembershipWrite(id, TokenWriteOptions{}, false)
	requireScopeCode(t, "add a node-local token to a team", err, TokenCodeNodeLocal)
	_, err = am.ResolveTokenMembershipWrite(id, TokenWriteOptions{}, true)
	requireScopeCode(t, "remove without node", err, TokenCodeNodeLocal)
	res, err := am.ResolveTokenMembershipWrite(id, here, true)
	if err != nil || !res.NodeLocal {
		t.Fatalf("remove with node: res %+v, err %v", res, err)
	}
	if err := rm.RemoveTokenFromTeamLocal(ctx, id, team.ID); err != nil {
		t.Fatalf("RemoveTokenFromTeamLocal: %v", err)
	}
	if err := rm.RemoveTokenFromTeamLocal(ctx, id, team.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("second removal: %v, want ErrNotFound", err)
	}
	err = rm.RemoveTokenFromTeamLocal(ctx, id, team2.ID)
	requireScopeCode(t, "remove the cluster token's membership", err, TokenCodeClusterMembership)
	var n int
	_ = am.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?`, id, team2.ID).Scan(&n)
	if n != 1 {
		t.Error("the cluster membership was removed")
	}

	// Standalone: unchanged, nothing classified.
	am.SetRaftProposer(nil)
	if res, err := am.ResolveTokenMembershipWrite(id, TokenWriteOptions{}, false); err != nil || res.NodeLocal {
		t.Errorf("standalone: res %+v, err %v", res, err)
	}
}

// Standalone ignores ?node entirely.
func TestStandaloneIgnoresNodeParam(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "solo")
	for _, opts := range []TokenWriteOptions{{}, {NodeSet: true}, {Node: "x", NodeSet: true}} {
		if res, err := am.RevokeTokenScoped(context.Background(), id, opts); err != nil || res.NodeLocal {
			t.Errorf("standalone revoke with %+v: res %+v, err %v", opts, res, err)
		}
	}
	if am.VerifyToken(secret) != nil {
		t.Error("still authenticates")
	}
	tokens, _ := am.ListTokens()
	for _, tk := range tokens {
		if tk.NodeLocal || tk.NodeID != "" {
			t.Errorf("standalone list marks %+v node-local", tk)
		}
	}
}

func TestListMarksNodeLocalTokens(t *testing.T) {
	am := newTokenWriteTestManager(t)
	legacyID, _ := legacyToken(t, am, "legacy")
	clusterID := applyClusterToken(t, am, 500, "svc")
	goneID := applyClusterToken(t, am, 600, "gone")      // the state no longer holds it
	otherID := applyClusterToken(t, am, 700, "old-hist") // another history
	futureID := applyClusterToken(t, am, 2000, "future") // above the applied index
	prop := newViewProposer()
	prop.tokens[clusterID] = ClusterTokenEntry{ID: clusterID, Name: "svc", TokenHash: "$2a$10$svcfakebcrypthash", CreatedAtUnixNano: 1_700_000_000_000_000_000 + clusterID}
	prop.tokens[otherID] = ClusterTokenEntry{ID: otherID, Name: "new-hist", TokenHash: "other", CreatedAtUnixNano: 5}
	am.SetRaftProposer(prop)
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatal(err)
	}
	// The label follows the write classification, not the marker alone.
	want := map[int64]bool{legacyID: true, clusterID: false, goneID: true, otherID: true, futureID: false}
	for _, tk := range tokens {
		w, ok := want[tk.ID]
		if !ok {
			continue
		}
		if tk.NodeLocal != w || (w && tk.NodeID != "n2") || (!w && tk.NodeID != "") {
			t.Errorf("token %d (%s): node_local %v node_id %q, want %v", tk.ID, tk.Name, tk.NodeLocal, tk.NodeID, w)
		}
		info, _ := am.GetTokenByID(tk.ID)
		am.MarkNodeLocal(info)
		if info.NodeLocal != w {
			t.Errorf("get of token %d: node_local %v, want %v", tk.ID, info.NodeLocal, w)
		}
	}
	info, _ := am.GetTokenByID(legacyID)
	am.MarkNodeLocal(info)
	if !info.NodeLocal || info.NodeID != "n2" {
		t.Errorf("MarkNodeLocal: %+v", info)
	}
}

// The rotate and update guards: an apply adopts the row between the check
// and the write, and the node-local write changes nothing.
func TestNodeLocalRotateUpdateRowChangedUnderneath(t *testing.T) {
	for _, op := range []string{"rotate", "update"} {
		t.Run(op, func(t *testing.T) {
			am := newTokenWriteTestManager(t)
			id, secret := legacyToken(t, am, "legacy")
			prop := newViewProposer()
			prop.onLookup = func() {
				if _, err := am.db.Exec(`UPDATE api_tokens SET raft_materialised = 1 WHERE id = ?`, id); err != nil {
					t.Errorf("adopt: %v", err)
				}
			}
			am.SetRaftProposer(prop)
			ctx := context.Background()
			var err error
			if op == "rotate" {
				_, _, err = am.RotateTokenScoped(ctx, id, here)
			} else {
				desc := "changed"
				_, err = am.UpdateTokenScoped(ctx, id, nil, &desc, nil, nil, here)
			}
			requireScopeCode(t, op+" of a row adopted underneath", err, TokenCodeChanged)
			if am.VerifyToken(secret) == nil {
				t.Error("the old secret no longer authenticates")
			}
			if info, _ := am.GetTokenByID(id); info == nil || info.Description != "" {
				t.Errorf("the refused write changed the row: %+v", info)
			}
		})
	}
}

// A cluster row whose creation instant an older reconcile left behind, while
// a rename is between the FSM map and the callback: the same secret still
// identifies it as the cluster token, so a plain revoke is proposed.
func TestMarkedRowWithStaleInstantIsTheClusterToken(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id := applyClusterToken(t, am, 300, "svc")
	mustExec(t, am, `UPDATE api_tokens SET created_at = '2020-01-01 00:00:00' WHERE id = ?`, id)
	prop := newViewProposer()
	prop.tokens[id] = ClusterTokenEntry{ID: id, Name: "svc-renamed", TokenHash: "$2a$10$svcfakebcrypthash", CreatedAtUnixNano: 1_700_000_000_000_000_000 + id}
	am.SetRaftProposer(prop)
	if _, err := am.RevokeTokenScoped(context.Background(), id, TokenWriteOptions{}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if got := prop.proposals(); len(got) != 1 {
		t.Errorf("proposals = %v, want the cluster revoke", got)
	}
}

// Team adds of a cluster row the state does not hold name the real reason.
func TestMembershipAddNamesTheClass(t *testing.T) {
	am := newTokenWriteTestManager(t)
	goneID := applyClusterToken(t, am, 600, "gone")
	otherID := applyClusterToken(t, am, 700, "old-hist")
	prop := newViewProposer()
	prop.tokens[otherID] = ClusterTokenEntry{ID: otherID, Name: "new-hist", TokenHash: "other", CreatedAtUnixNano: 5}
	am.SetRaftProposer(prop)
	_, err := am.ResolveTokenMembershipWrite(goneID, TokenWriteOptions{}, false)
	requireScopeCode(t, "add a row the state lacks", err, TokenCodeNotInClusterState)
	_, err = am.ResolveTokenMembershipWrite(otherID, TokenWriteOptions{}, false)
	requireScopeCode(t, "add a row from another history", err, TokenCodeHistoryMismatch)
	_, err = am.ResolveTokenMembershipWrite(otherID, here, true)
	requireScopeCode(t, "remove from a row from another history", err, TokenCodeDeleteOnly)
}

// A node that cannot tell its own ID refuses every ?node write.
func TestNodeParamFailsClosedWithoutLocalID(t *testing.T) {
	am := newTokenWriteTestManager(t)
	id, secret := legacyToken(t, am, "legacy")
	prop := newViewProposer()
	prop.nodeID = ""
	am.SetRaftProposer(prop)
	_, err := am.RevokeTokenScoped(context.Background(), id, here)
	requireScopeCode(t, "revoke", err, TokenCodeNodeIDUnknown)
	if am.VerifyToken(secret) == nil {
		t.Error("a refused revoke changed the token")
	}
}
