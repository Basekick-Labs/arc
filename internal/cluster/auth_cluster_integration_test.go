package cluster

// Phase A: Cluster Auth Convergence — two-node integration test.
//
// This test wires two AuthManagers + two ClusterFSMs together via a
// fake replicating-proposer that drives BOTH FSMs on every Propose call.
// It does NOT spin up real Raft over loopback (the existing
// filereplication_integration_test.go doesn't either) — what it pins is
// the customer-visible end-to-end behaviour:
//
//   1. CreateToken on node A → both FSMs apply → both nodes' local
//      SQLite gets the row → VerifyToken on node B returns valid info.
//   2. RevokeToken on node A → both nodes invalidate cache + flip
//      enabled=0 → VerifyToken on node B returns nil.
//   3. The plaintext token value never travels through the FSM payload —
//      only bcrypt hash + prefix. We assert this by inspecting the FSM
//      tokens map and confirming no plaintext appears.
//
// The bypass-Raft simulation is sufficient because the apply path is
// deterministic given identical inputs (the Raft layer's only job is
// total-order replication, which we simulate by calling Apply on both
// FSMs in the same goroutine).

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// twoNodeReplicator simulates Raft replication by applying every
// proposed command to two FSMs deterministically. A real Raft would
// serialise commits via the leader; we approximate that here with a
// shared atomic log index so both nodes apply with the same Index
// value (matching the production "every node sees the same log
// entry at the same index" invariant).
type twoNodeReplicator struct {
	logIdx atomic.Int64
	fsmA   *raft.ClusterFSM
	fsmB   *raft.ClusterFSM
}

func (r *twoNodeReplicator) Propose(ctx context.Context, cmdType uint8, payload []byte, timeout time.Duration) error {
	idx := r.logIdx.Add(1)
	cmd := raft.Command{Type: raft.CommandType(cmdType), Payload: payload}
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// Apply to node A first (the "leader"); if that errors, propagate
	// without touching B. If A succeeds, apply to B — same Index, so
	// the deterministic-stamping is consistent.
	resA := r.fsmA.Apply(&hraft.Log{Index: uint64(idx), Data: data})
	if errA, ok := resA.(error); ok && errA != nil {
		return errA
	}
	resB := r.fsmB.Apply(&hraft.Log{Index: uint64(idx), Data: data})
	if errB, ok := resB.(error); ok && errB != nil {
		return errB
	}
	return nil
}

// IsLeader: the in-test replicator always behaves as leader (the test
// drives applies to both FSMs directly). Phase A.1 extended RaftProposer
// with IsLeader for the seed path.
func (r *twoNodeReplicator) IsLeader() bool { return true }

func TestTwoNodeAuth_CreateOnAVerifyOnB(t *testing.T) {
	// Build two AuthManagers backed by isolated temp DBs.
	amA, err := auth.NewAuthManager(t.TempDir()+"/auth-a.db", 1*time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager A: %v", err)
	}
	defer amA.Close()
	amB, err := auth.NewAuthManager(t.TempDir()+"/auth-b.db", 1*time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager B: %v", err)
	}
	defer amB.Close()

	// Build two FSMs and wire their callbacks into the matching
	// AuthManager. This is the same wiring cmd/arc/main.go does at
	// runtime.
	fsmA := raft.NewClusterFSM(zerolog.Nop())
	fsmB := raft.NewClusterFSM(zerolog.Nop())
	fsmA.SetAuthCallbacks(
		func(e *raft.TokenEntry) {
			if err := amA.ApplyCreateToken(ToAuthTokenEntry(e)); err != nil {
				t.Errorf("amA.ApplyCreateToken: %v", err)
			}
		},
		func(e *raft.TokenEntry) { _ = amA.ApplyUpdateToken(ToAuthTokenEntry(e)) },
		func(id int64) { _ = amA.ApplyRevokeToken(id) },
		func(id int64) { _ = amA.ApplyDeleteToken(id) },
		func(id int64, h, p string, _ uint64) { _ = amA.ApplyRotateToken(id, h, p) },
	)
	fsmB.SetAuthCallbacks(
		func(e *raft.TokenEntry) {
			if err := amB.ApplyCreateToken(ToAuthTokenEntry(e)); err != nil {
				t.Errorf("amB.ApplyCreateToken: %v", err)
			}
		},
		func(e *raft.TokenEntry) { _ = amB.ApplyUpdateToken(ToAuthTokenEntry(e)) },
		func(id int64) { _ = amB.ApplyRevokeToken(id) },
		func(id int64) { _ = amB.ApplyDeleteToken(id) },
		func(id int64, h, p string, _ uint64) { _ = amB.ApplyRotateToken(id, h, p) },
	)

	// Wire the same replicator on both AuthManagers — every Propose
	// fans out to both FSMs.
	replicator := &twoNodeReplicator{fsmA: fsmA, fsmB: fsmB}
	amA.SetRaftProposer(replicator)
	amB.SetRaftProposer(replicator)

	// THE DECISIVE TEST: create a token on node A; verify on node B.
	plaintext, err := amA.CreateToken(context.Background(), "cross-node", "smoke", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken on A: %v", err)
	}
	if plaintext == "" {
		t.Fatal("expected plaintext token returned to caller")
	}

	// Both AuthManagers should now have the row materialised.
	infoA := amA.VerifyToken(plaintext)
	if infoA == nil {
		t.Fatal("amA.VerifyToken returned nil — proposer leader path broken")
	}
	infoB := amB.VerifyToken(plaintext)
	if infoB == nil {
		t.Fatal("amB.VerifyToken returned nil — Phase A bug NOT fixed (this is the customer-visible regression)")
	}
	if infoA.Name != "cross-node" || infoB.Name != "cross-node" {
		t.Errorf("name mismatch: A=%q B=%q", infoA.Name, infoB.Name)
	}

	// Plaintext must NOT appear anywhere in the FSM payloads / state.
	// We verify by JSON-marshalling each FSM's tokens map and grep'ing
	// the bytes for the plaintext substring. If anyone accidentally
	// ships plaintext through Raft this test breaks loudly.
	for _, fsm := range []*raft.ClusterFSM{fsmA, fsmB} {
		// Snapshot the FSM and check the marshalled blob.
		snap, err := fsm.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		sink := &testCapturingSink{}
		if err := snap.Persist(sink); err != nil {
			t.Fatalf("persist: %v", err)
		}
		if strings.Contains(string(sink.data), plaintext) {
			t.Fatal("plaintext token leaked into Raft snapshot! security regression")
		}
	}
}

func TestTwoNodeAuth_RevokeOnAInvalidatesB(t *testing.T) {
	amA, _ := auth.NewAuthManager(t.TempDir()+"/auth-a.db", 1*time.Second, 100, zerolog.Nop())
	defer amA.Close()
	amB, _ := auth.NewAuthManager(t.TempDir()+"/auth-b.db", 1*time.Second, 100, zerolog.Nop())
	defer amB.Close()

	fsmA := raft.NewClusterFSM(zerolog.Nop())
	fsmB := raft.NewClusterFSM(zerolog.Nop())
	wireAuthCallbacks(t, fsmA, amA)
	wireAuthCallbacks(t, fsmB, amB)

	replicator := &twoNodeReplicator{fsmA: fsmA, fsmB: fsmB}
	amA.SetRaftProposer(replicator)
	amB.SetRaftProposer(replicator)

	plaintext, err := amA.CreateToken(context.Background(), "revokable", "smoke", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Warm caches on both nodes.
	if amA.VerifyToken(plaintext) == nil {
		t.Fatal("amA setup verify failed")
	}
	if amB.VerifyToken(plaintext) == nil {
		t.Fatal("amB setup verify failed")
	}

	// Revoke on node A. Both FSMs apply CommandRevokeToken, both
	// AuthManagers call ApplyRevokeToken which flips enabled=0 + clears
	// the verify-token cache.
	tokens, _ := amA.ListTokens()
	var id int64
	for _, tk := range tokens {
		if tk.Name == "revokable" {
			id = tk.ID
		}
	}
	if id == 0 {
		t.Fatal("could not find token to revoke")
	}
	if err := amA.RevokeToken(context.Background(), id); err != nil {
		t.Fatalf("RevokeToken on A: %v", err)
	}

	if amA.VerifyToken(plaintext) != nil {
		t.Error("amA should reject revoked token immediately (cache invalidated)")
	}
	if amB.VerifyToken(plaintext) != nil {
		t.Error("amB should reject revoked token immediately — this is the security-incident-response gap we're closing")
	}
}

// wireAuthCallbacks installs the standard 5-callback set on an FSM that
// drives a given AuthManager. Used by tests so each test only has to
// declare the wiring once.
func wireAuthCallbacks(t *testing.T, fsm *raft.ClusterFSM, am *auth.AuthManager) {
	t.Helper()
	fsm.SetAuthCallbacks(
		func(e *raft.TokenEntry) {
			if err := am.ApplyCreateToken(ToAuthTokenEntry(e)); err != nil {
				t.Errorf("ApplyCreateToken: %v", err)
			}
		},
		func(e *raft.TokenEntry) { _ = am.ApplyUpdateToken(ToAuthTokenEntry(e)) },
		func(id int64) { _ = am.ApplyRevokeToken(id) },
		func(id int64) { _ = am.ApplyDeleteToken(id) },
		func(id int64, h, p string, _ uint64) { _ = am.ApplyRotateToken(id, h, p) },
	)
}

// testCapturingSink implements hraft.SnapshotSink for the security
// assertion in TestTwoNodeAuth_CreateOnAVerifyOnB. We don't import
// the test helpers from internal/cluster/raft (different package);
// this is the minimal in-place version.
type testCapturingSink struct {
	data []byte
}

func (s *testCapturingSink) Write(p []byte) (int, error) {
	s.data = append(s.data, p...)
	return len(p), nil
}
func (s *testCapturingSink) Close() error  { return nil }
func (s *testCapturingSink) ID() string    { return "test-sink" }
func (s *testCapturingSink) Cancel() error { return nil }

// -----------------------------------------------------------------------------
// Phase A.1: Cluster Auth Convergence (RBAC) — two-node integration tests.
//
// Mirrors the Phase A token tests above. Same twoNodeReplicator, same
// "create on A, verify on B" shape, but exercising RBAC writes through
// both AuthManagers AND RBACManagers wired against shared FSMs.
// -----------------------------------------------------------------------------

// rbacTestRig wires up two AuthManagers + two RBACManagers, each backed
// by its own SQLite DB and FSM, sharing a single twoNodeReplicator.
// Returns both RBACManagers so tests can drive writes on one and read on
// the other. Same shape as the Phase A test rig — a helper because the
// 13-callback wiring is verbose.
type rbacTestRig struct {
	rmA, rmB   *auth.RBACManager
	amA, amB   *auth.AuthManager
	fsmA, fsmB *raft.ClusterFSM
	replicator *twoNodeReplicator
}

func newRBACTestRig(t *testing.T) *rbacTestRig {
	t.Helper()
	amA, err := auth.NewAuthManager(t.TempDir()+"/auth-a.db", 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager A: %v", err)
	}
	amB, err := auth.NewAuthManager(t.TempDir()+"/auth-b.db", 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager B: %v", err)
	}
	rmA := auth.NewRBACManager(&auth.RBACManagerConfig{DB: amA.GetDB(), Logger: zerolog.Nop()})
	rmB := auth.NewRBACManager(&auth.RBACManagerConfig{DB: amB.GetDB(), Logger: zerolog.Nop()})

	t.Cleanup(func() {
		rmA.Close()
		rmB.Close()
		amA.Close()
		amB.Close()
	})

	fsmA := raft.NewClusterFSM(zerolog.Nop())
	fsmB := raft.NewClusterFSM(zerolog.Nop())

	// Token callbacks (required so AddTokenToTeam's FK check sees the
	// tokens after a CreateToken; mirrors production wiring).
	wireAuthCallbacks(t, fsmA, amA)
	wireAuthCallbacks(t, fsmB, amB)

	// RBAC callbacks — 13 each.
	wireRBACCallbacks(t, fsmA, rmA)
	wireRBACCallbacks(t, fsmB, rmB)

	replicator := &twoNodeReplicator{fsmA: fsmA, fsmB: fsmB}
	amA.SetRaftProposer(replicator)
	amB.SetRaftProposer(replicator)
	rmA.SetRaftProposer(replicator)
	rmB.SetRaftProposer(replicator)

	return &rbacTestRig{
		rmA: rmA, rmB: rmB,
		amA: amA, amB: amB,
		fsmA: fsmA, fsmB: fsmB,
		replicator: replicator,
	}
}

// wireRBACCallbacks installs the 13 RBAC callbacks on an FSM that
// drives a given RBACManager. Mirror of wireAuthCallbacks. Used so each
// test only has to declare the wiring once.
func wireRBACCallbacks(t *testing.T, fsm *raft.ClusterFSM, rm *auth.RBACManager) {
	t.Helper()
	fsm.SetRBACCallbacks(
		func(e *raft.OrganizationEntry) {
			if err := rm.ApplyCreateOrganization(ToAuthOrganizationEntry(e)); err != nil {
				t.Errorf("ApplyCreateOrganization: %v", err)
			}
		},
		func(e *raft.OrganizationEntry) {
			_ = rm.ApplyUpdateOrganization(ToAuthOrganizationEntry(e))
		},
		func(id int64) { _ = rm.ApplyDeleteOrganization(id) },
		func(e *raft.TeamEntry) {
			if err := rm.ApplyCreateTeam(ToAuthTeamEntry(e)); err != nil {
				t.Errorf("ApplyCreateTeam: %v", err)
			}
		},
		func(e *raft.TeamEntry) { _ = rm.ApplyUpdateTeam(ToAuthTeamEntry(e)) },
		func(id int64) { _ = rm.ApplyDeleteTeam(id) },
		func(e *raft.RoleEntry) {
			if err := rm.ApplyCreateRole(ToAuthRoleEntry(e)); err != nil {
				t.Errorf("ApplyCreateRole: %v", err)
			}
		},
		func(e *raft.RoleEntry) { _ = rm.ApplyUpdateRole(ToAuthRoleEntry(e)) },
		func(id int64) { _ = rm.ApplyDeleteRole(id) },
		func(e *raft.MeasurementPermissionEntry) {
			if err := rm.ApplyCreateMeasurementPermission(ToAuthMeasurementPermissionEntry(e)); err != nil {
				t.Errorf("ApplyCreateMeasurementPermission: %v", err)
			}
		},
		func(id int64) { _ = rm.ApplyDeleteMeasurementPermission(id) },
		func(e *raft.TokenMembershipEntry) {
			if err := rm.ApplyAddTokenToTeam(ToAuthTokenMembershipEntry(e)); err != nil {
				t.Errorf("ApplyAddTokenToTeam: %v", err)
			}
		},
		func(tokenID, teamID int64) {
			_ = rm.ApplyRemoveTokenFromTeam(tokenID, teamID)
		},
	)
}

// TestTwoNodeRBAC_CreateOnAReadOnB is the customer-visible test: an
// organization created on node A becomes readable on node B without
// any other coordination — proving the Phase A.1 replication closes
// the same gap for RBAC that Phase A closed for tokens.
func TestTwoNodeRBAC_CreateOnAReadOnB(t *testing.T) {
	rig := newRBACTestRig(t)

	org, err := rig.rmA.CreateOrganization(context.Background(), &auth.CreateOrganizationRequest{
		Name:        "acme",
		Description: "smoke",
	})
	if err != nil {
		t.Fatalf("CreateOrganization on A: %v", err)
	}
	if org.ID == 0 {
		t.Fatal("expected non-zero ID")
	}

	// Node B should see the row materialised via Raft apply callback.
	got, err := rig.rmB.GetOrganization(org.ID)
	if err != nil {
		t.Fatalf("GetOrganization on B: %v", err)
	}
	if got == nil {
		t.Fatal("Phase A.1 bug: org not visible on node B")
	}
	if got.Name != "acme" {
		t.Errorf("name mismatch on B: %q", got.Name)
	}
}

// TestTwoNodeRBAC_DeleteOrgCascadesAcrossNodes pins the cascade-in-apply
// invariant: a single CommandDeleteOrganization on the leader removes
// every descendant entity from BOTH nodes' FSM and local SQLite.
func TestTwoNodeRBAC_DeleteOrgCascadesAcrossNodes(t *testing.T) {
	rig := newRBACTestRig(t)
	ctx := context.Background()

	org, _ := rig.rmA.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "acme"})
	team, _ := rig.rmA.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "platform"})
	role, _ := rig.rmA.CreateRole(ctx, team.ID, &auth.CreateRoleRequest{
		DatabasePattern: "production",
		Permissions:     []string{"read", "write"},
	})

	// All three rows should be visible on B before delete.
	if got, _ := rig.rmB.GetOrganization(org.ID); got == nil {
		t.Fatal("setup: org should be on B")
	}
	if got, _ := rig.rmB.GetTeam(team.ID); got == nil {
		t.Fatal("setup: team should be on B")
	}
	if got, _ := rig.rmB.GetRole(role.ID); got == nil {
		t.Fatal("setup: role should be on B")
	}

	// Delete org on A — cascade should fire on both nodes.
	if err := rig.rmA.DeleteOrganization(ctx, org.ID); err != nil {
		t.Fatalf("DeleteOrganization on A: %v", err)
	}

	// All three rows should be gone on both nodes.
	if got, _ := rig.rmA.GetOrganization(org.ID); got != nil {
		t.Errorf("A: org should be deleted")
	}
	if got, _ := rig.rmA.GetTeam(team.ID); got != nil {
		t.Errorf("A: team should cascade-deleted")
	}
	if got, _ := rig.rmA.GetRole(role.ID); got != nil {
		t.Errorf("A: role should cascade-deleted")
	}
	if got, _ := rig.rmB.GetOrganization(org.ID); got != nil {
		t.Errorf("B: org should be deleted")
	}
	if got, _ := rig.rmB.GetTeam(team.ID); got != nil {
		t.Errorf("B: team should cascade-deleted")
	}
	if got, _ := rig.rmB.GetRole(role.ID); got != nil {
		t.Errorf("B: role should cascade-deleted")
	}
}

// TestTwoNodeRBAC_TokenMembershipRoundTrip pins the (token, team) pair
// flowing through Raft and landing on both nodes' rbac_token_memberships.
// Sets up the FK prerequisites — a real api_tokens row via CreateToken
// — then exercises AddTokenToTeam on A and reads on B.
func TestTwoNodeRBAC_TokenMembershipRoundTrip(t *testing.T) {
	rig := newRBACTestRig(t)
	ctx := context.Background()

	org, _ := rig.rmA.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "acme"})
	team, _ := rig.rmA.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "platform"})

	// Create a real token via the AuthManager so the FK is satisfied.
	plaintext, err := rig.amA.CreateToken(ctx, "smoke", "test", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if plaintext == "" {
		t.Fatal("expected plaintext returned")
	}
	// Find the token's stamped ID.
	infoA := rig.amA.VerifyToken(plaintext)
	if infoA == nil {
		t.Fatal("setup: VerifyToken returned nil")
	}

	mem, err := rig.rmA.AddTokenToTeam(ctx, infoA.ID, team.ID)
	if err != nil {
		t.Fatalf("AddTokenToTeam on A: %v", err)
	}
	if mem.TokenID != infoA.ID || mem.TeamID != team.ID {
		t.Errorf("membership fields mismatch: %+v", mem)
	}

	// Node B should now see the membership when listing the token's teams.
	teams, err := rig.rmB.GetTokenTeams(infoA.ID)
	if err != nil {
		t.Fatalf("GetTokenTeams on B: %v", err)
	}
	if len(teams) != 1 || teams[0].ID != team.ID {
		t.Errorf("expected 1 team on B, got %d (%+v)", len(teams), teams)
	}
}

// TestTwoNodeRBAC_DeleteTokenCascadesMemberships pins the Phase A.1
// extension to applyDeleteToken: hard-deleting a token also removes
// its memberships from FSM state on every node, mirroring SQLite ON
// DELETE CASCADE on rbac_token_memberships.token_id.
func TestTwoNodeRBAC_DeleteTokenCascadesMemberships(t *testing.T) {
	rig := newRBACTestRig(t)
	ctx := context.Background()

	org, _ := rig.rmA.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "acme"})
	teamA, _ := rig.rmA.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "platform"})
	teamB, _ := rig.rmA.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "data"})

	plaintext, _ := rig.amA.CreateToken(ctx, "delme", "test", "read,write", nil)
	infoA := rig.amA.VerifyToken(plaintext)
	if infoA == nil {
		t.Fatal("setup verify failed")
	}
	if _, err := rig.rmA.AddTokenToTeam(ctx, infoA.ID, teamA.ID); err != nil {
		t.Fatalf("AddTokenToTeam A: %v", err)
	}
	if _, err := rig.rmA.AddTokenToTeam(ctx, infoA.ID, teamB.ID); err != nil {
		t.Fatalf("AddTokenToTeam B: %v", err)
	}

	// Delete the token. Both teams' membership rows should be cleared on B.
	if err := rig.amA.DeleteToken(ctx, infoA.ID); err != nil {
		t.Fatalf("DeleteToken: %v", err)
	}

	teamsOnB, _ := rig.rmB.GetTokenTeams(infoA.ID)
	if len(teamsOnB) != 0 {
		t.Errorf("memberships should cascade-delete on B; got %d remaining", len(teamsOnB))
	}
	teamsOnA, _ := rig.rmA.GetTokenTeams(infoA.ID)
	if len(teamsOnA) != 0 {
		t.Errorf("memberships should cascade-delete on A; got %d remaining", len(teamsOnA))
	}
}

// TestRBACSeed_PopulatesOrgsFromLocalSQLite covers the leader-only
// upgrade-seed path. Scoped to organizations only (the seed
// intentionally does not auto-replicate teams/roles/etc. — see the
// FK-ID rebase note in rbac_seed.go's package doc). Pre-seeds an org
// directly into local SQLite, then wires the proposer and calls
// SeedRBACFromLocalSQLite. Node B should see the seeded org under a
// cluster-stamped ID via name lookup.
func TestRBACSeed_PopulatesOrgsFromLocalSQLite(t *testing.T) {
	// Build node A with a pre-seeded RBAC org in SQLite directly (i.e.
	// the OSS / pre-A.1 shape — proposer NOT yet wired).
	amA, err := auth.NewAuthManager(t.TempDir()+"/seed-a.db", 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager A: %v", err)
	}
	defer amA.Close()
	rmA := auth.NewRBACManager(&auth.RBACManagerConfig{DB: amA.GetDB(), Logger: zerolog.Nop()})
	defer rmA.Close()

	ctx := context.Background()
	// Pre-seed a few throwaway orgs and then delete them, advancing the
	// AUTOINCREMENT counter past id=1 BEFORE creating the target org.
	// This guarantees the pre-existing "preexisting" row lands at a
	// non-1 AUTOINCREMENT id (e.g. 4) while the FSM proposer will stamp
	// id=1 on its first apply — exercising the G1 upgrade-seed-accept
	// path where the cluster id and local id genuinely differ. Without
	// this nudge both ids coincide at 1 and the seed-accept branch
	// never fires.
	for _, throwaway := range []string{"throw-one", "throw-two", "throw-three"} {
		if _, err := rmA.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: throwaway}); err != nil {
			t.Fatalf("pre-seed throwaway %s: %v", throwaway, err)
		}
	}
	if _, err := amA.GetDB().Exec(`DELETE FROM rbac_organizations WHERE name LIKE 'throw-%'`); err != nil {
		t.Fatalf("clear throwaway orgs: %v", err)
	}
	// Now the target org lands at a high AUTOINCREMENT id (e.g. 4),
	// guaranteed to differ from the proposer's first log-index id (1).
	if _, err := rmA.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "preexisting"}); err != nil {
		t.Fatalf("pre-seed org: %v", err)
	}

	// Bring up node B (empty SQLite, will receive replicated state).
	amB, err := auth.NewAuthManager(t.TempDir()+"/seed-b.db", 100*time.Millisecond, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager B: %v", err)
	}
	defer amB.Close()
	rmB := auth.NewRBACManager(&auth.RBACManagerConfig{DB: amB.GetDB(), Logger: zerolog.Nop()})
	defer rmB.Close()

	fsmA := raft.NewClusterFSM(zerolog.Nop())
	fsmB := raft.NewClusterFSM(zerolog.Nop())
	wireRBACCallbacks(t, fsmA, rmA)
	wireRBACCallbacks(t, fsmB, rmB)
	wireAuthCallbacks(t, fsmA, amA)
	wireAuthCallbacks(t, fsmB, amB)

	replicator := &twoNodeReplicator{fsmA: fsmA, fsmB: fsmB}
	rmA.SetRaftProposer(replicator)
	rmB.SetRaftProposer(replicator)

	// Capture the pre-seed id on node A so we can assert the upgrade-seed
	// re-alignment branch actually swapped the leader's local row to the
	// FSM-stamped id. Gemini PR #458 flagged that without this, the
	// leader's SQLite keeps the old AUTOINCREMENT id, every subsequent
	// CreateTeam under the new cluster id fails the local FK check.
	var preSeedID int64
	if err := amA.GetDB().QueryRow(`SELECT id FROM rbac_organizations WHERE name = 'preexisting'`).Scan(&preSeedID); err != nil {
		t.Fatalf("look up pre-seed AUTOINCREMENT id: %v", err)
	}
	if preSeedID <= 1 {
		t.Fatalf("test setup: pre-seed id should be > 1 (we cleared throwaways to advance AUTOINCREMENT); got %d", preSeedID)
	}

	// Seed a pre-A.1 child row directly into SQLite under the OLD parent
	// id. The release notes claim these get cascade-deleted by SQLite ON
	// DELETE CASCADE as a side effect of the upgrade-seed realign — pin
	// that. Without this assertion, a future change to the realign
	// transaction (e.g. removing the DELETE in favour of UPDATE id=...)
	// would silently leave pre-A.1 child rows attached to a now-empty
	// parent id and the operator would discover the stranded rows in
	// production. Internal review round 2.
	now := time.Now()
	res, err := amA.GetDB().Exec(`
		INSERT INTO rbac_teams (organization_id, name, description, created_at, updated_at, enabled)
		VALUES (?, ?, ?, ?, ?, 1)
	`, preSeedID, "pre-a1-team", "pre-A.1 child row", now, now)
	if err != nil {
		t.Fatalf("seed pre-A.1 team row: %v", err)
	}
	preSeedTeamID, _ := res.LastInsertId()
	if preSeedTeamID == 0 {
		t.Fatalf("expected non-zero LastInsertId for pre-A.1 team")
	}

	// Run the seed on leader (A — twoNodeReplicator reports IsLeader=true).
	if err := rmA.SeedRBACFromLocalSQLite(ctx); err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// Node B should now hold the seeded org. The seed proposes
	// CreateOrganization; the FSM stamps a fresh log-index ID which
	// differs from the pre-existing AUTOINCREMENT ID on node A's
	// SQLite. We assert by name (UNIQUE), not by ID.
	orgsB, err := rmB.ListOrganizations()
	if err != nil {
		t.Fatalf("ListOrganizations B: %v", err)
	}
	foundOrg := false
	var orgB auth.Organization
	for _, o := range orgsB {
		if o.Name == "preexisting" {
			foundOrg = true
			orgB = o
		}
	}
	if !foundOrg {
		t.Fatalf("seeded org should be on node B; got %v", orgsB)
	}

	// G1 pin: the leader's local SQLite id must now equal the
	// FSM-stamped id (== node B's id). Without the upgrade-seed
	// re-alignment branch in ApplyCreateOrganization, the leader keeps
	// the old AUTOINCREMENT id and subsequent CreateTeam proposals fail
	// the local FK check.
	orgOnAByClusterID, err := rmA.GetOrganization(orgB.ID)
	if err != nil {
		t.Fatalf("GetOrganization on A by cluster id: %v", err)
	}
	if orgOnAByClusterID == nil {
		t.Fatalf("G1 regression: leader's SQLite has no row at cluster id=%d after upgrade seed", orgB.ID)
	}
	if orgOnAByClusterID.Name != "preexisting" {
		t.Errorf("G1 regression: leader's row at cluster id=%d has wrong name %q", orgB.ID, orgOnAByClusterID.Name)
	}
	// The pre-seed id must be GONE on the leader after re-alignment
	// (unless it happens to coincide with the FSM-stamped id, which
	// our test setup ensured against by advancing AUTOINCREMENT).
	if preSeedID != orgB.ID {
		if stale, _ := rmA.GetOrganization(preSeedID); stale != nil {
			t.Errorf("G1 regression: pre-A.1 id=%d row still present on leader after upgrade re-align: %+v", preSeedID, stale)
		}
	}

	// H2 pin: the pre-A.1 child team we inserted directly into SQLite
	// must be cascade-deleted by SQLite ON DELETE CASCADE as a side
	// effect of the realign transaction. The release notes claim this
	// behaviour — without this assertion a future change that swaps
	// DELETE for UPDATE would silently leave the pre-A.1 child stranded
	// against a parent id that no longer exists. Internal review round 2.
	if preSeedID != orgB.ID {
		var lingering int
		if err := amA.GetDB().QueryRow(`SELECT COUNT(*) FROM rbac_teams WHERE id = ?`, preSeedTeamID).Scan(&lingering); err != nil {
			t.Fatalf("count pre-A.1 team rows post-realign: %v", err)
		}
		if lingering != 0 {
			t.Errorf("H2 regression: pre-A.1 team id=%d (org=%d) survived the realign — release notes claim ON DELETE CASCADE clears it but it did not. lingering=%d", preSeedTeamID, preSeedID, lingering)
		}
	}

	// G1 follow-on: subsequent CreateTeam under the FSM-stamped org id
	// must succeed on BOTH nodes' SQLite (FK to rbac_organizations must
	// resolve). This is the customer-visible failure Gemini caught.
	team, err := rmA.CreateTeam(ctx, orgB.ID, &auth.CreateTeamRequest{Name: "post-seed-team"})
	if err != nil {
		t.Fatalf("G1 regression: CreateTeam under seeded org id=%d failed (likely local FK violation): %v", orgB.ID, err)
	}
	if team.OrganizationID != orgB.ID {
		t.Errorf("created team has wrong org id: got %d want %d", team.OrganizationID, orgB.ID)
	}

	// Re-running the seed should be idempotent (no errors, no duplicates).
	if err := rmA.SeedRBACFromLocalSQLite(ctx); err != nil {
		t.Fatalf("Seed (re-run): %v", err)
	}
	orgsBAgain, _ := rmB.ListOrganizations()
	if len(orgsBAgain) != len(orgsB) {
		t.Errorf("seed re-run created duplicates: before=%d after=%d", len(orgsB), len(orgsBAgain))
	}
}
