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
	plaintext, err := amA.CreateToken("cross-node", "smoke", "read,write", nil)
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

	plaintext, err := amA.CreateToken("revokable", "smoke", "read,write", nil)
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
	if err := amA.RevokeToken(id); err != nil {
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
