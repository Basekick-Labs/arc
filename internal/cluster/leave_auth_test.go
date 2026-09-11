package cluster

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// newLeaveTestCoordinator builds a coordinator with one registered peer, so a
// leave that is accepted unregisters it and one that is rejected does not.
func newLeaveTestCoordinator(t *testing.T, secret string) (*Coordinator, *Node) {
	t.Helper()
	local := NewNode("coord", "coord", RoleWriter, joinTestCluster)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	peer := NewNode("peer-1", "peer-1", RoleWriter, joinTestCluster)
	peer.UpdateState(StateHealthy)
	if err := reg.Register(peer); err != nil {
		t.Fatalf("register peer: %v", err)
	}
	c := &Coordinator{
		cfg:        &config.ClusterConfig{ClusterName: joinTestCluster, SharedSecret: secret},
		registry:   reg,
		localNode:  local,
		logger:     zerolog.Nop(),
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
	}
	return c, peer
}

func signedLeave(secret, nodeID, reason string) *protocol.LeaveNotify {
	leave := &protocol.LeaveNotify{NodeID: nodeID, Reason: reason}
	if secret != "" {
		nonce, _ := security.GenerateNonce()
		leave.AuthTimestamp = time.Now().Unix()
		leave.AuthNonce = nonce
		leave.AuthHMAC = security.ComputeLeaveHMAC(secret, nonce, leave.AuthTimestamp, leaveAuthFields(leave, joinTestCluster))
	}
	return leave
}

// TestHandleLeaveNotify_AcceptsValid is the control for the rejection tests.
func TestHandleLeaveNotify_AcceptsValid(t *testing.T) {
	c, peer := newLeaveTestCoordinator(t, joinTestSecret)
	c.handleLeaveNotify(signedLeave(joinTestSecret, peer.ID, "graceful shutdown"))
	if _, ok := c.registry.Get(peer.ID); ok {
		t.Error("a valid leave did not unregister the peer")
	}
}

// TestHandleLeaveNotify_RejectsMutatedReason: Reason is only logged today, but
// it is bound like every other field. If a future change starts acting on it,
// this test is what keeps it from being attacker-controlled.
func TestHandleLeaveNotify_RejectsMutatedReason(t *testing.T) {
	c, peer := newLeaveTestCoordinator(t, joinTestSecret)
	leave := signedLeave(joinTestSecret, peer.ID, "graceful shutdown")
	leave.Reason = "evicted by operator"

	c.handleLeaveNotify(leave)
	if _, ok := c.registry.Get(peer.ID); !ok {
		t.Error("a leave with a mutated Reason was accepted — the field is outside the MAC")
	}
}

// TestHandleLeaveNotify_RejectsMutatedNodeID: the eviction target must be
// bound, or an attacker could redirect a legitimate node's own leave onto a
// different node and remove it from the cluster.
func TestHandleLeaveNotify_RejectsMutatedNodeID(t *testing.T) {
	c, peer := newLeaveTestCoordinator(t, joinTestSecret)
	leave := signedLeave(joinTestSecret, "some-other-node", "graceful shutdown")
	leave.NodeID = peer.ID

	c.handleLeaveNotify(leave)
	if _, ok := c.registry.Get(peer.ID); !ok {
		t.Error("a leave retargeted at another node was accepted")
	}
}

// TestHandleLeaveNotify_RejectsReplay: a captured leave must not be replayable
// — re-delivering one after the node rejoined would evict it again.
func TestHandleLeaveNotify_RejectsReplay(t *testing.T) {
	c, peer := newLeaveTestCoordinator(t, joinTestSecret)
	leave := signedLeave(joinTestSecret, peer.ID, "graceful shutdown")

	c.handleLeaveNotify(leave)
	if _, ok := c.registry.Get(peer.ID); ok {
		t.Fatal("first leave was rejected")
	}

	// The node comes back, and the attacker replays the captured leave.
	rejoined := NewNode(peer.ID, peer.ID, RoleWriter, joinTestCluster)
	rejoined.UpdateState(StateHealthy)
	if err := c.registry.Register(rejoined); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	c.handleLeaveNotify(leave)
	if _, ok := c.registry.Get(peer.ID); !ok {
		t.Error("a replayed leave evicted the rejoined node — the nonce was not consumed")
	}
}

// TestHandleLeaveNotify_FailsClosedWithoutReplayGuard: see the heartbeat and
// join equivalents; this also covers the typed-nil guard path.
func TestHandleLeaveNotify_FailsClosedWithoutReplayGuard(t *testing.T) {
	c, peer := newLeaveTestCoordinator(t, joinTestSecret)
	c.nonceCache = nil

	c.handleLeaveNotify(signedLeave(joinTestSecret, peer.ID, "graceful shutdown"))
	if _, ok := c.registry.Get(peer.ID); !ok {
		t.Error("a leave was accepted with no replay guard installed")
	}
}

// TestHandleLeaveNotify_UnsignedRejected: the plain case.
func TestHandleLeaveNotify_UnsignedRejected(t *testing.T) {
	c, peer := newLeaveTestCoordinator(t, joinTestSecret)
	c.handleLeaveNotify(&protocol.LeaveNotify{NodeID: peer.ID, Reason: "bye"})
	if _, ok := c.registry.Get(peer.ID); !ok {
		t.Error("an unsigned leave was accepted by a secret-configured node")
	}
}
