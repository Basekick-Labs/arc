package cluster

import (
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// newHeartbeatTestCoordinator builds a minimal Coordinator with a registry
// containing the local node and one peer (in StateUnhealthy), so a heartbeat
// that is accepted will flip the peer to StateHealthy and one that is rejected
// will leave it unchanged.
func newHeartbeatTestCoordinator(t *testing.T, secret string) (*Coordinator, *Node) {
	t.Helper()
	local := NewNode("coord", "coord", RoleWriter, "test-cluster")
	reg := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	peer := NewNode("peer-1", "peer-1", RoleWriter, "test-cluster")
	peer.UpdateState(StateUnhealthy)
	if err := reg.Register(peer); err != nil {
		t.Fatalf("register peer: %v", err)
	}
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: "test-cluster", SharedSecret: secret},
		registry:  reg,
		localNode: local,
		logger:    zerolog.Nop(),
		// Required: the handshake validators fail closed without a replay
		// guard. Production installs this in Start() before the listener
		// accepts, so nil means a misconstructed Coordinator.
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
	}
	return c, peer
}

// deliverHeartbeat runs handleHeartbeat with a piped conn (it writes an ack)
// and drains the ack so the handler doesn't block.
func deliverHeartbeat(c *Coordinator, hb *protocol.Heartbeat) {
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.handleHeartbeat(server, hb)
		server.Close()
	}()
	// Drain whatever the handler writes (the ack), then close.
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 256)
	for {
		if _, err := client.Read(buf); err != nil {
			break
		}
	}
	client.Close()
	<-done
}

func signedHeartbeat(secret, nodeID, cluster string) *protocol.Heartbeat {
	hb := &protocol.Heartbeat{NodeID: nodeID, State: string(StateHealthy), Timestamp: time.Now()}
	if secret != "" {
		nonce, _ := security.GenerateNonce()
		hb.AuthTimestamp = time.Now().Unix()
		hb.AuthNonce = nonce
		hb.AuthHMAC = security.ComputeHeartbeatHMAC(secret, nonce, hb.AuthTimestamp, heartbeatAuthFields(hb, cluster))
	}
	return hb
}

// TestHandleHeartbeat_RejectsUnauthenticated is the regression test for
// GHSA-p378-jp5r-gpgw: with a shared secret configured, a heartbeat carrying
// no HMAC must NOT update the peer's state (an attacker must not be able to
// spoof a node's health).
func TestHandleHeartbeat_RejectsUnauthenticated(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")

	hb := &protocol.Heartbeat{NodeID: "peer-1", State: string(StateHealthy), Timestamp: time.Now()} // no auth fields
	deliverHeartbeat(c, hb)

	if got := peer.GetState(); got == StateHealthy {
		t.Errorf("unauthenticated heartbeat updated peer state to %v; want it rejected (state unchanged)", got)
	}
}

// TestHandleHeartbeat_RejectsBadHMAC: a heartbeat with a wrong HMAC is rejected.
func TestHandleHeartbeat_RejectsBadHMAC(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")

	hb := signedHeartbeat("WRONG-SECRET", "peer-1", "test-cluster")
	deliverHeartbeat(c, hb)

	if got := peer.GetState(); got == StateHealthy {
		t.Errorf("bad-HMAC heartbeat updated peer state to %v; want it rejected", got)
	}
}

// TestHandleHeartbeat_AcceptsValid: a correctly-signed heartbeat updates state.
func TestHandleHeartbeat_AcceptsValid(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")

	hb := signedHeartbeat("s3cret", "peer-1", "test-cluster")
	deliverHeartbeat(c, hb)

	if got := peer.GetState(); got != StateHealthy {
		t.Errorf("valid heartbeat: peer state = %v; want %v", got, StateHealthy)
	}
}

// TestHandleHeartbeat_NoSecretAcceptsUnsigned: when no secret is configured the
// auth gate is skipped (matching join/leave). Note: in production a secret-less
// cluster cannot start (fail-closed guard in main.go), but the handler must
// still behave for the no-secret code path.
func TestHandleHeartbeat_NoSecretAcceptsUnsigned(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "")

	hb := &protocol.Heartbeat{NodeID: "peer-1", State: string(StateHealthy), Timestamp: time.Now()}
	deliverHeartbeat(c, hb)

	if got := peer.GetState(); got != StateHealthy {
		t.Errorf("no-secret heartbeat: peer state = %v; want %v", got, StateHealthy)
	}
}

// TestHandleHeartbeat_RejectsMutatedState is the regression test for the
// heartbeat half of GHSA-p2rx, found while verifying the reported join issue.
//
// State was outside the MAC while the handler wrote it straight into the
// registry, so an on-path attacker could flip any node to unhealthy — which
// withdraws it from query routing and, with failover enabled, can trigger a
// writer promotion — using a heartbeat whose tag still verified.
func TestHandleHeartbeat_RejectsMutatedState(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")
	hb := signedHeartbeat("s3cret", peer.ID, "test-cluster")

	// On-path mutation: flip the self-reported state, keep the valid tag.
	hb.State = string(StateUnhealthy)

	deliverHeartbeat(c, hb)

	got, ok := c.registry.Get(peer.ID)
	if !ok {
		t.Fatal("peer vanished from registry")
	}
	if got.State != StateUnhealthy {
		t.Fatalf("peer state = %v; the test peer starts unhealthy so this should be unchanged", got.State)
	}
	// The mutated heartbeat must have been rejected outright: had it been
	// accepted, the handler would have recorded a heartbeat for the peer.
	if !c.registry.GetLastHeartbeat(peer.ID).IsZero() {
		t.Error("a heartbeat with a mutated State was accepted — State is outside the MAC")
	}
}

// TestHandleHeartbeat_RejectsMutatedIsLeader: IsLeader is not consumed by the
// current handler, but it is bound anyway (every field of the message is), so
// a mutation must still be rejected. This is the test that will fail if
// somebody later starts trusting the field without re-checking coverage.
func TestHandleHeartbeat_RejectsMutatedIsLeader(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")
	hb := signedHeartbeat("s3cret", peer.ID, "test-cluster")
	hb.IsLeader = !hb.IsLeader

	deliverHeartbeat(c, hb)

	if !c.registry.GetLastHeartbeat(peer.ID).IsZero() {
		t.Error("a heartbeat with a mutated IsLeader was accepted — the field is outside the MAC")
	}
}

// TestHandleHeartbeat_RejectsReplay: a captured heartbeat must not be
// replayable inside the freshness window.
func TestHandleHeartbeat_RejectsReplay(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")
	hb := signedHeartbeat("s3cret", peer.ID, "test-cluster")

	deliverHeartbeat(c, hb)
	first := c.registry.GetLastHeartbeat(peer.ID)
	if first.IsZero() {
		t.Fatal("legitimate heartbeat was rejected")
	}

	// Re-deliver the identical message, as a network attacker would.
	peer.UpdateState(StateUnhealthy)
	deliverHeartbeat(c, hb)

	got, _ := c.registry.Get(peer.ID)
	if got.State == StateHealthy {
		t.Error("a replayed heartbeat was accepted — the nonce was not consumed")
	}
}

// TestHandleHeartbeat_FailsClosedWithoutReplayGuard: a Coordinator with a
// shared secret but no nonce cache must reject rather than silently skip
// replay protection. Production always installs the cache before the listener
// accepts, so this only fires on a misconstructed Coordinator — but failing
// open there is how the check would get quietly lost.
func TestHandleHeartbeat_FailsClosedWithoutReplayGuard(t *testing.T) {
	c, peer := newHeartbeatTestCoordinator(t, "s3cret")
	c.nonceCache = nil

	deliverHeartbeat(c, signedHeartbeat("s3cret", peer.ID, "test-cluster"))

	if !c.registry.GetLastHeartbeat(peer.ID).IsZero() {
		t.Error("a heartbeat was accepted with no replay guard installed")
	}
}
