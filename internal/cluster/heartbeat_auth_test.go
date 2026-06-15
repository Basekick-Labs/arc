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
		hb.AuthHMAC = security.ComputeHMAC(secret, nonce, nodeID, cluster, hb.AuthTimestamp)
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
