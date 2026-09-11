package cluster

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// TestSendHeartbeats_SignsPerDestination pins the per-peer signing behaviour.
//
// Two things break if heartbeats are signed once per tick and the same message
// is sent to every peer:
//
//   - Correctness. The receiver now consumes the nonce in a replay cache, and
//     two registry entries CAN share an address: node IDs are hostname+PID, so
//     a bare-metal restart leaves the old entry behind and nothing evicts it.
//     The second delivery would be rejected as a replay every single tick,
//     making the one log line that is supposed to mean "attack" routine noise.
//   - Safety. The heartbeat is handed to N goroutines that each marshal it, so
//     writing the auth fields into a shared struct is a data race. Run this
//     test under -race, which is where that shows up.
//
// Both peers here deliberately point at one listener, which is the
// duplicate-address case.
func TestSendHeartbeats_SignsPerDestination(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	const wantDeliveries = 2
	var (
		mu       sync.Mutex
		received []*protocol.Heartbeat
	)
	// Counts deliveries, not accepts: sendHeartbeats dispatches a goroutine
	// per peer and returns immediately, so the test has to wait on the
	// messages actually arriving.
	var delivered sync.WaitGroup
	delivered.Add(wantDeliveries)

	go func() {
		for i := 0; i < wantDeliveries; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				msg, err := protocol.ReceiveMessage(conn, 2*time.Second)
				if err != nil {
					return
				}
				hb, ok := msg.Payload.(*protocol.Heartbeat)
				if !ok {
					return
				}
				mu.Lock()
				received = append(received, hb)
				mu.Unlock()
				// The sender blocks for up to 3s waiting on an ack, so reply
				// or this test pays that wall-clock cost per peer.
				_ = protocol.SendMessage(conn, protocol.NewHeartbeatAck(&protocol.HeartbeatAck{
					NodeID:    "peer",
					Timestamp: time.Now(),
				}), 2*time.Second)
				delivered.Done()
			}(conn)
		}
	}()

	local := NewNode("local-node", "local", RoleWriter, joinTestCluster)
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})

	// Two distinct node IDs sharing one address — what a bare-metal restart
	// leaves behind.
	for _, id := range []string{"peer-old", "peer-new"} {
		n := NewNode(id, id, RoleWriter, joinTestCluster)
		n.SetAddresses(addr, "")
		n.UpdateState(StateHealthy)
		if err := reg.Register(n); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	c := &Coordinator{
		cfg:        &config.ClusterConfig{ClusterName: joinTestCluster, SharedSecret: joinTestSecret},
		registry:   reg,
		localNode:  local,
		logger:     zerolog.Nop(),
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
	}

	c.sendHeartbeats()

	waited := make(chan struct{})
	go func() { delivered.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for heartbeat deliveries")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != wantDeliveries {
		t.Fatalf("got %d heartbeats, want %d", len(received), wantDeliveries)
	}

	// Distinct nonces: this is what keeps the second delivery from looking
	// like a replay.
	if received[0].AuthNonce == received[1].AuthNonce {
		t.Error("both heartbeats carried the same nonce — the second would be rejected as a replay every tick")
	}

	// And each must independently validate, so per-peer signing did not
	// produce a MAC over the wrong struct.
	guard := security.NewNonceCache(security.HMACTimestampTolerance)
	for i, hb := range received {
		if err := security.ValidateHeartbeatHMACWithReplay(
			guard, joinTestSecret, hb.AuthNonce, hb.AuthTimestamp,
			heartbeatAuthFields(hb, joinTestCluster), hb.AuthHMAC, security.HMACTimestampTolerance,
		); err != nil {
			t.Errorf("heartbeat %d failed validation: %v", i, err)
		}
	}
}
