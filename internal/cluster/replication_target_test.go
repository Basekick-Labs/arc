package cluster

// #885: a replica picked its replication source by ranging the registry map and
// taking the first healthy writer, with no check of WriterState. In local
// storage mode only the primary ingests, so a standby writer's Sender has
// nothing to stream — a replica attached to one receives no live entries at
// all. With the three-writer default that was roughly two nodes in three.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// targetRig builds a reader coordinator whose registry holds the given writers,
// backed by a real single-node Raft so the FSM's primary record is the same
// thing production reads.
//
// The primary must come from the FSM, not from the registry's WriterState: that
// is the whole point of the selector, since the registry's view filters on
// health and the FSM's does not. A rig that stubbed the registry instead would
// pass against a selector reading the wrong source.
func targetRig(t *testing.T, primaryID string, writerIDs ...string) *Coordinator {
	t.Helper()
	raftNode := startRaftNode(t, "reader-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	local := NewNode("reader-1", "reader-1", RoleReader, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 16, Logger: zerolog.Nop()})

	for _, id := range writerIDs {
		w := NewNode(id, id, RoleWriter, "test-cluster")
		w.Address = id + ":9000"
		w.UpdateState(StateHealthy)
		if err := reg.Register(w); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		if err := raftNode.AddNode(&raft.NodeInfo{
			ID: id, Name: id, Role: string(RoleWriter), ClusterName: "test-cluster",
			Address: w.Address, State: string(StateHealthy),
		}, 5*time.Second); err != nil {
			t.Fatalf("AddNode %s: %v", id, err)
		}
	}
	if primaryID != "" {
		if err := raftNode.PromoteWriter(primaryID, "", 5*time.Second); err != nil {
			t.Fatalf("PromoteWriter %s: %v", primaryID, err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for raftNode.FSM().GetPrimaryWriterID() != primaryID {
			if time.Now().After(deadline) {
				t.Fatalf("setup: FSM never recorded %s as primary", primaryID)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	c := &Coordinator{
		cfg:                 &config.ClusterConfig{},
		registry:            reg,
		localNode:           local,
		raftNode:            raftNode,
		replicationRetarget: make(chan struct{}, 1),
		logger:              zerolog.Nop(),
	}
	// startReceiverWithAddr hands c.ctx to Receiver.Start, which derives a
	// child from it; a nil parent panics.
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)
	return c
}

// The core of #885. Enough writers that map order cannot accidentally land on
// the primary every run: with the primary registered last of five, the old
// implementation had a 1-in-5 chance of being right per iteration, so a
// restored-bug run fails this essentially always.
func TestFindWriterAddrPrefersThePrimary(t *testing.T) {
	c := targetRig(t, "writer-5", "writer-1", "writer-2", "writer-3", "writer-4", "writer-5")

	if got, want := c.findWriterAddr(), "writer-5:9000"; got != want {
		t.Errorf("findWriterAddr() = %q, want %q (the designated primary; a standby has nothing to stream)", got, want)
	}
}

// Run it repeatedly, because the defect is map-iteration order and a single
// sample can pass on the bug by luck.
func TestFindWriterAddrPrefersThePrimaryEveryTime(t *testing.T) {
	c := targetRig(t, "writer-3", "writer-1", "writer-2", "writer-3", "writer-4", "writer-5")

	for i := 0; i < 200; i++ {
		if got, want := c.findWriterAddr(), "writer-3:9000"; got != want {
			t.Fatalf("iteration %d: findWriterAddr() = %q, want %q", i, got, want)
		}
	}
}

// Pattern 2 never designates a primary, and Pattern 1 has none before its first
// election. Falling back to a healthy writer keeps those clusters replicating
// instead of going dark, which is why the selector is a preference and not a
// requirement.
//
// The fallback must also be STABLE. The target loop compares its answer against
// the live receiver's address, so a fallback that reshuffled with map order
// would tear down a working stream on most passes — and in Pattern 2, where no
// primary is ever designated, the fallback is the only path, so that churn
// would be permanent.
func TestFindWriterAddrFallsBackDeterministicallyWhenNoPrimary(t *testing.T) {
	c := targetRig(t, "", "writer-5", "writer-3", "writer-1", "writer-4", "writer-2")

	first := c.findWriterAddr()
	if first == "" {
		t.Fatal("findWriterAddr() returned nothing with five healthy writers available")
	}
	for i := 0; i < 200; i++ {
		if got := c.findWriterAddr(); got != first {
			t.Fatalf("iteration %d: findWriterAddr() = %q, want %q — the fallback must not reshuffle, "+
				"or the target loop rebuilds the receiver on most passes", i, got, first)
		}
	}
}

// An unhealthy primary is still the target, and that is deliberate.
//
// The tempting behaviour — fall back to a healthy writer when the primary looks
// sick — is what makes the two sides of this protocol disagree. The replica's
// registry filters on health, so it would move on; every writer's accept check
// reads the FSM's durable record, which still names the sick primary, so every
// writer it tries would refuse it. The replica would be dark for the whole
// failover window, and forever on a cluster whose automatic failover is off.
//
// Staying put costs nothing: there is nowhere else with live entries, the dial
// simply fails and retries, and a promotion re-targets us.
func TestFindWriterAddrKeepsAnUnhealthyDesignatedPrimary(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1", "writer-2")
	c.registry.UpdateNodeState("writer-1", StateDead)

	if got, want := c.findWriterAddr(), "writer-1:9000"; got != want {
		t.Errorf("findWriterAddr() = %q, want %q — falling back to a writer the cluster has not designated "+
			"means dialling a node whose own accept check will refuse us", got, want)
	}
}

// Once the cluster promotes a replacement, the selector follows it.
func TestFindWriterAddrFollowsAReplacementPrimary(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1", "writer-2")
	c.registry.UpdateNodeState("writer-1", StateDead)

	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatalf("PromoteWriter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.raftNode.FSM().GetPrimaryWriterID() != "writer-2" {
		if time.Now().After(deadline) {
			t.Fatal("FSM never recorded writer-2 as primary")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got, want := c.findWriterAddr(), "writer-2:9000"; got != want {
		t.Errorf("findWriterAddr() = %q, want %q after the replacement was promoted", got, want)
	}
}

// The self-check. GetPrimaryWriter does not exclude the local node, so a node
// that is somehow both primary and running a receiver must not be handed its
// own address to dial.
func TestFindWriterAddrNeverReturnsSelf(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.Address = "writer-1:9000"
	local.UpdateState(StateHealthy)
	local.SetWriterState(WriterStatePrimary)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	if err := reg.Register(local); err != nil {
		t.Fatalf("register local: %v", err)
	}

	raftNode := startRaftNode(t, "writer-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if err := raftNode.AddNode(&raft.NodeInfo{
		ID: "writer-1", Name: "writer-1", Role: string(RoleWriter), ClusterName: "test-cluster",
		Address: "writer-1:9000", State: string(StateHealthy),
	}, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := raftNode.PromoteWriter("writer-1", "", 5*time.Second); err != nil {
		t.Fatalf("PromoteWriter: %v", err)
	}

	c := &Coordinator{
		cfg:                 &config.ClusterConfig{},
		registry:            reg,
		localNode:           local,
		raftNode:            raftNode,
		replicationRetarget: make(chan struct{}, 1),
		logger:              zerolog.Nop(),
	}

	if got := c.findWriterAddr(); got == "writer-1:9000" {
		t.Error("findWriterAddr() returned the local node's own address; the receiver would dial itself")
	}
}

// The FSM callbacks run on the Raft Apply goroutine under a registry-only
// contract (#797, #813): they must not block. pokeReplicationRetarget is the
// whole of what they do for re-targeting, so it has to stay non-blocking even
// when nothing is draining the channel.
func TestPokeReplicationRetargetNeverBlocks(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1")

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Far more pokes than the channel's depth of 1, with no reader.
		for i := 0; i < 1000; i++ {
			c.pokeReplicationRetarget()
		}
	}()

	select {
	case <-done:
	case <-timeoutAfterSeconds(5):
		t.Fatal("pokeReplicationRetarget blocked; an FSM callback would stall every cluster state update")
	}
}

// A Coordinator built without NewCoordinator has a nil channel. A send on a nil
// channel blocks forever, so the select's default arm is what keeps this a
// no-op rather than a deadlock.
func TestPokeReplicationRetargetToleratesNilChannel(t *testing.T) {
	c := &Coordinator{cfg: &config.ClusterConfig{}, logger: zerolog.Nop()}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.pokeReplicationRetarget()
	}()

	select {
	case <-done:
	case <-timeoutAfterSeconds(5):
		t.Fatal("pokeReplicationRetarget blocked on a nil channel")
	}
}

// The re-target itself. This is the highest-risk piece of the change — it stops
// a live receiver and installs a replacement across a lock release — and it had
// no coverage.
//
// The assertion is deliberately on the receiver POINTER, not just the address.
// Re-pointing an existing receiver by mutating cfg.WriterAddr would satisfy an
// address-only check while carrying the old writer's sequence high-water mark
// into a new writer's sequence space, which wedges the stream permanently
// (#887). Only a fresh Receiver starts at the zero mark that is safe to meet an
// unrelated space with.
func TestReevaluateReplicationTargetReplacesTheReceiver(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1", "writer-2")

	// Attach to the current primary.
	c.mu.Lock()
	err := c.startReceiverWithAddr("writer-1:9000")
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startReceiverWithAddr: %v", err)
	}
	c.mu.RLock()
	before := c.replicationReceiver
	c.mu.RUnlock()
	if before == nil {
		t.Fatal("setup: no receiver was installed")
	}
	t.Cleanup(func() {
		c.mu.RLock()
		r := c.replicationReceiver
		c.mu.RUnlock()
		if r != nil {
			_ = r.Stop()
		}
	})

	// The cluster hands over to writer-2.
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatalf("PromoteWriter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.raftNode.FSM().GetPrimaryWriterID() != "writer-2" {
		if time.Now().After(deadline) {
			t.Fatal("setup: FSM never recorded writer-2 as primary")
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.reevaluateReplicationTarget(context.Background())

	c.mu.RLock()
	after := c.replicationReceiver
	c.mu.RUnlock()
	if after == nil {
		t.Fatal("re-target left no receiver at all")
	}
	if after == before {
		t.Error("re-target reused the existing Receiver; it must build a fresh one so lastSeq starts at zero (#887)")
	}
	if got, want := after.WriterAddr(), "writer-2:9000"; got != want {
		t.Errorf("receiver WriterAddr() = %q, want %q", got, want)
	}
	if got, want := before.WriterAddr(), "writer-1:9000"; got != want {
		t.Errorf("the OLD receiver's address changed to %q; re-targeting must not mutate cfg.WriterAddr", got)
	}
}

// Re-evaluating when nothing has changed must be a no-op. The loop runs on a
// timer, so a pass that rebuilt the receiver anyway would tear down a working
// stream on every tick.
func TestReevaluateReplicationTargetIsIdempotent(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1", "writer-2")

	c.mu.Lock()
	err := c.startReceiverWithAddr("writer-1:9000")
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startReceiverWithAddr: %v", err)
	}
	c.mu.RLock()
	before := c.replicationReceiver
	c.mu.RUnlock()
	t.Cleanup(func() { _ = before.Stop() })

	for i := 0; i < 5; i++ {
		c.reevaluateReplicationTarget(context.Background())
	}

	c.mu.RLock()
	after := c.replicationReceiver
	c.mu.RUnlock()
	if after != before {
		t.Error("re-evaluating with an unchanged primary rebuilt the receiver; the timer would churn a live stream")
	}
}

// The accept-side check. A writer must refuse a reader when the cluster has
// designated someone else, and — the part that actually bites operationally —
// it must answer in the handshake's own message type and CLOSE the connection.
//
// handlePeerConnection hands ownership of the socket to this path
// (closeConn = false), so a rejection that does not close leaks an fd per
// reconnect interval, forever, on the writer being wrongly dialled.
func TestAcceptReplicationConnectionRefusesWhenAnotherNodeIsPrimary(t *testing.T) {
	c := targetRig(t, "writer-2", "writer-1", "writer-2")
	c.localNode = NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	c.replicationSender = replication.NewSender(&replication.SenderConfig{
		BufferSize:   10,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: "accept-check-test-shared-secret",
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close() })

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.AcceptReplicationConnection(serverConn, &replication.ReplicateSync{
			ReaderID:       "reader-1",
			HandshakeNonce: "nonce-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		})
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	msg, err := protocol.ReceiveMessage(clientConn, 5*time.Second)
	if err != nil {
		t.Fatalf("the reader could not decode the rejection: %v — a refusal must use the handshake's own "+
			"MsgReplicateSyncAck, or the reader sees an opaque framing error instead of the reason", err)
	}
	if msg.Type != protocol.MsgReplicateSyncAck {
		t.Fatalf("rejection message type = %v, want MsgReplicateSyncAck", msg.Type)
	}
	ack, ok := msg.Payload.(*protocol.ReplicateSyncAck)
	if !ok {
		t.Fatalf("rejection payload type = %T, want *protocol.ReplicateSyncAck", msg.Payload)
	}
	if ack.Error == "" {
		t.Error("rejection carried an empty Error; the reader has nothing to log")
	}

	if err := <-errCh; err == nil {
		t.Error("AcceptReplicationConnection returned nil for a refused connection")
	}

	// The socket must be closed by the rejection path: the caller handed it
	// over and will not close it.
	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := clientConn.Read(buf); err == nil {
		t.Error("the connection was still open after a rejection; handlePeerConnection does not close it, so this leaks an fd per reconnect")
	}
}

// And it must NOT refuse while the cluster has designated nobody — a booting
// cluster, or one whose hand-over found no successor. Gating on "am I the
// primary" instead of "is someone else" refused the first connection of every
// cluster start and cost a reconnect interval of replication before it healed.
func TestAcceptReplicationConnectionAcceptsWhenNoPrimaryIsDesignated(t *testing.T) {
	c := targetRig(t, "", "writer-1", "writer-2")
	c.localNode = NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	c.replicationSender = replication.NewSender(&replication.SenderConfig{
		BufferSize:   10,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: "accept-check-test-shared-secret",
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := c.replicationSender.Start(ctx); err != nil {
		t.Fatalf("sender Start: %v", err)
	}
	t.Cleanup(func() { c.replicationSender.Stop() })

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close() })

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.AcceptReplicationConnection(serverConn, &replication.ReplicateSync{
			ReaderID:       "reader-1",
			HandshakeNonce: "nonce-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		})
	}()

	msg, err := protocol.ReceiveMessage(clientConn, 5*time.Second)
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	ack, ok := msg.Payload.(*protocol.ReplicateSyncAck)
	if !ok {
		t.Fatalf("payload type = %T, want *protocol.ReplicateSyncAck", msg.Payload)
	}
	if ack.Error != "" {
		t.Errorf("a cluster with no designated primary refused the reader: %q — every cluster start would "+
			"lose a reconnect interval of replication", ack.Error)
	}
	if err := <-errCh; err != nil {
		t.Errorf("AcceptReplicationConnection = %v, want nil", err)
	}
}

// A designated primary that has been REMOVED from the registry — evicted, or
// restarted and not yet re-discovered — must still be resolvable, because every
// writer checks the same designation before accepting. Falling back to another
// writer means reconnecting into a guaranteed refusal every interval.
//
// Seen on a live cluster: a reader whose primary was dropped from its registry
// hammered the surviving standby with a rejected handshake every five seconds,
// while the primary itself was back up and serving.
func TestFindWriterAddrResolvesADesignatedPrimaryMissingFromTheRegistry(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1", "writer-2")

	// The primary drops out of the registry but keeps its FSM designation.
	c.registry.Unregister("writer-1")
	if _, ok := c.registry.Get("writer-1"); ok {
		t.Fatal("setup: writer-1 still in the registry")
	}
	if got := c.raftNode.FSM().GetPrimaryWriterID(); got != "writer-1" {
		t.Fatalf("setup: FSM primary = %q, want writer-1", got)
	}

	if got, want := c.findWriterAddr(), "writer-1:9000"; got != want {
		t.Errorf("findWriterAddr() = %q, want %q — resolved from the FSM. Falling back to writer-2 "+
			"attaches to a node whose own accept check refuses us, every reconnect interval, forever", got, want)
	}
}

// And when the designation names a node whose address is not known anywhere,
// hold rather than attach somewhere that will refuse.
//
// Note the state is narrow by construction: PromoteWriter refuses a node the
// FSM does not hold, and removing a node clears its designation, so "designated
// but absent from the FSM" cannot arise. What can is a designated node carrying
// no address, which is what this builds.
func TestFindWriterAddrHoldsWhenTheDesignatedPrimaryHasNoAddress(t *testing.T) {
	c := targetRig(t, "", "writer-2")

	if err := c.raftNode.AddNode(&raft.NodeInfo{
		ID: "writer-1", Name: "writer-1", Role: string(RoleWriter),
		ClusterName: "test-cluster", Address: "", State: string(StateHealthy),
	}, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := c.raftNode.PromoteWriter("writer-1", "", 5*time.Second); err != nil {
		t.Fatalf("PromoteWriter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.raftNode.FSM().GetPrimaryWriterID() != "writer-1" {
		if time.Now().After(deadline) {
			t.Fatal("setup: FSM never recorded writer-1 as primary")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := c.findWriterAddr(); got != "" {
		t.Errorf("findWriterAddr() = %q, want \"\" — writer-2 is healthy but undesignated, so dialling it "+
			"lands straight in its refusal", got)
	}
}
