package cluster

// #1324: a forwarded command's ack can carry its log index, so the follower
// that forwarded it waits until its own FSM has applied it before reading its
// own write back.

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// The index is signed only on a success ack to a request that asked for it,
// so an ack between older builds is signed exactly as before.
func TestForwardAck_IndexSignedOnlyWhenAsked(t *testing.T) {
	c := &Coordinator{cfg: &config.ClusterConfig{SharedSecret: forwardFSMSecret, ClusterName: "test-cluster"}, logger: zerolog.Nop()}

	ack := &protocol.ForwardApplyAck{Status: "ok", Index: 42}
	c.signForwardAckIndex(ack, "n1", true)
	if err := c.checkForwardAckIndex(ack, "n1", true); err != nil {
		t.Fatalf("signed index ack: %v", err)
	}
	tampered := *ack
	tampered.Index = 43
	if err := c.checkForwardAckIndex(&tampered, "n1", true); err == nil {
		t.Fatal("an ack whose index changed in flight validated")
	}
	if err := c.checkForwardAckIndex(ack, "n1", false); err == nil {
		t.Fatal("an index-signed ack validated as a legacy ack")
	}

	// Not asked for: the MAC is the legacy one, whatever the Index field.
	legacy := &protocol.ForwardApplyAck{Status: "ok"}
	c.signForwardAckIndex(legacy, "n2", false)
	want := security.ComputeForwardAckHMAC(forwardFSMSecret, "n2", legacy.AuthTimestamp,
		security.ForwardAckAuthFields{Status: "ok"})
	if legacy.AuthHMAC != want {
		t.Fatal("an ack to a request without WantIndex is not signed as before")
	}

	// An error ack never carries the index, asked or not.
	errAck := &protocol.ForwardApplyAck{Status: "error", Code: protocol.ForwardCodeApplyFailed, Error: "boom"}
	c.signForwardAck(errAck, "n3")
	if err := c.checkForwardAckIndex(errAck, "n3", true); err != nil {
		t.Fatalf("an error ack to a WantIndex request: %v", err)
	}
}

func forwardOnceWantIndex(t *testing.T, c *Coordinator, nodeID string, cmd *raft.Command, want bool) *protocol.ForwardApplyAck {
	t.Helper()
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	nonce, _ := security.GenerateNonce()
	ts := time.Now().Unix()
	req := &protocol.ForwardApplyRequest{
		CommandJSON: cmdJSON, NodeID: nodeID, Nonce: nonce, Timestamp: ts, WantIndex: want,
		HMAC: security.ComputeForwardHMAC(c.cfg.SharedSecret, nonce, nodeID, c.cfg.ClusterName, cmdJSON, ts),
	}
	server, client := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.handleForwardApply(server, req)
		server.Close()
	}()
	msg, err := protocol.ReceiveMessage(client, 10*time.Second)
	client.Close()
	<-done
	if err != nil {
		t.Fatalf("receive ack: %v", err)
	}
	ack := msg.Payload.(*protocol.ForwardApplyAck)
	if err := c.checkForwardAckIndex(ack, nonce, want); err != nil {
		t.Fatalf("ack does not validate: %v", err)
	}
	return ack
}

// The leader reports the applied command's index when asked, and only then.
func TestHandleForwardApply_ReturnsIndexWhenAsked(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()

	ack := forwardOnceWantIndex(t, c, "writer-2", registerFileCommand(t, "testdb/cpu/2026/10/20/10/idx-a.parquet", "writer-2"), true)
	if ack.Status != "ok" || ack.Index == 0 {
		t.Fatalf("ack = %+v, want ok with an index", ack)
	}
	if done := c.raftNode.FSM().AppliedDone(); ack.Index > done {
		t.Fatalf("ack index %d is past the leader's applied %d", ack.Index, done)
	}
	ack2 := forwardOnceWantIndex(t, c, "writer-2", registerFileCommand(t, "testdb/cpu/2026/10/20/10/idx-b.parquet", "writer-2"), true)
	if ack2.Index <= ack.Index {
		t.Fatalf("second index %d not after first %d", ack2.Index, ack.Index)
	}
	if plain := forwardOnceWantIndex(t, c, "writer-2", registerFileCommand(t, "testdb/cpu/2026/10/20/10/idx-c.parquet", "writer-2"), false); plain.Index != 0 {
		t.Fatalf("index %d sent to a request that did not ask", plain.Index)
	}
}

// indexLeader answers every forwarded command with an ok ack carrying a fixed
// index, signed as the real leader signs it, and records whether the request
// asked for it.
func indexLeader(t *testing.T, index uint64) (addr string, asked *atomic.Int32, requests *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	asked, requests = &atomic.Int32{}, &atomic.Int32{}
	signer := &Coordinator{cfg: &config.ClusterConfig{SharedSecret: forwardFSMSecret, ClusterName: "test-cluster"}, logger: zerolog.Nop()}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
					if err != nil {
						return
					}
					req := msg.Payload.(*protocol.ForwardApplyRequest)
					requests.Add(1)
					ack := &protocol.ForwardApplyAck{Status: "ok"}
					if req.WantIndex {
						asked.Add(1)
						ack.Index = index
					}
					signer.signForwardAckIndex(ack, req.Nonce, req.WantIndex)
					_ = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgForwardApplyAck, Payload: ack}, 5*time.Second)
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), asked, requests
}

// followerRig: a coordinator whose Raft view names another node as leader,
// reachable at leaderAddr. Its own FSM is a single bootstrapped node, so its
// applied index is known and moves only when the test applies something.
func followerRig(t *testing.T, leaderAddr string, policiesOn bool) *Coordinator {
	t.Helper()
	rn := startRaftNode(t, "forward-leader", allocFreePort(t), true)
	t.Cleanup(func() { _ = rn.Stop() })
	if err := rn.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	// One applied command, so the FSM's applied index is past zero.
	if err := rn.AddNode(&raft.NodeInfo{ID: "forward-leader", Name: "forward-leader", Role: string(RoleWriter), ClusterName: "test-cluster", Address: leaderAddr, State: string(StateHealthy)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if policiesOn {
		raw, _ := json.Marshal(raft.EnableClusterPoliciesPayload{Nodes: []string{"forward-leader"}, Now: time.Now()})
		if err := rn.Apply(&raft.Command{Type: raft.CommandEnableClusterPolicies, Payload: raw}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	local := NewNode("forward-follower", "forward-follower", RoleWriter, "test-cluster")
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	leader := NewNode("forward-leader", "forward-leader", RoleWriter, "test-cluster")
	leader.Address = leaderAddr
	if err := reg.Register(leader); err != nil {
		t.Fatal(err)
	}
	return &Coordinator{
		cfg:       &config.ClusterConfig{SharedSecret: forwardFSMSecret, ClusterName: "test-cluster"},
		registry:  reg,
		raftNode:  rn,
		localNode: local,
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
}

// A follower's forwarded proposal asks for the index once cluster policies
// are on, and does not return before its own FSM reaches it. (The rig's Raft
// node is its own leader, so this drives the follower path the auth proposer
// takes when it is not the leader.)
func TestForwardAndWaitHere(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft node")
	}
	t.Run("waits for an index not yet applied here", func(t *testing.T) {
		addr, asked, _ := indexLeader(t, 1<<40)
		c := followerRig(t, addr, true)
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := c.forwardAndWaitHere(ctx, &raft.Command{Type: raft.CommandCreateToken}); err != nil {
			t.Fatalf("a committed command reported as failed: %v", err)
		}
		if asked.Load() != 1 {
			t.Fatal("the follower did not ask for the index with cluster policies on")
		}
		if el := time.Since(start); el < 350*time.Millisecond {
			t.Fatalf("returned after %v, before its FSM reached the index", el)
		}
	})
	t.Run("returns once the index is applied here", func(t *testing.T) {
		addr, asked, _ := indexLeader(t, 1)
		c := followerRig(t, addr, true)
		start := time.Now()
		if err := c.forwardAndWaitHere(context.Background(), &raft.Command{Type: raft.CommandCreateToken}); err != nil {
			t.Fatal(err)
		}
		if asked.Load() != 1 {
			t.Fatal("the follower did not ask for the index")
		}
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("waited %v for an index already applied", el)
		}
	})
	t.Run("does not ask before cluster policies are on", func(t *testing.T) {
		addr, asked, requests := indexLeader(t, 1<<40)
		c := followerRig(t, addr, false)
		start := time.Now()
		if err := c.forwardAndWaitHere(context.Background(), &raft.Command{Type: raft.CommandCreateToken}); err != nil {
			t.Fatal(err)
		}
		if requests.Load() != 1 || asked.Load() != 0 {
			t.Fatalf("requests %d asked %d: an older leader would not know the field", requests.Load(), asked.Load())
		}
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("waited %v without an index", el)
		}
	})
}

// waitAppliedHere returns at once for an applied index and for zero, and
// gives up at the context deadline otherwise.
func TestWaitAppliedHere(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft node")
	}
	c := followerRig(t, "127.0.0.1:1", false)
	done := c.raftNode.FSM().AppliedDone()
	if done == 0 {
		t.Fatal("precondition: the bootstrapped node applied nothing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.waitAppliedHere(ctx, done); err != nil {
		t.Fatalf("applied index: %v", err)
	}
	if err := c.waitAppliedHere(ctx, 0); err != nil {
		t.Fatalf("zero index: %v", err)
	}
	if err := c.waitAppliedHere(ctx, done+1000); err == nil {
		t.Fatal("returned for an index this node never applied")
	}
}

// Through the auth proposer on a real follower (a non-voter, so it never
// becomes leader): the proposal is forwarded, asks for the index, and waits
// for this node to apply it.
func TestAuthProposer_FollowerWaitsForItsOwnApply(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft cluster")
	}
	leaderRaft := startRaftNode(t, "forward-leader", allocFreePort(t), true)
	t.Cleanup(func() { _ = leaderRaft.Stop() })
	if err := leaderRaft.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	followerAddr := allocFreePort(t)
	followerRaft := startRaftNode(t, "forward-follower", followerAddr, false)
	t.Cleanup(func() { _ = followerRaft.Stop() })
	if err := leaderRaft.AddNonvoter("forward-follower", followerAddr, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(raft.EnableClusterPoliciesPayload{Now: time.Now()})
	if err := leaderRaft.Apply(&raft.Command{Type: raft.CommandEnableClusterPolicies, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return followerRaft.FSM().ClusterPoliciesEnabled() && followerRaft.LeaderID() == "forward-leader"
	}, "follower never applied the flag or learned the leader")

	addr, asked, _ := indexLeader(t, 1<<40)
	local := NewNode("forward-follower", "forward-follower", RoleWriter, "test-cluster")
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	leader := NewNode("forward-leader", "forward-leader", RoleWriter, "test-cluster")
	leader.Address = addr
	if err := reg.Register(leader); err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{
		cfg:      &config.ClusterConfig{SharedSecret: forwardFSMSecret, ClusterName: "test-cluster"},
		registry: reg, raftNode: followerRaft, localNode: local, logger: zerolog.Nop(), ctx: context.Background(),
	}
	start := time.Now()
	if err := NewCoordinatorAuthProposer(c).Propose(context.Background(), uint8(raft.CommandCreateToken), []byte(`{}`), 400*time.Millisecond); err != nil {
		t.Fatalf("a committed command reported as failed: %v", err)
	}
	if asked.Load() != 1 {
		t.Fatal("the auth proposer did not ask for the index")
	}
	if el := time.Since(start); el < 350*time.Millisecond {
		t.Fatalf("returned after %v, before its FSM reached the index", el)
	}
}
