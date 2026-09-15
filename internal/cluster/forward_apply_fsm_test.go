package cluster

// Leader-side resolution of a forwarding node after a snapshot restore
// (#807). A snapshot restore refills the FSM node table but, before #807,
// not the in-memory registry, and handleForwardApply consulted only the
// registry, so every forwarded write from a node that had not re-joined was
// rejected as unknown. The handler now resolves the node from the FSM node
// table first and falls back to the registry.

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

const forwardFSMSecret = "test-cluster-secret-32-bytes-long!"

// forwardOnce runs handleForwardApply over a pipe for a signed request from
// nodeID carrying cmd, and returns the ack. The handler writes the ack and
// never reads, so the client side receives concurrently.
func forwardOnce(t *testing.T, c *Coordinator, nodeID string, cmd *raft.Command) *protocol.ForwardApplyAck {
	t.Helper()
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	nonce, err := security.GenerateNonce()
	if err != nil {
		t.Fatalf("nonce: %v", err)
	}
	ts := time.Now().Unix()
	req := &protocol.ForwardApplyRequest{
		CommandJSON: cmdJSON,
		NodeID:      nodeID,
		Nonce:       nonce,
		Timestamp:   ts,
		HMAC:        security.ComputeForwardHMAC(c.cfg.SharedSecret, nonce, nodeID, c.cfg.ClusterName, cmdJSON, ts),
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
	if msg.Type != protocol.MsgForwardApplyAck {
		t.Fatalf("ack type = %v", msg.Type)
	}
	ack, ok := msg.Payload.(*protocol.ForwardApplyAck)
	if !ok {
		t.Fatalf("ack payload = %T", msg.Payload)
	}
	return ack
}

func registerFileCommand(t *testing.T, path, origin string) *raft.Command {
	t.Helper()
	entry := makeFileEntry(path, []byte("x"), origin)
	entry.CreatedAt = time.Now()
	payload, err := json.Marshal(raft.RegisterFilePayload{File: entry})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return &raft.Command{Type: raft.CommandRegisterFile, Payload: payload}
}

// The leader's registry holds only the leader itself, as after a restart from
// a snapshot with no re-join; the peers exist only in the FSM node table.
func TestHandleForwardApply_RegistryMiss_FallsBackToFSMNodeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	raftNode := startRaftNode(t, "leader-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	// No FSM callbacks are installed, so AddNode fills the FSM node table
	// and leaves the registry untouched: the post-restore state.
	for _, n := range []*raft.NodeInfo{
		{ID: "writer-B", Name: "writer-B", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "127.0.0.1:1", State: string(StateHealthy)},
		{ID: "reader-C", Name: "reader-C", Role: string(RoleReader), ClusterName: "test-cluster", Address: "127.0.0.1:2", State: string(StateHealthy)},
	} {
		if err := raftNode.AddNode(n, 5*time.Second); err != nil {
			t.Fatalf("AddNode %s: %v", n.ID, err)
		}
	}
	local := NewNode("leader-1", "leader-1", RoleWriter, "test-cluster")
	registry := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	c := &Coordinator{
		cfg:        &config.ClusterConfig{SharedSecret: forwardFSMSecret, ClusterName: "test-cluster"},
		registry:   registry,
		raftNode:   raftNode,
		raftFSM:    raftNode.FSM(),
		localNode:  local,
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
		logger:     zerolog.Nop(),
		ctx:        context.Background(),
	}
	if _, inRegistry := registry.Get("writer-B"); inRegistry {
		t.Fatal("precondition: writer-B must not be in the registry")
	}

	t.Run("writer known only to the FSM applies", func(t *testing.T) {
		const path = "testdb/cpu/2026/09/15/10/fwd-writer.parquet"
		ack := forwardOnce(t, c, "writer-B", registerFileCommand(t, path, "writer-B"))
		if ack.Status != "ok" {
			t.Fatalf("ack = %+v; want ok (before #807: unknown node)", ack)
		}
		if _, ok := raftNode.FSM().GetFile(path); !ok {
			t.Fatalf("file %s not in the manifest after an ok ack", path)
		}
	})
	t.Run("reader known only to the FSM is gated by its FSM role", func(t *testing.T) {
		ack := forwardOnce(t, c, "reader-C", registerFileCommand(t, "testdb/cpu/2026/09/15/10/fwd-reader.parquet", "reader-C"))
		if ack.Status != "error" || ack.Code != protocol.ForwardCodeAuth || !strings.Contains(ack.Error, "unauthorized role") {
			t.Fatalf("ack = %+v; want auth error 'unauthorized role'", ack)
		}
	})
	t.Run("node in neither store is still rejected", func(t *testing.T) {
		ack := forwardOnce(t, c, "ghost-D", registerFileCommand(t, "testdb/cpu/2026/09/15/10/fwd-ghost.parquet", "ghost-D"))
		if ack.Status != "error" || ack.Code != protocol.ForwardCodeAuth || !strings.Contains(ack.Error, "unknown node") {
			t.Fatalf("ack = %+v; want auth error 'unknown node'", ack)
		}
	})
	t.Run("registry-only node still applies (defence-in-depth fallback)", func(t *testing.T) {
		writerE := NewNode("writer-E", "writer-E", RoleWriter, "test-cluster")
		if err := registry.Register(writerE); err != nil {
			t.Fatalf("register: %v", err)
		}
		const path = "testdb/cpu/2026/09/15/10/fwd-registry.parquet"
		ack := forwardOnce(t, c, "writer-E", registerFileCommand(t, path, "writer-E"))
		if ack.Status != "ok" {
			t.Fatalf("ack = %+v; want ok", ack)
		}
	})
}

func TestForwardingPeer_FSMFirstAndNilSafe(t *testing.T) {
	// Nothing configured: a miss, not a panic.
	empty := &Coordinator{logger: zerolog.Nop()}
	if node, ok := empty.forwardingPeer("x"); ok || node != nil {
		t.Fatalf("empty coordinator: got (%v, %v), want miss", node, ok)
	}

	local := NewNode("local", "local", RoleWriter, "c")
	registry := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	fsm := raft.NewClusterFSM(zerolog.Nop())
	c := &Coordinator{logger: zerolog.Nop(), registry: registry, raftFSM: fsm}

	// Registry only (the no-Raft join path).
	if err := registry.Register(NewNode("reg-only", "reg-only", RoleCompactor, "c")); err != nil {
		t.Fatalf("register: %v", err)
	}
	if node, ok := c.forwardingPeer("reg-only"); !ok || node.Role != RoleCompactor {
		t.Fatalf("registry-only: got (%v, %v)", node, ok)
	}

	// Both stores, disagreeing: the FSM is the source of record.
	if err := registry.Register(NewNode("both", "both", RoleWriter, "c")); err != nil {
		t.Fatalf("register: %v", err)
	}
	payload, _ := json.Marshal(raft.AddNodePayload{Node: raft.NodeInfo{ID: "both", Name: "both", Role: string(RoleReader), ClusterName: "c", Address: "127.0.0.1:9", WriterState: "standby"}})
	cmd, _ := json.Marshal(raft.Command{Type: raft.CommandAddNode, Payload: payload})
	if res := fsm.Apply(&hraft.Log{Index: 1, Data: cmd}); res != nil {
		t.Fatalf("apply AddNode: %v", res)
	}
	node, ok := c.forwardingPeer("both")
	if !ok || node.Role != RoleReader {
		t.Fatalf("both stores: got (%+v, %v), want the FSM's reader role", node, ok)
	}
	if node.Address != "127.0.0.1:9" || node.GetWriterState() != WriterStateStandby {
		t.Fatalf("FSM-built node lost fields: %+v", node)
	}
}
