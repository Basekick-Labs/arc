package cluster

// A cluster-state removal naming this node must not evict the local entry from
// this node's own registry: every local lookup expects it to be there, and a
// snapshot restore that predates our join would otherwise trigger it (#847).

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

func TestOnRaftNodeRemoved_KeepsTheLocalNode(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	peer := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := reg.Register(peer); err != nil {
		t.Fatalf("register peer: %v", err)
	}
	c := &Coordinator{registry: reg, localNode: local, logger: zerolog.Nop()}

	c.onRaftNodeRemoved("writer-1")
	if _, ok := reg.Get("writer-1"); !ok {
		t.Error("the local node was evicted from its own registry")
	}

	c.onRaftNodeRemoved("writer-2")
	if _, ok := reg.Get("writer-2"); ok {
		t.Error("a peer removal was ignored")
	}
}

// The behaviour the issue is actually about: a restore on a coordinator-wired
// FSM must prune the coordinator's registry, not just the FSM's own table.
func TestRestore_PrunesTheCoordinatorRegistry(t *testing.T) {
	local := NewNode("writer-1", "writer-1", RoleWriter, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	fsm := raft.NewClusterFSM(zerolog.Nop())
	c := &Coordinator{registry: reg, localNode: local, logger: zerolog.Nop(), raftFSM: fsm}
	fsm.SetCallbacks(
		func(n *raft.NodeInfo) { c.onRaftNodeAdded(n) },
		func(id string) { c.onRaftNodeRemoved(id) },
		func(n *raft.NodeInfo) { c.onRaftNodeUpdated(n) },
	)

	restoreCoordinatorFSM(t, fsm, map[string]*raft.NodeInfo{
		"writer-1": {ID: "writer-1", Role: string(RoleWriter), Address: "127.0.0.1:1", State: string(StateHealthy)},
		"writer-2": {ID: "writer-2", Role: string(RoleWriter), Address: "127.0.0.1:2", State: string(StateHealthy)},
	})
	if _, ok := reg.Get("writer-2"); !ok {
		t.Fatal("the restore did not add writer-2 to the registry")
	}

	// A later snapshot has dropped writer-2.
	restoreCoordinatorFSM(t, fsm, map[string]*raft.NodeInfo{
		"writer-1": {ID: "writer-1", Role: string(RoleWriter), Address: "127.0.0.1:1", State: string(StateHealthy)},
	})
	if _, ok := reg.Get("writer-2"); ok {
		t.Error("a node the snapshot dropped is still in the registry, so it stays health-checked and offered as a peer")
	}
	if _, ok := reg.Get("writer-1"); !ok {
		t.Error("the surviving node was pruned")
	}
}

func restoreCoordinatorFSM(t *testing.T, fsm *raft.ClusterFSM, nodes map[string]*raft.NodeInfo) {
	t.Helper()
	js, err := json.Marshal(raft.FSMSnapshot{Nodes: nodes})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(js))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
}
