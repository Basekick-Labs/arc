package cluster

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// #1829: a leader keeps its own record in cluster state current for the life
// of the coordinator, not only when it wins an election within 30 s of start.

func newUnstartedRaftNode(t *testing.T, id, bindAddr, dir string) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(&raft.NodeConfig{
		NodeID:             id,
		SharedSecret:       "test-cluster-secret-32-bytes-long!",
		DataDir:            dir,
		BindAddr:           bindAddr,
		AdvertiseAddr:      bindAddr,
		Bootstrap:          true,
		ElectionTimeout:    1 * time.Second,
		HeartbeatTimeout:   500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		Logger:             zerolog.Nop(),
	}, raft.NewClusterFSM(zerolog.Nop()))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func selfRecordCoordinator(t *testing.T, id string, rn *raft.Node, apiAddr, version string) *Coordinator {
	t.Helper()
	c := newPauseForwardingCoordinator(t, id, RoleWriter, rn)
	c.cfg.AdvertiseAddr = "127.0.0.1:9100"
	c.localNode.SetAddresses(c.cfg.AdvertiseAddr, apiAddr)
	c.localNode.SetVersion(version)
	c.stopCh = make(chan struct{})
	return c
}

func waitForSelfRecord(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The reported case: the previous process left a record with its own version
// and API address; this process becomes leader only after its loop has been
// running for a while. The record ends up describing this process.
func TestLeaderElectedLateRefreshesItsRecord1829(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	old := selfRecordInterval
	selfRecordInterval = 20 * time.Millisecond
	t.Cleanup(func() { selfRecordInterval = old })

	dir, addr := t.TempDir(), allocFreePort(t)

	// Previous process: leader, record with its version and address.
	rn := newUnstartedRaftNode(t, "w-1", addr, dir)
	if err := rn.Start(); err != nil {
		t.Fatal(err)
	}
	if err := rn.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	prev := selfRecordCoordinator(t, "w-1", rn, "10.0.0.5:8000", "26.09.4")
	if err := rn.AddNode(prev.selfNodeInfo(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := rn.Stop(); err != nil {
		t.Fatal(err)
	}

	// This process: new version and address; the loop runs before Raft has
	// even started, so leadership comes long after the loop began.
	rn2 := newUnstartedRaftNode(t, "w-1", addr, dir)
	t.Cleanup(func() { _ = rn2.Stop() })
	c := selfRecordCoordinator(t, "w-1", rn2, "10.0.0.9:8000", "27.01.1")
	go c.maintainSelfRecord()
	t.Cleanup(func() { close(c.stopCh) })
	time.Sleep(300 * time.Millisecond)
	if err := rn2.Start(); err != nil {
		t.Fatal(err)
	}
	waitForSelfRecord(t, "the record to describe this process", func() bool {
		n, ok := rn2.FSM().GetNode("w-1")
		return ok && n.APIAddress == "10.0.0.9:8000" && n.Version == "27.01.1"
	})
}

func TestSyncSelfRecord1829(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	rn := startSingleNodeRaft(t, "w-1")
	c := selfRecordCoordinator(t, "w-1", rn, "10.0.0.9:8000", "27.01.1")
	var s selfRecordState
	wrote := func(f func()) bool {
		before := rn.LastIndex()
		f()
		return rn.LastIndex() != before
	}

	// First write as leader: unconditional, even with the record absent.
	if !wrote(func() { c.syncSelfRecord(&s) }) || !s.announced {
		t.Fatal("first check as leader did not write the record")
	}
	n, ok := rn.FSM().GetNode("w-1")
	if !ok || n.APIAddress != "10.0.0.9:8000" || n.Version != "27.01.1" || n.WriterState != "" {
		t.Fatalf("record after first write: %+v", n)
	}
	// Identical: no Raft entry.
	if wrote(func() { c.syncSelfRecord(&s) }) {
		t.Fatal("an identical record was rewritten")
	}
	// Only CoreCount differs: not worth an entry.
	stale := c.selfNodeInfo()
	stale.CoreCount = 999
	if err := rn.AddNode(stale, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if wrote(func() { c.syncSelfRecord(&s) }) {
		t.Fatal("a core-count-only difference was rewritten")
	}
	// Stale address and version (another process's record): rewritten.
	stale = c.selfNodeInfo()
	stale.APIAddress, stale.Version = "10.0.0.5:8000", "26.09.4"
	if err := rn.AddNode(stale, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if !wrote(func() { c.syncSelfRecord(&s) }) {
		t.Fatal("a stale record was not rewritten")
	}
	if n, _ := rn.FSM().GetNode("w-1"); n.APIAddress != "10.0.0.9:8000" || n.Version != "27.01.1" {
		t.Fatalf("record after refresh: %+v", n)
	}
	if s.writtenDiff != "api_address,version" {
		t.Fatalf("difference recorded as %q", s.writtenDiff)
	}
	// The same difference back right after a write fixed it: not rewritten
	// every second.
	if err := rn.AddNode(stale, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if wrote(func() { c.syncSelfRecord(&s) }) {
		t.Fatal("a difference that did not stay fixed was rewritten at once")
	}
	s.writtenAt = time.Now().Add(-selfRecordMaxBackoff)
	if !wrote(func() { c.syncSelfRecord(&s) }) {
		t.Fatal("the difference was not retried after the backoff")
	}
	// Absent after the first write: not re-added.
	if err := rn.RemoveNode("w-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if wrote(func() { c.syncSelfRecord(&s) }) {
		t.Fatal("a removed record was re-added by a steady-state check")
	}
	// Leaving: no write, even on a fresh process's first check.
	c.leaving.Store(true)
	if wrote(func() { c.syncSelfRecord(&selfRecordState{}) }) {
		t.Fatal("a leaving node wrote its record")
	}
}

// A node that is not leader writes nothing.
func TestSyncSelfRecordNotLeader1829(t *testing.T) {
	rn := newUnstartedRaftNode(t, "w-1", allocFreePort(t), t.TempDir())
	c := selfRecordCoordinator(t, "w-1", rn, "10.0.0.9:8000", "27.01.1")
	var s selfRecordState
	if d := c.syncSelfRecord(&s); d != selfRecordInterval || s.announced {
		t.Fatalf("not leader: wait %v, announced %v", d, s.announced)
	}
}

func TestSelfRecordDiff1829(t *testing.T) {
	base := &raft.NodeInfo{ID: "w", Name: "w", Role: "writer", ClusterName: "c", Address: "a:1", APIAddress: "a:2", Version: "v", Capabilities: []string{"x", "y"}}
	same := *base
	same.Capabilities = []string{"y", "x"}
	same.State, same.WriterState, same.CoreCount = "unhealthy", "primary", 64
	if d := selfRecordDiff(&same, base); d != "" {
		t.Fatalf("order, state, writer state and cores counted as a difference: %q", d)
	}
	all := raft.NodeInfo{ID: "w", Name: "n", Role: "reader", ClusterName: "d", Address: "b:1", APIAddress: "b:2", Version: "u", Capabilities: []string{"x"}}
	if d := selfRecordDiff(&all, base); d != "name,role,cluster_name,address,api_address,version,capabilities" {
		t.Fatalf("diff = %q", d)
	}
}

// The join response carries no version: a peer the FSM knows keeps its own,
// and the joiner shows its own current version for itself, not the one its
// FSM still holds from its previous process.
func TestJoinResponseKeepsKnownVersions1829(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a real Raft leader")
	}
	rn := startSingleNodeRaft(t, "r-1")
	c := selfRecordCoordinator(t, "r-1", rn, "10.0.0.7:8000", "27.01.1")
	c.cfg.SharedSecret = ""
	if err := rn.AddNode(&raft.NodeInfo{ID: "w-1", Name: "w-1", Role: string(RoleWriter), ClusterName: "c", Address: "127.0.0.1:1", State: string(StateHealthy), Version: "27.01.1-w"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// This node's own record from its previous process.
	if err := rn.AddNode(&raft.NodeInfo{ID: "r-1", Name: "r-1", Role: string(RoleReader), ClusterName: "c", Address: "127.0.0.1:2", State: string(StateHealthy), Version: "26.09.4"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	resp := &protocol.JoinResponse{Success: true, LeaderID: "w-1", Nodes: []protocol.NodeInfo{
		{ID: "w-1", Name: "w-1", Role: string(RoleWriter), State: string(StateHealthy), CoordAddr: "127.0.0.1:1"},
		{ID: "r-1", Name: "r-1", Role: string(RoleReader), State: string(StateHealthy), CoordAddr: "127.0.0.1:2"},
	}}
	if err := c.handleJoinResponse(protocol.NewJoinResponse(resp), "seed", ""); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"w-1": "27.01.1-w", "r-1": "27.01.1"} {
		if n, ok := c.registry.Get(id); !ok || n.Version != want {
			t.Fatalf("%s version after the join response: %+v", id, n)
		}
	}
}
