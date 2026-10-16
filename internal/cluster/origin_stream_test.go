package cluster

// #1207 S3: the coordinator side of the origin stream — who may open it,
// whom a standby follows, and the trim guard.

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

const osSecret = "origin-secret"

// originRig: a single-voter Raft holding writer-1 (primary), writer-2,
// reader-1; the coordinator is localID.
func originRig(t *testing.T, localID string, role NodeRole) *Coordinator {
	t.Helper()
	raftNode := startRaftNode(t, "raft-1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	if err := raftNode.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	local := NewNode(localID, localID, role, "test-cluster")
	local.UpdateState(StateHealthy)
	reg := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	for _, n := range []struct {
		id   string
		role NodeRole
	}{{"writer-1", RoleWriter}, {"writer-2", RoleWriter}, {"reader-1", RoleReader}} {
		if err := raftNode.AddNode(&raft.NodeInfo{ID: n.id, Name: n.id, Role: string(n.role), ClusterName: "test-cluster", Address: n.id + ":9100", State: string(StateHealthy)}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := raftNode.PromoteWriter("writer-1", "", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return raftNode.FSM().GetPrimaryWriterID() == "writer-1" })
	c := &Coordinator{
		cfg:        &config.ClusterConfig{SharedSecret: osSecret, ClusterName: "test-cluster", NodeID: localID},
		registry:   reg,
		localNode:  local,
		raftNode:   raftNode,
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
		logger:     zerolog.Nop(),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)
	return c
}

func waitFSM(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("FSM condition not reached")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func withServer(t *testing.T, c *Coordinator) *originstream.Server {
	t.Helper()
	srv := originstream.NewServer(0, func() *originstream.Floors { return nil }, zerolog.Nop())
	srv.Start()
	t.Cleanup(srv.Stop)
	w := newTestWALWriter(t)
	c.originStream = &originStream{server: srv, wal: w}
	return srv
}

// hello runs one hello against the coordinator and returns the answer.
func hello(t *testing.T, c *Coordinator, node, secret, nonce string) *protocol.OriginStreamHelloAck {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { server.Close(); client.Close() })
	ts := time.Now().Unix()
	h := &protocol.OriginStreamHello{Version: originstream.ProtocolVersion, NodeID: node, ClusterName: "test-cluster", Nonce: nonce, Timestamp: ts,
		HMAC: security.ComputeOriginStreamHelloHMAC(secret, originstream.ProtocolVersion, nonce, node, "test-cluster", ts)}
	go func() {
		if !c.handleOriginStreamHello(server, h) {
			server.Close()
		}
	}()
	msg, err := protocol.ReceiveMessage(client, 5*time.Second)
	if err != nil {
		t.Fatalf("no answer: %v", err)
	}
	ack := msg.Payload.(*protocol.OriginStreamHelloAck)
	if err := security.ValidateOriginStreamHelloAckHMAC(osSecret, nonce, ack.OriginNodeID, ack.Instance, ack.Error, ack.Timestamp, ack.HMAC, time.Minute); err != nil {
		t.Fatalf("answer not authenticated: %v", err)
	}
	return ack
}

func TestOriginStreamHelloAcceptsOnlyAStandbyWriterOnThePrimary(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	srv := withServer(t, c)

	if ack := hello(t, c, "writer-2", osSecret, "n1"); ack.Error != "" || ack.Instance == "" {
		t.Fatalf("standby writer refused: %+v", ack)
	}
	waitFSM(t, func() bool { return len(srv.Sessions()) == 1 })

	for _, tc := range []struct{ name, node, secret, nonce string }{
		{"a reader", "reader-1", osSecret, "n2"},
		{"an unknown node", "writer-9", osSecret, "n3"},
		{"a wrong secret", "writer-2", "other", "n4"},
		{"a replayed nonce", "writer-2", osSecret, "n1"},
		{"itself", "writer-1", osSecret, "n5"},
	} {
		if ack := hello(t, c, tc.node, tc.secret, tc.nonce); ack.Error == "" {
			t.Errorf("%s was accepted", tc.name)
		}
	}

	// Not the designated primary any more: refused.
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.designatedPrimaryWriterID() == "writer-2" })
	if ack := hello(t, c, "writer-2", osSecret, "n6"); ack.Error == "" {
		t.Fatal("a node that is not the designated primary accepted a standby")
	}
}

func TestOriginStreamHelloRefusedWithoutTheStream(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if ack := hello(t, c, "writer-2", osSecret, "n1"); ack.Error == "" {
		t.Fatal("accepted with no origin stream running")
	}
}

func TestOriginStreamTarget(t *testing.T) {
	standby := originRig(t, "writer-2", RoleWriter)
	if node, addr, ok := standby.originStreamTarget(); !ok || node != "writer-1" || addr != "writer-1:9100" {
		t.Fatalf("standby target = %q %q %v, want writer-1", node, addr, ok)
	}
	primary := originRig(t, "writer-1", RoleWriter)
	if _, _, ok := primary.originStreamTarget(); ok {
		t.Fatal("the primary targets itself")
	}
	reader := originRig(t, "reader-1", RoleReader)
	if _, _, ok := reader.originStreamTarget(); ok {
		t.Fatal("a reader receives the origin stream")
	}
}

// The trim guard with stream claims off: nothing claims from the copy, so
// only this FSM's progress bounds the trim; files do not matter (#1266).
func TestOriginStreamTrimReadyClaimsOff(t *testing.T) {
	c := originRig(t, "writer-2", RoleWriter)
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.storage = store
	pass := (&originStream{}).trimReady(c)
	ready := func(idx uint64) bool {
		return pass(context.Background(), []originstream.TrimRequest{{TrimKey: originstream.TrimKey{Origin: "writer-1", Instance: 1, Key: "db/cpu", AppliedIndex: idx}, CSeqs: [][2]uint64{{1, 5}}}})[0]
	}
	if err := c.raftNode.RegisterFile(raft.FileEntry{Path: "db/cpu/a.parquet", Database: "db", Measurement: "cpu", SizeBytes: 3, Tier: "hot", OriginNodeID: "writer-1", CreatedAt: time.Now(), SHA256: "x", PartitionTime: time.Now()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	idx := c.raftNode.FSM().AppliedDone()
	if !ready(idx) {
		t.Fatal("claims off: not ready with this FSM caught up")
	}
	if ready(idx + 1000) {
		t.Fatal("ready before this FSM applied the primary's index")
	}
}

// The trim guard with stream claims on: a request waits only for the files
// whose coverage holds its batches. An unpulled file of other batches, or an
// unpulled compaction output, does not hold it back (#1266).
func TestOriginStreamTrimReadyPerBatch(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	dir := t.TempDir()
	store, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.storage = store
	peer := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := c.registry.Register(peer); err != nil {
		t.Fatal(err)
	}
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x10, FreshStart: true})
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x11})
	hour := time.Now().Truncate(time.Hour)
	reg := func(path string, size int64, cov ...raft.InstanceCoverage) {
		t.Helper()
		propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{
			File: raft.FileEntry{Path: path, SHA256: path, SizeBytes: size, Database: "db", Measurement: "cpu",
				PartitionTime: hour, OriginNodeID: "writer-2", CreatedAt: time.Now()}, Cov2: cov})
	}
	reg("db/cpu/old.parquet", 3, raft.InstanceCoverage{Instance: 0x10, Key: "db/cpu", Ranges: [][2]uint64{{1, 5}}})
	reg("db/cpu/new.parquet", 4, raft.InstanceCoverage{Instance: 0x10, Key: "db/cpu", Ranges: [][2]uint64{{6, 9}}})
	reg("db/cpu/compacted.parquet", 5)
	idx := c.raftNode.FSM().AppliedDone()
	st := &originStream{}
	pass := st.trimReady(c)
	ask := func(inst uint64, lo, hi uint64) bool {
		return pass(context.Background(), []originstream.TrimRequest{{TrimKey: originstream.TrimKey{Origin: "writer-2", Instance: inst, Key: "db/cpu", AppliedIndex: idx}, CSeqs: [][2]uint64{{lo, hi}}}})[0]
	}
	if ask(0x10, 1, 5) {
		t.Fatal("ready while the file holding the batches is not here")
	}
	// A path verified at another size is not taken from the cache.
	st.trimPresent = map[string]int64{"db/cpu/old.parquet": 9}
	if ask(0x10, 1, 5) {
		t.Fatal("ready from a cache entry at another size")
	}
	if err := store.Write(context.Background(), "db/cpu/old.parquet", []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if ask(0x10, 1, 5) {
		t.Fatal("ready with the file here at another size")
	}
	if err := store.Write(context.Background(), "db/cpu/old.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if !ask(0x10, 1, 5) {
		t.Fatal("not ready: an unpulled file of other batches or a compaction output held it back")
	}
	if ask(0x10, 5, 6) || ask(0x10, 6, 9) {
		t.Fatal("ready while a file holding some of the batches is not here")
	}
	if !ask(0x11, 1, 9) {
		t.Fatal("an untracked instance is never claimed: its batches need no file")
	}
	// The order: an FSM behind the floors answers no before anything else,
	// even for an untracked instance.
	r := originstream.TrimRequest{TrimKey: originstream.TrimKey{Origin: "writer-2", Instance: 0x11, Key: "db/cpu", AppliedIndex: idx + 1000}, CSeqs: [][2]uint64{{1, 1}}}
	if pass(context.Background(), []originstream.TrimRequest{r})[0] {
		t.Fatal("ready before this FSM applied the primary's index")
	}
	// An origin this node does not hear from keeps everything.
	peer.UpdateState(StateUnhealthy)
	if ask(0x11, 1, 9) || ask(0x10, 1, 5) {
		t.Fatal("trimmed the records of an origin that is not heard from")
	}
}

func newTestWALWriter(t *testing.T) *wal.Writer {
	t.Helper()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// Batches a standby kept while stream claims were off belong to an instance
// that, once the flag turns on, announces as not a fresh start: it is
// untracked and never claimed, so its batches still need no file (#1266).
func TestOriginStreamTrimReadyClaimsTurnOn(t *testing.T) {
	c, _ := takeoverOriginRig(t, false)
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.storage = store
	peer := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := c.registry.Register(peer); err != nil {
		t.Fatal(err)
	}
	pass := (&originStream{}).trimReady(c)
	ask := func() bool {
		idx := c.raftNode.FSM().AppliedDone()
		return pass(context.Background(), []originstream.TrimRequest{{TrimKey: originstream.TrimKey{Origin: "writer-2", Instance: 0x20, Key: "db/cpu", AppliedIndex: idx}, CSeqs: [][2]uint64{{1, 5}}}})[0]
	}
	if !ask() {
		t.Fatal("claims off: not ready")
	}
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
	c.maybeEnableStreamClaims()
	waitFSM(t, c.StreamClaimsEnabled)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x20})
	propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/late.parquet", SHA256: "x", SizeBytes: 3, Database: "db", Measurement: "cpu",
			PartitionTime: time.Now().Truncate(time.Hour), OriginNodeID: "writer-2", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: 0x20, Key: "db/cpu", Ranges: [][2]uint64{{1, 5}}}}})
	if !ask() {
		t.Fatal("an instance announced after the flag (not a fresh start) held its batches for a file")
	}
}

// The claimant drops a copy it no longer needs once the files holding the
// instance's rows of those keys are here; a compaction output does not hold
// it back.
func TestInstanceFilesLocal(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.storage = store
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x30, FreshStart: true})
	hour := time.Now().Truncate(time.Hour)
	reg := func(path string, cov ...raft.InstanceCoverage) {
		propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{
			File: raft.FileEntry{Path: path, SHA256: path, SizeBytes: 3, Database: "db", Measurement: "cpu",
				PartitionTime: hour, OriginNodeID: "writer-2", CreatedAt: time.Now()}, Cov2: cov})
	}
	reg("db/cpu/i30.parquet", raft.InstanceCoverage{Instance: 0x30, Key: "db/cpu", Ranges: [][2]uint64{{1, 5}}})
	reg("db/cpu/compacted.parquet")
	local := (&originStream{}).instanceFilesLocal(c)
	if local(context.Background(), 0x30, []string{"db/cpu"}) {
		t.Fatal("local without the instance's file")
	}
	if err := store.Write(context.Background(), "db/cpu/i30.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if !local(context.Background(), 0x30, []string{"db/cpu"}) {
		t.Fatal("a missing compaction output held the drop back")
	}
}
