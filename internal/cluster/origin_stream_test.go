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

// The trim guard: this FSM must have applied what the primary had, and
// every current hot file of the measurement must be here at its size —
// a compaction output included.
func TestOriginStreamTrimReady(t *testing.T) {
	c := originRig(t, "writer-2", RoleWriter)
	dir := t.TempDir()
	store, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.storage = store
	st := &originStream{present: map[string]int64{}}
	pass := st.trimReady(c)
	ready := func(origin, key string, idx uint64) bool {
		k := originstream.TrimKey{Origin: origin, Key: key, AppliedIndex: idx}
		return pass(context.Background(), []originstream.TrimKey{k})[k]
	}
	reg := func(path string, size int64) {
		t.Helper()
		if err := c.raftNode.RegisterFile(raft.FileEntry{Path: path, Database: "db", Measurement: "cpu", SizeBytes: size, Tier: "hot", OriginNodeID: "writer-1", CreatedAt: time.Now(), SHA256: "x", PartitionTime: time.Now()}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	reg("db/cpu/2026/10/11/10/a.parquet", 3)
	idx := c.raftNode.FSM().AppliedDone()
	if ready("writer-1", "db/cpu", idx) {
		t.Fatal("ready while the file is not here")
	}
	if err := store.Write(context.Background(), "db/cpu/2026/10/11/10/a.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if !ready("writer-1", "db/cpu", idx) {
		t.Fatal("not ready with every file here")
	}
	// Every file this FSM knows is here, but it has not applied what the
	// primary had: a registration it has not seen could be missing.
	if ready("writer-1", "db/cpu", idx+1000) {
		t.Fatal("ready before this FSM applied the primary's index")
	}
	// A compaction output registered later is current: required too.
	reg("db/cpu/2026/10/11/10/compacted.parquet", 5)
	if ready("writer-1", "db/cpu", idx) {
		t.Fatal("ready while a current file (a compaction output) is missing")
	}
	// Another measurement does not matter.
	if err := c.raftNode.RegisterFile(raft.FileEntry{Path: "db/mem/x.parquet", Database: "db", Measurement: "mem", SizeBytes: 9, Tier: "hot", CreatedAt: time.Now(), SHA256: "x", PartitionTime: time.Now()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(context.Background(), "db/cpu/2026/10/11/10/compacted.parquet", []byte("12345")); err != nil {
		t.Fatal(err)
	}
	if !ready("writer-1", "db/cpu", idx) {
		t.Fatal("another measurement's missing file blocked this one")
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
