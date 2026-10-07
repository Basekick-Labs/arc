package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// startRegistrarLeader is the smallest coordinator a registrar needs: a
// single bootstrapped Raft node that is the leader, so RegisterFileInManifest
// applies locally.
func startRegistrarLeader(t *testing.T) (*Coordinator, *raft.Node) {
	t.Helper()
	addrs := allocFreePorts(t, 1)
	n := startRaftNodeInDir(t, "writer-A", addrs[0], t.TempDir(), true)
	t.Cleanup(func() { _ = n.Stop() })
	if err := n.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	c := &Coordinator{
		cfg:       &config.ClusterConfig{},
		raftNode:  n,
		localNode: NewNode("writer-A", "writer-A", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
	return c, n
}

// TestCoordinatorFileRegistrar_StopAppliesEveryQueuedFile is the shutdown
// contract: every file enqueued before Stop returns is in the manifest when
// it does, including the one the worker was applying at the instant Stop was
// called and a backlog larger than one drain batch. Before this, Stop
// cancelled the worker's context (losing the in-flight apply) and drained the
// rest one Raft entry at a time against a 2 s clock.
func TestCoordinatorFileRegistrar_StopAppliesEveryQueuedFile(t *testing.T) {
	c, n := startRegistrarLeader(t)
	r := NewCoordinatorFileRegistrar(c, zerolog.Nop())
	r.Start(context.Background())

	// More than registrarDrainBatch, and far more than the worker applies
	// before Stop is called on the next line.
	const files = 2*registrarDrainBatch + 500
	for i := 0; i < files; i++ {
		r.RegisterFile("db", "m", fmt.Sprintf("db/m/2026/10/04/00/%05d.parquet", i), time.Now(), 1, "")
	}
	r.Stop()

	stats := r.Stats()
	if stats["dropped"] != 0 {
		t.Fatalf("dropped = %d, want 0 (queue capacity is the test's precondition)", stats["dropped"])
	}
	if stats["apply_errs"] != 0 {
		t.Fatalf("apply_errs = %d, want 0", stats["apply_errs"])
	}
	if stats["applied"] != files {
		t.Fatalf("applied = %d, want %d", stats["applied"], files)
	}
	if got := n.FSM().FileCount(); got != files {
		t.Fatalf("manifest holds %d files after Stop, want %d", got, files)
	}

	// A second Stop is a no-op, not a double close.
	r.Stop()
}

// TestCoordinatorFileRegistrar_StopBeforeStartIsNoop pins the main.go path
// where the registrar's shutdown step is registered unconditionally.
func TestCoordinatorFileRegistrar_StopBeforeStartIsNoop(t *testing.T) {
	c, _ := startRegistrarLeader(t)
	r := NewCoordinatorFileRegistrar(c, zerolog.Nop())
	r.Stop()
	if got := r.Stats()["applied"]; got != 0 {
		t.Fatalf("applied = %d, want 0", got)
	}
}

// TestCoordinatorFileRegistrar_DrainChunksFitTheForwardFrame pins the byte
// cap on drain batches. On a non-leader a batch travels to the leader as a
// ForwardApplyRequest whose CommandJSON wraps the Command whose Payload wraps
// the BatchFileOpsPayload whose ops wrap each file — three []byte fields, so
// three base64 encodings — inside a protocol frame of at most
// protocol.MaxMessageSize. With realistic long names a single 1000-op batch is
// over that cap; every chunk drainChunks produces must be under it.
func TestCoordinatorFileRegistrar_DrainChunksFitTheForwardFrame(t *testing.T) {
	const nodeID = "arc-writer-0.arc-writer.arc.svc.cluster.local"
	c := &Coordinator{localNode: NewNode(nodeID, nodeID, RoleWriter, "test-cluster"), logger: zerolog.Nop()}
	r := NewCoordinatorFileRegistrar(c, zerolog.Nop())

	const files = registrarDrainBatch
	pending := make([]fileRegistration, 0, files)
	for i := 0; i < files; i++ {
		measurement := fmt.Sprintf("page_view_events_with_tenant_and_region_suffix_%03d", i%300)
		pending = append(pending, fileRegistration{
			database:      "product_analytics",
			measurement:   measurement,
			path:          fmt.Sprintf("product_analytics/%s/2026/10/04/%02d/%s_20261004_171634_%09d.parquet", measurement, i%24, measurement, i),
			partitionTime: time.Now(),
			sizeBytes:     1 << 20,
			sha256:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		})
	}

	frameSize := func(ops []raft.BatchFileOp) int {
		payload, err := json.Marshal(raft.BatchFileOpsPayload{Ops: ops})
		if err != nil {
			t.Fatal(err)
		}
		cmdJSON, err := json.Marshal(&raft.Command{Type: raft.CommandBatchFileOps, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		err = protocol.NewEncoder(&buf).Encode(&protocol.Message{Type: protocol.MsgForwardApply, Payload: &protocol.ForwardApplyRequest{
			CommandJSON: cmdJSON,
			NodeID:      nodeID,
			Nonce:       "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Timestamp:   time.Now().Unix(),
			HMAC:        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		}})
		if err != nil {
			// Encode refuses an oversized frame before writing a byte.
			return protocol.MaxMessageSize + 1
		}
		return buf.Len()
	}

	// Precondition: one batch of registrarDrainBatch such files does not fit,
	// otherwise this test proves nothing about the byte cap.
	whole := make([]raft.BatchFileOp, 0, files)
	for _, reg := range pending {
		payload, err := json.Marshal(raft.RegisterFilePayload{File: r.entry(reg)})
		if err != nil {
			t.Fatal(err)
		}
		whole = append(whole, raft.BatchFileOp{Type: raft.CommandRegisterFile, Payload: payload})
	}
	if size := frameSize(whole); size <= protocol.MaxMessageSize {
		t.Fatalf("a %d-op batch encodes to %d bytes, under the %d cap; make the names longer", files, size, protocol.MaxMessageSize)
	}

	chunks, invalid := r.drainChunks(pending)
	if invalid != 0 {
		t.Fatalf("invalid = %d, want 0", invalid)
	}
	total := 0
	for i, ops := range chunks {
		total += len(ops)
		if size := frameSize(ops); size > protocol.MaxMessageSize {
			t.Fatalf("chunk %d (%d ops) encodes to %d bytes, over the %d cap", i, len(ops), size, protocol.MaxMessageSize)
		}
	}
	if total != files {
		t.Fatalf("chunks hold %d ops, want %d", total, files)
	}
	if len(chunks) < 2 {
		t.Fatalf("got %d chunk(s); the byte cap did not split a batch the frame cap refuses", len(chunks))
	}
}

// TestCoordinatorFileRegistrar_DrainSkipsAPathTheManifestRefuses pins that one
// refused path costs one file, not the whole batch: the FSM refuses a batch
// outright for a single bad path, so drainChunks applies the FSM's own check
// per entry and leaves the bad one out. Exercised on drainChunks directly —
// through Stop the worker might apply the entry per-file first and the
// outcome would not depend on this check.
func TestCoordinatorFileRegistrar_DrainSkipsAPathTheManifestRefuses(t *testing.T) {
	c := &Coordinator{localNode: NewNode("writer-A", "writer-A", RoleWriter, "test-cluster"), logger: zerolog.Nop()}
	r := NewCoordinatorFileRegistrar(c, zerolog.Nop())
	pending := []fileRegistration{
		{database: "db", measurement: "m", path: "db/m/2026/10/04/00/good-1.parquet", partitionTime: time.Now(), sizeBytes: 1},
		{database: "db", measurement: "m", path: "/etc/passwd", partitionTime: time.Now(), sizeBytes: 1},
		{database: "db", measurement: "m", path: "db/m/2026/10/04/00/good-2.parquet", partitionTime: time.Now(), sizeBytes: 1},
	}
	chunks, invalid := r.drainChunks(pending)
	if invalid != 1 {
		t.Fatalf("invalid = %d, want 1 (the absolute path)", invalid)
	}
	if len(chunks) != 1 || len(chunks[0]) != 2 {
		t.Fatalf("chunks = %d with %v ops, want one chunk of the 2 valid files", len(chunks), func() []int {
			n := make([]int, len(chunks))
			for i, ch := range chunks {
				n[i] = len(ch)
			}
			return n
		}())
	}
}
