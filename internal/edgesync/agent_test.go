package edgesync

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type agentRig struct {
	agent     *Agent
	ledger    *Ledger
	transport *MemoryTransport
	backend   storage.Backend
}

func newAgentRig(t *testing.T) *agentRig {
	t.Helper()

	dir, err := os.MkdirTemp("", "agent-storage-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	ledger := setupTestLedger(t)
	transport := NewMemoryTransport()

	agent, err := NewAgent(AgentConfig{
		Ledger:    ledger,
		Transport: transport,
		Backend:   backend,
		HubID:     DefaultHubID,
		SpokeID:   "rocket-01",
		Logger:    zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}

	return &agentRig{agent: agent, ledger: ledger, transport: transport, backend: backend}
}

// writeFile puts a Parquet file into the spoke's local storage.
func (r *agentRig) writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := r.backend.Write(context.Background(), path, content); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

const agentPath = "metrics/cpu/2026/08/07/14/cpu_001.parquet"

func TestAgent_RunSyncsDiscoveredFiles(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("parquet payload one")
	rig.writeFile(t, agentPath, content)

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Discovered != 1 {
		t.Errorf("discovered = %d, want 1", res.Discovered)
	}
	if res.Sent != 1 {
		t.Errorf("sent = %d, want 1", res.Sent)
	}
	if res.BytesSent != int64(len(content)) {
		t.Errorf("bytes = %d, want %d", res.BytesSent, len(content))
	}

	// The hub must hold the exact bytes, under the digest discovery computed.
	sha, ok := rig.transport.Has(DefaultHubID, agentPath)
	if !ok {
		t.Fatal("the hub does not have the file")
	}
	if sha != sha256Hex(content) {
		t.Error("the hub holds a different digest than the file's content")
	}

	// And the ledger must reflect it, or the next pass re-sends.
	entry, err := rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if entry.State != StateSynced {
		t.Errorf("state = %q, want %q", entry.State, StateSynced)
	}
}

func TestAgent_SecondRunSendsNothing(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)
	rig.writeFile(t, agentPath, []byte("payload"))

	if _, err := rig.agent.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// A pass over an already-synced corpus must be free. If discovery
	// re-tracked or the ledger did not advance, this is where a spoke would
	// re-upload its entire backlog on every contact.
	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if res.Discovered != 0 {
		t.Errorf("discovered = %d on a second pass, want 0", res.Discovered)
	}
	if res.Sent != 0 || res.BytesSent != 0 {
		t.Errorf("sent %d files / %d bytes on a second pass, want 0", res.Sent, res.BytesSent)
	}
}

func TestAgent_LostAckIsResolvedWithoutResending(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("payload the hub already has")
	rig.writeFile(t, agentPath, content)

	// The hub received this file, but the acknowledgment never arrived — so
	// the spoke's ledger still says pending. Reconcile must resolve it in bulk
	// rather than re-uploading.
	rig.transport.Seed(DefaultHubID, agentPath, sha256Hex(content), int64(len(content)))

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.AlreadyPresent != 1 {
		t.Errorf("already-present = %d, want 1", res.AlreadyPresent)
	}
	if res.BytesSent != 0 {
		t.Errorf("bytes sent = %d, want 0 — the file was re-uploaded despite the hub having it", res.BytesSent)
	}

	entry, err := rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if entry.State != StateSynced {
		t.Errorf("state = %q, want %q", entry.State, StateSynced)
	}
}

func TestAgent_NewestFirstOrdering(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	// Three partitions across different hours. When a contact window closes
	// mid-backlog, the freshest telemetry must already have landed — so the
	// agent has to send newest-first, not in discovery order.
	hours := []string{"10", "14", "12"}
	for i, h := range hours {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/%s/cpu_%d.parquet", h, i)
		rig.writeFile(t, p, []byte(fmt.Sprintf("payload %d", i)))
	}

	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}
	pending, err := rig.ledger.Pending(ctx, DefaultHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("pending = %d, want 3", len(pending))
	}
	for i := 1; i < len(pending); i++ {
		if pending[i].PartitionTime.After(pending[i-1].PartitionTime) {
			t.Errorf("entry %d (%s) is newer than %d (%s) — not newest-first",
				i, pending[i].PartitionTime, i-1, pending[i-1].PartitionTime)
		}
	}
	// The 14:00 partition must lead: it is the freshest.
	if pending[0].PartitionTime.Hour() != 14 {
		t.Errorf("first entry is hour %d, want 14", pending[0].PartitionTime.Hour())
	}
}

func TestAgent_DiscoveryComputesDigestsAndNamespace(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("parquet payload")
	rig.writeFile(t, agentPath, content)

	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	entry, err := rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}

	// The digest is the integrity anchor — §6.1 makes (path, sha256) the
	// file's identity, and ListObjects does not supply one.
	if entry.SHA256 != sha256Hex(content) {
		t.Errorf("sha256 = %q, want the file's actual digest", entry.SHA256)
	}
	if entry.SizeBytes != int64(len(content)) {
		t.Errorf("size = %d, want %d", entry.SizeBytes, len(content))
	}
	if entry.Database != "metrics" || entry.Measurement != "cpu" {
		t.Errorf("namespace = %s/%s, want metrics/cpu", entry.Database, entry.Measurement)
	}
	want := time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC)
	if !entry.PartitionTime.Equal(want) {
		t.Errorf("partition = %v, want %v", entry.PartitionTime, want)
	}
}

func TestAgent_DiscoverySkipsNonSyncableFiles(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	rig.writeFile(t, agentPath, []byte("real"))
	// Not Parquet, and dot-prefixed staging. A spoke shipping either would be
	// sending transient state rather than data.
	rig.writeFile(t, "metrics/cpu/2026/08/07/14/notes.txt", []byte("x"))
	rig.writeFile(t, ".sync-staging/rocket-02/leftover.parquet", []byte("x"))

	n, err := rig.agent.Discover(ctx)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if n != 1 {
		t.Errorf("discovered = %d, want only the parquet file", n)
	}
}

func TestAgent_ResumesAPartialTransfer(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("the complete parquet payload for this file")
	rig.writeFile(t, agentPath, content)

	// Discover the file whole, so the ledger holds the real digest and size.
	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	// Now truncate it, so the transfer delivers only a prefix and the hub
	// genuinely stages one. Scripting a partial result instead would claim an
	// offset the hub never retained — a different scenario, covered by
	// TestAgent_RecoversFromAStaleCheckpoint.
	const accepted = 15
	if err := rig.backend.Write(ctx, agentPath, content[:accepted]); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if res.Partial != 1 {
		t.Errorf("partial = %d, want 1", res.Partial)
	}
	if res.Sent != 0 {
		t.Errorf("sent = %d, want 0 — a partial transfer is not a completed one", res.Sent)
	}

	// The checkpoint must survive: restarting a large file from zero is what
	// resume exists to prevent.
	entry, err := rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if entry.BytesSent != accepted {
		t.Fatalf("checkpoint = %d, want %d", entry.BytesSent, accepted)
	}

	// Restore the rest of the file, as though writing had completed, and let
	// the next pass resume from the checkpoint.
	if err := rig.backend.Write(ctx, agentPath, content); err != nil {
		t.Fatalf("restore: %v", err)
	}
	res2, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if res2.Sent != 1 {
		t.Errorf("sent = %d on resume, want 1", res2.Sent)
	}

	// The reassembled file must match — a resume that mis-splices corrupts
	// silently, which is what the whole-file digest exists to catch.
	sha, ok := rig.transport.Has(DefaultHubID, agentPath)
	if !ok {
		t.Fatal("the hub does not have the resumed file")
	}
	if sha != sha256Hex(content) {
		t.Error("the resumed file does not match the original")
	}
}

func TestAgent_RecoversFromAStaleCheckpoint(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("the complete parquet payload for this file")
	rig.writeFile(t, agentPath, content)

	// The ledger holds a checkpoint the hub cannot honour — it restarted,
	// swept its staging area, or runs on a backend that cannot append. The
	// file itself is fine; only the checkpoint is stale.
	//
	// Without recovery the agent retries the same impossible offset until the
	// attempt cap, then abandons healthy data.
	rig.transport.ScriptPut(DefaultHubID, agentPath,
		&PutResult{Outcome: OutcomePartial, BytesAccepted: 15})
	if _, err := rig.agent.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	entry, err := rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if entry.BytesSent != 15 {
		t.Fatalf("checkpoint = %d, want the hub's claimed 15", entry.BytesSent)
	}

	// This pass discovers the hub has no such prefix and clears the checkpoint.
	if _, err := rig.agent.Run(ctx); err != nil {
		t.Fatalf("second run: %v", err)
	}
	entry, err = rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if entry.BytesSent != 0 {
		t.Fatalf("checkpoint = %d after a rejected resume, want it cleared", entry.BytesSent)
	}

	// And the next pass sends the whole file successfully.
	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("third run: %v", err)
	}
	if res.Sent != 1 {
		t.Fatalf("sent = %d, want 1 — a stale checkpoint stranded a healthy file", res.Sent)
	}
	sha, ok := rig.transport.Has(DefaultHubID, agentPath)
	if !ok || sha != sha256Hex(content) {
		t.Error("the recovered file does not match the original")
	}
}

func TestAgent_ConflictIsTerminalNotRetried(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	spokeContent := []byte("the spoke's version")
	rig.writeFile(t, agentPath, spokeContent)
	// The hub holds different content at the same path.
	rig.transport.Seed(DefaultHubID, agentPath, sha256Hex([]byte("the hub's version")), 19)

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Surfaced at reconcile time for the whole backlog at once, rather than
	// discovered one 409 at a time during transfer.
	if len(res.Conflicts) != 1 {
		t.Fatalf("conflicts = %v, want one", res.Conflicts)
	}
	if res.Conflicts[0].Path != agentPath {
		t.Errorf("conflict path = %q, want %q", res.Conflicts[0].Path, agentPath)
	}
	if res.Sent != 0 {
		t.Error("a conflicted file was sent; re-sending cannot resolve a content disagreement")
	}

	// It must not silently retry forever on later passes either.
	res2, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if res2.Sent != 0 {
		t.Error("a conflicted file was sent on a later pass")
	}
}

func TestAgent_ChecksumMismatchIsRetriedThenGivesUp(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("payload")
	rig.writeFile(t, agentPath, content)

	// A mismatch is retryable — the corruption may be in flight, or in the
	// edge's own flash — so it burns attempts rather than failing at once.
	const maxAttempts = 3
	agent, err := NewAgent(AgentConfig{
		Ledger: rig.ledger, Transport: rig.transport, Backend: rig.backend,
		HubID: DefaultHubID, SpokeID: "rocket-01", MaxAttempts: maxAttempts,
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("agent: %v", err)
	}

	for i := 0; i < maxAttempts; i++ {
		rig.transport.ScriptPut(DefaultHubID, agentPath, &PutResult{Outcome: OutcomeChecksumMismatch})
	}
	for i := 0; i < maxAttempts; i++ {
		if _, err := agent.Run(ctx); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}

	entry, err := rig.ledger.Get(ctx, DefaultHubID, agentPath)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if entry.State != StateFailed {
		t.Errorf("state = %q after %d mismatches, want %q", entry.State, maxAttempts, StateFailed)
	}
}

func TestAgent_RecoversInterruptedTransfers(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	content := []byte("payload")
	rig.writeFile(t, agentPath, content)
	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	// Simulate a crash mid-transfer: the row is left in_flight with no process
	// behind it, so nothing would ever pick it up again.
	if err := rig.ledger.MarkInFlight(ctx, DefaultHubID, agentPath); err != nil {
		t.Fatalf("mark in-flight: %v", err)
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Recovered != 1 {
		t.Errorf("recovered = %d, want 1 — a stranded transfer was never reset", res.Recovered)
	}
	if res.Sent != 1 {
		t.Errorf("sent = %d, want 1", res.Sent)
	}
}

func TestAgent_StatusReportsPendingWork(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	for i := 0; i < 3; i++ {
		rig.writeFile(t, fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%d.parquet", i), []byte("payload"))
	}
	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	// This is what an operator reads to know whether a contact window is
	// keeping up with production.
	st, err := rig.agent.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Pending != 3 {
		t.Errorf("pending = %d, want 3", st.Pending)
	}
	if st.PendingBytes != 3*int64(len("payload")) {
		t.Errorf("pending bytes = %d, want %d", st.PendingBytes, 3*len("payload"))
	}

	if _, err := rig.agent.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	st, err = rig.agent.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Pending != 0 || st.Synced != 3 {
		t.Errorf("after run: pending=%d synced=%d, want 0/3", st.Pending, st.Synced)
	}
	if st.LastSyncedAt == nil {
		t.Error("last-synced is unset after a successful run")
	}
}

func TestAgent_RunWithNothingToDoIsCheap(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	// A spoke with no local files must not error — a dark edge box that has
	// produced nothing is a normal state, not a failure.
	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("run on an empty spoke: %v", err)
	}
	if res.Discovered != 0 || res.Sent != 0 {
		t.Errorf("empty spoke reported discovered=%d sent=%d", res.Discovered, res.Sent)
	}
}

func TestAgent_HonorsContextCancellation(t *testing.T) {
	rig := newAgentRig(t)
	rig.writeFile(t, agentPath, []byte("payload"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A contact window that closes must abort rather than finish work against
	// a link that is already gone.
	if _, err := rig.agent.Run(ctx); err == nil {
		t.Error("Run with a cancelled context succeeded")
	}
}

func TestNewAgent_RequiresItsDependencies(t *testing.T) {
	rig := newAgentRig(t)

	base := AgentConfig{
		Ledger: rig.ledger, Transport: rig.transport, Backend: rig.backend,
		HubID: DefaultHubID, SpokeID: "rocket-01", Logger: zerolog.Nop(),
	}

	tests := []struct {
		name   string
		mutate func(*AgentConfig)
	}{
		{"no ledger", func(c *AgentConfig) { c.Ledger = nil }},
		{"no transport", func(c *AgentConfig) { c.Transport = nil }},
		{"no backend", func(c *AgentConfig) { c.Backend = nil }},
		{"no spoke ID", func(c *AgentConfig) { c.SpokeID = "" }},
		{"spoke ID with a separator", func(c *AgentConfig) { c.SpokeID = "rocket/../other" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			if _, err := NewAgent(cfg); err == nil {
				t.Error("an agent was built without a required dependency")
			}
		})
	}
}

func TestAgent_ConcurrentTransfersAreBounded(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	// §8.2 caps concurrency low because edge boxes are small. Run under -race
	// for this to mean anything.
	const files = 12
	for i := 0; i < files; i++ {
		rig.writeFile(t, fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%02d.parquet", i),
			[]byte(fmt.Sprintf("payload %d", i)))
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Sent != files {
		t.Errorf("sent = %d, want %d", res.Sent, files)
	}

	for i := 0; i < files; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%02d.parquet", i)
		if _, ok := rig.transport.Has(DefaultHubID, p); !ok {
			t.Errorf("file %d did not reach the hub", i)
		}
	}
}

// newAgentRigWithBatch builds a rig whose agent pages at batchSize.
func newAgentRigWithBatch(t *testing.T, batchSize int) *agentRig {
	t.Helper()
	rig := newAgentRig(t)

	agent, err := NewAgent(AgentConfig{
		Ledger:    rig.ledger,
		Transport: rig.transport,
		Backend:   rig.backend,
		HubID:     DefaultHubID,
		SpokeID:   "rocket-01",
		BatchSize: batchSize,
		Logger:    zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	rig.agent = agent
	return rig
}

// A backlog larger than one batch must drain completely. Reporting success
// after moving only the first page would silently strand the rest — and a
// spoke returning from a long outage, the case the feature exists for, is
// exactly the one whose backlog exceeds a batch.
func TestAgent_RunPagesThroughABacklogLargerThanOneBatch(t *testing.T) {
	ctx := context.Background()
	const batch, total = 3, 10
	rig := newAgentRigWithBatch(t, batch)

	for i := 0; i < total; i++ {
		rig.writeFile(t, fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%02d.parquet", i),
			[]byte(fmt.Sprintf("payload %02d", i)))
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Discovered != total {
		t.Errorf("discovered = %d, want %d", res.Discovered, total)
	}
	if res.Sent != total {
		t.Errorf("sent = %d of %d: the pass stopped after one page", res.Sent, total)
	}

	pending, err := rig.ledger.Pending(ctx, DefaultHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("%d files left pending after a full pass", len(pending))
	}
}

// Conflicts stay pending in the ledger, so a paging loop that re-fetched them
// would spin forever. It must terminate, and must report every page's
// conflicts rather than only the last page's.
func TestAgent_RunTerminatesAndAccumulatesConflictsAcrossPages(t *testing.T) {
	ctx := context.Background()
	const batch, total = 2, 6
	rig := newAgentRigWithBatch(t, batch)

	for i := 0; i < total; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/c_%02d.parquet", i)
		rig.writeFile(t, p, []byte(fmt.Sprintf("spoke copy %02d", i)))
		// The hub holds different content at every one of these paths.
		rig.transport.Seed(DefaultHubID, p, fmt.Sprintf("%064x", i+1), 11)
	}

	done := make(chan struct{})
	var res *RunResult
	var err error
	go func() {
		defer close(done)
		res, err = rig.agent.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not terminate on an all-conflict backlog")
	}
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(res.Conflicts) != total {
		t.Errorf("conflicts = %d, want %d: pages overwrote each other", len(res.Conflicts), total)
	}
	if res.Sent != 0 {
		t.Errorf("sent = %d, want 0: a conflict must not transfer bytes", res.Sent)
	}
}

// Newest-first must hold across pages, not just within one. A contact window
// that closes mid-backlog should have delivered the freshest telemetry, and
// paging must not reorder that.
func TestAgent_RunSendsNewestFirstAcrossPages(t *testing.T) {
	ctx := context.Background()
	const batch, total = 2, 6
	rig := newAgentRigWithBatch(t, batch)

	// Hours 00..05; hour 05 is newest.
	for i := 0; i < total; i++ {
		rig.writeFile(t, fmt.Sprintf("metrics/cpu/2026/08/07/%02d/f.parquet", i),
			[]byte(fmt.Sprintf("payload %02d", i)))
	}

	if _, err := rig.agent.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	order := rig.transport.PutOrder()
	if len(order) != total {
		t.Fatalf("sent %d files, want %d", len(order), total)
	}
	// Within a page, transfers race — maxConcurrent goroutines reach the
	// transport in nondeterministic order — so per-file position proves
	// nothing. The invariant paging must preserve is at the PAGE level: the
	// first page carries the newest files, so a contact window that closes
	// after one page has delivered the freshest telemetry.
	firstPage := make(map[string]bool, batch)
	for _, p := range order[:batch] {
		firstPage[p] = true
	}
	for _, want := range []string{"/05/", "/04/"} {
		found := false
		for p := range firstPage {
			if strings.Contains(p, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("hour %s is not in the first page (%v); paging did not send newest-first", want, order[:batch])
		}
	}

	// And the oldest must be in the final page, not pulled forward.
	lastPage := strings.Join(order[total-batch:], " ")
	if !strings.Contains(lastPage, "/00/") {
		t.Errorf("the oldest file is not in the last page (%v)", order[total-batch:])
	}
}

// batch_size reaches make()'s capacity argument in the paging loop, so a
// negative value panics the process on the first pass — a crash on the edge
// box, for a value an operator can set in arc.toml.
func TestAgent_NegativeBatchSizeDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRigWithBatch(t, -1)
	rig.writeFile(t, "metrics/cpu/2026/08/07/14/f.parquet", []byte("payload"))

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Sent != 1 {
		t.Errorf("sent = %d, want 1: a clamped batch size should behave like the default", res.Sent)
	}
}

// max_concurrent is clamped so an operator typo cannot spawn one goroutine and
// one open file handle per pending file on a small edge box.
func TestNewAgent_ClampsMaxConcurrent(t *testing.T) {
	rig := newAgentRig(t)

	agent, err := NewAgent(AgentConfig{
		Ledger: rig.ledger, Transport: rig.transport, Backend: rig.backend,
		HubID: DefaultHubID, SpokeID: "rocket-01",
		MaxConcurrent: 100000,
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("new agent: %v", err)
	}
	if agent.maxConcurrent != MaxAllowedConcurrent {
		t.Errorf("maxConcurrent = %d, want it clamped to %d", agent.maxConcurrent, MaxAllowedConcurrent)
	}
}

// A conflict found during reconcile must be terminal, exactly as one found
// during transfer is. Left pending it would ride along in every future
// reconcile payload forever, and the two paths would disagree about whether
// the same condition is terminal.
func TestAgent_ReconcileConflictIsTerminal(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	const p = "metrics/cpu/2026/08/07/14/contested.parquet"
	rig.writeFile(t, p, []byte("the spoke's version"))
	rig.transport.Seed(DefaultHubID, p, fmt.Sprintf("%064x", 1), 19)

	if _, err := rig.agent.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	entry, err := rig.ledger.Get(ctx, DefaultHubID, p)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry.State != StateFailed {
		t.Errorf("state = %q, want %q: a reconcile conflict stayed pending and will re-offer forever",
			entry.State, StateFailed)
	}

	// And a second pass must not re-offer it.
	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if len(res.Conflicts) != 0 {
		t.Errorf("the second pass re-reported %d conflicts; the file should be terminal", len(res.Conflicts))
	}
}
