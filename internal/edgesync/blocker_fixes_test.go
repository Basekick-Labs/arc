package edgesync

// Regression tests for the pre-26.09.1 edge sync audit blockers:
//
//   - a spoke whose reconcile batch exceeds the hub's cap must split and
//     retry rather than failing every pass identically (B3);
//   - a pending file deleted by compaction/retention must be marked skipped,
//     not wedge air-gap export forever or burn the network retry budget (B4);
//   - the ledger must be able to reclaim skipped rows (PruneSkipped).

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// A spoke at batch_size=0 (offer the whole backlog at once) against a hub
// whose reconcile cap is smaller must still drain: the 413 carries the hub's
// cap, and the agent splits the page. Before the fix this failed every pass
// with the same 413 and moved zero files — the audit's live-reproduced B3.
func TestAgent_RunSplitsAReconcilePageTheHubRefuses(t *testing.T) {
	ctx := context.Background()
	const hubCap, total = 3, 10
	rig := newAgentRigWithBatch(t, 0)
	rig.transport.MaxReconcileEntries = hubCap

	for i := 0; i < total; i++ {
		rig.writeFile(t, fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%02d.parquet", i),
			[]byte(fmt.Sprintf("payload %02d", i)))
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed instead of splitting the refused page: %v", err)
	}
	if res.Sent != total {
		t.Errorf("sent = %d of %d after 413 splitting", res.Sent, total)
	}

	pending, err := rig.ledger.Pending(ctx, DefaultHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("%d files left pending; the refused page was not fully retried", len(pending))
	}
}

// An explicit batch_size larger than the hub's cap is the same shape from the
// hub's side; the split must land under the cap rather than erroring.
func TestAgent_RunSplitsWhenExplicitBatchExceedsTheHubCap(t *testing.T) {
	ctx := context.Background()
	const hubCap, batch, total = 2, 5, 6
	rig := newAgentRigWithBatch(t, batch)
	rig.transport.MaxReconcileEntries = hubCap

	for i := 0; i < total; i++ {
		rig.writeFile(t, fmt.Sprintf("metrics/cpu/2026/08/07/14/g_%02d.parquet", i),
			[]byte(fmt.Sprintf("payload %02d", i)))
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Sent != total {
		t.Errorf("sent = %d of %d", res.Sent, total)
	}
}

// A pending file deleted from local storage after discovery (compaction is on
// by default and rewrites-then-deletes raw parquet) must be marked skipped by
// the network pass — not retried into terminal `failed` noise. Before the fix
// each such file burned the full attempt budget and sat failed forever (H3).
func TestAgent_RunSkipsAFileDeletedAfterDiscovery(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	rig.writeFile(t, "metrics/cpu/2026/08/07/14/keep.parquet", []byte("kept"))
	rig.writeFile(t, "metrics/cpu/2026/08/07/14/gone.parquet", []byte("doomed"))

	// Discover both, then delete one underneath the ledger — what compaction
	// does between discovery and the next pass.
	if _, err := rig.agent.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}
	if err := rig.backend.Delete(ctx, "metrics/cpu/2026/08/07/14/gone.parquet"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err := rig.agent.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Sent != 1 {
		t.Errorf("sent = %d, want 1 (the surviving file)", res.Sent)
	}
	if res.Skipped != 1 {
		t.Errorf("skipped = %d, want 1 (the vanished file)", res.Skipped)
	}
	if res.Failed != 0 {
		t.Errorf("failed = %d; a vanished file must skip, not burn attempts", res.Failed)
	}

	stats, err := rig.ledger.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Skipped != 1 {
		t.Errorf("ledger skipped = %d, want 1", stats.Skipped)
	}
	if stats.Pending != 0 {
		t.Errorf("ledger pending = %d, want 0", stats.Pending)
	}
}

// Air-gap export with a vanished pending file must produce a bundle with the
// surviving files and mark the vanished one skipped. Before the fix the whole
// export failed, the entry stayed pending, and every future export re-selected
// it — a permanent wedge on the box least able to receive a site visit (B2 of
// the audit; the flagship air-gap deployment is exactly the one with no
// network fallback).
func TestExporter_ExportSkipsAVanishedFileInsteadOfWedging(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	rig.writeFile(t, "metrics/cpu/2026/08/07/14/keep.parquet", []byte("kept"))
	rig.writeFile(t, "metrics/cpu/2026/08/07/15/gone.parquet", []byte("doomed"))

	dest, err := os.MkdirTemp("", "bundle-dest-*")
	if err != nil {
		t.Fatalf("dest dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dest) })

	policy, err := NewDestinationPolicy([]string{dest}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	writer, err := NewBundleWriter(BundleWriterConfig{
		Backend: rig.backend,
		SpokeID: "rocket-01",
		HubID:   DefaultHubID,
		Secret:  "0123456789abcdef0123456789abcdef",
		Logger:  zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	discoverer, err := NewDiscoverer(rig.ledger, rig.backend, DefaultHubID, zerolog.Nop())
	if err != nil {
		t.Fatalf("discoverer: %v", err)
	}
	exporter, err := NewExporter(ExporterConfig{
		Ledger:     rig.ledger,
		Writer:     writer,
		Policy:     policy,
		Discoverer: discoverer,
		HubID:      DefaultHubID,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}

	// Discover both, then delete one underneath the ledger.
	if _, err := discoverer.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}
	if err := rig.backend.Delete(ctx, "metrics/cpu/2026/08/07/15/gone.parquet"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err := exporter.Export(ctx, dest, 0)
	if err != nil {
		t.Fatalf("Export failed instead of skipping the vanished file: %v", err)
	}
	if res.FileCount != 1 {
		t.Errorf("bundle files = %d, want 1", res.FileCount)
	}
	if res.Skipped != 1 {
		t.Errorf("export skipped = %d, want 1", res.Skipped)
	}

	stats, err := rig.ledger.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Skipped != 1 {
		t.Errorf("ledger skipped = %d, want 1", stats.Skipped)
	}
	if stats.Exported != 1 {
		t.Errorf("ledger exported = %d, want 1", stats.Exported)
	}

	// The wedge regression proper: the NEXT export must not re-select the
	// vanished file — with nothing new it reports nothing to export.
	if _, err := exporter.Export(ctx, dest, 0); err != ErrNothingToExport {
		t.Errorf("second export = %v, want ErrNothingToExport (the vanished file must stay skipped)", err)
	}
}

// MarkSkipped is legal from pending and in_flight only; terminal states must
// refuse it, and PruneSkipped must reclaim old skipped rows by last_attempt.
func TestLedger_MarkSkippedAndPruneSkipped(t *testing.T) {
	ctx := context.Background()
	ledger := setupTestLedger(t)

	add := func(path string) {
		t.Helper()
		if err := ledger.Track(ctx, &LedgerEntry{
			HubID: DefaultHubID, Path: path, SHA256: "aa", SizeBytes: 2,
			Database: "metrics", Measurement: "cpu",
			PartitionTime: time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC),
			DiscoveredAt:  time.Now().UTC(),
		}); err != nil {
			t.Fatalf("add %s: %v", path, err)
		}
	}

	add("metrics/cpu/2026/08/07/14/a.parquet")
	if err := ledger.MarkSkipped(ctx, DefaultHubID, "metrics/cpu/2026/08/07/14/a.parquet", "gone"); err != nil {
		t.Fatalf("MarkSkipped from pending: %v", err)
	}

	add("metrics/cpu/2026/08/07/14/b.parquet")
	if err := ledger.MarkInFlight(ctx, DefaultHubID, "metrics/cpu/2026/08/07/14/b.parquet"); err != nil {
		t.Fatalf("MarkInFlight: %v", err)
	}
	if err := ledger.MarkSkipped(ctx, DefaultHubID, "metrics/cpu/2026/08/07/14/b.parquet", "gone"); err != nil {
		t.Fatalf("MarkSkipped from in_flight: %v", err)
	}

	// From synced: must refuse — skipping delivered data would misreport it.
	add("metrics/cpu/2026/08/07/14/c.parquet")
	if err := ledger.MarkInFlight(ctx, DefaultHubID, "metrics/cpu/2026/08/07/14/c.parquet"); err != nil {
		t.Fatalf("MarkInFlight c: %v", err)
	}
	if err := ledger.MarkSynced(ctx, DefaultHubID, "metrics/cpu/2026/08/07/14/c.parquet"); err != nil {
		t.Fatalf("MarkSynced c: %v", err)
	}
	if err := ledger.MarkSkipped(ctx, DefaultHubID, "metrics/cpu/2026/08/07/14/c.parquet", "gone"); err == nil {
		t.Error("MarkSkipped from synced succeeded; must refuse")
	}

	stats, err := ledger.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Skipped != 2 {
		t.Fatalf("skipped = %d, want 2", stats.Skipped)
	}

	// Fresh skipped rows survive a prune with a 1-day retention...
	pruned, err := ledger.PruneSkipped(ctx, 1)
	if err != nil {
		t.Fatalf("PruneSkipped: %v", err)
	}
	if pruned != 0 {
		t.Errorf("pruned %d fresh rows; retention must protect them", pruned)
	}

	// ...and are reclaimed once last_attempt ages past the cutoff.
	if _, err := ledger.db.ExecContext(ctx,
		`UPDATE sync_ledger SET last_attempt = ? WHERE state = 'skipped'`,
		time.Now().UTC().AddDate(0, 0, -10)); err != nil {
		t.Fatalf("age rows: %v", err)
	}
	pruned, err = ledger.PruneSkipped(ctx, 7)
	if err != nil {
		t.Fatalf("PruneSkipped aged: %v", err)
	}
	if pruned != 2 {
		t.Errorf("pruned = %d, want 2", pruned)
	}
}

// The hub mounts Arc's token middleware (write level) ahead of the sync
// routes, so the transport must present the API token from
// ARC_EDGE_SYNC_HUB_TOKEN on every request. Before the fix no token was ever
// sent and every spoke request against an auth-enabled hub — the default
// production posture — died with 401 (the audit's live-reproduced top
// blocker).
func TestHTTPTransport_SendsTheHubAPIToken(t *testing.T) {
	var gotAuth []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = append(gotAuth, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"missing":[],"present":[],"conflicts":[]}`))
	}))
	t.Cleanup(srv.Close)

	transport, err := NewHTTPTransport(HTTPTransportConfig{
		BaseURL:  srv.URL,
		SpokeID:  "rocket-01",
		Secret:   "0123456789abcdef0123456789abcdef",
		APIToken: "hub-write-token",
	})
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	if _, err := transport.Reconcile(context.Background(), DefaultHubID, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(gotAuth) != 1 || gotAuth[0] != "Bearer hub-write-token" {
		t.Errorf("Authorization = %v, want [Bearer hub-write-token]", gotAuth)
	}

	// Without a token no Authorization header is sent at all — an empty
	// Bearer would read as a malformed credential rather than "none".
	bare, err := NewHTTPTransport(HTTPTransportConfig{
		BaseURL: srv.URL,
		SpokeID: "rocket-01",
		Secret:  "0123456789abcdef0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("bare transport: %v", err)
	}
	if _, err := bare.Reconcile(context.Background(), DefaultHubID, nil); err != nil {
		t.Fatalf("bare reconcile: %v", err)
	}
	if gotAuth[1] != "" {
		t.Errorf("tokenless Authorization = %q, want empty", gotAuth[1])
	}
}

// Operator remediation (#612): failed rows can be requeued (fresh retry
// budget) or dismissed (terminal, prunable, reversible); the other two
// skipped classes — vanished files and compacted outputs — must never
// re-enter the queue through either op.
func TestLedger_RequeueAndDismissFailedRows(t *testing.T) {
	ctx := context.Background()
	ledger := setupTestLedger(t)

	add := func(path string) {
		t.Helper()
		if err := ledger.Track(ctx, &LedgerEntry{
			HubID: DefaultHubID, Path: path, SHA256: "aa", SizeBytes: 2,
			Database: "metrics", Measurement: "cpu",
			PartitionTime: time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC),
			DiscoveredAt:  time.Now().UTC(),
		}); err != nil {
			t.Fatalf("add %s: %v", path, err)
		}
	}
	fail := func(path string) {
		t.Helper()
		if err := ledger.MarkInFlight(ctx, DefaultHubID, path); err != nil {
			t.Fatalf("inflight %s: %v", path, err)
		}
		if err := ledger.MarkFailed(ctx, DefaultHubID, path, "boom", 1); err != nil {
			t.Fatalf("fail %s: %v", path, err)
		}
	}
	state := func(path string) SyncState {
		t.Helper()
		e, err := ledger.Get(ctx, DefaultHubID, path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		return e.State
	}

	f1 := "metrics/cpu/2026/08/07/14/f1.parquet"
	f2 := "metrics/cpu/2026/08/07/14/f2.parquet"
	add(f1)
	add(f2)
	fail(f1)
	fail(f2)

	// Vanished-skip and compacted-output rows: untouchable by both ops.
	vanished := "metrics/cpu/2026/08/07/14/vanished.parquet"
	add(vanished)
	if err := ledger.MarkSkipped(ctx, DefaultHubID, vanished, "source file removed before delivery (compaction or retention)"); err != nil {
		t.Fatalf("skip vanished: %v", err)
	}
	output := "metrics/cpu/2026/08/07/14/cpu_20260807_140000_1754575200000000000_b1_compacted.parquet"
	if err := ledger.TrackCompactedOutput(ctx, DefaultHubID, output); err != nil {
		t.Fatalf("track output: %v", err)
	}

	// Requeue one by path: pending again, attempts reset.
	n, err := ledger.RequeueFailed(ctx, DefaultHubID, f1)
	if err != nil || n != 1 {
		t.Fatalf("requeue f1 = %d, %v", n, err)
	}
	e, _ := ledger.Get(ctx, DefaultHubID, f1)
	if e.State != StatePending || e.Attempts != 0 {
		t.Errorf("f1 = %s attempts=%d, want pending/0", e.State, e.Attempts)
	}

	// Dismiss the other: skipped with the operator note, prune-eligible.
	n, err = ledger.DismissFailed(ctx, DefaultHubID, f2)
	if err != nil || n != 1 {
		t.Fatalf("dismiss f2 = %d, %v", n, err)
	}
	e, _ = ledger.Get(ctx, DefaultHubID, f2)
	if e.State != StateSkipped || e.LastError != NoteOperatorDismissed {
		t.Errorf("f2 = %s/%q, want skipped/operator-dismissed", e.State, e.LastError)
	}

	// Dismissal is reversible; the OTHER skip classes are not.
	n, err = ledger.RequeueFailed(ctx, DefaultHubID, "")
	if err != nil {
		t.Fatalf("requeue all: %v", err)
	}
	if n != 1 {
		t.Errorf("requeue all touched %d rows, want 1 (only the dismissed row)", n)
	}
	if got := state(f2); got != StatePending {
		t.Errorf("dismissed row after requeue = %s, want pending", got)
	}
	if got := state(vanished); got != StateSkipped {
		t.Errorf("vanished row = %s; must stay skipped", got)
	}
	if got := state(output); got != StateSkipped {
		t.Errorf("compacted-output row = %s; must stay skipped", got)
	}

	// Misses and empty-alls report honestly.
	if n, err := ledger.RequeueFailed(ctx, DefaultHubID, "metrics/cpu/nope.parquet"); err != nil || n != 0 {
		t.Errorf("requeue miss = %d, %v; want 0, nil", n, err)
	}
	if n, err := ledger.DismissFailed(ctx, DefaultHubID, ""); err != nil || n != 0 {
		t.Errorf("dismiss all with none failed = %d, %v; want 0, nil", n, err)
	}
}
