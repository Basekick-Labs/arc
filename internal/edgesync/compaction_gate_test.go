package edgesync

// Regression tests for issue #610: spoke compaction defers until edge sync
// has delivered the data, and compacted outputs never sync.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func gateAdd(t *testing.T, l *Ledger, path string, transition func(context.Context) error) {
	t.Helper()
	ctx := context.Background()
	if err := l.Track(ctx, &LedgerEntry{
		HubID: DefaultHubID, Path: path, SHA256: "aa", SizeBytes: 2,
		Database: "metrics", Measurement: "cpu",
		PartitionTime: time.Date(2026, 8, 7, 14, 0, 0, 0, time.UTC),
		DiscoveredAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("track %s: %v", path, err)
	}
	if transition != nil {
		if err := transition(ctx); err != nil {
			t.Fatalf("transition %s: %v", path, err)
		}
	}
}

// Only delivered content may be compacted: synced rows and compacted-output
// rows are eligible; every other state — and unknown raws — defers. This is
// the loss-impossibility half of #610.
func TestCompactionEligibility_OnlyDeliveredFilesAreEligible(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)
	epoch := time.Now().UTC()

	mark := func(fn func(context.Context) error) func(context.Context) error { return fn }

	synced := "metrics/cpu/2026/08/07/14/synced.parquet"
	gateAdd(t, l, synced, mark(func(c context.Context) error {
		if err := l.MarkInFlight(c, DefaultHubID, synced); err != nil {
			return err
		}
		return l.MarkSynced(c, DefaultHubID, synced)
	}))

	pending := "metrics/cpu/2026/08/07/14/pending.parquet"
	gateAdd(t, l, pending, nil)

	exported := "metrics/cpu/2026/08/07/14/exported.parquet"
	gateAdd(t, l, exported, mark(func(c context.Context) error {
		return l.MarkExported(c, DefaultHubID, exported, "06BUNDLE")
	}))

	vanished := "metrics/cpu/2026/08/07/14/vanished.parquet"
	gateAdd(t, l, vanished, mark(func(c context.Context) error {
		return l.MarkSkipped(c, DefaultHubID, vanished, "source file removed before delivery (compaction or retention)")
	}))

	output := "metrics/cpu/2026/08/07/14/cpu_20260807_140000_1754575200000000000_b1_compacted.parquet"
	if err := l.TrackCompactedOutput(ctx, DefaultHubID, output); err != nil {
		t.Fatalf("track output: %v", err)
	}

	unknownRaw := "metrics/cpu/2026/08/07/14/never_discovered.parquet"

	gate := NewCompactionEligibility(l, DefaultHubID, epoch, zerolog.Nop())
	got, err := gate(ctx, []string{synced, pending, exported, vanished, output, unknownRaw})
	if err != nil {
		t.Fatalf("gate: %v", err)
	}

	want := map[string]bool{
		synced:     true,  // delivered
		pending:    false, // not delivered
		exported:   false, // on a drive, unconfirmed
		vanished:   false, // skipped-for-vanished is NOT a compacted output
		output:     true,  // compacted output: contents delivered by construction
		unknownRaw: false, // discovery hasn't seen it
	}
	for path, wantEligible := range want {
		if got[path] != wantEligible {
			t.Errorf("%s: eligible = %v, want %v", path, got[path], wantEligible)
		}
	}
}

// A tier-suffixed file ABSENT from the ledger is a crash orphan when its
// embedded timestamp is after the enablement epoch (eligible: its inputs
// were delivered by construction) and legacy when before (ineligible until
// it has synced once — it may hold rows the hub never received).
func TestCompactionEligibility_EpochDiscriminatesOrphanFromLegacy(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)
	epoch := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	after := epoch.Add(24 * time.Hour).UnixNano()
	before := epoch.Add(-24 * time.Hour).UnixNano()

	orphan := "metrics/cpu/2026/08/02/10/cpu_20260802_100000_" + itoa64(after) + "_b1_compacted.parquet"
	legacy := "metrics/cpu/2026/07/30/10/cpu_20260730_100000_" + itoa64(before) + "_b1_compacted.parquet"
	dailyOrphan := "metrics/cpu/2026/08/02/cpu_20260802_100000_" + itoa64(after) + "_b2_daily.parquet"
	mangled := "metrics/cpu/2026/08/02/10/cpu_notanumber_b1_compacted.parquet"

	gate := NewCompactionEligibility(l, DefaultHubID, epoch, zerolog.Nop())
	got, err := gate(ctx, []string{orphan, legacy, dailyOrphan, mangled})
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if !got[orphan] {
		t.Error("post-epoch orphan should be eligible")
	}
	if got[legacy] {
		t.Error("pre-epoch legacy compacted file must NOT be eligible before syncing")
	}
	if !got[dailyOrphan] {
		t.Error("post-epoch daily orphan should be eligible (the _daily suffix counts)")
	}
	if got[mangled] {
		t.Error("an unparseable compacted-looking name must resolve toward syncing (ineligible)")
	}
}

// Discovery under the epoch tracks a post-epoch orphan as a compacted output
// (never offered) and a pre-epoch legacy file as ordinary syncable data.
func TestDiscovery_EpochRoutesOrphansAwayFromSync(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)
	epoch := time.Now().UTC().Add(-time.Hour)

	orphanTS := time.Now().UTC().UnixNano()
	legacyTS := epoch.Add(-48 * time.Hour).UnixNano()
	orphan := "metrics/cpu/2026/08/07/14/cpu_20260807_140000_" + itoa64(orphanTS) + "_b1_compacted.parquet"
	legacy := "metrics/cpu/2026/08/07/13/cpu_20260805_120000_" + itoa64(legacyTS) + "_b1_compacted.parquet"

	rig.writeFile(t, orphan, []byte("orphan payload"))
	rig.writeFile(t, legacy, []byte("legacy payload"))

	d, err := NewDiscoverer(rig.ledger, rig.backend, DefaultHubID, zerolog.Nop())
	if err != nil {
		t.Fatalf("discoverer: %v", err)
	}
	d.SetCompactionDeferEpoch(epoch)
	if _, err := d.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}

	orphanRow, err := rig.ledger.Get(ctx, DefaultHubID, orphan)
	if err != nil {
		t.Fatalf("orphan row: %v", err)
	}
	if orphanRow.State != StateSkipped || orphanRow.LastError != NoteCompactedOutput {
		t.Errorf("orphan tracked as %s/%q, want skipped/compacted-output", orphanRow.State, orphanRow.LastError)
	}

	legacyRow, err := rig.ledger.Get(ctx, DefaultHubID, legacy)
	if err != nil {
		t.Fatalf("legacy row: %v", err)
	}
	if legacyRow.State != StatePending {
		t.Errorf("legacy tracked as %s, want pending (must sync once)", legacyRow.State)
	}

	pending, err := rig.ledger.Pending(ctx, DefaultHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	for _, e := range pending {
		if e.Path == orphan {
			t.Error("orphaned compacted output must never be offered for sync")
		}
	}
}

// The observer records outputs idempotently and never downgrades a synced
// legacy row (which DID sync and must keep its history).
func TestTrackCompactedOutput_IdempotentAndPreservesSyncedRows(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	out := "metrics/cpu/2026/08/07/14/cpu_20260807_140000_1754575200000000000_b1_compacted.parquet"
	if err := l.TrackCompactedOutput(ctx, DefaultHubID, out); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Manifest recovery can fire the observer again on a retry.
	if err := l.TrackCompactedOutput(ctx, DefaultHubID, out); err != nil {
		t.Fatalf("second: %v", err)
	}
	stats, err := l.Stats(ctx, DefaultHubID)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Skipped != 1 {
		t.Errorf("skipped = %d, want 1 (idempotent)", stats.Skipped)
	}

	// A legacy compacted file that synced keeps its synced row.
	legacy := "metrics/cpu/2026/08/07/13/cpu_20260805_120000_100_b1_compacted.parquet"
	gateAdd(t, l, legacy, func(c context.Context) error {
		if err := l.MarkInFlight(c, DefaultHubID, legacy); err != nil {
			return err
		}
		return l.MarkSynced(c, DefaultHubID, legacy)
	})
	if err := l.TrackCompactedOutput(ctx, DefaultHubID, legacy); err != nil {
		t.Fatalf("legacy re-track: %v", err)
	}
	row, err := l.Get(ctx, DefaultHubID, legacy)
	if err != nil {
		t.Fatalf("legacy row: %v", err)
	}
	if row.State != StateSynced {
		t.Errorf("legacy row downgraded to %s; must stay synced", row.State)
	}
}

// Compacted-output rows survive PruneSkipped while their file exists —
// pruning one would let discovery re-sync the output — and are reclaimed by
// the sweep once the file is gone.
func TestCompactedOutputRows_PruneExemptUntilFileGone(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	out := "metrics/cpu/2026/08/07/14/cpu_20260807_140000_1754575200000000000_b1_compacted.parquet"
	if err := l.TrackCompactedOutput(ctx, DefaultHubID, out); err != nil {
		t.Fatalf("track: %v", err)
	}
	vanished := "metrics/cpu/2026/08/07/14/gone.parquet"
	gateAdd(t, l, vanished, func(c context.Context) error {
		return l.MarkSkipped(c, DefaultHubID, vanished, "source file removed before delivery (compaction or retention)")
	})

	// Age both rows past a 7-day retention.
	if _, err := l.db.ExecContext(ctx,
		`UPDATE sync_ledger SET last_attempt = ? WHERE state = 'skipped'`,
		time.Now().UTC().AddDate(0, 0, -10)); err != nil {
		t.Fatalf("age rows: %v", err)
	}

	pruned, err := l.PruneSkipped(ctx, 7)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned != 1 {
		t.Errorf("pruned = %d, want 1 (only the vanished-file row)", pruned)
	}
	if _, err := l.Get(ctx, DefaultHubID, out); err != nil {
		t.Errorf("compacted-output row was pruned while its file may still exist: %v", err)
	}

	// Sweep with the file "existing" keeps the row; gone deletes it.
	kept, err := l.SweepSkippedRows(ctx, DefaultHubID,
		func(context.Context, string) (bool, error) { return true, nil })
	if err != nil || kept != 0 {
		t.Fatalf("sweep(existing) = %d, %v; want 0, nil", kept, err)
	}
	// An exists ERROR must keep the row (fail-safe).
	if n, err := l.SweepSkippedRows(ctx, DefaultHubID,
		func(context.Context, string) (bool, error) { return false, errors.New("storage down") }); err != nil || n != 0 {
		t.Fatalf("sweep(error) = %d, %v; want 0, nil", n, err)
	}
	swept, err := l.SweepSkippedRows(ctx, DefaultHubID,
		func(context.Context, string) (bool, error) { return false, nil })
	if err != nil || swept != 1 {
		t.Fatalf("sweep(gone) = %d, %v; want 1, nil", swept, err)
	}
}

// The epoch is set exactly once and survives re-reads.
func TestEnsureMetaOnce_StableEpoch(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	first, err := l.EnsureMetaOnce(ctx, MetaCompactionDeferEpoch, "2026-08-19T00:00:00Z")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := l.EnsureMetaOnce(ctx, MetaCompactionDeferEpoch, "2026-09-01T00:00:00Z")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first != "2026-08-19T00:00:00Z" || second != first {
		t.Errorf("epoch not stable: first %q second %q", first, second)
	}
}

// DeliveryStates chunks past the SQLite bind limit.
func TestDeliveryStates_ChunksLargeLookups(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	paths := make([]string, 0, 1100)
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	for i := 0; i < 1100; i++ {
		p := "metrics/cpu/2026/08/07/14/f_" + itoa64(int64(i)) + ".parquet"
		paths = append(paths, p)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO sync_ledger (hub_id, path, sha256, size_bytes, database, measurement,
				partition_time, discovered_at, state)
			VALUES (?, ?, 'aa', 2, 'metrics', 'cpu', ?, ?, 'synced')`,
			DefaultHubID, p, time.Now().UTC(), time.Now().UTC()); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	states, err := l.DeliveryStates(ctx, DefaultHubID, paths)
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	if len(states) != 1100 {
		t.Errorf("states = %d, want 1100", len(states))
	}
}

func TestCompactedFileTimestamp(t *testing.T) {
	ts := time.Date(2026, 8, 7, 14, 0, 0, 12345, time.UTC)
	name := "metrics/cpu/2026/08/07/14/cpu_20260807_140000_" + itoa64(ts.UnixNano()) + "_b3_compacted.parquet"
	got, ok := compactedFileTimestamp(name)
	if !ok || !got.Equal(ts) {
		t.Errorf("parse = %v, %v; want %v, true", got, ok, ts)
	}
	for _, bad := range []string{
		"metrics/cpu/2026/08/07/14/plain_raw.parquet",
		"metrics/cpu/2026/08/07/14/cpu_20260807_140000_notanano_b1_compacted.parquet",
		"metrics/cpu/2026/08/07/14/cpu_x_compacted.parquet",
		"metrics/cpu/2026/08/07/14/cpu_20260807_140000_123_b1_compacted.csv",
	} {
		if _, ok := compactedFileTimestamp(bad); ok {
			t.Errorf("%s parsed as a compacted output; must not", bad)
		}
	}
	if !strings.HasSuffix(name, "_compacted.parquet") {
		t.Fatal("test name construction broke")
	}
}

func itoa64(n int64) string {
	return strconv.FormatInt(n, 10)
}

// A recovered output whose filename predates the epoch must NOT be tracked:
// it came from an ungated period (pre-upgrade crash, or defer=false), so its
// inputs were not necessarily delivered — discovery's legacy rule syncs it
// once instead. Post-epoch outputs track normally.
func TestCompactedOutputObserver_SkipsPreEpochOutputs(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)
	epoch := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	obs := NewCompactedOutputObserver(l, DefaultHubID, epoch, zerolog.Nop())

	pre := "metrics/cpu/2026/08/05/10/cpu_20260805_100000_" + itoa64(epoch.AddDate(0, 0, -3).UnixNano()) + "_b1_compacted.parquet"
	post := "metrics/cpu/2026/08/12/10/cpu_20260812_100000_" + itoa64(epoch.AddDate(0, 0, 2).UnixNano()) + "_b1_compacted.parquet"
	obs(pre)
	obs(post)

	if _, err := l.Get(ctx, DefaultHubID, pre); err == nil {
		t.Error("pre-epoch output was tracked as delivered; it must be left for discovery's legacy sync")
	}
	row, err := l.Get(ctx, DefaultHubID, post)
	if err != nil || row.State != StateSkipped || row.LastError != NoteCompactedOutput {
		t.Errorf("post-epoch output not tracked as compacted output: %v %v", row, err)
	}
}

// The defer=false→true round trip: an ungated run clears the epoch, so the
// next activation stamps a FRESH one and outputs from the ungated period —
// timestamped after the ORIGINAL epoch but before the new one — classify as
// legacy (sync once) instead of delivered (silent loss). This is the deep
// review's B1.
func TestClearMeta_UngatedPeriodOutputsClassifyAsLegacy(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	e1, err := l.EnsureMetaOnce(ctx, MetaCompactionDeferEpoch, time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("stamp e1: %v", err)
	}

	// Operator flips defer=false: startup clears the epoch, compaction runs
	// ungated and produces an output NOW (after e1).
	if err := l.ClearMeta(ctx, MetaCompactionDeferEpoch); err != nil {
		t.Fatalf("clear: %v", err)
	}
	ungatedOutput := "metrics/cpu/2026/08/19/12/cpu_20260819_120000_" + itoa64(time.Now().UTC().UnixNano()) + "_b1_compacted.parquet"

	// Operator flips back: a FRESH epoch is stamped (later than the output).
	time.Sleep(5 * time.Millisecond)
	e2, err := l.EnsureMetaOnce(ctx, MetaCompactionDeferEpoch, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("stamp e2: %v", err)
	}
	if e2 == e1 {
		t.Fatal("epoch was not re-stamped after clear")
	}
	epoch, err := time.Parse(time.RFC3339Nano, e2)
	if err != nil {
		t.Fatalf("parse e2: %v", err)
	}

	// Under the fresh epoch the ungated output is PRE-epoch → ineligible for
	// compaction until synced, and the observer refuses to track it.
	gate := NewCompactionEligibility(l, DefaultHubID, epoch, zerolog.Nop())
	got, err := gate(ctx, []string{ungatedOutput})
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if got[ungatedOutput] {
		t.Error("ungated-period output eligible for compaction before syncing; would risk its only copy")
	}
	NewCompactedOutputObserver(l, DefaultHubID, epoch, zerolog.Nop())(ungatedOutput)
	if _, err := l.Get(ctx, DefaultHubID, ungatedOutput); err == nil {
		t.Error("ungated-period output tracked as delivered; must stay legacy so it syncs once")
	}
}

// An operator-dismissed file is compaction-eligible: delivery was explicitly
// renounced, so local compaction destroys nothing the hub is owed — and
// ineligibility would wedge the partition forever on a written-off file.
func TestCompactionEligibility_DismissedFilesAreEligible(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	p := "metrics/cpu/2026/08/07/14/junk.parquet"
	gateAdd(t, l, p, func(c context.Context) error {
		if err := l.MarkInFlight(c, DefaultHubID, p); err != nil {
			return err
		}
		if err := l.MarkFailed(c, DefaultHubID, p, "boom", 1); err != nil {
			return err
		}
		_, err := l.DismissFailed(c, DefaultHubID, p)
		return err
	})

	gate := NewCompactionEligibility(l, DefaultHubID, time.Now().UTC(), zerolog.Nop())
	got, err := gate(ctx, []string{p})
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if !got[p] {
		t.Error("operator-dismissed file must be compaction-eligible")
	}
}

// Deep-review H1 (#612): an operator-dismissed row must survive the blind
// retention prune while its file exists — pruning it would let discovery
// re-track the file and resurrect the dismissed failure — and is reclaimed
// by the existence-gated sweep once the file is gone.
func TestDismissedRows_PruneExemptUntilFileGone(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	p := "metrics/cpu/2026/08/07/14/junk.parquet"
	gateAdd(t, l, p, func(c context.Context) error {
		if err := l.MarkInFlight(c, DefaultHubID, p); err != nil {
			return err
		}
		if err := l.MarkFailed(c, DefaultHubID, p, "boom", 1); err != nil {
			return err
		}
		_, err := l.DismissFailed(c, DefaultHubID, p)
		return err
	})

	if _, err := l.db.ExecContext(ctx,
		`UPDATE sync_ledger SET last_attempt = ? WHERE state = 'skipped'`,
		time.Now().UTC().AddDate(0, 0, -100)); err != nil {
		t.Fatalf("age: %v", err)
	}
	pruned, err := l.PruneSkipped(ctx, 90)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned != 0 {
		t.Errorf("prune deleted %d dismissed rows; they must be exempt while the file may exist", pruned)
	}

	swept, err := l.SweepSkippedRows(ctx, DefaultHubID,
		func(context.Context, string) (bool, error) { return false, nil })
	if err != nil || swept != 1 {
		t.Fatalf("sweep(gone) = %d, %v; want 1", swept, err)
	}
}

// Dual-role interplay (#619 review M2): the spoke-side compacted-output
// observer also fires for the node's HUB-compaction outputs in received
// namespaces. The resulting ledger row is inert: skipped, never offered
// (its namespace is excluded from discovery anyway), and prunable.
func TestTrackCompactedOutput_SpokeNamespacePathIsInert(t *testing.T) {
	ctx := context.Background()
	l := setupTestLedger(t)

	p := "child-01/factory/temps/2026/08/19/10/temps_20260819_100000_1787240000000000000_b1_compacted.parquet"
	if err := l.TrackCompactedOutput(ctx, DefaultHubID, p); err != nil {
		t.Fatalf("track: %v", err)
	}
	row, err := l.Get(ctx, DefaultHubID, p)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if row.State != StateSkipped || row.LastError != NoteCompactedOutput {
		t.Errorf("row = %s/%q, want skipped/compacted-output", row.State, row.LastError)
	}
	pending, err := l.Pending(ctx, DefaultHubID, 0)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("spoke-namespace output offered for sync: %v", pending)
	}
}
