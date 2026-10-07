package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
)

// Regression tests for #1106: the manifest set of an exported table grew by two manifests per
// removal pass and nothing ever shed one, so both Arc's own commit path (~1.6 ms per accumulated
// manifest per pass) and every reader's scan plan (~0.13 ms per manifest) degraded forever. A pass
// that finds the current snapshot at or above manifestCollapseThreshold now also commits a
// merge-enabled append that rewrites every data manifest into one.
//
// Four of these tests FAIL with collapseManifests neutered to a no-op — verified, not assumed.
// The other three cannot, and each says why on itself rather than borrowing credit from the group:
//
//   - BelowThresholdLeavesManifestsAlone is the over-collapse control; its obligation is to fail
//     when the threshold is lowered to 1.
//   - ShouldCollapseFailsOpen tests the decision function, not the collapse; its obligation is to
//     fail when shouldCollapse returns true on an unreadable manifest list.
//   - StaleDeletedHistoryStillReregistered is a pure regression guard on #633, which the collapse
//     now rides on top of. It passes before and after by design.

// livePaths is the invariant a collapse must preserve: the set of live data files and their sizes.
// The manifest LAYOUT is explicitly not an invariant — rearranging it is the point.
func (r *costRig) livePaths(t *testing.T, ctx context.Context) map[string]int64 {
	t.Helper()
	have, err := r.exp.tableDataFiles(ctx, r.load(t, ctx))
	if err != nil {
		t.Fatalf("tableDataFiles: %v", err)
	}
	return have
}

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestManifestCollapse_BoundsManifestGrowth is the headline: over many removal passes the manifest
// count stays bounded instead of growing by two per pass, and the live file set is untouched.
func TestManifestCollapse_BoundsManifestGrowth(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = manifestCollapseThreshold // the measurement rig defaults it off

	live := r.write(t, "base", 40)
	r.reconcile(t, ctx, live)
	before := r.livePaths(t, ctx)

	spare := r.write(t, "spare", 20)
	maxSeen := 0
	for i := 0; i < 20; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
		total, _, _ := r.manifestShape(t, ctx)
		if total > maxSeen {
			maxSeen = total
		}
	}

	// A pass reads the CURRENT snapshot, so the count can sit at threshold-1 (no collapse) and
	// then gain this pass's two manifests: threshold+1 is the ceiling, and the measured peak over
	// these 20 passes is exactly that. The per-file fallback can exceed it by one manifest per
	// added file, but this test never takes that path (see PartitionFallbackStillCollapses).
	if ceiling := manifestCollapseThreshold + 1; maxSeen > ceiling {
		t.Errorf("manifest count peaked at %d over 20 removal passes, want <= %d", maxSeen, ceiling)
	}
	after := r.livePaths(t, ctx)
	if len(after) != len(before) {
		t.Fatalf("live file count changed: %d -> %d", len(before), len(after))
	}
	// The file list is the invariant a collapse must preserve, so compare the SET, not its size:
	// a collapse that retained a dropped path and lost a live one keeps the count.
	want := make([]string, 0, len(live))
	for _, f := range live {
		want = append(want, fileURI(f))
	}
	sort.Strings(want)
	got := sortedKeys(after)
	if len(got) != len(want) {
		t.Fatalf("live set size %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("live set differs at %d: have %s, want %s", i, got[i], want[i])
		}
	}
}

// TestManifestCollapse_RemovalOnlyPassCollapses covers the pass shape that has no files to add:
// retention alone, on a measurement that gained nothing since the last tick. The merge producer is
// reachable only through AddFiles, so an earlier design welded the collapse to the add path and
// would have skipped exactly these passes.
func TestManifestCollapse_RemovalOnlyPassCollapses(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = 0 // grow the pile first, with no collapse

	live := r.write(t, "base", 40)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 16)
	for i := 0; i < 16; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
	}
	grown, _, _ := r.manifestShape(t, ctx)
	if grown < manifestCollapseThreshold {
		t.Fatalf("setup did not grow the manifest set past the threshold: %d", grown)
	}

	// A pass that only removes: no file is added back.
	r.exp.collapseThreshold = manifestCollapseThreshold
	dropped := live[0]
	live = live[1:]
	r.reconcile(t, ctx, live)

	total, deletedOnly, _ := r.manifestShape(t, ctx)
	if total != 1 {
		t.Errorf("removal-only pass left %d manifests (was %d), want exactly 1", total, grown)
	}
	if deletedOnly != 0 {
		t.Errorf("collapse left %d DELETED-only manifests behind", deletedOnly)
	}
	have := r.livePaths(t, ctx)
	if _, ok := have[fileURI(dropped)]; ok {
		t.Errorf("removed file is still live after the collapse: %s", dropped)
	}
	if len(have) != len(live) {
		t.Errorf("live file count %d, want %d", len(have), len(live))
	}
}

// TestManifestCollapse_BelowThresholdLeavesManifestsAlone is the inverse control: the collapse must
// not fire on every pass, or an ordinary pass would rewrite the whole live set as metadata.
//
// NOTE: unlike every other test here this one PASSES with the collapse neutered — it is the
// over-collapse control, and nothing can make it fail that way. Its proof obligation is the
// opposite: it must FAIL when collapseThreshold is lowered to 1.
func TestManifestCollapse_BelowThresholdLeavesManifestsAlone(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = manifestCollapseThreshold // the measurement rig defaults it off

	live := r.write(t, "base", 20)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 3)
	for i := 0; i < 3; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
	}

	total, _, _ := r.manifestShape(t, ctx)
	// 3 removal passes past the initial single manifest, nowhere near the threshold of 12.
	if want := 1 + 2*3; total != want {
		t.Errorf("manifests = %d after 3 sub-threshold passes, want %d (a collapse fired early)", total, want)
	}
}

// TestManifestCollapse_StaleDeletedHistoryStillReregistered guards the #633 path, which the
// collapse now rides on top of: a file rewritten in place (same path, fewer rows) must still be
// re-registered in a pass that also collapses.
//
// It carries NO assertion about the manifest count, because #633's own addFilesMerging already
// merges the set — such an assertion would pass with the collapse neutered and prove nothing (the
// failure mode #1107's review caught). This is a regression guard, and only that.
func TestManifestCollapse_StaleDeletedHistoryStillReregistered(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = 0

	live := r.write(t, "base", 20)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 16)
	for i := 0; i < 16; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
	}
	// The delete API's partial-match branch: same path, fewer rows, new size. The path is in both
	// toRemove and toAdd, so iceberg-go refuses the add and the #633 branch takes over.
	r.exp.collapseThreshold = manifestCollapseThreshold
	target := live[len(live)-1]
	rewriteInPlace(t, target, 1_752_500_000_000_000, 1)
	r.reconcile(t, ctx, live)

	have := r.livePaths(t, ctx)
	st, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := have[fileURI(target)]; !ok {
		t.Fatalf("rewritten file was dropped from the table: %s", target)
	} else if got != st.Size() {
		t.Errorf("rewritten file registered at size %d, want %d (stale entry)", got, st.Size())
	}
	if len(have) != len(live) {
		t.Errorf("live file count %d, want %d", len(have), len(live))
	}
}

// TestManifestCollapse_PartitionFallbackStillCollapses covers the per-file fallback, which stages
// one snapshot AND one manifest per added file and is sticky — a day-straddling file is never
// registered, so it is back in the diff on every later pass and the measurement takes this path for
// good. It is the package's heaviest manifest producer and must not be the path exempt from the
// collapse.
func TestManifestCollapse_PartitionFallbackStillCollapses(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = 0

	live := r.write(t, "base", 20)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 16)
	for i := 0; i < 16; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
	}
	grown, _, _ := r.manifestShape(t, ctx)

	// A file whose time range spans more than one UTC day: iceberg-go cannot infer its day()
	// partition, the batch add fails, and the pass falls back to one AddFiles per file.
	r.exp.collapseThreshold = manifestCollapseThreshold
	bad := filepath.Join(r.dataDir, "straddler.parquet")
	writeStraddlingParquet(t, bad, 1_752_500_000_000_000)
	good := r.write(t, "late", 1)
	live = append(live[1:], bad, good[0])
	r.reconcile(t, ctx, live)

	have := r.livePaths(t, ctx)
	if _, ok := have[fileURI(bad)]; ok {
		t.Errorf("straddling file was registered: %s", bad)
	}
	if _, ok := have[fileURI(good[0])]; !ok {
		t.Errorf("good file in the same pass was not registered: %s", good[0])
	}
	if total, _, _ := r.manifestShape(t, ctx); total != 1 {
		t.Errorf("per-file fallback left %d manifests (was %d), want exactly 1", total, grown)
	}
}

// TestManifestCollapse_EmptyingPassSucceeds is the regression guard for the one pass the collapse
// must refuse. The scheduler reconciles a measurement whose data files are all gone to an EMPTY
// table rather than leave it pointing at deleted paths; that pass stages an overwrite in which
// every entry is DELETED and attributed to the overwrite snapshot, so the merge would shed all of
// them and write an empty manifest — which iceberg-go refuses. The pass would then fail, the
// scheduler would cache no fingerprint for a failed pass, and the same pass would fail on every
// tick forever, leaving the table permanently pointing at deleted files.
func TestManifestCollapse_EmptyingPassSucceeds(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = 0 // grow the pile first, with no collapse

	live := r.write(t, "base", 20)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 16)
	for i := 0; i < 16; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
	}
	if grown, _, _ := r.manifestShape(t, ctx); grown < manifestCollapseThreshold {
		t.Fatalf("setup did not grow the manifest set past the threshold: %d", grown)
	}

	// Every data file gone. This is scheduler.go's all-files-gone branch: nil file list, nil
	// schema, the existing table reconciled to empty.
	r.exp.collapseThreshold = manifestCollapseThreshold
	if _, err := r.exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", ArcSchema{}, nil); err != nil {
		t.Fatalf("emptying pass failed: %v", err)
	}
	if have := r.livePaths(t, ctx); len(have) != 0 {
		t.Errorf("table still references %d files after being reconciled to empty", len(have))
	}
	// And the pass is repeatable — the scheduler re-enters it until it succeeds.
	if _, err := r.exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", ArcSchema{}, nil); err != nil {
		t.Fatalf("second emptying pass failed: %v", err)
	}

	// A table that comes back from empty still collapses once it is over the threshold again.
	revived := r.write(t, "revived", 4)
	r.reconcile(t, ctx, revived)
	if have := r.livePaths(t, ctx); len(have) != len(revived) {
		t.Errorf("revived table has %d live files, want %d", len(have), len(revived))
	}
}

// TestManifestCollapse_AllSurvivorsSkippedDoesNotCollapse closes the gap the caller's guard cannot
// see. shouldCollapse is given len(want) — the files the pass INTENDS to register — but the
// per-file fallback skips the ones iceberg-go cannot partition, and a skipped file never enters the
// table. A table at the threshold whose only remaining file is day-straddling therefore removes
// everything, registers nothing, and would merge a snapshot with no live entries: the same
// forever-failing wedge as TestManifestCollapse_EmptyingPassSucceeds, reached by a different route.
func TestManifestCollapse_AllSurvivorsSkippedDoesNotCollapse(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = 0 // grow the pile first, with no collapse

	live := r.write(t, "base", 20)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 16)
	for i := 0; i < 16; i++ {
		live = append(live[1:], spare[i])
		r.reconcile(t, ctx, live)
	}
	if grown, _, _ := r.manifestShape(t, ctx); grown < manifestCollapseThreshold {
		t.Fatalf("setup did not grow the manifest set past the threshold: %d", grown)
	}

	// Storage now holds exactly ONE file, and it straddles a UTC day. len(want) == 1, so the
	// caller's liveAfter guard passes, but the file is skipped and the table ends up empty.
	r.exp.collapseThreshold = manifestCollapseThreshold
	bad := filepath.Join(r.dataDir, "lone-straddler.parquet")
	writeStraddlingParquet(t, bad, 1_752_500_000_000_000)
	r.reconcile(t, ctx, []string{bad})

	if have := r.livePaths(t, ctx); len(have) != 0 {
		t.Errorf("table references %d files, want 0 (the only candidate was unmappable)", len(have))
	}
	// Repeatable, like any pass the scheduler re-enters until the file set changes.
	r.reconcile(t, ctx, []string{bad})
}

// TestManifestCollapse_ShouldCollapseFailsOpen: a table whose manifest list cannot be read is not a
// table to start rewriting manifests on. The decision returns false and the pass proceeds.
func TestManifestCollapse_ShouldCollapseFailsOpen(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = manifestCollapseThreshold // the measurement rig defaults it off

	live := r.write(t, "base", 5)
	r.reconcile(t, ctx, live)
	tbl := r.load(t, ctx)
	if r.exp.shouldCollapse(ctx, tbl, 5, "mydb", "cpu") {
		t.Fatal("shouldCollapse true on a one-manifest table")
	}
	// liveAfter == 0 is refused whatever the manifest count. Checked here, while the manifest list
	// is still READABLE — after the truncation below, a false return proves nothing about this
	// guard. The emptying pass is covered end to end by TestManifestCollapse_EmptyingPassSucceeds.
	r.exp.collapseThreshold = 1
	if !r.exp.shouldCollapse(ctx, tbl, 5, "mydb", "cpu") {
		t.Fatal("shouldCollapse false at threshold 1 with a live file: the rest of this test proves nothing")
	}
	if r.exp.shouldCollapse(ctx, tbl, 0, "mydb", "cpu") {
		t.Error("shouldCollapse returned true for a pass that empties the table")
	}

	// Truncate the current snapshot's manifest list: present, unreadable as avro.
	snapList := trimFileScheme(tbl.CurrentSnapshot().ManifestList)
	if err := os.WriteFile(snapList, []byte("not avro"), 0o600); err != nil {
		t.Fatal(err)
	}
	if r.exp.shouldCollapse(ctx, r.load(t, ctx), 5, "mydb", "cpu") {
		t.Error("shouldCollapse returned true despite an unreadable manifest list")
	}

	// And on a freshly created table, which has no snapshot yet — shouldCollapse runs on every
	// pass including the first.
	r2 := newCostRig(t, 10)
	r2.exp.collapseThreshold = 1
	seed := r2.write(t, "base", 1)
	sc, err := UnionSchema(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := r2.exp.EnsureTable(ctx, "mydb", "other", sc)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.CurrentSnapshot() != nil {
		t.Fatal("expected a freshly created table to have no snapshot")
	}
	if r2.exp.shouldCollapse(ctx, fresh, 1, "mydb", "other") {
		t.Error("shouldCollapse returned true on a table with no snapshot")
	}
}

// TestManifestCollapse_MergeOffRestoresIcebergDefaults: manifestMergeOn's tuning (min-count 2,
// 1 GiB target) must not be left on the table as its standing merge policy — v0.6.0 cannot remove
// a property, so manifestMergeOff writes iceberg-go's own defaults back. Before the collapse only
// #633 tables ever carried these; now nearly every exported table commits through this path.
func TestManifestCollapse_MergeOffRestoresIcebergDefaults(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)
	r.exp.collapseThreshold = 1 // the table has one manifest after creation, so collapse on the first removal pass

	live := r.write(t, "base", 10)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 1)
	live = append(live[1:], spare[0])
	r.reconcile(t, ctx, live)

	props := r.load(t, ctx).Properties()
	for key, want := range map[string]string{
		icetable.ManifestMergeEnabledKey:    "false",
		icetable.ManifestMinMergeCountKey:   strconv.Itoa(icetable.ManifestMinMergeCountDefault),
		icetable.ManifestTargetSizeBytesKey: strconv.Itoa(icetable.ManifestTargetSizeBytesDefault),
	} {
		if got := props[key]; got != want {
			t.Errorf("table property %s = %q, want %q", key, got, want)
		}
	}
}
