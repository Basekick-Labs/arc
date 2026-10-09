package iceberg

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Regression tests for #1092: every Arc mechanism that removes a data file — compaction,
// retention, both branches of the delete API — left the older snapshots naming a path Arc
// had already deleted from the primary backend. Time travel to such a snapshot does not
// return stale rows, it fails the read outright:
//
//	IO Error: Cannot open file "...cpu_20261008_155058_567682000.parquet": No such file or directory
//
// A pass that removes anything now floors history to the snapshot it just committed, so the
// metadata stops advertising history it cannot serve. Flooring loses nothing readable: a
// removed path is referenced by every older snapshot, so all of them were already broken.
//
// snapshotCount is the field every assertion here lives in. Asserting the live file set
// instead would pass on a build that floors nothing, because flooring never touches it.
func snapshotCount(t *testing.T, ctx context.Context, r *costRig) int {
	t.Helper()
	return len(r.load(t, ctx).Metadata().Snapshots())
}

// TestFloorHistory_RemovalPassAtDefaultRetainLeavesOneSnapshot is THE test for this fix, and
// specifically for the blocker the adversarial pass caught: expireSnapshots returns early on
// `len(snaps) <= e.retain` BEFORE any floor flag could be read, so a floor consulted after
// that guard does nothing in the default configuration.
//
// retain is 10 here because that is the shipped default (config.go setDefaults) and the
// reproducing table in #1092 had three snapshots. Three is less than ten, so the guard is
// exactly what a naive implementation trips over, and the reported IO Error survives the fix.
func TestFloorHistory_RemovalPassAtDefaultRetainLeavesOneSnapshot(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10) // the shipped default

	live := r.write(t, "base", 3)
	r.reconcile(t, ctx, live)
	extra := r.write(t, "extra", 2)
	r.reconcile(t, ctx, append(live, extra[0]))
	r.reconcile(t, ctx, append(live, extra...))

	// Precondition: history is BELOW the configured retain, so the guard would return early
	// and nothing about this assertion is meaningful without it.
	if n := snapshotCount(t, ctx, r); n < 2 || n > 10 {
		t.Fatalf("precondition: want 2..10 snapshots so the retain guard is the thing under test, got %d", n)
	}

	// A removal pass: drop one file from the live set, as compaction or retention would.
	r.reconcile(t, ctx, append(live, extra[0]))

	if n := snapshotCount(t, ctx, r); n != 1 {
		t.Errorf("snapshots = %d, want 1: a pass that removed a path must floor history, "+
			"because every older snapshot names a file Arc deleted and time travel to it fails the read", n)
	}
}

// TestFloorHistory_AppendOnlyPassHonoursRetain is the over-expiry control. An append-only
// pass removes nothing, so every older snapshot is still readable and must survive — a fix
// that floored unconditionally would destroy working time travel, which is a regression and
// not a fix.
func TestFloorHistory_AppendOnlyPassHonoursRetain(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)

	live := r.write(t, "base", 1)
	r.reconcile(t, ctx, live)
	spare := r.write(t, "spare", 4)
	for i := range spare {
		live = append(live, spare[i])
		r.reconcile(t, ctx, live) // pure append: toRemove is empty every time
	}

	if n := snapshotCount(t, ctx, r); n < 2 {
		t.Errorf("snapshots = %d, want more than 1: append-only passes remove nothing, so their "+
			"history is still readable and retain_snapshots must govern it", n)
	}
}

// TestFloorHistory_RewritePassFloors covers the subset the plan's first draft described
// wrongly. A rewritten path is in toRemove AND toAdd and is still PRESENT in storage — only
// its bytes changed. So it is not "a file Arc deleted", yet the older snapshots' record_count
// and bounds describe content that is no longer at that path, which is the original #1092
// symptom of silently counting deleted rows. The trigger is therefore len(toRemove) > 0 and
// not len(toRemove)-len(rewritten) > 0; this test is what pins that choice.
func TestFloorHistory_RewritePassFloors(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)

	live := r.write(t, "base", 2)
	r.reconcile(t, ctx, live)
	more := r.write(t, "more", 1)
	r.reconcile(t, ctx, append(live, more...))
	if n := snapshotCount(t, ctx, r); n < 2 {
		t.Fatalf("precondition: want at least 2 snapshots before the rewrite, got %d", n)
	}

	// Same paths, different bytes: rewrite one file with a different row count so its size
	// changes. The reconciler sees (path, size) differ and takes the rewritten branch.
	writeArcStyleParquet(t, live[0], 1_752_500_000_000_000, 9)
	r.reconcile(t, ctx, append(live, more...))

	if n := snapshotCount(t, ctx, r); n != 1 {
		t.Errorf("snapshots = %d, want 1: a pass that re-registered a path whose bytes changed "+
			"must floor, because the older snapshots describe content that path no longer holds", n)
	}
}

// TestFloorHistory_RetainOneDoesNotClaimDroppedHistory is the log-correctness case. At
// retain_snapshots=1 the floor equals the configured value, so the function is doing its
// ordinary job and must not announce that it dropped history. Gating the Warn on the mode
// alone rather than on `effective < e.retain && after < before` would make this fire.
func TestFloorHistory_RetainOneDoesNotClaimDroppedHistory(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	r := newCostRig(t, 1)
	r.exp.logger = zerolog.New(&logs)

	live := r.write(t, "base", 3)
	r.reconcile(t, ctx, live)
	r.reconcile(t, ctx, live[:2]) // a removal pass, so the floor is requested

	if n := snapshotCount(t, ctx, r); n != 1 {
		t.Errorf("snapshots = %d, want 1 at retain_snapshots=1", n)
	}
	if strings.Contains(logs.String(), "floored to the current snapshot") {
		t.Errorf("claimed it floored history at retain_snapshots=1, where the floor is the "+
			"configured value and no history was dropped:\n%s", logs.String())
	}
}

// TestFloorHistory_WarnsWhenItActuallyDropsHistory is the other half: the operator whose
// history went from several snapshots to one has to be able to find out why.
func TestFloorHistory_WarnsWhenItActuallyDropsHistory(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	r := newCostRig(t, 10)
	r.exp.logger = zerolog.New(&logs)

	live := r.write(t, "base", 3)
	r.reconcile(t, ctx, live)
	more := r.write(t, "more", 1)
	r.reconcile(t, ctx, append(live, more...))
	r.reconcile(t, ctx, live) // removal pass

	if !strings.Contains(logs.String(), "floored to the current snapshot") {
		t.Errorf("dropped snapshot history without saying so; an operator has no other breadcrumb:\n%s", logs.String())
	}
}

// TestExpireModeFor pins the mapping the reconcile pass uses, so a refactor cannot quietly
// invert it.
func TestExpireModeFor(t *testing.T) {
	if got := expireModeFor(0); got != retainConfigured {
		t.Errorf("expireModeFor(0) = %v, want retainConfigured: a pass that removed nothing has readable history", got)
	}
	for _, n := range []int{1, 2, 50} {
		if got := expireModeFor(n); got != floorHistory {
			t.Errorf("expireModeFor(%d) = %v, want floorHistory", n, got)
		}
	}
}

// TestExpireSnapshots_ReportsFailureSoThePassIsNotCached is the retry contract. A failed floor
// cannot be left best-effort the way a failed retention expiry can: only a pass that REMOVES
// something asks to floor, so once the file set goes quiet the scheduler skips the measurement
// and there is no next pass to retry on. Published metadata would keep naming deleted files.
func TestExpireSnapshots_ReportsFailureSoThePassIsNotCached(t *testing.T) {
	ctx := context.Background()
	r := newCostRig(t, 10)

	live := r.write(t, "base", 3)
	r.reconcile(t, ctx, live)
	more := r.write(t, "more", 1)
	r.reconcile(t, ctx, append(live, more...))
	tbl := r.load(t, ctx)

	// Break the catalog the commit writes through, so ExpireSnapshots stages and then fails.
	r.db.Close()

	got, ok := r.exp.expireSnapshots(ctx, tbl, "mydb", "cpu", floorHistory)
	if ok {
		t.Error("reported success with the catalog closed; the scheduler would cache a fingerprint over metadata that still names deleted files")
	}
	if got != tbl {
		t.Error("returned a different table on failure; the caller publishes this one as the version hint")
	}
}
