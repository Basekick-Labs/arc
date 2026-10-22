package cluster

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/rs/zerolog"
)

// unregisterAs deletes key from the manifest with reason, as a committed
// compaction does for its inputs ("compaction:<job>"); no delete callback is
// wired, so the local copy stays, as it does until the unlink runs.
func (r *staleRigT) unregisterAs(key, reason string) {
	r.t.Helper()
	raw, _ := json.Marshal(raft.DeleteFilePayload{Path: key, Reason: reason})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandDeleteFile, Payload: raw}, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
}

// A copy this node saw registered and the manifest has since deleted is
// reported; a listed copy, a file never registered, and a path registered
// again are not.
func TestLocalCopyDeleted(t *testing.T) {
	r := staleRig(t)
	gone, kept, again := staleHour+"gone.parquet", staleHour+"kept.parquet", staleHour+"again.parquet"
	never := staleHour + "never.parquet"
	r.register(gone, "rows gone")
	r.register(kept, "rows kept")
	r.register(again, "rows again")
	r.write(never, "rows never")
	r.pass()
	r.unregisterAs(gone, "compaction:job-1")
	r.unregisterAs(again, "retention")
	r.register(again, "rows again")

	for key, want := range map[string]bool{gone: true, kept: false, again: false, never: false} {
		if got := r.c.LocalCopyDeleted(key); got != want {
			t.Errorf("LocalCopyDeleted(%s) = %v, want %v", filepath.Base(key), got, want)
		}
	}
	if !r.c.TracksLocalCopies() {
		t.Error("TracksLocalCopies = false on a local node with replication and catch-up")
	}
}

// The registration is not applied here yet: the journal saw it at a later
// index than this FSM has applied, so its absence from the manifest proves
// nothing and the copy is kept in the export.
func TestLocalCopyDeletedWaitsForTheRegistrationToApply(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "ahead.parquet"
	r.register(key, "rows")
	r.pass()
	r.unregisterAs(key, "retention")
	seen := r.c.stale.seen.Load()
	seen.update(key, func(old seenRecord, _ bool) (seenRecord, bool) {
		old.lsn = r.c.raftNode.FSM().AppliedDone() + 100
		return old, true
	})
	if r.c.LocalCopyDeleted(key) {
		t.Fatal("a copy whose registration this node has not applied reads as deleted")
	}
}

// Without the journal (not running, or before it is open) nothing is
// reported, so the export behaves as before rather than dropping files.
func TestLocalCopyDeletedWithoutTracking(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows")
	r.pass()
	r.unregisterAs(key, "retention")
	r.c.stale.seen.Store(nil)
	if r.c.LocalCopyDeleted(key) {
		t.Fatal("reported a deleted copy with no journal")
	}
}

// The staged compaction window (#1207) seen by the Iceberg walk: the output
// is in place and registered, the inputs are deleted from the manifest but
// still on this node's disk. The export must publish the output alone; on
// main it published all three, doubling the inputs' rows.
func TestIcebergWalkLeavesOutDeletedLocalCopies(t *testing.T) {
	r := staleRig(t)
	in1, in2 := staleHour+"in1.parquet", staleHour+"in2.parquet"
	out := staleHour + "cpu_compacted.parquet"
	r.register(in1, "rows 1")
	r.register(in2, "rows 2")
	r.pass()
	r.register(out, "rows 1 and 2")
	r.unregisterAs(in1, "compaction:job-1")
	r.unregisterAs(in2, "compaction:job-1")

	src := iceberg.NewStorageWalkSource(r.c.storage, "arc", zerolog.Nop())
	m := iceberg.Measurement{Database: "testdb", Measurement: "cpu"}
	keys := func() []string {
		t.Helper()
		files, _, err := src.FilesAndLocal(context.Background(), m)
		if err != nil {
			t.Fatal(err)
		}
		var ks []string
		for _, f := range files {
			ks = append(ks, filepath.Base(f.PhysicalPath))
		}
		sort.Strings(ks)
		return ks
	}
	if got := keys(); len(got) != 3 {
		t.Fatalf("without the filter the walk lists %v; the test needs the window open", got)
	}
	src.SetDeletedHere(r.c.LocalCopyDeleted)
	if got := keys(); len(got) != 1 || got[0] != "cpu_compacted.parquet" {
		t.Fatalf("walk = %v, want only the compaction output", got)
	}
}

// A different file written at a path since the one seen registered was
// deleted is not that copy: it stays in the export, as the sweep keeps it.
func TestLocalCopyDeletedOnlyTheFileSeenRegistered(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "replaced.parquet"
	r.register(key, "rows")
	r.pass()
	r.unregisterAs(key, "retention")
	if !r.c.LocalCopyDeleted(key) {
		t.Fatal("the deleted copy is not reported")
	}
	r.write(key, "another file, not registered yet")
	if r.c.LocalCopyDeleted(key) {
		t.Fatal("a different file at the path reads as the deleted copy")
	}
}

// Too many deleted copies to believe (a manifest rolled back in a recovery
// looks like this): the export keeps them all, by the sweep's own rule, and
// also while the sweep itself reports its breaker tripped.
func TestLocalCopyDeletedBreaker(t *testing.T) {
	r := staleRig(t)
	oldMin, oldEvery := staleLocalBreakerMin, staleExportBreakerEvery
	staleLocalBreakerMin, staleExportBreakerEvery = 2, 0
	t.Cleanup(func() { staleLocalBreakerMin, staleExportBreakerEvery = oldMin, oldEvery })
	a, b, c := staleHour+"a.parquet", staleHour+"b.parquet", staleHour+"c.parquet"
	r.register(a, "rows a")
	r.register(b, "rows b")
	r.register(c, "rows c")
	r.pass()
	r.unregisterAs(a, "retention")
	if !r.c.LocalCopyDeleted(a) {
		t.Fatal("one deleted copy of three is under the breaker and must be reported")
	}
	r.unregisterAs(b, "retention")
	if r.c.LocalCopyDeleted(a) || r.c.LocalCopyDeleted(b) {
		t.Fatal("two of three deleted (over half, at the minimum): the breaker must keep them")
	}

	r2 := staleRig(t)
	key := staleHour + "x.parquet"
	r2.register(key, "rows")
	r2.pass()
	r2.unregisterAs(key, "retention")
	r2.c.setStaleStatus(StaleLocalStatus{Blocked: true})
	if r2.c.LocalCopyDeleted(key) {
		t.Fatal("reported a deleted copy while the sweep's breaker is tripped")
	}
}
