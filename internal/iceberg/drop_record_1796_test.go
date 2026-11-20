package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1796, #1799: the drop records of a shared bucket (shared_drop.go) after a
// restore, a failed drop, and a table removed from inside its own pass.

// snapshotDirs copies every file under the given directories of the rig, as
// a backup would.
func (r *warehouseRig) snapshotDirs(dirs ...string) map[string][]byte {
	r.t.Helper()
	saved := map[string][]byte{}
	for _, d := range dirs {
		_ = filepath.WalkDir(filepath.Join(r.root, d), func(p string, e os.DirEntry, err error) error {
			if err == nil && !e.IsDir() {
				saved[p], _ = os.ReadFile(p)
			}
			return nil
		})
	}
	return saved
}

// restore writes a snapshot back, as a restore does (it adds; it does not
// remove what is there). hintAhead makes every version hint name the version
// after the one it named: the hint was copied after the v<N> files were
// listed (#1613).
func (r *warehouseRig) restore(saved map[string][]byte, hintAhead bool) {
	r.t.Helper()
	for p, body := range saved {
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			r.t.Fatal(err)
		}
		if hintAhead && strings.HasSuffix(p, "version-hint.text") {
			n, _ := strconv.Atoi(strings.TrimSpace(string(body)))
			body = []byte(strconv.Itoa(n + 1))
		}
		if err := os.WriteFile(p, body, 0o600); err != nil {
			r.t.Fatal(err)
		}
	}
	// The restore's data files are newer than the run's own counter.
	for p := range saved {
		if base := filepath.Base(p); strings.HasPrefix(base, "f") && strings.HasSuffix(base, ".parquet") {
			if n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(base, "f"), ".parquet")); err == nil && n > r.n {
				r.n = n
			}
		}
	}
}

// A database backed up, deleted on a shared bucket, and restored. The drop's
// record and the table's marker outlive the restore (backups do not copy the
// reserved root, and the restore does not remove them: #1796). Every later
// pass works and the table ends with every file, also when the restored
// hint names a version the backup did not list, which failed every pass for
// good before (#1799). The table is exported again from its data: what
// predates a drop is not kept (shared_drop.go's header).
func TestDropRecord1799_ARestoreAfterADrop(t *testing.T) {
	for _, hintAhead := range []bool{false, true} {
		t.Run("hintAhead="+strconv.FormatBool(hintAhead), func(t *testing.T) {
			r, a, b := sharedDropRig(t)
			saved := r.snapshotDirs("arc_mydb.db", "mydb")
			r.deleteDatabase(a)
			r.restore(saved, hintAhead)
			if _, err := os.Stat(filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))); err != nil {
				t.Fatalf("the test's premise: the drop's marker outlives the restore (%v)", err)
			}
			for i := 0; i < 3; i++ {
				r.addFile()
				if err := r.passOf(b); err != nil {
					t.Fatalf("pass %d after the restore: %v", i, err)
				}
			}
			if n := r.live(b); n != len(r.files()) {
				t.Fatalf("the restored table holds %d files, want %d", n, len(r.files()))
			}
			// And a node that takes over the export, with no row of its own.
			c := r.node(true)
			c.SetForgetDroppedRows(true)
			r.addFile()
			if err := r.passOf(c); err != nil {
				t.Fatalf("a node taking over after the restore: %v", err)
			}
			if n := r.live(c); n != len(r.files()) {
				t.Fatalf("the taken-over table holds %d files, want %d", n, len(r.files()))
			}
		})
	}
}

// The same restore when the drop left a residue the exporter never got to
// remove (its pass overlapped the drop and did not look at its end): the
// residue names manifests the backup does not hold. Kept, the drop record is
// what lets the exporter recognise and remove it; it must not be cleared by
// the restore (the #1796 review's B1).
func TestDropRecord1796_ARestoreOverAResidue(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	saved := r.snapshotDirs("arc_mydb.db", "mydb")
	for i := 0; i < 2; i++ {
		r.addFile()
		if err := r.passOf(b); err != nil {
			t.Fatal(err)
		}
	}
	r.addFile()
	current, sc := r.files(), r.schemaNow()
	b.SetForgetDroppedRows(false) // the pass does not look at its end
	_ = r.overlap(a, func(canCommit func() bool) error {
		_, err := b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current, canCommit: canCommit})
		return err
	})
	b.SetForgetDroppedRows(true)
	r.restore(saved, false)
	for i := 0; i < 3; i++ {
		r.addFile()
		if err := r.passOf(b); err != nil {
			t.Fatalf("pass %d after a restore over a residue: %v", i, err)
		}
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("the table holds %d files, want %d", n, len(r.files()))
	}
}

// When does a marker mean a drop may be running (the #1613 fallbacks are off
// while it does)?
func TestDropRecord1799_DropInProgress(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	marker := filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))
	setMarker := func(at time.Time) {
		_ = os.MkdirAll(filepath.Dir(marker), 0o700)
		if err := os.WriteFile(marker, []byte(at.UTC().Format(time.RFC3339)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	setRecord := func(rec dropRecord) {
		if err := a.writeDropRecord(ctx, "mydb", rec); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	cases := []struct {
		name  string
		setup func()
		want  bool
	}{
		{"no marker", func() { _ = os.Remove(marker) }, false},
		{"a marker and no record (a drop by a node without records)", func() {
			setMarker(now)
			_ = os.Remove(filepath.Join(r.root, filepath.FromSlash(dropRecordKey("mydb"))))
		}, true},
		{"a marker of a finished drop", func() {
			setMarker(now.Add(-time.Minute))
			setRecord(dropRecord{ID: "x", Started: now.Add(-2 * time.Minute), Done: true, Finished: now})
		}, false},
		{"a marker newer than the finished drop (a later drop without a record)", func() {
			setMarker(now.Add(time.Minute))
			setRecord(dropRecord{ID: "x", Started: now.Add(-2 * time.Minute), Done: true, Finished: now})
		}, true},
		{"a drop that is running", func() {
			setMarker(now)
			setRecord(dropRecord{ID: "x", Started: now})
		}, true},
		{"a drop that died long ago", func() {
			setMarker(now.Add(-time.Hour))
			setRecord(dropRecord{ID: "x", Started: now.Add(-time.Hour)})
		}, false},
		{"a marker that cannot be read", func() {
			if err := os.WriteFile(marker, []byte("garbage"), 0o600); err != nil {
				t.Fatal(err)
			}
			setRecord(dropRecord{ID: "x", Started: now.Add(-2 * time.Minute), Done: true, Finished: now})
		}, true},
		{"a record that cannot be read", func() {
			setMarker(now.Add(-time.Minute))
			if err := r.backend.Write(ctx, dropRecordKey("mydb"), []byte("{")); err != nil {
				t.Fatal(err)
			}
		}, true},
	}
	for _, c := range cases {
		c.setup()
		if got := b.dropInProgress(ctx, "mydb", "arc_mydb", "cpu"); got != c.want {
			t.Errorf("%s: dropInProgress = %v, want %v", c.name, got, c.want)
		}
	}
	// markerSet, for a rowless node's colocated adoption (#1676), is the
	// marker alone, whatever became of the drop.
	setMarker(now.Add(-time.Minute))
	setRecord(dropRecord{ID: "x", Started: now.Add(-2 * time.Minute), Done: true, Finished: now})
	if !b.markerSet(ctx, "arc_mydb", "cpu") {
		t.Error("markerSet ignores a marker because its drop is over")
	}
}

// A table removed because its database was dropped on another node gets a
// marker stamped with when that drop stopped, not with the time of the
// removal: it must not read as a drop still running.
func TestDropRecord1799_ARemovalsMarkerIsTheDrops(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	rec, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	a.finishDropRecord(ctx, "mydb", rec)
	time.Sleep(1100 * time.Millisecond) // the removal is in a later second than the drop's end
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))); err != nil {
		t.Fatalf("the removal wrote no marker: %v", err)
	}
	if b.dropInProgress(ctx, "mydb", "arc_mydb", "cpu") {
		t.Fatal("the marker of a removal reads as a drop running")
	}
}

// A drop that fails has stopped deleting: its record is finished, so the
// database's export does not wait for it, unless it failed on its context
// (a backend need not end a delete when its context does): then the record
// stays running until it is stale.
func TestDropRecord1796_AFailedDropFinishesItsRecord(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	// The table's marker cannot be written (a file where its directory goes):
	// the drop fails before it deletes anything.
	blocker := filepath.Join(r.root, droppedMarkerRoot, "arc_mydb")
	if err := os.MkdirAll(filepath.Dir(blocker), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.deleteData(); err != nil {
		t.Fatal(err)
	}
	if err := a.DropDatabase(ctx, "mydb"); err == nil {
		t.Fatal("the test's premise: the drop fails")
	}
	rec, exists := r.dropRecord(a)
	if !exists || !rec.Done || rec.Finished.IsZero() {
		t.Fatalf("after a failed drop the record is exists=%v done=%v finished=%v, want finished", exists, rec.Done, rec.Finished)
	}
	// The export does not wait: the table predating the drop is removed.
	_ = os.Remove(blocker)
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil {
		t.Fatalf("a pass after a failed drop: %v", err)
	}
	r.goneFromStorage("arc_mydb.db")

	// On its context: the record stays running.
	r2, a2, _ := sharedDropRig(t)
	old := dropRecordStale
	dropRecordStale = 300 * time.Millisecond
	t.Cleanup(func() { dropRecordStale = old })
	if err := r2.deleteData(); err != nil {
		t.Fatal(err)
	}
	a2.dropAfterListing = func() { time.Sleep(200 * time.Millisecond) }
	if err := a2.DropDatabase(ctx, "mydb"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the test's premise: the drop runs out of time (%v)", err)
	}
	if rec, exists := r2.dropRecord(a2); !exists || rec.Done {
		t.Fatalf("a drop that ran out of time finished its record (exists=%v done=%v)", exists, rec.Done)
	}
}

// A table removed at the start of its own pass is built again from the files
// as they are, not from what the scheduler cached of the removed table: the
// database written again with a column of another type is exported with it.
func TestDropRecord1796_TheRemovingPassDoesNotUseTheRemovedTablesSchema(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	sched := NewScheduler(SchedulerConfig{Exporter: b, Source: NewStorageWalkSource(r.backend, "arc", zerolog.Nop()),
		Interval: time.Hour, Logger: zerolog.Nop()})
	m, key := Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"
	if _, err := sched.reconcileOneRecovered(ctx, m, key); err != nil {
		t.Fatal(err)
	}
	if _, cached := sched.state[key]; !cached {
		t.Fatal("the test's premise: the scheduler cached the table")
	}
	r.deleteDatabase(a)
	// Written again, "value" now a string.
	for i := 0; i < 2; i++ {
		r.n++
		writeStringValueParquet(t, filepath.Join(r.root, fmt.Sprintf("mydb/cpu/2023/11/14/22/f%03d.parquet", r.n)), r.base+int64(r.n)*1_000_000, 5)
	}
	var last error
	for i := 0; i < 3; i++ {
		_, last = sched.reconcileOneRecovered(ctx, m, key)
	}
	if last != nil {
		t.Fatalf("the passes over the database written again: %v", last)
	}
	tbl, err := b.catalog.LoadTable(ctx, b.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	f, ok := tbl.Schema().FindFieldByName("value")
	if !ok || f.Type.String() != "string" {
		t.Fatalf("the table built after the removal has value %v (found %v), want string: it was built from the removed table's schema", f.Type, ok)
	}
	if n := r.live(b); n != 2 {
		t.Fatalf("the table holds %d files, want 2", n)
	}
}

// What a former exporter leaves when it publishes during a drop and never
// looks at its end (it crashed, or lost the export): the node that takes over
// exports beside it, and the orphan sweep collects it once it is older than
// the sweep's grace. A node with no row follows the new table.
func TestDropRecord1796_ATakeoverResidueIsCollected(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	sc := r.schemaNow()
	current := r.addFile()
	b.SetForgetDroppedRows(false)
	_ = r.overlap(a, func(canCommit func() bool) error {
		_, err := b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current, canCommit: canCommit})
		return err
	})
	listing := func() []string {
		es, _ := os.ReadDir(r.metaDir())
		var n []string
		for _, e := range es {
			n = append(n, e.Name())
		}
		sort.Strings(n)
		return n
	}
	residue := listing()
	if len(residue) == 0 {
		t.Fatal("the test's premise: a residue")
	}
	aged := time.Now().Add(-2 * minOrphanGrace)
	for _, n := range residue {
		_ = os.Chtimes(filepath.Join(r.metaDir(), n), aged, aged)
	}
	c := r.node(true)
	c.SetForgetDroppedRows(true)
	for i := 0; i < 4; i++ {
		r.addFile()
		if err := r.passOf(c); err != nil {
			t.Fatalf("c's pass %d: %v", i, err)
		}
	}
	ctbl, err := c.catalog.LoadTable(ctx, c.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range residue {
		p := filepath.Join(r.metaDir(), n)
		body, err := os.ReadFile(p)
		if err != nil {
			continue // collected
		}
		if strings.HasSuffix(n, ".metadata.json") && strings.Contains(string(body), ctbl.Metadata().TableUUID().String()) {
			continue // the new table's own file under the same name (its v<N>)
		}
		t.Errorf("residue %s is still there after the new exporter's passes", n)
	}
	d := r.node(true)
	d.SetForgetDroppedRows(true)
	if err := d.adoptBeforeLoad(ctx, "mydb", "cpu"); err != nil {
		t.Fatal(err)
	}
	dtbl, err := d.catalog.LoadTable(ctx, d.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if dtbl.Metadata().TableUUID() != ctbl.Metadata().TableUUID() {
		t.Fatal("a node with no row follows the residue, not the new exporter's table")
	}
}
