package iceberg

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// #1611: a database DROP on one node removes the shared warehouse's objects
// and only that node's catalog rows. Another node that exported the table
// keeps rows pointing at deleted metadata.

// exportedThenDropped has b export the table to version R (passes passes),
// then a drop it through DropDatabase. b keeps its rows and, being the last
// writer, still believes its location published (isPublishedHere).
func exportedThenDropped(t *testing.T, passes int) (*warehouseRig, *Exporter, *Exporter) {
	t.Helper()
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(true)
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < passes; i++ {
		r.addFile()
		if err := r.reconcile(b); err != nil {
			t.Fatal(err)
		}
	}
	a.SetForgetDroppedRows(true) // a shared bucket: the drop leaves markers
	if err := a.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	return r, a, b
}

// Recreated and exported again by a, at a version below, equal to and above
// the one b's dead row names: b follows the warehouse in every case.
func TestDeadRow_FollowsTheRecreatedTableWhateverItsVersion(t *testing.T) {
	for _, tc := range []struct {
		name      string
		bPasses   int // b's row ends near version bPasses+1
		reexports int // a's passes on the recreated table
		restarted bool
	}{
		{"recreated below the dead row", 4, 1, false},
		{"recreated at the dead row's version", 2, 3, false},
		{"recreated above the dead row", 1, 5, false},
		// A restarted b no longer remembers publishing its row, so the
		// adoption check compares its (missing) file with the published one.
		{"recreated below the dead row, b restarted", 4, 1, true},
		{"recreated at the dead row's version, b restarted", 2, 3, true},
		{"recreated above the dead row, b restarted", 1, 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, a, b := exportedThenDropped(t, tc.bPasses)
			if tc.restarted {
				b.publishedMu.Lock()
				b.published = nil
				b.publishedMu.Unlock()
			}
			for i := 0; i < tc.reexports; i++ {
				r.addFile()
				if err := r.reconcile(a); err != nil {
					t.Fatalf("a re-export %d: %v", i, err)
				}
			}
			r.addFile()
			if err := r.reconcile(b); err != nil {
				t.Fatalf("b after the recreate: %v", err)
			}
			if f := r.forkedVersions(); len(f) != 0 {
				t.Fatalf("forked versions %v", f)
			}
			if n := r.live(b); n != len(r.files()) {
				t.Fatalf("b's table holds %d files, want %d", n, len(r.files()))
			}
		})
	}
}

// Nothing published since the drop: the dead row is forgotten only on a
// shared bucket (SetForgetDroppedRows) and only when the drop left its
// marker. Otherwise the #637 error stands (a standalone node: restore the
// warehouse).
func TestDeadRow_NothingPublished(t *testing.T) {
	for _, tc := range []struct {
		name          string
		forget        bool
		removeMarker  bool
		wantRecovered bool
	}{
		{"shared bucket, drop marker", true, false, true},
		{"standalone (no forgetting), drop marker", false, false, false},
		{"shared bucket, no marker (a restore, a wiped prefix)", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, b := exportedThenDropped(t, 2)
			b.SetForgetDroppedRows(tc.forget)
			if tc.removeMarker {
				if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))); err != nil {
					t.Fatal(err)
				}
			}
			r.addFile()
			err := r.reconcile(b)
			if tc.wantRecovered {
				if err != nil {
					t.Fatalf("reconcile: %v; want the row forgotten and the table recreated", err)
				}
				if n := r.live(b); n != len(r.files()) {
					t.Fatalf("recreated table holds %d files, want %d", n, len(r.files()))
				}
				return
			}
			if err == nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("reconcile: %v; want the #637 metadata-missing error", err)
			}
		})
	}
}

// The primary writer often never exported (another node holds the export
// lease): it has no namespace in its own catalog, yet its DropDatabase must
// still delete the shared warehouse objects and leave the marker.
func TestDropDatabase_WithoutALocalNamespaceDropsTheSharedWarehouse(t *testing.T) {
	r := newWarehouseRig(t)
	exporter, primary := r.node(true), r.node(true)
	primary.SetForgetDroppedRows(true)
	r.addFile()
	if err := r.reconcile(exporter); err != nil {
		t.Fatal(err)
	}
	if err := primary.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatalf("DropDatabase on a node that never exported: %v", err)
	}
	if _, err := os.Stat(r.metaDir()); !os.IsNotExist(err) {
		t.Fatalf("table metadata still in the warehouse after the drop (stat err %v)", err)
	}
	if _, err := os.Stat(filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))); err != nil {
		t.Fatalf("no drop marker: %v", err)
	}
	// The namespace directory is gone: no ghost database in the listings.
	if _, err := os.Stat(filepath.Join(r.root, "arc_mydb.db")); !os.IsNotExist(err) {
		t.Fatalf("namespace directory left behind (stat err %v)", err)
	}
}

// A database Iceberg never exported gets no marker.
func TestDropDatabase_NeverExportedLeavesNoMarker(t *testing.T) {
	r := newWarehouseRig(t)
	n := r.node(true)
	n.SetForgetDroppedRows(true)
	if err := n.DropDatabase(context.Background(), "neverexported"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(r.root, "arc_neverexported.db")); !os.IsNotExist(err) {
		t.Fatalf("a never-exported database left a warehouse directory (stat err %v)", err)
	}
	if _, err := os.Stat(filepath.Join(r.root, droppedMarkerRoot)); !os.IsNotExist(err) {
		t.Fatalf("a never-exported database left a drop marker (stat err %v)", err)
	}
}

// A table's drop marker is removed by its next successful publish: from then
// on a lagging node follows the published version, and a later loss of the
// table is the #637 error again.
func TestDropMarker_ClearedByTheNextPublish(t *testing.T) {
	r, a, _ := exportedThenDropped(t, 2)
	marker := filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("no marker after the drop: %v", err)
	}
	r.addFile()
	if err := r.reconcile(a); err != nil { // the recreated table's first publish
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker still there after the table was published again (stat err %v)", err)
	}
}

// Markers are written before anything is deleted: a drop whose marker write
// fails deletes nothing, and the re-run writes the marker and deletes.
func TestDropDatabase_MarkerFailureDeletesNothingAndTheRerunConverges(t *testing.T) {
	r := newWarehouseRig(t)
	exporter := r.node(true)
	r.addFile()
	if err := r.reconcile(exporter); err != nil {
		t.Fatal(err)
	}
	// A plain file where the marker root directory should be: the marker
	// write cannot create its directory.
	blocker := filepath.Join(r.root, droppedMarkerRoot)
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	primary := r.node(true)
	primary.SetForgetDroppedRows(true)
	if err := primary.DropDatabase(context.Background(), "mydb"); err == nil {
		t.Fatal("DropDatabase succeeded although its marker write failed")
	}
	if _, err := os.Stat(r.metaDir()); err != nil {
		t.Fatalf("the failed drop deleted warehouse objects: %v", err)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := primary.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))); err != nil {
		t.Fatalf("re-run wrote no marker: %v", err)
	}
	if _, err := os.Stat(r.metaDir()); !os.IsNotExist(err) {
		t.Fatalf("re-run left the table's metadata (stat err %v)", err)
	}
}

// A path under a symlinked warehouse whose directories are gone resolves in
// the warehouse's spelling, on any OS (#1611: a dropped table's row).
func TestResolveFileURI_ResolvesTheNearestExistingAncestor(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	realResolved, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	got := resolveFileURI(localFileURI(filepath.Join(link, "gone", "deeper", "00001-x.metadata.json")))
	want := localFileURI(filepath.Join(realResolved, "gone", "deeper", "00001-x.metadata.json"))
	if got != want {
		t.Fatalf("resolveFileURI = %q, want %q", got, want)
	}
}

// A catalog row whose location has no "/" (not one this exporter wrote) is
// reported, not a panic in the recovery path.
func TestDeadRow_LocationWithoutASlashIsAnError(t *testing.T) {
	r := newWarehouseRig(t)
	b := r.node(true)
	r.addFile()
	if err := r.reconcile(b); err != nil {
		t.Fatal(err)
	}
	if _, err := b.db.Exec(`UPDATE iceberg_tables SET metadata_location = 'x'`); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.tableSeed(context.Background(), "mydb", "cpu"); err == nil {
		t.Fatal("tableSeed succeeded on a row with an unusable location")
	}
}

// The node that recovers is one that published the table before the drop:
// its once-per-process "marker cleared" note must not stop it clearing the
// marker after it re-publishes the recreated table (found in the binary run).
func TestDropMarker_ClearedByAnExporterThatPublishedBeforeTheDrop(t *testing.T) {
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(true)
	a.SetForgetDroppedRows(true)
	b.SetForgetDroppedRows(true)
	r.addFile()
	if err := r.reconcile(b); err != nil { // b publishes (and notes its marker cleared)
		t.Fatal(err)
	}
	if err := a.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))
	r.addFile()
	if err := r.reconcile(b); err != nil { // dead row: forgotten, table recreated and published
		t.Fatalf("b after the drop: %v", err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("marker still there after b re-published the recreated table (stat err %v)", err)
	}
}
