package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
)

// otherNode is a second exporter over the rig's warehouse, with its own
// catalog: another process exporting the same bucket.
func (r *appendRig) otherNode() *Exporter {
	r.t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(r.t.TempDir(), "other.db"))
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { db.Close() })
	other, err := NewExporter(db, r.be, "file://"+r.root, "arc", 0, zerolog.Nop())
	if err != nil {
		r.t.Fatal(err)
	}
	other.SetWarehouseAuthoritative(true) // as every production node is (main.go)
	return other
}

// passOn runs a reconcile pass of the measurement on exp, over the current
// listing.
func (r *appendRig) passOn(exp *Exporter) reconcileResult {
	r.t.Helper()
	ctx := context.Background()
	files, _, err := r.src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		r.t.Fatal(err)
	}
	sc, err := r.sched.unionFileSchemas(ctx, files)
	if err != nil {
		r.t.Fatal(err)
	}
	res, err := exp.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: files})
	if err != nil {
		r.t.Fatal(err)
	}
	return res
}

// #1795, the sequence measured in the two-node run: node A's ingest reports a
// file; node B's pass lists the bucket and registers it; node A's ingest
// export adopts B's published version and appends the file again.
func TestDoubleRegistration1795_AnAppendAfterAnotherNodesPass(t *testing.T) {
	r := newAppendRig(t)
	r.exp.SetWarehouseAuthoritative(true)
	first := r.settled()

	f := r.newFile()
	r.report(f) // A's ingest flushed it

	other := r.otherNode()
	if res := r.passOn(other); res.commits == 0 {
		t.Fatalf("node B's pass committed nothing: it did not register %s, the test shows nothing", f)
	}

	listed := r.listings()
	r.serve() // A's ingest export
	r.wantOnce(first, f)
	if got := r.listings(); got != listed+1 {
		t.Errorf("listings %d → %d: A's export appended onto B's version instead of running a pass", listed, got)
	}
}

// holdTwice adds a second entry for a file the table already holds, as the
// two-exporter sequence did before #1795, and publishes it.
func (r *appendRig) holdTwice(name string) {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	live, err := r.exp.tableLiveFiles(ctx, tbl)
	if err != nil {
		r.t.Fatal(err)
	}
	var df iceberg.DataFile
	for p, d := range live {
		if filepath.Base(p) == name {
			df = d
		}
	}
	if df == nil {
		r.t.Fatalf("%s is not in the table", name)
	}
	txn := tbl.NewTransaction()
	if err := txn.AddDataFiles(ctx, []iceberg.DataFile{df}, nil, icetable.WithoutDuplicateCheck()); err != nil {
		r.t.Fatal(err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	if !r.exp.writeVersionHint(ctx, committed) {
		r.t.Fatal("publish of the duplicate failed")
	}
	if got := r.entries()[name]; got != 2 {
		r.t.Fatalf("%s is in the table %d times after holdTwice, want 2", name, got)
	}
}

func (r *appendRig) snapshotCount() int {
	r.t.Helper()
	tbl, err := r.exp.catalog.LoadTable(context.Background(), r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	return len(tbl.Metadata().Snapshots())
}

// The detection: a path held twice is found, a path held once is not.
func TestDoubleRegistration1795_TheDuplicateIsFound(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	f := r.newFile()
	r.report(f)
	r.serve()
	r.holdTwice(f)
	ctx := context.Background()
	tbl, err := r.exp.catalog.LoadTable(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	live, dups, err := r.exp.tableLiveEntries(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 || len(dups) != 1 {
		t.Fatalf("%d live paths, %d held more than once; want 2 and 1 (%s)", len(live), len(dups), f)
	}
	for p, entries := range dups {
		if filepath.Base(p) != f || len(entries) != 2 {
			t.Fatalf("held more than once: %s ×%d, want %s ×2 (and %s once)", filepath.Base(p), len(entries), f, first)
		}
	}
}

// A table that holds a path twice, its file still in storage: the next pass
// holds it once, as one commit, and floors the history that held it twice.
func TestDoubleRegistration1795_APassRepairsAPathHeldTwice(t *testing.T) {
	r := newAppendRig(t)
	r.exp.retain = 10
	first := r.settled()
	f := r.newFile()
	r.report(f)
	r.serve()
	r.holdTwice(f)

	res := r.passOn(r.exp)
	if !res.settled || res.commits != 1 {
		t.Fatalf("repair pass: settled %v, %d commits; want settled with the repair as its one commit", res.settled, res.commits)
	}
	r.wantOnce(first, f)
	if got := r.snapshotCount(); got != 1 {
		t.Errorf("%d snapshots after the repair, want 1: older ones hold the path twice", got)
	}
	// Nothing more to do on the next pass.
	if res := r.passOn(r.exp); res.commits != 0 || !res.settled {
		t.Fatalf("the pass after the repair: %d commits, settled %v; want none and settled", res.commits, res.settled)
	}
}

// The file of a path held twice leaves storage (a compaction's input,
// retention): the pass repairs and then removes it. It used to fail at every
// pass with "cannot delete files that do not belong to the table".
func TestDoubleRegistration1795_APathHeldTwiceCanLeaveTheTable(t *testing.T) {
	r := newAppendRig(t)
	r.exp.retain = 10
	first := r.settled()
	f := r.newFile()
	r.report(f)
	r.serve()
	r.holdTwice(f)
	if err := os.Remove(filepath.Join(r.root, appendDir, f)); err != nil {
		t.Fatal(err)
	}
	res := r.passOn(r.exp)
	if !res.settled || res.commits != 2 {
		t.Fatalf("pass: settled %v, %d commits; want settled, the repair and the removal", res.settled, res.commits)
	}
	r.wantOnce(first)
}

// A repair and new files in one pass: both commit, and the table holds every
// file once.
func TestDoubleRegistration1795_ARepairAndNewFilesInOnePass(t *testing.T) {
	r := newAppendRig(t)
	r.exp.retain = 10
	first := r.settled()
	f := r.newFile()
	r.report(f)
	r.serve()
	r.holdTwice(f)
	g := r.newFile()
	res := r.passOn(r.exp)
	if !res.settled || res.commits != 2 {
		t.Fatalf("pass: settled %v, %d commits; want settled, the repair and the new file", res.settled, res.commits)
	}
	r.wantOnce(first, f, g)
}

// An append goes on only from where this process left the table; any other
// version (taken over from the warehouse, a recovered row, a restart that left
// no record) falls back to a pass.
func TestDoubleRegistration1795_AnAppendOntoATableThisNodeDidNotLeaveIsAPass(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	key := "mydb\x00cpu"
	r.exp.chunkMu.Lock()
	r.exp.leftAt[key] = "file:///somewhere/else/00099-x.metadata.json"
	r.exp.chunkMu.Unlock()
	f := r.newFile()
	r.report(f)
	listed := r.listings()
	r.serve()
	if got := r.listings(); got != listed+1 {
		t.Fatalf("listings %d → %d: the append went on from a version this node did not leave the table at", listed, got)
	}
	r.wantOnce(first, f)
	// The pass that followed settled: the next append goes on as usual.
	g := r.newFile()
	r.report(g)
	listed = r.listings()
	r.serve()
	if got := r.listings(); got != listed {
		t.Errorf("the append after a settled pass listed the measurement (%d → %d)", listed, got)
	}
	r.wantOnce(first, f, g)
}

// appendFiles itself refuses: errTableMoved, nothing committed.
func TestDoubleRegistration1795_AppendFilesRefusesATableThatMoved(t *testing.T) {
	r := newAppendRig(t)
	first := r.settled()
	r.exp.chunkMu.Lock()
	r.exp.leftAt["mydb\x00cpu"] = "file:///somewhere/else/00099-x.metadata.json"
	r.exp.chunkMu.Unlock()
	f := r.newFile()
	ref, err := refOfKey(r, appendDir+f)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sc, err := r.sched.unionFileSchemas(ctx, []FileRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.exp.appendFiles(ctx, "mydb", "cpu", []FileRef{ref}, func(context.Context, []FileRef) (ArcSchema, error) { return sc, nil }, nil)
	if !errors.Is(err, errTableMoved) {
		t.Fatalf("appendFiles onto a table this node did not leave: %v, want errTableMoved", err)
	}
	r.wantOnce(first)
	// With no record at all (a process that has not settled a pass of the table).
	r.exp.chunkMu.Lock()
	delete(r.exp.leftAt, "mydb\x00cpu")
	r.exp.chunkMu.Unlock()
	if _, err = r.exp.appendFiles(ctx, "mydb", "cpu", []FileRef{ref}, func(context.Context, []FileRef) (ArcSchema, error) { return sc, nil }, nil); !errors.Is(err, errTableMoved) {
		t.Fatalf("appendFiles with no record of the table: %v, want errTableMoved", err)
	}
	r.wantOnce(first)
}

// refOfKey resolves one storage key of the rig to its FileRef, as the append path does.
func refOfKey(r *appendRig, key string) (FileRef, error) {
	files, _, err := r.src.FilesAndLocal(context.Background(), Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		return FileRef{}, err
	}
	for _, f := range files {
		if f.Key == key {
			return f, nil
		}
	}
	return FileRef{}, errors.New("not listed: " + key)
}
