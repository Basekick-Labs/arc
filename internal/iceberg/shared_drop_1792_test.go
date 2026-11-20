package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1792: on a shared bucket the node that handles a database DELETE (a) is
// not the node that exports (b). a's drop removes the warehouse objects it
// listed; b's reconcile pass, which loaded the table before that, commits and
// publishes after it. One node's database lock does not hold the other's
// pass.

// sharedDropRig is two nodes on one bucket, b having exported the table.
func sharedDropRig(t *testing.T) (r *warehouseRig, a, b *Exporter) {
	t.Helper()
	r = newWarehouseRig(t)
	a, b = r.node(true), r.node(true)
	a.SetForgetDroppedRows(true)
	b.SetForgetDroppedRows(true)
	for i := 0; i < 3; i++ {
		r.addFile()
		if err := r.reconcile(b); err != nil {
			t.Fatal(err)
		}
	}
	return r, a, b
}

// schemaNow is the schema of the measurement's files as they are.
func (r *warehouseRig) schemaNow() ArcSchema {
	r.t.Helper()
	for i := r.n; i >= 1; i-- {
		p := filepath.Join(r.root, fmt.Sprintf("mydb/cpu/2023/11/14/22/f%03d.parquet", i))
		if _, err := os.Stat(p); err == nil {
			sc, err := SchemaFromParquet(p)
			if err != nil {
				r.t.Fatal(err)
			}
			return sc
		}
	}
	return ArcSchema{}
}

// deleteDatabase is the DELETE as node a runs it: the data files, then the
// Iceberg drop.
func (r *warehouseRig) deleteDatabase(a *Exporter) {
	r.t.Helper()
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		r.t.Fatal(err)
	}
	if err := a.DropDatabase(context.Background(), "mydb"); err != nil {
		r.t.Fatalf("DropDatabase: %v", err)
	}
}

// The DELETE lands on a while b's pass is between loading the table and
// committing. The database is then written again, and b exports it: the
// table b ends with must load and hold exactly the new files.
func TestSharedDrop1792_ADeleteOnAnotherNodeDuringAPass(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	r.addFile() // what b's pass is about to register
	deleted := false
	_, passErr := b.reconcile(ctx, reconcileRequest{
		database: "mydb", measurement: "cpu", schema: r.schemaNow(), current: r.files(),
		canCommit: func() bool {
			if !deleted {
				deleted = true
				r.deleteDatabase(a) // right before b commits
			}
			return true
		},
	})
	if !deleted {
		t.Fatal("the pass never reached a commit: the test's premise")
	}
	t.Logf("b's pass that the DELETE overlapped: err=%v", passErr)
	// The database is written again.
	r.addFile()
	r.addFile()
	var err error
	for i := 0; i < 3; i++ {
		_, err = b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: r.schemaNow(), current: r.files()})
	}
	if err != nil {
		t.Fatalf("b's passes over the recreated database still fail: %v", err)
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("b's table holds %d files, want the %d written since the DELETE", n, len(r.files()))
	}
}

// The damaging order. b's pass has read its new file and is about to commit.
// a deletes the data files, lists the table's objects and writes its markers.
// b commits and publishes (everything it reads is still there). a deletes
// what it listed. What b wrote is not in a's listing and stays: metadata
// naming manifests that are gone, and b's catalog row pointing at it. The
// database is then written again and b exports it.
func TestSharedDrop1792_APassCommitsBetweenTheDropsListingAndItsDeletes(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	r.addFile() // what b's pass registers
	current, sc := r.files(), r.schemaNow()

	readyToCommit, listed, passDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var passErr error
	var once sync.Once
	go func() {
		defer close(passDone)
		_, passErr = b.reconcile(ctx, reconcileRequest{
			database: "mydb", measurement: "cpu", schema: sc, current: current,
			canCommit: func() bool {
				once.Do(func() {
					close(readyToCommit) // the new file is read; the commit is next
					<-listed             // a has listed and written its markers
				})
				return true
			},
		})
	}()
	select {
	case <-readyToCommit:
	case <-time.After(30 * time.Second):
		t.Fatal("b's pass never reached its commit")
	}
	a.dropAfterListing = func() {
		close(listed)
		<-passDone // b commits and publishes before a deletes anything
	}
	// The DELETE on a: the data files, then the drop.
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		t.Fatal(err)
	}
	if err := a.DropDatabase(ctx, "mydb"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	a.dropAfterListing = nil
	t.Logf("b's pass inside a's drop: err=%v", passErr)
	left, _ := os.ReadDir(r.metaDir())
	t.Logf("objects left in the table's metadata directory after the drop: %d", len(left))
	// The database is written again, and b exports it.
	r.addFile()
	r.addFile()
	var err error
	for i := 0; i < 3; i++ {
		_, err = b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: r.schemaNow(), current: r.files()})
	}
	if err != nil {
		t.Fatalf("b's passes over the recreated database still fail: %v", err)
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("b's table holds %d files, want the %d written since the DELETE", n, len(r.files()))
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) == 0 {
		t.Fatal("no table after the recreate")
	}
}

// passOf runs one reconcile pass of the measurement on exp over the files as
// they are.
func (r *warehouseRig) passOf(exp *Exporter) error {
	r.t.Helper()
	_, err := exp.reconcile(context.Background(), reconcileRequest{database: "mydb", measurement: "cpu", schema: r.schemaNow(), current: r.files()})
	return err
}

func (r *warehouseRig) dropRecord(exp *Exporter) (dropRecord, bool) {
	r.t.Helper()
	rec, exists, err := exp.readDropRecord(context.Background(), "mydb")
	if err != nil {
		r.t.Fatal(err)
	}
	return rec, exists
}

// The backstop: a pass that could not look at its end (here: it did not look
// at all, as before the fix) leaves the broken table; the next pass that
// reads the database's drop record sees a table from before the drop and
// removes it, and the database written again is exported whole.
func TestSharedDrop1792_TheNextPassRemovesATableFromBeforeTheDrop(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	r.addFile()
	current, sc := r.files(), r.schemaNow()
	b.SetForgetDroppedRows(false) // this pass checks nothing
	readyToCommit, listed, passDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	go func() {
		defer close(passDone)
		_, _ = b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current,
			canCommit: func() bool {
				once.Do(func() { close(readyToCommit); <-listed })
				return true
			}})
	}()
	<-readyToCommit
	a.dropAfterListing = func() { close(listed); <-passDone }
	r.deleteDatabase(a)
	a.dropAfterListing = nil
	if left, _ := os.ReadDir(r.metaDir()); len(left) == 0 {
		t.Fatal("the pass left nothing behind: the test's premise")
	}
	b.SetForgetDroppedRows(true)
	// The table is broken, and the database is still empty: the next pass
	// removes what is left.
	if err := b.settleDroppedTable(ctx, "mydb", "cpu"); err != nil {
		t.Fatalf("the next pass's look at the drop record: %v", err)
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) != 0 {
		t.Fatalf("%d objects of the dropped table are left after the next pass", len(left))
	}
	if _, rowExists, err := b.catalogRow(ctx, "arc_mydb", "cpu"); err != nil || rowExists {
		t.Fatalf("the exporting node still has a row for the dropped table (exists=%v, err=%v)", rowExists, err)
	}
	// Written again.
	r.addFile()
	r.addFile()
	for i := 0; i < 2; i++ {
		if err := r.passOf(b); err != nil {
			t.Fatalf("pass %d over the recreated database: %v", i, err)
		}
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("b's table holds %d files, want %d", n, len(r.files()))
	}
	// The recreated table is this drop's, and is left alone from here on.
	rec, _ := r.dropRecord(b)
	tbl, err := b.catalog.LoadTable(ctx, b.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if got := dropIDOf(tbl); got != rec.ID || got == "" {
		t.Fatalf("the recreated table carries drop id %q, the record has %q", got, rec.ID)
	}
	r.addFile()
	if err := r.passOf(b); err != nil {
		t.Fatal(err)
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("after one more pass the table holds %d files, want %d: a table created after the drop was removed", n, len(r.files()))
	}
}

// While a drop is running on another node, a pass gives way: it writes
// nothing and creates nothing the drop would then delete.
func TestSharedDrop1792_APassGivesWayWhileADropRunsOnAnotherNode(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	r.addFile()
	current, sc := r.files(), r.schemaNow()
	var during error
	var before, after int
	a.dropAfterListing = func() {
		entries, _ := os.ReadDir(r.metaDir())
		before = len(entries)
		_, during = b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current})
		entries, _ = os.ReadDir(r.metaDir())
		after = len(entries)
		if rec, exists := r.dropRecord(b); !exists || rec.Done {
			t.Errorf("during the drop the record is exists=%v done=%v, want a drop that is running", exists, rec.Done)
		}
	}
	r.deleteDatabase(a)
	if !errors.Is(during, errDatabaseBeingDropped) {
		t.Fatalf("a pass during another node's drop: %v, want errDatabaseBeingDropped", during)
	}
	if before != after {
		t.Fatalf("the pass wrote during the drop: %d objects before, %d after", before, after)
	}
	if rec, exists := r.dropRecord(b); !exists || !rec.Done {
		t.Fatalf("after the drop the record is exists=%v done=%v, want a finished drop", exists, rec.Done)
	}
}

// A drop that died (its record still says "running") stops being believed
// after a while; a record that cannot be read is never taken for evidence.
func TestSharedDrop1792_AStaleOrUnreadableRecord(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	// A drop that began and never finished.
	rec, recorded, err := a.beginDropRecord(ctx, "mydb")
	if err != nil || !recorded {
		t.Fatalf("beginDropRecord: %v %v", recorded, err)
	}
	if err := b.settleDroppedTable(ctx, "mydb", "cpu"); !errors.Is(err, errDatabaseBeingDropped) {
		t.Fatalf("a pass while a drop's record says running: %v", err)
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("the table was touched while the drop's record said running (%d files)", n)
	}
	old := dropRecordStale
	dropRecordStale = 0
	t.Cleanup(func() { dropRecordStale = old })
	// No longer believed to be running: the table predates it and goes.
	if err := b.settleDroppedTable(ctx, "mydb", "cpu"); err != nil {
		t.Fatalf("a pass once the record is stale: %v", err)
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) != 0 {
		t.Fatalf("%d objects of a table from before the drop are left", len(left))
	}
	dropRecordStale = old
	// Exported again, under that drop's id.
	if err := a.writeDropRecord(ctx, "mydb", dropRecord{ID: rec.ID, Started: rec.Started, Done: true}); err != nil {
		t.Fatal(err)
	}
	r.addFile()
	if err := r.passOf(b); err != nil {
		t.Fatal(err)
	}
	// The record is damaged: the pass gives way and removes nothing.
	if err := r.backend.Write(ctx, dropRecordKey("mydb"), []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadDir(r.metaDir())
	if err := r.passOf(b); !errors.Is(err, errDatabaseBeingDropped) {
		t.Fatalf("a pass with an unreadable record: %v, want it to give way", err)
	}
	after, _ := os.ReadDir(r.metaDir())
	if len(before) == 0 || len(before) != len(after) {
		t.Fatalf("a pass with an unreadable record changed the table: %d objects before, %d after", len(before), len(after))
	}
}

// Where there are no records (a standalone node, a node-local cluster), a
// drop writes none, a pass reads none, and a table is never taken for one of
// an earlier drop. Nor is one whose database has no record (a restore does
// not bring the record back), whatever id the table carries.
func TestSharedDrop1792_NoRecordNoRemoval(t *testing.T) {
	r := newWarehouseRig(t)
	ctx := context.Background()
	solo := r.node(true) // forgetDroppedRows is off
	r.addFile()
	if err := r.reconcile(solo); err != nil {
		t.Fatal(err)
	}
	if err := solo.DropDatabase(ctx, "mydb"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(r.root, filepath.FromSlash(dropRecordKey("mydb")))); !os.IsNotExist(err) {
		t.Fatalf("a drop wrote a record where records are off (stat: %v)", err)
	}
	if err := solo.settleDroppedTable(ctx, "mydb", "cpu"); err != nil || solo.dropID != "" {
		t.Fatalf("a pass where records are off: err=%v dropID=%q", err, solo.dropID)
	}
	// A shared bucket, a table created under a drop, and the record gone.
	r2, a, b := sharedDropRig(t)
	r2.deleteDatabase(a)
	r2.addFile()
	if err := r2.passOf(b); err != nil {
		t.Fatal(err)
	}
	r2.addFile()
	if err := r2.passOf(b); err != nil {
		t.Fatal(err)
	}
	tbl, err := b.catalog.LoadTable(ctx, b.tableIdent("mydb", "cpu"))
	if err != nil || dropIDOf(tbl) == "" {
		t.Fatalf("the recreated table has no drop id (err=%v)", err)
	}
	if err := os.Remove(filepath.Join(r2.root, filepath.FromSlash(dropRecordKey("mydb")))); err != nil {
		t.Fatal(err)
	}
	r2.addFile()
	if err := r2.passOf(b); err != nil {
		t.Fatal(err)
	}
	if n := r2.live(b); n != len(r2.files()) {
		t.Fatalf("with the record gone the table holds %d files, want %d: it was taken for a dropped one", n, len(r2.files()))
	}
}

// A record's key: one per database, a spoke's databases under the spoke, and
// never a table marker's directory.
func TestSharedDrop1792_RecordKey(t *testing.T) {
	if dropRecordKey("spoke/db") != dropRecordKey("spoke") || dropRecordKey("spoke/other") != dropRecordKey("spoke") {
		t.Error("a spoke's databases do not share the spoke's drop record")
	}
	if dropRecordKey("a") == dropRecordKey("b") {
		t.Error("two databases share a drop record")
	}
	// A namespace key that is literally "%drop" has its own marker directory.
	if strings.HasPrefix(droppedMarkerKey("%drop", "t"), droppedMarkerRoot+"/%drop/") {
		t.Errorf("a table marker can land in the records' directory: %s", droppedMarkerKey("%drop", "t"))
	}
	// Two drops have different ids, also within one second.
	r, a, _ := sharedDropRig(t)
	r1, _, err := a.beginDropRecord(context.Background(), "mydb")
	if err != nil {
		t.Fatal(err)
	}
	r2, _, err := a.beginDropRecord(context.Background(), "mydb")
	if err != nil {
		t.Fatal(err)
	}
	if r1.ID == r2.ID || r1.ID == "" {
		t.Errorf("two drops got ids %q and %q", r1.ID, r2.ID)
	}
	_ = r
}

// Through the scheduler: the exporting node's timed pass finds the broken
// table a drop on another node left, before it reads anything of it, removes
// it, and exports the database written again.
func TestSharedDrop1792_TheSchedulersPassRemovesTheBrokenTable(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	r.addFile()
	current, sc := r.files(), r.schemaNow()
	b.SetForgetDroppedRows(false) // the overlapped pass checks nothing, as before the fix
	readyToCommit, listed, passDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	go func() {
		defer close(passDone)
		_, _ = b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current,
			canCommit: func() bool {
				once.Do(func() { close(readyToCommit); <-listed })
				return true
			}})
	}()
	<-readyToCommit
	a.dropAfterListing = func() { close(listed); <-passDone }
	r.deleteDatabase(a)
	a.dropAfterListing = nil
	b.SetForgetDroppedRows(true)
	// The database is written again before the exporting node's next pass.
	r.addFile()
	r.addFile()
	sched := NewScheduler(SchedulerConfig{Exporter: b, Source: NewStorageWalkSource(r.backend, "arc", zerolog.Nop()),
		Interval: time.Hour, Logger: zerolog.Nop()})
	m, key := Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"
	for i := 0; i < 3; i++ {
		if _, err := sched.reconcileOneRecovered(ctx, m, key); err != nil {
			t.Logf("pass %d: %v", i, err)
		}
	}
	if _, err := sched.reconcileOneRecovered(ctx, m, key); err != nil {
		t.Fatalf("the scheduler's passes over the recreated database still fail: %v", err)
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("b's table holds %d files, want %d", n, len(r.files()))
	}
	if _, settled := sched.state[key]; !settled {
		t.Error("the recreated table did not settle")
	}
}
