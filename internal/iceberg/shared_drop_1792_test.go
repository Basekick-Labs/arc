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
	return sharedDropRigLayout(t, layoutWarehouse)
}

func sharedDropRigLayout(t *testing.T, layout string) (r *warehouseRig, a, b *Exporter) {
	t.Helper()
	r = newWarehouseRig(t)
	a, b = r.node(true), r.node(true)
	for _, n := range []*Exporter{a, b} {
		if err := n.SetLayout(layout); err != nil {
			t.Fatal(err)
		}
		n.SetForgetDroppedRows(true)
	}
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

// deleteData removes the database's data files, as the DELETE does before
// the Iceberg drop: not the colocated metadata beside them, which is the
// drop's.
func (r *warehouseRig) deleteData() error {
	return filepath.WalkDir(filepath.Join(r.root, "mydb"), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		return os.Remove(p)
	})
}

// deleteDatabase is the DELETE as node a runs it: the data files, then the
// Iceberg drop.
func (r *warehouseRig) deleteDatabase(a *Exporter) {
	r.t.Helper()
	if err := r.deleteData(); err != nil {
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
	// The pass saw at its end that the database had been dropped under it,
	// and removed what it had written: nothing waits for a later pass.
	if !errors.Is(passErr, errDroppedUnderPass) {
		t.Fatalf("b's pass inside a's drop ended with %v, want errDroppedUnderPass", passErr)
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) != 0 {
		t.Fatalf("%d objects of the dropped table are left once the drop and the pass have ended", len(left))
	}
	r.goneFromStorage("arc_mydb.db", "mydb/cpu/metadata")
	if _, rowExists, err := b.catalogRow(ctx, "arc_mydb", "cpu"); err != nil || rowExists {
		t.Fatalf("the exporting node kept its row for the dropped table (exists=%v, err=%v)", rowExists, err)
	}
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
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil {
		t.Fatalf("the next pass's look at the drop record: %v", err)
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) != 0 {
		t.Fatalf("%d objects of the dropped table are left after the next pass", len(left))
	}
	if _, rowExists, err := b.catalogRow(ctx, "arc_mydb", "cpu"); err != nil || rowExists {
		t.Fatalf("the exporting node still has a row for the dropped table (exists=%v, err=%v)", rowExists, err)
	}
	// No drop follows this removal to sweep the directories it emptied.
	r.goneFromStorage("arc_mydb.db")
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
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", nil); !errors.Is(err, errDatabaseBeingDropped) {
		t.Fatalf("a pass while a drop's record says running: %v", err)
	}
	if n := r.live(b); n != len(r.files()) {
		t.Fatalf("the table was touched while the drop's record said running (%d files)", n)
	}
	old := dropRecordStale
	dropRecordStale = 0
	t.Cleanup(func() { dropRecordStale = old })
	// No longer believed to be running: the table predates it and goes.
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil {
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
	if err := solo.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil || solo.dropID != "" {
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

// The look at the end of a pass: a node that is no longer the exporter
// removes nothing (the node that is decides); the exporter removes the table,
// forgets what it knew of the database and tells its scheduler.
func TestSharedDrop1792_TheEndOfAPassWhoseDatabaseWasDropped(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	// b's pass started with no record; a drop has been recorded since.
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil || b.dropID != "" {
		t.Fatalf("settle with no record: err=%v dropID=%q", err, b.dropID)
	}
	rec, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	rec.Done = true
	if err := a.writeDropRecord(ctx, "mydb", rec); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadDir(r.metaDir())
	if b.droppedUnderPass(ctx, "mydb", "cpu", func() bool { return false }) {
		t.Fatal("a node that lost the writer role acted on another node's drop")
	}
	if after, _ := os.ReadDir(r.metaDir()); len(after) != len(before) || len(before) == 0 {
		t.Fatalf("a node that lost the writer role removed the table (%d objects, was %d)", len(after), len(before))
	}
	var told []string
	b.SetOnDatabaseDropped(func(db string) { told = append(told, db) })
	stamp, other := b.tableDropStamp("mydb", "cpu"), b.tableDropStamp("mydb", "mem")
	b.setSmallManifests("mydb\x00cpu", 2)
	b.setSmallManifests("mydb\x00mem", 3)
	// With the pass's own context already ended: a pass that ran out of
	// time after a commit still has to look.
	ended, cancel := context.WithCancel(ctx)
	cancel()
	if !b.droppedUnderPass(ended, "mydb", "cpu", func() bool { return true }) {
		t.Fatal("the exporter did not act on a drop recorded during its pass")
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) != 0 {
		t.Fatalf("%d objects of the dropped table are left", len(left))
	}
	if b.tableDropStamp("mydb", "cpu") == stamp {
		t.Error("the removal did not move the table's stamp: a pass would trust what was cached for the removed table")
	}
	if b.tableDropStamp("mydb", "mem") != other {
		t.Error("the removal of one table moved another table's stamp: its cache would be thrown away")
	}
	if _, ok := b.smallManifests("mydb\x00cpu"); ok {
		t.Error("the exporter kept what it knew of the removed table")
	}
	if n, ok := b.smallManifests("mydb\x00mem"); !ok || n != 3 {
		t.Error("the exporter forgot what it knew of another table of the database")
	}
	if len(told) != 0 {
		t.Errorf("the scheduler was told the whole database %v was dropped, for one table removed", told)
	}
	// The same record again is not another drop.
	b.dropID = rec.ID
	if b.droppedUnderPass(ctx, "mydb", "cpu", func() bool { return true }) {
		t.Error("a pass that started under the record took it for a drop during the pass")
	}
}

// overlap runs export on node b up to its commit, then the DELETE on node a,
// whose drop lists and writes its markers before the export commits and
// publishes, and deletes after: the order that left a broken table. It
// returns the export's error.
func (r *warehouseRig) overlap(a *Exporter, export func(canCommit func() bool) error) error {
	r.t.Helper()
	readyToCommit, listed, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	var exportErr error
	go func() {
		defer close(done)
		exportErr = export(func() bool {
			once.Do(func() { close(readyToCommit); <-listed })
			return true
		})
	}()
	select {
	case <-readyToCommit:
	case <-done:
		r.t.Fatalf("the export ended before its commit: %v", exportErr)
	}
	a.dropAfterListing = func() { close(listed); <-done }
	r.deleteDatabase(a)
	a.dropAfterListing = nil
	return exportErr
}

// goneFromStorage fails when anything of the dropped database is left: on a
// shared filesystem an empty directory keeps a database or a namespace
// listed.
func (r *warehouseRig) goneFromStorage(dirs ...string) {
	r.t.Helper()
	for _, d := range dirs {
		if _, err := os.Stat(filepath.Join(r.root, d)); !os.IsNotExist(err) {
			r.t.Errorf("%s is still there once the drop and the overlapped export have ended (stat: %v)", d, err)
		}
	}
}

// The overlapped pass in the colocated layout, where the table's metadata
// sits in the measurement's own directory: an empty one left behind would
// keep the dropped database listed.
func TestSharedDrop1792_APassOverlapsTheDropColocated(t *testing.T) {
	r, a, b := sharedDropRigLayout(t, layoutColocated)
	ctx := context.Background()
	r.addFile()
	current, sc := r.files(), r.schemaNow()
	err := r.overlap(a, func(canCommit func() bool) error {
		_, err := b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current, canCommit: canCommit})
		return err
	})
	if !errors.Is(err, errDroppedUnderPass) {
		t.Fatalf("the pass inside another node's drop ended with %v, want errDroppedUnderPass", err)
	}
	r.goneFromStorage("arc_mydb.db", "mydb/cpu/metadata")
}

// A drop stops deleting well before the other nodes stop believing its
// record: past that, they export the database again and the drop would
// delete what they write. The DELETE reports it and is repeated.
func TestSharedDrop1792_ADropStopsBeforeItsRecordIsStale(t *testing.T) {
	r, a, _ := sharedDropRig(t)
	ctx := context.Background()
	old := dropRecordStale
	dropRecordStale = 300 * time.Millisecond
	t.Cleanup(func() { dropRecordStale = old })
	if err := r.deleteData(); err != nil {
		t.Fatal(err)
	}
	a.dropAfterListing = func() { time.Sleep(350 * time.Millisecond) } // a drop that takes too long
	err := a.DropDatabase(ctx, "mydb")
	a.dropAfterListing = nil
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a drop that outlasted its record: %v, want its deadline", err)
	}
	if rec, exists := r.dropRecord(a); !exists || rec.Done {
		t.Fatalf("the record of a drop that did not finish: exists=%v done=%v", exists, rec.Done)
	}
	if left, _ := os.ReadDir(r.metaDir()); len(left) == 0 {
		t.Fatal("the drop went on deleting past its deadline")
	}
	dropRecordStale = old
	if err := a.DropDatabase(ctx, "mydb"); err != nil { // the DELETE, repeated
		t.Fatal(err)
	}
	if rec, exists := r.dropRecord(a); !exists || !rec.Done {
		t.Fatalf("after the repeated DELETE the record is exists=%v done=%v", exists, rec.Done)
	}
	r.goneFromStorage("arc_mydb.db")
}

// The directories a removed table leaves empty go, up to the database's:
// the DELETE's own sweep of empty directories may already have run. One
// that holds something (data written since, another table) stays.
func TestSharedDrop1792_EmptiedDirectoriesAreRemoved(t *testing.T) {
	r, _, b := sharedDropRigLayout(t, layoutColocated)
	ctx := context.Background()
	mk := func(dirs ...string) {
		for _, d := range dirs {
			if err := os.MkdirAll(filepath.Join(r.root, d), 0o700); err != nil {
				t.Fatal(err)
			}
		}
	}
	loc := func(db, table string) string {
		return "file://" + filepath.Join(r.root, db, table, "metadata", "00001-x.metadata.json")
	}
	mk("gone/cpu/metadata")
	b.removeEmptiedTableDirs(ctx, "gone", loc("gone", "cpu"))
	r.goneFromStorage("gone")
	// Another measurement of the database is still there.
	mk("kept/cpu/metadata", "kept/mem/metadata")
	b.removeEmptiedTableDirs(ctx, "kept", loc("kept", "cpu"))
	r.goneFromStorage("kept/cpu")
	if _, err := os.Stat(filepath.Join(r.root, "kept/mem/metadata")); err != nil {
		t.Errorf("another table's directory was removed: %v", err)
	}
	// Data written since.
	mk("live/cpu/metadata", "live/cpu/2023")
	b.removeEmptiedTableDirs(ctx, "live", loc("live", "cpu"))
	r.goneFromStorage("live/cpu/metadata")
	if _, err := os.Stat(filepath.Join(r.root, "live/cpu/2023")); err != nil {
		t.Errorf("a directory with data written since was removed: %v", err)
	}
	// A spoke's database: up to the spoke.
	mk("spoke/db/cpu/metadata")
	b.removeEmptiedTableDirs(ctx, "spoke/db", loc("spoke/db", "cpu"))
	r.goneFromStorage("spoke")
	// Never above the database: the storage root's other directories stay.
	if _, err := os.Stat(r.root); err != nil {
		t.Fatal(err)
	}
}

// A pass that commits and then fails still looks at its end: what its first
// commit wrote into the dropped table goes, though the pass returns an error
// of its own.
func TestSharedDrop1792_APassThatCommitsAndThenFailsStillLooks(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	sc := r.schemaNow()
	r.addFile()
	current := r.addFile()
	unread := current[len(current)-2:]
	b.chunkMu.Lock()
	if b.chunkSize == nil {
		b.chunkSize = make(map[string]int)
	}
	b.chunkSize["mydb\x00cpu"] = 1 // one new file per commit
	b.chunkMu.Unlock()
	boom := errors.New("the second commit's files could not be read")
	reads := 0
	err := r.overlap(a, func(canCommit func() bool) error {
		_, err := b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current, unread: unread,
			readSchemas: func(context.Context, []FileRef) (ArcSchema, error) {
				if reads++; reads > 1 {
					return ArcSchema{}, boom
				}
				return sc, nil
			}, canCommit: canCommit})
		return err
	})
	if !errors.Is(err, boom) {
		t.Fatalf("the pass ended with %v, want its own error", err)
	}
	if reads < 2 {
		t.Fatalf("the pass read schemas %d times: it did not commit once and then fail", reads)
	}
	r.goneFromStorage("arc_mydb.db")
	if _, rowExists, err := b.catalogRow(ctx, "arc_mydb", "cpu"); err != nil || rowExists {
		t.Fatalf("the exporting node kept its row for the dropped table (exists=%v, err=%v)", rowExists, err)
	}
}

// A table's first export overlapping the DELETE: the drop finds nothing, and
// the pass creates the table after it. The drop's record is kept for this:
// the pass sees at its end that the database was dropped, and removes the
// table it created for a database that is gone.
func TestSharedDrop1792_AFirstExportOverlapsTheDrop(t *testing.T) {
	for _, layout := range []string{layoutWarehouse, layoutColocated} {
		t.Run(layout, func(t *testing.T) {
			r := newWarehouseRig(t)
			a, b := r.node(true), r.node(true)
			for _, n := range []*Exporter{a, b} {
				if err := n.SetLayout(layout); err != nil {
					t.Fatal(err)
				}
				n.SetForgetDroppedRows(true)
			}
			ctx := context.Background()
			current := r.addFile()
			sc := r.schemaNow()
			_, err := b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", current: current, unread: current,
				readSchemas: func(context.Context, []FileRef) (ArcSchema, error) {
					r.deleteDatabase(a) // the whole DELETE, between the pass's footer read and its create
					return sc, nil
				}})
			t.Logf("the first export: %v", err)
			if rec, exists := r.dropRecord(a); !exists || !rec.Done {
				t.Fatalf("a drop that found nothing left record exists=%v done=%v, want a finished record", exists, rec.Done)
			}
			r.goneFromStorage("arc_mydb.db", "mydb/cpu/metadata")
			if _, rowExists, err := b.catalogRow(ctx, "arc_mydb", "cpu"); err != nil || rowExists {
				t.Fatalf("the exporting node has a table for a database that was deleted (row exists=%v, err=%v)", rowExists, err)
			}
		})
	}
}

// Marking a record done is for the drop that wrote it: with two writers
// around a failover, the earlier drop's end must not call the later one's
// finished.
func TestSharedDrop1792_ADropFinishesOnlyItsOwnRecord(t *testing.T) {
	r, a, _ := sharedDropRig(t)
	ctx := context.Background()
	first, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	a.finishDropRecord(ctx, "mydb", first)
	if rec, _ := r.dropRecord(a); rec.ID != second.ID || rec.Done {
		t.Fatalf("after the earlier drop ended the record is id=%q done=%v, want the later drop's, still running", rec.ID, rec.Done)
	}
	a.finishDropRecord(ctx, "mydb", second)
	if rec, _ := r.dropRecord(a); rec.ID != second.ID || !rec.Done {
		t.Fatalf("after the later drop ended the record is id=%q done=%v", rec.ID, rec.Done)
	}
}

// The drop stops deleting with at least half of the record's life left: that
// half is what the nodes' clocks may differ by.
func TestSharedDrop1792_TheDropsDeadlineLeavesHalfTheRecordsLife(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	if left := started.Add(dropRecordStale).Sub(dropDeadline(started)); left < dropRecordStale/2 {
		t.Fatalf("the drop's deadline leaves %v of the record's %v", left, dropRecordStale)
	}
	if !dropDeadline(started).After(started.Add(time.Minute)) {
		t.Fatal("the drop has no time to run")
	}
}

// A pass that finds a table's files unchanged does not read the drop record
// (one request per table per tick otherwise), and a pass that does read it
// loads the table to compare only once per table and drop.
func TestSharedDrop1792_AnUnchangedTableCostsNothing(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	r.deleteDatabase(a)
	r.addFile()
	sched := NewScheduler(SchedulerConfig{Exporter: b, Source: NewStorageWalkSource(r.backend, "arc", zerolog.Nop()),
		Interval: time.Hour, Logger: zerolog.Nop()})
	m, key := Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"
	for i := 0; i < 3; i++ {
		_, _ = sched.reconcileOneRecovered(ctx, m, key)
	}
	if _, settled := sched.state[key]; !settled {
		t.Fatal("the recreated table did not settle")
	}
	rec, _ := r.dropRecord(b)
	b.dropSeenMu.Lock()
	seen := b.dropSeen[key]
	b.dropSeenMu.Unlock()
	if seen != rec.ID {
		t.Fatalf("the table's drop id was not remembered (%q, record %q): every pass would load the table to compare", seen, rec.ID)
	}
	// The record is now unreadable: a pass that read it would give way.
	if err := r.backend.Write(ctx, dropRecordKey("mydb"), []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	if out, err := sched.reconcileOneRecovered(ctx, m, key); err != nil || out != outcomeUnchanged {
		t.Fatalf("a pass over unchanged files: outcome=%v err=%v, want unchanged and no look at the record", out, err)
	}
	r.addFile() // changed: now it looks
	if _, err := sched.reconcileOneRecovered(ctx, m, key); !errors.Is(err, errDatabaseBeingDropped) {
		t.Fatalf("a pass over changed files with an unreadable record: %v, want it to give way", err)
	}
}

// Removing a table that predates a drop: not on a node that is not the
// exporter; and the table's drop marker is written back first, so that a
// row which then cannot be forgotten (or another node's) is not a dead row
// for good.
func TestSharedDrop1792_RemovalIsTheExportersAndWritesTheMarkerBack(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	rec, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	a.finishDropRecord(ctx, "mydb", rec)
	before, _ := os.ReadDir(r.metaDir())
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", func() bool { return false }); err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadDir(r.metaDir()); len(after) != len(before) || len(before) == 0 {
		t.Fatalf("a node that is not the exporter removed the table (%d objects, was %d)", len(after), len(before))
	}
	marker := filepath.Join(r.root, filepath.FromSlash(droppedMarkerKey("arc_mydb", "cpu")))
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("the test's premise: no marker before the removal (stat: %v)", err)
	}
	if err := b.settleDroppedTable(ctx, "mydb", "cpu", func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	r.goneFromStorage("arc_mydb.db")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the removed table's drop marker was not written: %v", err)
	}
}

// A node with no row of its own for the table (it never exported it, or
// forgot it) judges the table the warehouse has, as its pass would load it:
// what a former exporter left of a dropped table goes all the same.
func TestSharedDrop1792_ANodeWithoutARowRemovesWhatAnotherLeft(t *testing.T) {
	r, a, _ := sharedDropRig(t) // b exported the table
	ctx := context.Background()
	c := r.node(true)
	c.SetForgetDroppedRows(true)
	rec, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	a.finishDropRecord(ctx, "mydb", rec)
	if err := c.settleDroppedTable(ctx, "mydb", "cpu", nil); err != nil {
		t.Fatal(err)
	}
	r.goneFromStorage("arc_mydb.db")
}

// The scheduler's pass over a measurement whose files are all gone (the
// directory survives the DELETE in some layouts) settles too, before it
// looks at the table: it removes a table from before the drop instead of
// committing it empty.
func TestSharedDrop1792_ThePassOverAnEmptiedMeasurementSettles(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	rec, _, err := a.beginDropRecord(ctx, "mydb")
	if err != nil {
		t.Fatal(err)
	}
	a.finishDropRecord(ctx, "mydb", rec)
	if err := r.deleteData(); err != nil { // the data, not its directories
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: b, Source: NewStorageWalkSource(r.backend, "arc", zerolog.Nop()),
		Interval: time.Hour, Logger: zerolog.Nop()})
	if _, err := sched.reconcileOneRecovered(ctx, Measurement{Database: "mydb", Measurement: "cpu"}, "mydb\x00cpu"); err != nil {
		t.Logf("the pass: %v", err)
	}
	r.goneFromStorage("arc_mydb.db")
	if _, rowExists, err := b.catalogRow(ctx, "arc_mydb", "cpu"); err != nil || rowExists {
		t.Fatalf("the exporting node kept the table of a dropped database (row exists=%v, err=%v)", rowExists, err)
	}
}

// After a real overlapped drop (per-table markers written, the exporter's own
// checks off as before the fix), ANOTHER node that never exported the table
// takes over the export: what does it make of the residue, and of the
// database written again?
func TestSharedDrop1792_AnotherNodeTakesOverAfterAnOverlappedDrop(t *testing.T) {
	r, a, b := sharedDropRig(t)
	ctx := context.Background()
	c := r.node(true)
	c.SetForgetDroppedRows(true)
	r.addFile()
	current, sc := r.files(), r.schemaNow()
	b.SetForgetDroppedRows(false)
	_ = r.overlap(a, func(canCommit func() bool) error {
		_, err := b.reconcile(ctx, reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: current, canCommit: canCommit})
		return err
	})
	if left, _ := os.ReadDir(r.metaDir()); len(left) == 0 {
		t.Fatal("the overlapped pass left nothing behind: the test's premise")
	}
	// The database is written again, and c exports it.
	r.addFile()
	r.addFile()
	var last error
	for i := 0; i < 3; i++ {
		last = r.passOf(c)
		t.Logf("c's pass %d: %v", i, last)
	}
	if last != nil {
		t.Fatalf("the node that took over still fails: %v", last)
	}
	if n := r.live(c); n != len(r.files()) {
		t.Fatalf("c's table holds %d files, want %d", n, len(r.files()))
	}
	rec, _ := r.dropRecord(c)
	tbl, err := c.catalog.LoadTable(ctx, c.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if got := dropIDOf(tbl); got != rec.ID {
		t.Fatalf("c exports a table with drop id %q under record %q: it took the dropped table's residue for the table", got, rec.ID)
	}
}
