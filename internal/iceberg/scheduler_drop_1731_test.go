package iceberg

// #1731: a database deleted on a node-local-storage cluster has its tables
// dropped on every node, including one whose gate is closed, unless the
// cluster has files under the database again; a recreated database is then
// reconciled from scratch.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type gateSwitch struct{ open bool }

func (g *gateSwitch) CanRun() bool { return g.open }

func newDropRig1731(t *testing.T) (*colocRig, *Scheduler, *gateSwitch) {
	t.Helper()
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/a.parquet"), 1_700_000_000_000_000, 5)
	r := newColocRig(t, root, "n.db", layoutWarehouse)
	gate := &gateSwitch{open: true}
	s := NewScheduler(SchedulerConfig{Exporter: r.exp, Source: NewStorageWalkSource(r.backend, "arc", zerolog.Nop()), Gate: gate, Logger: zerolog.Nop()})
	s.runPass(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") == "" {
		t.Fatal("not exported")
	}
	return r, s, gate
}

func TestRequestedDropRunsOnANodeThatDoesNotExport(t *testing.T) {
	r, s, gate := newDropRig1731(t)
	gate.open = false // another node exports now
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		t.Fatal(err)
	}
	s.RequestDropDatabase("mydb")
	s.runPass(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") != "" {
		t.Fatal("the catalog row survived the drop")
	}
	if _, err := os.Stat(filepath.Join(r.root, "arc_mydb.db")); !os.IsNotExist(err) {
		t.Fatalf("the warehouse namespace survived: %v", err)
	}
	for k := range s.state {
		if k == "mydb\x00cpu" {
			t.Fatal("the dropped measurement's fingerprint was kept")
		}
	}
}

func TestRequestedDropSkippedWhileTheClusterHasFiles(t *testing.T) {
	r, s, _ := newDropRig1731(t)
	s.SetDropGuard(func(_ context.Context, db string) (bool, error) { return db == "mydb", nil })
	s.RequestDropDatabase("mydb")
	s.serviceDrops(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") == "" {
		t.Fatal("dropped although the guard says the database has files")
	}
}

// After a drop, a database recreated under the same name is exported again
// even with no file newer than the fingerprint (the fingerprint is forgotten).
func TestADroppedDatabaseIsExportedAgainWhenRecreated(t *testing.T) {
	r, s, _ := newDropRig1731(t)
	s.RequestDropDatabase("mydb")
	s.serviceDrops(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") != "" {
		t.Fatal("not dropped")
	}
	// The same file set as before the drop (as if recreated identically).
	s.runPass(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") == "" {
		t.Fatal("not exported again after the drop")
	}
}

func TestDroppedThisPassCoversSpokeDatabases(t *testing.T) {
	s := &Scheduler{passDrops: map[string]struct{}{"spoke1": {}, "plain": {}}}
	for db, want := range map[string]bool{"plain": true, "spoke1/db": true, "spoke1": true, "other": false, "spoke10/db": false} {
		if got := s.droppedThisPass(db); got != want {
			t.Fatalf("droppedThisPass(%q) = %v", db, got)
		}
	}
	if (&Scheduler{}).droppedThisPass("x") {
		t.Fatal("empty pass dropped x")
	}
}

func TestRequestDropDatabaseRefusesNamespacedNames(t *testing.T) {
	s := NewScheduler(SchedulerConfig{Logger: zerolog.Nop()})
	for _, bad := range []string{"", "a/b", `a\b`} {
		s.RequestDropDatabase(bad)
	}
	if len(s.drops) != 0 {
		t.Fatalf("drops = %v", s.drops)
	}
}

// The backstop drops the tables of a database that has no files in the
// cluster and none on this node (a missed notice), and keeps the others.
func TestBackstopDropsTablesOfDatabasesWithNoFiles(t *testing.T) {
	r, s, gate := newDropRig1731(t)
	gate.open = false
	writeArcStyleParquet(t, filepath.Join(r.root, "keep/cpu/2023/11/14/22/k.parquet"), 1_700_000_000_000_000, 5)
	gate.open = true
	s.runPass(context.Background())
	if r.catalogLoc(t, "arc_keep", "cpu") == "" {
		t.Fatal("keep not exported")
	}
	gate.open = false
	// mydb is gone from the cluster and from this node; keep is gone from the
	// cluster's manifest but this node still holds its data (not yet
	// unlinked): only mydb is dropped.
	if err := os.RemoveAll(filepath.Join(r.root, "mydb")); err != nil {
		t.Fatal(err)
	}
	s.SetDropGuard(func(context.Context, string) (bool, error) { return false, nil })
	s.runPass(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") != "" {
		t.Fatal("the backstop left a table of a database with no files")
	}
	if r.catalogLoc(t, "arc_keep", "cpu") == "" {
		t.Fatal("the backstop dropped a table whose data is still on this node")
	}
	// Not again before dropBackstopEvery.
	if !time.Now().Before(s.backstopDue) {
		t.Fatal("backstop not rate-limited")
	}
}

func TestDropRetriedWhenTheGuardCannotTell(t *testing.T) {
	r, s, _ := newDropRig1731(t)
	s.SetDropGuard(func(context.Context, string) (bool, error) { return false, errors.New("raft barrier failed") })
	s.backstopDue = time.Now().Add(time.Hour) // the backstop is not under test
	s.RequestDropDatabase("mydb")
	s.serviceDrops(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") == "" {
		t.Fatal("dropped although the guard could not tell")
	}
	if rr, ok := s.dropRetry["mydb"]; !ok || rr.attempts != 1 {
		t.Fatalf("retry state = %+v", s.dropRetry["mydb"])
	}
	if _, queued := s.drops["mydb"]; !queued {
		t.Fatal("the drop was not kept for retry")
	}
}

// requestOnList asks for a drop while the pass lists measurements, after the
// pass-start drops ran: the drop happens between measurements, and the pass
// must not reconcile the dropped database again.
type requestOnList struct {
	FileSetSourceWithSchema
	s  *Scheduler
	db string
}

func (r *requestOnList) Measurements(ctx context.Context) ([]Measurement, error) {
	ms, err := r.FileSetSourceWithSchema.Measurements(ctx)
	r.s.RequestDropDatabase(r.db)
	return ms, err
}

func TestDropMidPassSkipsTheDatabase(t *testing.T) {
	r, s, _ := newDropRig1731(t)
	s.source = &requestOnList{FileSetSourceWithSchema: s.source, s: s, db: "mydb"}
	s.backstopDue = time.Now().Add(time.Hour)
	// mydb's files are still on disk: without the skip, the pass would
	// reconcile it right after the drop and recreate the table.
	s.runPass(context.Background())
	if r.catalogLoc(t, "arc_mydb", "cpu") != "" {
		t.Fatal("a database dropped mid-pass was reconciled again in the same pass")
	}
}
