package iceberg

// #1731: a database deleted on a node-local-storage cluster has its tables
// dropped on every node, including one whose gate is closed, unless the
// cluster has files under the database again; a recreated database is then
// reconciled from scratch.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
	s.SetDropGuard(func(db string) bool { return db == "mydb" })
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
