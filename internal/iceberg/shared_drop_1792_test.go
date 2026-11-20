package iceberg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
