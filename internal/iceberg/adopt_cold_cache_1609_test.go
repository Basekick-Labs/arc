package iceberg

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

// #1609: the scheduler's first loads after a restart (tableSeed, and tableExists
// on an emptied measurement) ran before EnsureTable's adoption. On a shared
// warehouse a node's catalog row can point at metadata another node has since
// pruned; that load failed every pass.

// staleRowWithPrunedMetadata leaves b with a catalog row whose metadata file
// is gone, while the warehouse publishes a newer version a committed.
func staleRowWithPrunedMetadata(t *testing.T, bAuthoritative bool) (*warehouseRig, *Exporter) {
	t.Helper()
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(bAuthoritative)
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	r.addFile()
	if err := r.reconcile(b); err != nil { // no row yet: b adopts a's table, then commits
		t.Fatal(err)
	}
	r.addFile()
	if err := r.reconcile(a); err != nil { // a adopts b's version and commits past it
		t.Fatal(err)
	}
	ident := b.tableIdent("mydb", "cpu")
	loc, exists, err := b.catalogRow(context.Background(), catalogNamespaceKey(ident[:len(ident)-1]), "cpu")
	if err != nil || !exists {
		t.Fatalf("b's catalog row: exists %v, err %v", exists, err)
	}
	// Pruned by a's retention of old metadata (or its delete-after-commit).
	if err := os.Remove(strings.TrimPrefix(loc, "file://")); err != nil {
		t.Fatalf("remove b's metadata %s: %v", loc, err)
	}
	return r, b
}

func TestTableSeed_AdoptsBeforeLoadingAPrunedRow(t *testing.T) {
	r, b := staleRowWithPrunedMetadata(t, true)
	_, files, ok, err := b.tableSeed(context.Background(), "mydb", "cpu")
	if err != nil || !ok {
		t.Fatalf("tableSeed: ok %v, err %v; want the published table adopted and loaded", ok, err)
	}
	if len(files) != len(r.files()) {
		t.Fatalf("seeded %d files, want %d (the published table)", len(files), len(r.files()))
	}
}

// Without an authoritative warehouse (no longer wired since #1613) a row whose metadata is
// gone is still recovered when the warehouse publishes the table: the load
// fails, and recoverDeadRow moves the row onto the published version (#1611).
func TestTableSeed_NonAuthoritativeRecoversAPrunedRowFromThePublishedVersion(t *testing.T) {
	r, b := staleRowWithPrunedMetadata(t, false)
	_, files, ok, err := b.tableSeed(context.Background(), "mydb", "cpu")
	if err != nil || !ok || len(files) != len(r.files()) {
		t.Fatalf("tableSeed: ok %v, %d files, err %v; want the published table", ok, len(files), err)
	}
}

func TestTableExists_AdoptsBeforeLoadingAPrunedRow(t *testing.T) {
	_, b := staleRowWithPrunedMetadata(t, true)
	exists, err := b.tableExists(context.Background(), "mydb", "cpu")
	if err != nil || !exists {
		t.Fatalf("tableExists: %v, err %v; want true after adopting the published table", exists, err)
	}
}

// A node with no catalog row sees a table another node published, so an
// emptied measurement is emptied in the published table too.
func TestTableExists_RowlessNodeSeesThePublishedTable(t *testing.T) {
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(true)
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	exists, err := b.tableExists(context.Background(), "mydb", "cpu")
	if err != nil || !exists {
		t.Fatalf("tableExists on a node with no row: %v, err %v; want true", exists, err)
	}
}

// The fork this fixes on a shared bucket without shared_storage_mode (#1609):
// a node that is not authoritative and holds a stale catalog row is fenced
// pass after pass, until its own commits pass the published version. Then it
// publishes its own lineage, and the warehouse holds two metadata files for
// the same version numbers. With adoption on, the same sequence never forks.
func TestAdopt_StaleWriterForksWithoutAdoptionOnly(t *testing.T) {
	for _, authoritative := range []bool{false, true} {
		r := newWarehouseRig(t)
		a, b := r.node(true), r.node(authoritative)
		r.addFile()
		if err := r.reconcile(a); err != nil {
			t.Fatal(err)
		}
		r.addFile()
		if err := r.reconcile(b); err != nil { // no row: b adopts, then commits
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ { // the export moves to a, which advances the table
			r.addFile()
			if err := r.reconcile(a); err != nil {
				t.Fatal(err)
			}
		}
		// The export moves back to b, whose row is now several versions behind.
		fenced := 0
		for pass := 0; pass < 10; pass++ {
			r.addFile()
			err := r.reconcile(b)
			if err == nil {
				break
			}
			if !errors.Is(err, errFenced) {
				t.Fatalf("authoritative=%v, pass %d: %v", authoritative, pass, err)
			}
			fenced++
		}
		forked := r.forkedVersions()
		if authoritative && (fenced != 0 || len(forked) != 0) {
			t.Fatalf("with adoption: %d fenced passes, forked versions %v; want none", fenced, forked)
		}
		if !authoritative && (fenced == 0 || len(forked) == 0) {
			t.Fatalf("without adoption: %d fenced passes, forked versions %v; want the climb and the fork this guards against", fenced, forked)
		}
	}
}
