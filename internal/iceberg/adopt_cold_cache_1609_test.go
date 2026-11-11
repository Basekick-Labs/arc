package iceberg

import (
	"context"
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

// Without an authoritative warehouse (local storage) nothing changes: the
// load of a row whose metadata is gone still fails, as before.
func TestTableSeed_NonAuthoritativeStillFailsOnAPrunedRow(t *testing.T) {
	_, b := staleRowWithPrunedMetadata(t, false)
	if _, _, _, err := b.tableSeed(context.Background(), "mydb", "cpu"); err == nil {
		t.Fatal("tableSeed succeeded without adoption on a row whose metadata is gone")
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

// Across many passes an authoritative node that falls behind never forks the
// table: it adopts the published version before every load.
func TestAdopt_AuthoritativeNodesNeverFork(t *testing.T) {
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(true)
	for i := 0; i < 8; i++ {
		r.addFile()
		exp := a
		if i%3 == 2 {
			exp = b
		}
		if err := r.reconcile(exp); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if f := r.forkedVersions(); len(f) != 0 {
		t.Fatalf("forked versions %v", f)
	}
	if n := r.live(a); n != len(r.files()) {
		t.Fatalf("live = %d, want %d", n, len(r.files()))
	}
}
