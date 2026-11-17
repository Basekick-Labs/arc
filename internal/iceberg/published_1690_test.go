package iceberg

// #1690: the published state Arc's Iceberg REST catalog serves. It must be the
// version directory readers see, the same on every node, and never "not
// found" for a table that is there but cannot be read right now.

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func newPublishedRig(t *testing.T, root, warehouse, layout string) *colocRig {
	t.Helper()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if warehouse == "" {
		warehouse = DefaultWarehouse(backend)
	}
	exp, err := NewExporter(db, backend, warehouse, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.SetLayout(layout); err != nil {
		t.Fatal(err)
	}
	exp.SetWarehouseAuthoritative(true)
	return &colocRig{root: root, backend: backend, db: db, exp: exp}
}

func mustLoadPublished(t *testing.T, e *Exporter, db, m string) PublishedTable {
	t.Helper()
	pt, found, err := e.LoadPublished(context.Background(), db, m)
	if err != nil {
		t.Fatalf("LoadPublished(%s, %s): %v", db, m, err)
	}
	if !found {
		t.Fatalf("LoadPublished(%s, %s): not found", db, m)
	}
	if !json.Valid(pt.Metadata) {
		t.Fatalf("metadata is not JSON")
	}
	return pt
}

func hintOf(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "version-hint.text"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPublishedServesTheHintedVersionInEveryLayout(t *testing.T) {
	for _, tc := range []struct {
		name, layout, sub string
		metaDir           []string
	}{
		{"warehouse", layoutWarehouse, "", []string{"arc_mydb.db", "cpu", "metadata"}},
		{"colocated", layoutColocated, "", []string{"mydb", "cpu", "metadata"}},
		// Non-default iceberg.warehouse: a subdirectory of the storage root (#534).
		{"warehouse-subdir", layoutWarehouse, "wh", []string{"wh", "arc_mydb.db", "cpu", "metadata"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
			wh := ""
			if tc.sub != "" {
				wh = localFileURI(filepath.Join(root, tc.sub))
			}
			r := newPublishedRig(t, root, wh, tc.layout)
			r.pass(t)
			metaDir := filepath.Join(append([]string{root}, tc.metaDir...)...)
			hint := hintOf(t, metaDir)

			pt := mustLoadPublished(t, r.exp, "mydb", "cpu")
			want := localFileURI(filepath.Join(metaDir, "v"+hint+".metadata.json"))
			if !sameLocation(pt.MetadataLocation, want) {
				t.Fatalf("metadata location = %s, want %s", pt.MetadataLocation, want)
			}
			disk, err := os.ReadFile(filepath.Join(metaDir, "v"+hint+".metadata.json"))
			if err != nil || string(disk) != string(pt.Metadata) {
				t.Fatalf("served metadata differs from v%s on disk (err %v)", hint, err)
			}

			tables, err := r.exp.PublishedTables(context.Background(), "mydb", 0)
			if err != nil || strings.Join(tables, ",") != "cpu" {
				t.Fatalf("PublishedTables = %v, %v; want [cpu]", tables, err)
			}
			dbs, err := r.exp.PublishedDatabases(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(dbs, ",") != "mydb" {
				t.Fatalf("PublishedDatabases = %v, want [mydb] (no warehouse directory listed as a database)", dbs)
			}
		})
	}
}

// A node that never exported (no catalog rows) answers exactly what the
// exporter answers: the catalog reads the warehouse, not the node's SQLite.
func TestPublishedIsTheSameOnANodeWithoutCatalogRows(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	exporter := newPublishedRig(t, root, "", layoutWarehouse)
	exporter.pass(t)
	reader := newPublishedRig(t, root, "", layoutWarehouse)
	a := mustLoadPublished(t, exporter.exp, "mydb", "cpu")
	b := mustLoadPublished(t, reader.exp, "mydb", "cpu")
	if a.MetadataLocation != b.MetadataLocation || string(a.Metadata) != string(b.Metadata) {
		t.Fatalf("exporter served %s, reader %s", a.MetadataLocation, b.MetadataLocation)
	}
	if reader.catalogLoc(t, "arc_mydb", "cpu") != "" {
		t.Fatal("serving the published version wrote a catalog row on the reader")
	}
}

// A table created under the warehouse layout is still served after the
// layout is switched to colocated, as adoption finds it.
func TestPublishedFindsATableUnderTheOtherLayout(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	newPublishedRig(t, root, "", layoutWarehouse).pass(t)
	coloc := newPublishedRig(t, root, "", layoutColocated)
	pt := mustLoadPublished(t, coloc.exp, "mydb", "cpu")
	if !strings.Contains(pt.MetadataLocation, "/arc_mydb.db/cpu/metadata/") {
		t.Fatalf("served %s, want the warehouse-layout table", pt.MetadataLocation)
	}
	tables, err := coloc.exp.PublishedTables(context.Background(), "mydb", 0)
	if err != nil || strings.Join(tables, ",") != "cpu" {
		t.Fatalf("PublishedTables = %v, %v", tables, err)
	}
}

func TestPublishedRefusesUnservableStatesInsteadOfHidingThem(t *testing.T) {
	setup := func(t *testing.T) (*colocRig, string) {
		root := t.TempDir()
		writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
		r := newPublishedRig(t, root, "", layoutWarehouse)
		r.pass(t)
		return r, filepath.Join(root, "arc_mydb.db", "cpu", "metadata")
	}

	t.Run("hint names a missing version", func(t *testing.T) {
		r, dir := setup(t)
		if err := os.WriteFile(filepath.Join(dir, "version-hint.text"), []byte("99"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err == nil || found {
			t.Fatalf("found=%v err=%v, want an error", found, err)
		}
	})

	t.Run("current manifest list is gone", func(t *testing.T) {
		r, dir := setup(t)
		pt := mustLoadPublished(t, r.exp, "mydb", "cpu")
		var meta struct {
			Cur   int64 `json:"current-snapshot-id"`
			Snaps []struct {
				ID int64  `json:"snapshot-id"`
				ML string `json:"manifest-list"`
			} `json:"snapshots"`
		}
		if err := json.Unmarshal(pt.Metadata, &meta); err != nil {
			t.Fatal(err)
		}
		removed := false
		for _, s := range meta.Snaps {
			if s.ID == meta.Cur {
				if err := os.Remove(filepath.Join(dir, filepath.Base(s.ML))); err != nil {
					t.Fatal(err)
				}
				removed = true
			}
		}
		if !removed {
			t.Fatal("no current snapshot to damage")
		}
		if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err == nil || found {
			t.Fatalf("found=%v err=%v, want an error", found, err)
		}
	})

	// Directory readers follow the hint, and a colocated DROP deletes the
	// hint first: a newer v<N> without one is not served.
	t.Run("no hint is not published", func(t *testing.T) {
		r, dir := setup(t)
		if err := os.Remove(filepath.Join(dir, "version-hint.text")); err != nil {
			t.Fatal(err)
		}
		if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err != nil || found {
			t.Fatalf("found=%v err=%v, want not found", found, err)
		}
		if tables, err := r.exp.PublishedTables(context.Background(), "mydb", 0); err != nil || len(tables) != 0 {
			t.Fatalf("PublishedTables = %v, %v; want none", tables, err)
		}
	})

	// A drop marker does not hide a published table: the exporter clears a
	// marker at most once per process, so one can outlive a recreated
	// table's publish (see the package comment).
	t.Run("a stale drop marker does not hide a published table", func(t *testing.T) {
		r, _ := setup(t)
		r.exp.SetForgetDroppedRows(true)
		if err := r.backend.Write(context.Background(), droppedMarkerKey("arc_mydb", "cpu"), []byte("{}")); err != nil {
			t.Fatal(err)
		}
		mustLoadPublished(t, r.exp, "mydb", "cpu")
	})

	// One damaged table fails its own load, not every listing.
	t.Run("an unreadable hint fails the load and keeps the listing", func(t *testing.T) {
		r, dir := setup(t)
		if err := os.WriteFile(filepath.Join(dir, "version-hint.text"), []byte("garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		if tables, err := r.exp.PublishedTables(context.Background(), "mydb", 0); err != nil || strings.Join(tables, ",") != "cpu" {
			t.Fatalf("PublishedTables = %v, %v; want [cpu]", tables, err)
		}
		if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err == nil || found {
			t.Fatalf("found=%v err=%v, want an error", found, err)
		}
	})
}

func TestPublishedNotFoundAndUnsafeNames(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	if err := os.MkdirAll(filepath.Join(root, "mydb", "notexported"), 0o700); err != nil {
		t.Fatal(err)
	}
	r := newPublishedRig(t, root, "", layoutWarehouse)
	r.pass(t)
	for _, tc := range [][2]string{
		{"mydb", "nope"}, {"other", "cpu"}, {"..", "cpu"}, {"mydb", ".."}, {"mydb", "a/b"},
		{"mydb", "%2e%2e"}, {"_iceberg_dropped", "cpu"}, {"mydb", ""}, {"a\x00b", "cpu"},
	} {
		if _, found, err := r.exp.LoadPublished(context.Background(), tc[0], tc[1]); err != nil || found {
			t.Fatalf("LoadPublished(%q, %q) = found %v, err %v; want not found", tc[0], tc[1], found, err)
		}
	}
	// A measurement directory with no exported table is not listed.
	tables, err := r.exp.PublishedTables(context.Background(), "mydb", 0)
	if err != nil || strings.Join(tables, ",") != "cpu" {
		t.Fatalf("PublishedTables = %v, %v", tables, err)
	}
	if tables, err := r.exp.PublishedTables(context.Background(), "..", 0); err != nil || len(tables) != 0 {
		t.Fatalf("PublishedTables(..) = %v, %v", tables, err)
	}
}

func TestPublishedTablesLimitStopsEarly(t *testing.T) {
	root := t.TempDir()
	for _, m := range []string{"a", "b", "c"} {
		writeArcStyleParquet(t, filepath.Join(root, "mydb", m, "2023/11/14/22", m+"_1.parquet"), 1_700_000_000_000_000, 5)
	}
	r := newPublishedRig(t, root, "", layoutWarehouse)
	r.pass(t)
	all, err := r.exp.PublishedTables(context.Background(), "mydb", 0)
	if err != nil || len(all) != 3 {
		t.Fatalf("all = %v, %v", all, err)
	}
	one, err := r.exp.PublishedTables(context.Background(), "mydb", 1)
	if err != nil || len(one) != 1 {
		t.Fatalf("limit 1 = %v, %v", one, err)
	}
}

// A database whose hot data is gone still has its table, found through the
// warehouse namespace directory.
func TestPublishedDatabasesIncludesWarehouseOnlyDatabases(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newPublishedRig(t, root, localFileURI(filepath.Join(root, "wh")), layoutWarehouse)
	r.pass(t)
	if err := os.RemoveAll(filepath.Join(root, "mydb")); err != nil {
		t.Fatal(err)
	}
	dbs, err := r.exp.PublishedDatabases(context.Background())
	if err != nil || strings.Join(dbs, ",") != "mydb" {
		t.Fatalf("PublishedDatabases = %v, %v", dbs, err)
	}
	tables, err := r.exp.PublishedTables(context.Background(), "mydb", 0)
	if err != nil || strings.Join(tables, ",") != "cpu" {
		t.Fatalf("PublishedTables = %v, %v", tables, err)
	}
}

func TestPublishedDropDatabaseRemovesTheTable(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newPublishedRig(t, root, "", layoutWarehouse)
	r.pass(t)
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err != nil || found {
		t.Fatalf("after DROP: found=%v err=%v", found, err)
	}
}

func TestPublishedAddressable(t *testing.T) {
	root := t.TempDir()
	if !newPublishedRig(t, root, "", layoutWarehouse).exp.PublishedAddressable() {
		t.Fatal("default warehouse not addressable")
	}
	if !newPublishedRig(t, root, localFileURI(filepath.Join(root, "wh")), layoutWarehouse).exp.PublishedAddressable() {
		t.Fatal("subdirectory warehouse not addressable")
	}
	outside := localFileURI(filepath.Join(t.TempDir(), "elsewhere"))
	if newPublishedRig(t, root, outside, layoutWarehouse).exp.PublishedAddressable() {
		t.Fatal("a warehouse outside the storage root reported addressable")
	}
}

// #1690 review H1: a warehouse namespace directory is not a database. With
// the warehouse at the storage root, "arc_mydb.db/cpu" is the warehouse
// table's own directory, which would otherwise read as a colocated table of
// a database named "arc_mydb.db" and be checked against that name's grants.
func TestPublishedRefusesWarehouseDirectoriesAsDatabases(t *testing.T) {
	for _, layout := range []string{layoutWarehouse, layoutColocated} {
		t.Run(layout, func(t *testing.T) {
			root := t.TempDir()
			writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
			newPublishedRig(t, root, "", layoutWarehouse).pass(t)
			r := newPublishedRig(t, root, "", layout)
			if _, found, err := r.exp.LoadPublished(context.Background(), "arc_mydb.db", "cpu"); err != nil || found {
				t.Fatalf("LoadPublished(arc_mydb.db, cpu) = found %v, err %v; want not found", found, err)
			}
			if tables, err := r.exp.PublishedTables(context.Background(), "arc_mydb.db", 0); err != nil || len(tables) != 0 {
				t.Fatalf("PublishedTables(arc_mydb.db) = %v, %v", tables, err)
			}
		})
	}
	// A subdirectory warehouse's root directory is not a database either.
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newPublishedRig(t, root, localFileURI(filepath.Join(root, "wh")), layoutWarehouse)
	r.pass(t)
	if tables, err := r.exp.PublishedTables(context.Background(), "wh", 0); err != nil || len(tables) != 0 {
		t.Fatalf("PublishedTables(wh) = %v, %v", tables, err)
	}
}

// #1690 review H2: a colocated DROP deletes the hint first; from then on the
// table is gone from the catalog, though v<N> files remain for a moment.
func TestPublishedColocatedTableWithoutItsHintIsGone(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newPublishedRig(t, root, "", layoutColocated)
	r.pass(t)
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/cpu_b.parquet"), 1_700_000_100_000_000, 5)
	r.pass(t)
	if err := os.Remove(filepath.Join(root, "mydb", "cpu", "metadata", "version-hint.text")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err != nil || found {
		t.Fatalf("found=%v err=%v, want not found", found, err)
	}
}

// Shared bucket with a coordinator: the database is dropped and recreated
// while the same exporter keeps running. The drop marker can survive the
// recreated table's publish (clearDropMarker runs once per process); the
// catalog must serve the new table on the exporter and on any other node.
func TestPublishedServesATableRecreatedAfterADrop(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newPublishedRig(t, root, "", layoutWarehouse)
	r.exp.SetForgetDroppedRows(true)
	r.pass(t)
	if err := r.exp.DropDatabase(context.Background(), "mydb"); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "mydb")); err != nil {
		t.Fatal(err)
	}
	if _, found, err := r.exp.LoadPublished(context.Background(), "mydb", "cpu"); err != nil || found {
		t.Fatalf("after DROP: found=%v err=%v", found, err)
	}
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/15/10/cpu_b.parquet"), 1_700_050_000_000_000, 7)
	r.pass(t)
	mustLoadPublished(t, r.exp, "mydb", "cpu")
	other := newPublishedRig(t, root, "", layoutWarehouse)
	other.exp.SetForgetDroppedRows(true)
	mustLoadPublished(t, other.exp, "mydb", "cpu")
}
