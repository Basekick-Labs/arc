package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1730: a hub deleting a spoke namespace (DELETE /databases/{spoke}) in the
// warehouse layout. The spoke's tables live in the namespace
// {<prefix>_<spoke>, <db>}, whose warehouse directory is a sibling of the
// spoke's own, "<prefix>_<spoke>.<db>.db". Both the catalog rows and that
// directory must go.
func TestDropDatabaseOfASpokeNamespaceRemovesItsWarehouseTables(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "local"
		if shared {
			name = "shared bucket"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			f := filepath.Join(root, "spoke01/mydb/cpu/2023/11/14/22/cpu_a.parquet")
			writeArcStyleParquet(t, f, 1_700_000_000_000_000, 50)
			r := newColocRig(t, root, "a.db", layoutWarehouse)
			r.exp.SetForgetDroppedRows(shared)
			sc, err := SchemaFromParquet(f)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.exp.ReconcileMeasurement(ctx, "spoke01/mydb", "cpu", sc, []FileRef{refOf(t, f)}); err != nil {
				t.Fatalf("reconcile the spoke table: %v", err)
			}
			ns2, err := namespaceIdentifier("arc", "spoke01/mydb")
			if err != nil {
				t.Fatal(err)
			}
			nsKey := catalogNamespaceKey(ns2)
			nsDir := filepath.Join(root, nsKey+".db")
			if _, err := os.Stat(nsDir); err != nil {
				t.Fatalf("the spoke table is not in the warehouse at %s: %v", nsDir, err)
			}
			// The database DELETE removed the data files first.
			if err := os.RemoveAll(filepath.Join(root, "spoke01")); err != nil {
				t.Fatal(err)
			}

			if err := r.exp.DropDatabase(ctx, "spoke01"); err != nil {
				t.Fatalf("DropDatabase: %v", err)
			}
			if loc := r.catalogLoc(t, nsKey, "cpu"); loc != "" {
				t.Fatalf("the spoke table's catalog row survived: %q", loc)
			}
			if left, _ := r.backend.List(ctx, nsKey+".db/"); len(left) != 0 {
				t.Fatalf("left in the spoke namespace's warehouse directory: %v", left)
			}
			if shared {
				if ok, err := r.backend.Exists(ctx, droppedMarkerKey(nsKey, "cpu")); err != nil || !ok {
					t.Fatalf("spoke table drop marker = (%v, %v), want written", ok, err)
				}
			}
			if err := r.exp.DropDatabase(ctx, "spoke01"); err != nil {
				t.Fatalf("second DropDatabase: %v", err)
			}
		})
	}
}

// dropRig is an exporter over root with its own catalog and warehouse URI.
type dropRig struct {
	root    string
	backend storage.Backend
	db      *sql.DB
	exp     *Exporter
}

func newDropRig(t *testing.T, root, warehouse string, backend storage.Backend) *dropRig {
	t.Helper()
	if backend == nil {
		var err error
		if backend, err = storage.NewLocalBackend(root, zerolog.Nop()); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, warehouse, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return &dropRig{root: root, backend: backend, db: db, exp: exp}
}

// export reconciles one spoke table from a fresh data file.
func (r *dropRig) export(t *testing.T, database, measurement string) {
	t.Helper()
	f := filepath.Join(r.root, filepath.FromSlash(database), measurement, "2023/11/14/22", measurement+"_a.parquet")
	writeArcStyleParquet(t, f, 1_700_000_000_000_000, 5)
	sc, err := SchemaFromParquet(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.exp.ReconcileMeasurement(context.Background(), database, measurement, sc, []FileRef{refOf(t, f)}); err != nil {
		t.Fatalf("reconcile %s/%s: %v", database, measurement, err)
	}
}

func (r *dropRig) rows(t *testing.T) []string {
	t.Helper()
	rs, err := r.db.Query(`SELECT table_namespace || '|' || table_name FROM iceberg_tables ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var s string
		if err := rs.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func dirExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// A non-default warehouse: a subdirectory of the storage root (#534 shape).
func TestDropSpokeWithTheWarehouseInASubdirectory(t *testing.T) {
	root := t.TempDir()
	r := newDropRig(t, root, localFileURI(filepath.Join(root, "wh")), nil)
	r.export(t, "spoke01/mydb", "cpu")
	dir := filepath.Join(root, "wh", "arc_spoke01.mydb.db")
	if !dirExists(dir) {
		t.Fatalf("fixture: no warehouse dir %s", dir)
	}
	if err := r.exp.DropDatabase(context.Background(), "spoke01"); err != nil {
		t.Fatal(err)
	}
	if rows := r.rows(t); len(rows) != 0 {
		t.Fatalf("catalog rows left: %v", rows)
	}
	if left, _ := r.backend.List(context.Background(), "wh/arc_spoke01.mydb.db/"); len(left) != 0 {
		t.Fatalf("warehouse objects left: %v", left)
	}
}

// A warehouse outside the storage root: the backend cannot address it, so
// only the catalog rows go, and the drop does not fail.
func TestDropSpokeWithTheWarehouseOutsideTheRoot(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	r := newDropRig(t, root, localFileURI(outside), nil)
	r.export(t, "spoke01/mydb", "cpu")
	if err := r.exp.DropDatabase(context.Background(), "spoke01"); err != nil {
		t.Fatal(err)
	}
	if rows := r.rows(t); len(rows) != 0 {
		t.Fatalf("catalog rows left: %v", rows)
	}
}

// The node running the drop never exported (a shared bucket's primary
// writer, #1611): another node's spoke tables are found in the warehouse,
// their markers written and their metadata deleted.
func TestDropSpokeFromANodeThatNeverExported(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	exporter := newDropRig(t, root, localFileURI(root), nil)
	exporter.export(t, "spoke01/mydb", "cpu")
	writer := newDropRig(t, root, localFileURI(root), exporter.backend)
	writer.exp.SetForgetDroppedRows(true)
	if err := writer.exp.DropDatabase(ctx, "spoke01"); err != nil {
		t.Fatal(err)
	}
	if left, _ := writer.backend.List(ctx, "arc_spoke01.mydb.db/"); len(left) != 0 {
		t.Fatalf("warehouse objects left: %v", left)
	}
	if ok, err := writer.backend.Exists(ctx, droppedMarkerKey("arc_spoke01.mydb", "cpu")); err != nil || !ok {
		t.Fatalf("drop marker = (%v, %v), want written for the exporter's row", ok, err)
	}
}

// Spoke IDs may contain dots: spoke "edge.x" must not claim spoke "edge"'s
// database "x" (same plain key "arc_edge.x"), and spoke "edge" must not claim
// "edge.x"'s databases. A spoke "edge1" sharing the prefix is left alone.
func TestDropSpokeLeavesSiblingSpokes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	r := newDropRig(t, root, localFileURI(root), nil)
	r.export(t, "edge/x", "cpu")
	r.export(t, "edge.x/y", "mem")
	r.export(t, "edge1/z", "disk")
	edgeX := catalogNamespaceKey(icetable.Identifier{"arc_edge.x", "y"})

	if err := r.exp.DropDatabase(ctx, "edge.x"); err != nil {
		t.Fatal(err)
	}
	rows := r.rows(t)
	if strings.Join(rows, ",") != "arc_edge.x|cpu,arc_edge1.z|disk" {
		t.Fatalf("after dropping edge.x: %v", rows)
	}
	if !dirExists(filepath.Join(root, "arc_edge.x.db")) || dirExists(filepath.Join(root, edgeX+".db")) {
		t.Fatalf("dropping edge.x: edge's dir kept=%v, edge.x's dir removed=%v",
			dirExists(filepath.Join(root, "arc_edge.x.db")), !dirExists(filepath.Join(root, edgeX+".db")))
	}

	r.export(t, "edge.x/y", "mem")
	// And a pre-#1129 table of spoke "edge.x" under the plain key
	// "arc_edge.x.y", which spoke "edge" must not read as its database "x.y".
	legacyEdgeX := filepath.Join(root, "arc_edge.x.y.db", "old", "metadata", "v1.metadata.json")
	if err := os.MkdirAll(filepath.Dir(legacyEdgeX), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyEdgeX, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.exp.DropDatabase(ctx, "edge"); err != nil {
		t.Fatal(err)
	}
	if !dirExists(legacyEdgeX) {
		t.Fatal("dropping edge deleted spoke edge.x's legacy table")
	}
	rows = r.rows(t)
	if strings.Join(rows, ",") != edgeX+"|mem,arc_edge1.z|disk" {
		t.Fatalf("after dropping edge: %v", rows)
	}
	if dirExists(filepath.Join(root, "arc_edge.x.db")) || !dirExists(filepath.Join(root, edgeX+".db")) || !dirExists(filepath.Join(root, "arc_edge1.z.db")) {
		t.Fatal("dropping edge touched the wrong warehouse directories")
	}
}

// A dotted spoke ID's table written before #1129: its warehouse directory is
// the plain key, and its catalog row is either still there (the namespace
// migration defaults to dry run) or rekeyed to the encoded form. Both go.
func TestDropSpokeWithALegacyDottedNamespace(t *testing.T) {
	for _, migrated := range []bool{false, true} {
		name := "unmigrated"
		if migrated {
			name = "migrated"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			r := newDropRig(t, root, localFileURI(root), nil)
			r.exp.SetForgetDroppedRows(true)
			database, measurement := "rocket.01/telemetry", "cpu"
			identifier := r.exp.tableIdent(database, measurement)
			encoded := catalogNamespaceKey(identifier[:len(identifier)-1])
			legacy := "arc_rocket.01.telemetry"

			f := filepath.Join(root, "rocket.01", "telemetry", measurement, "2026", "07", "14", "15", "a.parquet")
			writeArcStyleParquet(t, f, 1_752_500_000_000_000, 2)
			sc, err := UnionSchema(ctx, []string{f})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.exp.EnsureTable(ctx, database, measurement, sc); err != nil {
				t.Fatal(err)
			}
			if _, _, err := r.exp.catalog.CommitTable(ctx, identifier, nil,
				[]icetable.Update{icetable.NewSetLocationUpdate(localFileURI(filepath.Join(root, legacy+".db", measurement)))}); err != nil {
				t.Fatal(err)
			}
			if err := r.exp.ReconcileMeasurement(ctx, database, measurement, sc, []FileRef{refOf(t, f)}); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(filepath.Join(root, encoded+".db")); err != nil {
				t.Fatal(err)
			}
			if !migrated {
				if _, err := r.db.ExecContext(ctx, `UPDATE iceberg_tables SET table_namespace = ? WHERE table_namespace = ?`, legacy, encoded); err != nil {
					t.Fatal(err)
				}
			}
			if !dirExists(filepath.Join(root, legacy+".db")) {
				t.Fatal("fixture: no legacy warehouse dir")
			}

			if err := r.exp.DropDatabase(ctx, "rocket.01"); err != nil {
				t.Fatal(err)
			}
			if rows := r.rows(t); len(rows) != 0 {
				t.Fatalf("catalog rows left: %v", rows)
			}
			if left, _ := r.backend.List(ctx, legacy+".db/"); len(left) != 0 {
				t.Fatalf("legacy warehouse objects left: %v", left)
			}
			for _, nsk := range []string{encoded, legacy} {
				if ok, err := r.backend.Exists(ctx, droppedMarkerKey(nsk, measurement)); err != nil || !ok {
					t.Fatalf("drop marker under %q = (%v, %v), want written", nsk, ok, err)
				}
			}
		})
	}
}

// A spoke database known only as an empty namespace (no table yet) is
// dropped too.
func TestDropSpokeDropsAnEmptySubNamespace(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	r := newDropRig(t, root, localFileURI(root), nil)
	if err := r.exp.catalog.CreateNamespace(ctx, icetable.Identifier{"arc_spoke01", "mydb"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.exp.DropDatabase(ctx, "spoke01"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := r.exp.catalog.CheckNamespaceExists(ctx, icetable.Identifier{"arc_spoke01", "mydb"}); ok {
		t.Fatal("the empty spoke namespace survived")
	}
}
