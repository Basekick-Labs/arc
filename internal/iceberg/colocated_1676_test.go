package iceberg

// #1676: iceberg.layout = "colocated" creates new tables in the measurement's
// own directory, <db>/<measurement>/metadata, so a reader that reaches data
// only through the table folder (a OneLake shortcut) finds every data file
// under it. Existing tables keep their location whichever layout is set.

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	icetable "github.com/apache/iceberg-go/table"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type colocRig struct {
	root    string
	backend *storage.LocalBackend
	db      *sql.DB
	exp     *Exporter
}

// newColocRig builds an exporter over a local storage root with the default
// warehouse (the storage root) and the given layout. dbFile names the
// catalog, so two rigs on one root model two nodes.
func newColocRig(t *testing.T, root, dbFile, layout string) *colocRig {
	t.Helper()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), dbFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, DefaultWarehouse(backend), "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.SetLayout(layout); err != nil {
		t.Fatalf("SetLayout(%q): %v", layout, err)
	}
	exp.SetWarehouseAuthoritative(true)
	return &colocRig{root: root, backend: backend, db: db, exp: exp}
}

func (r *colocRig) pass(t *testing.T) {
	t.Helper()
	src := NewStorageWalkSource(r.backend, "arc", zerolog.Nop())
	NewScheduler(SchedulerConfig{Exporter: r.exp, Source: src, Logger: zerolog.Nop()}).runPass(context.Background())
}

func (r *colocRig) catalogLoc(t *testing.T, ns, table string) string {
	t.Helper()
	var loc string
	err := r.db.QueryRow(`SELECT metadata_location FROM iceberg_tables WHERE table_namespace = ? AND table_name = ?`, ns, table).Scan(&loc)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

func readMetaLocation(t *testing.T, metaFile string) string {
	t.Helper()
	body, err := os.ReadFile(metaFile)
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Location string `json:"location"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	return m.Location
}

func TestColocatedLayoutCreatesTheTableInTheMeasurementDirectory(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newColocRig(t, root, "a.db", layoutColocated)
	r.pass(t)

	metaDir := filepath.Join(root, "mydb", "cpu", "metadata")
	if _, err := os.Stat(filepath.Join(metaDir, "version-hint.text")); err != nil {
		t.Fatalf("no version-hint in the measurement's metadata directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "arc_mydb.db")); !os.IsNotExist(err) {
		t.Fatalf("the warehouse namespace directory was created under the colocated layout: %v", err)
	}
	loc := readMetaLocation(t, filepath.Join(metaDir, "v1.metadata.json"))
	if !sameLocation(loc, localFileURI(filepath.Join(root, "mydb", "cpu"))) {
		t.Fatalf("table location = %q, want the measurement directory", loc)
	}
	files, err := r.exp.tableDataFiles(context.Background(), mustLoad(t, r.exp, "mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("data files = %v, want the one Parquet file", files)
	}
	for f := range files {
		if !strings.HasSuffix(f, "/mydb/cpu/2023/11/14/22/cpu_a.parquet") {
			t.Fatalf("data file %q is not the measurement's Parquet", f)
		}
	}

	// A second pass must not register the metadata as data.
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/cpu_b.parquet"), 1_700_000_100_000_000, 10)
	r.pass(t)
	files, err = r.exp.tableDataFiles(context.Background(), mustLoad(t, r.exp, "mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("data files after the second pass = %d, want 2 (the metadata must not be registered)", len(files))
	}
	for f := range files {
		if strings.Contains(f, "/metadata/") {
			t.Fatalf("the table registered its own metadata as data: %s", f)
		}
	}
}

func mustLoad(t *testing.T, exp *Exporter, database, measurement string) *icetable.Table {
	t.Helper()
	tbl, err := exp.loadTableRecovering(context.Background(), exp.tableIdent(database, measurement))
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func TestSwitchingTheLayoutKeepsExistingTablesWhereTheyAre(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	before := newColocRig(t, root, "node.db", layoutWarehouse)
	before.pass(t)
	legacy := before.catalogLoc(t, "arc_mydb", "cpu")
	if !strings.Contains(legacy, "/arc_mydb.db/cpu/metadata/") {
		t.Fatalf("warehouse-layout table at %q", legacy)
	}

	// The same node (same catalog) restarted with the colocated layout.
	after := &colocRig{root: root, backend: before.backend, db: before.db}
	exp, err := NewExporter(before.db, before.backend, DefaultWarehouse(before.backend), "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := exp.SetLayout(layoutColocated); err != nil {
		t.Fatal(err)
	}
	exp.SetWarehouseAuthoritative(true)
	after.exp = exp
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/23/cpu_b.parquet"), 1_700_000_100_000_000, 10)
	writeArcStyleParquet(t, filepath.Join(root, "mydb/mem/2023/11/14/22/mem_a.parquet"), 1_700_000_000_000_000, 5)
	after.pass(t)

	if loc := after.catalogLoc(t, "arc_mydb", "cpu"); !strings.Contains(loc, "/arc_mydb.db/cpu/metadata/") {
		t.Fatalf("the existing table moved to %q; it must keep its location", loc)
	}
	if _, err := os.Stat(filepath.Join(root, "mydb", "cpu", "metadata")); !os.IsNotExist(err) {
		t.Fatalf("a second, colocated table was created for an existing one: %v", err)
	}
	if loc := after.catalogLoc(t, "arc_mydb", "mem"); !strings.Contains(loc, "/mydb/mem/metadata/") {
		t.Fatalf("a new table under the colocated layout is at %q", loc)
	}
}

// A node with no catalog row adopts the table another node published, under
// whichever layout it was created, rather than creating a second one.
func TestNoRowAdoptsATablePublishedUnderTheOtherLayout(t *testing.T) {
	for _, tc := range []struct{ creator, adopter, wantPath string }{
		{layoutWarehouse, layoutColocated, "/arc_mydb.db/cpu/metadata/"},
		{layoutColocated, layoutWarehouse, "/mydb/cpu/metadata/"},
	} {
		t.Run(tc.creator+"->"+tc.adopter, func(t *testing.T) {
			root := t.TempDir()
			writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
			a := newColocRig(t, root, "a.db", tc.creator)
			a.pass(t)
			b := newColocRig(t, root, "b.db", tc.adopter)
			b.pass(t)
			if loc := b.catalogLoc(t, "arc_mydb", "cpu"); !strings.Contains(loc, tc.wantPath) {
				t.Fatalf("node B's row = %q, want the table node A published (%s)", loc, tc.wantPath)
			}
			other := filepath.Join(root, "mydb", "cpu", "metadata")
			if tc.creator == layoutColocated {
				other = filepath.Join(root, "arc_mydb.db")
			}
			if _, err := os.Stat(other); !os.IsNotExist(err) {
				t.Fatalf("node B created a second table at %s: %v", other, err)
			}
		})
	}
}

func TestDropDatabaseRemovesColocatedMetadataAfterItsMarkers(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	r := newColocRig(t, root, "a.db", layoutColocated)
	r.exp.SetForgetDroppedRows(true)
	r.pass(t)

	// The database DELETE removes the data and leaves the metadata keys
	// OwnsMetadataKey claims.
	keys, err := r.backend.List(ctx, "mydb/")
	if err != nil {
		t.Fatal(err)
	}
	left := 0
	for _, k := range keys {
		if r.exp.OwnsMetadataKey(k) {
			left++
			continue
		}
		if err := r.backend.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	if left == 0 {
		t.Fatal("OwnsMetadataKey claimed none of the table's metadata")
	}

	if err := r.exp.DropDatabase(ctx, "mydb"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	if ok, err := r.backend.Exists(ctx, droppedMarkerKey("arc_mydb", "cpu")); err != nil || !ok {
		t.Fatalf("drop marker = (%v, %v), want written", ok, err)
	}
	if _, err := os.Stat(filepath.Join(root, "mydb", "cpu", "metadata")); !os.IsNotExist(err) {
		t.Fatalf("colocated metadata survived DropDatabase: %v", err)
	}
	if loc := r.catalogLoc(t, "arc_mydb", "cpu"); loc != "" {
		t.Fatalf("catalog row survived DropDatabase: %q", loc)
	}
}

// Metadata a commit racing a database DROP left in the measurement's
// directory, with no data and no catalog row, must not be adopted back into
// an empty table (which would keep the dropped database listed).
func TestColocatedResidueWithNoDataIsNotAdopted(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	a := newColocRig(t, root, "a.db", layoutColocated)
	a.pass(t)
	if err := os.RemoveAll(filepath.Join(root, "mydb", "cpu", "2023")); err != nil {
		t.Fatal(err)
	}
	b := newColocRig(t, root, "b.db", layoutColocated)
	b.pass(t)
	if loc := b.catalogLoc(t, "arc_mydb", "cpu"); loc != "" {
		t.Fatalf("a node with no row adopted a table with no data: %q", loc)
	}
}

func TestAdoptRefusesColocatedMetadataFromAnotherRoot(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	a := newColocRig(t, root, "a.db", layoutColocated)
	a.pass(t)
	// A backup of that root restored somewhere else: the metadata still names
	// the first root.
	other := t.TempDir()
	if err := os.CopyFS(other, os.DirFS(root)); err != nil {
		t.Fatal(err)
	}
	b := newColocRig(t, other, "b.db", layoutColocated)
	_, err := b.exp.adoptPublished(context.Background(), b.exp.tableIdent("mydb", "cpu"), "arc_mydb", "mydb", "cpu")
	if err == nil || !strings.Contains(err.Error(), "restored from another storage root") {
		t.Fatalf("adopting foreign-root metadata = %v, want a refusal naming the cause", err)
	}
}

func TestSetLayoutColocatedNeedsTheWarehouseToBeTheStorageRoot(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		warehouse string
		ok        bool
	}{
		{DefaultWarehouse(backend), true},
		{DefaultWarehouse(backend) + "/", true},                                                // trailing slash, same directory
		{NormalizeLocalWarehouse(root), true},                                                  // bare path, same directory
		{DefaultWarehouse(backend) + "/wh", false},                                             // a subdirectory
		{localFileURI(filepath.Join(filepath.Dir(root), filepath.Base(root)+"-other")), false}, // a sibling sharing the prefix
	} {
		exp, err := NewExporter(db, backend, tc.warehouse, "arc", 2, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		err = exp.SetLayout(layoutColocated)
		if (err == nil) != tc.ok {
			t.Fatalf("SetLayout(colocated) with warehouse %q = %v, want ok=%v", tc.warehouse, err, tc.ok)
		}
	}
}

func TestColocatedLocationEscapesNames(t *testing.T) {
	root := t.TempDir()
	r := newColocRig(t, root, "a.db", layoutColocated)
	loc, err := r.exp.colocatedTableLocation("my db", "c%u")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("location %q does not parse: %v", loc, err)
	}
	if u.Fragment != "" || u.RawQuery != "" {
		t.Fatalf("location %q parsed with a fragment or query", loc)
	}
	if !strings.HasSuffix(u.Path, "/my db/c%u") {
		t.Fatalf("location path = %q, want the literal names", u.Path)
	}
}

func TestIsColocatedMetadataKey(t *testing.T) {
	for key, want := range map[string]bool{
		"mydb/cpu/metadata/v1.metadata.json":       true,
		"mydb/cpu/metadata/version-hint.text":      true,
		"mydb/cpu/metadata/snap-1-0-abc.avro":      true,
		"spoke/mydb/cpu/metadata/v1.metadata.json": true,
		"mydb/cpu/metadata/stray.parquet":          false, // data is never metadata
		"mydb/cpu/2023/11/14/22/cpu_a.parquet":     false,
		"mydb/metadata/v1.metadata.json":           false, // no measurement above it
		"mydb/cpu/metadata/":                       false,
	} {
		if got := IsColocatedMetadataKey(key); got != want {
			t.Errorf("IsColocatedMetadataKey(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestDatabaseOfIdent(t *testing.T) {
	for _, tc := range []struct {
		ident []string
		want  string
	}{
		{[]string{"arc_mydb", "cpu"}, "mydb"},
		{[]string{"arc_spoke1", "mydb", "cpu"}, "spoke1/mydb"},
		{[]string{"other_mydb", "cpu"}, ""},
		{[]string{"cpu"}, ""},
	} {
		if got := databaseOfIdent("arc", tc.ident); got != tc.want {
			t.Errorf("databaseOfIdent(%v) = %q, want %q", tc.ident, got, tc.want)
		}
	}
}

// On a shared bucket the export moves with the compactor lease, so a node
// with no row must still empty a published colocated table whose data is all
// gone (#1609); only a set drop marker means it is a DROP's residue.
func TestSharedBucketRowlessNodeEmptiesAPublishedColocatedTable(t *testing.T) {
	root := t.TempDir()
	writeArcStyleParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 1_700_000_000_000_000, 50)
	a := newColocRig(t, root, "a.db", layoutColocated)
	a.exp.SetForgetDroppedRows(true)
	a.pass(t)
	if err := os.RemoveAll(filepath.Join(root, "mydb", "cpu", "2023")); err != nil {
		t.Fatal(err)
	}
	b := newColocRig(t, root, "b.db", layoutColocated)
	b.exp.SetForgetDroppedRows(true)
	exists, err := b.exp.tableExists(context.Background(), "mydb", "cpu")
	if err != nil || !exists {
		t.Fatalf("tableExists on a shared-bucket node with no row = (%v, %v), want the published table", exists, err)
	}
	b.pass(t)
	files, err := b.exp.tableDataFiles(context.Background(), mustLoad(t, b.exp, "mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("the published table still lists %d deleted files", len(files))
	}
}

// A hub deleting a spoke namespace (DELETE /databases/{spoke}): the spoke
// tables' colocated metadata, <spoke>/<db>/<m>/metadata, which the DELETE
// leaves to DropDatabase, is collected and removed with its catalog rows,
// so the namespace does not stay listed and undeletable.
func TestDropDatabaseOfASpokeNamespaceRemovesItsColocatedTables(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	f := filepath.Join(root, "spoke01/mydb/cpu/2023/11/14/22/cpu_a.parquet")
	writeArcStyleParquet(t, f, 1_700_000_000_000_000, 50)
	r := newColocRig(t, root, "a.db", layoutColocated)
	r.exp.SetForgetDroppedRows(true)
	sc, err := SchemaFromParquet(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.exp.ReconcileMeasurement(ctx, "spoke01/mydb", "cpu", sc, []FileRef{refOf(t, f)}); err != nil {
		t.Fatalf("reconcile the spoke table: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "spoke01", "mydb", "cpu", "metadata", "version-hint.text")); err != nil {
		t.Fatalf("the spoke table is not colocated: %v", err)
	}
	keys, err := r.backend.List(ctx, "spoke01/")
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !r.exp.OwnsMetadataKey(k) {
			if err := r.backend.Delete(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := r.exp.DropDatabase(ctx, "spoke01"); err != nil {
		t.Fatalf("DropDatabase: %v", err)
	}
	if left, _ := r.backend.List(ctx, "spoke01/"); len(left) != 0 {
		t.Fatalf("left under the spoke namespace after the drop: %v", left)
	}
	ns2, err := namespaceIdentifier("arc", "spoke01/mydb")
	if err != nil {
		t.Fatal(err)
	}
	if loc := r.catalogLoc(t, catalogNamespaceKey(ns2), "cpu"); loc != "" {
		t.Fatalf("the spoke table's catalog row survived: %q", loc)
	}
	if ok, err := r.backend.Exists(ctx, droppedMarkerKey(catalogNamespaceKey(ns2), "cpu")); err != nil || !ok {
		t.Fatalf("spoke table drop marker = (%v, %v), want written", ok, err)
	}
}
