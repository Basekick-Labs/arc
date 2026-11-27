package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #939 A2: the warehouse, not a node's catalog row, is the authority on a
// table's version. Two "nodes" here are two Exporters with their own SQLite
// catalogs over one storage root, as two shared-storage nodes are.

type warehouseRig struct {
	t       *testing.T
	root    string
	backend storage.Backend
	base    int64
	n       int
}

func newWarehouseRig(t *testing.T) *warehouseRig {
	t.Helper()
	root := t.TempDir()
	b, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return &warehouseRig{t: t, root: root, backend: b, base: 1_700_000_000_000_000}
}

// node is one exporter with its own catalog over the shared root.
func (r *warehouseRig) node(authoritative bool) *Exporter {
	r.t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(r.t.TempDir(), "arc.db"))
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, r.backend, DefaultWarehouse(r.backend), "arc", 3, zerolog.Nop())
	if err != nil {
		r.t.Fatal(err)
	}
	exp.SetWarehouseAuthoritative(authoritative)
	return exp
}

// addFile writes one more data file and returns the measurement's whole set.
func (r *warehouseRig) addFile() []FileRef {
	r.t.Helper()
	r.n++
	key := fmt.Sprintf("mydb/cpu/2023/11/14/22/f%03d.parquet", r.n)
	writeArcStyleParquet(r.t, filepath.Join(r.root, key), r.base+int64(r.n)*1_000_000, 5)
	return r.files()
}

func (r *warehouseRig) files() []FileRef {
	r.t.Helper()
	var refs []FileRef
	for i := 1; i <= r.n; i++ {
		p := filepath.Join(r.root, fmt.Sprintf("mydb/cpu/2023/11/14/22/f%03d.parquet", i))
		if _, err := os.Stat(p); err == nil {
			refs = append(refs, refOf(r.t, p))
		}
	}
	return refs
}

func (r *warehouseRig) reconcile(exp *Exporter) error {
	r.t.Helper()
	sc, err := SchemaFromParquet(filepath.Join(r.root, "mydb/cpu/2023/11/14/22/f001.parquet"))
	if err != nil {
		r.t.Fatal(err)
	}
	return exp.ReconcileMeasurement(context.Background(), "mydb", "cpu", sc, r.files())
}

func (r *warehouseRig) metaDir() string {
	return filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata")
}

func (r *warehouseRig) hint() int {
	r.t.Helper()
	data, err := os.ReadFile(filepath.Join(r.metaDir(), "version-hint.text"))
	if err != nil {
		r.t.Fatal(err)
	}
	n, err := strconv.Atoi(string(data))
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

// versionsWritten counts how many NNNNN-<uuid>.metadata.json share each version number: more
// than one means two writers committed the same version, a fork.
func (r *warehouseRig) forkedVersions() []string {
	r.t.Helper()
	entries, err := os.ReadDir(r.metaDir())
	if err != nil {
		r.t.Fatal(err)
	}
	seen := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if v, _, ok := strings.Cut(name, "-"); ok && strings.HasSuffix(name, ".metadata.json") {
			seen[v]++
		}
	}
	var forked []string
	for v, n := range seen {
		if n > 1 {
			forked = append(forked, v)
		}
	}
	return forked
}

// live opens the table through exp and returns its live file count, which reads every manifest
// the current metadata references: a manifest deleted from under it fails here.
func (r *warehouseRig) live(exp *Exporter) int {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := exp.catalog.LoadTable(ctx, exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	files, err := exp.tableDataFiles(ctx, tbl)
	if err != nil {
		r.t.Fatalf("current metadata references a manifest that is gone: %v", err)
	}
	return len(files)
}

// A node whose catalog row is missing (a new node, a lost SQLite) continues the published table
// instead of creating a second lineage in the same directory.
func TestAdopt_MissingRowContinuesTheTable(t *testing.T) {
	r := newWarehouseRig(t)
	a := r.node(false)
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	before := r.hint()

	b := r.node(false) // empty catalog, same warehouse
	r.addFile()
	if err := r.reconcile(b); err != nil {
		t.Fatal(err)
	}
	if got := r.hint(); got <= before {
		t.Fatalf("hint %d after the new node's pass, want past %d", got, before)
	}
	if f := r.forkedVersions(); len(f) != 0 {
		t.Fatalf("versions %v were written twice: the new node forked the table", f)
	}
	if n := r.live(b); n != 3 {
		t.Fatalf("live files = %d, want 3", n)
	}
}

// Failover then fail-back in shared-storage mode: the node coming back with a stale row adopts
// what the other node published, so versions and the hint only move forward and every manifest
// the newest metadata references is still there.
func TestAdopt_FailBackDoesNotFork(t *testing.T) {
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(true)
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	hints := []int{r.hint()}
	for i := 0; i < 3; i++ { // the lease moves to b
		r.addFile()
		if err := r.reconcile(b); err != nil {
			t.Fatal(err)
		}
		hints = append(hints, r.hint())
	}
	for i := 0; i < 2; i++ { // and back to a, whose row is still at its own last version
		r.addFile()
		if err := r.reconcile(a); err != nil {
			t.Fatal(err)
		}
		hints = append(hints, r.hint())
	}
	for i := 1; i < len(hints); i++ {
		if hints[i] <= hints[i-1] {
			t.Fatalf("version-hint went %v: not strictly increasing", hints)
		}
	}
	if f := r.forkedVersions(); len(f) != 0 {
		t.Fatalf("versions %v were written twice: fail-back forked the table", f)
	}
	if n := r.live(a); n != 6 {
		t.Fatalf("live files = %d, want 6", n)
	}
	// b's row is still where b left it until its next pass adopts a's versions.
	if err := r.reconcile(b); err != nil {
		t.Fatal(err)
	}
	if n := r.live(b); n != 6 {
		t.Fatalf("live files through the other node after its next pass = %d, want 6", n)
	}
	if f := r.forkedVersions(); len(f) != 0 {
		t.Fatalf("versions %v were written twice after the other node's pass", f)
	}
}

// The fence: a node that commits from a stale row (it skipped adoption) cannot publish over the
// version another node published; its pass fails, and with adoption on the next pass converges.
func TestAdopt_FenceStopsAStaleWriter(t *testing.T) {
	r := newWarehouseRig(t)
	a, b := r.node(true), r.node(false) // b does not adopt while its row exists
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	r.addFile()
	if err := r.reconcile(b); err != nil { // b has no row: it adopts a's table
		t.Fatal(err)
	}
	r.addFile()
	if err := r.reconcile(a); err != nil { // a adopts b's version and commits the next one
		t.Fatal(err)
	}
	published := r.hint()

	r.addFile()
	err := r.reconcile(b) // b's row is one behind: it commits that same version number
	if !errors.Is(err, errFenced) {
		t.Fatalf("stale writer's pass: err = %v, want the fence", err)
	}
	if got := r.hint(); got != published {
		t.Fatalf("hint = %d after the fenced pass, want it left at %d", got, published)
	}
	vBody, err := os.ReadFile(filepath.Join(r.metaDir(), "v"+strconv.Itoa(published)+".metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if n := r.live(a); n != 3 {
		t.Fatalf("the published table lost files: live = %d, want 3", n)
	}

	b.SetWarehouseAuthoritative(true)
	if err := r.reconcile(b); err != nil {
		t.Fatalf("with adoption on, the next pass: %v", err)
	}
	if got := r.hint(); got <= published {
		t.Fatalf("hint = %d, want past %d", got, published)
	}
	after, err := os.ReadFile(filepath.Join(r.metaDir(), "v"+strconv.Itoa(published)+".metadata.json"))
	if err != nil || string(after) != string(vBody) {
		t.Fatalf("published v%d was overwritten (err %v)", published, err)
	}
	if n := r.live(b); n != 4 {
		t.Fatalf("live files = %d, want 4", n)
	}
}

// A hint naming a version whose metadata is unreadable fails the pass rather than guess.
func TestAdopt_UnreadablePublishedVersionFailsThePass(t *testing.T) {
	r := newWarehouseRig(t)
	a := r.node(false)
	r.addFile()
	if err := r.reconcile(a); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.metaDir(), "version-hint.text"), []byte("999"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := r.node(false)
	if err := r.reconcile(b); err == nil || !strings.Contains(err.Error(), path.Join("cpu", "metadata", "v999.metadata.json")) {
		t.Fatalf("err = %v, want the unreadable published version named", err)
	}
}
