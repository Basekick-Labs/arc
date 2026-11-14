package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1097: a heal commit's v<N>.metadata.json copy is pruned on a converged pass, without ever
// deleting the version a reader may have just resolved. #1098: a failed heal leaves the pass
// unsettled, so it is retried by the next pass instead of whenever the file set next changes.

// newHealRig is rig1686 with a storage backend, so the exporter publishes and prunes the
// v<N>.metadata.json copies (a nil backend skips both).
func newHealRig(t *testing.T, retain int) *rig1686 {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := NewExporter(db, backend, "file://"+dir, "arc", retain, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	exp.chunkMax = 16
	return &rig1686{t: t, root: dir, exp: exp}
}

// vCopies returns the version numbers of the v<N>.metadata.json copies on disk, ascending.
func (r *rig1686) vCopies() []int {
	r.t.Helper()
	entries, err := os.ReadDir(filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata"))
	if err != nil {
		r.t.Fatal(err)
	}
	var out []int
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "v") && strings.HasSuffix(n, ".metadata.json") {
			if v, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(n, "v"), ".metadata.json")); err == nil {
				out = append(out, v)
			}
		}
	}
	sort.Ints(out)
	return out
}

// history builds a table with several published versions: one pass per file.
func (r *rig1686) history(n int) ([]FileRef, ArcSchema) {
	r.t.Helper()
	var files []FileRef
	for i := 0; i < n; i++ {
		files = append(files, r.write(0, fmt.Sprintf("h%d", i)))
		if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", r.schema(files[0]), files); err != nil {
			r.t.Fatal(err)
		}
	}
	return files, r.schema(files[0])
}

// The issue's rig: a table built at retain_snapshots=10, then reconciled at 1. The retention
// heal publishes a copy on a pass that commits no data; that pass now prunes down to retain+1.
func TestHeal1097_ConvergedPassPrunesTheHealCopy(t *testing.T) {
	r := newHealRig(t, 10)
	files, sc := r.history(4)
	r.exp.retain = 1
	settled, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if err != nil || !settled {
		t.Fatalf("converged pass: settled %v err %v", settled, err)
	}
	if got := r.vCopies(); len(got) != 2 {
		t.Fatalf("v<N> copies %v after a converged heal pass at retain=1, want retain+1 = 2", got)
	}
}

// commitDirect commits property changes outside the exporter and publishes the hint, as a
// previous pass would have.
func (r *rig1686) commitDirect(props iceberg.Properties) int {
	r.t.Helper()
	ctx := context.Background()
	tbl, err := r.exp.loadTableRecovering(ctx, r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		r.t.Fatal(err)
	}
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(props); err != nil {
		r.t.Fatal(err)
	}
	tbl, err = txn.Commit(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	if !r.exp.writeVersionHint(ctx, tbl) {
		r.t.Fatal("publish")
	}
	return r.exp.metadataVersion(tbl)
}

// A pass that publishes twice before its prune (name-mapping heal, then retention heal) at
// retain=1: the retain+1 cushion alone would delete the version the hint named when the pass
// started. It is kept.
func TestHeal1097_PassStartVersionSurvivesTwoPublishes(t *testing.T) {
	r := newHealRig(t, 1)
	files, sc := r.history(3)
	start := r.commitDirect(iceberg.Properties{
		icetable.DefaultNameMappingKey:          "[]",
		icetable.MetadataPreviousVersionsMaxKey: "5",
	})
	settled, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if err != nil || !settled {
		t.Fatalf("heal pass: settled %v err %v", settled, err)
	}
	got := r.vCopies()
	if len(got) < 3 || got[len(got)-1] < start+2 {
		t.Fatalf("v<N> copies %v: expected two heal publishes after v%d", got, start)
	}
	if got[0] != start {
		t.Fatalf("v<N> copies %v: v%d, which the hint named when the pass started, was pruned", got, start)
	}
}

// A converged pass on a node that lost the writer role between the scheduler's gate and the
// end of the pass publishes and prunes nothing, and does not settle.
func TestHeal1097_LostWriterConvergedPassDoesNotPrune(t *testing.T) {
	r := newHealRig(t, 10)
	files, sc := r.history(4)
	r.exp.retain = 1
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: files,
		canCommit: func() bool { return false },
	})
	if err != nil || res.settled {
		t.Fatalf("result %+v err %v; want unsettled, no error", res, err)
	}
	if got := r.vCopies(); len(got) <= 2 {
		t.Fatalf("v<N> copies %v: a node that lost the writer role pruned", got)
	}
}

var errCatalogBusy = errors.New("database is locked")

func failingCommit(context.Context, *icetable.Transaction) (*icetable.Table, error) {
	return nil, errCatalogBusy
}

// A retention heal that fails leaves the pass unsettled (the scheduler then does not cache the
// measurement); the next pass heals.
func TestHeal1098_FailedRetentionHealIsRetried(t *testing.T) {
	r := newHealRig(t, 10)
	files, sc := r.history(2)
	r.exp.retain = 1
	r.exp.propsCommit = failingCommit
	settled, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if err != nil || settled {
		t.Fatalf("failed heal: settled %v err %v; want unsettled, no error", settled, err)
	}
	r.exp.propsCommit = nil
	settled, err = r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if err != nil || !settled {
		t.Fatalf("retry: settled %v err %v", settled, err)
	}
	tbl, err := r.exp.loadTableRecovering(context.Background(), r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if got := tbl.Properties()[icetable.MetadataPreviousVersionsMaxKey]; got != "1" {
		t.Fatalf("previous-versions-max %q after the retry, want 1", got)
	}
}

// Same for the name-mapping heal.
func TestHeal1098_FailedNameMappingHealIsRetried(t *testing.T) {
	r := newHealRig(t, 3)
	files, sc := r.history(2)
	r.commitDirect(iceberg.Properties{icetable.DefaultNameMappingKey: "[]"})
	r.exp.propsCommit = failingCommit
	settled, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if err != nil || settled {
		t.Fatalf("failed heal: settled %v err %v; want unsettled, no error", settled, err)
	}
	r.exp.propsCommit = nil
	if settled, err = r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files); err != nil || !settled {
		t.Fatalf("retry: settled %v err %v", settled, err)
	}
	tbl, err := r.exp.loadTableRecovering(context.Background(), r.exp.tableIdent("mydb", "cpu"))
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Properties()[icetable.DefaultNameMappingKey] == "[]" {
		t.Fatal("name mapping not healed by the retry")
	}
}
