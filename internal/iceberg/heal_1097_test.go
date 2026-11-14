package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
	if got[len(got)-1] < start+2 {
		t.Fatalf("v<N> copies %v: expected two heal publishes after v%d", got, start)
	}
	if len(got) != 3 {
		t.Fatalf("v<N> copies %v: want the two newest plus the pass-start v%d", got, start)
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
	before := r.vCopies()
	hintBefore := r.hint()
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: files,
		canCommit: func() bool { return false },
	})
	if err != nil || res.settled {
		t.Fatalf("lost-writer pass: %+v %v; want unsettled, no error", res, err)
	}
	if got := r.vCopies(); len(got) != len(before) || len(got) <= 2 {
		t.Fatalf("v<N> copies %v (before %v): a node that lost the writer role published or pruned", got, before)
	}
	if h := r.hint(); h != hintBefore {
		t.Fatalf("version-hint moved %d -> %d on a node that lost the writer role", hintBefore, h)
	}
}

// hint returns the version version-hint.text names.
func (r *rig1686) hint() int {
	r.t.Helper()
	b, err := os.ReadFile(filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata", "version-hint.text"))
	if err != nil {
		r.t.Fatal(err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

// A hint that lags the table (an earlier pass committed, then failed its hint write): both the
// version the hint named and the table's version at pass start are kept, even when the pass
// publishes twice (both heals) so neither is in the retain+1 cushion any more.
func TestHeal1097_LaggingHintAndLoadedVersionsAreKept(t *testing.T) {
	r := newHealRig(t, 1)
	files, sc := r.history(3)
	top := r.commitDirect(iceberg.Properties{
		icetable.DefaultNameMappingKey:          "[]",
		icetable.MetadataPreviousVersionsMaxKey: "5",
	})
	lag := top - 1
	if !slices.Contains(r.vCopies(), lag) {
		t.Fatalf("setup: v%d not on disk (%v)", lag, r.vCopies())
	}
	hintPath := filepath.Join(r.root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	if err := os.WriteFile(hintPath, []byte(strconv.Itoa(lag)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files); err != nil {
		t.Fatal(err)
	}
	got := r.vCopies()
	if got[len(got)-1] < top+2 {
		t.Fatalf("v<N> copies %v: expected two heal publishes after v%d", got, top)
	}
	for _, v := range []int{lag, top} {
		if !slices.Contains(got, v) {
			t.Fatalf("v<N> copies %v: v%d (hint at pass start v%d, table v%d) was pruned", got, v, lag, top)
		}
	}
}

// A zero-commit pass that is not converged (every new file vanished) on a node that lost the
// writer role publishes and prunes nothing.
func TestHeal1097_LostWriterZeroCommitPassDoesNotPrune(t *testing.T) {
	r := newHealRig(t, 10)
	files, sc := r.history(4)
	r.exp.retain = 1
	gone := r.write(0, "gone")
	removeFile1097(t, gone)
	before, hintBefore := r.vCopies(), r.hint()
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: append(files, gone),
		unread: []FileRef{gone}, readSchemas: readLocal, exists: existsLocal,
		canCommit: func() bool { return false },
	})
	if err != nil || res.commits != 0 {
		t.Fatalf("result %+v err %v; want no commit, no error", res, err)
	}
	if got := r.vCopies(); len(got) != len(before) {
		t.Fatalf("v<N> copies %v (before %v): a node that lost the writer role pruned or published", got, before)
	}
	if h := r.hint(); h != hintBefore {
		t.Fatalf("version-hint moved %d -> %d on a node that lost the writer role", hintBefore, h)
	}
}

func removeFile1097(t *testing.T, f FileRef) {
	t.Helper()
	if err := os.Remove(f.LocalPath); err != nil {
		t.Fatal(err)
	}
}

// The name-mapping heal is gated on the writer role too.
func TestHeal1097_LostWriterSkipsTheNameMappingHeal(t *testing.T) {
	r := newHealRig(t, 3)
	files, sc := r.history(2)
	r.commitDirect(iceberg.Properties{icetable.DefaultNameMappingKey: "[]"})
	before, hintBefore := r.vCopies(), r.hint()
	if _, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: files,
		canCommit: func() bool { return false },
	}); err != nil {
		t.Fatal(err)
	}
	if got, h := r.vCopies(), r.hint(); len(got) != len(before) || h != hintBefore {
		t.Fatalf("copies %v hint %d (before %v, %d): a node that lost the writer role healed the name mapping", got, h, before, hintBefore)
	}
}

// Every version this pass wrote to version-hint.text is kept by its prune: a reader may have
// resolved any of them (two heals publish before the data commits and the final publish).
func TestHeal1097_VersionsHintedDuringThePassAreKept(t *testing.T) {
	r := newHealRig(t, 1)
	r.exp.chunkMax = 1
	files, sc := r.history(3)
	start := r.commitDirect(iceberg.Properties{
		icetable.DefaultNameMappingKey:          "[]",
		icetable.MetadataPreviousVersionsMaxKey: "5",
	})
	n1, n2 := r.write(1, "n1"), r.write(1, "n2")
	files = append(files, n1, n2)
	if _, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: files,
		unread: []FileRef{n1, n2}, readSchemas: readLocal,
	}); err != nil {
		t.Fatal(err)
	}
	got := r.vCopies()
	for _, v := range []int{start + 1, start + 2} { // the two heals' hints
		if !slices.Contains(got, v) {
			t.Fatalf("v<N> copies %v: v%d, which version-hint.text named during the pass, was pruned", got, v)
		}
	}
}

// failHintBackend fails every version-hint.text write.
type failHintBackend struct{ storage.Backend }

func (f failHintBackend) Write(ctx context.Context, p string, data []byte) error {
	if strings.HasSuffix(p, "version-hint.text") {
		return errors.New("disk full")
	}
	return f.Backend.Write(ctx, p, data)
}

// A pass whose publish failed prunes nothing: the hint may still name an old version.
func TestHeal1097_NoPruneAfterAFailedPublish(t *testing.T) {
	r := newHealRig(t, 10)
	files, sc := r.history(4)
	before := r.vCopies()
	r.exp.retain = 1
	r.exp.backend = failHintBackend{r.exp.backend}
	settled, err := r.exp.ReconcileMeasurementWithHint(context.Background(), "mydb", "cpu", sc, files)
	if err != nil || settled {
		t.Fatalf("settled %v err %v; want unsettled", settled, err)
	}
	for _, v := range before {
		if !slices.Contains(r.vCopies(), v) {
			t.Fatalf("v<N> copies %v (before %v): pruned after a failed publish", r.vCopies(), before)
		}
	}
}

// A heal that fails on a pass that also commits data still leaves the pass unsettled (the
// second settled site, at the end of reconcile).
func TestHeal1098_FailedHealOnACommittingPassIsUnsettled(t *testing.T) {
	r := newHealRig(t, 3)
	files, sc := r.history(2)
	r.commitDirect(iceberg.Properties{icetable.DefaultNameMappingKey: "[]"})
	r.exp.propsCommit = failingCommit
	files = append(files, r.write(0, "new"))
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{
		database: "mydb", measurement: "cpu", schema: sc, current: files,
	})
	if err != nil || res.settled || len(r.live()) != len(files) {
		t.Fatalf("result %+v err %v live %d; want the data committed and the pass unsettled", res, err, len(r.live()))
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
