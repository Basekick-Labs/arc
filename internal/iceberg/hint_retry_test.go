package iceberg

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// newHintTestExporter builds an exporter over a real LocalBackend rooted at dir.
//
// The backend must be a *storage.LocalBackend rather than a wrapper:
// DefaultWarehouse type-switches on the concrete backend type, and an embedding
// wrapper falls through to the "./data" default, which makes warehouseRelKey
// reject every metadata path. Failure is therefore injected by making the
// metadata directory unwritable, not by wrapping Write.
func newHintTestExporter(t *testing.T, dir string, retain int) *Exporter {
	t.Helper()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dir, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	exp, err := NewExporter(db, backend, "file://"+dir, "arc", retain, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return exp
}

// A failed discovery-file write must be reported to the caller.
//
// The snapshot commits either way, but the scheduler caches a per-measurement
// fingerprint and skips measurements whose file set is unchanged. Treating a
// failed hint write as success would gate the measurement out of every later
// pass, so a file set that then goes quiet — the steady state — would leave
// version-hint.text stale indefinitely, and directory-based readers (DuckDB
// iceberg_scan, Spark hadoop-format) would resolve the wrong version forever.
func TestReconcile_HintFailureIsReported(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: an unwritable directory would still be writable")
	}
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not block writes the same way on Windows")
	}

	ctx := context.Background()
	root := t.TempDir()
	exp := newHintTestExporter(t, root, 3)

	f := filepath.Join(root, "mydb", "cpu", "2023", "11", "14", "22", "a.parquet")
	writeArcStyleParquet(t, f, 1_700_000_000_000_000, 5)
	sc, err := SchemaFromParquet(f)
	if err != nil {
		t.Fatal(err)
	}
	files := []FileRef{{PhysicalPath: fileURI(f)}}

	// First pass creates the table and publishes normally.
	hintOK, err := exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", sc, files)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if !hintOK {
		t.Fatal("a healthy first pass should publish the discovery files")
	}

	metaDir := filepath.Join(root, "arc_mydb.db", "cpu", "metadata")
	if _, err := os.Stat(filepath.Join(metaDir, "version-hint.text")); err != nil {
		t.Fatalf("first pass should have published the hint: %v", err)
	}

	// Make the metadata directory unwritable, then run a converged reconcile so
	// only the discovery-file writes are attempted (no catalog commit).
	info, err := os.Stat(metaDir)
	if err != nil {
		t.Fatal(err)
	}
	origMode := info.Mode().Perm()
	if err := os.Chmod(metaDir, 0o500); err != nil { // r-x: no create, no overwrite
		t.Fatal(err)
	}
	defer os.Chmod(metaDir, origMode)

	hintOK, err = exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", sc, files)
	if err != nil {
		t.Fatalf("converged reconcile should not fail: %v", err)
	}
	if hintOK {
		t.Fatal("expected hintOK=false when the discovery-file write fails")
	}

	// Storage recovers: the same converged pass must republish and report success.
	if err := os.Chmod(metaDir, origMode); err != nil {
		t.Fatal(err)
	}
	hintOK, err = exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", sc, files)
	if err != nil {
		t.Fatalf("reconcile after recovery: %v", err)
	}
	if !hintOK {
		t.Fatal("a converged reconcile must republish the discovery files after an earlier failure")
	}
}

// A converged reconcile (no file-set change, no new snapshot) must still
// republish the discovery files. Without this, a hint that failed to publish is
// never rewritten once the measurement goes quiet — the reconciler's
// "self-heals next tick" property would not hold for these files.
func TestReconcile_ConvergedPassRepublishesHint(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	exp := newHintTestExporter(t, root, 3)

	f := filepath.Join(root, "mydb", "cpu", "2023", "11", "14", "22", "a.parquet")
	writeArcStyleParquet(t, f, 1_700_000_000_000_000, 5)
	sc, err := SchemaFromParquet(f)
	if err != nil {
		t.Fatal(err)
	}
	files := []FileRef{{PhysicalPath: fileURI(f)}}

	if _, err := exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", sc, files); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}

	// Delete the hint to stand in for a publish that never landed, then reconcile
	// an unchanged file set — the converged path.
	hintPath := filepath.Join(root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	if err := os.Remove(hintPath); err != nil {
		t.Fatalf("remove hint: %v", err)
	}

	hintOK, err := exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", sc, files)
	if err != nil {
		t.Fatalf("converged reconcile: %v", err)
	}
	if !hintOK {
		t.Error("converged reconcile reported the hint as unpublished")
	}
	if _, err := os.Stat(hintPath); err != nil {
		t.Fatalf("converged reconcile did not republish version-hint.text: %v", err)
	}
}

// A metadata copy failure must not advance version-hint.text. Directory-based readers fetch the
// hint first and then v<N>.metadata.json, so advancing the hint before that copy succeeds would
// point them at a file that does not exist yet (or at stale data after a failed read).
func TestWriteVersionHint_MetadataFailurePreservesOldHint(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: unreadable metadata would still be readable")
	}
	if runtime.GOOS == "windows" {
		t.Skip("file permissions do not block reads the same way on Windows")
	}

	ctx := context.Background()
	root := t.TempDir()
	exp := newHintTestExporter(t, root, 3)

	f1 := filepath.Join(root, "mydb", "cpu", "2023", "11", "14", "22", "a.parquet")
	f2 := filepath.Join(root, "mydb", "cpu", "2023", "11", "14", "22", "b.parquet")
	writeArcStyleParquet(t, f1, 1_700_000_000_000_000, 5)
	writeArcStyleParquet(t, f2, 1_700_000_100_000_000, 5)
	sc, err := SchemaFromParquet(f1)
	if err != nil {
		t.Fatal(err)
	}

	if hintOK, err := exp.ReconcileMeasurementWithHint(ctx, "mydb", "cpu", sc, []FileRef{{PhysicalPath: fileURI(f1)}}); err != nil || !hintOK {
		t.Fatalf("first reconcile: hintOK=%v err=%v", hintOK, err)
	}
	hintPath := filepath.Join(root, "arc_mydb.db", "cpu", "metadata", "version-hint.text")
	oldHint, err := os.ReadFile(hintPath)
	if err != nil {
		t.Fatalf("read old hint: %v", err)
	}

	tbl, err := exp.EnsureTable(ctx, "mydb", "cpu", sc)
	if err != nil {
		t.Fatalf("load current table: %v", err)
	}
	txn := tbl.NewTransaction()
	if err := txn.ReplaceDataFiles(ctx, []string{fileURI(f1)}, []string{fileURI(f2)}, nil); err != nil {
		t.Fatalf("prepare newer metadata version: %v", err)
	}
	tbl, err = txn.Commit(ctx)
	if err != nil {
		t.Fatalf("commit newer metadata version: %v", err)
	}
	newVersion, _, ok := exp.parseVersionAndMetaDir(tbl.MetadataLocation())
	if !ok {
		t.Fatalf("parse current metadata location %q", tbl.MetadataLocation())
	}
	if bytes.Equal(oldHint, []byte(newVersion)) {
		t.Fatalf("reconcile did not create a newer metadata version: old hint=%q", oldHint)
	}
	if err := os.WriteFile(hintPath, oldHint, 0o600); err != nil {
		t.Fatalf("restore old hint: %v", err)
	}

	metaPath := strings.TrimPrefix(tbl.MetadataLocation(), "file://")
	metaInfo, err := os.Stat(metaPath)
	if err != nil {
		t.Fatalf("stat current metadata: %v", err)
	}
	origMode := metaInfo.Mode().Perm()
	if err := os.Chmod(metaPath, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(metaPath, origMode)

	if hintOK := exp.writeVersionHint(ctx, tbl); hintOK {
		t.Fatal("metadata read failure should not publish discovery files")
	}
	gotHint, err := os.ReadFile(hintPath)
	if err != nil {
		t.Fatalf("read hint after metadata failure: %v", err)
	}
	if !bytes.Equal(gotHint, oldHint) {
		t.Fatalf("hint changed after metadata failure: got %q, want %q", gotHint, oldHint)
	}

	if err := os.Chmod(metaPath, origMode); err != nil {
		t.Fatal(err)
	}
	if hintOK := exp.writeVersionHint(ctx, tbl); !hintOK {
		t.Fatal("discovery files should publish after metadata recovery")
	}
	gotHint, err = os.ReadFile(hintPath)
	if err != nil {
		t.Fatalf("read recovered hint: %v", err)
	}
	if !bytes.Equal(gotHint, []byte(newVersion)) {
		t.Fatalf("recovered hint = %q, want %q", gotHint, newVersion)
	}
}

// The scheduler must not cache a measurement's fingerprint when the discovery
// files failed to publish, so the next tick reconciles again rather than
// skipping on an unchanged file set.
func TestScheduler_DoesNotCacheAfterHintFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: an unwritable directory would still be writable")
	}
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not block writes the same way on Windows")
	}

	ctx := context.Background()
	root := t.TempDir()
	exp := newHintTestExporter(t, root, 3)

	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	f := filepath.Join(root, "mydb", "cpu", "2023", "11", "14", "22", "a.parquet")
	writeArcStyleParquet(t, f, 1_700_000_000_000_000, 5)

	s := &Scheduler{
		exporter: exp,
		source:   NewStorageWalkSource(backend, "arc", zerolog.Nop()),
		logger:   zerolog.Nop(),
		state:    make(map[string]measurementState),
	}

	m := Measurement{Database: "mydb", Measurement: "cpu"}
	key := m.Database + "\x00" + m.Measurement

	// Healthy pass caches the fingerprint.
	if _, err := s.reconcileOne(ctx, m, key); err != nil {
		t.Fatalf("reconcileOne: %v", err)
	}
	if _, cached := s.state[key]; !cached {
		t.Fatal("a healthy pass should cache the fingerprint")
	}

	// Break publishing and change the file set so the measurement is revisited.
	metaDir := filepath.Join(root, "arc_mydb.db", "cpu", "metadata")
	info, err := os.Stat(metaDir)
	if err != nil {
		t.Fatal(err)
	}
	origMode := info.Mode().Perm()

	f2 := filepath.Join(root, "mydb", "cpu", "2023", "11", "14", "22", "b.parquet")
	writeArcStyleParquet(t, f2, 1_700_000_100_000_000, 5)

	if err := os.Chmod(metaDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(metaDir, origMode)

	if _, err := s.reconcileOne(ctx, m, key); err != nil {
		// A catalog commit that cannot write metadata fails outright; that is a
		// different (already-fatal, already-retried) path than the one under test.
		t.Skipf("catalog commit could not proceed with an unwritable metadata dir: %v", err)
	}
	if _, cached := s.state[key]; cached {
		t.Fatal("fingerprint must NOT be cached when the discovery files failed to publish — " +
			"the measurement would be skipped forever once its file set goes quiet")
	}
}
