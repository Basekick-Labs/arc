package reconciliation

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestIntegration_RealLocalBackend runs the full reconciler against a
// real LocalBackend on a temp dir so the storage walk + ObjectLister +
// Delete paths exercise actual filesystem semantics. The fake
// coordinator stays in (unit-test scope) — the cluster-level
// integration test belongs in internal/cluster where the existing
// two-node helpers live, and is intentionally out of scope for this
// package's tests.
func TestIntegration_RealLocalBackend(t *testing.T) {
	tmp := t.TempDir()

	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()

	// Plant the layout:
	//   db/m/2026/04/27/12/in-manifest.parquet  — manifest + storage
	//   db/m/2026/04/27/12/orphan-manifest.parquet — manifest only
	//   db/m/2026/04/27/12/orphan-storage.parquet — storage only, OLD
	//   db/m/2026/04/27/12/young-orphan.parquet — storage only, YOUNG
	now := time.Now().UTC()
	mustPut := func(rel string, data []byte) {
		if err := backend.Write(ctx, rel, data); err != nil {
			t.Fatalf("backend.Write(%q): %v", rel, err)
		}
	}
	setMtime := func(rel string, mtime time.Time) {
		full := filepath.Join(tmp, rel)
		if err := os.Chtimes(full, mtime, mtime); err != nil {
			t.Fatalf("os.Chtimes(%q): %v", full, err)
		}
	}

	mustPut("db/m/2026/04/27/12/in-manifest.parquet", []byte("a"))
	mustPut("db/m/2026/04/27/12/orphan-storage.parquet", []byte("b"))
	mustPut("db/m/2026/04/27/12/young-orphan.parquet", []byte("c"))
	// Push orphan-storage's mtime back well past the grace window.
	setMtime("db/m/2026/04/27/12/orphan-storage.parquet", now.Add(-48*time.Hour))
	// young-orphan stays at "now" (mtime when written by Write).

	coord := newFakeCoordinator(
		fileEntry("db/m/2026/04/27/12/in-manifest.parquet", "node-a"),
		fileEntry("db/m/2026/04/27/12/orphan-manifest.parquet", "node-a"),
	)

	r, err := NewReconciler(
		Config{
			Enabled:                  true,
			BackendKind:              BackendShared,
			GraceWindow:              1 * time.Hour, // tighter than default to test boundary
			ClockSkewAllowance:       1 * time.Minute,
			DeletePreManifestOrphans: true,
		},
		coord, backend, &fakeGate{scan: true, sweep: true}, nil, zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	run, err := r.Reconcile(ctx, false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Expectations:
	//   - 1 orphan-manifest delete (orphan-manifest.parquet removed from coord)
	//   - 1 orphan-storage delete (orphan-storage.parquet removed from disk)
	//   - 1 grace skip (young-orphan.parquet still on disk)
	if run.ManifestDeletes != 1 {
		t.Errorf("expected 1 manifest delete, got %d", run.ManifestDeletes)
	}
	if run.StorageDeletes != 1 {
		t.Errorf("expected 1 storage delete, got %d", run.StorageDeletes)
	}
	if run.SkippedGrace != 1 {
		t.Errorf("expected 1 grace skip, got %d", run.SkippedGrace)
	}

	// Coordinator state: orphan-manifest gone, in-manifest preserved.
	if _, ok := coord.manifest["db/m/2026/04/27/12/orphan-manifest.parquet"]; ok {
		t.Error("orphan-manifest.parquet should be gone from manifest")
	}
	if _, ok := coord.manifest["db/m/2026/04/27/12/in-manifest.parquet"]; !ok {
		t.Error("in-manifest.parquet should be preserved")
	}

	// Disk state: orphan-storage gone, in-manifest + young-orphan preserved.
	for path, wantExists := range map[string]bool{
		"db/m/2026/04/27/12/in-manifest.parquet":     true,
		"db/m/2026/04/27/12/orphan-storage.parquet":  false,
		"db/m/2026/04/27/12/young-orphan.parquet":    true,
		"db/m/2026/04/27/12/orphan-manifest.parquet": false, // never existed on disk
	} {
		exists, err := backend.Exists(ctx, path)
		if err != nil {
			t.Errorf("Exists(%q): %v", path, err)
			continue
		}
		if exists != wantExists {
			t.Errorf("Exists(%q) = %v, want %v", path, exists, wantExists)
		}
	}

	// A second reconcile on the now-clean state should be a no-op.
	run2, err := r.Reconcile(ctx, false)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if run2.ManifestDeletes != 0 || run2.StorageDeletes != 0 {
		t.Errorf("second run on clean state should be no-op, got: %+v", run2)
	}
	if run2.SkippedGrace != 1 {
		t.Errorf("young-orphan should still be in grace, got SkippedGrace=%d", run2.SkippedGrace)
	}
}

// TestIntegration_LocalBackendDirectoryListerFallback exercises the
// root-walk path: a file in storage that the manifest-derived prefixes
// don't enumerate. The reconciler must find it via DirectoryLister and
// flag it as orphan-storage.
func TestIntegration_LocalBackendDirectoryListerFallback(t *testing.T) {
	tmp := t.TempDir()
	backend, err := storage.NewLocalBackend(tmp, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	defer backend.Close()
	ctx := context.Background()

	// Manifest is empty. File lives in a database the manifest doesn't know.
	if err := backend.Write(ctx, "unknowndb/m/old.parquet", []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	full := filepath.Join(tmp, "unknowndb/m/old.parquet")
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(full, old, old); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	r, err := NewReconciler(
		Config{
			Enabled:                  true,
			BackendKind:              BackendShared,
			GraceWindow:              1 * time.Hour,
			DeletePreManifestOrphans: true,
			MaxRootWalkDatabases:     -1, // enable root walk with default cap
		},
		newFakeCoordinator(), backend, &fakeGate{scan: true, sweep: true}, nil, zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	run, err := r.Reconcile(ctx, false)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if run.StorageDeletes != 1 {
		t.Errorf("DirectoryLister fallback should have found the orphan, got StorageDeletes=%d", run.StorageDeletes)
	}
	exists, _ := backend.Exists(ctx, "unknowndb/m/old.parquet")
	if exists {
		t.Error("orphan should have been deleted")
	}
}

// Ensure the raft package import is referenced even when the tests
// don't directly use raft types — the fake coordinator constructor
// is sufficient but pin the dep here for clarity.
var _ = raft.FileEntry{}
