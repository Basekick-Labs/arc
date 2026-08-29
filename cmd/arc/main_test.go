package main

import (
	"os"
	"path/filepath"
	"testing"
)

func assertSQLitePermissions(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		info, err := os.Stat(p)
		if os.IsNotExist(err) {
			t.Fatalf("%s does not exist", filepath.Base(p))
		}
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("%s perm = %o, want 0600", filepath.Base(p), perm)
		}
	}
}

func TestSharedSQLiteHandle_LocksDedicatedDBAndSidecars(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sub", "metadata.db")
	db, owned, err := sharedSQLiteHandle(nil, dbPath)
	if err != nil {
		t.Fatalf("sharedSQLiteHandle: %v", err)
	}
	if !owned {
		t.Fatal("dedicated handle reports owned=false")
	}
	t.Cleanup(func() { db.Close() })

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin sidecar materialization transaction: %v", err)
	}
	if _, err := tx.Exec(`CREATE TABLE sidecar_permission_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("create sidecar materialization table: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO sidecar_permission_probe (value) VALUES ('probe')`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("write sidecar materialization row: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit sidecar materialization transaction: %v", err)
	}

	assertSQLitePermissions(t, dbPath, dbPath+"-wal", dbPath+"-shm")
}

func TestSharedSQLiteHandle_SymlinkedDBLocksRealSidecars(t *testing.T) {
	base := t.TempDir()
	realDir := filepath.Join(base, "real")
	linkDir := filepath.Join(base, "link")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlink creation not supported or permitted: %v", err)
	}

	realDB := filepath.Join(realDir, "metadata.db")
	for _, p := range []string{realDB, realDB + "-wal", realDB + "-shm"} {
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatalf("create %s: %v", p, err)
		}
	}

	db, owned, err := sharedSQLiteHandle(nil, filepath.Join(linkDir, "metadata.db"))
	if err != nil {
		t.Fatalf("sharedSQLiteHandle via symlink: %v", err)
	}
	if !owned {
		t.Fatal("dedicated handle reports owned=false")
	}
	t.Cleanup(func() { db.Close() })

	assertSQLitePermissions(t, realDB, realDB+"-wal", realDB+"-shm")
}
