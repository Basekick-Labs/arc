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

// With auth disabled there is no handle to borrow, so sharedSQLiteHandle opens
// one itself. Nothing else in that configuration creates the parent directory
// or tightens the file mode, so this helper has to do both — auth.NewAuthManager
// does exactly this, and it is the code path that does not run here.
func TestSharedSQLiteHandle_CreatesParentDirectory(t *testing.T) {
	// Nested path that does not exist yet — a fresh install.
	dbPath := filepath.Join(t.TempDir(), "nested", "data", "arc.db")

	db, owned, err := sharedSQLiteHandle(nil, dbPath)
	if err != nil {
		t.Fatalf("sharedSQLiteHandle: %v", err)
	}
	defer db.Close()

	if !owned {
		t.Error("handle opened without an auth manager must be owned by the caller")
	}

	// sql.Open is lazy: it validates the DSN without touching the filesystem,
	// so a missing directory only surfaces on first use. Verify the handle is
	// actually usable rather than merely constructed.
	if err := db.Ping(); err != nil {
		t.Fatalf("handle is not usable: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("cannot write to the database: %v", err)
	}
}

// The shared file holds audit logs and tiering metadata. SQLite creates it with
// the process umask (typically 0644) on first write, and the chmod that would
// normally tighten it lives inside the auth-enabled branch.
func TestSharedSQLiteHandle_FileIsNotWorldReadable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "arc.db")

	db, _, err := sharedSQLiteHandle(nil, dbPath)
	if err != nil {
		t.Fatalf("sharedSQLiteHandle: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("write: %v", err)
	}

	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("database file mode = %v, want owner-only (0600); it holds audit logs", mode)
	}
}

// A path that cannot be opened must fail here, not leave a broken handle for
// the caller to discover on its first query — where the error reads as
// "feature disabled" rather than a startup failure.
func TestSharedSQLiteHandle_ReportsUnusablePath(t *testing.T) {
	// A directory where a file is expected: MkdirAll succeeds, the open fails.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	if err := os.MkdirAll(dbPath, 0o700); err != nil {
		t.Fatalf("setup: %v", err)
	}

	db, _, err := sharedSQLiteHandle(nil, dbPath)
	if err == nil {
		if db != nil {
			db.Close()
		}
		t.Fatal("expected an error for a path that cannot be opened as a database")
	}
}
