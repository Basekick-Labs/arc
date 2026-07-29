package main

import (
	"os"
	"path/filepath"
	"testing"
)

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
