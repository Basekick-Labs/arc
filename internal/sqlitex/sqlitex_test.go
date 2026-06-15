package sqlitex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// assertPerm fails unless the file at p exists with exactly 0600. Missing files
// are tolerated for WAL/SHM (journal state dependent) only when allowMissing.
func assertPerm(t *testing.T, p string, allowMissing bool) {
	t.Helper()
	info, err := os.Lstat(p)
	if os.IsNotExist(err) {
		if allowMissing {
			return
		}
		t.Fatalf("%s does not exist", p)
	}
	if err != nil {
		t.Fatalf("lstat %s: %v", p, err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("%s perm = %o, want 0600", filepath.Base(p), perm)
	}
}

// TestOpen_CreatesDirAndLocksPerms verifies the core M4 hardening: Open creates
// a not-yet-existing parent directory and locks the DB file to 0600.
func TestOpen_CreatesDirAndLocksPerms(t *testing.T) {
	base := t.TempDir()
	dbPath := filepath.Join(base, "sub", "data.db") // "sub" must be created by Open

	db, err := Open(dbPath, "_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Open into non-existent dir failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	assertPerm(t, dbPath, false)

	// Parent dir must be 0700.
	dirInfo, err := os.Stat(filepath.Dir(dbPath))
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("dir perm = %o, want 0700", perm)
	}
}

// TestOpen_NoParams verifies the bare-path DSN form (no "?" suffix) used by the
// CQ/retention/audit/tiering call sites.
func TestOpen_NoParams(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "data.db")
	db, err := Open(dbPath, "")
	if err != nil {
		t.Fatalf("Open with no params failed: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	assertPerm(t, dbPath, false)
}

// TestHardenWALSHM_LocksSidecars verifies the WAL/SHM siblings are locked to
// 0600 after a write forces SQLite to create them.
func TestHardenWALSHM_LocksSidecars(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "data.db")
	db, err := Open(dbPath, "_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Force a write so -wal/-shm exist.
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := HardenWALSHM(dbPath, zerolog.Nop()); err != nil {
		t.Fatalf("HardenWALSHM: %v", err)
	}

	assertPerm(t, dbPath, false)
	assertPerm(t, dbPath+"-wal", true)
	assertPerm(t, dbPath+"-shm", true)
}

// TestOpen_Idempotent verifies that opening the same path twice (as happens when
// subsystems share ./data/arc.db) is a harmless no-op that keeps 0600.
func TestOpen_Idempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.db")

	db1, err := Open(dbPath, "")
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	db1.Close()

	// Loosen perms to simulate a pre-existing umask-created file from an older
	// deployment; the second Open must re-tighten.
	if err := os.Chmod(dbPath, 0644); err != nil {
		t.Fatalf("chmod loosen: %v", err)
	}

	db2, err := Open(dbPath, "")
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	t.Cleanup(func() { db2.Close() })

	assertPerm(t, dbPath, false)
}

// TestHardenWALSHM_Symlink verifies that when dbPath is a symlink, the -wal/-shm
// files beside the symlink's REAL target are locked (EvalSymlinks resolution).
func TestHardenWALSHM_Symlink(t *testing.T) {
	base := t.TempDir()
	realDir := filepath.Join(base, "real")
	linkDir := filepath.Join(base, "link")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("symlink creation not supported or permitted: %v", err)
	}

	linkPath := filepath.Join(linkDir, "data.db")
	db, err := Open(linkPath, "_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("Open via symlink: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);`); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := HardenWALSHM(linkPath, zerolog.Nop()); err != nil {
		t.Fatalf("HardenWALSHM via symlink: %v", err)
	}

	realDB := filepath.Join(realDir, "data.db")
	assertPerm(t, realDB, false)
	assertPerm(t, realDB+"-wal", true)
	assertPerm(t, realDB+"-shm", true)
}

// TestOpen_InMemorySkipsFileOps verifies the in-memory forms used by tests do no
// filesystem work and don't error.
func TestOpen_InMemorySkipsFileOps(t *testing.T) {
	for _, p := range []string{":memory:", "file::memory:?cache=shared"} {
		db, err := Open(p, "")
		if err != nil {
			t.Fatalf("Open(%q): %v", p, err)
		}
		if err := db.Ping(); err != nil {
			t.Errorf("ping(%q): %v", p, err)
		}
		db.Close()
		// HardenWALSHM must be a no-op (no panic, no error) for in-memory.
		if err := HardenWALSHM(p, zerolog.Nop()); err != nil {
			t.Errorf("HardenWALSHM(%q): %v", p, err)
		}
	}
}
