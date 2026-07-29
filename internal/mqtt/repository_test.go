package mqtt

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// A borrowed handle must survive Repository.Close.
//
// Arc keeps auth, audit, tiering and MQTT metadata in one SQLite file. MQTT
// shuts down at PriorityIngest(20) while the auth manager owns the handle
// until PriorityAuth(70), so closing a borrowed DB here would pull it out from
// under every component still shutting down (#329).
func TestRepository_BorrowedDBSurvivesClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.db")
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	repo, err := NewRepository(db, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	if err := repo.Close(); err != nil {
		t.Fatalf("Close on a borrowed handle should be a no-op, got: %v", err)
	}

	// The owner must still be able to use the handle.
	if err := db.Ping(); err != nil {
		t.Fatalf("borrowed DB was closed by Repository.Close: %v", err)
	}
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatalf("borrowed DB unusable after Repository.Close: %v", err)
	}
}

// The owned case must keep its previous behavior: Close really closes.
func TestRepository_OwnedDBIsClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "own.db")

	repo, err := NewSQLiteRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSQLiteRepository: %v", err)
	}

	if err := repo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := repo.db.Ping(); err == nil {
		t.Fatal("owned DB should be closed after Repository.Close")
	}
}

// A nil handle is a programming error and must be rejected rather than
// deferred to a nil-pointer panic on first query.
func TestNewRepository_RejectsNilDB(t *testing.T) {
	if _, err := NewRepository(nil, nil, zerolog.Nop()); err == nil {
		t.Fatal("expected an error for a nil database handle")
	}
}

// Schema init must be safe on a handle that already has the MQTT schema —
// the borrowed-handle case runs it against a database another component may
// have already initialized, and a restart runs it again.
func TestRepository_InitSchemaIsIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.db")
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	for i := 0; i < 3; i++ {
		repo, err := NewRepository(db, nil, zerolog.Nop())
		if err != nil {
			t.Fatalf("NewRepository (iteration %d): %v", i, err)
		}
		if err := repo.Close(); err != nil {
			t.Fatalf("Close (iteration %d): %v", i, err)
		}
	}
}

// Sharing one handle between MQTT and another component must not deadlock or
// error: this is the configuration the fix introduces, where two components
// issue writes through the same single-connection pool.
func TestRepository_SharesHandleWithAnotherWriter(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "shared.db")
	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	// Stand in for another component (auth/audit/tiering) using the same handle.
	if _, err := db.Exec(`CREATE TABLE other_component (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create other table: %v", err)
	}

	repo, err := NewRepository(db, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	defer repo.Close()

	// Interleave writes from both "components" through the shared pool.
	for i := 0; i < 20; i++ {
		if _, err := db.Exec("INSERT INTO other_component (v) VALUES (?)", "x"); err != nil {
			t.Fatalf("other component write %d failed: %v", i, err)
		}
		if _, err := repo.db.Exec(
			`INSERT OR REPLACE INTO mqtt_subscriptions
			 (id, name, broker, client_id, topics, database, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))`,
			"id", "n", "tcp://localhost:1883", "c", "[]", "db",
		); err != nil {
			t.Fatalf("mqtt write %d failed: %v", i, err)
		}
	}

	var n int
	if err := db.QueryRow("SELECT count(*) FROM other_component").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 20 {
		t.Errorf("other component rows = %d, want 20", n)
	}
}

// A fresh install has no data directory yet. sql.Open is lazy, so a missing
// parent directory does not fail at open — it fails at schema init, which
// main.go treats as fatal. This is the MQTT-enabled/auth-disabled path.
func TestNewSQLiteRepository_CreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nested", "data", "arc.db")

	repo, err := NewSQLiteRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSQLiteRepository on a fresh install: %v", err)
	}
	defer repo.Close()

	if _, err := repo.db.Exec("SELECT 1"); err != nil {
		t.Fatalf("database unusable: %v", err)
	}
}

// The MQTT database holds broker credentials in password_encrypted, so it must
// not be world-readable. Nothing else tightens the mode when auth is disabled.
func TestOpenDB_FileIsNotWorldReadable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mqtt.db")

	db, err := OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
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
		t.Errorf("database file mode = %v, want owner-only (0600); it holds broker credentials", mode)
	}
}

// OpenDB must produce a handle constrained to SQLite's single writer, matching
// the auth manager's pool settings.
func TestOpenDB_PoolIsSingleConnection(t *testing.T) {
	db, err := OpenDB(filepath.Join(t.TempDir(), "pool.db"))
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	defer db.Close()

	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1", got)
	}
	var _ *sql.DB = db
}
