package backup

// Boot-apply crash matrix for #635. Partial states are constructed directly
// on disk and ApplyPendingRestores must converge from every one of them.

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func makeSQLite(t *testing.T, path string, rows int) []byte {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO t (v) VALUES (?)", i); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func rowCount(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("count rows in %s: %v", path, err)
	}
	return n
}

// Crash between the safety rename and the final rename: no live database,
// pending still staged. The apply MUST resume, not skip — skipping would let
// the server boot a fresh empty auth database and silently lose the restore.
func TestApply_ResumesWhenDestinationMissing(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	staged := makeSQLite(t, StagePath(dbPath), 3)

	ApplyPendingRestores(zerolog.Nop(), dbPath)

	if got := rowCount(t, dbPath); got != 3 {
		t.Fatalf("rows = %d, want the staged restore's 3", got)
	}
	if data, _ := os.ReadFile(dbPath); !bytes.Equal(data, staged) {
		t.Fatal("applied database differs from the staged restore")
	}
	if _, err := os.Stat(StagePath(dbPath)); !os.IsNotExist(err) {
		t.Fatal("pending file survived the apply")
	}
}

func TestApply_QuarantinesGarbagePending(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	current := makeSQLite(t, dbPath, 2)
	if err := os.WriteFile(StagePath(dbPath), []byte("definitely not a database"), 0o600); err != nil {
		t.Fatal(err)
	}

	ApplyPendingRestores(zerolog.Nop(), dbPath)

	if data, _ := os.ReadFile(dbPath); !bytes.Equal(data, current) {
		t.Fatal("garbage pending file modified the current database")
	}
	if _, err := os.Stat(StagePath(dbPath) + ".rejected"); err != nil {
		t.Fatalf("garbage pending not quarantined: %v", err)
	}
	if _, err := os.Stat(StagePath(dbPath)); !os.IsNotExist(err) {
		t.Fatal("garbage pending still staged")
	}
}

func TestApply_QuarantinesCorruptButMagicked(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	makeSQLite(t, dbPath, 2)
	// Valid magic, plausible size, truncated mid-page — the realistic
	// crash-during-stage artifact; quick_check must catch it.
	good := makeSQLite(t, filepath.Join(dir, "donor.db"), 500)
	truncated := good[:len(good)-700]
	if err := os.WriteFile(StagePath(dbPath), truncated, 0o600); err != nil {
		t.Fatal(err)
	}

	ApplyPendingRestores(zerolog.Nop(), dbPath)

	if got := rowCount(t, dbPath); got != 2 {
		t.Fatalf("rows = %d, want the untouched current database's 2", got)
	}
	if _, err := os.Stat(StagePath(dbPath) + ".rejected"); err != nil {
		t.Fatalf("corrupt pending not quarantined: %v", err)
	}
}

func TestApply_RemovesStaleSafetySidecars(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	makeSQLite(t, dbPath, 2)
	makeSQLite(t, StagePath(dbPath), 5)
	// Leftovers from an OLDER restore's safety copy: stale sidecars beside
	// the new safety copy are the mismatched-WAL corruption class.
	for _, stale := range []string{dbPath + ".before-restore-wal", dbPath + ".before-restore-shm"} {
		if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	ApplyPendingRestores(zerolog.Nop(), dbPath)

	if got := rowCount(t, dbPath); got != 5 {
		t.Fatalf("rows = %d, want the staged restore's 5", got)
	}
	for _, stale := range []string{dbPath + ".before-restore-wal", dbPath + ".before-restore-shm"} {
		if _, err := os.Stat(stale); !os.IsNotExist(err) {
			t.Errorf("stale safety sidecar %s survived", stale)
		}
	}
	if got := rowCount(t, dbPath+".before-restore"); got != 2 {
		t.Fatalf("safety copy rows = %d, want the previous database's 2", got)
	}
}

func TestApply_NoPendingIsNoOp(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	current := makeSQLite(t, dbPath, 2)

	ApplyPendingRestores(zerolog.Nop(), dbPath, "", dbPath)

	if data, _ := os.ReadFile(dbPath); !bytes.Equal(data, current) {
		t.Fatal("no-op apply modified the database")
	}
}

// Re-running the apply from each constructible partial state converges.
func TestApply_ConvergesFromPartialStates(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	// State: sidecars already removed (crash after checkpoint+removal),
	// current db and pending both present.
	makeSQLite(t, dbPath, 2)
	makeSQLite(t, StagePath(dbPath), 4)
	ApplyPendingRestores(zerolog.Nop(), dbPath)
	if got := rowCount(t, dbPath); got != 4 {
		t.Fatalf("converge-1 rows = %d, want 4", got)
	}

	// State: previous apply fully done, a SECOND restore staged over it.
	makeSQLite(t, StagePath(dbPath), 6)
	ApplyPendingRestores(zerolog.Nop(), dbPath)
	if got := rowCount(t, dbPath); got != 6 {
		t.Fatalf("converge-2 rows = %d, want 6", got)
	}
	if got := rowCount(t, dbPath+".before-restore"); got != 4 {
		t.Fatalf("safety copy rows = %d, want the first restore's 4 (overwritten in order)", got)
	}
}

// The checkpoint-failure branch: a held read transaction makes
// wal_checkpoint(TRUNCATE) fail busy, so the apply must move the WAL
// sidecars ALONGSIDE the safety copy (keeping it recoverable) instead of
// deleting them, and still apply the staged restore.
func TestApply_CheckpointFailureKeepsSafetyCopyRecoverable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	live, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if _, err := live.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t (v INTEGER); INSERT INTO t (v) VALUES (1), (2), (3)"); err != nil {
		t.Fatal(err)
	}
	// Hold a read transaction so TRUNCATE checkpointing cannot complete.
	tx, err := live.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil || n != 3 {
		t.Fatalf("read txn: (%d, %v)", n, err)
	}

	makeSQLite(t, StagePath(dbPath), 7)
	ApplyPendingRestores(zerolog.Nop(), dbPath)
	tx.Rollback()
	live.Close()

	if got := rowCount(t, dbPath); got != 7 {
		t.Fatalf("applied rows = %d, want the staged restore's 7", got)
	}
	if _, err := os.Stat(dbPath + ".before-restore-wal"); err != nil {
		t.Fatalf("WAL not moved alongside the safety copy on checkpoint failure: %v", err)
	}
	// The safety copy with its moved WAL must recover every live commit.
	if got := rowCount(t, dbPath+".before-restore"); got != 3 {
		t.Fatalf("safety copy rows = %d, want 3 (WAL replayed on open)", got)
	}
}
