package backup

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// #1550: main.go records that a restored arc.db's tier rows may be missing
// cold files through ApplyPendingRestoresWith's hook. The hook must run for a
// valid staged restore, before the swap; and a hook error must leave the
// restore staged rather than apply it unrecorded.

func TestApplyWith_HookRunsBeforeTheSwap(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	current := makeSQLite(t, dbPath, 2)
	makeSQLite(t, StagePath(dbPath), 5)

	var calls []string
	ApplyPendingRestoresWith(zerolog.Nop(), func(p string) error {
		calls = append(calls, p)
		// Still the current database: nothing moved yet.
		if data, _ := os.ReadFile(dbPath); !bytes.Equal(data, current) {
			t.Error("hook ran after the database was swapped")
		}
		return nil
	}, dbPath, "", dbPath)

	if len(calls) != 1 || calls[0] != dbPath {
		t.Fatalf("hook calls = %v, want one for %s", calls, dbPath)
	}
	if got := rowCount(t, dbPath); got != 5 {
		t.Fatalf("rows = %d, want the restored 5", got)
	}
}

func TestApplyWith_HookErrorLeavesTheRestoreStaged(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	current := makeSQLite(t, dbPath, 2)
	makeSQLite(t, StagePath(dbPath), 5)

	ApplyPendingRestoresWith(zerolog.Nop(), func(string) error { return errors.New("disk full") }, dbPath)

	if data, _ := os.ReadFile(dbPath); !bytes.Equal(data, current) {
		t.Fatal("a failed hook still swapped the database")
	}
	if _, err := os.Stat(StagePath(dbPath)); err != nil {
		t.Fatalf("restore no longer staged after a failed hook: %v", err)
	}
}

// No staged restore, or an invalid one: the hook never runs.
func TestApplyWith_HookNotRunWithoutAValidRestore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")
	makeSQLite(t, dbPath, 2)
	called := false
	hook := func(string) error { called = true; return nil }

	ApplyPendingRestoresWith(zerolog.Nop(), hook, dbPath)
	if err := os.WriteFile(StagePath(dbPath), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	ApplyPendingRestoresWith(zerolog.Nop(), hook, dbPath)
	if called {
		t.Fatal("hook ran without a valid staged restore")
	}
}
