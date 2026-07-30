package auth

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// SharesDBPath decides whether a feature with its own *_db_path config key can
// borrow the auth handle. A false negative opens a redundant connection pool on
// the same file; a false positive silently ignores an operator's explicit
// choice to use a separate database.
func TestSharesDBPath(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	am, err := NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	t.Run("identical path", func(t *testing.T) {
		if !am.SharesDBPath(dbPath) {
			t.Error("the same path must be reported as shared")
		}
	})

	t.Run("relative form of the same file", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Skip("cannot determine working directory")
		}
		rel, err := filepath.Rel(cwd, dbPath)
		if err != nil {
			t.Skip("paths are not relatable on this filesystem")
		}
		if !am.SharesDBPath(rel) {
			t.Errorf("relative path %q refers to the same file and must be reported as shared", rel)
		}
	})

	t.Run("path with redundant separators", func(t *testing.T) {
		noisy := filepath.Join(dir, ".", "arc.db")
		if !am.SharesDBPath(noisy) {
			t.Errorf("%q refers to the same file and must be reported as shared", noisy)
		}
	})

	t.Run("different file", func(t *testing.T) {
		if am.SharesDBPath(filepath.Join(dir, "retention.db")) {
			t.Error("a different file must not be reported as shared — the operator asked for a separate database")
		}
	})

	t.Run("same basename in another directory", func(t *testing.T) {
		if am.SharesDBPath(filepath.Join(t.TempDir(), "arc.db")) {
			t.Error("matching basenames in different directories are different databases")
		}
	})

	t.Run("empty path", func(t *testing.T) {
		if am.SharesDBPath("") {
			t.Error("an empty path must not be reported as shared")
		}
	})
}

// A symlinked path is the same database, and answering otherwise opens a second
// pool against one file.
func TestSharesDBPath_ResolvesSymlinks(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	am, err := NewAuthManager(dbPath, time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	link := filepath.Join(dir, "linked.db")
	if err := os.Symlink(dbPath, link); err != nil {
		t.Skipf("cannot create symlinks here: %v", err)
	}

	if !am.SharesDBPath(link) {
		t.Error("a symlink to the auth database is the same database")
	}
}

// The nil receiver is reachable: features gated on a license rather than on
// cfg.Auth.Enabled can run with auth disabled, and must not panic.
func TestSharesDBPath_NilReceiver(t *testing.T) {
	var am *AuthManager
	if am.SharesDBPath("./data/arc.db") {
		t.Error("a nil auth manager has no database to share")
	}
}

// In-memory databases are private to the connection that opened them, so two
// handles naming ":memory:" are not the same database.
func TestSharesDBPath_InMemoryIsNeverShared(t *testing.T) {
	am, err := NewAuthManager(":memory:", time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()

	if am.SharesDBPath(":memory:") {
		t.Error("in-memory databases are per-connection and must never be reported as shared")
	}
}
