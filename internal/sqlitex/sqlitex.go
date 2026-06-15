// Package sqlitex centralizes the secure-by-default opening of Arc's SQLite
// databases. Arc stores token hashes, RBAC roles, audit records, and tiering
// metadata in SQLite; those files must never be created with the default
// process umask (typically 0644, world-readable on multi-user hosts).
//
// The hardening logic originated inline in internal/auth.NewAuthManager
// (PR #508, security finding H2). This package is the single source of truth so
// every subsystem that opens a SQLite DB on its own path gets the same 0600
// treatment (security finding M4, issue #509).
//
// The package has no internal dependencies (only zerolog and the go-sqlite3
// driver), so any subsystem can import it without risking an import cycle.
package sqlitex

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"

	// Register the SQLite driver so callers that import only this package still
	// get a working "sqlite3" driver registration.
	_ "github.com/mattn/go-sqlite3"
)

// isInMemory reports whether dbPath is one of SQLite's in-memory forms, which
// have no on-disk footprint and therefore skip all file operations. Only tests
// use these.
func isInMemory(dbPath string) bool {
	return dbPath == ":memory:" || strings.HasPrefix(dbPath, "file::memory:")
}

// Open opens (and, for a fresh database, creates) a SQLite database at dbPath
// with owner-only (0600) permissions, eliminating the umask window in which the
// file would otherwise be world-readable.
//
// dbPath is a bare filesystem path by contract: it must NOT be a "file:" URI or
// already carry query parameters, because Open appends params to build the DSN.
// params is the query string WITHOUT a leading "?", e.g.
// "_journal_mode=WAL&_busy_timeout=5000"; pass "" for none. The in-memory forms
// (":memory:", "file::memory:") are accepted and skip all file operations.
//
// Open is idempotent against an existing database: MkdirAll is a no-op when the
// directory exists, O_CREATE leaves an existing file's permissions unchanged,
// and the Chmod re-tightens to 0600 either way. This matters because several
// subsystems share the default ./data/arc.db path and each calls Open.
//
// Open does NOT touch the -wal/-shm sidecar files, which SQLite only creates on
// first write. Call HardenWALSHM after the caller's schema initialization to
// lock those.
func Open(dbPath, params string) (*sql.DB, error) {
	if !isInMemory(dbPath) {
		// Ensure the parent directory exists with owner-only permissions.
		if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
			return nil, fmt.Errorf("failed to create db directory: %w", err)
		}

		// Pre-create the SQLite file with owner-only permissions (0600) so there
		// is no window where the file exists with the default umask (typically
		// 0644, world-readable) before the Chmod below runs. If the file already
		// exists, OpenFile leaves its permissions unchanged, which the explicit
		// Chmod after Ping then tightens.
		f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to create SQLite DB file: %w", err)
		}
		f.Close()
	}

	dsn := dbPath
	if params != "" {
		dsn = dbPath + "?" + params
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// SQLite only supports a single writer.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	// Force a connection so the SQLite file is initialized on disk (sql.Open
	// validates the DSN but defers file creation to first use).
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping SQLite database: %w", err)
	}

	if !isInMemory(dbPath) {
		// Tighten to 0600 even if the file pre-existed with looser permissions
		// from an earlier deployment.
		if err := os.Chmod(dbPath, 0600); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set SQLite DB permissions: %w", err)
		}
	}

	return db, nil
}

// HardenWALSHM locks the -wal and -shm sidecar files beside dbPath to 0600.
// SQLite in WAL mode creates these on first write; they can hold
// recently-committed secret material, so they must not inherit the umask.
//
// Call this AFTER schema initialization, when the sidecars are guaranteed to
// exist on a fresh install. It is a harmless no-op when they don't exist yet
// (not-exist errors are ignored) and when called repeatedly on a shared path.
// In-memory databases have no sidecars and are skipped.
//
// The logger receives a Warn only when symlink resolution fails on a path that
// might be a symlink — see the inline comment for why that matters.
func HardenWALSHM(dbPath string, logger zerolog.Logger) error {
	if isInMemory(dbPath) {
		return nil
	}

	// Resolve symlinks first: SQLite creates -wal/-shm beside the DB's real
	// path, so if dbPath is a symlink (e.g. /etc/arc/auth.db ->
	// /var/lib/arc/auth.db), "dbPath+ext" points at a non-existent sibling of
	// the link and the Chmod would silently no-op, leaving the real WAL/SHM at
	// the process umask. EvalSymlinks gives the canonical path; fall back to
	// dbPath if it can't be resolved.
	walBase := dbPath
	if resolved, err := filepath.EvalSymlinks(dbPath); err == nil {
		walBase = resolved
	} else {
		// Fall back to dbPath, but surface it: if dbPath is in fact a symlink,
		// the WAL/SHM chmod below targets the wrong (link-side) paths and
		// silently no-ops, leaving the real files at the umask.
		logger.Warn().Err(err).Str("db_path", dbPath).
			Msg("Could not resolve SQLite DB symlinks; WAL/SHM permissions may not be hardened if the path is a symlink")
	}

	for _, ext := range []string{"-wal", "-shm"} {
		// Chmod directly and ignore not-exist errors — avoids the TOCTOU race
		// between Stat+Chmod.
		p := walBase + ext
		if err := os.Chmod(p, 0600); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to set SQLite DB %s permissions: %w", ext, err)
		}
	}
	return nil
}
