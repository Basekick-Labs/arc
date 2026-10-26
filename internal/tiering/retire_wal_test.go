package tiering

// Retiring tier rows in volume keeps the store's WAL small. A DELETE ...
// RETURNING read with QueryRow never reached SQLite's DONE step, so its
// autocheckpoint never ran: 6.2M stale rows retired by one startup scan grew
// a customer's WAL to 99.6 GB.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// walStore opens a tier store the way production does: a file in WAL mode,
// one connection (internal/auth).
func walStore(t *testing.T) (*MetadataStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=ON")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	s, err := NewMetadataStore(db, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return s, path
}

func walBytes(t *testing.T, dbPath string) int64 {
	t.Helper()
	fi, err := os.Stat(dbPath + "-wal")
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// hotRows inserts n old hot rows in one transaction and returns their paths.
func hotRows(t *testing.T, s *MetadataStore, dbPath string, n int) []string {
	t.Helper()
	old := time.Now().Add(-time.Hour).UTC()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	st, err := tx.Prepare(`INSERT INTO tier_files (path, database, measurement, partition_time, tier, size_bytes, created_at) VALUES (?, 'db', ?, ?, 'hot', 7, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, n)
	for i := range n {
		paths[i] = fmt.Sprintf("db/m%d/2026/01/01/00/f_%07d.parquet", i%10, i)
		if _, err := st.Exec(paths[i], fmt.Sprintf("m%d", i%10), old, old); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	if w := walBytes(t, dbPath); w != 0 {
		t.Fatalf("setup left a %d-byte WAL", w)
	}
	return paths
}

// Each of the delete helpers, run row by row as their callers run them (the
// replication drainer's unlinks, cold-row retirement): the WAL stays near
// SQLite's 1000-page autocheckpoint, not ~30 KB a row.
func TestRetiringRowsOneByOneKeepsTheWALSmall(t *testing.T) {
	const n = 4000
	const limit = 16 << 20 // QueryRow measured ~120 MB here
	ctx := context.Background()

	s, dbPath := walStore(t)
	for _, p := range hotRows(t, s, dbPath, n) {
		if ok, _, _, err := s.retireHotRow(ctx, p); err != nil || !ok {
			t.Fatalf("retireHotRow(%s) = %v %v", p, ok, err)
		}
	}
	if w := walBytes(t, dbPath); w > limit {
		t.Fatalf("retireHotRow: WAL %d bytes after %d rows, want under %d", w, n, limit)
	}

	s, dbPath = walStore(t)
	for _, p := range hotRows(t, s, dbPath, n) {
		if ok, err := s.DeleteFileInTier(ctx, p, TierHot, time.Now()); err != nil || !ok {
			t.Fatalf("DeleteFileInTier(%s) = %v %v", p, ok, err)
		}
	}
	if w := walBytes(t, dbPath); w > limit {
		t.Fatalf("DeleteFileInTier: WAL %d bytes after %d rows, want under %d", w, n, limit)
	}
}

// The scan's stale-row walk reads the hot rows a page at a time, retires
// every stale one across pages (a quarantined one kept), and truncates a WAL
// file larger than truncateWALAboveBytes afterwards.
func TestRetireVanishedHotRowsPagesAndTruncatesTheWAL(t *testing.T) {
	oldPage, oldAbove := retireHotRowsPage, truncateWALAboveBytes
	retireHotRowsPage, truncateWALAboveBytes = 7, 0
	t.Cleanup(func() { retireHotRowsPage, truncateWALAboveBytes = oldPage, oldAbove })
	ctx := context.Background()
	s, dbPath := walStore(t)
	paths := hotRows(t, s, dbPath, 30)
	hot, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, hot, paths[3]) // still here: kept
	if err := s.QuarantineFile(ctx, paths[11], quarantineReasonInvalidPath); err != nil {
		t.Fatal(err)
	}
	m := &Manager{metadata: s, hotBackend: hot, logger: zerolog.Nop()}
	res := &ScanResult{}
	got := m.retireVanishedHotRows(ctx, []storage.ObjectInfo{{Path: paths[3]}}, time.Now(), res)
	if got != 28 || res.Errors != 0 || res.Truncated {
		t.Fatalf("retired %d %+v, want 28 (30 less the present and the quarantined)", got, res)
	}
	left, err := s.GetFilesInTier(ctx, TierHot)
	if err != nil || len(left) != 2 {
		t.Fatalf("left %d hot rows %v, want 2", len(left), err)
	}
	if w := walBytes(t, dbPath); w != 0 {
		t.Fatalf("WAL %d bytes after the pass, want it truncated", w)
	}
}

// A WAL file within truncateWALAboveBytes is left as it is: the truncate can
// hold the store's only connection on the busy handler, so it runs only for a
// WAL an earlier version grew.
func TestRetireLeavesASmallWALAlone(t *testing.T) {
	ctx := context.Background()
	s, dbPath := walStore(t)
	hotRows(t, s, dbPath, 30)
	hot, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{metadata: s, hotBackend: hot, logger: zerolog.Nop()}
	if got := m.retireVanishedHotRows(ctx, nil, time.Now(), &ScanResult{}); got != 30 {
		t.Fatalf("retired %d, want 30", got)
	}
	if w := walBytes(t, dbPath); w == 0 {
		t.Fatal("a small WAL was truncated")
	}
}
