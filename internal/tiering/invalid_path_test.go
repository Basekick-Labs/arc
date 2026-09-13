package tiering

// Regression tests for #758: a migration candidate whose storage key no
// backend can address (storage.ErrInvalidPath, permanent by contract) was
// re-selected every scheduled cycle, forever, with no backoff or attempt cap,
// and each cycle wrote another failed-migration row. The candidate set is
// recomputed from SQLite every cycle, so "dropped" has to be persisted on the
// file index row rather than remembered in memory.
//
// Each quarantine test is paired with an over-correction guard proving a
// TRANSIENT failure on the same path still retries, which is the dangerous
// direction: a quarantine widened to "any copy error" would silently retire
// a file from tiering on a network blip.

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// unusableDailyKey parses as a day-level daily file (so FindCandidates admits
// it) and is refused by storage.ValidateKey: a backslash past index 0 is one
// of the spellings an earlier binary could store (#747). The database and
// measurement segments are clean so parseFilePath's own traversal check does
// not reject it first; the point is a row that reaches the copy.
const unusableDailyKey = `db1/cpu/2024/03/15/cpu\20240315_daily.parquet`

func quarantinedTotal(t *testing.T) int64 {
	t.Helper()
	n, ok := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64)
	if !ok {
		t.Fatal("storage_invalid_path_quarantined_total missing from metrics snapshot")
	}
	return n
}

// registerUnusableHotDaily plants the file in hot storage the way the field
// gets it (under the contract's nose) and lets the scan index it, which is
// the provenance chain the issue describes: local listings do not filter
// unusable keys (#756), so ScanAndRegisterFiles turns the file into a row.
func registerUnusableHotDaily(t *testing.T, m *Manager, hot *mockBackend) {
	t.Helper()
	ctx := context.Background()
	hot.seedRaw(unusableDailyKey, []byte("daily"))
	if err := storage.ValidateKey(unusableDailyKey); err == nil {
		t.Fatal("test key must be refused by the storage contract")
	}
	res, err := m.ScanAndRegisterFiles(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if res.FilesRegistered != 1 {
		t.Fatalf("scan registered %d files, want the unusable key indexed (that is the bug's precondition)", res.FilesRegistered)
	}
}

func TestMigrationQuarantinesUnusableKeyInsteadOfRetrying(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	registerUnusableHotDaily(t, m, hot)

	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Path != unusableDailyKey {
		t.Fatalf("candidates = %+v, want exactly the unusable key (precondition)", candidates)
	}

	before := quarantinedTotal(t)
	migrated, failed := m.migrator.MigrateBatch(ctx, candidates)
	if migrated != 0 || failed != 1 {
		t.Fatalf("first cycle MigrateBatch = (%d, %d), want (0, 1): the cycle that discovers the condition reports it", migrated, failed)
	}
	if got := quarantinedTotal(t); got != before+1 {
		t.Errorf("storage_invalid_path_quarantined_total = %d, want %d", got, before+1)
	}

	// The row is marked, its tier untouched, and the file is still there.
	meta, err := m.metadata.GetFile(ctx, unusableDailyKey)
	if err != nil || meta == nil {
		t.Fatalf("GetFile: (%+v, %v)", meta, err)
	}
	if meta.QuarantinedAt == nil {
		t.Fatal("row was not quarantined; the next cycle would re-select it")
	}
	if meta.QuarantineReason != quarantineReasonInvalidPath {
		t.Errorf("quarantine_reason = %q, want %q", meta.QuarantineReason, quarantineReasonInvalidPath)
	}
	if meta.Tier != TierHot {
		t.Errorf("tier = %s, want hot: quarantine must not re-tier the row", meta.Tier)
	}
	hot.mu.RLock()
	_, stillThere := hot.files[unusableDailyKey]
	hot.mu.RUnlock()
	if !stillThere {
		t.Error("hot copy was removed; a quarantined file must not be deleted")
	}
	cold.mu.RLock()
	_, inCold := cold.files[unusableDailyKey]
	cold.mu.RUnlock()
	if inCold {
		t.Error("an unusable key landed in cold storage")
	}

	// This is the bug: the next cycle must not see it again.
	candidates, err = m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("second cycle candidates = %+v, want none", candidates)
	}
	if migrated, failed := m.migrator.MigrateTier(ctx, TierHot, TierCold); migrated != 0 || failed != 0 {
		t.Fatalf("second cycle MigrateTier = (%d, %d), want (0, 0)", migrated, failed)
	}

	// Exactly one failed-migration row, from the cycle that discovered it.
	history, err := m.metadata.GetRecentMigrations(ctx, 10)
	if err != nil {
		t.Fatalf("GetRecentMigrations: %v", err)
	}
	if len(history) != 1 || history[0].Error == "" {
		t.Fatalf("migration history = %+v, want one failed record", history)
	}

	// A rescan (every cycle runs one) must not clear the mark: RecordFile's
	// upsert touches tier and size, never the quarantine columns.
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatalf("rescan: %v", err)
	}
	meta, _ = m.metadata.GetFile(ctx, unusableDailyKey)
	if meta.QuarantinedAt == nil {
		t.Fatal("rescan cleared the quarantine mark")
	}
	n, err := m.metadata.CountQuarantinedFiles(ctx)
	if err != nil || n != 1 {
		t.Fatalf("CountQuarantinedFiles = (%d, %v), want 1", n, err)
	}
}

// readFailingBackend fails ReadTo for one key with a transient error and
// otherwise behaves as the mock it wraps.
type readFailingBackend struct {
	*mockBackend
	failKey string
}

func (b *readFailingBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	if path == b.failKey {
		return errors.New("connection reset by peer")
	}
	return b.mockBackend.ReadTo(ctx, path, w)
}

// TestMigrationRetriesTransientCopyFailure is the over-correction guard.
func TestMigrationRetriesTransientCopyFailure(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	flaky := "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	if err := hot.Write(ctx, flaky, []byte("daily")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatal(err)
	}
	m.hotBackend = &readFailingBackend{mockBackend: hot, failKey: flaky}

	candidates, _ := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v, want one", candidates)
	}
	before := quarantinedTotal(t)
	if migrated, failed := m.migrator.MigrateBatch(ctx, candidates); migrated != 0 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d, %d), want (0, 1)", migrated, failed)
	}
	if got := quarantinedTotal(t); got != before {
		t.Errorf("a transient failure moved the quarantine counter (%d -> %d)", before, got)
	}
	meta, _ := m.metadata.GetFile(ctx, flaky)
	if meta.QuarantinedAt != nil {
		t.Fatal("a transient copy failure quarantined the file; it must retry next cycle")
	}
	candidates, _ = m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if len(candidates) != 1 || candidates[0].Path != flaky {
		t.Fatalf("second cycle candidates = %+v, want the file re-selected", candidates)
	}

	// And once the blip clears, the very next cycle migrates it.
	m.hotBackend = hot
	if migrated, failed := m.migrator.MigrateBatch(ctx, candidates); migrated != 1 || failed != 0 {
		t.Fatalf("MigrateBatch after recovery = (%d, %d), want (1, 0)", migrated, failed)
	}
}

// TestMigrateFileErrorIsBothQuarantinedAndInvalidPath pins the error shape
// MigrateBatch relies on to suppress its duplicate log line, and that the
// underlying cause survives for anyone else who branches on it.
func TestMigrateFileErrorIsBothQuarantinedAndInvalidPath(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	registerUnusableHotDaily(t, m, hot)

	candidates, _ := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	err := m.migrator.MigrateFile(ctx, candidates[0])
	if !errors.Is(err, ErrCandidateQuarantined) {
		t.Errorf("err = %v, want errors.Is ErrCandidateQuarantined", err)
	}
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("err = %v, want errors.Is storage.ErrInvalidPath", err)
	}
}

// TestStreamingCopyReportsInvalidPathWhicheverSideFailsFirst covers the race
// in copyFileStreaming: the reader and writer goroutines each send an error,
// and the writer's may be its own rather than the pipe's. Whichever arrives
// first, the permanent one must be what the caller classifies on.
func TestStreamingCopyReportsInvalidPathWhicheverSideFailsFirst(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	// Source accepts the key and streams data; only the destination refuses
	// it, after the reader has already sent nil. Then the reverse.
	src := &keyBlindBackend{data: []byte("bytes")}
	dst := &refusingBackend{}
	err := m.migrator.copyFileStreaming(ctx, src, dst, unusableDailyKey, 5)
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("dst-only refusal: err = %v, want ErrInvalidPath", err)
	}

	err = m.migrator.copyFileStreaming(ctx, &refusingBackend{}, &keyBlindBackend{}, unusableDailyKey, 5)
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("src-only refusal: err = %v, want ErrInvalidPath", err)
	}

	// A transient error on one side and a permanent one on the other still
	// classifies as permanent, in either arrival order.
	err = m.migrator.copyFileStreaming(ctx, &transientBackend{}, &refusingBackend{}, unusableDailyKey, 5)
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("transient src + refusing dst: err = %v, want ErrInvalidPath", err)
	}
	err = m.migrator.copyFileStreaming(ctx, &refusingBackend{}, &transientBackend{}, unusableDailyKey, 5)
	if !errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("refusing src + transient dst: err = %v, want ErrInvalidPath", err)
	}

	// And two transient errors stay transient.
	err = m.migrator.copyFileStreaming(ctx, &transientBackend{}, &transientBackend{}, unusableDailyKey, 5)
	if err == nil || errors.Is(err, storage.ErrInvalidPath) {
		t.Errorf("transient both sides: err = %v, want a non-permanent error", err)
	}
}

// keyBlindBackend streams without validating, standing in for a backend that
// happens to accept a key its counterpart refuses.
type keyBlindBackend struct{ data []byte }

func (b *keyBlindBackend) ReadTo(_ context.Context, _ string, w io.Writer) error {
	_, err := w.Write(b.data)
	return err
}
func (b *keyBlindBackend) WriteReader(_ context.Context, _ string, r io.Reader, _ int64) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

// refusingBackend enforces the contract and nothing else.
type refusingBackend struct{}

func (refusingBackend) ReadTo(_ context.Context, path string, _ io.Writer) error {
	return storage.ValidateKey(path)
}
func (refusingBackend) WriteReader(_ context.Context, path string, r io.Reader, _ int64) error {
	if err := storage.ValidateKey(path); err != nil {
		return err
	}
	_, err := io.Copy(io.Discard, r)
	return err
}

// transientBackend fails both directions with an ordinary I/O error.
type transientBackend struct{}

func (transientBackend) ReadTo(_ context.Context, _ string, _ io.Writer) error {
	return errors.New("connection reset by peer")
}
func (transientBackend) WriteReader(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return errors.New("connection reset by peer")
}

// registerUnusableColdRow builds the reconciliation precondition: a cold row
// with a recent migrated_at whose key the hot backend refuses. Only an
// earlier binary could have migrated such a file, so the row is planted
// through the metadata store directly.
func registerUnusableColdRow(t *testing.T, m *Manager) {
	t.Helper()
	ctx := context.Background()
	if err := m.metadata.RecordFile(ctx, &FileMetadata{
		Path:          unusableDailyKey,
		Database:      "db1",
		Measurement:   "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Tier:          TierHot,
		SizeBytes:     5,
	}); err != nil {
		t.Fatal(err)
	}
	// UpdateTier stamps migrated_at, which puts the row in the 48-hour window.
	if err := m.metadata.UpdateTier(ctx, unusableDailyKey, TierCold); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileQuarantinesUnusableColdRow(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	registerUnusableColdRow(t, m)

	before := quarantinedTotal(t)
	found, deleted, failed := m.migrator.ReconcileOrphanedFiles(ctx)
	if found != 0 || deleted != 0 || failed != 1 {
		t.Fatalf("first pass = (%d, %d, %d), want (0, 0, 1)", found, deleted, failed)
	}
	if got := quarantinedTotal(t); got != before+1 {
		t.Errorf("storage_invalid_path_quarantined_total = %d, want %d", got, before+1)
	}
	meta, _ := m.metadata.GetFile(ctx, unusableDailyKey)
	if meta.QuarantinedAt == nil || meta.Tier != TierCold {
		t.Fatalf("row after first pass = %+v, want quarantined and still cold", meta)
	}

	found, deleted, failed = m.migrator.ReconcileOrphanedFiles(ctx)
	if found != 0 || deleted != 0 || failed != 0 {
		t.Fatalf("second pass = (%d, %d, %d), want (0, 0, 0): the row must leave the reconcile work set", found, deleted, failed)
	}
}

// existsFailingBackend fails Exists for one key with a transient error.
type existsFailingBackend struct {
	*mockBackend
	failKey string
}

func (b *existsFailingBackend) Exists(ctx context.Context, path string) (bool, error) {
	if path == b.failKey {
		return false, errors.New("connection reset by peer")
	}
	return b.mockBackend.Exists(ctx, path)
}

// TestReconcileRetriesTransientExistsFailure is the over-correction guard for
// the reconciliation site.
func TestReconcileRetriesTransientExistsFailure(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	coldPath := "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	if err := m.metadata.RecordFile(ctx, &FileMetadata{
		Path: coldPath, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Tier:          TierHot, SizeBytes: 5,
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.UpdateTier(ctx, coldPath, TierCold); err != nil {
		t.Fatal(err)
	}
	m.hotBackend = &existsFailingBackend{mockBackend: hot, failKey: coldPath}

	before := quarantinedTotal(t)
	for pass := 1; pass <= 2; pass++ {
		if found, deleted, failed := m.migrator.ReconcileOrphanedFiles(ctx); found != 0 || deleted != 0 || failed != 1 {
			t.Fatalf("pass %d = (%d, %d, %d), want (0, 0, 1): a transient error must keep retrying", pass, found, deleted, failed)
		}
	}
	if got := quarantinedTotal(t); got != before {
		t.Errorf("a transient failure moved the quarantine counter (%d -> %d)", before, got)
	}
	meta, _ := m.metadata.GetFile(ctx, coldPath)
	if meta.QuarantinedAt != nil {
		t.Fatal("a transient Exists failure quarantined the row")
	}
}

func TestQuarantineFileIsIdempotentAndReportsUnknownPath(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	if err := store.QuarantineFile(ctx, "nope/x/2024/03/15/x_daily.parquet", "r"); err == nil {
		t.Fatal("quarantining an unknown path must fail rather than silently succeed")
	}

	path := "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	if err := store.RecordFile(ctx, &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
		Tier:          TierHot, SizeBytes: 5,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.QuarantineFile(ctx, path, "first"); err != nil {
		t.Fatal(err)
	}
	first, _ := store.GetFile(ctx, path)
	time.Sleep(10 * time.Millisecond)
	if err := store.QuarantineFile(ctx, path, "second"); err != nil {
		t.Fatal(err)
	}
	again, _ := store.GetFile(ctx, path)
	if !again.QuarantinedAt.Equal(*first.QuarantinedAt) || again.QuarantineReason != "first" {
		t.Fatalf("second mark rewrote the record: %+v -> %+v; the first observation must be kept", first, again)
	}

	list, err := store.GetQuarantinedFiles(ctx)
	if err != nil || len(list) != 1 || list[0].Path != path {
		t.Fatalf("GetQuarantinedFiles = (%+v, %v), want the one row", list, err)
	}
	old, err := store.GetFilesOlderThan(ctx, TierHot, 24*time.Hour)
	if err != nil || len(old) != 0 {
		t.Fatalf("GetFilesOlderThan = (%+v, %v), want the quarantined row excluded", old, err)
	}
	// The non-work-set reads keep describing what is on disk.
	inTier, _ := store.GetFilesInTier(ctx, TierHot)
	if len(inTier) != 1 {
		t.Fatalf("GetFilesInTier dropped the quarantined row; the status and files endpoints must still show it")
	}
	tiers, _ := store.GetTiersForMeasurement(ctx, "db1", "cpu")
	if !tiers[TierHot] {
		t.Fatal("GetTiersForMeasurement dropped hot; the file is still a real file in the hot partition")
	}
}

// TestSchemaUpgradeAddsQuarantineColumns opens a store over a tier_files
// table created by a binary that predates #758. CREATE TABLE IF NOT EXISTS
// leaves it as is, so the columns have to be added on open or every
// candidate query fails on the missing column.
func TestSchemaUpgradeAddsQuarantineColumns(t *testing.T) {
	tmp, err := os.CreateTemp("", "tiering_schema_upgrade_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	db, err := sql.Open("sqlite3", tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(`
		CREATE TABLE tier_files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT UNIQUE NOT NULL,
			database TEXT NOT NULL,
			measurement TEXT NOT NULL,
			partition_time TIMESTAMP NOT NULL,
			tier TEXT NOT NULL DEFAULT 'hot',
			size_bytes INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			migrated_at TIMESTAMP
		);
		INSERT INTO tier_files (path, database, measurement, partition_time, tier, size_bytes)
		VALUES ('db1/cpu/2024/03/15/cpu_20240315_daily.parquet', 'db1', 'cpu', '2024-03-15 00:00:00', 'hot', 5);
	`); err != nil {
		t.Fatal(err)
	}

	for open := 1; open <= 2; open++ { // the second open exercises the duplicate-column tolerance
		store, err := NewMetadataStore(db, zerolog.Nop())
		if err != nil {
			t.Fatalf("open %d over a pre-#758 table: %v", open, err)
		}
		ctx := context.Background()
		old, err := store.GetFilesOlderThan(ctx, TierHot, 24*time.Hour)
		if err != nil || len(old) != 1 {
			t.Fatalf("open %d: GetFilesOlderThan = (%+v, %v), want the pre-existing row, not quarantined", open, old, err)
		}
		if old[0].QuarantinedAt != nil {
			t.Fatalf("open %d: a pre-existing row reads as quarantined", open)
		}
	}
}
