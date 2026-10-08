package tiering

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func batchRow(path, database, measurement string, size int64) FileMetadata {
	return FileMetadata{
		Path:          path,
		Database:      database,
		Measurement:   measurement,
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		SizeBytes:     size,
	}
}

// Every row in the batch lands, and migrated_at stays in the TEXT domain the
// single-file recorder writes. go-sqlite3 binds a time.Time with its offset
// appended ("2025-01-01 00:00:00+00:00"), and migrated_at is compared as a
// string against every other row — so a bound time here would sort against
// every row Arc has ever written.
func TestRecordColdFilesBatchWritesEveryRow(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	stamp := time.Date(2026, 3, 6, 13, 0, 0, 0, time.UTC)
	files := []FileMetadata{
		batchRow("db1/cpu/2025/01/01/00/a.parquet", "db1", "cpu", 10),
		batchRow("db1/cpu/2025/01/01/01/b.parquet", "db1", "cpu", 20),
		batchRow("db1/mem/2025/01/01/00/c.parquet", "db1", "mem", 30),
	}
	notWritten, err := store.RecordColdFilesBatch(ctx, files, stamp)
	if err != nil {
		t.Fatalf("RecordColdFilesBatch: %v", err)
	}
	if len(notWritten) != 0 {
		t.Fatalf("notWritten = %v, want none", notWritten)
	}

	for _, f := range files {
		got, err := store.GetFile(ctx, f.Path)
		if err != nil {
			t.Fatalf("GetFile %s: %v", f.Path, err)
		}
		if got.Tier != TierCold || got.SizeBytes != f.SizeBytes {
			t.Errorf("%s = tier %q size %d, want cold and %d", f.Path, got.Tier, got.SizeBytes, f.SizeBytes)
		}
	}

	// Read through an expression so the driver returns the stored TEXT: a
	// bare column declared TIMESTAMP is parsed back into a time.Time on scan
	// and re-rendered, which would hide the stored shape entirely.
	var raw string
	if err := store.db.QueryRowContext(ctx, `SELECT migrated_at || '' FROM tier_files WHERE path = ?`, files[0].Path).Scan(&raw); err != nil {
		t.Fatalf("reading migrated_at: %v", err)
	}
	if want := stamp.Format(sqliteTimestampLayout); raw != want {
		t.Errorf("migrated_at = %q, want %q: the column is compared as text against every other row", raw, want)
	}

	// And byte-identical to what the single-file recorder writes for the same
	// moment — the sync still uses that one, so the two must sort against each
	// other.
	single := batchRow("db1/cpu/2025/01/01/09/single.parquet", "db1", "cpu", 40)
	if _, err := store.RecordColdFile(ctx, &single, stamp); err != nil {
		t.Fatal(err)
	}
	var singleRaw string
	if err := store.db.QueryRowContext(ctx, `SELECT migrated_at || '' FROM tier_files WHERE path = ?`, single.Path).Scan(&singleRaw); err != nil {
		t.Fatal(err)
	}
	if singleRaw != raw {
		t.Errorf("batch wrote migrated_at %q and the single-file recorder wrote %q for the same moment; they are compared as strings against each other", raw, singleRaw)
	}
}

// A quarantined row in the middle of a batch is left exactly as it is and
// reported, and the rows around it are still written. The whole point of
// returning paths rather than a count is that the caller can tell this from a
// failure (#758: tiering has established it can never act on that key).
func TestRecordColdFilesBatchLeavesAQuarantinedRowAloneAndWritesTheRest(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	const bad = "db1/cpu/2025/01/01/00/quarantined.parquet"
	recordAt(t, store, bad, "db1", TierHot)
	if err := store.QuarantineFile(ctx, bad, "unusable"); err != nil {
		t.Fatal(err)
	}

	files := []FileMetadata{
		batchRow("db1/cpu/2025/01/01/00/a.parquet", "db1", "cpu", 10),
		batchRow(bad, "db1", "cpu", 20),
		batchRow("db1/cpu/2025/01/01/02/c.parquet", "db1", "cpu", 30),
	}
	notWritten, err := store.RecordColdFilesBatch(ctx, files, time.Now())
	if err != nil {
		t.Fatalf("RecordColdFilesBatch: %v", err)
	}
	if len(notWritten) != 1 || notWritten[0] != bad {
		t.Fatalf("notWritten = %v, want only the quarantined path", notWritten)
	}
	if got, _ := store.GetFile(ctx, bad); got.Tier != TierHot {
		t.Errorf("the quarantined row is now tier %q, want hot (untouched)", got.Tier)
	}
	for _, p := range []string{files[0].Path, files[2].Path} {
		if got, _ := store.GetFile(ctx, p); got == nil || got.Tier != TierCold {
			t.Errorf("%s was not written; one quarantined path must not cost the others their rows", p)
		}
	}
}

// One transaction per call is what makes the caller's accounting exact: a
// returned error means NOTHING was written, so the caller counts the whole
// chunk as unrecorded without having to ask how far it got.
//
// Proved with a trigger that aborts ONE row in the middle of the batch, on a
// live context: the rows before it have already executed inside the
// transaction, so if the batch were not atomic they would survive. A batch
// that skipped the bad row and committed the rest would break the contract in
// both directions — it would report success for a chunk with a missing row,
// and leave the caller unable to count either outcome.
func TestRecordColdFilesBatchIsAllOrNothing(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	files := []FileMetadata{
		batchRow("db1/cpu/2025/01/01/00/a.parquet", "db1", "cpu", 10),
		batchRow("db1/cpu/2025/01/01/01/b.parquet", "db1", "cpu", 20),
		batchRow("db1/cpu/2025/01/01/02/c.parquet", "db1", "cpu", 30),
	}
	// The path is a literal because trigger DDL cannot use bound parameters.
	if _, err := store.db.ExecContext(ctx, `
		CREATE TRIGGER reject_the_middle_row BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db1/cpu/2025/01/01/01/b.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("installing the trigger: %v", err)
	}
	if files[1].Path != "db1/cpu/2025/01/01/01/b.parquet" {
		t.Fatalf("the trigger and the batch disagree about the rejected path: %s", files[1].Path)
	}

	notWritten, err := store.RecordColdFilesBatch(ctx, files, time.Now())
	if err == nil {
		t.Fatal("RecordColdFilesBatch reported success although a row was rejected")
	}
	if notWritten != nil {
		t.Errorf("notWritten = %v, want nil: an error means the caller counts the whole chunk", notWritten)
	}
	for _, f := range files {
		if got, _ := store.GetFile(ctx, f.Path); got != nil {
			t.Errorf("%s survived a failed batch; the rows before the rejected one must roll back with it", f.Path)
		}
	}
}

// And the same contract when the transaction cannot even begin.
func TestRecordColdFilesBatchWritesNothingOnADeadContext(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()

	dead, cancel := context.WithCancel(context.Background())
	cancel()

	files := []FileMetadata{batchRow("db1/cpu/2025/01/01/00/a.parquet", "db1", "cpu", 10)}
	if _, err := store.RecordColdFilesBatch(dead, files, time.Now()); err == nil {
		t.Fatal("RecordColdFilesBatch on a dead context reported success")
	}
	if got, _ := store.GetFile(context.Background(), files[0].Path); got != nil {
		t.Error("a row was written on a dead context")
	}
}

// The forced hot batch clears migrated_at on every row, because none of those
// files is a migrated copy any more — a leftover stamp would put the row in
// the orphan-reconciliation window as a cold candidate with no cold object to
// verify against.
func TestRecordRestoredHotFilesBatchClearsMigratedAt(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	files := []FileMetadata{
		batchRow("db1/cpu/2025/01/01/00/a.parquet", "db1", "cpu", 10),
		batchRow("db1/cpu/2025/01/01/01/b.parquet", "db1", "cpu", 20),
	}
	if _, err := store.RecordColdFilesBatch(ctx, files, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRestoredHotFilesBatch(ctx, files); err != nil {
		t.Fatalf("RecordRestoredHotFilesBatch: %v", err)
	}
	for _, f := range files {
		got, err := store.GetFile(ctx, f.Path)
		if err != nil {
			t.Fatal(err)
		}
		if got.Tier != TierHot {
			t.Errorf("%s = tier %q, want hot: the ordinary hot report cannot move a cold row, which is why this exists", f.Path, got.Tier)
		}
		if got.MigratedAt != nil {
			t.Errorf("%s migrated_at = %v, want cleared", f.Path, got.MigratedAt)
		}
	}
}

// The cache is keyed by database/measurement and a restore writes many files
// under each pair, so a batch invalidates once per pair — not once per row.
func TestRecordColdFilesBatchInvalidatesTheCacheOncePerMeasurement(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	var files []FileMetadata
	for i := 0; i < 5; i++ {
		files = append(files, batchRow(fmt.Sprintf("db1/cpu/2025/01/01/00/f%d.parquet", i), "db1", "cpu", 10))
	}
	files = append(files, batchRow("db1/mem/2025/01/01/00/m.parquet", "db1", "mem", 10))

	store.tierCacheMu.Lock()
	before := store.tierCacheGen
	store.tierCacheMu.Unlock()

	if _, err := store.RecordColdFilesBatch(ctx, files, time.Now()); err != nil {
		t.Fatal(err)
	}

	store.tierCacheMu.Lock()
	after := store.tierCacheGen
	store.tierCacheMu.Unlock()
	if got := after - before; got != 2 {
		t.Errorf("cache generation moved %d times for %d rows in 2 measurements, want 2", got, len(files))
	}
}

// An unparseable path costs only itself. It is reported as FAILED rather than
// quarantined, because the two are different facts about a key: one is
// something tiering recorded, the other is Arc not knowing.
func TestRecordRestoredColdFilesReportsUnparseablePathsSeparately(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	m := &Manager{metadata: store}

	const good = "db1/cpu/2025/01/01/00/a.parquet"
	const bad = "not-a-data-path.parquet"
	quarantined, failed, err := m.RecordRestoredColdFiles(ctx, map[string]int64{good: 10, bad: 20})
	if err != nil {
		t.Fatalf("RecordRestoredColdFiles: %v", err)
	}
	if len(quarantined) != 0 {
		t.Errorf("quarantined = %v, want none: an unparseable path is not a quarantined one", quarantined)
	}
	if len(failed) != 1 || failed[0] != bad {
		t.Fatalf("failed = %v, want only %s", failed, bad)
	}
	if got, _ := store.GetFile(ctx, good); got == nil || got.Tier != TierCold {
		t.Errorf("%s was not written; one unparseable path must not cost the others their rows", good)
	}
}

// The failed batch must not leave its transaction open, and this is the test
// that can tell: the shared handle Arc runs on allows exactly ONE connection
// (SetMaxOpenConns(1) in internal/auth), so a transaction that is never rolled
// back holds the only connection forever and the next SQLite user in the
// process — auth, audit, MQTT, tier registration — waits for it indefinitely.
// Row visibility cannot show this: an uncommitted transaction's rows are
// invisible either way.
func TestAFailedBatchReleasesTheOnlyConnection(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	store.db.SetMaxOpenConns(1)

	if _, err := store.db.ExecContext(ctx, `
		CREATE TRIGGER reject_the_doomed_row BEFORE INSERT ON tier_files
		WHEN NEW.path = 'db1/cpu/2025/01/01/00/doomed.parquet'
		BEGIN SELECT RAISE(ABORT, 'rejected by the test'); END
	`); err != nil {
		t.Fatalf("installing the trigger: %v", err)
	}

	doomed := []FileMetadata{batchRow("db1/cpu/2025/01/01/00/doomed.parquet", "db1", "cpu", 10)}
	if _, err := store.RecordColdFilesBatch(ctx, doomed, time.Now()); err == nil {
		t.Fatal("the rejected batch reported success")
	}

	// Bounded, because the failure mode under test is an indefinite wait.
	next, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	good := []FileMetadata{batchRow("db1/cpu/2025/01/01/01/good.parquet", "db1", "cpu", 20)}
	if _, err := store.RecordColdFilesBatch(next, good, time.Now()); err != nil {
		t.Fatalf("the batch after a failed one could not get a connection: %v", err)
	}
	if got, _ := store.GetFile(ctx, good[0].Path); got == nil {
		t.Error("the batch after a failed one wrote nothing")
	}
}

// A real transaction at the size the caller actually sends. Every other store
// test here runs a handful of rows, so nothing else exercises a full chunk
// against SQLite — the 1001-row test lives at the backup layer and never
// reaches a database. Just above the threshold, per the SQLite checklist.
func TestRecordColdFilesBatchAtAFullChunk(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	// Bulk insert in one transaction is still one fsync per commit; this keeps
	// the test off the disk entirely.
	if _, err := store.db.ExecContext(ctx, `PRAGMA synchronous = OFF`); err != nil {
		t.Fatal(err)
	}

	const rows = 1050
	files := make([]FileMetadata, 0, rows)
	for i := 0; i < rows; i++ {
		files = append(files, batchRow(fmt.Sprintf("db1/cpu/2025/01/01/%02d/f%d.parquet", i%24, i), "db1", "cpu", int64(i+1)))
	}

	start := time.Now()
	notWritten, err := store.RecordColdFilesBatch(ctx, files, time.Now())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("RecordColdFilesBatch of %d rows: %v", rows, err)
	}
	if len(notWritten) != 0 {
		t.Fatalf("notWritten = %d paths, want none", len(notWritten))
	}

	var cold int
	if err := store.db.QueryRowContext(ctx, `SELECT count(*) FROM tier_files WHERE tier = 'cold'`).Scan(&cold); err != nil {
		t.Fatal(err)
	}
	if cold != rows {
		t.Errorf("%d cold rows, want %d", cold, rows)
	}
	// Not a benchmark — a guard that one chunk is nowhere near the flush
	// timeout the restore allows it (coldRowFlushTimeout, 30s).
	if elapsed > 10*time.Second {
		t.Errorf("one chunk of %d rows took %v, which is close enough to the flush timeout to matter", rows, elapsed)
	}
	t.Logf("one chunk of %d rows in %v", rows, elapsed)
}
