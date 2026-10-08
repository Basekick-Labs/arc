//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// writeFixtureParquet writes a single-row parquet file whose columns come from
// the given SELECT list. Paths go through ToSlash because they are interpolated
// into DuckDB SQL.
func writeFixtureParquet(t *testing.T, ctx context.Context, db *sql.DB, path, selectList string) {
	t.Helper()
	q := fmt.Sprintf(`COPY (SELECT %s) TO '%s' (FORMAT PARQUET)`, selectList, escapeSQLPath(filepath.ToSlash(path)))
	if _, err := db.ExecContext(ctx, q); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}

type deleteInputAtLogHook struct {
	trigger string
	path    string
	once    sync.Once
	err     error
}

func (h *deleteInputAtLogHook) Run(_ *zerolog.Event, _ zerolog.Level, message string) {
	if message == h.trigger {
		h.once.Do(func() { h.err = os.Remove(h.path) })
	}
}

func TestJobRunInputDeletedBeforeValidationIsSkipped(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	baseDir := t.TempDir()
	backend, err := storage.NewLocalBackend(baseDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	const partition = "testdb/cpu/2026/04/11/14"
	keys := []string{partition + "/a.parquet", partition + "/b.parquet"}
	for i, key := range keys {
		fixture := filepath.Join(t.TempDir(), fmt.Sprintf("input-%d.parquet", i))
		writeFixtureParquet(t, ctx, db, fixture, fmt.Sprintf("TIMESTAMPTZ '2026-04-11 14:00:0%dZ' AS time, 'h%d' AS host, %d.0 AS value", i, i, i+1))
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}

	deletedPath, err := storage.ObjectURI(backend, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	hook := &deleteInputAtLogHook{trigger: "Downloaded files for compaction", path: deletedPath}
	logger := zerolog.New(os.Stderr).Hook(hook)
	job := NewJob(&JobConfig{
		Measurement: "cpu", PartitionPath: partition, Files: keys,
		StorageBackend: backend, Database: "testdb", Tier: "hourly",
		TempDirectory: t.TempDir(), Logger: logger, DB: db,
		ManifestManager: NewManifestManager(backend, zerolog.Nop()), JobID: "pre-validation-delete",
	})
	if err := job.Run(ctx); err != nil {
		t.Fatalf("Job.Run should skip the input removed before validation: %v", err)
	}
	if hook.err != nil {
		t.Fatalf("delete input at hook: %v", hook.err)
	}
	if job.Status != JobStatusCompleted || job.FilesCompacted != 1 {
		t.Fatalf("job status/files compacted = %s/%d, want completed/1", job.Status, job.FilesCompacted)
	}
	if job.OutputStorageKey == "" {
		t.Fatal("remaining valid input should produce an output")
	}
	var rows int
	outputURI, err := storage.ObjectURI(backend, job.OutputStorageKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", escapeSQLPath(filepath.ToSlash(outputURI)))).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("compacted rows = %d, want the surviving input's single row", rows)
	}
}

func TestJobRunInputDeletedAfterValidationFailsWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	baseDir := t.TempDir()
	backend, err := storage.NewLocalBackend(baseDir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	const partition = "testdb/cpu/2026/04/11/14"
	keys := []string{partition + "/a.parquet", partition + "/b.parquet"}
	for i, key := range keys {
		fixture := filepath.Join(t.TempDir(), fmt.Sprintf("input-%d.parquet", i))
		writeFixtureParquet(t, ctx, db, fixture, fmt.Sprintf("TIMESTAMPTZ '2026-04-11 14:00:0%dZ' AS time, 'h%d' AS host, %d.0 AS value", i, i, i+1))
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Write(ctx, key, data); err != nil {
			t.Fatal(err)
		}
	}

	deletedPath, err := storage.ObjectURI(backend, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	hook := &deleteInputAtLogHook{trigger: "Validated files for compaction", path: deletedPath}
	logger := zerolog.New(os.Stderr).Hook(hook)
	manifestManager := NewManifestManager(backend, zerolog.Nop())
	job := NewJob(&JobConfig{
		Measurement: "cpu", PartitionPath: partition, Files: keys,
		StorageBackend: backend, Database: "testdb", Tier: "hourly",
		TempDirectory: t.TempDir(), Logger: logger, DB: db,
		ManifestManager: manifestManager, JobID: "post-validation-delete",
	})
	err = job.Run(ctx)
	if err == nil {
		t.Fatal("Job.Run should fail when an input disappears after validation")
	}
	if hook.err != nil {
		t.Fatalf("delete input at hook: %v", hook.err)
	}
	if recoverable, reason := ClassifySubprocessError(err, ""); recoverable || reason != "permanent_error" {
		t.Fatalf("concurrent-delete error classified as recoverable=%v reason=%q; want permanent_error", recoverable, reason)
	}
	if job.Status != JobStatusFailed {
		t.Fatalf("job status = %s, want failed", job.Status)
	}
	if job.OutputStorageKey != "" || len(job.compactedFiles) != 0 || job.FilesCompacted != 0 {
		t.Fatalf("failed job published state: output=%q compacted=%v files=%d", job.OutputStorageKey, job.compactedFiles, job.FilesCompacted)
	}
	if exists, err := backend.Exists(ctx, keys[1]); err != nil || !exists {
		t.Fatalf("surviving source exists=%v err=%v, want it preserved", exists, err)
	}
	manifestPath := manifestManager.GenerateManifestPath("hourly", "testdb", partition, job.JobID)
	if exists, err := backend.Exists(ctx, manifestPath); err != nil || exists {
		t.Fatalf("storage recovery manifest exists=%v err=%v, want none", exists, err)
	}
}

// TestParquetFilesHaveTimeColumn exercises the schema probe that gates the
// time-normalizing REPLACE (and the ORDER BY "time" default) in compaction.
func TestParquetFilesHaveTimeColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	noTime1 := filepath.Join(dir, "notime1.parquet")
	noTime2 := filepath.Join(dir, "notime2.parquet")
	withTime := filepath.Join(dir, "withtime.parquet")
	writeFixtureParquet(t, ctx, db, noTime1, `'AAPL' AS symbol, 1.5 AS price, 1723600000000000::BIGINT AS timestamp`)
	writeFixtureParquet(t, ctx, db, noTime2, `'MSFT' AS symbol, 2.5 AS price, 1723600001000000::BIGINT AS timestamp`)
	writeFixtureParquet(t, ctx, db, withTime, `now() AS time, 'h1' AS host, 1.0 AS value`)

	list := func(paths ...string) string {
		out := "["
		for i, p := range paths {
			if i > 0 {
				out += ", "
			}
			out += fmt.Sprintf("'%s'", escapeSQLPath(filepath.ToSlash(p)))
		}
		return out + "]"
	}

	tests := []struct {
		name    string
		files   string
		hasTime bool
	}{
		{"no file has time", list(noTime1, noTime2), false},
		{"all files have time", list(withTime), true},
		// union_by_name backfills the missing column with NULLs, so a mixed
		// partition binds and must NOT be skipped.
		{"mixed: one file has time", list(noTime1, withTime), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parquetFilesHaveTimeColumn(ctx, db, tt.files)
			if err != nil {
				t.Fatalf("parquetFilesHaveTimeColumn: %v", err)
			}
			if got != tt.hasTime {
				t.Errorf("parquetFilesHaveTimeColumn = %v, want %v", got, tt.hasTime)
			}
		})
	}
}

// TestCompactFiles_SkipsPartitionWithoutTimeColumn is the end-to-end regression
// for the trades partition: files whose unified schema has no "time" column can
// never satisfy the REPLACE("time") normalization (nor the default ORDER BY
// "time"), so compactFiles must return errNoTimeColumn — a clean skip — instead
// of a Binder Error that the retry ladder then amplifies. Pre-fix, this test
// fails with: "Binder Error: Column \"time\" in REPLACE list not found in FROM
// clause".
func TestCompactFiles_SkipsPartitionWithoutTimeColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	f1 := filepath.Join(dir, "trades_1.parquet")
	f2 := filepath.Join(dir, "trades_2.parquet")
	writeFixtureParquet(t, ctx, db, f1, `'AAPL' AS symbol, 'buy' AS side, 1.5 AS price, 2.0 AS amount, 1723600000000000::BIGINT AS timestamp`)
	writeFixtureParquet(t, ctx, db, f2, `'MSFT' AS symbol, 'sell' AS side, 2.5 AS price, 3.0 AS amount, 1723600001000000::BIGINT AS timestamp`)

	job := NewJob(&JobConfig{
		Measurement:   "trades",
		PartitionPath: "trades/trades/2026/08/01",
		Database:      "trades",
		Tier:          "daily",
		TempDirectory: dir,
		Logger:        zerolog.Nop(),
		DB:            db,
	})

	files := []downloadedFile{
		{storageKey: "trades/trades/2026/08/01/00/trades_1.parquet", localPath: f1, size: fileSize(t, f1)},
		{storageKey: "trades/trades/2026/08/01/01/trades_2.parquet", localPath: f2, size: fileSize(t, f2)},
	}

	_, err = job.compactFiles(ctx, files, dir)
	if !errors.Is(err, errNoTimeColumn) {
		t.Fatalf("compactFiles error = %v, want errNoTimeColumn", err)
	}
	if len(job.compactedFiles) != 0 {
		t.Errorf("compactedFiles = %v, want empty (skip must not mark files deletable)", job.compactedFiles)
	}
	if job.FilesCompacted != 0 {
		t.Errorf("FilesCompacted = %d, want 0", job.FilesCompacted)
	}
}

// TestCompactFiles_TimeColumnPresentStillCompacts is the positive control: a
// normal partition (time column present) must be unaffected by the probe.
func TestCompactFiles_TimeColumnPresentStillCompacts(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	f1 := filepath.Join(dir, "cpu_1.parquet")
	f2 := filepath.Join(dir, "cpu_2.parquet")
	writeFixtureParquet(t, ctx, db, f1, `TIMESTAMPTZ '2026-08-01 00:00:00Z' AS time, 'h1' AS host, 1.0 AS value`)
	writeFixtureParquet(t, ctx, db, f2, `TIMESTAMPTZ '2026-08-01 00:00:01Z' AS time, 'h2' AS host, 2.0 AS value`)

	job := NewJob(&JobConfig{
		Measurement:   "cpu",
		PartitionPath: "production/cpu/2026/08/01/00",
		Database:      "production",
		Tier:          "hourly",
		TempDirectory: dir,
		Logger:        zerolog.Nop(),
		DB:            db,
	})

	files := []downloadedFile{
		{storageKey: "production/cpu/2026/08/01/00/cpu_1.parquet", localPath: f1, size: fileSize(t, f1)},
		{storageKey: "production/cpu/2026/08/01/00/cpu_2.parquet", localPath: f2, size: fileSize(t, f2)},
	}

	out, err := job.compactFiles(ctx, files, dir)
	if err != nil {
		t.Fatalf("compactFiles: %v", err)
	}
	if _, statErr := os.Stat(out); statErr != nil {
		t.Fatalf("compacted output missing: %v", statErr)
	}
	if job.FilesCompacted != 2 {
		t.Errorf("FilesCompacted = %d, want 2", job.FilesCompacted)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}
