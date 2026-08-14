//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
