//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Two different hour partitions can contain different Parquet files with the
// same basename. Both sets of rows must survive downloading and compaction.
func TestCompactionTwoHoursSameBasenamePreservesRows(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	fixtureDir := t.TempDir()
	basename := "cpu_20260411_140001_123456789.parquet"

	keys := []string{
		"testdb/cpu/2026/04/11/14/" + basename,
		"testdb/cpu/2026/04/11/15/" + basename,
	}

	fixtures := []struct {
		hour string
		sql  string
	}{
		{
			hour: "14",
			sql:  `TIMESTAMPTZ '2026-04-11 14:00:00Z' AS time, 'hour14' AS host, 1.0 AS value`,
		},
		{
			hour: "15",
			sql:  `TIMESTAMPTZ '2026-04-11 15:00:00Z' AS time, 'hour15' AS host, 2.0 AS value`,
		},
	}

	for i, fixture := range fixtures {
		dir := filepath.Join(fixtureDir, fixture.hour)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}

		source := filepath.Join(dir, basename)
		writeFixtureParquet(t, ctx, db, source, fixture.sql)

		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.Write(ctx, keys[i], data); err != nil {
			t.Fatalf("seed %s: %v", keys[i], err)
		}
	}

	workDir := t.TempDir()
	job := NewJob(&JobConfig{
		Measurement:    "cpu",
		PartitionPath:  "testdb/cpu/2026/04/11",
		Files:          keys,
		StorageBackend: backend,
		Database:       "testdb",
		Tier:           "daily",
		TempDirectory:  workDir,
		Logger:         zerolog.Nop(),
		DB:             db,
	})

	downloaded, err := job.downloadFiles(ctx, workDir)
	if err != nil {
		t.Fatalf("downloadFiles: %v", err)
	}
	if len(downloaded) != 2 {
		t.Fatalf("downloaded %d files, want 2", len(downloaded))
	}
	if downloaded[0].localPath == downloaded[1].localPath {
		t.Fatal("different Parquet inputs share a temporary path")
	}

	output, err := job.compactFiles(ctx, downloaded, workDir)
	if err != nil {
		t.Fatalf("compactFiles: %v", err)
	}

	query := fmt.Sprintf(
		"SELECT count(*), count(DISTINCT host), sum(value) FROM read_parquet('%s')",
		escapeSQLPath(filepath.ToSlash(output)),
	)

	var rows, distinctHosts int
	var total float64
	if err := db.QueryRowContext(ctx, query).Scan(
		&rows, &distinctHosts, &total,
	); err != nil {
		t.Fatalf("query compacted output: %v", err)
	}

	if rows != 2 || distinctHosts != 2 || total != 3 {
		t.Fatalf(
			"compacted data corrupted: rows=%d, distinct hosts=%d, sum=%g; want 2, 2, 3",
			rows, distinctHosts, total,
		)
	}

	if job.FilesCompacted != 2 || len(job.compactedFiles) != 2 {
		t.Fatalf(
			"compaction tracked %d files and %d source keys; want 2 each",
			job.FilesCompacted, len(job.compactedFiles),
		)
	}
}
