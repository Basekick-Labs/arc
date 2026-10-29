package api

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A real scheduled run (SQLite definitions, DuckDB, LocalBackend, ArrowBuffer).
// tag_columns that do not cover the query's grouping used to fail the run
// (#1023), because compaction kept one row per (tags, time) and would have
// deleted the other regions' rows. Compaction now removes only rows identical
// in every column (#1232), so the run succeeds and writes every row.
func TestExecuteCQ_TagColumnsNotCoveringTheGroupingStillWrite(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dataDir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buf.Close() })

	ctx := context.Background()
	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	us := hour.UnixMicro()
	write := func(meas string, cols map[string][]interface{}) {
		t.Helper()
		if err := buf.WriteColumnarDirectNoWAL(ctx, "db1", meas, cols); err != nil {
			t.Fatal(err)
		}
	}
	write("cpu", map[string][]interface{}{
		"time":   {us, us + 1, us + 2, us + 3},
		"host":   {"a", "a", "a", "a"},
		"region": {"eu", "eu", "us", "us"},
		"usage":  {1.0, 2.0, 3.0, 4.0},
	})
	// The same reading twice, as a client retry leaves it before compaction.
	write("dup", map[string][]interface{}{
		"time":  {us, us},
		"host":  {"a", "a"},
		"usage": {1.0, 1.0},
	})
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	h := newSQLiteOnlyCQHandler(t, filepath.Join(root, "cq.db"))
	t.Cleanup(func() { h.sqliteDB.Close() })
	h.db, h.storage, h.arrowBuffer = duck, backend, buf

	watermark := hour.Add(-time.Hour).Format(time.RFC3339)
	const grouped = "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, region, avg(usage) AS usage " +
		"FROM db1.cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2, 3"
	const rawCopy = "SELECT time, host, usage FROM db1.dup WHERE time >= {start_time} AND time < {end_time}"
	seed := func(id int, name, query, dest, tags string) {
		t.Helper()
		if _, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries
			(id, name, database, source_measurement, destination_measurement, query, interval, tag_columns, is_active, last_processed_time)
			VALUES (?, ?, 'db1', ?, ?, ?, '1h', ?, 1, ?)`, id, name, map[int]string{1: "cpu", 2: "cpu", 3: "dup", 4: "cpu", 5: "cpu"}[id], dest, query, tags, watermark); err != nil {
			t.Fatal(err)
		}
	}
	seed(1, "stale_tags", grouped, "cpu_stale", `["host"]`)
	seed(2, "full_tags", grouped, "cpu_full", `["host","region"]`)
	seed(3, "raw_copy", rawCopy, "dup_copy", `["host"]`)
	seed(4, "tagless", grouped, "cpu_tagless", `[]`)
	const underscored = "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host AS _h, avg(usage) AS usage " +
		"FROM db1.cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2"
	seed(5, "underscore_tag", underscored, "cpu_underscore", `["_h"]`)

	destFiles := func(meas string) int {
		t.Helper()
		if err := buf.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
		n := 0
		_ = filepath.Walk(filepath.Join(dataDir, "db1", meas), func(p string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() && strings.HasSuffix(p, ".parquet") {
				n++
			}
			return nil
		})
		return n
	}

	resp, err := h.ExecuteCQ(ctx, 1)
	if err != nil {
		t.Fatalf("a run whose tag_columns do not cover its grouping failed: %v", err)
	}
	if resp.RecordsWritten != 2 {
		t.Errorf("wrote %d rows, want 2 (one per region)", resp.RecordsWritten)
	}
	if n := destFiles("cpu_stale"); n == 0 {
		t.Error("the run wrote no file")
	}

	resp, err = h.ExecuteCQ(ctx, 2)
	if err != nil {
		t.Fatalf("a run whose tag_columns cover its grouping failed: %v", err)
	}
	if resp.RecordsWritten != 2 {
		t.Errorf("wrote %d rows, want 2 (one per region)", resp.RecordsWritten)
	}
	if n := destFiles("cpu_full"); n == 0 {
		t.Error("an accepted run wrote no file")
	}

	if _, err := h.ExecuteCQ(ctx, 3); err != nil {
		t.Fatalf("a raw copy whose duplicate rows are identical failed: %v", err)
	}

	// Output without tag columns is marked arc:dedup_exact whatever its shape —
	// two rows per timestamp here — so compaction checks it for exact copies.
	if _, err := h.ExecuteCQ(ctx, 4); err != nil {
		t.Fatalf("tagless grouped run failed: %v", err)
	}
	if n := destFiles("cpu_tagless"); n == 0 {
		t.Fatal("tagless run wrote no file")
	}
	marker := func(meas string) (string, error) {
		var v string
		glob := filepath.ToSlash(filepath.Join(dataDir, "db1", meas)) + "/**/*.parquet"
		err := duck.DB().QueryRow(`SELECT value FROM parquet_kv_metadata('` + glob + `') WHERE key = 'arc:dedup_exact'`).Scan(&v)
		return v, err
	}
	if v, err := marker("cpu_tagless"); err != nil || v != "true" {
		t.Errorf("tagless output marker = %q (err %v), want true", v, err)
	}

	// A definition stored before tag columns starting with '_' were refused
	// (#1258). Arc does not store that column, so its name must not reach the
	// file's arc:tags (compaction could never bind it), and the output must
	// not be marked either: rows that differed only in the unstored column
	// would look identical and collapse.
	if _, err := h.ExecuteCQ(ctx, 5); err != nil {
		t.Fatalf("run with an underscore tag column failed: %v", err)
	}
	if n := destFiles("cpu_underscore"); n == 0 {
		t.Fatal("underscore-tag run wrote no file")
	}
	if v, err := marker("cpu_underscore"); err != sql.ErrNoRows {
		t.Errorf("underscore-tag output is marked %q (err %v); rows differing only in _h would collapse", v, err)
	}
	var tagsMeta string
	glob := filepath.ToSlash(filepath.Join(dataDir, "db1", "cpu_underscore")) + "/**/*.parquet"
	if err := duck.DB().QueryRow(`SELECT value FROM parquet_kv_metadata('` + glob + `') WHERE key = 'arc:tags'`).Scan(&tagsMeta); err != sql.ErrNoRows {
		t.Errorf("underscore-tag output carries arc:tags %q (err %v)", tagsMeta, err)
	}
}
