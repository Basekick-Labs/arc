package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func metricsSnap() map[string]int64 {
	out := map[string]int64{}
	for k, v := range metrics.Get().Snapshot() {
		if n, ok := v.(int64); ok {
			out[k] = n
		}
	}
	return out
}

// #1343, through the real compaction subprocess (this test binary dispatches
// its compact child into the production subcommand, see the init in
// license_compaction_subprocess_test.go): the parent's own metrics move for a
// job that succeeds and one that fails, including the storage writes the
// child made.
func TestCompactionSubprocessOutcomeReachesParentMetrics(t *testing.T) {
	t.Setenv("ARC_TEST_LICENSE_COMPACTION_CHILD", "1")
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const part = "testdb/cpu/2026/10/01/00"
	if err := os.MkdirAll(filepath.Join(root, part), 0700); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("%s/raw%d.parquet", part, i)
		q := fmt.Sprintf("COPY (SELECT TIMESTAMPTZ '2026-10-01 00:00:00Z' + INTERVAL '%d seconds' AS time, %d AS value) TO '%s' (FORMAT PARQUET)",
			i, i, strings.ReplaceAll(filepath.Join(root, key), "'", "''"))
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend,
		LockManager:    compaction.NewLockManager(),
		MemoryLimit:    "256MB",
		TempDirectory:  t.TempDir(),
		Logger:         zerolog.Nop(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run := func(files []string) {
		t.Helper()
		_ = manager.CompactPartition(ctx, compaction.Candidate{Database: "testdb", Measurement: "cpu", PartitionPath: part,
			Files: files, FileCount: len(files), Tier: "hourly", BatchNumber: 1})
	}

	before := metricsSnap()
	run(keys)
	after := metricsSnap()
	for k, want := range map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_success": 1, "compaction_files_compacted": 2} {
		if got := after[k] - before[k]; got != want {
			t.Errorf("success: %s moved by %d, want %d", k, got, want)
		}
	}
	if after["compaction_bytes_written"] <= before["compaction_bytes_written"] {
		t.Error("success: bytes written did not move")
	}
	if after["storage_writes_total"] <= before["storage_writes_total"] {
		t.Error("success: the child's storage writes did not reach the parent")
	}

	// A job the child fails: its temp directory cannot be created (a file is
	// in the way).
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // the first job consumed the inputs
		key := fmt.Sprintf("%s/again%d.parquet", part, i)
		q := fmt.Sprintf("COPY (SELECT TIMESTAMPTZ '2026-10-01 00:00:00Z' + INTERVAL '%d seconds' AS time, %d AS value) TO '%s' (FORMAT PARQUET)",
			i, i, strings.ReplaceAll(filepath.Join(root, key), "'", "''"))
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
		keys[i] = key
	}
	manager = compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend,
		LockManager:    compaction.NewLockManager(),
		MemoryLimit:    "256MB",
		TempDirectory:  blocker,
		Logger:         zerolog.Nop(),
	})
	before = metricsSnap()
	run(keys)
	after = metricsSnap()
	for k, want := range map[string]int64{"compaction_jobs_total": 1, "compaction_jobs_failed": 1, "compaction_jobs_success": 0} {
		if got := after[k] - before[k]; got != want {
			t.Errorf("failure: %s moved by %d, want %d", k, got, want)
		}
	}
}
