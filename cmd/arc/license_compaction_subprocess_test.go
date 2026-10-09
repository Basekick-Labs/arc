package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The manager launches this test binary. Only dispatch its compact child into
// the real production subcommand; the parent runs the usual Go test entrypoint.
func init() {
	if os.Getenv("ARC_TEST_LICENSE_COMPACTION_CHILD") == "1" && len(os.Args) > 1 && os.Args[1] == "compact" {
		main()
		os.Exit(0)
	}
}

func TestApplyLicenseCoreLimits_ReachesDuckDBSubprocess(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		effective, licensed, configured, want int
	}{
		{"license_binds", 64, 4, 32, 4},
		{"quota_binds", 2, 64, 32, 2},
		{"lower_explicit", 64, 4, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withCores(t, 64, tc.effective)
			t.Setenv("ARC_TEST_LICENSE_COMPACTION_CHILD", "1")
			cfg := baseConfig()
			cfg.Compaction.Threads = tc.configured
			applyLicenseCoreLimits(&license.License{MaxCores: tc.licensed}, cfg)
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
				path := filepath.Join(root, key)
				query := fmt.Sprintf("COPY (SELECT TIMESTAMPTZ '2026-10-01 00:00:00Z' + INTERVAL '%d seconds' AS time, %d AS value) TO '%s' (FORMAT PARQUET)", i, i, strings.ReplaceAll(path, "'", "''"))
				if _, err := db.Exec(query); err != nil {
					t.Fatal(err)
				}
				keys = append(keys, key)
			}
			var logs bytes.Buffer
			manager := compaction.NewManager(&compaction.ManagerConfig{
				StorageBackend: backend,
				LockManager:    compaction.NewLockManager(),
				Threads:        cfg.Compaction.Threads,
				MemoryLimit:    "256MB",
				TempDirectory:  t.TempDir(),
				Logger:         zerolog.New(&logs),
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := manager.CompactPartition(ctx, compaction.Candidate{
				Database:      "testdb",
				Measurement:   "cpu",
				PartitionPath: part,
				Files:         keys,
				FileCount:     2,
				Tier:          "hourly",
				BatchNumber:   1,
			}); err != nil {
				t.Fatalf("compaction: %v\n%s", err, logs.String())
			}
			found := false
			for _, line := range strings.Split(logs.String(), "\n") {
				var outer struct {
					Message    string `json:"message"`
					Subprocess string `json:"subprocess"`
				}
				if json.Unmarshal([]byte(line), &outer) != nil || outer.Subprocess != "compaction" {
					continue
				}
				var child struct {
					Message string `json:"message"`
					Threads int    `json:"threads"`
				}
				if json.Unmarshal([]byte(outer.Message), &child) == nil && child.Message == "DuckDB thread count configured" {
					found = true
					if child.Threads != tc.want {
						t.Fatalf("child SET threads=%d, want %d", child.Threads, tc.want)
					}
				}
			}
			if !found {
				t.Fatalf("no successful child SET threads in logs:\n%s", logs.String())
			}
			files, err := backend.List(ctx, part+"/")
			if err != nil || len(files) != 1 {
				t.Fatalf("output files=%v err=%v", files, err)
			}
			var rows int
			if err := db.QueryRow("SELECT count(*) FROM read_parquet(?)", filepath.Join(root, files[0])).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != 2 {
				t.Fatalf("rows=%d, want 2", rows)
			}
			t.Logf("parent cap reached real child: configured=%d effective=%d license=%d child_threads=%d rows=%d", tc.configured, tc.effective, tc.licensed, tc.want, rows)
		})
	}
}

func TestApplyLicenseCoreLimits_LoadResolvesAutoBeforeLicense(t *testing.T) {
	withCores(t, 64, 2)
	t.Chdir(t.TempDir())
	t.Setenv("ARC_COMPACTION_THREADS", "0")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Compaction.Threads != 1 {
		t.Fatalf("loaded auto=%d, want 1", cfg.Compaction.Threads)
	}
	applyLicenseCoreLimits(&license.License{MaxCores: 64}, cfg)
	if cfg.Compaction.Threads != 1 {
		t.Fatalf("licensed auto=%d, want preserved 1", cfg.Compaction.Threads)
	}
}
