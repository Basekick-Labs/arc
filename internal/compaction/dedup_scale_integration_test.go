//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestBuildCompactionQuery_DedupMixedTimeAtScale is a DuckDB-backed regression
// test for the live wedge that hit production/cpu/2026/06/18/03: a tagged
// (dedup-branch) partition holding many TIMESTAMPTZ-time files plus one
// VARCHAR-time file (from a pre-#493 writer), at the real cpu schema WIDTH
// (host, time, value, cpu_idle, cpu_user — 5 columns).
//
// Why width matters: a plain CTE is not a materialization barrier, so above a
// column-count threshold the dedup window's "time" reference fails to bind over
// the raw union_by_name scan → `Failed to bind column reference "time":
// TIMESTAMP WITH TIME ZONE != VARCHAR`. The original #493 dedup test used a
// 3-column fixture and did NOT trip it, so it passed while real 5-column cpu
// partitions stayed wedged. The fix is to materialize the normalized read into
// a CREATE OR REPLACE TEMP TABLE before running the dedup window over it (see
// the long comment in dedup.go; OFFSET 0 was tried and fails on real files).
//
// This test fans out over file counts and VARCHAR-file positions so the
// pushdown can't hide behind a particular ordering, and asserts both that the
// query binds and that dedup is correct.
func TestBuildCompactionQuery_DedupMixedTimeAtScale(t *testing.T) {
	for _, nFiles := range []int{20, 79} {
		// vcPos == -1 means NO VARCHAR file (every file TIMESTAMPTZ); the rest add a
		// real VARCHAR-time file at that index. NOTE: these synthetic fixtures are a
		// WEAKER guard than the real wedge — the production/cpu wedge fired even with
		// vcPos==-1 (all-TIMESTAMPTZ, phantom VARCHAR bind error), but synthetic
		// files do NOT reproduce that all-TZ binder quirk (something in Arc's real
		// Arrow schema metadata is required). The faithful regression is the opt-in
		// TestDedupCompaction_RealPartition below (run against ARC_REPRO_DIR). This
		// test still guards the VARCHAR-present cases and the temp-table correctness.
		for _, vcPos := range []int{-1, 0, 16, nFiles - 1} {
			t.Run(fmt.Sprintf("files=%d/varchar_at=%d", nFiles, vcPos), func(t *testing.T) {
				dir := t.TempDir()
				db, err := sql.Open("duckdb", "")
				if err != nil {
					t.Fatalf("open duckdb: %v", err)
				}
				defer db.Close()
				ctx := context.Background()
				if _, err := db.ExecContext(ctx, "SET TimeZone='Etc/GMT+3'"); err != nil {
					t.Fatalf("set tz: %v", err)
				}

				// Each file has a UNIQUE (host, time) so no real duplicates exist;
				// correct output row count == nFiles. The VARCHAR-time file encodes
				// the same instant as a numeric-epoch string, which must normalize
				// to TIMESTAMPTZ rather than wedge the bind.
				var files []string
				for i := 0; i < nFiles; i++ {
					f := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("f%03d.parquet", i)))
					base := int64(1718683200000000) + int64(i)*1_000_000
					var copySQL string
					if i == vcPos {
						copySQL = fmt.Sprintf(
							`COPY (SELECT 'h%d' AS host, '%d' AS "time", 1.0 AS value, 2.0 AS cpu_idle, 3.0 AS cpu_user) TO '%s' (FORMAT PARQUET)`,
							i, base, escapeSQLPath(f))
					} else {
						copySQL = fmt.Sprintf(
							`COPY (SELECT 'h%d' AS host, make_timestamptz(%d) AS "time", 1.0 AS value, 2.0 AS cpu_idle, 3.0 AS cpu_user) TO '%s' (FORMAT PARQUET)`,
							i, base, escapeSQLPath(f))
					}
					if _, err := db.ExecContext(ctx, copySQL); err != nil {
						t.Fatalf("write file %d: %v", i, err)
					}
					files = append(files, f)
				}

				quoted := ""
				for i, f := range files {
					if i > 0 {
						quoted += ", "
					}
					quoted += "'" + escapeSQLPath(f) + "'"
				}
				fileList := "[" + quoted + "]"
				out := filepath.ToSlash(filepath.Join(dir, "out.parquet"))

				// Dedup branch (host tag) — the one that wedged.
				if err := execCompaction(ctx, db, fileList, `ORDER BY "time"`, out, []string{"host"}, false); err != nil {
					t.Fatalf("dedup compaction wedged (temp-table materialization missing?): %v", err)
				}

				var rows int
				if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(out))).Scan(&rows); err != nil {
					t.Fatalf("count output: %v", err)
				}
				if rows != nFiles {
					t.Errorf("got %d rows, want %d (each file is a distinct (host,time); none should be dropped or duplicated)", rows, nFiles)
				}

				var colType string
				if err := db.QueryRowContext(ctx, fmt.Sprintf(
					`SELECT typeof("time") FROM read_parquet('%s') LIMIT 1`, escapeSQLPath(out))).Scan(&colType); err != nil {
					t.Fatalf("type of time: %v", err)
				}
				if colType != "TIMESTAMP WITH TIME ZONE" {
					t.Errorf("output time type = %q, want TIMESTAMP WITH TIME ZONE", colType)
				}
			})
		}
	}
}
