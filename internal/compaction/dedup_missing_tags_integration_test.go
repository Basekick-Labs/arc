//go:build duckdb_arrow

package compaction

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// Files written before ingest checked its tag names can carry arc:tags entries
// with no column: a line-protocol tag key starting with '_' (the file leaves
// that column out), an escaped comma ("a,b" reads back as a and b), or a
// MessagePack tag_keys typo. The dedup query failed to bind them, so the
// partition never compacted (#1258). Compaction now leaves those names out of
// the key and dedups on the rest.
func TestCompactFilesSkipsTagNamesWithoutColumns(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tags     string // arc:tags as an older build wrote it
		wantRows int64  // both files hold the same two rows
		wantMode string
		hostCol  string // the column's name in the files; "host" when empty
	}{
		{"escaped_comma", "a,b,host", 2, "Auto-dedup enabled: found tag metadata", ""},
		{"underscore_key", "_x,host", 2, "Auto-dedup enabled: found tag metadata", ""},
		{"typo", "host_typo", 4, "", ""},
		{"case_differs", "HOST", 2, "Auto-dedup enabled: found tag metadata", ""},
		{"column_case_differs", "host", 2, "Auto-dedup enabled: found tag metadata", "Host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()
			ctx := context.Background()

			hostCol := tc.hostCol
			if hostCol == "" {
				hostCol = "host"
			}
			rows := `SELECT 'h1' AS "` + hostCol + `", TIMESTAMPTZ '2026-01-01 00:00:00' AS "time", 1 AS v
			         UNION ALL SELECT 'h2', TIMESTAMPTZ '2026-01-01 00:00:01', 2`
			write := func(name string) string {
				path := filepath.ToSlash(filepath.Join(dir, name))
				if _, err := db.ExecContext(ctx, fmt.Sprintf(
					`COPY (%s) TO '%s' (FORMAT PARQUET, KV_METADATA {'arc:tags': '%s'})`,
					rows, escapeSQLPath(path), tc.tags)); err != nil {
					t.Fatalf("write fixture %s: %v", name, err)
				}
				return path
			}

			var logs bytes.Buffer
			job := &Job{
				Measurement:   "cpu",
				Tier:          "hourly",
				TempDirectory: dir,
				JobID:         "missing-tags",
				logger:        zerolog.New(&logs),
				db:            db,
			}
			output, err := job.compactFiles(ctx, []downloadedFile{
				{localPath: write("a.parquet"), storageKey: "db/cpu/a.parquet"},
				{localPath: write("b.parquet"), storageKey: "db/cpu/b.parquet"},
			}, dir)
			if err != nil {
				t.Fatalf("compactFiles: %v\nlog:\n%s", err, logs.String())
			}
			var got int64
			if err := db.QueryRowContext(ctx, fmt.Sprintf(
				`SELECT COUNT(*) FROM read_parquet('%s')`, escapeSQLPath(output))).Scan(&got); err != nil {
				t.Fatalf("count output rows: %v", err)
			}
			if got != tc.wantRows {
				t.Errorf("output rows = %d, want %d\nlog:\n%s", got, tc.wantRows, logs.String())
			}
			if tc.wantMode != "" && !strings.Contains(logs.String(), tc.wantMode) {
				t.Errorf("dedup did not run on the remaining tags; log:\n%s", logs.String())
			}
			if tc.wantMode == "" && strings.Contains(logs.String(), "Auto-dedup enabled") {
				t.Errorf("dedup ran with no usable tag; log:\n%s", logs.String())
			}
			if !strings.HasSuffix(tc.name, "case_differs") && !strings.Contains(logs.String(), "missing_tag_columns") {
				t.Errorf("the dropped names were not logged; log:\n%s", logs.String())
			}

			// When the schema probe fails, decideDedup gets no columns and keeps
			// every name, as before #1258: the compaction query then reports
			// the authoritative error instead of dedup turning off silently.
			paths := []string{filepath.ToSlash(filepath.Join(dir, "a.parquet"))}
			if tags, mode := job.decideDedup(ctx, db, paths, nil); mode != dedupExact || len(tags) != len(strings.Split(tc.tags, ",")) {
				t.Errorf("with no probe result decideDedup = %v, %v; want every name of %q kept", tags, mode, tc.tags)
			}
		})
	}
}
