//go:build duckdb_arrow

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// TestDedupCompaction_RealPartition is the FAITHFUL regression for the
// production/cpu/2026/06/18/03 wedge. Synthetic fixtures do not reproduce the
// all-TIMESTAMPTZ phantom-VARCHAR bind failure (see the note in
// TestBuildCompactionQuery_DedupMixedTimeAtScale) — only Arc's real Parquet
// files (with their embedded Arrow schema metadata) trigger it. This test is
// opt-in: set ARC_REPRO_DIR to a directory of real Arc-written .parquet files
// that carry tag metadata (e.g. a copied cpu partition).
//
//	ARC_REPRO_DIR=/path/to/partition go test -tags duckdb_arrow \
//	  ./internal/compaction/ -run TestDedupCompaction_RealPartition -v
//
// It asserts the dedup compaction (temp-table form) binds and produces output
// on real files, AND that injecting a VARCHAR-time duplicate still dedups to a
// single row. Before the temp-table fix, the first step wedged with
// `Failed to bind column reference "time": TIMESTAMP WITH TIME ZONE != VARCHAR`.
func TestDedupCompaction_RealPartition(t *testing.T) {
	dir := os.Getenv("ARC_REPRO_DIR")
	if dir == "" {
		t.Skip("set ARC_REPRO_DIR to a directory of real Arc-written .parquet files")
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var files []string
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".parquet" {
			files = append(files, filepath.ToSlash(filepath.Join(dir, e.Name())))
		}
	}
	if len(files) < 2 {
		t.Skipf("need >=2 real parquet files, found %d", len(files))
	}
	sort.Strings(files)
	if len(files) > 20 {
		files = files[:20] // enough to trigger; keep it fast
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	quote := func(fs []string) string {
		s := ""
		for i, f := range fs {
			if i > 0 {
				s += ", "
			}
			s += "'" + escapeSQLPath(f) + "'"
		}
		return "[" + s + "]"
	}

	// Step 1: real files only (all TIMESTAMPTZ) — must bind.
	out1 := filepath.ToSlash(filepath.Join(t.TempDir(), "real.parquet"))
	if err := execCompaction(ctx, db, quote(files), `ORDER BY "time"`, out1, []string{"host"}); err != nil {
		t.Fatalf("dedup compaction wedged on real all-TIMESTAMPTZ files: %v", err)
	}
	var baseRows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(out1))).Scan(&baseRows); err != nil {
		t.Fatalf("count real output: %v", err)
	}
	t.Logf("real-only dedup output rows = %d", baseRows)

	// Step 2: inject a VARCHAR-time duplicate of an existing (host,time) and
	// confirm it both binds and dedups (output rows unchanged, not +1).
	tmp := t.TempDir()
	vc := filepath.ToSlash(filepath.Join(tmp, "varchar_dup.parquet"))
	// Pull one real (host, epoch_us) to duplicate as a VARCHAR-time row.
	var host string
	var epochUS int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT "host", epoch_us("time") FROM read_parquet('%s') LIMIT 1`, escapeSQLPath(out1))).Scan(&host, &epochUS); err != nil {
		t.Fatalf("sample row: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT '%s' AS host, '%d' AS "time", 1.0 AS value, 2.0 AS cpu_idle, 3.0 AS cpu_user) TO '%s' (FORMAT PARQUET)`,
		host, epochUS, escapeSQLPath(vc))); err != nil {
		t.Fatalf("write varchar dup: %v", err)
	}
	out2 := filepath.ToSlash(filepath.Join(tmp, "withdup.parquet"))
	if err := execCompaction(ctx, db, quote(append(files, vc)), `ORDER BY "time"`, out2, []string{"host"}); err != nil {
		t.Fatalf("dedup compaction wedged with injected VARCHAR-time file: %v", err)
	}
	var dupRows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT count(*) FROM read_parquet('%s') WHERE "host"='%s' AND epoch_us("time")=%d`,
		escapeSQLPath(out2), host, epochUS)).Scan(&dupRows); err != nil {
		t.Fatalf("count dup key: %v", err)
	}
	if dupRows != 1 {
		t.Errorf("injected VARCHAR duplicate of (%s,%d) produced %d rows, want 1 (under-dedup)", host, epochUS, dupRows)
	}
}
