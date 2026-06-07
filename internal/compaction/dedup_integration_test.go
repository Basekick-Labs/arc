package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

// TestBuildCompactionQuery_DedupMixedTimeType is a DuckDB-backed regression test
// for the dedup compaction query. String assertions cannot catch the binder
// behavior this code depends on, so this exercises the real generated SQL
// against real parquet fixtures.
//
// It guards two distinct DuckDB pitfalls in the dedup path:
//   - the subquery `SELECT *, ROW_NUMBER() ... ) WHERE rn=1` form mis-binds time
//     as VARCHAR under union_by_name (loud plan error), and
//   - a top-level `SELECT * REPLACE(time...) ... QUALIFY ROW_NUMBER() OVER(... time)`
//     runs the window over the RAW time (QUALIFY precedes projection), silently
//     under-deduping a mixed-type partition.
//
// The fixtures put the SAME (host, time) key in two files — one with time as a
// proper TIMESTAMPTZ, one with time as a VARCHAR epoch string (a pre-fix writer).
// Correct dedup collapses them to exactly one row.
func TestBuildCompactionQuery_DedupMixedTimeType(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Worst case for tz handling: a non-UTC session timezone.
	if _, err := db.ExecContext(ctx, "SET TimeZone='America/Argentina/Buenos_Aires'"); err != nil {
		t.Fatalf("set tz: %v", err)
	}

	fileTZ := filepath.Join(dir, "a_tz.parquet")
	fileStr := filepath.Join(dir, "b_str.parquet")
	out := filepath.Join(dir, "out.parquet")

	// 2021-01-01T00:00:00Z = 1609459200000000 microseconds. Same (host,time) in both.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'h1' AS host, make_timestamptz(1609459200000000) AS "time", 1.0 AS v) TO '%s' (FORMAT PARQUET)`, fileTZ)); err != nil {
		t.Fatalf("write tz fixture: %v", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'h1' AS host, '1609459200000000' AS "time", 2.0 AS v) TO '%s' (FORMAT PARQUET)`, fileStr)); err != nil {
		t.Fatalf("write varchar fixture: %v", err)
	}

	fileList := fmt.Sprintf("['%s', '%s']", fileTZ, fileStr)
	query := buildCompactionQuery(fileList, `ORDER BY "time"`, out, []string{"host"})

	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("compaction query failed (bind error regression?): %v\nquery:\n%s", err, query)
	}

	// Correct dedup → exactly one row.
	var rows int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, out)).Scan(&rows); err != nil {
		t.Fatalf("count output: %v", err)
	}
	if rows != 1 {
		t.Errorf("dedup produced %d rows, want 1 (mixed-type (host,time) must collapse to one)", rows)
	}

	// Output time must be TIMESTAMP WITH TIME ZONE (matches Arc's ingest schema; UTC-anchored).
	var colType string
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT column_type FROM (DESCRIBE SELECT * FROM read_parquet('%s')) WHERE column_name='time'`, out)).Scan(&colType); err != nil {
		t.Fatalf("describe output: %v", err)
	}
	if colType != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("output time type = %q, want TIMESTAMP WITH TIME ZONE", colType)
	}

	// The instant must be exactly 1609459200000000 µs regardless of the non-UTC session tz.
	var epochUS int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT epoch_us("time") FROM read_parquet('%s')`, out)).Scan(&epochUS); err != nil {
		t.Fatalf("epoch_us: %v", err)
	}
	if epochUS != 1609459200000000 {
		t.Errorf("stored instant = %d µs, want 1609459200000000 (UTC must be preserved)", epochUS)
	}
	_ = os.Remove
}

// TestBuildCompactionQuery_DedupNonDedupBothNormalizeTime asserts both branches
// (with and without tag columns) normalize time to TIMESTAMPTZ so a VARCHAR-time
// file never wedges compaction.
func TestBuildCompactionQuery_DedupNonDedupBothNormalizeTime(t *testing.T) {
	withTags := buildCompactionQuery("['a.parquet']", "", "/tmp/o.parquet", []string{"host"})
	noTags := buildCompactionQuery("['a.parquet']", "", "/tmp/o.parquet", nil)
	for name, q := range map[string]string{"dedup": withTags, "non-dedup": noTags} {
		if !strings.Contains(q, "make_timestamptz") || !strings.Contains(q, "TIMESTAMPTZ") {
			t.Errorf("%s branch does not normalize time to TIMESTAMPTZ:\n%s", name, q)
		}
	}
}
