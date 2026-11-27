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

// sortKeysFixture writes one Parquet file per hour, the way ingest does with
// sort keys fleet,name: each file sorted by (fleet, name, time). rowsPerTruck
// rows per truck per hour, 10 s apart, starting 2016-01-01T00:00:00Z.
func sortKeysFixture(t *testing.T, ctx context.Context, db *sql.DB, dir string, hours, trucks, rowsPerTruck int, kv string) []downloadedFile {
	t.Helper()
	var files []downloadedFile
	for h := 0; h < hours; h++ {
		path := filepath.ToSlash(filepath.Join(dir, fmt.Sprintf("h%02d.parquet", h)))
		q := fmt.Sprintf(`COPY (
			SELECT CASE WHEN t %% 2 = 0 THEN 'East' ELSE 'West' END AS fleet,
			       'truck_' || lpad(CAST(t AS VARCHAR), 5, '0') AS name,
			       make_timestamptz(1451606400000000 + %d * 3600000000 + r * 10000000) AS "time",
			       random() AS velocity
			FROM range(%d) AS a(t), range(%d) AS b(r)
			ORDER BY fleet, name, "time"
		) TO '%s' (FORMAT PARQUET%s)`, h, trucks, rowsPerTruck, escapeSQLPath(path), kv)
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("write fixture hour %d: %v", h, err)
		}
		files = append(files, downloadedFile{localPath: path, storageKey: fmt.Sprintf("db/readings/h%02d.parquet", h)})
	}
	return files
}

func openDuckDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	// A session zone with a half-hour offset: a zone-dependent bucket
	// (date_trunc on a TIMESTAMPTZ) would cut hours at :30 UTC and break the
	// order assertions; the epoch-based bucket must not depend on the zone.
	if _, err := db.ExecContext(ctx, "SET TimeZone='Asia/Kolkata'"); err != nil {
		t.Fatalf("set tz: %v", err)
	}
	return db, ctx
}

// assertHourKeyTimeOrder checks the output is ordered by (hour, fleet, name, time).
func assertHourKeyTimeOrder(t *testing.T, ctx context.Context, db *sql.DB, output string) {
	t.Helper()
	var inversions int64
	q := fmt.Sprintf(`SELECT count(*) FROM (
		SELECT epoch_us("time") // 3600000000 AS h, fleet, name, "time",
		       lag(epoch_us("time") // 3600000000) OVER w AS ph, lag(fleet) OVER w AS pf,
		       lag(name) OVER w AS pn, lag("time") OVER w AS pt
		FROM (SELECT *, row_number() OVER () AS rn FROM read_parquet('%s'))
		WINDOW w AS (ORDER BY rn))
		WHERE ph IS NOT NULL AND (h, fleet, name, "time") < (ph, pf, pn, pt)`, escapeSQLPath(output))
	if err := db.QueryRowContext(ctx, q).Scan(&inversions); err != nil {
		t.Fatalf("order check: %v", err)
	}
	if inversions != 0 {
		t.Fatalf("compacted output has %d rows out of (hour, fleet, name, time) order", inversions)
	}
}

// TestCompactionSortKeysKeepHourOrder is the reason for the hour prefix: a
// daily compaction of hourly files sorted by (fleet, name, time) must keep each
// row group to about an hour, not spread every row group across the whole day.
// Ordered by the keys alone, every row group of this fixture spans all 3 hours.
func TestCompactionSortKeysKeepHourOrder(t *testing.T) {
	db, ctx := openDuckDB(t)
	dir := t.TempDir()
	// 3 hours x 400 trucks x 360 rows = 432,000 rows: several 122,880-row groups.
	files := sortKeysFixture(t, ctx, db, dir, 3, 400, 360, "")

	var logs bytes.Buffer
	job := &Job{
		Measurement: "readings", Tier: "daily", TempDirectory: dir, JobID: "sort-hour",
		SortKeys: []string{"fleet", "name", "time"}, SortKeysExplicit: true,
		logger: zerolog.New(&logs), db: db,
	}
	output, err := job.compactFiles(ctx, files, dir)
	if err != nil {
		t.Fatalf("compactFiles: %v\nlog:\n%s", err, logs.String())
	}
	assertHourKeyTimeOrder(t, ctx, db, output)

	// Every row group spans at most two adjacent hours (a group may straddle
	// an hour boundary); without the prefix each spans all three.
	var groups, wide int64
	q := fmt.Sprintf(`SELECT count(*), count(*) FILTER (WHERE span > 1) FROM (
		SELECT row_group_id,
		       (epoch_us(max(CAST(stats_max AS TIMESTAMPTZ))) // 3600000000) -
		       (epoch_us(min(CAST(stats_min AS TIMESTAMPTZ))) // 3600000000) AS span
		FROM parquet_metadata('%s') WHERE path_in_schema = 'time' GROUP BY row_group_id)`, escapeSQLPath(output))
	if err := db.QueryRowContext(ctx, q).Scan(&groups, &wide); err != nil {
		t.Fatalf("row group stats: %v", err)
	}
	if groups < 3 {
		t.Fatalf("fixture produced %d row groups; the span assertion needs several", groups)
	}
	if wide != 0 {
		t.Fatalf("%d of %d row groups span more than two adjacent hours", wide, groups)
	}
	if strings.Contains(logs.String(), "missing_sort_keys") {
		t.Fatalf("no key is missing, but the missing-key log fired:\n%s", logs.String())
	}
}

// TestCompactionSortKeyWithoutColumn: a configured sort key that the
// partition's files have no column for used to fail the ORDER BY bind, so the
// partition failed to compact on every cycle. It now compacts by the keys it
// has, and says so: at Warn for a key configured for the measurement, at Debug
// for a default_sort_keys key (normal for measurements without that column).
func TestCompactionSortKeyWithoutColumn(t *testing.T) {
	for _, explicit := range []bool{true, false} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			db, ctx := openDuckDB(t)
			dir := t.TempDir()
			files := sortKeysFixture(t, ctx, db, dir, 2, 20, 10, "")

			var logs bytes.Buffer
			job := &Job{
				Measurement: "readings", Tier: "daily", TempDirectory: dir, JobID: "sort-missing",
				SortKeys: []string{"site", "fleet", "name", "time"}, SortKeysExplicit: explicit,
				logger: zerolog.New(&logs), db: db,
			}
			output, err := job.compactFiles(ctx, files, dir)
			if err != nil {
				t.Fatalf("compactFiles with a key the files lack: %v\nlog:\n%s", err, logs.String())
			}
			assertHourKeyTimeOrder(t, ctx, db, output)

			wantLevel := `"level":"debug"`
			if explicit {
				wantLevel = `"level":"warn"`
			}
			var line string
			for _, l := range strings.Split(logs.String(), "\n") {
				if strings.Contains(l, "missing_sort_keys") {
					line = l
				}
			}
			if line == "" || !strings.Contains(line, `"missing_sort_keys":["site"]`) || !strings.Contains(line, wantLevel) {
				t.Fatalf("want one %s missing-key log naming site; got %q", wantLevel, line)
			}
		})
	}
}

// TestCompactionSortKeysDedupPath runs custom sort keys through the dedup
// branch, whose output is a UNION: DuckDB rejects the hour expression in an
// ORDER BY directly on a UNION, so the query must order a derived table.
func TestCompactionSortKeysDedupPath(t *testing.T) {
	db, ctx := openDuckDB(t)
	dir := t.TempDir()
	// The arc:tags footer sends the batch down the dedup branch. Two copies of
	// the same hours: dedup collapses them to one.
	kv := `, KV_METADATA {'arc:tags': 'fleet,name'}`
	a := sortKeysFixture(t, ctx, db, filepath.Join(dir), 2, 20, 10, kv)
	dirB := t.TempDir()
	// Same rows (random() differs, so make velocity deterministic instead).
	for _, f := range a {
		dup := filepath.ToSlash(filepath.Join(dirB, filepath.Base(f.localPath)))
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT PARQUET%s)`,
			escapeSQLPath(f.localPath), escapeSQLPath(dup), kv)); err != nil {
			t.Fatalf("copy fixture: %v", err)
		}
		a = append(a, downloadedFile{localPath: dup, storageKey: "db/readings/dup_" + filepath.Base(dup)})
	}

	var logs bytes.Buffer
	job := &Job{
		Measurement: "readings", Tier: "daily", TempDirectory: dir, JobID: "sort-dedup",
		SortKeys: []string{"fleet", "name", "time"}, SortKeysExplicit: true,
		logger: zerolog.New(&logs), db: db,
	}
	output, err := job.compactFiles(ctx, a, dir)
	if err != nil {
		t.Fatalf("dedup compaction with sort keys: %v\nlog:\n%s", err, logs.String())
	}
	var rows int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM read_parquet('%s')`, escapeSQLPath(output))).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 2*20*10 {
		t.Fatalf("dedup output has %d rows, want %d", rows, 2*20*10)
	}
	assertHourKeyTimeOrder(t, ctx, db, output)
}

// TestCompactionSortKeysMixedTimeType: a partition where one file has "time"
// as VARCHAR (a pre-fix writer). The hour expression must bind to the
// normalized TIMESTAMPTZ "time"; on the raw read it is epoch_us(VARCHAR), a
// bind error that would fail the partition every cycle.
func TestCompactionSortKeysMixedTimeType(t *testing.T) {
	db, ctx := openDuckDB(t)
	dir := t.TempDir()
	files := sortKeysFixture(t, ctx, db, dir, 1, 10, 5, "")
	str := filepath.ToSlash(filepath.Join(dir, "str.parquet"))
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`COPY (SELECT 'East' AS fleet, 'truck_00000' AS name, '1451610000000000' AS "time", 0.5 AS velocity) TO '%s' (FORMAT PARQUET)`,
		escapeSQLPath(str))); err != nil {
		t.Fatalf("write varchar fixture: %v", err)
	}
	files = append(files, downloadedFile{localPath: str, storageKey: "db/readings/str.parquet"})

	var logs bytes.Buffer
	job := &Job{
		Measurement: "readings", Tier: "daily", TempDirectory: dir, JobID: "sort-mixed",
		SortKeys: []string{"fleet", "name", "time"}, SortKeysExplicit: true,
		logger: zerolog.New(&logs), db: db,
	}
	output, err := job.compactFiles(ctx, files, dir)
	if err != nil {
		t.Fatalf("mixed time types with sort keys: %v\nlog:\n%s", err, logs.String())
	}
	assertHourKeyTimeOrder(t, ctx, db, output)
}
