//go:build duckdb_arrow

package rehome

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"

	_ "github.com/duckdb/duckdb-go/v2" // duckdb driver
)

func TestParseKey(t *testing.T) {
	for _, tc := range []struct {
		key   string
		shape Shape
		at    string // "" : refused
	}{
		{"db/cpu/2026/11/18/23/cpu_20261118_231000_1.parquet", ShapeHour, "2026-11-18T23:00:00Z"},
		{"db/cpu/2026/11/18/cpu_x_b1_daily.parquet", ShapeDay, "2026-11-18T00:00:00Z"},
		{"db/cpu/1969/12/31/23/f.parquet", ShapeHour, "1969-12-31T23:00:00Z"},
		{"db/cpu/2026/11/18/24/f.parquet", "", ""},
		{"db/cpu/2026/13/18/f.parquet", "", ""},
		{"db/cpu/2026/02/30/f.parquet", "", ""}, // not a calendar day
		{"db/cpu/2026/11/18/23/f.txt", "", ""},
		{"db/cpu/2026/1/18/23/f.parquet", "", ""},
		{"_compaction_state/cpu/2026/11/18/23/f.parquet", "", ""},
		{".tmp/cpu/2026/11/18/23/f.parquet", "", ""},
		{"spoke/db/cpu/2026/11/18/23/f.parquet", "", ""}, // a hub's spoke namespace
		{"db/cpu/2026/11/f.parquet", "", ""},
		{"db/cpu/2026/11/18/23/../f.parquet", "", ""},
		{"/db/cpu/2026/11/18/23/f.parquet", "", ""},
		{"db//2026/11/18/23/f.parquet", "", ""},
		{"", "", ""},
	} {
		src, err := ParseKey(tc.key)
		if tc.at == "" {
			if _, refused := IsRefused(err); !refused {
				t.Errorf("ParseKey(%q) = %+v, %v; want a refusal", tc.key, src, err)
			}
			continue
		}
		if err != nil || src.Shape != tc.shape || src.Partition.Format(time.RFC3339) != tc.at || src.Database != "db" || src.Measurement != "cpu" ||
			src.Dir+"/"+src.File != tc.key {
			t.Errorf("ParseKey(%q) = %+v, %v", tc.key, src, err)
		}
	}
}

func rewriteDB(t *testing.T, dirs ...string) *sql.DB {
	t.Helper()
	spill := t.TempDir()
	db, err := database.OpenRewriteDB(context.Background(), database.RewriteDBOptions{
		MemoryLimitBytes: 512 << 20, SpillDir: spill, AllowedDirs: append([]string{spill}, dirs...),
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// writeSource writes a Parquet file from a SELECT, with Arc's dedup metadata.
func writeSource(t *testing.T, sel string) string {
	t.Helper()
	w, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Exec("SET TimeZone='UTC'"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "time=2031", "it's the source.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`COPY (%s) TO '%s' (FORMAT PARQUET, KV_METADATA {'arc:tags': 'ho''st', 'arc:dedup_exact': 'true', 'other': 'not carried'})`,
		sel, strings.ReplaceAll(path, "'", "''"))
	if _, err := w.Exec(q); err != nil {
		t.Fatal(err)
	}
	return path
}

const misfiledHour = `SELECT * FROM (VALUES
	(TIMESTAMPTZ '2026-11-19 05:00:00Z', 'b', 5, 1.5::FLOAT, 7::SMALLINT),
	(TIMESTAMPTZ '2026-11-18 23:10:00Z', 'a', 1, 2.5::FLOAT, 8::SMALLINT),
	(NULL,                               'n', 9, 3.5::FLOAT, 9::SMALLINT),
	(TIMESTAMPTZ '2026-11-18 23:59:59.999999Z', 'a', 2, 4.5::FLOAT, 1::SMALLINT),
	(TIMESTAMPTZ '1969-12-31 23:30:00Z', 'old', 3, 5.5::FLOAT, 2::SMALLINT),
	(TIMESTAMPTZ 'infinity',             'inf', 4, 6.5::FLOAT, 3::SMALLINT),
	(TIMESTAMPTZ '2026-11-19 05:59:00Z', 'b', 6, 7.5::FLOAT, 4::SMALLINT),
	(TIMESTAMPTZ '2026-11-19 00:00:00Z', 'c', 7, 8.5::FLOAT, 5::SMALLINT)
) v("time", host, n, f, s)`

// A file in an hour directory holding rows of three other hours, one before
// 1970, and two rows with no usable time.
func TestPlanAndSplitAnHourFile(t *testing.T) {
	ctx := context.Background()
	path := writeSource(t, misfiledHour)
	out := t.TempDir()
	db := rewriteDB(t, filepath.Dir(path), out)
	src, err := ParseKey("db/cpu/2026/11/18/23/cpu_20261118_231000_1.parquet")
	if err != nil {
		t.Fatal(err)
	}
	p, err := MakePlan(ctx, db, src, path)
	if err != nil {
		t.Fatal(err)
	}
	type dest struct {
		dir  string
		rows int64
		own  bool
	}
	want := []dest{
		{"db/cpu/1969/12/31/23", 1, false},
		{"db/cpu/2026/11/18/23", 2, true},
		{"db/cpu/2026/11/19/00", 1, false},
		{"db/cpu/2026/11/19/05", 2, false},
	}
	if p.Rows != 8 || p.RemainderRows != 2 || len(p.Destinations) != len(want) || !p.Misfiled() || p.MisfiledRows() != 4 || p.TimeType != "TIMESTAMP WITH TIME ZONE" {
		t.Fatalf("plan = %+v", p)
	}
	for i, w := range want {
		d := p.Destinations[i]
		if d.Dir != w.dir || d.Rows != w.rows || d.Own != w.own {
			t.Errorf("destination %d = %+v, want %+v", i, d, w)
		}
	}

	outs, err := Split(ctx, db, p, path, out)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if len(outs) != 5 || !outs[4].Remainder || outs[4].Dir != src.Dir || outs[4].Rows != 2 {
		t.Fatalf("outputs = %+v", outs)
	}
	// Each output: its rows, in the source's order, with the source's values
	// and types, and Arc's metadata.
	rowsOf := func(p string) string {
		rs, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT host, n, f, s FROM read_parquet('%s')`, p))
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		var got []string
		for rs.Next() {
			var host string
			var n, s int64
			var f float64
			if err := rs.Scan(&host, &n, &f, &s); err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%s/%d/%g/%d", host, n, f, s))
		}
		return strings.Join(got, " ")
	}
	for i, w := range []string{"old/3/5.5/2", "a/1/2.5/8 a/2/4.5/1", "c/7/8.5/5", "b/5/1.5/7 b/6/7.5/4", "n/9/3.5/9 inf/4/6.5/3"} {
		if got := rowsOf(outs[i].LocalPath); got != w {
			t.Errorf("output %d (%s) holds %q, want %q", i, outs[i].Dir, got, w)
		}
		var types string
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT string_agg(column_type, ',') FROM (DESCRIBE SELECT * FROM read_parquet('%s'))`, outs[i].LocalPath)).Scan(&types); err != nil {
			t.Fatal(err)
		}
		if types != "TIMESTAMP WITH TIME ZONE,VARCHAR,INTEGER,FLOAT,SMALLINT" {
			t.Errorf("output %d has column types %s; the split changed a type", i, types)
		}
		// Scanned raw: a cast to VARCHAR would print the quote as an escape.
		krows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT key, value FROM parquet_kv_metadata('%s') ORDER BY key`, outs[i].LocalPath))
		if err != nil {
			t.Fatal(err)
		}
		var pairs []string
		for krows.Next() {
			var k, v []byte
			if err := krows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(string(k), "arc:") || string(k) == "other" {
				pairs = append(pairs, string(k)+"="+string(v))
			}
		}
		krows.Close()
		kv := strings.Join(pairs, ";")
		if kv != "arc:dedup_exact=true;arc:tags=ho'st" {
			t.Errorf("output %d carries metadata %q", i, kv)
		}
	}
	if !outs[1].MinTime.Equal(time.Date(2026, 11, 18, 23, 10, 0, 0, time.UTC)) || outs[1].MaxTime.Nanosecond() != 999999000 || outs[0].Bytes == 0 {
		t.Errorf("output 1 = %+v", outs[1])
	}
	// The pre-1970 row is filed under 1969, where ingest files such a row,
	// not in the first hour of 1970.
	if outs[0].Start.Year() != 1969 || outs[0].Start.Hour() != 23 {
		t.Errorf("the pre-1970 row is filed at %v", outs[0].Start)
	}
}

// A daily file spanning three days splits one file per day.
func TestPlanADayFile(t *testing.T) {
	ctx := context.Background()
	path := writeSource(t, `SELECT * FROM (VALUES
		(TIMESTAMPTZ '2026-10-01 00:00:00Z', 1), (TIMESTAMPTZ '2026-10-01 23:59:59Z', 2),
		(TIMESTAMPTZ '2026-10-02 12:00:00Z', 3), (TIMESTAMPTZ '2026-10-05 00:00:00Z', 4)) v("time", n)`)
	out := t.TempDir()
	db := rewriteDB(t, filepath.Dir(path), out)
	src, _ := ParseKey("db/cpu/2026/10/01/cpu_x_b1_daily.parquet")
	p, err := MakePlan(ctx, db, src, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Destinations) != 3 || p.Destinations[0].Dir != "db/cpu/2026/10/01" || !p.Destinations[0].Own || p.Destinations[0].Rows != 2 ||
		p.Destinations[2].Dir != "db/cpu/2026/10/05" || p.MisfiledRows() != 2 {
		t.Fatalf("plan = %+v", p.Destinations)
	}
	if _, err := Split(ctx, db, p, path, out); err != nil {
		t.Fatalf("Split: %v", err)
	}
	// A file whose rows are all where they belong is not misfiled.
	ok := writeSource(t, `SELECT TIMESTAMPTZ '2026-10-01 05:00:00Z' AS "time", 1 AS n`)
	db2 := rewriteDB(t, filepath.Dir(ok))
	p, err = MakePlan(ctx, db2, src, ok)
	if err != nil || p.Misfiled() || len(p.Destinations) != 1 {
		t.Fatalf("plan of a well-filed file = %+v, %v", p, err)
	}
}

// A naive TIMESTAMP is split in UTC, like a TIMESTAMPTZ.
func TestNaiveTimestampIsSplitInUTC(t *testing.T) {
	ctx := context.Background()
	path := writeSource(t, `SELECT * FROM (VALUES (TIMESTAMP '2026-11-18 23:59:59', 1), (TIMESTAMP '2026-11-19 00:00:00', 2)) v("time", n)`)
	out := t.TempDir()
	db := rewriteDB(t, filepath.Dir(path), out)
	src, _ := ParseKey("db/cpu/2026/11/18/23/f.parquet")
	p, err := MakePlan(ctx, db, src, path)
	if err != nil || len(p.Destinations) != 2 || p.Destinations[1].Dir != "db/cpu/2026/11/19/00" {
		t.Fatalf("plan = %+v, %v", p, err)
	}
	if _, err := Split(ctx, db, p, path, out); err != nil {
		t.Fatalf("Split: %v", err)
	}
}

// A time column that is not a microsecond timestamp is refused: compaction's
// normalisation would let it through and change its values.
func TestTimeTypesThatAreRefused(t *testing.T) {
	ctx := context.Background()
	src, _ := ParseKey("db/cpu/2026/11/18/23/f.parquet")
	for name, sel := range map[string]string{
		"nanoseconds":    `SELECT TIMESTAMP_NS '2026-11-18 23:10:00.123456789' AS "time", 1 AS n`,
		"string":         `SELECT '2026-11-18 23:10:00' AS "time", 1 AS n`,
		"integer":        `SELECT 1795000000::BIGINT AS "time", 1 AS n`,
		"date":           `SELECT DATE '2026-11-18' AS "time", 1 AS n`,
		"no time column": `SELECT 1 AS n`,
	} {
		path := writeSource(t, sel)
		db := rewriteDB(t, filepath.Dir(path))
		if _, err := MakePlan(ctx, db, src, path); err == nil {
			t.Errorf("%s: planned", name)
		} else if _, refused := IsRefused(err); !refused {
			t.Errorf("%s: %v, want a refusal with a reason", name, err)
		}
	}
}

// A timestamp stored in milliseconds or seconds reads as a microsecond
// TIMESTAMP with the same values, so it is split like one.
func TestCoarserTimestampsKeepTheirValues(t *testing.T) {
	ctx := context.Background()
	src, _ := ParseKey("db/cpu/2026/11/18/23/f.parquet")
	for name, sel := range map[string]string{
		"milliseconds": `SELECT * FROM (VALUES (TIMESTAMP_MS '2026-11-18 23:10:00.123', 1), (TIMESTAMP_MS '2026-11-19 05:00:00.456', 2)) v("time", n)`,
		"seconds":      `SELECT * FROM (VALUES (TIMESTAMP_S '2026-11-18 23:10:00', 1), (TIMESTAMP_S '2026-11-19 05:00:00', 2)) v("time", n)`,
	} {
		path := writeSource(t, sel)
		out := t.TempDir()
		db := rewriteDB(t, filepath.Dir(path), out)
		p, err := MakePlan(ctx, db, src, path)
		if err != nil || len(p.Destinations) != 2 {
			t.Fatalf("%s: plan = %+v, %v", name, p, err)
		}
		outs, err := Split(ctx, db, p, path, out)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var before, after int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT sum(epoch_us("time")) FROM read_parquet('%s', hive_partitioning=false)`, strings.ReplaceAll(path, "'", "''"))).Scan(&before); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT sum(epoch_us("time")) FROM read_parquet(['%s', '%s'])`, outs[0].LocalPath, outs[1].LocalPath)).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if before != after {
			t.Errorf("%s: the times changed (%d before, %d after)", name, before, after)
		}
	}
}

// The split refuses to run on a connection that does not read times in UTC:
// date_trunc on a TIMESTAMPTZ follows the session's zone.
func TestPlanNeedsUTC(t *testing.T) {
	path := writeSource(t, `SELECT TIMESTAMPTZ '2026-11-18 23:10:00Z' AS "time", 1 AS n`)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SET GLOBAL TimeZone='Asia/Tokyo'"); err != nil {
		t.Fatal(err)
	}
	src, _ := ParseKey("db/cpu/2026/11/18/23/f.parquet")
	if _, err := MakePlan(context.Background(), db, src, path); err == nil {
		t.Fatal("planned under a session that is not UTC")
	}
}

// Check catches a split that lost a row, wrote a row to the wrong directory,
// or put a time in the remainder.
func TestCheckCatchesABadSplit(t *testing.T) {
	ctx := context.Background()
	path := writeSource(t, misfiledHour)
	out := t.TempDir()
	db := rewriteDB(t, filepath.Dir(path), out)
	src, _ := ParseKey("db/cpu/2026/11/18/23/f.parquet")
	p, err := MakePlan(ctx, db, src, path)
	if err != nil {
		t.Fatal(err)
	}
	good, err := Split(ctx, db, p, path, out)
	if err != nil {
		t.Fatal(err)
	}
	clone := func() []Output { return append([]Output(nil), good...) }

	// The files of two destinations swapped: rows in the wrong directory.
	swapped := clone()
	swapped[2].LocalPath, swapped[3].LocalPath = swapped[3].LocalPath, swapped[2].LocalPath
	if err := Check(ctx, db, p, swapped); err == nil {
		t.Error("two outputs in each other's directories passed")
	}
	// The remainder's file given to a destination, and the reverse.
	mixed := clone()
	mixed[0].LocalPath, mixed[4].LocalPath = mixed[4].LocalPath, mixed[0].LocalPath
	if err := Check(ctx, db, p, mixed); err == nil {
		t.Error("a remainder holding times passed")
	}
	// A destination missing.
	if err := Check(ctx, db, p, clone()[:4]); err == nil {
		t.Error("a split with a file missing passed")
	}
	// A plan that counted one more row than the outputs hold.
	more := p
	more.Rows++
	if err := Check(ctx, db, more, clone()); err == nil {
		t.Error("a split one row short passed")
	}
	// The remainder holding two rows that have times, with every count right.
	timed := clone()
	timed[4].LocalPath = timed[3].LocalPath
	if err := Check(ctx, db, p, timed); err == nil {
		t.Error("a remainder of rows that have times passed, with the right row count")
	}
	// A plan that counted the same total, differently per directory.
	shifted := p
	shifted.Destinations = append([]Destination(nil), p.Destinations...)
	shifted.Destinations[1].Rows, shifted.Destinations[2].Rows = 1, 2
	if err := Check(ctx, db, shifted, clone()); err == nil {
		t.Error("outputs whose rows per directory differ from the plan passed")
	}
	if err := Check(ctx, db, p, clone()); err != nil {
		t.Errorf("the good split failed its check: %v", err)
	}
}
