//go:build cgo && arcx_engine

package arcxengine

// Arc-via-arcx vs Arc-via-DuckDB comparison harness — engine vs engine, both
// in-process, no CLI process startup.
//
// The router that would route Arc's HTTP queries to arcx isn't built yet, so this
// drives BOTH engines directly from Go on identical queries:
//   - arcx via the FFI bridge (Query).
//   - DuckDB via Arc's EMBEDDED engine: the same github.com/duckdb/duckdb-go/v2
//     driver Arc's binary uses, opened once with a persistent pinned connection
//     (like Arc's warm pool), queried via the same conn.Raw → NewArrowFromConn
//     native-Arrow hot path Arc uses in production. HTTP/RBAC/transform overhead is
//     identical for both engines and so excluded — it wouldn't change the delta.
//
// Run:
//   ARCX_COMPARE=1 CGO_ENABLED=1 go test -tags=duckdb_arrow,arcx_engine \
//     -run TestCompareArcxVsDuckDB -v ./internal/arcxengine/
//   ARCX_COLD=production/mem CGO_ENABLED=1 go test -tags=duckdb_arrow,arcx_engine \
//     -run TestCompareColdWarmHot -v ./internal/arcxengine/   (after `sudo purge`)

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

const arcDataRoot = "/Users/nacho/dev/basekick-labs/arc/data/arc"

// --- embedded DuckDB (Arc's real in-process engine) ------------------------

// embeddedDuckDB holds a warm, in-process DuckDB — Arc's exact driver, one pinned
// connection reused across queries (like Arc's pool). icu is loaded so
// date_trunc(TIMESTAMPTZ) respects the UTC session, matching arcx's UTC buckets.
type embeddedDuckDB struct {
	db   *sql.DB
	conn *sql.Conn
}

func openEmbeddedDuckDB(ctx context.Context) (*embeddedDuckDB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, s := range []string{"INSTALL icu", "LOAD icu", "SET TimeZone='UTC'"} {
		if _, err := conn.ExecContext(ctx, s); err != nil && strings.Contains(s, "TimeZone") {
			conn.Close()
			db.Close()
			return nil, fmt.Errorf("%s: %w", s, err)
		}
	}
	return &embeddedDuckDB{db: db, conn: conn}, nil
}

func (e *embeddedDuckDB) close() {
	if e.conn != nil {
		e.conn.Close()
	}
	if e.db != nil {
		e.db.Close()
	}
}

// query runs sql on the warm connection via the native Arrow hot path (the same
// conn.Raw → NewArrowFromConn path Arc uses in production), returns normalized
// rows + latency in ms.
func (e *embeddedDuckDB) query(ctx context.Context, sqlText string) ([]string, float64, error) {
	start := time.Now()
	var rows []string
	err := e.conn.Raw(func(dc any) error {
		conn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("driver conn does not implement driver.Conn")
		}
		ar, err := duckdb.NewArrowFromConn(conn)
		if err != nil {
			return err
		}
		rdr, err := ar.QueryContext(ctx, sqlText)
		if err != nil {
			return err
		}
		defer rdr.Release()
		for rdr.Next() {
			rows = append(rows, recordRows(rdr.Record())...)
		}
		return rdr.Err()
	})
	ms := float64(time.Since(start).Microseconds()) / 1000.0
	return rows, ms, err
}

// --- shapes ----------------------------------------------------------------

type shape struct {
	name    string
	arcxSQL string // arcx gets an explicit path array
	duckSQL string // duckdb gets the glob
	ctx     Context
}

func shapesFor(db, m, arr, glob string) []shape {
	return []shape{
		{"count(*)",
			"SELECT count(*) FROM read_parquet(" + arr + ")",
			"SELECT count(*) FROM read_parquet('" + glob + "')",
			Context{Database: db, Measurement: m}},
		{"date_trunc(day) agg",
			"SELECT date_trunc('day', time), count(*) FROM read_parquet(" + arr + ") GROUP BY 1 ORDER BY 1",
			"SELECT date_trunc('day', time), count(*) FROM read_parquet('" + glob + "') GROUP BY 1 ORDER BY 1",
			Context{Database: db, Measurement: m, TimeColumn: "time"}},
	}
}

// --- warm best-of-N comparison across all production measurements ----------

func TestCompareArcxVsDuckDB(t *testing.T) {
	if os.Getenv("ARCX_COMPARE") == "" {
		t.Skip("set ARCX_COMPARE=1 to run the arcx-vs-DuckDB comparison")
	}
	ctx := context.Background()
	duck, err := openEmbeddedDuckDB(ctx)
	if err != nil {
		t.Fatalf("open embedded duckdb: %v", err)
	}
	defer duck.close()

	measurements := []struct{ db, m string }{
		{"production", "cpu"}, {"production", "mem"},
		{"production", "net"}, {"production", "disk"},
	}

	fmt.Println("\n========= Arc-via-arcx vs Arc-via-EMBEDDED-DuckDB (both in-process) =========")
	fmt.Printf("%-12s %-22s %12s %12s %10s  %s\n", "measurement", "shape", "arcx", "duckdb", "speedup", "values")
	fmt.Println(strings.Repeat("-", 84))

	for _, meas := range measurements {
		dir := filepath.Join(arcDataRoot, meas.db, meas.m)
		files := globParquet(dir)
		if len(files) == 0 {
			continue
		}
		arr, glob := pathArray(files), dir+"/**/*.parquet"
		for _, sh := range shapesFor(meas.db, meas.m, arr, glob) {
			aRows, aMs, aErr := bestArcx(sh.arcxSQL, sh.ctx, 5)
			if aErr != nil {
				fmt.Printf("%-12s %-22s  arcx: %v\n", meas.m, sh.name, aErr)
				continue
			}
			dRows, dMs, dErr := bestDuck(ctx, duck, sh.duckSQL, 5)
			if dErr != nil {
				fmt.Printf("%-12s %-22s  duckdb: %v\n", meas.m, sh.name, dErr)
				continue
			}
			match := "MATCH"
			if !rowsEqual(aRows, dRows) {
				match = fmt.Sprintf("MISMATCH (arcx %d / duck %d)", len(aRows), len(dRows))
			}
			fmt.Printf("%-12s %-22s %10.1fms %10.1fms %10s  %s\n",
				meas.m, sh.name, aMs, dMs, speedup(aMs, dMs), match)
		}
	}
	fmt.Println(strings.Repeat("-", 84))
}

// --- cold / warm / hot on one measurement ----------------------------------

func TestCompareColdWarmHot(t *testing.T) {
	spec := os.Getenv("ARCX_COLD")
	if spec == "" {
		t.Skip("set ARCX_COLD=<db>/<measurement> (e.g. production/mem) to run cold/warm/hot")
	}
	parts := strings.SplitN(spec, "/", 2)
	if len(parts) != 2 {
		t.Fatalf("ARCX_COLD must be <db>/<measurement>, got %q", spec)
	}
	db, m := parts[0], parts[1]
	dir := filepath.Join(arcDataRoot, db, m)
	files := globParquet(dir)
	if len(files) == 0 {
		t.Fatalf("no parquet files under %s", dir)
	}
	arr, glob := pathArray(files), dir+"/**/*.parquet"

	ctx := context.Background()
	duck, err := openEmbeddedDuckDB(ctx)
	if err != nil {
		t.Fatalf("open embedded duckdb: %v", err)
	}
	defer duck.close()

	labels := []string{"COLD", "warm", "hot "}
	fmt.Printf("\n======== cold/warm/hot: %s/%s (%d files), embedded DuckDB ========\n", db, m, len(files))
	fmt.Printf("%-22s %-8s %12s %12s %10s\n", "shape", "temp", "arcx", "duckdb", "speedup")
	fmt.Println(strings.Repeat("-", 68))

	for _, sh := range shapesFor(db, m, arr, glob) {
		for i, label := range labels {
			aRows, aMs, aErr := runOnce(sh.arcxSQL, sh.ctx)
			dRows, dMs, dErr := duck.query(ctx, sh.duckSQL)
			if aErr != nil || dErr != nil {
				fmt.Printf("%-22s %-8s  arcx=%v duck=%v\n", sh.name, label, aErr, dErr)
				continue
			}
			match := ""
			if i == 0 && !rowsEqual(aRows, dRows) {
				match = "  MISMATCH"
			}
			fmt.Printf("%-22s %-8s %10.1fms %10.1fms %10s%s\n",
				sh.name, label, aMs, dMs, speedup(aMs, dMs), match)
		}
	}
	fmt.Println(strings.Repeat("-", 68))
}

// --- runners ---------------------------------------------------------------

func bestArcx(sql string, ctx Context, n int) ([]string, float64, error) {
	var rows []string
	best := 1e18
	for i := 0; i < n; i++ {
		r, ms, err := runOnce(sql, ctx)
		if err != nil {
			return nil, 0, err
		}
		if i == 0 {
			rows = r
		}
		if ms < best {
			best = ms
		}
	}
	return rows, best, nil
}

func bestDuck(ctx context.Context, duck *embeddedDuckDB, sql string, n int) ([]string, float64, error) {
	var rows []string
	best := 1e18
	for i := 0; i < n; i++ {
		r, ms, err := duck.query(ctx, sql)
		if err != nil {
			return nil, 0, err
		}
		if i == 0 {
			rows = r
		}
		if ms < best {
			best = ms
		}
	}
	return rows, best, nil
}

func runOnce(sql string, ctx Context) ([]string, float64, error) {
	start := time.Now()
	rec, err := Query(sql, ctx)
	ms := float64(time.Since(start).Microseconds()) / 1000.0
	if err != nil {
		return nil, 0, err
	}
	rows := recordRows(rec)
	rec.Release()
	return rows, ms, nil
}

func speedup(arcxMs, duckMs float64) string {
	if arcxMs <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.1fx", duckMs/arcxMs)
}

// --- normalization (order-independent value compare) -----------------------

// recordRows normalizes an arrow.Record into "col=val;..." strings.
func recordRows(rec arrow.Record) []string {
	n := int(rec.NumRows())
	rows := make([]string, 0, n)
	for i := 0; i < n; i++ {
		m := make(map[string]any, int(rec.NumCols()))
		for c := 0; c < int(rec.NumCols()); c++ {
			m[rec.ColumnName(c)] = cellValue(rec.Column(c), i)
		}
		rows = append(rows, normalizeRow(m))
	}
	return rows
}

func cellValue(col arrow.Array, row int) any {
	switch a := col.(type) {
	case *array.Int64:
		return a.Value(row)
	case *array.Timestamp:
		return time.UnixMicro(int64(a.Value(row))).UTC().Format("2006-01-02 15:04:05")
	default:
		return col.ValueStr(row)
	}
}

func normalizeRow(m map[string]any) string {
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s=%v", canonCol(k), canonVal(v)))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// canonCol collapses the known-cosmetic column-name differences so a keyword-
// quoting nuance can't cause a false value mismatch.
func canonCol(c string) string {
	switch {
	case strings.HasPrefix(c, "date_trunc("):
		return "bucket"
	case strings.HasPrefix(c, "count"):
		return "count"
	default:
		return c
	}
}

func canonVal(v any) string {
	switch x := v.(type) {
	case int64:
		return fmt.Sprintf("%d", x)
	case string:
		return strings.TrimSpace(strings.TrimSuffix(x, "+00"))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func rowsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- fixtures --------------------------------------------------------------

func globParquet(dir string) []string {
	var out []string
	_ = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			out = append(out, p)
		}
		return nil
	})
	sort.Strings(out)
	return out
}

func pathArray(files []string) string {
	q := make([]string, len(files))
	for i, f := range files {
		q[i] = "'" + f + "'"
	}
	return "[" + strings.Join(q, ",") + "]"
}
