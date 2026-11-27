// res-3c end-to-end: every new HAVING form the router routes is built into engine
// SQL, EXECUTED by the real engine and shadow-compared against DuckDB.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/arcxengine"
)

func TestHavingExprEndToEnd(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	base := t.TempDir()
	dir := filepath.Join(base, "prod", "cpu", "2026", "02", "03", "10")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "a.parquet")
	// Six hosts with different row counts; x/y with NULLs; ok all-TRUE for h0,
	// all-NULL for h5; i (BIGINT) spans negatives; code a small BIGINT.
	if _, err := db.Exec(`COPY (
		SELECT TIMESTAMPTZ '2026-02-03 10:00:00+00' + to_seconds(r) AS time,
		       'h' || k AS host,
		       CASE WHEN r % 13 = 0 THEN NULL ELSE (r % 9) * 0.75 + k END::DOUBLE AS x,
		       CASE WHEN r % 17 = 0 THEN NULL ELSE (r % 5) + 0.5 * k END::DOUBLE AS y,
		       CASE WHEN r % 11 = 0 THEN NULL ELSE (r % 9) - 4 + k END::BIGINT AS i,
		       (r % 7)::BIGINT AS code,
		       CASE WHEN k = 0 THEN true WHEN k = 5 THEN NULL ELSE r % (k + 2) <> 0 END AS ok
		FROM (SELECT k, unnest(range(100 + 7 * k)) AS r FROM range(6) t(k))
		) TO '` + file + `' (FORMAT parquet)`); err != nil {
		t.Fatal(err)
	}
	deps := newLocalDeps(t, base)
	deps.DB = db
	deps.AllowedDirs = []string{base + "/"}
	ctx := context.Background()
	const b = "SELECT date_trunc('minute', time) AS t, "
	for _, q := range []string{
		// Arithmetic: BIGINT, HUGEINT (integer sum, count_if), DOUBLE; decimal and
		// scientific literals; hidden aggregates inside expressions.
		"SELECT host FROM cpu GROUP BY host HAVING sum(i) / count(*) > 1",
		"SELECT host, max(x), min(x) FROM cpu GROUP BY host HAVING max(x) - min(x) > 6.5",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) * 1.5 >= 160.5",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING -sum(i) < -100",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING (count(*) + 1) * 2 BETWEEN 200 AND 240",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1.1e2",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*)-1 > 110",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING min(i) -2 * max(i) > -20",
		b + "count(*) FROM cpu GROUP BY 1 HAVING count_if(i > 0) * 2 > count(*)",
		"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) * 2 > 9",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING sum(x) / count(*) > 4",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) / 0 > 1e308",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING sum(code) / count(*) IS NOT NULL",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 > max(i) * 12",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT (count(*) + 1 > 115) AND (max(i) + 1 > 5)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) * 2 IN (200, 214, 228.0)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) BETWEEN 107 AND 1e20",
		// The footer shape (metadata-only count per hour bucket) takes the same grammar.
		"SELECT date_trunc('hour', time) AS t, count(*) FROM cpu GROUP BY 1 HAVING count(*) * 2 > 1000",
		"SELECT date_trunc('hour', time) AS t, count(*) FROM cpu GROUP BY 1 HAVING count(*) - 1 IS DISTINCT FROM 1e3",
		b + "count(*), max(i) FROM cpu GROUP BY 1 HAVING count(*) * 2 > 20",
		// An integer sum (HUGEINT) in DOUBLE arithmetic, through DuckDB's own cast.
		"SELECT host, sum(i) FROM cpu GROUP BY host HAVING sum(i) * 1e0 > 2.5e2",
		// IS [NOT] DISTINCT FROM, IS [NOT] TRUE / FALSE, [NOT] LIKE.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(i) IS DISTINCT FROM 5",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(i) IS NOT DISTINCT FROM NULL",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IS DISTINCT FROM max(i)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(i) + 1 IS NOT DISTINCT FROM min(i) + 9",
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) IS NOT TRUE",
		b + "count(*) FROM cpu GROUP BY 1 HAVING bool_or(ok) IS FALSE",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host LIKE 'h%'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host NOT LIKE '_1' AND count(*) * 2 > 200",
	} {
		d := Decide(q, "prod", deps)
		if !d.Eligible {
			t.Fatalf("not routed: %s", q)
		}
		esql, ok := deps.buildEngineSQL(ctx, d)
		if !ok {
			t.Fatalf("buildEngineSQL declined: %s", q)
		}
		rec, err := arcxengine.Query(esql, d.Ctx)
		if err != nil {
			t.Fatalf("engine did not serve a routed HAVING (%v):\n user %s\n engine %s", err, q, esql)
		}
		deps.ConvertedSQL = strings.Replace(q, "FROM cpu", "FROM read_parquet('"+file+"')", 1)
		diff, err := deps.compareToOracle(ctx, d, rec)
		rec.Release()
		if err != nil {
			t.Fatalf("oracle: %v\n%s", err, q)
		}
		if diff != "" && !strings.HasPrefix(diff, "oraclediff:") && !strings.HasPrefix(diff, "argdiff:") {
			t.Fatalf("shadow mismatch %q\n user %s\n engine %s", diff, q, esql)
		}
	}
}
