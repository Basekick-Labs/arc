// res-3 end-to-end: every HAVING form the router now routes is built into engine
// SQL, EXECUTED by the real engine (a routed-then-declined shape is a wasted DuckDB
// re-run, gotcha #4) and shadow-compared against DuckDB on the same parquet.

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

func TestHavingAllAggsEndToEnd(t *testing.T) {
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
	// all-NULL for h5; i spans negatives.
	if _, err := db.Exec(`COPY (
		SELECT TIMESTAMPTZ '2026-02-03 10:00:00+00' + to_seconds(r) AS time,
		       'h' || k AS host,
		       CASE WHEN r % 13 = 0 THEN NULL ELSE (r % 9) * 0.75 + k END::DOUBLE AS x,
		       CASE WHEN r % 17 = 0 THEN NULL ELSE (r % 5) + 0.5 * k END::DOUBLE AS y,
		       CASE WHEN r % 11 = 0 THEN NULL ELSE (r % 9) - 4 + k END::BIGINT AS i,
		       CASE WHEN k = 0 THEN true WHEN k = 5 THEN NULL ELSE r % (k + 2) <> 0 END AS ok
		FROM (SELECT k, unnest(range(100 + 7 * k)) AS r FROM range(6) t(k))
		) TO '` + file + `' (FORMAT parquet)`); err != nil {
		t.Fatal(err)
	}
	deps := newLocalDeps(t, base)
	deps.DB = db
	deps.AllowedDirs = []string{base + "/"}
	ctx := context.Background()
	const b = "SELECT date_trunc('hour', time) AS t, "
	for _, q := range []string{
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 113.5 ORDER BY 1",
		"SELECT host, count(*) AS n FROM cpu GROUP BY host HAVING n >= 107 AND n < 128.5",
		"SELECT host, max(x) AS x FROM cpu GROUP BY host HAVING x > 7",
		"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) <= 5.25",
		"SELECT host, count(*) AS n FROM cpu GROUP BY host HAVING n > 100 ORDER BY n DESC LIMIT 3",
		b + "count(*) AS n, sum(i) AS s FROM cpu GROUP BY 1 HAVING n > 2 AND s <> 3.5",
		b + "regr_count(y, x) AS rc FROM cpu GROUP BY 1 HAVING rc >= 99.5",
		b + "median(x) FROM cpu GROUP BY 1 HAVING median(x) > 3",
		b + "quantile_cont(x, 0.9) FROM cpu GROUP BY 1 HAVING quantile_cont(x, 0.9) > 5.5",
		b + "quantile_disc(i, 0.5) AS q FROM cpu GROUP BY 1 HAVING q >= 0.5",
		b + "mad(x) FROM cpu GROUP BY 1 HAVING mad(x) < 3 ORDER BY t",
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok)",
		b + "bool_or(ok) AS bo FROM cpu GROUP BY 1 HAVING bo <> FALSE OR (bo)",
		b + "bool_and(ok), count(*) FROM cpu GROUP BY 1 HAVING bool_and(ok) = false AND count(*) > 2 ORDER BY 1",
		b + "count_if(x > 1) FROM cpu GROUP BY 1 HAVING count_if(x>1) > 2",
		b + "bool_and(x > 1.5) FROM cpu GROUP BY 1 HAVING bool_and(x > 1.5) = false",
		b + "skewness(x) FROM cpu GROUP BY 1 HAVING skewness(x) > -10",
		b + "kurtosis(x) AS k FROM cpu GROUP BY 1 HAVING k < 3 ORDER BY t DESC",
		b + "fsum(x) FROM cpu GROUP BY 1 HAVING fsum(x) > 0",
		b + "covar_pop(y, x) FROM cpu GROUP BY 1 HAVING covar_pop(y, x) > -100",
		"SELECT host, arg_max(x, time) FROM cpu GROUP BY host HAVING arg_max(x, time) > 1 ORDER BY 1",
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
