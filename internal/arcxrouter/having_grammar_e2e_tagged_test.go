// res-3b end-to-end: every new HAVING form the router routes is built into engine
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

func TestHavingGrammarEndToEnd(t *testing.T) {
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
		// Hidden aggregates.
		"SELECT host FROM cpu GROUP BY host HAVING count(*) > 113.5",
		"SELECT host FROM cpu GROUP BY host HAVING count(*) > 100 AND count(*) < 130 ORDER BY 1",
		"SELECT host, avg(x) FROM cpu GROUP BY host HAVING max(i) > 5 OR sum(i) < 0",
		"SELECT host, count(*) AS n FROM cpu GROUP BY host HAVING min(i) < -2 ORDER BY n DESC LIMIT 3",
		"SELECT date_trunc('minute', time) AS t FROM cpu GROUP BY 1 HAVING stddev(x) > 2 ORDER BY 1",
		b + "count(*) FROM cpu GROUP BY 1 HAVING median(x) > 3 AND quantile_cont(x, 0.9) < 9",
		b + "count(*) FROM cpu GROUP BY 1 HAVING count_if(x > 1) > 20",
		b + "count(*) FROM cpu GROUP BY 1 HAVING skewness(x) > -10",
		"SELECT host FROM cpu GROUP BY host HAVING arg_max(x, time) > 1 ORDER BY 1",
		b + "count(*) FROM cpu GROUP BY 1 HAVING NOT bool_and(ok)",
		// NOT, IS NULL, BETWEEN, IN, literal-left.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT count(*) > 114",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT (count(*) > 120 OR NOT host = 'h1')",
		"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) IS NOT NULL AND max(i) IS NOT NULL AND host IS NOT NULL",
		b + "count(*) FROM cpu GROUP BY 1 HAVING bool_and(ok) IS NULL OR count(*) > 59",
		"SELECT date_trunc('hour', time) AS t, count(*) FROM cpu GROUP BY 1 HAVING t IS NOT NULL AND NOT count(*) < 600 AND count(*) IN (600, 1000) AND 5 < count(*)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) BETWEEN 107 AND 121.5",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) NOT BETWEEN 107 AND 121",
		b + "count(*), sum(i) FROM cpu GROUP BY 1 HAVING t BETWEEN '2026-02-03T10:02:00Z' AND '2026-02-03T10:05:00Z'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IN (100, 114.5, 121)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host NOT IN ('h1', 'h3')",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING 114 <= count(*)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING -5 <= sum(i)",
		b + "bool_or(ok) FROM cpu GROUP BY 1 HAVING TRUE = bool_or(ok)",
		// Reference vs reference.
		"SELECT host, count(*), max(i) FROM cpu GROUP BY host HAVING count(*) > max(i)",
		"SELECT host FROM cpu GROUP BY host HAVING max(x) > min(x)",
		b + "regr_count(y, x), count(*) FROM cpu GROUP BY 1 HAVING regr_count(y, x) < count(*)",
		b + "bool_and(ok), bool_or(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) <> bool_or(ok)",
		b + "max(time) FROM cpu GROUP BY 1 HAVING t < max(time)",
		b + "count(*) FROM cpu GROUP BY 1 HAVING quantile_disc(code, 0.5) >= min(code)",
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
