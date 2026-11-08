// agg-6a companion: the recognizer accepts the unary moment aggregates
// (var_pop/var_samp/variance, stddev/stddev_samp/stddev_pop, sem) and the `mean`
// alias exactly where the engine serves them (ungrouped, grouped, HAVING), and
// keeps declining what the engine does not build (
// the binary covariance/correlation family, DISTINCT/expression arguments). Pure
// tokenizer tests — no engine, no cgo.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestScanAggMoments(t *testing.T) {
	cases := []struct {
		sql   string
		items []string
	}{
		{"SELECT stddev(cpu_user) FROM cpu", []string{"stddev(cpu_user)"}},
		{"SELECT avg(v), stddev(v), var_pop(v), sem(v) FROM cpu WHERE host = 'a'",
			[]string{"avg(v)", "stddev(v)", "var_pop(v)", "sem(v)"}},
		// Function name lowercased, argument spelling preserved (the derived name).
		{"SELECT VARIANCE(X), Stddev_Samp(x), MEAN(y), Var_Samp(x), STDDEV_POP(x) FROM cpu",
			[]string{"variance(X)", "stddev_samp(x)", "mean(y)", "var_samp(x)", "stddev_pop(x)"}},
		{"SELECT count(*), mean(v) FROM cpu WHERE v > 50", []string{"count(*)", "mean(v)"}},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if m.shape != ShapeScanAgg {
			t.Fatalf("shape = %q, want scan_agg: %s", m.shape, c.sql)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) {
			t.Fatalf("items = %v, want %v: %s", m.aggItems, c.items, c.sql)
		}
	}
}

func TestGroupedAggMoments(t *testing.T) {
	cases := []struct {
		sql   string
		items []string
	}{
		// Bucket-keyed moments route (engine folds bucket runs: 6.1 s vs 8.9 s).
		{"SELECT date_trunc('hour', time), var_samp(value) FROM cpu GROUP BY 1",
			[]string{"date_trunc('hour', time)", "var_samp(value)"}},
		{"SELECT date_trunc('minute', time), count(*), stddev(v), mean(v) FROM cpu WHERE v > 1 GROUP BY 1",
			[]string{"date_trunc('minute', time)", "count(*)", "stddev(v)", "mean(v)"}},
		// NOTE: a FLOAT literal in HAVING (`> 0.5`) declines today for every
		// aggregate, sum included — a pre-existing recognizer limit, not agg-6a's.
		{"SELECT date_trunc('hour', time), count(*), stddev(v) FROM cpu GROUP BY 1 HAVING stddev(v) > 1",
			[]string{"date_trunc('hour', time)", "count(*)", "stddev(v)"}},
		// mean is avg: tag-keyed is fine (same state as avg).
		{"SELECT host, mean(v), count(*) FROM cpu WHERE v > 1 GROUP BY host HAVING count(*) > 1 ORDER BY host",
			[]string{"host", "mean(v)", "count(*)"}},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if m.shape != ShapeScanAggGrouped {
			t.Fatalf("shape = %q, want scan_agg_grouped: %s", m.shape, c.sql)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) {
			t.Fatalf("items = %v, want %v: %s", m.aggItems, c.items, c.sql)
		}
	}
}

func TestMomentsDecline(t *testing.T) {
	for _, sql := range []string{
		// tag-keyed shape statistics stay gated (agg-8)
		"SELECT host, skewness(v) FROM cpu GROUP BY host",
		// argument shapes the engine declines
		"SELECT stddev(DISTINCT v) FROM cpu",
		"SELECT stddev(v + 1) FROM cpu",
		"SELECT stddev(*) FROM cpu",
		"SELECT stddev(v, w) FROM cpu",
		"SELECT stddev(m['k']) FROM cpu",
		"SELECT stddev() FROM cpu",
		// a HAVING aggregate absent from the select list
		"SELECT date_trunc('hour', time), count(*) FROM cpu GROUP BY 1 HAVING stddev(v) > 1",
		// tag-keyed moments: a measured 2.7× loss (18.3 s vs 6.7 s by host) — held
		"SELECT host, stddev(cpu_user), var_pop(cpu_idle) FROM cpu GROUP BY host",
		"SELECT host, count(*), stddev(v) FROM cpu GROUP BY host HAVING stddev(v) > 1",
		"SELECT date_trunc('hour', time), host, stddev(v) FROM cpu GROUP BY 1, host",
		"SELECT host, sem(v) FROM cpu WHERE v > 1 GROUP BY host ORDER BY host",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline (%s): %s", m.shape, sql)
		}
	}
}
