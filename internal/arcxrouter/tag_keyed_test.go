// C2 companion: every aggregate family the agg-6a tag-key gate used to refuse
// routes tag-keyed, under tag + bucket keys, and as a hidden HAVING aggregate under
// a tag key (arcX docs/2026-11-13-c2-tag-keyed-families.md; engine gate:
// tests/tag_keyed_families_differential.rs). What declines for its own reason still
// declines with a tag key. Pure tokenizer tests.

package arcxrouter

import "testing"

func TestTagKeyedFamiliesRoute(t *testing.T) {
	if len(tagGatedFns) != 0 {
		t.Fatalf("tagGatedFns should be empty since C2: %v", tagGatedFns)
	}
	for _, sql := range []string{
		// moments
		"SELECT host, stddev(cpu_user), var_pop(cpu_idle) FROM cpu GROUP BY host",
		"SELECT host, var_samp(v), variance(v), stddev_samp(v), stddev_pop(v), sem(v) FROM cpu GROUP BY host",
		"SELECT host, sem(v) FROM cpu WHERE v > 1 GROUP BY host ORDER BY host",
		// shape statistics
		"SELECT host, skewness(v), kurtosis(v), kurtosis_pop(v) FROM cpu GROUP BY host",
		// binary
		"SELECT host, corr(cpu_user, cpu_idle) FROM cpu GROUP BY host",
		"SELECT host, regr_slope(y, x), count(*) FROM cpu GROUP BY host",
		"SELECT host, covar_pop(y, x), covar_samp(y, x), regr_intercept(y, x), regr_r2(y, x), regr_count(y, x) FROM cpu GROUP BY host",
		"SELECT host, regr_avgx(y, x), regr_avgy(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x) FROM cpu GROUP BY host",
		// numeric
		"SELECT host, fsum(f), count_if(b) FROM m GROUP BY host",
		"SELECT host, sumkahan(f), kahan_sum(f), favg(f) FROM m GROUP BY host",
		"SELECT host, bit_and(i), bit_or(i), bit_xor(i), bool_and(b), bool_or(b), countif(b) FROM m GROUP BY host",
		// predicate arguments
		"SELECT host, count_if(x > 0) FROM t GROUP BY host",
		// order statistics
		"SELECT host, median(x), mad(x) FROM t GROUP BY host",
		"SELECT host, quantile_cont(x, 0.9), quantile_disc(x, 0.25), quantile(x, 0.5) FROM t GROUP BY host",
		// tag + bucket
		"SELECT date_trunc('hour', time), host, stddev(v) FROM cpu GROUP BY 1, host",
		"SELECT date_trunc('hour', time), host, covar_pop(y, x) FROM cpu GROUP BY 1, host",
		"SELECT date_trunc('hour', time), host, count_if(b) FROM m GROUP BY 1, host",
		"SELECT date_trunc('hour', time), host, median(x) FROM t GROUP BY 1, host",
		// hidden HAVING aggregates under a tag key
		"SELECT host, count(*), stddev(v) FROM cpu GROUP BY host HAVING stddev(v) > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING stddev(x) > 1",
		"SELECT host FROM cpu GROUP BY host HAVING median(x) > 1",
		"SELECT host FROM cpu GROUP BY host HAVING corr(y, x) > 0",
		"SELECT host FROM cpu GROUP BY host HAVING count_if(x > 1) > 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING corr(y, x) < 0.5 AND fsum(x) > 0",
	} {
		m, ok := eligibleShape(sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should route grouped (ok=%v shape=%v): %s", ok, m.shape, sql)
		}
	}
}

func TestTagKeyedStillDeclines(t *testing.T) {
	for _, sql := range []string{
		// grouped mode: refused for its own reason (tie choice differs across DuckDB versions)
		"SELECT host, mode(code) FROM t GROUP BY host",
		// refused ORDER BY targets (shape / tolerant / binary items) hold under a tag key
		"SELECT host, skewness(v) FROM cpu GROUP BY host ORDER BY 2",
		"SELECT host, covar_pop(y, x) FROM cpu GROUP BY host ORDER BY 2",
		"SELECT host, fsum(f) FROM m GROUP BY host ORDER BY 2 DESC LIMIT 3",
		// argument shapes the engine declines
		"SELECT host, stddev(v + 1) FROM cpu GROUP BY host",
		"SELECT host, median(DISTINCT x) FROM t GROUP BY host",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline (%s): %s", m.shape, sql)
		}
	}
}
