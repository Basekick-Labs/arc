// agg-6b companion: the fn(y, x) covariance / correlation / regression family
// routes ungrouped and under a date_trunc bucket key; a tag key with any of them
// declines (the hasMoment gate, set in the two-argument arm). Pure tokenizer
// tests — no engine, no cgo.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestBinaryMomentsRecognized(t *testing.T) {
	scan := []struct {
		sql   string
		items []string
	}{
		{"SELECT corr(cpu_user, cpu_idle) FROM cpu", []string{"corr(cpu_user, cpu_idle)"}},
		{"SELECT COVAR_SAMP(Y, X), Regr_Count(y, x), regr_r2(y, x) FROM cpu WHERE host = 'a'",
			[]string{"covar_samp(Y, X)", "regr_count(y, x)", "regr_r2(y, x)"}},
		{"SELECT count(*), regr_slope(y, x), regr_intercept(y, x), regr_avgx(y, x), regr_avgy(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x), covar_pop(y, x) FROM cpu",
			[]string{"count(*)", "regr_slope(y, x)", "regr_intercept(y, x)", "regr_avgx(y, x)", "regr_avgy(y, x)", "regr_sxx(y, x)", "regr_syy(y, x)", "regr_sxy(y, x)", "covar_pop(y, x)"}},
	}
	for _, c := range scan {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAgg {
			t.Fatalf("should be scan_agg: %s (got %q, %t)", c.sql, m.shape, ok)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) {
			t.Fatalf("items = %v, want %v", m.aggItems, c.items)
		}
	}
	grouped := []struct {
		sql   string
		items []string
	}{
		{"SELECT date_trunc('hour', time), corr(cpu_user, value) FROM cpu GROUP BY 1",
			[]string{"date_trunc('hour', time)", "corr(cpu_user, value)"}},
		{"SELECT date_trunc('hour', time), count(*), regr_slope(y, x) FROM cpu GROUP BY 1 HAVING regr_slope(y, x) > 1",
			[]string{"date_trunc('hour', time)", "count(*)", "regr_slope(y, x)"}},
	}
	for _, c := range grouped {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should be scan_agg_grouped: %s (got %q, %t)", c.sql, m.shape, ok)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) {
			t.Fatalf("items = %v, want %v", m.aggItems, c.items)
		}
	}
}

func TestBinaryMomentsDecline(t *testing.T) {
	for _, sql := range []string{
		// tag-keyed: the measured loss class — gated (plan-adversarial M2)
		"SELECT host, corr(cpu_user, cpu_idle) FROM cpu GROUP BY host",
		"SELECT host, regr_slope(y, x), count(*) FROM cpu GROUP BY host",
		"SELECT date_trunc('hour', time), host, covar_pop(y, x) FROM cpu GROUP BY 1, host",
		// argument shapes the engine declines
		"SELECT corr(y) FROM cpu",
		"SELECT corr(y, x, z) FROM cpu",
		"SELECT corr(DISTINCT y, x) FROM cpu",
		"SELECT corr(y + 1, x) FROM cpu",
		"SELECT corr(y, 1) FROM cpu",
		"SELECT corr(m['a'], x) FROM cpu",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline (%s): %s", m.shape, sql)
		}
	}
}
