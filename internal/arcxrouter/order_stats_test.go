// agg-7 companion: the recognizer routes the order statistics exactly where the
// engine serves them and the perf gate measured a win — ungrouped and under a time
// bucket; tag-keyed grouping, grouped mode and HAVING stay on DuckDB. Pure
// tokenizer tests.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestScanAggOrderStats(t *testing.T) {
	cases := []struct {
		sql   string
		items []string
	}{
		{"SELECT median(cpu_user) FROM cpu", []string{"median(cpu_user)"}},
		{"SELECT MEDIAN(X), Quantile_Cont(x, 0.99), quantile_disc(code, 0.5), QUANTILE(code, 1), mad(v), mode(code) FROM cpu WHERE host = 'a'",
			[]string{"median(X)", "quantile_cont(x, 0.99)", "quantile_disc(code, 0.5)", "quantile(code, 1)", "mad(v)", "mode(code)"}},
		{"SELECT quantile_cont(v, 0), quantile_cont(v, 1.00), quantile_disc(v, 0.0001) FROM cpu",
			[]string{"quantile_cont(v, 0)", "quantile_cont(v, 1.00)", "quantile_disc(v, 0.0001)"}},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) {
			t.Fatalf("items = %v, want %v: %s", m.aggItems, c.items, c.sql)
		}
	}
}

func TestGroupedAggOrderStats(t *testing.T) {
	sql := "SELECT date_trunc('hour', time), median(cpu_user), quantile_cont(cpu_user, 0.99), mad(v) FROM cpu GROUP BY 1"
	m, ok := eligibleShape(sql)
	if !ok {
		t.Fatalf("should be eligible: %s", sql)
	}
	want := []string{"date_trunc('hour', time)", "median(cpu_user)", "quantile_cont(cpu_user, 0.99)", "mad(v)"}
	if !reflect.DeepEqual(m.aggItems, want) {
		t.Fatalf("items = %v, want %v", m.aggItems, want)
	}
}

func TestOrderStatsDecline(t *testing.T) {
	for _, sql := range []string{
		"SELECT quantile_cont(x, [0.1, 0.5]) FROM t",
		"SELECT quantile_cont(x, -0.5) FROM t",
		"SELECT quantile_cont(x, 1.5) FROM t",
		"SELECT quantile_cont(x, 2) FROM t",
		"SELECT quantile_cont(x, .5) FROM t",
		"SELECT quantile_cont(x, 00.5) FROM t",
		"SELECT quantile_cont(x, 0.5::double) FROM t",
		"SELECT quantile_cont(x) FROM t",
		"SELECT quantile_cont(x, 0.5, 0.6) FROM t",
		"SELECT quantile_cont(t.x, 0.5) FROM t",
		"SELECT quantile_cont(end, 0.5) FROM t",
		"SELECT median(DISTINCT x) FROM t",
		"SELECT median(x * 2) FROM t",
		// Tag-keyed grouping: gated.
		"SELECT host, median(x) FROM t GROUP BY host",
		"SELECT host, quantile_cont(x, 0.9) FROM t GROUP BY host",
		// Grouped mode: not routed in v1 (ties decline the whole query).
		"SELECT date_trunc('hour', time), mode(code) FROM t GROUP BY 1",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (items %v)", sql, m.aggItems)
		}
	}
}
