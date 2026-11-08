// agg-8 companion: skewness / kurtosis / kurtosis_pop route ungrouped and under a
// time bucket; tag-keyed grouping stays gated. Pure tokenizer tests.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestShapeStatsRoute(t *testing.T) {
	cases := []struct {
		sql   string
		items []string
	}{
		{"SELECT skewness(v), KURTOSIS(V), kurtosis_pop(v) FROM cpu WHERE host = 'a'",
			[]string{"skewness(v)", "kurtosis(V)", "kurtosis_pop(v)"}},
		{"SELECT date_trunc('hour', time), skewness(cpu_user), stddev(cpu_user) FROM cpu GROUP BY 1",
			[]string{"date_trunc('hour', time)", "skewness(cpu_user)", "stddev(cpu_user)"}},
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
	for _, sql := range []string{
		"SELECT host, kurtosis(v) FROM cpu GROUP BY host",
		"SELECT skewness(DISTINCT v) FROM cpu",
		"SELECT skewness(v * 2) FROM cpu",
		"SELECT skewness(v, w) FROM cpu",
		// Review H1: HAVING over a shape statistic would turn DuckDB 1.5.6's
		// garbage into a row-set difference the cell exemption cannot see.
		"SELECT date_trunc('hour', time), skewness(v) FROM cpu GROUP BY 1 HAVING skewness(v) > 0",
		"SELECT date_trunc('hour', time), kurtosis(v) FROM cpu GROUP BY 1 HAVING kurtosis(v) < 3",
	} {
		if _, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s", sql)
		}
	}
}
