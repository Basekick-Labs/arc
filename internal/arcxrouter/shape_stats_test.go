// agg-8 companion: skewness / kurtosis / kurtosis_pop route ungrouped and under a
// time bucket, and tag-keyed since C2. Pure tokenizer tests.

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
		"SELECT skewness(DISTINCT v) FROM cpu",
		"SELECT skewness(v * 2) FROM cpu",
		"SELECT skewness(v, w) FROM cpu",
	} {
		if _, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s", sql)
		}
	}
}
