// agg-6b' companion: the Kahan sums, bitwise, and Boolean-column bool_and /
// bool_or / count_if route ungrouped and under either key kind (tag keys since C2);
// product, predicate arguments and Boolean aggregates in HAVING decline.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestNumericAggsRecognized(t *testing.T) {
	scan := []struct {
		sql   string
		items []string
	}{
		{"SELECT fsum(f), SUMKAHAN(F), kahan_sum(f), favg(f) FROM m",
			[]string{"fsum(f)", "sumkahan(F)", "kahan_sum(f)", "favg(f)"}},
		{"SELECT bit_and(i), bit_or(i), bit_xor(i), bool_and(b), bool_or(b), count_if(b), COUNTIF(b) FROM m WHERE host = 'a'",
			[]string{"bit_and(i)", "bit_or(i)", "bit_xor(i)", "bool_and(b)", "bool_or(b)", "count_if(b)", "countif(b)"}},
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
	// time-bucket grouping routes (measured win), HAVING on a numeric one
	for _, sql := range []string{
		"SELECT date_trunc('hour', time), fsum(value), bit_or(i) FROM m GROUP BY 1",
		"SELECT date_trunc('hour', time), count_if(b) FROM m GROUP BY 1 HAVING count_if(b) > 2",
	} {
		if m, ok := eligibleShape(sql); !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should be scan_agg_grouped: %s (got %q, %t)", sql, m.shape, ok)
		}
	}
}

func TestNumericAggsDecline(t *testing.T) {
	for _, sql := range []string{
		// (Predicate arguments route since agg-6b2 — pred_arg_test.go.)
		"SELECT count_if(*) FROM m",
		"SELECT fsum(DISTINCT f) FROM m",
		"SELECT host, bool_or(b) FROM m GROUP BY host HAVING bool_or(b) = 1",
		"SELECT host, bool_and(b) FROM m GROUP BY host HAVING bool_and(b) > 0",
		// product: engine-served, not routed (measured loss)
		"SELECT product(f) FROM m",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline (%s): %s", m.shape, sql)
		}
	}
}
