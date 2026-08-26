// Recognizer tests for the agg-1 ungrouped-aggregation shape (scan_agg). Pure
// tokenizer tests — no engine, no cgo, run in every build mode.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestScanAggRecognized(t *testing.T) {
	cases := []struct {
		sql       string
		items     []string
		whereText string
	}{
		// The headline shape: count(*) with a data-column WHERE.
		{
			"SELECT count(*) FROM cpu WHERE host = 'a'",
			[]string{"count(*)"},
			"host = 'a'",
		},
		// Multi-aggregate dashboard shape.
		{
			"SELECT count(*), avg(usage_user) FROM cpu WHERE host = 'a'",
			[]string{"count(*)", "avg(usage_user)"},
			"host = 'a'",
		},
		// sum/avg have no footer shape — recognized with or without WHERE.
		{
			"SELECT sum(value) FROM cpu",
			[]string{"sum(value)"},
			"",
		},
		// min/max with WHERE fall past the footer scalar matcher into scan_agg.
		{
			"SELECT min(time), max(time) FROM cpu WHERE host = 'a'",
			[]string{"min(time)", "max(time)"},
			"host = 'a'",
		},
		// Function name lowercased; ARGUMENT spelling preserved (client-visible
		// derived name `sum(X)` — canonicalizing the arg would change it).
		{
			"SELECT SUM(X) FROM cpu WHERE X > 1",
			[]string{"sum(X)"},
			"X > 1",
		},
		// Boolean-tree WHERE reuses the scan's re-serialization.
		{
			"SELECT count(*), sum(x) FROM cpu WHERE host = 'a' OR x > 3",
			[]string{"count(*)", "sum(x)"},
			"host = 'a' OR x > 3",
		},
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
		if m.whereText != c.whereText {
			t.Fatalf("whereText = %q, want %q: %s", m.whereText, c.whereText, c.sql)
		}
	}
}

func TestScanAggFooterShapesKeepFirstRefusal(t *testing.T) {
	// No-WHERE single aggregates stay on their proven footer shapes; the engine
	// itself routes footer-first for these.
	for sql, shape := range map[string]string{
		"SELECT count(*) FROM cpu":  ShapeCountStar,
		"SELECT count(x) FROM cpu":  ShapeCountCol,
		"SELECT min(time) FROM cpu": ShapeMinCol,
		"SELECT max(time) FROM cpu": ShapeMaxCol,
	} {
		m, ok := eligibleShape(sql)
		if !ok || m.shape != shape {
			t.Fatalf("eligibleShape(%q) = (%q, %t), want (%q, true)", sql, m.shape, ok, shape)
		}
	}
}

func TestScanAggDeclines(t *testing.T) {
	for _, sql := range []string{
		// Later slices / engine-declined shapes must stay off the arcx path.
		"SELECT count(*) FROM cpu GROUP BY host",
		"SELECT count(*) FROM cpu WHERE x > 1 ORDER BY 1",
		"SELECT count(*) FROM cpu WHERE x > 1 LIMIT 1",
		"SELECT count(DISTINCT x) FROM cpu",
		"SELECT sum(a * b) FROM cpu",
		"SELECT sum(*) FROM cpu",
		"SELECT host, count(*) FROM cpu",          // mixed list
		"SELECT count(*) AS c FROM cpu WHERE x=1", // alias
		"SELECT median(x) FROM cpu WHERE x > 1",   // unsupported aggregate
		"SELECT sum(x) FROM cpu WHERE",            // dangling WHERE
	} {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScanAgg {
			t.Fatalf("must not match scan_agg: %s", sql)
		}
	}
}
