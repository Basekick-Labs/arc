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

func TestGroupedAggRecognized(t *testing.T) {
	cases := []struct {
		sql       string
		items     []string
		key       string
		whereText string
	}{
		{"SELECT host, count(*) FROM cpu GROUP BY host", []string{"host", "count(*)"}, "host", ""},
		{"SELECT count(*), host FROM cpu GROUP BY host", []string{"count(*)", "host"}, "host", ""},
		{"SELECT host, count(*) FROM cpu GROUP BY 1", []string{"host", "count(*)"}, "host", ""},
		{"SELECT count(*), host FROM cpu GROUP BY 2", []string{"count(*)", "host"}, "host", ""},
		{"SELECT host, count(*), count(*) FROM cpu GROUP BY host", []string{"host", "count(*)", "count(*)"}, "host", ""},
		// agg-2c widened the class to agg-1's full aggregate set (no WHERE).
		{"SELECT host, sum(x), avg(x) FROM cpu GROUP BY host", []string{"host", "sum(x)", "avg(x)"}, "host", ""},
		{"SELECT host, count(*), avg(Usage), max(Usage) FROM cpu GROUP BY host", []string{"host", "count(*)", "avg(Usage)", "max(Usage)"}, "host", ""},
		{"SELECT min(time), host FROM cpu GROUP BY 2", []string{"min(time)", "host"}, "host", ""},
		// mimalloc slice: the WHERE-bearing shapes cleared the perf gate (both the
		// selective and broad arms + the masked dashboard pair win vs v1.5.5).
		{
			"SELECT host, count(*), avg(cpu_user) FROM cpu WHERE cpu_user > 90 GROUP BY host",
			[]string{"host", "count(*)", "avg(cpu_user)"}, "host", "cpu_user > 90",
		},
		{
			"SELECT host, sum(x) FROM cpu WHERE host = 'a' GROUP BY host",
			[]string{"host", "sum(x)"}, "host", "host = 'a'",
		},
		{
			"SELECT host, sum(f), min(f) FROM cpu WHERE f > 1.5 OR host = 'b' GROUP BY 1",
			[]string{"host", "sum(f)", "min(f)"}, "host", "f > 1.5 OR host = 'b'",
		},
		// agg-3: the time-bucket key + ORDER BY on the key.
		{
			"SELECT date_trunc('minute', time), count(*), avg(cpu_user) FROM cpu WHERE time >= '2026-01-01T00:00:00Z' GROUP BY 1 ORDER BY 1",
			[]string{"date_trunc('minute', time)", "count(*)", "avg(cpu_user)"},
			"date_trunc('minute', time)", "time >= '2026-01-01T00:00:00Z'",
		},
		{
			"SELECT count(*), date_trunc('hour', Time) FROM cpu GROUP BY 2",
			[]string{"count(*)", "date_trunc('hour', Time)"},
			"date_trunc('hour', Time)", "",
		},
		{
			"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY host ASC",
			[]string{"host", "count(*)"}, "host", "",
		},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should be scan_agg_grouped: %s (got %q, %t)", c.sql, m.shape, ok)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) || m.groupKey != c.key || m.whereText != c.whereText {
			t.Fatalf("items/key/where = %v/%q/%q, want %v/%q/%q: %s",
				m.aggItems, m.groupKey, m.whereText, c.items, c.key, c.whereText, c.sql)
		}
	}
}

func TestGroupedAggDeclines(t *testing.T) {
	for _, sql := range []string{
		// Outside the allow-listed subclass — the engine serves wider grouped
		// shapes but they have NOT cleared the perf gate.
		"SELECT host, sum(a * b) FROM cpu GROUP BY host",               // expression arg
		"SELECT host, count(DISTINCT x) FROM cpu GROUP BY host",        // DISTINCT
		"SELECT host, region, count(*) FROM cpu GROUP BY host, region", // multi-key
		"SELECT host FROM cpu GROUP BY host",                           // no count
		"SELECT count(*) FROM cpu GROUP BY host",                       // key not projected
		"SELECT host, count(*) FROM cpu GROUP BY region",               // wrong key
		"SELECT host, count(*) FROM cpu GROUP BY 2",                    // position at count
		// `ORDER BY 1` on the key became eligible at agg-3 (see the Recognized
		// test); DESC and ORDER on an aggregate still decline.
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 1 DESC",
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2",
		"SELECT host, count(*) FROM cpu GROUP BY host LIMIT 5",
		// WHERE joined the class (mimalloc slice) — but ONLY through the shared
		// reserializeWhere vocabulary; anything it declines stays declined here.
		"SELECT host, count(*) FROM cpu WHERE GROUP BY host",                   // empty WHERE
		"SELECT host, count(*) FROM cpu WHERE x > GROUP BY host",               // dangling atom
		"SELECT host, count(*) FROM cpu WHERE lower(host) = 'a' GROUP BY host", // fn call in WHERE
		"SELECT host, count(*) FROM cpu WHERE x > 1 GROUP BY host LIMIT 5",     // trailing after GROUP BY
	} {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScanAggGrouped {
			t.Fatalf("must not match scan_agg_grouped: %s", sql)
		}
	}
}

func TestEpochMathBucketRecognized(t *testing.T) {
	// agg-3b: the exact Grafana $__timeGroup emission (mandatory alias; the
	// time-series frame needs a column named `time`).
	em := "to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS time"
	cases := []struct {
		sql        string
		orderByKey bool
	}{
		{"SELECT " + em + ", avg(cpu_user) FROM cpu WHERE time >= '2026-08-13T00:00:00Z' AND time < '2026-08-14T00:00:00Z' GROUP BY 1 ORDER BY time", true},
		{"SELECT " + em + ", count(*), avg(cpu_user) FROM cpu GROUP BY 1 ORDER BY 1", true},
		{"SELECT " + em + ", avg(cpu_user) FROM cpu GROUP BY 1 ORDER BY time ASC", true},
		{"SELECT " + em + ", avg(cpu_user) FROM cpu GROUP BY 1", false},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should be scan_agg_grouped: %s (got %q, %t)", c.sql, m.shape, ok)
		}
		if m.epochWidthSecs != 300 || m.bucketCol != "time" || m.bucketAlias != "time" {
			t.Fatalf("parts = %d/%q/%q: %s", m.epochWidthSecs, m.bucketCol, m.bucketAlias, c.sql)
		}
		if m.orderByKey != c.orderByKey {
			t.Fatalf("orderByKey = %t, want %t: %s", m.orderByKey, c.orderByKey, c.sql)
		}
	}
}

func TestEpochMathBucketDeclines(t *testing.T) {
	for _, sql := range []string{
		// A SPACED `/ /` is a DuckDB Parser Error — must never lex as `//` (the
		// 2e `--` comment adjacency class; greedy lexer pin).
		"SELECT to_timestamp((epoch_ns(time) / / 1000000000 // 300) * 300) AS time, avg(x) FROM cpu GROUP BY 1",
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 / / 300) * 300) AS time, avg(x) FROM cpu GROUP BY 1",
		// Width literals must match (a different, legal expression otherwise).
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 600) AS time, avg(x) FROM cpu GROUP BY 1",
		// Unaliased form declines (derived-name reproduction not worth it).
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300), avg(x) FROM cpu GROUP BY 1",
		// GROUP BY <alias>: DuckDB binds the RAW column there (oracle-probed) —
		// only positional GROUP BY is safe.
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS b, avg(x) FROM cpu GROUP BY b",
		// Width out of range / zero.
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 0) * 0) AS time, avg(x) FROM cpu GROUP BY 1",
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 99999999) * 99999999) AS time, avg(x) FROM cpu GROUP BY 1",
		// Wrong ns divisor.
		"SELECT to_timestamp((epoch_ns(time) // 1000000 // 300) * 300) AS time, avg(x) FROM cpu GROUP BY 1",
		// Two bucket items.
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS a, date_trunc('minute', time), avg(x) FROM cpu GROUP BY 1, 2",
		// ORDER BY DESC still declines.
		"SELECT to_timestamp((epoch_ns(time) // 1000000000 // 300) * 300) AS time, avg(x) FROM cpu GROUP BY 1 ORDER BY time DESC",
	} {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScanAggGrouped {
			t.Fatalf("must not match scan_agg_grouped: %s", sql)
		}
	}
}
