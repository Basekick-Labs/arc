// Eligibility recognizer tests. Cgo-free — run in stock builds. These are the
// correctness spine of the router's Go-side pre-filter: every accept, every
// decline reason, and the whole-token mis-accepts the engine's Phase 0 review
// caught (defense in depth — the router must not RE-introduce a mis-accept the
// engine already rejects).

package arcxrouter

import "testing"

func TestEligibleShape_Accepts(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantShape string
		wantUnit  string
		wantMeas  string
	}{
		{"count star bare", "SELECT count(*) FROM cpu", ShapeCountStar, "", "cpu"},
		{"count star dotted", "SELECT count(*) FROM mydb.cpu", ShapeCountStar, "", "mydb.cpu"},
		{"count star trailing semicolon", "SELECT count(*) FROM cpu;", ShapeCountStar, "", "cpu"},
		{"count star lowercase", "select count(*) from cpu", ShapeCountStar, "", "cpu"},
		{"count star mixed ws", "SELECT   count(  *  )\nFROM\tcpu", ShapeCountStar, "", "cpu"},
		{"agg day", "SELECT date_trunc('day', time), count(*) FROM cpu GROUP BY 1", ShapeDateTruncCent, "day", "cpu"},
		{"agg hour", "SELECT date_trunc('hour', time), count(*) FROM cpu GROUP BY 1", ShapeDateTruncCent, "hour", "cpu"},
		{"agg month dotted", "SELECT date_trunc('month', time), count(*) FROM mydb.mem GROUP BY 1", ShapeDateTruncCent, "month", "mydb.mem"},
		{"agg year order by", "SELECT date_trunc('year', time), count(*) FROM cpu GROUP BY 1 ORDER BY 1", ShapeDateTruncCent, "year", "cpu"},
		{"agg unit case preserved", "SELECT date_trunc('Day', time), count(*) FROM cpu GROUP BY 1", ShapeDateTruncCent, "Day", "cpu"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok {
				t.Fatalf("expected eligible, got decline for %q", tc.sql)
			}
			if m.shape != tc.wantShape || m.unit != tc.wantUnit || m.measurement != tc.wantMeas {
				t.Fatalf("got (shape=%q unit=%q meas=%q), want (%q %q %q)",
					m.shape, m.unit, m.measurement, tc.wantShape, tc.wantUnit, tc.wantMeas)
			}
		})
	}
}

func TestEligibleShape_ScalarAggregates(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		wantShape string
		wantCol   string
		wantMeas  string
	}{
		{"min time", "SELECT min(time) FROM cpu", ShapeMinCol, "time", "cpu"},
		{"max time", "SELECT max(time) FROM cpu", ShapeMaxCol, "time", "cpu"},
		{"count col", "SELECT count(host) FROM cpu", ShapeCountCol, "host", "cpu"},
		{"min dotted", "SELECT min(usage) FROM mydb.cpu", ShapeMinCol, "usage", "mydb.cpu"},
		{"count col preserves case", "SELECT count(MyCol) FROM cpu", ShapeCountCol, "MyCol", "cpu"},
		{"lowercase kw", "select max(value) from cpu", ShapeMaxCol, "value", "cpu"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok {
				t.Fatalf("expected eligible, got decline for %q", tc.sql)
			}
			if m.shape != tc.wantShape || m.col != tc.wantCol || m.measurement != tc.wantMeas {
				t.Fatalf("got (shape=%q col=%q meas=%q), want (%q %q %q)",
					m.shape, m.col, m.measurement, tc.wantShape, tc.wantCol, tc.wantMeas)
			}
			// count(*) must NOT be misread as count(col).
			if m.shape == ShapeCountCol && m.col == "*" {
				t.Fatal("count(*) leaked into count(col)")
			}
		})
	}
}

func TestEligibleShape_ScalarDeclines(t *testing.T) {
	for _, sql := range []string{
		"SELECT min(time) FROM cpu WHERE x > 1",
		"SELECT max(time) FROM cpu GROUP BY 1",
		"SELECT min(time) AS m FROM cpu",
		"SELECT min(time + 1) FROM cpu",
		"SELECT min(*) FROM cpu",
		"SELECT sum(value) FROM cpu",        // sum not supported (footers lack sums)
		"SELECT avg(value) FROM cpu",        // avg not supported
		"SELECT min(a), max(b) FROM cpu",    // two aggregates
		"SELECT min(time) FROM cpu, mem",    // two tables
		"SELECT min(time) FROM cpu LIMIT 1", // trailing clause
	} {
		if _, ok := eligibleShape(sql); ok {
			t.Fatalf("expected decline, got ELIGIBLE for %q", sql)
		}
	}
	// count(*) must still be ShapeCountStar, not a scalar col shape.
	if m, ok := eligibleShape("SELECT count(*) FROM cpu"); !ok || m.shape != ShapeCountStar {
		t.Fatalf("count(*) should be ShapeCountStar, got shape=%q ok=%v", m.shape, ok)
	}
}

func TestEligibleShape_Declines(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		// --- whole-token mis-accepts (glued/adjacent keywords must not match) ---
		// NOTE: unlike the engine (which parses `read_parquet(...)`), the router
		// sees the USER SQL where `FROM read_parquet_foo` is a legitimate table
		// token named read_parquet_foo — it's ACCEPTED here as a measurement and
		// declines downstream (no such measurement → empty LIST). So that case is
		// NOT a router mis-accept. What the router MUST reject is glued keywords
		// that would make a non-count/non-agg query look like one.
		{"SELECTcount glued", "SELECTcount(*) FROM cpu"},
		{"countstar glued", "SELECT countstar(*) FROM cpu"},
		{"junk before parens", "SELECT count x(*) FROM cpu"},
		{"date_trunc suffix", "SELECT date_truncx('day', time), count(*) FROM cpu GROUP BY 1"},

		// --- count(*) declines ---
		{"count with where", "SELECT count(*) FROM cpu WHERE x > 1"},
		{"count with groupby", "SELECT count(*) FROM cpu GROUP BY host"},
		{"count with limit", "SELECT count(*) FROM cpu LIMIT 10"},
		{"count with join", "SELECT count(*) FROM cpu JOIN mem ON cpu.time = mem.time"},
		// NOTE: `count(x)` is now ELIGIBLE (ShapeCountCol, Phase 1b) — no longer a
		// decline. Its acceptance is covered by TestEligibleShape_ScalarAggregates.
		{"count alias", "SELECT count(*) AS n FROM cpu"},
		{"count two tables", "SELECT count(*) FROM cpu, mem"},
		{"not count", "SELECT sum(x) FROM cpu"},
		{"select star", "SELECT * FROM cpu"},

		// --- agg declines ---
		{"agg wrong column", "SELECT date_trunc('day', ts), count(*) FROM cpu GROUP BY 1"},
		{"agg column expr", "SELECT date_trunc('day', time + 1), count(*) FROM cpu GROUP BY 1"},
		{"agg unsupported unit", "SELECT date_trunc('week', time), count(*) FROM cpu GROUP BY 1"},
		// minute/second ARE supported, but ONLY with a time-range WHERE — unfiltered
		// sub-hour declines (would miss DuckDB's NULL bucket). Covered in detail by
		// TestEligibleShape_FilteredDateTrunc.
		{"agg minute no where declines", "SELECT date_trunc('minute', time), count(*) FROM cpu GROUP BY 1"},
		{"agg second no where declines", "SELECT date_trunc('second', time), count(*) FROM cpu GROUP BY 1"},
		{"agg group by expr", "SELECT date_trunc('day', time), count(*) FROM cpu GROUP BY date_trunc('day', time)"},
		{"agg group by 2", "SELECT date_trunc('day', time), count(*) FROM cpu GROUP BY 2"},
		{"agg no group by", "SELECT date_trunc('day', time), count(*) FROM cpu"},
		{"agg extra projection", "SELECT date_trunc('day', time), host, count(*) FROM cpu GROUP BY 1"},
		{"agg having", "SELECT date_trunc('day', time), count(*) FROM cpu GROUP BY 1 HAVING count(*) > 1"},
		{"agg order by 2", "SELECT date_trunc('day', time), count(*) FROM cpu GROUP BY 1 ORDER BY 2"},
		{"agg sum not count", "SELECT date_trunc('day', time), sum(x) FROM cpu GROUP BY 1"},

		// --- TZ-injection declines (M3) ---
		{"set timezone", "SET TimeZone='America/New_York'; SELECT count(*) FROM cpu"},
		{"at time zone", "SELECT count(*) FROM cpu WHERE time AT TIME ZONE 'UTC' > now()"},

		// --- collation declines (2b-4): byte-wise == DuckDB only under default BINARY ---
		{"collate clause", "SELECT host FROM cpu WHERE host < 'm' COLLATE NOCASE"},
		{"set default_collation", "SET default_collation='nocase'; SELECT host FROM cpu WHERE host < 'm'"},

		// --- structural / vocabulary declines ---
		{"multiple statements", "SELECT count(*) FROM cpu; SELECT count(*) FROM mem"},
		{"empty", ""},
		{"garbage", "not sql at all !@#"},
		{"leading dot table", "SELECT count(*) FROM .cpu"},
		{"trailing dot table", "SELECT count(*) FROM cpu."},
		{"double dot table", "SELECT count(*) FROM db..cpu"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := eligibleShape(tc.sql); ok {
				t.Fatalf("expected decline, got ELIGIBLE for %q — a mis-accept is a silent-wrong-answer risk", tc.sql)
			}
		})
	}
}

// TestEligibleShape_FilteredDateTrunc covers PR-A: a time-range WHERE on the
// date_trunc agg. The WHERE is re-serialized into whereText (the engine re-lexes it
// and is the RFC3339-UTC authority); only `time <range-op> '<str>'` AND-conjoined is
// accepted, everything else declines to DuckDB.
func TestEligibleShape_FilteredDateTrunc(t *testing.T) {
	eligible := []struct {
		name, sql, wantWhere string
	}{
		{"time range (the Grafana shape)",
			"SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time >= '2024-01-01T00:00:00Z' AND time < '2024-01-02T00:00:00Z' GROUP BY 1 ORDER BY 1",
			"time >= '2024-01-01T00:00:00Z' AND time < '2024-01-02T00:00:00Z'"},
		{"half-bounded lower",
			"SELECT date_trunc('day', time), count(*) FROM cpu WHERE time >= '2024-01-01T00:00:00Z' GROUP BY 1",
			"time >= '2024-01-01T00:00:00Z'"},
		{"half-bounded upper",
			"SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time < '2024-01-02T00:00:00Z' GROUP BY 1",
			"time < '2024-01-02T00:00:00Z'"},
		{"strict ops preserved",
			"SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time > '2024-01-01T00:00:00Z' AND time <= '2024-01-02T00:00:00Z' GROUP BY 1",
			"time > '2024-01-01T00:00:00Z' AND time <= '2024-01-02T00:00:00Z'"},
		{"unfiltered hour still eligible (empty whereText)",
			"SELECT date_trunc('hour', time), count(*) FROM cpu GROUP BY 1", ""},
		{"minute WITH a time range (the sub-hour Grafana shape)",
			"SELECT date_trunc('minute', time), count(*) FROM cpu WHERE time >= '2024-01-01T03:00:00Z' AND time < '2024-01-01T04:00:00Z' GROUP BY 1 ORDER BY 1",
			"time >= '2024-01-01T03:00:00Z' AND time < '2024-01-01T04:00:00Z'"},
		{"second WITH a half-bounded range",
			"SELECT date_trunc('second', time), count(*) FROM cpu WHERE time >= '2024-01-01T03:00:00Z' GROUP BY 1",
			"time >= '2024-01-01T03:00:00Z'"},
	}
	for _, tc := range eligible {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := eligibleShape(tc.sql)
			if !ok {
				t.Fatalf("expected eligible, got decline for %q", tc.sql)
			}
			if m.shape != ShapeDateTruncCent {
				t.Fatalf("shape=%q want %q", m.shape, ShapeDateTruncCent)
			}
			if m.whereText != tc.wantWhere {
				t.Fatalf("whereText=%q want %q", m.whereText, tc.wantWhere)
			}
		})
	}

	declines := []struct{ name, sql string }{
		{"= on time (not a range)", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time = '2024-01-01T00:00:00Z' GROUP BY 1"},
		{"!= on time", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time != '2024-01-01T00:00:00Z' GROUP BY 1"},
		{"OR", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time >= '2024-01-01T00:00:00Z' OR time < '2024-01-02T00:00:00Z' GROUP BY 1"},
		{"non-time column", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE host = 'web1' GROUP BY 1"},
		{"BETWEEN", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-02T00:00:00Z' GROUP BY 1"},
		{"numeric RHS", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time >= 5 GROUP BY 1"},
		{"IS NULL", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time IS NULL GROUP BY 1"},
		{"in-query default_null_order", "SELECT date_trunc('hour', time), count(*) FROM cpu WHERE time >= '2024-01-01T00:00:00Z' GROUP BY 1 /* default_null_order */"},
		// Sub-hour units REQUIRE a time range: unfiltered would miss DuckDB's NULL
		// bucket (the engine's per-row decode counts only non-null in-range rows).
		{"minute no where", "SELECT date_trunc('minute', time), count(*) FROM cpu GROUP BY 1"},
		{"second no where", "SELECT date_trunc('second', time), count(*) FROM cpu GROUP BY 1"},
		// A sub-hour WHERE that isn't a pure time range still declines (same as hour).
		{"minute IS NULL OR range (grammar guardrail)", "SELECT date_trunc('minute', time), count(*) FROM cpu WHERE time IS NULL OR time >= '2024-01-01T00:00:00Z' GROUP BY 1"},
		{"minute non-time predicate", "SELECT date_trunc('minute', time), count(*) FROM cpu WHERE host = 'web1' GROUP BY 1"},
	}
	for _, tc := range declines {
		t.Run("decline/"+tc.name, func(t *testing.T) {
			if _, ok := eligibleShape(tc.sql); ok {
				t.Fatalf("expected decline, got eligible for %q", tc.sql)
			}
		})
	}
}
