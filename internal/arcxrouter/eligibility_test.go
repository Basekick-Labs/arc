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
			shape, unit, meas, ok := eligibleShape(tc.sql)
			if !ok {
				t.Fatalf("expected eligible, got decline for %q", tc.sql)
			}
			if shape != tc.wantShape || unit != tc.wantUnit || meas != tc.wantMeas {
				t.Fatalf("got (shape=%q unit=%q meas=%q), want (%q %q %q)",
					shape, unit, meas, tc.wantShape, tc.wantUnit, tc.wantMeas)
			}
		})
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
		{"count column not star", "SELECT count(x) FROM cpu"},
		{"count alias", "SELECT count(*) AS n FROM cpu"},
		{"count two tables", "SELECT count(*) FROM cpu, mem"},
		{"not count", "SELECT sum(x) FROM cpu"},
		{"select star", "SELECT * FROM cpu"},

		// --- agg declines ---
		{"agg wrong column", "SELECT date_trunc('day', ts), count(*) FROM cpu GROUP BY 1"},
		{"agg column expr", "SELECT date_trunc('day', time + 1), count(*) FROM cpu GROUP BY 1"},
		{"agg unsupported unit", "SELECT date_trunc('week', time), count(*) FROM cpu GROUP BY 1"},
		{"agg unsupported minute", "SELECT date_trunc('minute', time), count(*) FROM cpu GROUP BY 1"},
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
			if _, _, _, ok := eligibleShape(tc.sql); ok {
				t.Fatalf("expected decline, got ELIGIBLE for %q — a mis-accept is a silent-wrong-answer risk", tc.sql)
			}
		})
	}
}
