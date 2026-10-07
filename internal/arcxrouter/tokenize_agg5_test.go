package arcxrouter

import "testing"

// agg-5a: the bucket item accepts an optional `AS <alias>`, which is what makes the
// real Grafana panel emission routable. Before this, `date_trunc('hour', time) AS
// time, count(*) … GROUP BY 1` declined while the unaliased form served (measured
// 2026-10-02) — so a dashboard that named its time column, which Grafana's frame
// requires, never reached the engine.
func TestBucketAliasIsAcceptedAndReEmitted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		wantAlias string
	}{
		{"unaliased still serves", "SELECT date_trunc('hour', time), count(*) FROM cpu GROUP BY 1", ""},
		{"aliased AS time (Grafana)", "SELECT date_trunc('hour', time) AS time, count(*) FROM cpu GROUP BY 1", "time"},
		{"aliased AS h", "SELECT date_trunc('hour', time) AS h, count(*) FROM cpu GROUP BY 1", "h"},
		{"aliased + ORDER BY the alias", "SELECT date_trunc('hour', time) AS time, count(*) FROM cpu GROUP BY 1 ORDER BY time", "time"},
		{"aliased + tag key", "SELECT date_trunc('hour', time) AS time, host, count(*) FROM cpu GROUP BY 1, host", "time"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			toks, lexed := tokenize(tc.sql)
			if !lexed {
				t.Fatalf("tokenize failed for %q", tc.sql)
			}
			gm, ok := matchGroupedAgg(toks)
			if !ok {
				t.Fatalf("expected grouped eligibility for %q", tc.sql)
			}
			if gm.bucketAlias != tc.wantAlias {
				t.Fatalf("bucketAlias = %q, want %q", gm.bucketAlias, tc.wantAlias)
			}
		})
	}
}

// The alias must NOT become resolvable in GROUP BY. DuckDB binds a bare identifier
// there to the RAW column, so `… AS time … GROUP BY time` groups by the raw
// timestamp — a DIFFERENT query from `GROUP BY 1` (oracle-probed at agg-3b: 10 rows
// vs 2 on a second-bucket fixture). Accepting it would silently answer the wrong
// question, so it declines.
func TestBucketAliasNeverResolvesInGroupBy(t *testing.T) {
	for _, sql := range []string{
		"SELECT date_trunc('hour', time) AS time, count(*) FROM cpu GROUP BY time",
		"SELECT date_trunc('hour', time) AS h, count(*) FROM cpu GROUP BY h",
	} {
		toks, lexed := tokenize(sql)
		if !lexed {
			continue // an unlexable form is already a decline
		}
		if _, ok := matchGroupedAgg(toks); ok {
			t.Fatalf("alias must not resolve in GROUP BY: %q", sql)
		}
	}
}

// A malformed or unsafe alias declines rather than being re-emitted.
func TestBucketAliasRejectsUnsafeSpellings(t *testing.T) {
	for _, sql := range []string{
		"SELECT date_trunc('hour', time) AS, count(*) FROM cpu GROUP BY 1",
		"SELECT date_trunc('hour', time) AS 'time', count(*) FROM cpu GROUP BY 1",
		"SELECT date_trunc('hour', time) AS where, count(*) FROM cpu GROUP BY 1",
		"SELECT date_trunc('hour', time) AS a-b, count(*) FROM cpu GROUP BY 1",
	} {
		toks, lexed := tokenize(sql)
		if !lexed {
			continue // an unlexable form is already a decline
		}
		if _, ok := matchGroupedAgg(toks); ok {
			t.Fatalf("unsafe alias must decline: %q", sql)
		}
	}
}
