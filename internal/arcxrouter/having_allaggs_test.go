// res-3 companion: HAVING over every served aggregate (arcX feat/having-all-aggs).

package arcxrouter

import "testing"

func TestHavingAllAggsRecognized(t *testing.T) {
	const b = "SELECT date_trunc('hour', time) AS t, "
	cases := []struct {
		sql      string
		having   string
		unstable string
	}{
		// Decimal literals (the engine's exact integer rewrite) — any item.
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 2.5", "count(*) > 2.5", ""},
		{"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) <= -0.25", "avg(x) <= -0.25", ""},
		{b + "regr_count(y, x) FROM cpu GROUP BY 1 HAVING regr_count(y, x) >= 99.5", "regr_count(y, x) >= 99.5", ""},
		// Aliases resolve to the ITEM TEXT (review M5), key first.
		{"SELECT host, count(*) AS n FROM cpu GROUP BY host HAVING n > 5", "count(*) > 5", ""},
		{"SELECT host, count(*) AS N FROM cpu GROUP BY host HAVING n > 5 AND N < 9.5", "count(*) > 5 AND count(*) < 9.5", ""},
		{"SELECT host, max(x) AS x FROM cpu GROUP BY host HAVING x > 3", "max(x) > 3", ""},
		{"SELECT host, count(*) AS count FROM cpu GROUP BY host HAVING count > 3", "count(*) > 3", ""},
		{b + "count(*) AS n FROM cpu GROUP BY 1 HAVING t > '2026-01-01T00:00:00Z' AND n > 1", "t > '2026-01-01T00:00:00Z' AND count(*) > 1", ""},
		// Order statistics.
		{b + "median(x) FROM cpu GROUP BY 1 HAVING median(x) > 3", "median(x) > 3", ""},
		{b + "quantile_cont(x, 0.9) FROM cpu GROUP BY 1 HAVING quantile_cont(x, 0.9) > 5.5", "quantile_cont(x, 0.9) > 5.5", ""},
		{b + "quantile_disc(i, 0.5) AS q FROM cpu GROUP BY 1 HAVING q >= 1", "quantile_disc(i, 0.5) >= 1", ""},
		{b + "mad(x) FROM cpu GROUP BY 1 HAVING mad(x) < 1 ORDER BY t", "mad(x) < 1", ""},
		// Boolean aggregates: bare, = / <> / != TRUE|FALSE, via alias.
		{b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok)", "bool_and(ok)", ""},
		{b + "bool_or(ok) FROM cpu GROUP BY 1 HAVING bool_or(ok) = true", "bool_or(ok) = TRUE", ""},
		{b + "bool_or(ok) AS bo FROM cpu GROUP BY 1 HAVING bo <> FALSE OR (bo)", "bool_or(ok) != FALSE OR (bool_or(ok))", ""},
		{b + "bool_and(ok), count(*) FROM cpu GROUP BY 1 HAVING bool_and(ok) AND count(*) > 2 ORDER BY 1", "bool_and(ok) AND count(*) > 2", ""},
		// Predicate-argument aggregates (canonical re-serialization, review M4).
		{b + "count_if(x > 1) FROM cpu GROUP BY 1 HAVING count_if(x>1) > 2", "count_if(x > 1) > 2", ""},
		{b + "bool_and(x > 1.5) FROM cpu GROUP BY 1 HAVING bool_and(x > 1.5) = false", "bool_and(x > 1.5) = FALSE", ""},
		// Unstable items: routed with the shadow class; a key-only ORDER BY without
		// LIMIT is fine.
		{b + "skewness(x) FROM cpu GROUP BY 1 HAVING skewness(x) > 0", "skewness(x) > 0", "oracle"},
		{b + "kurtosis(x) AS k FROM cpu GROUP BY 1 HAVING k < 3 ORDER BY t DESC", "kurtosis(x) < 3", "oracle"},
		{b + "fsum(x) FROM cpu GROUP BY 1 HAVING fsum(x) > 0", "fsum(x) > 0", "oracle"},
		{b + "covar_pop(y, x) FROM cpu GROUP BY 1 HAVING covar_pop(y, x) > 0", "covar_pop(y, x) > 0", "oracle"},
		{"SELECT host, arg_max(x, time) FROM cpu GROUP BY host HAVING arg_max(x, time) > 1 ORDER BY 1", "arg_max(x, time) > 1", "arg"},
		{b + "arg_max(x, time), fsum(x) FROM cpu GROUP BY 1 HAVING arg_max(x, time) > 1 AND fsum(x) > 0", "arg_max(x, time) > 1 AND fsum(x) > 0", "oracle"},
		// A stable HAVING beside an unstable item is not unstable.
		{b + "skewness(x), count(*) FROM cpu GROUP BY 1 HAVING count(*) > 2 ORDER BY 3 DESC LIMIT 2", "count(*) > 2", ""},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should route: %s", c.sql)
		}
		if m.havingText != c.having || m.havingUnstable != c.unstable {
			t.Fatalf("%s\n got having=%q unstable=%q\nwant having=%q unstable=%q", c.sql, m.havingText, m.havingUnstable, c.having, c.unstable)
		}
	}
}

func TestHavingAllAggsDecline(t *testing.T) {
	const b = "SELECT date_trunc('hour', time) AS t, "
	for _, sql := range []string{
		// Boolean grammar the engine does not mirror (review M6).
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) > false",
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) = 1",
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) = 'true'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) = true",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host",
		"SELECT host, count(*) AS n FROM cpu GROUP BY host HAVING n",
		// A number against the timestamp bucket alias.
		b + "count(*) FROM cpu GROUP BY 1 HAVING t > 5",
		b + "count(*) FROM cpu GROUP BY 1 HAVING t > 2.5",
		// Unknown names (hidden aggregates and a different predicate literal route
		// since res-3b — having_grammar_test.go).
		"SELECT host, count(*) AS n FROM cpu GROUP BY host HAVING m > 3",
		// A decimal wider than 15 significant digits (the engine's exactness bound).
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1234567890123456.5",
		// Unstable HAVING with LIMIT or an aggregate ORDER BY target (review H3).
		b + "skewness(x), count(*) FROM cpu GROUP BY 1 HAVING skewness(x) > 0 ORDER BY 1 LIMIT 5",
		b + "skewness(x), count(*) FROM cpu GROUP BY 1 HAVING skewness(x) > 0 ORDER BY 3 DESC",
		b + "fsum(x) AS f, count(*) FROM cpu GROUP BY 1 HAVING f > 0 ORDER BY count(*)",
		"SELECT host, arg_max(x, time), count(*) FROM cpu GROUP BY host HAVING arg_max(x, time) > 1 ORDER BY 3",
		// Grouped mode is never routed.
		b + "mode(code) FROM cpu GROUP BY 1 HAVING mode(code) > 1",
		// Post-impl review B1: names DuckDB binds to a literal / value function.
		"SELECT host, count(*) AS true FROM cpu GROUP BY host HAVING true > 5",
		"SELECT host, count(*) AS null FROM cpu GROUP BY host HAVING null > 5",
		"SELECT host, count(*) AS false FROM cpu GROUP BY host HAVING false < 5000",
		"SELECT host, max(time) AS current_timestamp FROM cpu GROUP BY host HAVING current_timestamp > '2026-06-01T00:00:00Z'",
		b + "max(time) AS localtimestamp FROM cpu GROUP BY 1 HAVING localtimestamp > '2026-06-01T00:00:00Z'",
		"SELECT host, count(*) AS user FROM cpu GROUP BY host HAVING user > 5",
		"SELECT host, count(*) AS current_date FROM cpu GROUP BY host HAVING current_date > 5",
		"SELECT host, count(*) AS true FROM cpu GROUP BY host ORDER BY true DESC LIMIT 2",
		"SELECT host, count(*) AS user FROM cpu GROUP BY host ORDER BY user",
		// Duplicate / colliding aliases still decline (res-1).
		"SELECT host, count(*) AS n, avg(x) AS n FROM cpu GROUP BY host HAVING n > 3",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (having %q)", sql, m.havingText)
		}
	}
}

// res-2 follow-up found while wiring res-3: the tolerant-key-not-last gate read
// the bucket/tag item indices before they were assigned (both 0), so an
// aggregate in select position 1 slipped through and the engine declined it.
func TestTopNTolerantKeyGateUsesItemIndices(t *testing.T) {
	for _, sql := range []string{
		"SELECT avg(x), host FROM cpu GROUP BY host ORDER BY 1, 2",
		"SELECT date_trunc('hour', time), host, avg(x) FROM cpu GROUP BY 1, 2 ORDER BY 3, 1",
	} {
		if _, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline (tolerant aggregate not last): %s", sql)
		}
	}
	for _, sql := range []string{
		"SELECT host, avg(x) FROM cpu GROUP BY host ORDER BY 1, 2",
		"SELECT date_trunc('hour', time), host, avg(x) FROM cpu GROUP BY 1, 2 ORDER BY 2, 1, 3",
	} {
		if _, ok := eligibleShape(sql); !ok {
			t.Fatalf("should route: %s", sql)
		}
	}
}
