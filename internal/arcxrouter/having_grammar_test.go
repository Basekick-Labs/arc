// res-3b companion: the HAVING grammar (arcX feat/having-grammar).

package arcxrouter

import (
	"strings"
	"testing"
)

func TestHavingGrammarRecognized(t *testing.T) {
	const b = "SELECT date_trunc('hour', time) AS t, "
	cases := []struct {
		sql      string
		having   string
		hidden   int
		unstable string
	}{
		// Hidden aggregates: zero visible, mixed, dedup, bucket-keyed moments,
		// predicate arguments, unstable classes.
		{"SELECT host FROM cpu GROUP BY host HAVING count(*) > 100", "count(*) > 100", 1, ""},
		{"SELECT host FROM cpu GROUP BY host HAVING count(*) > 100 AND count(*) < 200", "count(*) > 100 AND count(*) < 200", 1, ""},
		{"SELECT host, avg(x) FROM cpu GROUP BY host HAVING max(x) > 90 OR sum(code) > 5", "max(x) > 90 OR sum(code) > 5", 2, ""},
		{"SELECT date_trunc('hour', time) AS t FROM cpu GROUP BY 1 HAVING stddev(x) > 2 ORDER BY 1", "stddev(x) > 2", 1, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING median(x) > 3 AND quantile_cont(x, 0.9) < 9", "median(x) > 3 AND quantile_cont(x, 0.9) < 9", 2, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING count_if(x > 1) > 2", "count_if(x > 1) > 2", 1, ""},
		{"SELECT date_trunc('hour', time), count(*) FROM cpu GROUP BY 1 HAVING corr(y, x) > 0", "corr(y, x) > 0", 1, ""},
		{"SELECT date_trunc('hour', time), count(*) FROM cpu GROUP BY 1 HAVING stddev(v) > 1", "stddev(v) > 1", 1, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING max(time) > '2026-01-01T00:00:00Z'", "max(time) > '2026-01-01T00:00:00Z'", 1, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING skewness(x) > 0", "skewness(x) > 0", 1, "oracle"},
		{"SELECT host FROM cpu GROUP BY host HAVING arg_max(x, time) > 1 ORDER BY 1", "arg_max(x, time) > 1", 1, "arg"},
		// NOT, IS NULL, BETWEEN, IN, literal-left.
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT count(*) > 5", "NOT count(*) > 5", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT (count(*) > 5 OR NOT host = 'a')", "NOT (count(*) > 5 OR NOT host = 'a')", 0, ""},
		{b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING NOT bool_and(ok)", "NOT bool_and(ok)", 0, ""},
		{"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) IS NOT NULL AND host IS NULL", "avg(x) IS NOT NULL AND host IS NULL", 0, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING t IS NOT NULL", "t IS NOT NULL", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) BETWEEN 1 AND 2.5", "count(*) BETWEEN 1 AND 2.5", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) NOT BETWEEN -1 AND 2", "count(*) NOT BETWEEN -1 AND 2", 0, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING t BETWEEN '2026-01-01T00:00:00Z' AND '2026-02-01T00:00:00Z'", "t BETWEEN '2026-01-01T00:00:00Z' AND '2026-02-01T00:00:00Z'", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IN (1, 2.5, 3)", "count(*) IN (1, 2.5, 3)", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING host NOT IN ('a', 'b')", "host NOT IN ('a', 'b')", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING 5 < count(*)", "5 < count(*)", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING -5 <= sum(code)", "-5 <= sum(code)", 1, ""},
		{b + "bool_or(ok) FROM cpu GROUP BY 1 HAVING TRUE = bool_or(ok)", "TRUE = bool_or(ok)", 0, ""},
		// Reference vs reference: order-exact, compatible kinds.
		{"SELECT host, count(*), max(code) FROM cpu GROUP BY host HAVING count(*) > max(code)", "count(*) > max(code)", 0, ""},
		{"SELECT host FROM cpu GROUP BY host HAVING max(x) > min(x)", "max(x) > min(x)", 2, ""},
		{b + "regr_count(y, x), count(*) FROM cpu GROUP BY 1 HAVING regr_count(y, x) < count(*)", "regr_count(y, x) < count(*)", 0, ""},
		{b + "bool_and(ok), bool_or(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) <> bool_or(ok)", "bool_and(ok) != bool_or(ok)", 0, ""},
		{b + "max(time) FROM cpu GROUP BY 1 HAVING t < max(time)", "t < max(time)", 0, ""},
		{"SELECT date_trunc('hour', time) AS time, count(*) FROM cpu GROUP BY 1 HAVING time = min(time)", "time = min(time)", 1, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING quantile_disc(code, 0.5) >= min(code)", "quantile_disc(code, 0.5) >= min(code)", 2, ""},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should route: %s", c.sql)
		}
		if m.havingText != c.having || m.havingHidden != c.hidden || m.havingUnstable != c.unstable {
			t.Fatalf("%s\n got having=%q hidden=%d unstable=%q\nwant having=%q hidden=%d unstable=%q",
				c.sql, m.havingText, m.havingHidden, m.havingUnstable, c.having, c.hidden, c.unstable)
		}
	}
}

func TestHavingGrammarDecline(t *testing.T) {
	const b = "SELECT date_trunc('hour', time) AS t, "
	for _, sql := range []string{
		// No aggregate anywhere (bare DISTINCT).
		"SELECT host FROM cpu GROUP BY host",
		"SELECT host FROM cpu GROUP BY host HAVING host = 'a'",
		// A hidden bucket-gated aggregate on a tag key (the agg-6a gate).
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING stddev(x) > 1",
		"SELECT host FROM cpu GROUP BY host HAVING median(x) > 1",
		"SELECT host FROM cpu GROUP BY host HAVING corr(y, x) > 0",
		"SELECT host FROM cpu GROUP BY host HAVING count_if(x > 1) > 0",
		// Grouped mode is never routed, hidden or not.
		b + "count(*) FROM cpu GROUP BY 1 HAVING mode(code) > 1",
		// ORDER BY a hidden aggregate: the shadow cannot see its values.
		"SELECT host FROM cpu GROUP BY host HAVING count(*) > 1 ORDER BY count(*) DESC LIMIT 3",
		// Reference vs reference with a float-accumulated, unstable or untyped side.
		b + "avg(x), max(x) FROM cpu GROUP BY 1 HAVING avg(x) < max(x)",
		b + "sum(code), count(*) FROM cpu GROUP BY 1 HAVING sum(code) > count(*)",
		b + "median(x) FROM cpu GROUP BY 1 HAVING median(x) > min(x)",
		"SELECT host, arg_max(x, time) FROM cpu GROUP BY host HAVING arg_max(x, time) > min(x)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host = max(code)",
		b + "bool_and(ok), count(*) FROM cpu GROUP BY 1 HAVING bool_and(ok) = count(*)",
		b + "max(time), count(*) FROM cpu GROUP BY 1 HAVING max(time) > count(*)",
		b + "count(*) FROM cpu GROUP BY 1 HAVING t < max(x)",
		// min/max over the bucket's own column is a timestamp (review M2).
		b + "count(*) FROM cpu GROUP BY 1 HAVING max(time) > 5",
		b + "count(*) FROM cpu GROUP BY 1 HAVING min(time) BETWEEN 1 AND 2",
		// IN / BETWEEN forms the engine declines.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host NOT IN ('a', NULL)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IN (1, code)",
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) IN (TRUE)",
		b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) BETWEEN FALSE AND TRUE",
		b + "count(*) FROM cpu GROUP BY 1 HAVING t BETWEEN 1 AND 2",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) NOT > 5",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING 1 = 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IS 5",
		// Still out of scope after res-3c: `//`, ILIKE, IS TRUE on a non-Boolean.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) // 2 > 5",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host ILIKE 'a%'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IS TRUE",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (having %q)", sql, m.havingText)
		}
	}
}

// Atom accounting mirrors the engine's MAX_PRED_ATOMS: a BETWEEN counts two atoms
// and an IN list one per element, so a tree the engine would refuse at the cap is
// refused here too.
func TestHavingGrammarAtomCap(t *testing.T) {
	base := "SELECT host, count(*) FROM cpu GROUP BY host HAVING "
	fill := strings.Repeat("count(*) > 1 OR ", maxWhereAtoms-2)
	if _, ok := eligibleShape(base + fill + "count(*) > 2 OR count(*) > 3"); !ok {
		t.Fatalf("exactly %d atoms must route", maxWhereAtoms)
	}
	if _, ok := eligibleShape(base + fill + "count(*) BETWEEN 2 AND 3"); !ok {
		t.Fatalf("%d atoms incl. a BETWEEN must route", maxWhereAtoms)
	}
	if _, ok := eligibleShape(base + fill + "count(*) > 1 OR count(*) BETWEEN 2 AND 3"); ok {
		t.Fatal("a BETWEEN past the cap must decline")
	}
	if _, ok := eligibleShape(base + fill + "count(*) IN (1, 2, 3)"); ok {
		t.Fatal("an IN list past the cap must decline")
	}
}
