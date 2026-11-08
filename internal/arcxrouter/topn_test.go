// res-2 companion: the grouped ORDER BY / LIMIT grammar.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestGroupedTopNRecognized(t *testing.T) {
	cases := []struct {
		sql      string
		keys     []OrderKey
		limit    int
		hasLimit bool
	}{
		{"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 DESC LIMIT 10",
			[]OrderKey{{Pos: 2, Desc: true}}, 10, true},
		// LIMIT 0 is a real limit (adversarial B2).
		{"SELECT host, avg(x) AS a FROM cpu GROUP BY host ORDER BY a DESC NULLS FIRST LIMIT 0",
			[]OrderKey{{Pos: 2, Desc: true, NullsFirst: true}}, 0, true},
		{"SELECT host, count(*), avg(x) FROM cpu GROUP BY host ORDER BY count(*) DESC, avg(x) LIMIT 5",
			[]OrderKey{{Pos: 2, Desc: true}, {Pos: 3}}, 5, true},
		{"SELECT date_trunc('hour', time), host, max(x) FROM cpu GROUP BY 1, 2 ORDER BY host, 1 DESC",
			[]OrderKey{{Pos: 2}, {Pos: 1, Desc: true}}, 0, false},
		{"SELECT host, COUNT(*) FROM cpu GROUP BY host ORDER BY count(*)",
			[]OrderKey{{Pos: 2}}, 0, false},
		{"SELECT host, max(x) FROM cpu GROUP BY host ORDER BY host ASC NULLS LAST, MAX(x) desc",
			[]OrderKey{{Pos: 1}, {Pos: 2, Desc: true}}, 0, false},
		// A predicate-argument aggregate by position (never by expression);
		// moments stay bucket-keyed (agg-6a), so the refused-target declines
		// below use a bucket key too — a tag key would decline for that reason.
		{"SELECT date_trunc('hour', time), count_if(x > 1) FROM cpu GROUP BY 1 ORDER BY 2 DESC LIMIT 3",
			[]OrderKey{{Pos: 2, Desc: true}}, 3, true},
		{"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 DESC LIMIT 2147483647",
			[]OrderKey{{Pos: 2, Desc: true}}, maxTopNLimit, true},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should be scan_agg_grouped: %s (got %q, %t)", c.sql, m.shape, ok)
		}
		if !reflect.DeepEqual(m.orderKeys, c.keys) || m.limit != c.limit || m.hasLimit != c.hasLimit {
			t.Fatalf("%s:\n got keys=%v limit=%d/%t\nwant keys=%v limit=%d/%t",
				c.sql, m.orderKeys, m.limit, m.hasLimit, c.keys, c.limit, c.hasLimit)
		}
	}
}

func TestGroupedTopNDeclines(t *testing.T) {
	for _, sql := range []string{
		"SELECT host, count(*) FROM cpu GROUP BY host LIMIT 5",                                          // unordered LIMIT
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2, 1, 2, 1, 2",                           // five keys
		"SELECT host, avg(x) FROM cpu GROUP BY host ORDER BY avg(x), host",                              // tolerant not last
		"SELECT host, avg(x) FROM cpu GROUP BY host ORDER BY 2 DESC, 1",                                 // same, by position
		"SELECT host, arg_max(v, time) FROM cpu GROUP BY host ORDER BY 2",                               // refused target
		"SELECT host, max_by(v, time) AS m FROM cpu GROUP BY host ORDER BY m",                           // refused, by alias
		"SELECT date_trunc('hour', time), fsum(x) FROM cpu GROUP BY 1 ORDER BY 2 DESC LIMIT 3",          // oracle-unstable
		"SELECT date_trunc('hour', time), skewness(x) FROM cpu GROUP BY 1 ORDER BY 2",                   // shape statistic
		"SELECT date_trunc('hour', time), covar_pop(x, y) FROM cpu GROUP BY 1 ORDER BY 2",               // cancellation-prone
		"SELECT host, max(x) FROM cpu GROUP BY host ORDER BY min(x)",                                    // hidden aggregate
		"SELECT date_trunc('hour', time), count_if(x > 1) FROM cpu GROUP BY 1 ORDER BY count_if(x > 1)", // predicate by expression
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 3",                                       // out of range
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 0",                                       // position 0
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 LIMIT 2147483648",                      // over the cap
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 LIMIT 5 OFFSET 2",                      // offset
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 LIMIT -1",                              // negative
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 LIMIT 1.5",                             // non-integer
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 NULLS",                                 // dangling NULLS
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 DESC DESC",                             // repeated direction
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY count(*) + 1",                            // expression
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 LIMIT 5 LIMIT 6",                       // trailing clause
		// `AS desc` resolved by ORDER BY is a DuckDB parser error (review M2).
		"SELECT host, max(x) AS desc FROM cpu GROUP BY host ORDER BY desc",
		"SELECT host, max(x) AS asc FROM cpu GROUP BY host ORDER BY asc LIMIT 3",
		// res-2 session guard: an in-query default_order change.
		"SELECT host, count(*) FROM cpu WHERE default_order > 1 GROUP BY host ORDER BY 2",
	} {
		if m, ok := eligibleShape(sql); ok && m.shape == ShapeScanAggGrouped {
			t.Fatalf("must not match scan_agg_grouped: %s", sql)
		}
	}
}
