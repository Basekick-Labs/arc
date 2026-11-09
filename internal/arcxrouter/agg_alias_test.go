// res-1 companion: `AS <alias>` on aggregates routes (ungrouped, grouped, the
// count(*) and scalar footer shapes); aliases ride a parallel slice, items stay
// the bare call text. Pure tokenizer tests.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestAggAliasesRoute(t *testing.T) {
	cases := []struct {
		sql     string
		shape   string
		items   []string
		aliases []string
		alias   string
	}{
		{"SELECT avg(value) AS value, count(*) AS n, max(x) FROM cpu WHERE host = 'a'", ShapeScanAgg,
			[]string{"avg(value)", "count(*)", "max(x)"}, []string{"value", "n", ""}, ""},
		{"SELECT count_if(x > 1) AS hot, median(x) AS Med, quantile_cont(x, 0.9) AS p90 FROM cpu WHERE x > 0", ShapeScanAgg,
			[]string{"count_if(x > 1)", "median(x)", "quantile_cont(x, 0.9)"}, []string{"hot", "Med", "p90"}, ""},
		{"SELECT date_trunc('hour', time) AS time, avg(value) AS value, count(*) AS n FROM cpu GROUP BY 1", ShapeScanAggGrouped,
			[]string{"date_trunc('hour', time) AS time", "avg(value)", "count(*)"}, []string{"", "value", "n"}, ""},
		{"SELECT count(*) AS n FROM cpu", ShapeCountStar, nil, nil, "n"},
		{"SELECT max(time) AS newest FROM cpu", ShapeMaxCol, nil, nil, "newest"},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if m.shape != c.shape {
			t.Fatalf("shape = %q, want %q: %s", m.shape, c.shape, c.sql)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) || !reflect.DeepEqual(m.aggAliases, c.aliases) || m.aggAlias != c.alias {
			t.Fatalf("items %v aliases %v alias %q; want %v %v %q: %s", m.aggItems, m.aggAliases, m.aggAlias, c.items, c.aliases, c.alias, c.sql)
		}
	}
}

func TestAggAliasesDecline(t *testing.T) {
	for _, sql := range []string{
		"SELECT min(x) mx FROM cpu WHERE x > 1",        // no AS
		"SELECT min(x) AS \"Max X\" FROM cpu",          // quoted (does not lex)
		"SELECT min(x) AS like FROM cpu WHERE x > 1",   // engine clause keyword
		"SELECT min(x) AS between FROM cpu",            // engine clause keyword
		"SELECT min(x) AS having FROM cpu WHERE x > 1", // router clause keyword
		"SELECT min(x) AS a.b FROM cpu WHERE x > 1",    // dotted
		// collisions
		"SELECT host, sum(i) AS host FROM cpu GROUP BY host",
		"SELECT host, sum(i) AS HOST FROM cpu GROUP BY host",
		"SELECT date_trunc('hour', time) AS t, count(*) AS t FROM cpu GROUP BY 1",
		"SELECT date_trunc('hour', time), min(i) AS a, max(i) AS A FROM cpu GROUP BY 1",
		// HAVING naming an alias (ORDER BY an alias is served since res-2)
		"SELECT date_trunc('hour', time), avg(x) AS a FROM cpu GROUP BY 1 HAVING a > 2",
		// alias on a tag key
		"SELECT host AS h, avg(x) FROM cpu GROUP BY host",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (items %v aliases %v)", sql, m.aggItems, m.aggAliases)
		}
	}
}
