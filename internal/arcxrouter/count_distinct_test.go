// cd-1 companion: the recognizer routes `count(DISTINCT <col>)` exactly where the
// engine serves it — ungrouped (bare column or map subscript), grouped under a tag
// or a time bucket, aliased, in HAVING (selected or hidden) and as an ORDER BY
// target — and nowhere else. Pure tokenizer tests.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestScanAggCountDistinct(t *testing.T) {
	cases := []struct {
		sql     string
		items   []string
		aliases []string
	}{
		{"SELECT count(DISTINCT norad_id) FROM sat", []string{"count(DISTINCT norad_id)"}, []string{""}},
		// Keyword case is free; the column keeps its spelling; re-emitted upper-case.
		{"SELECT COUNT(distinct Host), count( DISTINCT  v ) FROM cpu WHERE v > 1",
			[]string{"count(DISTINCT Host)", "count(DISTINCT v)"}, []string{"", ""}},
		{"SELECT count(*), count(DISTINCT host) AS hosts, count(host) FROM cpu",
			[]string{"count(*)", "count(DISTINCT host)", "count(host)"}, []string{"", "hosts", ""}},
		{"SELECT count(DISTINCT tags['host']) FROM cpu", []string{"count(DISTINCT tags['host'])"}, []string{""}},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if m.shape != ShapeScanAgg {
			t.Fatalf("shape %q, want %q: %s", m.shape, ShapeScanAgg, c.sql)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) || !reflect.DeepEqual(m.aggAliases, c.aliases) {
			t.Fatalf("items %v aliases %v, want %v %v: %s", m.aggItems, m.aggAliases, c.items, c.aliases, c.sql)
		}
	}
}

// The footer scalar shape (count(<col>) answered from null counts) must never
// take a DISTINCT count — the engine's C3 regression, mirrored.
func TestCountDistinctIsNotTheFooterCountShape(t *testing.T) {
	toks, ok := tokenize("SELECT count(DISTINCT host) FROM cpu")
	if !ok {
		t.Fatal("lex")
	}
	if _, _, _, _, ok := matchScalarAgg(toks); ok {
		t.Fatal("matchScalarAgg must reject count(DISTINCT x)")
	}
	m, ok := eligibleShape("SELECT count(DISTINCT host) FROM cpu")
	if !ok || m.shape != ShapeScanAgg {
		t.Fatalf("want the decode shape, got %q ok=%v", m.shape, ok)
	}
}

func TestGroupedCountDistinct(t *testing.T) {
	cases := []struct {
		sql    string
		items  []string
		having string
		hidden int
		order  []OrderKey
		limit  int
	}{
		{sql: "SELECT date_trunc('hour', time), count(DISTINCT norad_id) FROM sat GROUP BY 1 ORDER BY 1",
			items: []string{"date_trunc('hour', time)", "count(DISTINCT norad_id)"},
			order: []OrderKey{{Pos: 1}}},
		{sql: "SELECT host, count(DISTINCT id) AS ids, avg(v) FROM cpu WHERE v > 1 GROUP BY host",
			items: []string{"host", "count(DISTINCT id)", "avg(v)"}},
		{sql: "SELECT date_trunc('minute', time), host, count(DISTINCT id) FROM cpu GROUP BY 1, 2",
			items: []string{"date_trunc('minute', time)", "host", "count(DISTINCT id)"}},
		// HAVING on the selected item, by expression and by alias.
		{sql: "SELECT host, count(DISTINCT id) AS ids FROM cpu GROUP BY host HAVING count(DISTINCT id) > 3",
			items: []string{"host", "count(DISTINCT id)"}, having: "count(DISTINCT id) > 3"},
		{sql: "SELECT host, count(DISTINCT id) AS ids FROM cpu GROUP BY host HAVING ids > 3",
			items: []string{"host", "count(DISTINCT id)"}, having: "count(DISTINCT id) > 3"},
		// res-3b: a hidden DISTINCT aggregate, distinct from the selected count(id).
		{sql: "SELECT host, count(id) FROM cpu GROUP BY host HAVING count(DISTINCT id) >= 2",
			items: []string{"host", "count(id)"}, having: "count(DISTINCT id) >= 2", hidden: 1},
		// res-2: an ORDER BY target, by expression and by alias, with LIMIT.
		{sql: "SELECT host, count(DISTINCT id) FROM cpu GROUP BY host ORDER BY count(DISTINCT id) DESC LIMIT 5",
			items: []string{"host", "count(DISTINCT id)"}, order: []OrderKey{{Pos: 2, Desc: true}}, limit: 5},
		{sql: "SELECT host, count(DISTINCT id) AS ids FROM cpu GROUP BY host ORDER BY ids DESC, host",
			items: []string{"host", "count(DISTINCT id)"}, order: []OrderKey{{Pos: 2, Desc: true}, {Pos: 1}}},
	}
	for _, c := range cases {
		toks, ok := tokenize(c.sql)
		if !ok {
			t.Fatalf("lex: %s", c.sql)
		}
		gm, ok := matchGroupedAgg(toks)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if !reflect.DeepEqual(gm.items, c.items) {
			t.Fatalf("items %v, want %v: %s", gm.items, c.items, c.sql)
		}
		if gm.havingText != c.having || gm.havingHidden != c.hidden {
			t.Fatalf("having %q hidden %d, want %q %d: %s", gm.havingText, gm.havingHidden, c.having, c.hidden, c.sql)
		}
		if len(c.order) > 0 && !reflect.DeepEqual(gm.orderKeys, c.order) {
			t.Fatalf("order %v, want %v: %s", gm.orderKeys, c.order, c.sql)
		}
		if c.limit > 0 && (!gm.hasLimit || gm.limit != c.limit) {
			t.Fatalf("limit %d/%v, want %d: %s", gm.limit, gm.hasLimit, c.limit, c.sql)
		}
	}
}

func TestCountDistinctDecline(t *testing.T) {
	for _, sql := range []string{
		// Not served by the engine (user-error class / row form / other aggregates).
		"SELECT count(DISTINCT a, b) FROM t",
		"SELECT count(DISTINCT (a, b)) FROM t",
		"SELECT count(DISTINCT *) FROM t",
		"SELECT count(DISTINCT a ORDER BY b) FROM t",
		"SELECT count(DISTINCT a + 1) FROM t",
		"SELECT count(DISTINCT end) FROM t",
		"SELECT count(DISTINCT) FROM t",
		"SELECT sum(DISTINCT a) FROM t",
		"SELECT avg(DISTINCT a) FROM t",
		"SELECT approx_count_distinct(a) FROM t",
		"SELECT DISTINCT host FROM t",
		// Grouped: a map-subscript argument is not routed grouped.
		"SELECT host, count(DISTINCT tags['k']) FROM t GROUP BY host",
		// A DISTINCT count that is not a select item cannot be an ORDER BY target.
		"SELECT host, count(id) FROM t GROUP BY host ORDER BY count(DISTINCT id)",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (items %v)", sql, m.aggItems)
		}
	}
}
