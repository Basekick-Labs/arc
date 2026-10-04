package arcxrouter

import "testing"

// agg-5a: HAVING joins the grouped allow-list. The emitted engine SQL must be
// rebuilt from validated parts — item text for aggregates, validated identifiers
// for keys, re-emitted literals — so a shape that reaches the engine is always one
// the engine's own (identical) grammar accepts.
func TestHavingEligibility(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string // expected HAVING text in the engine SQL
	}{
		{"agg on a tag-keyed group", "SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1", "count(*) > 1"},
		{"key comparison", "SELECT host, count(*) FROM cpu GROUP BY host HAVING host = 'web1'", "host = 'web1'"},
		{"AND tree", "SELECT host, count(*), sum(x) FROM cpu GROUP BY host HAVING count(*) > 1 AND sum(x) > 2", "count(*) > 1 AND sum(x) > 2"},
		{"OR tree", "SELECT host, count(*), sum(x) FROM cpu GROUP BY host HAVING count(*) > 1 OR sum(x) > 2", "count(*) > 1 OR sum(x) > 2"},
		{"parens", "SELECT host, count(*), sum(x) FROM cpu GROUP BY host HAVING (count(*) > 1 OR sum(x) > 2) AND host != 'a'", "(count(*) > 1 OR sum(x) > 2) AND host != 'a'"},
		{"bucket + HAVING", "SELECT date_trunc('day', time), count(*) FROM cpu GROUP BY 1 HAVING count(*) > 1", "count(*) > 1"},
		{"bucket alias referenced in HAVING", "SELECT date_trunc('day', time) AS h, count(*) FROM cpu GROUP BY 1 HAVING h > '2024-01-01T00:00:00Z'", "h > '2024-01-01T00:00:00Z'"},
		{"HAVING then ORDER BY", "SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1 ORDER BY 1", "count(*) > 1"},
		{"spacing is normalised", "SELECT host, count( * ) FROM cpu GROUP BY host HAVING count( * )  >  1", "count(*) > 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			toks, lexed := tokenize(tc.sql)
			if !lexed {
				t.Fatalf("tokenize failed: %q", tc.sql)
			}
			gm, ok := matchGroupedAgg(toks)
			if !ok {
				t.Fatalf("expected grouped eligibility: %q", tc.sql)
			}
			if gm.havingText != tc.want {
				t.Fatalf("havingText = %q, want %q", gm.havingText, tc.want)
			}
		})
	}
}

// A literal with an embedded quote must be re-emitted with standard doubling, not
// copied from source — the router never passes user bytes into the engine SQL.
func TestHavingLiteralIsReEmittedNotCopied(t *testing.T) {
	toks, lexed := tokenize(`SELECT host, count(*) FROM cpu GROUP BY host HAVING host = 'a''b'`)
	if !lexed {
		t.Skip("spelling not lexable; already a decline")
	}
	gm, ok := matchGroupedAgg(toks)
	if !ok {
		t.Skip("not eligible; already a decline")
	}
	if gm.havingText != "host = 'a''b'" {
		t.Fatalf("havingText = %q, want %q", gm.havingText, "host = 'a''b'")
	}
}

// An aggregate spelled differently than the select list must NOT half-match: DuckDB
// canonicalises `max_by` to `arg_max`, our text match does not, so it declines.
func TestHavingAggSpellingMismatchDeclines(t *testing.T) {
	for _, sql := range []string{
		"SELECT host, arg_max(value, time) FROM cpu GROUP BY host HAVING max_by(value, time) > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING avg(value) > 1",
	} {
		toks, lexed := tokenize(sql)
		if !lexed {
			continue
		}
		if _, ok := matchGroupedAgg(toks); ok {
			t.Fatalf("spelling mismatch must decline: %q", sql)
		}
	}
}
