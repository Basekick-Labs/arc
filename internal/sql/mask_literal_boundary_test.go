package sql

import "testing"

// DuckDB's three quoted forms do not share one escape rule: a plain '…' string
// and a "…" identifier end at the first undoubled quote, with backslash an
// ordinary character, while E'…' honours backslash. The masker used to apply
// the backslash rule to all of them, so a literal ending in a backslash ran the
// scan past its own closing quote and swallowed the rest of the statement into
// one placeholder. Every consumer of the masked form then saw a statement
// shorter than the one DuckDB would parse.
func TestMaskStringLiterals_LiteralBoundariesFollowDuckDB(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		masked string
		masks  int
	}{
		{
			// The trailing quote sits in a comment, which is copied through
			// whole, so it opens nothing. What matters is that the
			// table-position literal in the middle is its own placeholder and
			// no longer hidden inside the first one.
			name:   "plain literal ending in a backslash ends at its own quote",
			sql:    `WHERE host = '\' UNION ALL SELECT * FROM '/other/x.parquet' -- '`,
			masked: `WHERE host = __STR_0__ UNION ALL SELECT * FROM __STR_1__ -- '`,
			masks:  2,
		},
		{
			name:   "windows path value does not shift the next literal",
			sql:    `WHERE dir = 'C:\' AND name = 'report.csv'`,
			masked: `WHERE dir = __STR_0__ AND name = __STR_1__`,
			masks:  2,
		},
		{
			name:   "doubled quote still escapes in a plain literal",
			sql:    `WHERE note = 'it''s here' AND x = 1`,
			masked: `WHERE note = __STR_0__ AND x = 1`,
			masks:  1,
		},
		{
			name:   "E-string still honours a backslash-escaped quote",
			sql:    `WHERE note = E'it\'s here' AND x = 1`,
			masked: `WHERE note = __STR_0__ AND x = 1`,
			masks:  1,
		},
		{
			name:   "E-string ending in an escaped backslash ends at its own quote",
			sql:    `WHERE dir = E'C:\\' AND name = 'report.csv'`,
			masked: `WHERE dir = __STR_0__ AND name = __STR_1__`,
			masks:  2,
		},
		{
			name:   "quoted identifier ending in a backslash ends at its own quote",
			sql:    `SELECT "weird\" FROM t WHERE x = 'a'`,
			masked: `SELECT __IDENT_0__ FROM t WHERE x = __STR_1__`,
			masks:  2,
		},
		{
			name:   "dollar-quoted body is untouched by the backslash rule",
			sql:    `WHERE note = $$a\$$ AND x = 1`,
			masked: `WHERE note = __STR_0__ AND x = 1`,
			masks:  1,
		},
		{
			// A quote inside a comment used to open a literal that ran to the
			// next quote, swallowing the newline the comment stripper needs to
			// find the comment's end; the stripper then deleted everything to
			// the end of the statement and the gates saw none of it.
			name:   "a quote in a line comment opens nothing",
			sql:    "SELECT 0 -- '\nUNION ALL SELECT * FROM read_parquet('/other/x.parquet')",
			masked: "SELECT 0 -- '\nUNION ALL SELECT * FROM read_parquet(__STR_0__)",
			masks:  1,
		},
		{
			name:   "a quote in a block comment opens nothing",
			sql:    `SELECT 0 /* ' */ UNION ALL SELECT * FROM read_parquet('/other/x.parquet')`,
			masked: `SELECT 0 /* ' */ UNION ALL SELECT * FROM read_parquet(__STR_0__)`,
			masks:  1,
		},
		{
			// DuckDB accepts `$` inside an unquoted identifier, so `t$$$` is one
			// name. Reading its second and third `$` as a dollar-quote opener
			// found no closing tag and masked the rest of the statement away.
			name:   "a dollar run continuing an identifier opens nothing",
			sql:    `SELECT * FROM cpu AS t$$$ UNION ALL SELECT * FROM read_parquet('/other/x.parquet')`,
			masked: `SELECT * FROM cpu AS t$$$ UNION ALL SELECT * FROM read_parquet(__STR_0__)`,
			masks:  1,
		},
		{
			name:   "a real dollar-quoted literal is still masked",
			sql:    `SELECT * FROM cpu WHERE note = $tag$body$tag$ AND x = 1`,
			masked: `SELECT * FROM cpu WHERE note = __STR_0__ AND x = 1`,
			masks:  1,
		},
		{
			name:   "a quote inside a quoted identifier stays inside it",
			sql:    `SELECT 1 AS "a'b", read_parquet('/other/x.parquet')`,
			masked: `SELECT 1 AS __IDENT_0__, read_parquet(__STR_1__)`,
			masks:  2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			masked, masks := MaskStringLiterals(tc.sql, HasQuotes(tc.sql))
			if masked != tc.masked {
				t.Errorf("masked =\n  %q\nwant\n  %q", masked, tc.masked)
			}
			if len(masks) != tc.masks {
				t.Errorf("produced %d masks, want %d: %+v", len(masks), tc.masks, masks)
			}
			// Round-trip: substituting every placeholder back must rebuild the
			// input exactly, or a rewriter would emit SQL the user never wrote.
			restored := masked
			for _, m := range masks {
				restored = replaceAll(restored, m.Placeholder, m.Original)
			}
			if restored != tc.sql {
				t.Errorf("round trip =\n  %q\nwant\n  %q", restored, tc.sql)
			}
		})
	}
}

func replaceAll(s, old, new string) string {
	for {
		next := replaceOnce(s, old, new)
		if next == s {
			return s
		}
		s = next
	}
}

func replaceOnce(s, old, new string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}
