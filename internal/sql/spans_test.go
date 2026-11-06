package sql

import (
	"strings"
	"testing"
)

// span helper: the [start,end) of the first occurrence of sub in sql.
func at(sql, sub string) Span {
	i := strings.Index(sql, sub)
	if i < 0 {
		panic("substring not found: " + sub)
	}
	return Span{i, i + len(sub)}
}

func TestProtectedSpans(t *testing.T) {
	for _, tt := range []struct {
		sql  string
		want []string // the exact literal/comment substrings, in order
	}{
		{"SELECT 1", nil},
		{"SELECT * FROM cpu WHERE a = 'x' AND b = 'y'", []string{"'x'", "'y'"}},
		{"SELECT 'it''s' FROM t", []string{"'it''s'"}},
		{`SELECT "id", 'v' FROM t`, []string{`"id"`, "'v'"}},
		{`SELECT e'a\'b', 'c' FROM t`, []string{`e'a\'b'`, "'c'"}},
		{"SELECT $a$ hi 'nope' $a$, 'z' FROM t", []string{"$a$ hi 'nope' $a$", "'z'"}},
		{"SELECT 1 -- 'x'\nFROM t", []string{"-- 'x'"}},
		{"SELECT 1 /* 'x' */ , 'y' FROM t", []string{"/* 'x' */", "'y'"}},
		{"SELECT 'unterminated", []string{"'unterminated"}},
		{"SELECT $a$ unterminated", []string{"$a$ unterminated"}},
		{"SELECT ts$a$ FROM t", nil}, // $ continues the identifier, no literal
	} {
		got := ProtectedSpans(tt.sql)
		var want []Span
		for _, sub := range tt.want {
			want = append(want, at(tt.sql, sub))
		}
		if len(got) != len(want) {
			t.Errorf("%q: got %d spans, want %d (%v)", tt.sql, len(got), len(want), got)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%q: span %d = %q, want %q", tt.sql, i, tt.sql[got[i].Start:got[i].End], tt.want[i])
			}
		}
	}
}

// Every byte MaskStringLiterals replaced with a placeholder must be inside a
// ProtectedSpan, and every code byte must be outside: the two scans agree on
// what is a string literal. Comments are checked by TestProtectedSpans above.
func TestProtectedSpansCoverMaskerLiterals(t *testing.T) {
	for _, sql := range []string{
		"SELECT 'it''s' , \"id\" FROM t WHERE x = e'a\\'b' AND y = $a$z$a$",
		"SELECT col FROM t WHERE time_bucket('1 hour', time') > 0",
		"SELECT '$a$ not a tag', 'ok'",
	} {
		spans := ProtectedSpans(sql)
		_, masks := MaskStringLiterals(sql, true)
		// Each mask's Original appears at some offset that must be covered.
		for _, m := range masks {
			idx := strings.Index(sql, m.Original)
			if idx < 0 {
				continue
			}
			if !InProtectedSpan(spans, idx) || !InProtectedSpan(spans, idx+len(m.Original)-1) {
				t.Errorf("literal %q in %q not fully covered by %v", m.Original, sql, spans)
			}
		}
	}
}

func TestIsMaskerClean(t *testing.T) {
	clean := []string{"time", "cpu.time", "ts", "col_1", "a b", "coalesce", "ts$a$", "t$$$"}
	dirty := []string{"time'", "ts $a$", "a--b", "x/*y", "a'b'c", `id"x`, "a*/b"}
	for _, s := range clean {
		if !IsMaskerClean(s) {
			t.Errorf("want clean: %q", s)
		}
	}
	for _, s := range dirty {
		if IsMaskerClean(s) {
			t.Errorf("want dirty: %q", s)
		}
	}
}

// A randomized property check that ProtectedSpans agrees with the masker and
// the comment rules, in both directions, so the two scans cannot silently
// drift as literal/comment forms evolve (the span-start guard and the
// IsMaskerClean guard both depend on this agreement, #1488).
func TestProtectedSpansPropertyVsMasker(t *testing.T) {
	// An alphabet dense in the bytes that open/close literals and comments.
	alphabet := []byte("ab _,()'\"$e-/*\n01")
	rng := newDet(1488)
	for iter := 0; iter < 20000; iter++ {
		n := rng.intn(24)
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = alphabet[rng.intn(len(alphabet))]
		}
		sql := string(buf)
		spans := ProtectedSpans(sql)

		// Ascending, non-overlapping, in range.
		prev := 0
		litSpans := 0
		for _, s := range spans {
			if s.Start < prev || s.End > len(sql) || s.Start >= s.End {
				t.Fatalf("bad/overlapping span %+v in %q (spans %v)", s, sql, spans)
			}
			prev = s.End
			head := sql[s.Start]
			isComment := strings.HasPrefix(sql[s.Start:], "--") || strings.HasPrefix(sql[s.Start:], "/*")
			if !isComment {
				litSpans++
				// A literal span must begin with a quote opener.
				if head != '\'' && head != '"' && head != '$' && head != 'e' && head != 'E' {
					t.Fatalf("literal span starts with %q in %q", head, sql)
				}
			}
		}

		// Direction 1 (nothing missed): blanking every protected span leaves
		// text the masker sees as fully code — no literal, no comment marker.
		blanked := []byte(sql)
		for _, s := range spans {
			for i := s.Start; i < s.End; i++ {
				blanked[i] = ' '
			}
		}
		rest := string(blanked)
		if m, _ := MaskStringLiterals(rest, HasQuotes(rest)); m != rest {
			t.Fatalf("a literal survived span blanking: %q -> rest %q masks to %q", sql, rest, m)
		}
		if strings.Contains(rest, "--") || strings.Contains(rest, "/*") {
			t.Fatalf("a comment marker survived span blanking: %q -> %q", sql, rest)
		}

		// Direction 2 (nothing over-marked): each literal span is EXACTLY one
		// literal — masking its own text collapses it to a single placeholder
		// with no residual code. (A count check cannot be used: the masker
		// deduplicates identical quoted identifiers into one mask entry, while
		// ProtectedSpans records every physical occurrence.)
		for _, sp := range spans {
			lit := sql[sp.Start:sp.End]
			if strings.HasPrefix(lit, "--") || strings.HasPrefix(lit, "/*") {
				continue
			}
			m, _ := MaskStringLiterals(lit, true)
			if !isSinglePlaceholder(m) {
				t.Fatalf("literal span %q is not exactly one literal (masks to %q) in %q", lit, m, sql)
			}
		}
		_ = litSpans
	}
}

// isSinglePlaceholder reports whether s is exactly one __STR_n__/__IDENT_n__
// token and nothing else.
func isSinglePlaceholder(s string) bool {
	if !strings.HasPrefix(s, "__STR_") && !strings.HasPrefix(s, "__IDENT_") {
		return false
	}
	return strings.HasSuffix(s, "__") && strings.Count(s, "__") == 2
}

// newDet / det: a tiny deterministic LCG so the fuzz is reproducible without
// depending on the test seed or importing math/rand's global state.
type det struct{ s uint64 }

func newDet(seed uint64) *det { return &det{s: seed*2862933555777941757 + 1} }
func (d *det) next() uint64   { d.s = d.s*6364136223846793005 + 1442695040888963407; return d.s }
func (d *det) intn(n int) int { return int(d.next() >> 33 % uint64(n)) }
