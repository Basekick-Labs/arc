package sql

import (
	"strings"
	"testing"
)

func TestForLogMasksLiterals(t *testing.T) {
	cases := []struct{ in, want string }{
		{`SELECT v FROM cpu WHERE host = 'prod-db-07'`,
			`SELECT v FROM cpu WHERE host = '...'`},
		// Doubled-quote escape stays inside ONE literal.
		{`WHERE note = 'It''s secret'`, `WHERE note = '...'`},
		// THE BACKSLASH CASE (the shared masker's leak): backslash is a literal
		// char in standard strings, so the first literal ends at the next quote
		// and the second literal's content must NOT surface.
		{`WHERE a = 'x\' AND password = 'secret123'`,
			`WHERE a = '...' AND password = '...'`},
		// E-strings DO honor backslash: \' stays inside.
		{`WHERE a = E'x\' still inside' AND b = 'v'`,
			`WHERE a = E'...' AND b = '...'`},
		// Dollar quoting.
		{`WHERE a = $$raw secret$$`, `WHERE a = $$...$$`},
		{`WHERE a = $tag$raw ' secret$tag$ AND b='v'`, `WHERE a = $$...$$ AND b='...'`},
		// Quoted identifiers survive; a quote inside one can't derail scanning.
		{`SELECT "weird""col" FROM t WHERE x='v'`, `SELECT "weird""col" FROM t WHERE x='...'`},
		// Comments are user text.
		{"SELECT v FROM t -- note: acme-corp\nWHERE x='v'", "SELECT v FROM t --...\nWHERE x='...'"},
		{`SELECT v /* acme */ FROM t`, `SELECT v /*...*/ FROM t`},
		// Unterminated masks to the end (fail closed).
		{`WHERE a = 'never closed and secret`, `WHERE a = '...'`},
		// Identifier ending in E is not an E-string prefix.
		{`SELECT CASE WHEN mode='x' THEN 1 END`, `SELECT CASE WHEN mode='...' THEN 1 END`},
	}
	for _, c := range cases {
		if got := ForLog(c.in); got != c.want {
			t.Errorf("ForLog(%q)\n  got  %q\n  want %q", c.in, got, c.want)
		}
	}
}

func TestForLogSentinels(t *testing.T) {
	probes := []string{
		`SELECT v FROM cpu WHERE host = 'ZZSENTINELZZ' AND t = 'ZZ2'`,
		`WHERE a = 'x\' AND b = 'ZZSENTINELZZ'`,
		`WHERE a = E'\' ZZSENTINELZZ'`,
		`WHERE a = $q$ZZSENTINELZZ$q$`,
		"-- ZZSENTINELZZ\nSELECT 1",
		`WHERE a = 'ZZSENTINELZZ`, // unterminated
	}
	for _, p := range probes {
		if got := ForLog(p); strings.Contains(got, "ZZSENTINELZZ") {
			t.Errorf("sentinel leaked: ForLog(%q) = %q", p, got)
		}
	}
}

func TestMaskErrTextMasksBothQuoteStyles(t *testing.T) {
	in := `Conversion Error: Could not convert string 'secret-host-42' to INT64 near "2026-99-99"`
	got := MaskErrText(in)
	for _, leak := range []string{"secret-host-42", "2026-99-99"} {
		if strings.Contains(got, leak) {
			t.Errorf("leaked %q in %q", leak, got)
		}
	}
	if !strings.Contains(got, "Could not convert string") {
		t.Errorf("message class lost: %q", got)
	}
}
