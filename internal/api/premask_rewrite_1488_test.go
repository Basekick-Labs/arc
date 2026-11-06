package api

import (
	"context"
	"strings"
	"testing"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// A rewrite target that must not fire — a dirty column argument, or a call
// written inside a string literal — survives verbatim into the transformed
// SQL, so the rewrite cannot change the statement's structure (#1488).
// (Comments are a separate matter: stripSQLComments removes them, so a call
// inside a comment is gone from the output entirely; that is safe and is not
// checked here.)
func TestUnsafeRewriteTargetsSurviveVerbatim_Issue1488(t *testing.T) {
	h := newCommaJoinTestHandler()
	cases := []struct{ sql, mustContain string }{
		{"SELECT time_bucket('1 hour', time') FROM cpu", "time_bucket('1 hour', time')"},
		{"SELECT date_trunc('hour', time') FROM cpu", "date_trunc('hour', time')"},
		{"SELECT time_bucket('1 hour', ts $a$ x $a$ ) FROM cpu", "time_bucket('1 hour', ts $a$ x $a$ )"},
		{`SELECT 'date_trunc(''hour'', t)' AS doc FROM cpu`, `'date_trunc(''hour'', t)'`},
		// A dollar-quoted string holds the call unescaped, so the regex would
		// match it; the span-start skip must leave it alone.
		{"SELECT $$ time_bucket('1 hour', x) $$ AS doc FROM cpu", "$$ time_bucket('1 hour', x) $$"},
	}
	for _, tt := range cases {
		out := h.convertSQLToStoragePaths(context.Background(), tt.sql)
		if !strings.Contains(out, tt.mustContain) {
			t.Errorf("expected %q to survive verbatim\n in:  %q\n out: %q", tt.mustContain, tt.sql, out)
		}
		if strings.Contains(out, "make_timestamptz") {
			t.Errorf("unsafe call was rewritten\n in:  %q\n out: %q", tt.sql, out)
		}
	}
}

// The time rewrites leave a call unrewritten when its column argument is not
// masker-clean, so a quote there cannot desync the statement (#1488).
func TestTimeRewriteSkipsDirtyColumn_Issue1488(t *testing.T) {
	timeBucket := []string{
		"SELECT time_bucket('1 hour', time') FROM cpu",
		"SELECT time_bucket('1 minute', ts $a$ $a$) FROM cpu",
		"SELECT time_bucket('1 hour', c /* x */) FROM cpu",
	}
	for _, sql := range timeBucket {
		if got := rewriteTimeBucket(sql); got != sql {
			t.Errorf("time_bucket rewrote a dirty column: %q -> %q", sql, got)
		}
	}
	dateTrunc := []string{
		"SELECT date_trunc('hour', time') FROM cpu",
		"SELECT date_trunc('day', c /* x */) FROM cpu",
	}
	for _, sql := range dateTrunc {
		if got := rewriteDateTrunc(sql); got != sql {
			t.Errorf("date_trunc rewrote a dirty column: %q -> %q", sql, got)
		}
	}
}

// Normal queries transform exactly as before: the guards are no-ops when there
// is no stray quote and the call is not inside a literal/comment.
func TestCleanRewritesUnchanged_Issue1488(t *testing.T) {
	for _, tt := range []struct{ sql, must string }{
		{"SELECT time_bucket('1 hour', time) FROM cpu", "make_timestamptz"},
		{"SELECT date_trunc('hour', time) FROM cpu", "make_timestamptz"},
		{"SELECT date_trunc('hour', cpu.time) FROM cpu", "epoch_us(cpu.time)"},
	} {
		got := rewriteDateTrunc(rewriteTimeBucket(tt.sql))
		if !strings.Contains(got, tt.must) {
			t.Errorf("clean query not rewritten: %q -> %q", tt.sql, got)
		}
	}
	// LIKE reorder still fires on the clean ClickBench shape.
	like := "SELECT 1 FROM t WHERE URL LIKE '%google%' AND SearchPhrase <> ''"
	got, changed := OptimizeLikePatterns(like)
	w := got[strings.Index(got, "WHERE"):]
	if !changed || !strings.HasPrefix(w, "WHERE SearchPhrase <> ''") {
		t.Errorf("clean LIKE not reordered: %q -> %q (changed=%v)", like, got, changed)
	}
}

// The LIKE reorder does not move a predicate that hides a comment marker, and
// the spanStraddles primitive refuses a boundary cutting a literal (#1488).
func TestLikeReorderSkipsUnsafe_Issue1488(t *testing.T) {
	// A moved middle predicate carrying a comment marker is left in place
	// (conservative: it could comment out a following predicate once moved).
	withComment := "SELECT 1 FROM t WHERE x LIKE '%a%' /* c */ AND y <> ''"
	if got, changed := OptimizeLikePatterns(withComment); changed {
		t.Errorf("reordered a predicate with a comment marker: %q -> %q", withComment, got)
	}
}

func TestSpanStraddles(t *testing.T) {
	// "a 'bc' d": literal at [2,6). A boundary strictly inside it straddles;
	// an edge-touching or code boundary does not.
	spans := sqlutilProtectedSpans("a 'bc' d")
	for pos, want := range map[int]bool{0: false, 2: false, 3: true, 5: true, 6: false, 7: false} {
		if got := spanStraddles(spans, pos); got != want {
			t.Errorf("spanStraddles(%d) = %v, want %v", pos, got, want)
		}
	}
}

func sqlutilProtectedSpans(sql string) []sqlutil.Span { return sqlutil.ProtectedSpans(sql) }
