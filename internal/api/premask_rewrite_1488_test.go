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
	// The 3-arg (with-origin) form takes the same column guard.
	timeBucket3 := []string{
		"SELECT time_bucket('1 hour', time', TIMESTAMP '2024-01-01 00:00:00') FROM cpu",
		"SELECT time_bucket('1 hour', ts $a$ $a$, TIMESTAMP '2024-01-01 00:00:00') FROM cpu",
	}
	for _, sql := range timeBucket3 {
		if got := rewriteTimeBucket(sql); got != sql {
			t.Errorf("3-arg time_bucket rewrote a dirty column: %q -> %q", sql, got)
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
	// Exercise reorderEmptyCheckBeforeLike specifically (the WHERE col LIKE
	// 'pat' AND col2 <> '' shape): a comment in the LIKE pattern's predicate
	// must not be reordered ahead of the empty check.
	emptyCheckShape := "SELECT 1 FROM t WHERE url LIKE '%a%' -- c\n AND sp <> ''"
	if got, changed := OptimizeLikePatterns(emptyCheckShape); changed {
		t.Errorf("reorderEmptyCheckBeforeLike moved a predicate with a comment: %q -> %q", emptyCheckShape, got)
	}
}

// The PR's whole contract: OptimizeLikePatterns never changes which bytes are
// inside a string literal or comment. If a reorder ever minted or dropped a
// literal/comment boundary, the SQL DuckDB runs would stop matching the SQL
// ValidateSQLRequest approved (#1488, #1490 review). Checked two-sided over a
// corpus of predicate fragments, including the pieces that can splice into a
// new opener at a reorder seam (lone quote, dollar fragment, comment marker).
func TestLikeReorderPreservesLiteralPartition_Issue1488(t *testing.T) {
	frags := []string{
		"url LIKE '%a%'", "sp <> ''", "x = 1", "y = 'v'",
		"k = $tag$b", "k2 = $q", "m = '", "n = $$r", "p /* c */ = 2",
		"q LIKE '%b%'", "r <> ''", "s = $a$t$a$",
	}
	tails := []string{"", " GROUP BY z", " ORDER BY z", " LIMIT 10"}
	n := 0
	for _, a := range frags {
		for _, b := range frags {
			for _, c := range frags {
				for _, tl := range tails {
					sql := "SELECT 1 FROM t WHERE " + a + " AND " + b + " AND " + c + tl
					got, _ := OptimizeLikePatterns(sql)
					if in, out := protectedSpanContents(sql), protectedSpanContents(got); !equalStrings(in, out) {
						t.Errorf("reorder changed the literal/comment partition\n in:  %q\n out: %q\n spans in=%v out=%v", sql, got, in, out)
					}
					n++
				}
			}
		}
	}
	t.Logf("checked %d reorder inputs", n)
}

// sameProtectedPartition accepts a genuine reordering of the same literal and
// comment regions and rejects one whose regions differ.
func TestSameProtectedPartition_Issue1488(t *testing.T) {
	// Same literals, reordered: accepted.
	if !sameProtectedPartition("WHERE a LIKE '%x%' AND b <> ''", "WHERE b <> '' AND a LIKE '%x%'") {
		t.Error("rejected a reorder that keeps the same literals")
	}
	// A candidate whose quote now encloses different text (a literal grew to
	// swallow code that was executable before) has different span content.
	if sameProtectedPartition("'p' AND x", "'p AND x'") {
		t.Error("accepted a candidate that merged code into a literal")
	}
	// A candidate whose comment now reaches further is a different partition.
	if sameProtectedPartition("x -- c\n", "x -- c AND y\n") {
		t.Error("accepted a candidate that changes a comment's reach")
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
