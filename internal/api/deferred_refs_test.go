package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
)

func TestSentinelScopes(t *testing.T) {
	s := string(refSentinel)
	for _, tt := range []struct {
		name, sql string
		want      []int
	}{
		{"one FROM", "SELECT * FROM " + s + " JOIN " + s + " USING (a)", []int{1, 1}},
		{"a parenthesised condition returns to its scope", "SELECT * FROM " + s + " JOIN " + s + " USING (a) JOIN " + s + " ON (x)", []int{1, 1, 1}},
		{"subquery", "SELECT * FROM " + s + " WHERE a IN (SELECT a FROM " + s + ")", []int{1, 3}},
		{"UNION arms", "SELECT * FROM " + s + " UNION ALL SELECT * FROM " + s, []int{1, 2}},
		{"FROM-first", "FROM " + s + s, []int{0, 0}},
		{"select inside a word is not SELECT", "SELECT selected FROM " + s + " JOIN " + s, []int{1, 1}},
		{"unbalanced close", ") FROM " + s, []int{0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sentinelScopes(tt.sql, len(tt.want)))
		})
	}
}

// Each pass keeps the expressions in the order their sentinels appear,
// however the passes interleave their references.
func TestDeferredRefsKeepOrderAcrossPasses(t *testing.T) {
	h := newCommaJoinTestHandler()
	got := h.convertSQLToStoragePaths(context.Background(),
		"SELECT * FROM a, b JOIN c ON true, d JOIN x.e ON true, x.f LEFT JOIN g ON true")
	want := []string{"default/a/", "default/b/", "default/c/", "default/d/", "x/e/", "x/f/", "default/g/"}
	last := -1
	for _, w := range want {
		i := strings.Index(got, w)
		require.Greater(t, i, last, "%s out of order in\n%s", w, got)
		require.Equal(t, 1, strings.Count(got, w), got)
		last = i
	}
	require.NotContains(t, got, string(refSentinel))
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		require.Contains(t, got, `AS "`+name+`"`, "every table in this FROM is named (#1483)")
	}
}

func TestOneTableAScopeIsNotRenamed_Issue1483(t *testing.T) {
	h := newCommaJoinTestHandler()
	for _, sql := range []string{
		"SELECT * FROM cpu",
		"SELECT * FROM cpu WHERE host IN (SELECT host FROM mem)",
		"SELECT * FROM cpu UNION ALL SELECT * FROM mem",
		"WITH m AS (SELECT * FROM mem) SELECT * FROM cpu JOIN m USING (host)",
	} {
		got := h.convertSQLToStoragePaths(context.Background(), sql)
		require.NotContains(t, got, ` AS "`, "%s\n%s", sql, got)
		got = h.convertSQLToStoragePathsWithHeaderDB(context.Background(), sql, "plant")
		require.NotContains(t, got, ` AS "`, "%s\n%s", sql, got)
	}
}

func TestNamesTheQueryUsesAreNotGivenToATable_Issue1483(t *testing.T) {
	h := newCommaJoinTestHandler()
	for _, tt := range []struct{ sql, notWant string }{
		// A CTE of that name is in the same scope.
		{"WITH cpu AS (SELECT * FROM x.cpu) SELECT * FROM cpu JOIN y.cpu USING (time) JOIN mem USING (time)", `AS "cpu"`},
		// Another relation is aliased with it (#1450).
		{"SELECT * FROM cpu JOIN mem USING (host) JOIN (SELECT 1 AS host) cpu USING (host)", `AS "cpu"`},
		// The reference has an alias of its own.
		{"SELECT * FROM cpu c JOIN mem USING (host)", `AS "cpu"`},
	} {
		got := h.convertSQLToStoragePaths(context.Background(), tt.sql)
		require.NotContains(t, got, tt.notWant, "%s\n%s", tt.sql, got)
		require.Contains(t, got, `AS "mem"`, "%s\n%s", tt.sql, got)
	}
}

// A NUL outside a string literal would read as a rewritten reference, and
// DuckDB would stop reading the statement at it: refused. One inside a
// literal is masked before the passes run and comes back unchanged.
func TestNULOutsideAStringLiteralIsRefused(t *testing.T) {
	h := newCommaJoinTestHandler()
	h.queryCache = database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize)

	_, _, _, err := h.getTransformedSQLWithRefs(context.Background(), "SELECT * FROM cpu\x00, mem", "")
	require.ErrorIs(t, err, errSQLContainsNUL)
	require.Equal(t, fiber.StatusBadRequest, transformErrorStatus(err))

	got, _, _, err := h.getTransformedSQLWithRefs(context.Background(), "SELECT * FROM cpu WHERE host = 'a\x00b'", "")
	require.NoError(t, err)
	require.Contains(t, got, "'a\x00b'")
	require.Contains(t, got, "default/cpu/")
}

// A transform whose request deadline passed is refused as a timeout, never
// run or cached: a reference it left unrewritten could keep a quoted,
// path-shaped name (#1482). Both the cached path and the single-table fast
// path check.
func TestTransformRefusedOnceTheDeadlinePasses_Issue1482(t *testing.T) {
	h := newCommaJoinTestHandler()
	h.queryCache = database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	for _, tt := range []struct{ name, sql, db string }{
		{"cached path", "SELECT * FROM cpu, mem", ""},
		{"header path", "SELECT * FROM cpu JOIN mem USING (host)", "plant"},
		{"single-table fast path", "SELECT * FROM cpu", "plant"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, _, _, err := h.getTransformedSQLForQueryWithRefs(expired, tt.sql, tt.db)
			require.ErrorIs(t, err, errQueryTimedOut)
			require.Empty(t, got)
			require.Equal(t, fiber.StatusGatewayTimeout, transformErrorStatus(err))
			require.Zero(t, h.queryCache.Size())
		})
	}

	cancelled, stop := context.WithCancel(context.Background())
	stop()
	_, _, _, err := h.getTransformedSQLWithRefs(cancelled, "SELECT * FROM cpu, mem", "")
	require.True(t, errors.Is(err, context.Canceled), "%v", err)
	require.Equal(t, fiber.StatusServiceUnavailable, transformErrorStatus(err))
	require.Zero(t, h.queryCache.Size())
}

// The passes stop rewriting once the context ends, checked every
// cancelCheckInterval references, and record why.
func TestDeferredRefsStopWhenTheRequestEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, failure := withStoragePathFailure(ctx)
	d := newDeferredRefs(ctx, nil, nil, nil)
	sql := "SELECT 1 FROM x" + strings.Repeat(", x", 3*cancelCheckInterval)
	calls := 0
	out := d.replaceComma(sql, func(ref commaJoinRef) (deferredRef, bool) {
		calls++
		if calls == cancelCheckInterval/2 {
			cancel()
		}
		return deferredRef{expr: ", y"}, true
	})
	require.Equal(t, cancelCheckInterval-1, calls, "the check at the 1024th reference stops the pass")
	require.Len(t, d.refs, calls)
	require.Equal(t, calls, strings.Count(out, string(refSentinel)))
	require.ErrorIs(t, failure.err, context.Canceled)
}
