package api

import (
	"context"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
)

// sqlOfLength is a valid SELECT exactly n bytes long.
func sqlOfLength(n int) string {
	head, tail := "SELECT '", "' AS s"
	return head + strings.Repeat("x", n-len(head)-len(tail)) + tail
}

// Without SetMaxSQLLength (tests, or anything that runs before main sets it)
// the cap is the default, not unlimited.
func TestValidateSQLRequestDefaultCap_Issue1448(t *testing.T) {
	SetMaxSQLLength(0)
	if err := ValidateSQLRequest(sqlOfLength(config.DefaultMaxSQLLength)); err != nil {
		t.Fatalf("a statement at the default cap was refused: %v", err)
	}
	err := ValidateSQLRequest(sqlOfLength(config.DefaultMaxSQLLength + 1))
	if err == nil || !strings.Contains(err.Error(), "query.max_sql_length") {
		t.Fatalf("a statement over the default cap: %v", err)
	}
}

func TestValidateSQLRequestHonoursMaxSQLLength_Issue1448(t *testing.T) {
	SetMaxSQLLength(50000)
	t.Cleanup(func() { SetMaxSQLLength(0) })
	if err := ValidateSQLRequest(sqlOfLength(50000)); err != nil {
		t.Fatalf("a statement at the raised cap was refused: %v", err)
	}
	err := ValidateSQLRequest(sqlOfLength(50001))
	if err == nil || !strings.Contains(err.Error(), "50000 characters") {
		t.Fatalf("a statement over the raised cap: %v", err)
	}
}

// A statement longer than the default cap is transformed but not cached: the
// cache is bounded by entry count, so caching long statements would multiply
// its memory bound.
func TestTransformCacheSkipsLongStatements_Issue1448(t *testing.T) {
	SetMaxSQLLength(config.MaxSQLLengthCeiling)
	t.Cleanup(func() { SetMaxSQLLength(0) })
	h := newCommaJoinTestHandler()
	h.queryCache = database.NewQueryCache(database.QueryCacheTTL, 100)

	short := "SELECT * FROM cpu WHERE host = 'a'"
	long := "SELECT * FROM cpu WHERE host IN ('" + strings.Repeat("a', '", transformCacheMaxSQLLength/5) + "b')"
	if len(long) <= transformCacheMaxSQLLength {
		t.Fatalf("long statement is only %d bytes", len(long))
	}
	for _, sql := range []string{short, long} {
		if _, _, _, err := h.getTransformedSQLWithRefs(context.Background(), sql, ""); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.queryCache.Size(); got != 1 {
		t.Fatalf("cache holds %d entries, want 1 (the short statement only)", got)
	}
	_, cached, _, err := h.getTransformedSQLWithRefs(context.Background(), long, "")
	if err != nil || cached {
		t.Fatalf("long statement served from cache = %v, err %v", cached, err)
	}
}
