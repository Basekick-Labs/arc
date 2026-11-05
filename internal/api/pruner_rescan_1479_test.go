package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/tiering"
)

// Regression tests for #1479: the pruner read the query's time range, its
// relative-time flag and its hash from the whole SQL once per table
// reference, so a query of many short references cost time quadratic in its
// length. A comma join (`FROM x, x, x, …`) took 5.6 s at the default 10 KB
// limit. The rewrite now computes them once per query (pruning.WithQueryFacts).

// commaJoin returns a comma join of n references to table.
func commaJoin(table string, n int) string {
	return "SELECT * FROM " + table + strings.Repeat(", "+table, n-1)
}

// TestPartitionPruningScansTheQueryOnce: every reference's partition pruning
// (optimize) shares one scan, with and without a header database.
func TestPartitionPruningScansTheQueryOnce(t *testing.T) {
	h := newParityTestHandler()
	for _, headerDB := range []string{"", "mydb"} {
		ctx := pruning.WithQueryFacts(context.Background())
		sql := commaJoin("cpu", 50)
		if headerDB == "" {
			h.convertSQLToStoragePaths(ctx, sql)
		} else {
			h.convertSQLToStoragePathsWithHeaderDB(ctx, sql, headerDB)
		}
		if got := pruning.QueryFactsComputed(ctx); got != 1 {
			t.Errorf("header %q: the query's facts were computed %d times for 50 references, want 1", headerDB, got)
		}
	}
}

// TestColdTierGuardScansTheQueryOnce: with the cold tier missing, every
// reference passes through refuseReadWithoutColdTier, which reads the range.
func TestColdTierGuardScansTheQueryOnce(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "fresh", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	guard := &fakeColdGuard{missing: true, cold: map[string]bool{}}
	e.h.coldGuard = guard

	before := pruning.TimeRangeScans()
	e.h.convertSQLToStoragePaths(context.Background(), commaJoin("sch.fresh", 20))
	if guard.calls == 0 {
		t.Fatal("the cold-tier guard was never consulted; the test does not reach refuseReadWithoutColdTier")
	}
	// Counting scans, not memo computations: partition pruning computes the
	// facts once anyway, so only a scan count shows the guard reading the
	// range around the memo.
	if got := pruning.TimeRangeScans() - before; got != 1 {
		t.Errorf("the query was scanned for its range %d times for 20 references, want 1", got)
	}
}

// TestMultiTierReadScansTheQueryOnce: a measurement with a cold tier goes
// through buildMultiTierReadParquet, which reads the range, once per
// reference. Reaching it through the rewrite needs a tiering manager and
// router, so the per-reference calls are made directly, on one per-query
// context as the rewrite makes them (multiTierRead does the same for #1364);
// the rewrite installing that context is pinned by the tests around this one.
func TestMultiTierReadScansTheQueryOnce(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "both", time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), map[string]interface{}{"host": []string{"a"}, "v": []int64{1}})
	view := fakeTierView{tiers: map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true}, cold: emptyCold(t, e)}
	e.h.tierView = view
	paths := map[tiering.Tier]string{
		tiering.TierHot:  e.h.getStoragePath(context.Background(), "sch", "both"),
		tiering.TierCold: e.h.storagePathForBackend(context.Background(), view.cold, "sch", "both"),
	}

	sql := commaJoin("sch.both", 20)
	ctx := pruning.WithQueryFacts(context.Background())
	before := pruning.TimeRangeScans()
	for i := 0; i < 20; i++ {
		e.h.buildMultiTierReadParquet(ctx, "sch", "both", sql, paths, "FROM")
	}
	if got := pruning.TimeRangeScans() - before; got != 1 {
		t.Errorf("the query was scanned for its range %d times for 20 references, want 1", got)
	}
}

// TestManyReferenceRewriteIsLinear: the worst shape is a comma join, 3 bytes a
// reference. 64 KB of it took minutes before #1479 and takes a fraction of a
// second after. The memo is NOT pre-installed here, so this also pins the
// rewrite's own entry points installing it.
func TestManyReferenceRewriteIsLinear(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	h := newParityTestHandler()
	sql := commaJoin("x", (64<<10)/3)
	for _, headerDB := range []string{"", "mydb"} {
		start := time.Now()
		if headerDB == "" {
			h.convertSQLToStoragePaths(context.Background(), sql)
		} else {
			h.convertSQLToStoragePathsWithHeaderDB(context.Background(), sql, headerDB)
		}
		// Loose enough for -race; the quadratic rewrite took minutes.
		if d := time.Since(start); d > 20*time.Second {
			t.Errorf("header %q: a 64 KB comma join took %v to rewrite", headerDB, d)
		}
	}
}

// TestQueryFactsFollowTheSQL: a context reused for a second query must not
// hand it the first query's range.
func TestQueryFactsFollowTheSQL(t *testing.T) {
	p := pruning.NewPartitionPruner(zerolog.Nop())
	ctx := pruning.WithQueryFacts(context.Background())
	a := p.ExtractTimeRangeCtx(ctx, "SELECT * FROM cpu WHERE time >= '2026-01-01' AND time < '2026-01-02'")
	b := p.ExtractTimeRangeCtx(ctx, "SELECT * FROM cpu WHERE time >= '2026-02-01' AND time < '2026-02-02'")
	if a == nil || b == nil || a.Start.Equal(b.Start) {
		t.Fatalf("ranges %v and %v: the second query got the first one's", a, b)
	}
	if got := pruning.QueryFactsComputed(ctx); got != 2 {
		t.Errorf("computed %d times for two different queries, want 2", got)
	}
}
