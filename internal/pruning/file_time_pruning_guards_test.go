package pruning

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Guard-pinning tests: these exist so that deleting the volatility guards
// fails a test, not a production dashboard. They pin, at the
// OptimizeTablePath level:
//  1. expanded results are NOT cached — a second identical call re-globs and
//     sees a file flushed in between (live-freshness guarantee);
//  2. non-expanded results ARE cached (the pre-existing behavior).

// optimizeSetup builds base/db/m/<hour dirs> for the current test hour and
// returns (pruner, originalPath, sql, dir).
func optimizeSetup(t *testing.T, now time.Time) (*PartitionPruner, string, string, string) {
	t.Helper()
	base := t.TempDir()
	hour := now.Truncate(time.Hour)
	dir := filepath.Join(base, "db", "m",
		hour.Format("2006"), hour.Format("01"), hour.Format("02"), hour.Format("15"))
	mkdirAll(t, dir)
	p := newTestPruner(now)
	p.SetFileTimePruning(true, time.Minute)
	originalPath := filepath.Join(base, "db", "m", "**", "*.parquet")
	sqlQ := "SELECT * FROM t WHERE time > now() - INTERVAL '5 minutes' ORDER BY time"
	return p, originalPath, sqlQ, dir
}

func mkdirAll(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestOptimizeTablePathExpandedNeverCached(t *testing.T) {
	// The pruner's nowFn is fixed, but ExtractTimeRange uses the real
	// clock for NOW() — so run the test at the real current UTC hour.
	now := time.Now().UTC()
	// The "old" file sits at the hour start and must be older than
	// window(5m)+margin(1m); near a rollover the boundary hour is unstable.
	if m := now.Sub(now.Truncate(time.Hour)); m < 7*time.Minute || m > 58*time.Minute {
		t.Skip("current hour phase unsuitable for a stable old/fresh split")
	}
	p, originalPath, sqlQ, dir := optimizeSetup(t, now)

	old := mkfile(t, dir, fname("m", now.Truncate(time.Hour)))
	fresh := mkfile(t, dir, fname("m", now.Add(-10*time.Second)))

	res1, opt1 := p.OptimizeTablePath(context.Background(), originalPath, sqlQ)
	if !opt1 {
		t.Fatal("expected optimization")
	}
	list1 := asPathList(t, res1)
	if !sliceHas(list1, fresh) || sliceHas(list1, old) {
		t.Fatalf("expansion wrong: %v", list1)
	}

	// A file flushed after the first call MUST appear in the second call's
	// result — this is the freshness guarantee the cache-skip exists for.
	newer := mkfile(t, dir, fname("m", now.Add(-5*time.Second)))
	res2, _ := p.OptimizeTablePath(context.Background(), originalPath, sqlQ)
	if !sliceHas(asPathList(t, res2), newer) {
		t.Fatal("newly flushed file missing from second call — expanded result was cached")
	}
}

func TestOptimizeTablePathNonExpandedStillCached(t *testing.T) {
	now := time.Now().UTC()
	p, originalPath, sqlQ, dir := optimizeSetup(t, now)
	p.SetFileTimePruning(false, 0) // feature off → glob results, cached as before
	mkfile(t, dir, fname("m", now.Add(-10*time.Second)))

	if _, opt := p.OptimizeTablePath(context.Background(), originalPath, sqlQ); !opt {
		t.Fatal("expected optimization")
	}
	statsBefore := p.GetPartitionCacheStats()
	if _, opt := p.OptimizeTablePath(context.Background(), originalPath, sqlQ); !opt {
		t.Fatal("expected optimization on second call")
	}
	statsAfter := p.GetPartitionCacheStats()
	hb, _ := statsBefore["cache_hits"].(int64)
	ha, _ := statsAfter["cache_hits"].(int64)
	if ha <= hb {
		t.Fatalf("expected a partition-cache hit on the second call (hits %d -> %d)", hb, ha)
	}
}

// asPathList normalizes OptimizeTablePath's string-or-[]string result.
func asPathList(t *testing.T, res interface{}) []string {
	t.Helper()
	switch v := res.(type) {
	case []string:
		return v
	case string:
		return []string{v}
	default:
		t.Fatalf("unexpected result type %T", res)
		return nil
	}
}

func sliceHas(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
