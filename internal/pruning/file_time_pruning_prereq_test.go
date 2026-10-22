package pruning

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func fnameNanos(m string, ts time.Time) string {
	return fmt.Sprintf("%s_%s_%s_%09d.parquet", m, ts.Format("20060102"), ts.Format("150405"), ts.Nanosecond())
}

// A margin under a second is raised to one, and a name's nanoseconds count:
// a file flushed at hh:mm:ss.9 can hold a row stamped at hh:mm:ss.5, so a
// query starting at hh:mm:ss.3 must keep it.
func TestFileTimePruningSubSecond(t *testing.T) {
	now := time.Date(2026, 8, 28, 20, 40, 0, 0, time.UTC)
	hour := now.Truncate(time.Hour)
	dir, glob := buildHourDir(t, t.TempDir(), hour)
	flush := hour.Add(20*time.Minute + 900*time.Millisecond) // 20:20:00.9
	keep := mkfile(t, dir, fnameNanos("m", flush))
	mkfile(t, dir, fnameNanos("m", hour.Add(time.Minute))) // 20:01, out of range

	p := NewPartitionPruner(zerolog.Nop())
	p.SetFileTimePruning(true, 0)
	p.nowFn = func() time.Time { return now }
	if got := time.Duration(p.fileTimeMargin.Load()); got != time.Second {
		t.Fatalf("margin 0 stored as %v, want the 1s floor", got)
	}
	start := hour.Add(20*time.Minute + 300*time.Millisecond) // 20:20:00.3
	out, changed := p.applyFileTimePruning(context.Background(), []string{glob}, &TimeRange{Start: start, End: now})
	if !changed {
		t.Fatal("the 20:01 file should have been dropped")
	}
	if len(out) != 1 || out[0] != keep {
		t.Fatalf("kept %v, want only the sub-second flush %s", out, filepath.Base(keep))
	}
}

// An eligible pass is volatile whether or not it changed the list, is never
// cached by the pruner, and is counted.
func TestFileTimePruningDeclinedPassIsNotCachedAndIsCounted(t *testing.T) {
	now := time.Now().UTC()
	if now.Sub(now.Truncate(time.Hour)) < 2*time.Minute {
		t.Skip("too close to the hour boundary for a live-hour pass")
	}
	hour := now.Truncate(time.Hour)
	base := t.TempDir()
	dir, _ := buildHourDir(t, base, hour)
	mkfile(t, dir, fnameNanos("m", now.Add(-10*time.Second)))
	p := NewPartitionPruner(zerolog.Nop())
	p.SetFileTimePruning(true, 5*time.Minute)
	sql := fmt.Sprintf("SELECT * FROM db.m WHERE time >= '%s'", now.Add(-30*time.Second).Format("2006-01-02 15:04:05"))
	glob := filepath.Join(base, "db", "m", "**", "*.parquet")

	m := metrics.Get()
	before := m.Snapshot()
	ctx, vol := WithVolatileResult(context.Background())
	p.OptimizeTablePath(ctx, glob, sql)
	if !vol.Volatile {
		t.Error("a declined live-hour pass must mark the result volatile for the SQL transform cache")
	}
	if _, _, ok := p.partitionCache.get(p.partitionCache.cacheKey(glob, sql, p.CacheEpoch(sql))); ok {
		t.Error("a declined live-hour pass was cached; the next query could have expanded")
	}
	after := m.Snapshot()
	for _, k := range []string{"file_time_pruning_passes_total", "file_time_pruning_nothing_dropped_total"} {
		if after[k].(int64)-before[k].(int64) != 1 {
			t.Errorf("%s moved by %d, want 1", k, after[k].(int64)-before[k].(int64))
		}
	}
}

// An end-only range assumes its start and never reaches the live hour.
func TestFileTimePruningNeverActsOnAnAssumedStart(t *testing.T) {
	now := time.Now().UTC()
	p := NewPartitionPruner(zerolog.Nop())
	p.SetFileTimePruning(true, time.Second)
	tr := &TimeRange{Start: now.Truncate(time.Hour), End: now, StartAssumed: true}
	if _, _, eligible := p.fileTimePass(context.Background(), []string{"/x/2026/01/01/00/*.parquet"}, tr); eligible {
		t.Fatal("an assumed start was treated as a real lower bound")
	}
}

// The pruning caches are keyed by the hours a range starts and ends in, so a
// relative end that crosses into a new hour gets a new key.
func TestCacheEpochFollowsARelativeEndAcrossAnHour(t *testing.T) {
	now := time.Now().UTC()
	h := now.Truncate(time.Hour)
	if now.Sub(h) < 10*time.Second || h.Add(time.Hour).Sub(now) < 10*time.Second {
		t.Skip("too close to an hour boundary")
	}
	// The end sits 2s before the current hour's start, then crosses it.
	x := int((now.Sub(h) + 2*time.Second) / time.Second)
	sql := fmt.Sprintf("SELECT * FROM db.cpu WHERE time >= now() - INTERVAL '3 hours' AND time < now() - INTERVAL '%d seconds'", x)
	p := NewPartitionPruner(zerolog.Nop())
	k1 := p.CacheEpoch(sql)
	time.Sleep(4 * time.Second)
	if k2 := p.CacheEpoch(sql); k1 == k2 {
		t.Fatalf("cache key %q unchanged after the range's end entered a new hour", k1)
	}
}
