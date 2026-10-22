package pruning

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// livePruner is a pruner whose clock reads now.
func livePruner(now time.Time) *PartitionPruner {
	p := NewPartitionPruner(zerolog.Nop())
	p.nowFn = func() time.Time { return now }
	return p
}

func writeHour(t *testing.T, root string, h time.Time) {
	t.Helper()
	dir := filepath.Join(root, "db", "cpu", h.Format("2006"), h.Format("01"), h.Format("02"), h.Format("15"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func asList(r interface{}) []string {
	switch v := r.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	}
	return nil
}

func hasHour(paths []string, h time.Time) bool {
	want := filepath.Join(h.Format("2006"), h.Format("01"), h.Format("02"), h.Format("15"), "*.parquet")
	for _, p := range paths {
		if len(p) >= len(want) && p[len(p)-len(want):] == want {
			return true
		}
	}
	return false
}

// A query run before the current hour's first flush sees that hour as soon
// as it has files: its absence is not remembered by the partition cache, the
// glob cache, or (through the volatile flag) the SQL transform cache.
func TestLiveHourAppearsAfterItsFirstFlush(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	for _, sql := range []string{
		"SELECT * FROM t WHERE time >= '2026-10-17T12:00:00Z' AND time < '2026-10-17T16:00:00Z'",
		"SELECT * FROM t WHERE time >= '2026-10-17T12:00:00Z'", // open-ended
	} {
		root := t.TempDir()
		glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
		p := livePruner(now)
		writeHour(t, root, now.Add(-time.Hour)) // 13:00
		ctx, vol := WithVolatileResult(context.Background())
		r, ok := p.OptimizeTablePath(ctx, glob, sql)
		if !ok || hasHour(asList(r), now) {
			t.Fatalf("%s: before the flush: %v %v", sql, ok, r)
		}
		writeHour(t, root, now) // 14:00 gets its first file
		r, ok = p.OptimizeTablePath(context.Background(), glob, sql)
		if !ok || !hasHour(asList(r), now) {
			t.Fatalf("%s: the new hour is not seen: %v", sql, r)
		}
		if !vol.Volatile {
			t.Fatalf("%s: a result missing a live hour must be volatile", sql)
		}
	}
}

// A range entirely before the current hour is cached as before.
func TestPastRangeStillCached(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, now.Add(-3*time.Hour))
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T10:00:00Z' AND time < '2026-10-17T13:00:00Z'"
	ctx, vol := WithVolatileResult(context.Background())
	p.OptimizeTablePath(ctx, glob, sql)
	if vol.Volatile {
		t.Fatal("a past range must stay cacheable")
	}
	p.OptimizeTablePath(context.Background(), glob, sql)
	if hits := p.GetPartitionCacheStats()["cache_hits"]; hits != int64(1) {
		t.Fatalf("cache hits %v", hits)
	}
}

// Remote: a cached parent listing missing a live hour is listed again; one
// missing only past hours is used as is.
func TestLiveHourRemoteListing(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
		"db/cpu/2026/10/17/": {"13"},
		"db/cpu/2026/10/":    {"17"},
	}}, dayFiles: map[string][]string{}}
	p := livePruner(now)
	p.SetStorageBackend(remote)
	glob := "s3://bucket/db/cpu/**/*.parquet"
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T12:00:00Z' AND time < '2026-10-17T16:00:00Z'"
	r, _ := p.OptimizeTablePath(context.Background(), glob, sql)
	if hasRemoteHour(asList(r), "14") {
		t.Fatalf("before: %v", r)
	}
	remote.existingDirs["db/cpu/2026/10/17/"] = []string{"13", "14"}
	r, _ = p.OptimizeTablePath(context.Background(), glob, sql)
	if !hasRemoteHour(asList(r), "14") {
		t.Fatalf("the new remote hour is not seen: %v", r)
	}

	// Past range: the second query uses the cached listing (no new calls).
	past := "SELECT * FROM t WHERE time >= '2026-10-16T10:00:00Z' AND time < '2026-10-16T12:00:00Z'"
	remote.existingDirs["db/cpu/2026/10/16/"] = []string{"10"}
	p.OptimizeTablePath(context.Background(), glob, past)
	calls := remote.listDirCalls
	p.partitionCache.invalidate() // force the existence check through the glob cache
	p.OptimizeTablePath(context.Background(), glob, past)
	if remote.listDirCalls != calls {
		t.Fatalf("a past range re-listed: %d -> %d", calls, remote.listDirCalls)
	}
}

func hasRemoteHour(paths []string, hour string) bool {
	for _, p := range paths {
		if filepath.Base(filepath.Dir(p)) == hour {
			return true
		}
	}
	return false
}

func TestLiveWindow(t *testing.T) {
	cases := []struct {
		now  time.Time
		path string
		want bool
	}{
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/14/*.parquet", true},
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/13/*.parquet", false},
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/15/*.parquet", false}, // not started
		{time.Date(2026, 10, 17, 14, 2, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/13/*.parquet", true},   // grace: late flush
		{time.Date(2026, 10, 17, 14, 6, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/13/*.parquet", false},
		{time.Date(2026, 10, 18, 0, 2, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/23/*.parquet", true},
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "/d/db/cpu/2026/10/17/*.parquet", false}, // day files: compaction only
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "s3://b/db/cpu/2026/10/17/14/*.parquet", true},
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), `C:\d\db\cpu\2026\10\17\14\*.parquet`, true},
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), `C:\d\db\cpu\2026\10\17\13\*.parquet`, false},
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "/d/db/cpu/**/*.parquet", true},            // unreadable: never remembered as absent
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC), "/d/db/cpu/2026/13/40/99/*.parquet", true}, // not a date
		{time.Date(2026, 10, 17, 14, 20, 0, 0, time.FixedZone("x", -3*3600)), "/d/db/cpu/2026/10/17/17/*.parquet", true},
	}
	for _, c := range cases {
		if got := liveWindowAt(c.now).isLive(c.path); got != c.want {
			t.Errorf("%s at %s: %v, want %v", c.path, c.now, got, c.want)
		}
	}
}

// A list cached before an hour started is never served after it, and the
// empty glob entry written for that hour while it had not started is not
// trusted once it has.
func TestHourStartedAfterCachingIsSeen(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 59, 50, 0, time.UTC)
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC))
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T14:00:00Z' AND time < '2026-10-17T16:00:00Z'"
	ctx, vol := WithVolatileResult(context.Background())
	if r, ok := p.OptimizeTablePath(ctx, glob, sql); !ok || len(asList(r)) != 1 {
		t.Fatalf("before the hour: %v %v", ok, r)
	}
	if vol.Volatile {
		t.Fatal("an hour not started yet is cached as absent, as before")
	}
	next := time.Date(2026, 10, 17, 15, 0, 10, 0, time.UTC)
	p.nowFn = func() time.Time { return next }
	writeHour(t, root, next)
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if !ok || !hasHour(asList(r), next) {
		t.Fatalf("the hour that started is not seen: %v", r)
	}
}

// Rows stamped just before the boundary flush just after it: the previous
// hour stays live for the grace period.
func TestPreviousHourLateFlushIsSeen(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 0, 2, 0, time.UTC)
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	prev := now.Add(-time.Hour)
	writeHour(t, root, now.Add(-2*time.Hour))
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T12:00:00Z' AND time < '2026-10-17T14:00:00Z'"
	p.OptimizeTablePath(context.Background(), glob, sql)
	writeHour(t, root, prev) // 13:59:58 rows flushed at 14:00:03
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if !ok || !hasHour(asList(r), prev) {
		t.Fatalf("the late-flushed previous hour is not seen: %v", r)
	}
}

// Steady state: once the current hour has files, an open-ended range is
// cached even though its future hours do not exist.
func TestOpenEndedRangeCachedOnceTheHourExists(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, now)
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T12:00:00Z' AND time < '2026-10-17T20:00:00Z'"
	ctx, vol := WithVolatileResult(context.Background())
	p.OptimizeTablePath(ctx, glob, sql)
	if vol.Volatile {
		t.Fatal("future hours must not make the result volatile")
	}
	p.OptimizeTablePath(context.Background(), glob, sql)
	if hits := p.GetPartitionCacheStats()["cache_hits"]; hits != int64(1) {
		t.Fatalf("cache hits %v", hits)
	}
}

// No partition has files and the current hour is among them: the fallback is
// not cached either.
func TestAllEmptyWithLiveHourNotCached(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "db", "cpu"), 0o755); err != nil {
		t.Fatal(err)
	}
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T14:00:00Z' AND time < '2026-10-17T15:00:00Z'"
	p.OptimizeTablePath(context.Background(), glob, sql)
	p.OptimizeTablePath(context.Background(), glob, sql)
	if hits := p.GetPartitionCacheStats()["cache_hits"]; hits != int64(0) {
		t.Fatalf("cache hits %v", hits)
	}
}

// Tiered reads: the current hour missing from the hot tier makes the result
// volatile, and a cached remote listing that lacks it is listed again.
func TestTierLiveHour(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	tr := tierRange(t, "2026-10-17T12:00:00Z", "2026-10-17T15:00:00Z")

	root := t.TempDir()
	p := livePruner(now)
	writeHour(t, root, now.Add(-time.Hour))
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	ctx, vol := WithVolatileResult(context.Background())
	if paths, _ := p.PruneTierPaths(ctx, glob, "db", "cpu", tr, nil, false); hasHour(paths, now) || !vol.Volatile {
		t.Fatalf("local before: %v volatile=%v", paths, vol.Volatile)
	}
	writeHour(t, root, now)
	if paths, _ := p.PruneTierPaths(context.Background(), glob, "db", "cpu", tr, nil, false); !hasHour(paths, now) {
		t.Fatalf("local: the new hour is not seen: %v", paths)
	}

	backend := &tierMock{mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
		"db/cpu/2026/10/17/": {"13"},
	}}, files: map[string][]string{}}
	rp := livePruner(now)
	rglob := "s3://bucket/db/cpu/**/*.parquet"
	ctx, vol = WithVolatileResult(context.Background())
	if paths, _ := rp.PruneTierPaths(ctx, rglob, "db", "cpu", tr, backend, false); hasRemoteHour(paths, "14") || !vol.Volatile {
		t.Fatalf("remote before: %v volatile=%v", paths, vol.Volatile)
	}
	backend.existingDirs["db/cpu/2026/10/17/"] = []string{"13", "14"}
	if paths, _ := rp.PruneTierPaths(context.Background(), rglob, "db", "cpu", tr, backend, false); !hasRemoteHour(paths, "14") {
		t.Fatalf("remote: the new hour is not seen: %v", paths)
	}
	// Past range: the cached listing is used as is.
	past := tierRange(t, "2026-10-17T10:00:00Z", "2026-10-17T12:00:00Z")
	rp.PruneTierPaths(context.Background(), rglob, "db", "cpu", past, backend, false)
	n := len(backend.requested)
	ctx, vol = WithVolatileResult(context.Background())
	rp.PruneTierPaths(ctx, rglob, "db", "cpu", past, backend, false)
	if len(backend.requested) != n || vol.Volatile {
		t.Fatalf("a past range re-listed (%d -> %d) or went volatile", n, len(backend.requested))
	}
}

// The previous hour's absence seen during the grace period is not written to
// the glob cache, where it would be trusted once the grace period ends.
func TestLateFlushAtTheEndOfTheGraceIsSeen(t *testing.T) {
	now := time.Date(2026, 10, 17, 14, 4, 55, 0, time.UTC)
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC))
	sql := "SELECT * FROM t WHERE time >= '2026-10-17T12:00:00Z' AND time < '2026-10-17T14:00:00Z'"
	p.OptimizeTablePath(context.Background(), glob, sql)
	later := time.Date(2026, 10, 17, 14, 5, 5, 0, time.UTC)
	p.nowFn = func() time.Time { return later }
	prev := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	writeHour(t, root, prev)
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if !ok || !hasHour(asList(r), prev) {
		t.Fatalf("the previous hour flushed at the end of the grace is not seen: %v", r)
	}
}
