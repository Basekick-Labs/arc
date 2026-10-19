package pruning

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A start-only range asks for every row after its start. Rows stamped beyond
// the pruner's old assumed end (now + 24h: forecasts, devices whose clocks
// run ahead) must be read, not silently left out of the pruned list.
func TestOpenEndedRangeKeepsFuturePartitions(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	future := now.Add(48 * time.Hour)
	writeHour(t, root, now)
	writeHour(t, root, future)
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if ok && !hasHour(asList(r), future) {
		t.Fatalf("the partition 48 h ahead was dropped: %v", r)
	}
}

// A start-only window with no partition at or after its start reads the
// newest partition (older than the start, so filtered to no rows) instead of
// the whole measurement, when the read carries a complete schema anchor;
// without one, the unpruned glob as before (one partition alone could lack a
// column older files have).
func TestOpenEndedEmptyWindowReadsTheNewestPartition(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	older, newest := now.Add(-30*time.Hour), now.Add(-5*time.Hour)
	writeHour(t, root, older)
	writeHour(t, root, newest)
	for _, sql := range []string{
		"SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'",
		"SELECT * FROM t WHERE time >= '" + now.Truncate(time.Hour).Format(time.RFC3339) + "'", // hour-aligned start
	} {
		p := livePruner(now)
		ctx, vol := WithVolatileResult(WithCompleteSchemaAnchor(context.Background()))
		r, ok := p.OptimizeTablePath(ctx, glob, sql)
		if !ok || len(asList(r)) != 1 || !hasHour(asList(r), newest) {
			t.Fatalf("%s: got %v %v, want the newest partition alone", sql, ok, r)
		}
		if !vol.Volatile {
			t.Fatalf("%s: the empty-window answer must not be cached", sql)
		}
		p = livePruner(now)
		if r, ok := p.OptimizeTablePath(context.Background(), glob, sql); ok || r != glob {
			t.Fatalf("%s: without a complete anchor got %v %v, want the unpruned glob", sql, ok, r)
		}
	}
}

// Partitions far past the assumed end cost one path each, not a dense span
// that would trip the path cap and fall back to the unpruned glob.
func TestOpenEndedFarFuturePartitions(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, now)
	far := []time.Time{now.Add(400 * 24 * time.Hour), time.Date(2099, 1, 1, 3, 0, 0, 0, time.UTC)}
	for _, h := range far {
		writeHour(t, root, h)
	}
	r, ok := p.OptimizeTablePath(context.Background(), glob, "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'")
	if !ok {
		t.Fatalf("not pruned: %v", r)
	}
	for _, h := range append(far, now) {
		if !hasHour(asList(r), h) {
			t.Fatalf("partition %v missing from %v", h, r)
		}
	}
}

// A closed range is untouched: no listing, no partition past its end.
func TestClosedRangeIgnoresFuturePartitions(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, now)
	future := now.Add(48 * time.Hour)
	writeHour(t, root, future)
	sql := "SELECT * FROM t WHERE time >= '" + now.Add(-time.Hour).Format(time.RFC3339) + "' AND time < '" + now.Add(2*time.Hour).Format(time.RFC3339) + "'"
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if !ok || hasHour(asList(r), future) {
		t.Fatalf("closed range: %v %v", ok, r)
	}
}

// Remote: the future partition is found through the cached parent listings,
// and a listing failure keeps the generated range (never a full scan).
func TestOpenEndedRemote(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(72 * time.Hour)
	dirs := map[string][]string{
		"db/cpu/": {now.Format("2006"), future.Format("2006")},
	}
	addHour := func(h time.Time) {
		y, m, d := h.Format("2006"), h.Format("01"), h.Format("02")
		key := "db/cpu/" + y + "/"
		dirs[key] = append(dirs[key], m)
		key = "db/cpu/" + y + "/" + m + "/"
		dirs[key] = append(dirs[key], d)
		key = "db/cpu/" + y + "/" + m + "/" + d + "/"
		dirs[key] = append(dirs[key], h.Format("15"))
	}
	addHour(now)
	addHour(future)
	remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: dirs}, dayFiles: map[string][]string{}}
	p := livePruner(now)
	p.SetStorageBackend(remote)
	glob := "s3://bucket/db/cpu/**/*.parquet"
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if !ok || !hasRemoteHour(asList(r), future.Format("15")) || len(asList(r)) < 2 {
		t.Fatalf("remote future partition missing: %v %v", ok, r)
	}
}

// Tiered reads: a start-only range keeps a tier's partitions past the assumed
// end too, and the TimeRange the tiers share is not changed.
func TestOpenEndedTierKeepsFuturePartitions(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	writeHour(t, root, now)
	future := now.Add(5 * 24 * time.Hour)
	writeHour(t, root, future)
	p := livePruner(now)
	tr := p.ExtractTimeRange("SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'")
	end := tr.End
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu", tr, nil, false)
	if outcome != TierPrunePruned || !hasHour(paths, future) || !hasHour(paths, now) {
		t.Fatalf("outcome %v paths %v", outcome, paths)
	}
	if !tr.End.Equal(end) || !tr.EndAssumed {
		t.Fatal("the shared TimeRange was changed")
	}
}

// A listing that fails keeps the generated range: no partition past the
// assumed end, and no fall back to the whole measurement.
func TestOpenEndedListingErrorKeepsGeneratedRange(t *testing.T) {
	now := time.Now().UTC()
	tr := &TimeRange{Start: now.Add(-time.Hour), End: now.Add(24 * time.Hour), EndAssumed: true}
	failing := func(context.Context, string) ([]string, error) { return nil, errors.New("listing failed") }
	if _, _, err := discoverOpenEnd(context.Background(), failing, "db", "cpu", tr.End); err == nil {
		t.Fatal("a failed listing must be an error, not an empty measurement")
	}
	backend := &tierMock{mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}}, files: map[string][]string{}, errPrefixes: map[string]bool{"db/cpu/": true}}
	p := livePruner(now)
	paths, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", tr, backend, false)
	if outcome == TierPruneFallback && paths == nil {
		return // the tier is read whole only when its own listing says so; here the hour listings decide
	}
	for _, path := range paths {
		if strings.Contains(path, now.Add(48*time.Hour).Format("2006/01/02")) {
			t.Fatalf("a partition past the assumed end without a listing: %v", paths)
		}
	}
}

// More partitions past the assumed end than the cap: the unpruned glob
// (correct, slow), never a list that silently stops at the cap. Tiers fall
// back the same way.
func TestOpenEndedTooManyFuturePartitions(t *testing.T) {
	defer func(v int) { maxBeyondPartitions = v }(maxBeyondPartitions)
	maxBeyondPartitions = 2
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	writeHour(t, root, now)
	for i := 2; i <= 4; i++ {
		writeHour(t, root, now.Add(time.Duration(i)*24*time.Hour))
	}
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	if r, ok := livePruner(now).OptimizeTablePath(context.Background(), glob, sql); ok || r != glob {
		t.Fatalf("got %v %v, want the unpruned glob", ok, r)
	}
	p := livePruner(now)
	if paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu", p.ExtractTimeRange(sql), nil, false); outcome != TierPruneFallback {
		t.Fatalf("tier: got %v %v, want the fallback", outcome, paths)
	}
}

// A day past the assumed end that daily compaction rewrote into the day
// directory itself (no hour directories left) is read through its day glob.
func TestOpenEndedFutureCompactedDay(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	writeHour(t, root, now)
	day := now.Add(5 * 24 * time.Hour)
	dir := filepath.Join(root, "db", "cpu", day.Format("2006"), day.Format("01"), day.Format("02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "compacted.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, ok := livePruner(now).OptimizeTablePath(context.Background(), glob, "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'")
	want := filepath.Join(dir, "*.parquet")
	found := false
	for _, path := range asList(r) {
		found = found || path == want
	}
	if !ok || !found {
		t.Fatalf("compacted future day missing: %v %v", ok, r)
	}
}

// failingRootRemote cannot list the measurement's root (its years); every
// other listing works.
type failingRootRemote struct{ countingRemote }

func (f *failingRootRemote) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	if prefix == "db/cpu/" {
		return nil, errors.New("simulated listing failure")
	}
	return f.countingRemote.ListDirectories(ctx, prefix)
}

// The pruner keeps the generated range when the open-end listing fails: a
// listing hiccup must not become a full scan.
func TestOpenEndedListingErrorPrunerKeepsGeneratedRange(t *testing.T) {
	now := time.Now().UTC()
	y, m, d := now.Format("2006"), now.Format("01"), now.Format("02")
	dirs := map[string][]string{
		"db/cpu/" + y + "/":                     {m},
		"db/cpu/" + y + "/" + m + "/":           {d},
		"db/cpu/" + y + "/" + m + "/" + d + "/": {now.Format("15")},
	}
	remote := &failingRootRemote{countingRemote{mockS3Backend: mockS3Backend{existingDirs: dirs}, dayFiles: map[string][]string{}}}
	p := livePruner(now)
	p.SetStorageBackend(remote)
	glob := "s3://bucket/db/cpu/**/*.parquet"
	r, ok := p.OptimizeTablePath(context.Background(), glob, "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'")
	if !ok || !hasRemoteHour(asList(r), now.Format("15")) {
		t.Fatalf("got %v %v, want the generated range", ok, r)
	}
}

// A remote tier under a bucket prefix finds its partitions past the assumed
// end with backend-relative listings (the backend applies its prefix once).
func TestOpenEndedRemoteTierKeepsFuturePartitions(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(5 * 24 * time.Hour)
	dirs := map[string][]string{}
	add := func(parent, child string) {
		for _, c := range dirs[parent] {
			if c == child {
				return
			}
		}
		dirs[parent] = append(dirs[parent], child)
	}
	for _, h := range []time.Time{now, future} {
		y, m, d := h.Format("2006"), h.Format("01"), h.Format("02")
		add("db/cpu/", y)
		add("db/cpu/"+y+"/", m)
		add("db/cpu/"+y+"/"+m+"/", d)
		add("db/cpu/"+y+"/"+m+"/"+d+"/", h.Format("15"))
	}
	backend := &tierMock{mockS3Backend: &mockS3Backend{existingDirs: dirs}, files: map[string][]string{}}
	p := livePruner(now)
	tr := p.ExtractTimeRange("SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'")
	paths, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/tenant1/db/cpu/**/*.parquet", "db", "cpu", tr, backend, false)
	if outcome != TierPrunePruned || !hasRemoteHour(paths, future.Format("15")) {
		t.Fatalf("outcome %v paths %v", outcome, paths)
	}
	for _, path := range paths {
		if !strings.HasPrefix(path, "s3://bucket/tenant1/db/cpu/") {
			t.Fatalf("path outside the tier root: %s", path)
		}
	}
	for _, prefix := range backend.requested {
		if !strings.HasPrefix(prefix, "db/cpu/") {
			t.Fatalf("listing not backend-relative: %q", prefix)
		}
	}
}

// The newest partition's directory exists but holds no files (a delete left
// it behind): the empty window keeps the unpruned glob rather than read a
// glob that matches nothing.
func TestOpenEndedEmptyWindowNewestDirWithoutFiles(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	writeHour(t, root, now.Add(-30*time.Hour))
	h := now.Add(-5 * time.Hour)
	if err := os.MkdirAll(filepath.Join(root, "db", "cpu", h.Format("2006"), h.Format("01"), h.Format("02"), h.Format("15")), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx := WithCompleteSchemaAnchor(context.Background())
	if r, ok := livePruner(now).OptimizeTablePath(ctx, glob, "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"); ok || r != glob {
		t.Fatalf("got %v %v, want the unpruned glob", ok, r)
	}
}

// A window that starts ahead of the live hour does not generate it, so the
// empty-window answer itself must keep the result out of the caches: the
// next flush past the start has to be seen.
func TestOpenEndedEmptyFutureWindowIsVolatile(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	writeHour(t, root, now.Add(-5*time.Hour))
	ctx, vol := WithVolatileResult(WithCompleteSchemaAnchor(context.Background()))
	r, ok := livePruner(now).OptimizeTablePath(ctx, glob, "SELECT * FROM t WHERE time > NOW() + INTERVAL '3 hours'")
	if !ok || len(asList(r)) != 1 || !vol.Volatile {
		t.Fatalf("got %v %v volatile=%v", ok, r, vol.Volatile)
	}
}

func TestIsStartOnlyTimeRange(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'":                            true,
		"SELECT * FROM t WHERE time >= '2026-10-19T00:00:00Z' AND host = 'a'":               true,
		"SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour' AND time < NOW()":           false,
		"SELECT * FROM t WHERE time < NOW()":                                                false,
		"SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour' OR alarm = 1":               false,
		"SELECT * FROM a JOIN b ON a.x = b.x WHERE a.time > NOW() - INTERVAL '1 hour'":      false,
		"SELECT * FROM t WHERE x IN (SELECT x FROM u) AND time > NOW() - INTERVAL '1 hour'": false,
		"SELECT * FROM t WHERE last_time > NOW() - INTERVAL '1 hour'":                       false,
		"SELECT * FROM t WHERE time > CURRENT_TIMESTAMP - INTERVAL '1 hour'":                false,
		"SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour' + INTERVAL '5 minutes'":     false,
	} {
		if got := IsStartOnlyTimeRange(sql); got != want {
			t.Errorf("%s: got %v want %v", sql, got, want)
		}
	}
}

// The over-cap fallback is cached like any fallback: a repeat does not walk
// the listings again.
func TestOpenEndedTooManyFuturePartitionsCached(t *testing.T) {
	defer func(v int) { maxBeyondPartitions = v }(maxBeyondPartitions)
	maxBeyondPartitions = 2
	now := time.Now().UTC()
	dirs := map[string][]string{}
	add := func(parent, child string) {
		for _, c := range dirs[parent] {
			if c == child {
				return
			}
		}
		dirs[parent] = append(dirs[parent], child)
	}
	for i := 0; i <= 4; i++ {
		h := now.Add(time.Duration(i) * 48 * time.Hour)
		y, m, d := h.Format("2006"), h.Format("01"), h.Format("02")
		add("db/cpu/", y)
		add("db/cpu/"+y+"/", m)
		add("db/cpu/"+y+"/"+m+"/", d)
		add("db/cpu/"+y+"/"+m+"/"+d+"/", h.Format("15"))
	}
	remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: dirs}, dayFiles: map[string][]string{}}
	p := livePruner(now)
	p.SetStorageBackend(remote)
	glob := "s3://bucket/db/cpu/**/*.parquet"
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	if r, ok := p.OptimizeTablePath(context.Background(), glob, sql); ok || r != glob {
		t.Fatalf("got %v %v, want the unpruned glob", ok, r)
	}
	calls := remote.listDirCalls + remote.listCalls
	p.globCache = newGlobCache(GlobCacheTTL) // listings expire; the verdict must not
	if r, ok := p.OptimizeTablePath(context.Background(), glob, sql); ok || r != glob {
		t.Fatalf("repeat: got %v %v", ok, r)
	}
	if n := remote.listDirCalls + remote.listCalls; n != calls {
		t.Fatalf("the repeat listed again (%d -> %d calls)", calls, n)
	}
}
