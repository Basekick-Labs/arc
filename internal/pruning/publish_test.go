package pruning

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

var publishNow = time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)

const (
	// 10:00 to 15:00: four past hours and the current one.
	publishSQL = "SELECT * FROM t WHERE time >= '2026-10-17T10:00:00Z' AND time < '2026-10-17T15:00:00Z'"
	// The same range in a text the caches have not seen.
	publishSQLOther = "SELECT v FROM t WHERE time >= '2026-10-17T10:00:00Z' AND time < '2026-10-17T15:00:00Z'"
	// Through 18:00: includes hours that have not started.
	publishSQLFuture = "SELECT * FROM t WHERE time >= '2026-10-17T13:00:00Z' AND time < '2026-10-17T18:00:00Z'"
)

func prunedPaths(p *PartitionPruner, glob, sql string) []string {
	r, _ := p.OptimizeTablePath(context.Background(), glob, sql)
	return asList(r)
}

// The bug, then the fix, on local storage. A late row's hour (or a
// future-stamped row's) has no directory when a query runs; the file is then
// written. Until the pruner is told, a repeated query and a new query text
// both keep leaving the hour out (the partition cache and the glob cache).
// Once told, both include it.
func TestFilePublishedOutsideTheCurrentHourIsSeen(t *testing.T) {
	for name, tc := range map[string]struct {
		sql, other string
		hour       time.Time
	}{
		"late row":           {publishSQL, publishSQLOther, publishNow.Add(-3 * time.Hour)},
		"future-stamped row": {publishSQLFuture, "SELECT v" + publishSQLFuture[len("SELECT *"):], publishNow.Add(2 * time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
			p := livePruner(publishNow)
			writeHour(t, root, publishNow) // the measurement exists, so the range is pruned to a list
			if paths := prunedPaths(p, glob, tc.sql); hasHour(paths, tc.hour) || !hasHour(paths, publishNow) {
				t.Fatalf("before the file: %v", paths)
			}

			writeHour(t, root, tc.hour)
			if hasHour(prunedPaths(p, glob, tc.sql), tc.hour) {
				t.Fatal("the repeated query saw the file without being told: the partition cache no longer holds the list, so this test pins nothing")
			}
			if hasHour(prunedPaths(p, glob, tc.other), tc.hour) {
				t.Fatal("a new query text saw the file without being told: the glob cache no longer holds the absence, so this test pins nothing")
			}

			p.FilePublished("db", "cpu", tc.hour.Add(7*time.Minute))
			if paths := prunedPaths(p, glob, tc.sql); !hasHour(paths, tc.hour) {
				t.Errorf("the repeated query still leaves the hour out: %v", paths)
			}
			if paths := prunedPaths(p, glob, tc.other); !hasHour(paths, tc.hour) {
				t.Errorf("a new query text still leaves the hour out: %v", paths)
			}
			// And what is cached now is used again.
			hits := p.GetPartitionCacheStats()["cache_hits"].(int64)
			prunedPaths(p, glob, tc.sql)
			if got := p.GetPartitionCacheStats()["cache_hits"].(int64); got != hits+1 {
				t.Errorf("the list made after the file was published is not cached (hits %d -> %d)", hits, got)
			}
		})
	}
}

// The same on an object store, where absence is remembered as a parent
// directory listing, for the single-tier filter and for the tier filter.
func TestFilePublishedOutsideTheCurrentHourIsSeenRemote(t *testing.T) {
	late := publishNow.Add(-3 * time.Hour)
	glob := "s3://bucket/db/cpu/**/*.parquet"

	t.Run("single tier", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/2026/10/17/": {"13", "14"},
		}}, dayFiles: map[string][]string{}}
		p := livePruner(publishNow)
		p.SetStorageBackend(remote)
		if paths := prunedPaths(p, glob, publishSQL); hasRemoteHour(paths, "11") || !hasRemoteHour(paths, "14") {
			t.Fatalf("before the file: %v", paths)
		}
		remote.existingDirs["db/cpu/2026/10/17/"] = []string{"11", "13", "14"}
		if hasRemoteHour(prunedPaths(p, glob, publishSQLOther), "11") {
			t.Fatal("a new query text saw the file without being told: this test pins nothing")
		}
		p.FilePublished("db", "cpu", late)
		for _, sql := range []string{publishSQL, publishSQLOther} {
			if paths := prunedPaths(p, glob, sql); !hasRemoteHour(paths, "11") {
				t.Errorf("still left out for %q: %v", sql, paths)
			}
		}
	})

	t.Run("tier", func(t *testing.T) {
		backend := &tierMock{mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/2026/10/17/": {"13", "14"},
		}}, files: map[string][]string{}}
		p := livePruner(publishNow)
		tr := tierRange(t, "2026-10-17T10:00:00Z", "2026-10-17T15:00:00Z")
		prune := func() []string {
			paths, _ := p.PruneTierPaths(context.Background(), glob, "db", "cpu", tr, backend, false)
			return paths
		}
		if paths := prune(); hasRemoteHour(paths, "11") || !hasRemoteHour(paths, "14") {
			t.Fatalf("before the file: %v", paths)
		}
		backend.existingDirs["db/cpu/2026/10/17/"] = []string{"11", "13", "14"}
		if hasRemoteHour(prune(), "11") {
			t.Fatal("the tier filter saw the file without being told: this test pins nothing")
		}
		p.FilePublished("db", "cpu", late)
		if paths := prune(); !hasRemoteHour(paths, "11") {
			t.Errorf("the tier filter still leaves the hour out: %v", paths)
		}
	})
}

// racingRemote publishes a file while a listing that does not include it is
// on its way back to the pruner: the window between "listed" and "stored".
type racingRemote struct {
	countingRemote
	afterList func()
}

func (r *racingRemote) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	dirs, err := r.countingRemote.ListDirectories(ctx, prefix)
	if f := r.afterList; f != nil && prefix == "db/cpu/2026/10/17/" { // the day whose hours the query asks for
		r.afterList = nil
		f()
	}
	return dirs, err
}

// A listing made just before the file landed, and stored just after the
// pruner was told, must not be served: it carries the generation read before
// it started. Observed from inside the window (the listing call itself), not
// after the fact.
func TestListingThatRacedAPublishedFileIsNotServed(t *testing.T) {
	glob := "s3://bucket/db/cpu/**/*.parquet"
	remote := &racingRemote{countingRemote: countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
		"db/cpu/2026/10/17/": {"13", "14"},
	}}, dayFiles: map[string][]string{}}}
	p := livePruner(publishNow)
	p.SetStorageBackend(remote)
	remote.afterList = func() {
		// The file lands and the flush reports it, after the listing was
		// read from the store and before the pruner has stored it.
		remote.existingDirs["db/cpu/2026/10/17/"] = []string{"11", "13", "14"}
		p.FilePublished("db", "cpu", publishNow.Add(-3*time.Hour))
	}
	if paths := prunedPaths(p, glob, publishSQL); hasRemoteHour(paths, "11") {
		t.Fatalf("the racing query itself listed before the file existed, yet returned it: %v", paths)
	}
	if remote.afterList != nil {
		t.Fatal("the listing was never made")
	}
	if paths := prunedPaths(p, glob, publishSQL); !hasRemoteHour(paths, "11") {
		t.Errorf("the repeated query was served the list made before the file existed: %v", paths)
	}
	if paths := prunedPaths(p, glob, publishSQLOther); !hasRemoteHour(paths, "11") {
		t.Errorf("a new query text was served the listing made before the file existed: %v", paths)
	}
}

// What moves the generation: any hour but the clock's current one. The
// previous hour does, even inside the live grace: an entry made after the
// grace could have left it out.
func TestFilePublishedSkipsOnlyTheCurrentHour(t *testing.T) {
	stripe := publishStripe("db", "cpu")
	for name, tc := range map[string]struct {
		now, file time.Time
		moves     bool
	}{
		"current hour":                    {publishNow, publishNow.Add(5 * time.Minute), false},
		"current hour, first second":      {publishNow, publishNow.Truncate(time.Hour), false},
		"current hour, another time zone": {publishNow, publishNow.In(time.FixedZone("x", -6*3600)), false},
		"previous hour past the grace":    {publishNow, publishNow.Add(-time.Hour), true},
		"previous hour inside the grace":  {publishNow.Truncate(time.Hour).Add(2 * time.Minute), publishNow.Add(-time.Hour), true},
		"three hours back":                {publishNow, publishNow.Add(-3 * time.Hour), true},
		"next hour":                       {publishNow, publishNow.Add(time.Hour), true},
		"yesterday, same hour of day":     {publishNow, publishNow.Add(-24 * time.Hour), true},
	} {
		t.Run(name, func(t *testing.T) {
			p := livePruner(tc.now)
			p.FilePublished("db", "cpu", tc.file)
			if moved := p.published[stripe].Load() != 0; moved != tc.moves {
				t.Errorf("generation moved = %v, want %v", moved, tc.moves)
			}
		})
	}
}

// A file published for one measurement leaves another's cached list in use.
func TestFilePublishedLeavesOtherMeasurementsCached(t *testing.T) {
	if publishStripe("db", "cpu") == publishStripe("db", "mem") || publishStripe("db", "cpu") == publishStripe("other", "cpu") {
		t.Fatal("pick names on different stripes: this test needs measurements that do not share a generation")
	}
	root := t.TempDir()
	writeHour(t, root, publishNow)
	p := livePruner(publishNow)
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	prunedPaths(p, glob, publishSQL)
	p.FilePublished("db", "mem", publishNow.Add(-3*time.Hour))
	p.FilePublished("other", "cpu", publishNow.Add(-3*time.Hour))
	prunedPaths(p, glob, publishSQL)
	if hits := p.GetPartitionCacheStats()["cache_hits"]; hits != int64(1) {
		t.Errorf("partition cache hits = %v, want 1: another measurement's file dropped this one's list", hits)
	}
	p.FilePublished("db", "cpu", publishNow.Add(-3*time.Hour))
	prunedPaths(p, glob, publishSQL)
	if hits := p.GetPartitionCacheStats()["cache_hits"]; hits != int64(1) {
		t.Errorf("partition cache hits = %v, want still 1: the measurement's own file did not drop its list", hits)
	}
}

// Every listing the pruner caches while pruning a measurement carries that
// measurement's generation. An entry stored without one would never go stale
// when a file is published, which is the bug again for whatever stored it.
func TestEveryCachedListingIsScoped(t *testing.T) {
	check := func(t *testing.T, p *PartitionPruner) {
		t.Helper()
		p.globCache.mu.RLock()
		defer p.globCache.mu.RUnlock()
		if len(p.globCache.entries) == 0 {
			t.Fatal("nothing was cached")
		}
		for key, e := range p.globCache.entries {
			if e.scope.stripe == nil {
				t.Errorf("cached without a measurement generation: %s", key)
			}
		}
		p.partitionCache.mu.RLock()
		defer p.partitionCache.mu.RUnlock()
		for key, e := range p.partitionCache.entries {
			if e.scope.stripe == nil {
				t.Errorf("pruned list cached without a measurement generation: %s", key)
			}
		}
	}
	openEnded := "SELECT * FROM t WHERE time >= '2026-10-17T10:00:00Z'"
	exact := "SELECT * FROM t WHERE time >= '2026-10-17T10:00:00Z' AND time < '2026-10-17T12:00:00Z'"

	t.Run("local", func(t *testing.T) {
		root := t.TempDir()
		writeHour(t, root, publishNow)
		p := livePruner(publishNow)
		glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
		for _, sql := range []string{publishSQL, openEnded} {
			p.OptimizeTablePath(context.Background(), glob, sql)
		}
		p.OptimizeTablePathVerdict(context.Background(), glob, exact) // the empty-range proof and its layout verdict
		check(t, p)
	})
	t.Run("remote", func(t *testing.T) {
		remote := &countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/":            {"2026"},
			"db/cpu/2026/":       {"10"},
			"db/cpu/2026/10/":    {"17"},
			"db/cpu/2026/10/17/": {"13", "14"},
		}}, dayFiles: map[string][]string{}}
		p := livePruner(publishNow)
		p.SetStorageBackend(remote)
		glob := "s3://bucket/db/cpu/**/*.parquet"
		for _, sql := range []string{publishSQL, openEnded} {
			p.OptimizeTablePath(context.Background(), glob, sql)
		}
		p.OptimizeTablePathVerdict(context.Background(), glob, exact)
		check(t, p)
	})
	t.Run("tier", func(t *testing.T) {
		backend := &tierMock{mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/":            {"2026"},
			"db/cpu/2026/":       {"10"},
			"db/cpu/2026/10/":    {"17"},
			"db/cpu/2026/10/17/": {"13", "14"},
		}}, files: map[string][]string{}}
		p := livePruner(publishNow)
		for _, tr := range []*TimeRange{
			tierRange(t, "2026-10-17T10:00:00Z", "2026-10-17T15:00:00Z"),
			p.ExtractTimeRange(openEnded),
		} {
			p.PruneTierPaths(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", tr, backend, false)
		}
		check(t, p)
	})
}

// splitStorageGlob reads the same database and measurement as the pattern
// optimize parses the glob with, and refuses what it refuses.
func TestSplitStorageGlobAgreesWithThePattern(t *testing.T) {
	for _, glob := range []string{
		"/data/arc/db/cpu/**/*.parquet",
		"./data/db/cpu/**/*.parquet",
		"db/cpu/**/*.parquet",
		"s3://bucket/db/cpu/**/*.parquet",
		"s3://bucket/a/prefix/db/cpu/**/*.parquet",
		"azure://c/db/my.measure-1/**/*.parquet",
		"/root/db/cpu/2026/10/**/*.parquet",
		"/root/db/cpu/*.parquet",
		"/root/db/cpu/**/*.parquet/",
		"cpu/**/*.parquet",
		"/cpu/**/*.parquet",
		"//cpu/**/*.parquet",
		"/db//**/*.parquet",
		"/**/*.parquet",
		"",
	} {
		m := storagePathPattern.FindStringSubmatch(glob)
		db, meas, ok := splitStorageGlob(glob)
		switch {
		case len(m) < 4 && ok:
			t.Errorf("%q: accepted (%q, %q), the pattern refuses it", glob, db, meas)
		case len(m) == 4 && (!ok || db != m[2] || meas != m[3]):
			t.Errorf("%q: (%q, %q, %v), the pattern reads (%q, %q)", glob, db, meas, ok, m[2], m[3])
		}
	}
}

// The transform cache's side: a stamp taken from a snapshot goes stale with
// its measurement, and only with it.
func TestPublishStamps(t *testing.T) {
	p := livePruner(publishNow)
	snap := p.PublishSnapshot()
	cpu, mem := snap.Stamp("db", "cpu"), snap.Stamp("db", "mem")
	if !p.StampsCurrent([]PublishStamp{cpu, mem}) || !p.StampsCurrent(nil) {
		t.Fatal("stamps are stale before anything was published")
	}
	p.FilePublished("db", "cpu", publishNow.Add(5*time.Minute)) // the current hour: nothing moves
	if !p.StampsCurrent([]PublishStamp{cpu, mem}) {
		t.Error("a file in the current hour made the stamps stale")
	}
	p.FilePublished("db", "cpu", publishNow.Add(-3*time.Hour))
	if p.StampsCurrent([]PublishStamp{mem, cpu}) || !p.StampsCurrent([]PublishStamp{mem}) {
		t.Error("a published file must make its measurement's stamp stale and leave the other's")
	}
	// A snapshot taken after the file is current again.
	if !p.StampsCurrent([]PublishStamp{p.PublishSnapshot().Stamp("db", "cpu")}) {
		t.Error("a stamp taken after the file was published is stale")
	}
}
