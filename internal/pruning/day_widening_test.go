package pruning

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const (
	lvlDayWide = "s3://bucket/db/cpu/2025/01/15/**/*.parquet"
	lvlHour11G = "s3://bucket/db/cpu/2025/01/15/11/*.parquet"
	lvlDay16   = "db/cpu/2025/01/16/"
	lvlDay16G  = "s3://bucket/db/cpu/2025/01/16/*.parquet"
	lvlHour16G = "s3://bucket/db/cpu/2025/01/16/09/*.parquet"
)

// A day whose level listing fails is read through ONE recursive glob that
// replaces every glob of that day: nothing is assumed about its hours and
// its daily files are not left out.
func TestUnknownDayIsReadThroughOneRecursiveGlob(t *testing.T) {
	p, b := newLevelPruner(t, level{dirs: []string{"10"}, files: []string{"cpu_daily.parquet"}})
	b.levels[lvlMonth] = level{dirs: []string{"15", "16"}}
	b.levels[lvlDay16] = level{dirs: []string{"09"}, files: []string{"d.parquet"}}
	b.fail[lvlDay] = errListing

	in := []string{lvlHour16G, lvlHourG, lvlHour11G, lvlDayG, lvlDay16G}
	got, verified, _ := p.filterExistingRemotePathsOpts(context.Background(), in, lvlBase, 0)
	// The other day is untouched; the unknown day's three globs become one,
	// in the place of the first.
	if want := []string{lvlHour16G, lvlDayWide, lvlDay16G}; !reflect.DeepEqual(got, want) {
		t.Errorf("paths = %v, want %v", got, want)
	}
	if verified {
		t.Error("verified = true although a day could not be listed")
	}
	// One attempt at the day: the storage client retries by itself, and a
	// second listing would only be more seconds against a failing store.
	if n := b.callsFor(lvlDay); n != 1 {
		t.Errorf("the day directory was listed %d times, want 1", n)
	}

	// Storage answers again: the day is planned normally.
	delete(b.fail, lvlDay)
	got, verified, _ = p.filterExistingRemotePathsOpts(context.Background(), []string{lvlHourG, lvlDayG}, lvlBase, 0)
	if want := []string{lvlHourG, lvlDayG}; !reflect.DeepEqual(got, want) || !verified {
		t.Errorf("after recovery: paths = %v, verified = %v; want %v, true", got, verified, want)
	}
}

// The failed listing must not be remembered as "this day has no daily
// files": asked about the day glob alone afterwards, it is kept.
func TestFailedDayListingIsNotRememberedAsEmpty(t *testing.T) {
	p, b := newLevelPruner(t, level{dirs: []string{"10"}, files: []string{"cpu_daily.parquet"}})
	b.fail[lvlDay] = errListing
	p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG, lvlHourG}, lvlBase, 0)

	delete(b.fail, lvlDay)
	got, verified, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG}, lvlBase, 0)
	if want := []string{lvlDayG}; !reflect.DeepEqual(got, want) || !verified {
		t.Errorf("day glob alone after recovery: paths = %v, verified = %v; want %v, true", got, verified, want)
	}
}

// Off s3:// the day-level check still makes its own attempt after the hour
// check's listing failed, as before: when it answers, the day glob is kept
// on a verified answer (the hours stay assumed).
func TestDayCheckStillRetriesOffS3(t *testing.T) {
	p, b := newLevelPruner(t, level{dirs: []string{"10"}, files: []string{"cpu_daily.parquet"}})
	b.failFirst[lvlDay] = 1
	const base = "gs://bucket"
	day, hour := base+"/db/cpu/2025/01/15/*.parquet", base+"/db/cpu/2025/01/15/10/*.parquet"
	got, _, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{day, hour}, base, 0)
	if want := []string{day, hour}; !reflect.DeepEqual(got, want) {
		t.Errorf("paths = %v, want %v", got, want)
	}
	if n := b.callsFor(lvlDay); n != 2 {
		t.Errorf("the day directory was listed %d times, want 2", n)
	}
}

// The day glob alone (the newest-partition shortcut asks this way): unknown
// becomes the day-wide glob too.
func TestUnknownDayAloneIsWidened(t *testing.T) {
	p, b := newLevelPruner(t, level{files: []string{"cpu_daily.parquet"}})
	b.fail[lvlDay] = errListing
	got, verified, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG}, lvlBase, 0)
	if want := []string{lvlDayWide}; !reflect.DeepEqual(got, want) || verified {
		t.Errorf("paths = %v, verified = %v; want %v, false", got, verified, want)
	}
}

// Hours asked about without their day glob keep today's handling: assumed
// to exist. Nothing asked for the day's daily files, so nothing is widened.
func TestUnknownDayWithoutItsDayGlobIsNotWidened(t *testing.T) {
	p, b := newLevelPruner(t, level{dirs: []string{"10"}})
	b.fail[lvlDay] = errListing
	got, verified, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{lvlHourG, lvlHour11G}, lvlBase, 0)
	if want := []string{lvlHourG, lvlHour11G}; !reflect.DeepEqual(got, want) || verified {
		t.Errorf("paths = %v, verified = %v; want %v, false", got, verified, want)
	}
}

// Off s3:// the recursive glob is not used: the hours are assumed and the day
// glob is dropped, as before.
func TestUnknownDayIsNotWidenedOffS3(t *testing.T) {
	for _, scheme := range []string{"gs", "azure"} {
		t.Run(scheme, func(t *testing.T) {
			p, b := newLevelPruner(t, level{dirs: []string{"10"}, files: []string{"cpu_daily.parquet"}})
			b.fail[lvlDay] = errListing
			base := scheme + "://bucket"
			day := base + "/db/cpu/2025/01/15/*.parquet"
			hour := base + "/db/cpu/2025/01/15/10/*.parquet"
			got, verified, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{day, hour}, base, 0)
			if want := []string{hour}; !reflect.DeepEqual(got, want) || verified {
				t.Errorf("paths = %v, verified = %v; want %v, false", got, verified, want)
			}
		})
	}
}

// The month listing shows the day does not exist and the day's own listing
// fails: its hours are known absent, not assumed present.
func TestHoursOfADayTheMonthDoesNotShowAreDropped(t *testing.T) {
	p, b := newLevelPruner(t, level{})
	b.levels[lvlMonth] = level{dirs: []string{"14"}}
	b.fail[lvlDay] = errListing
	got, _, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG, lvlHourG, lvlHour11G}, lvlBase, 0)
	if len(got) != 0 {
		t.Errorf("paths = %v, want none: the day does not exist", got)
	}
}

// The newest-partition shortcut reads its glob alone. When that glob is a
// day whose listing fails, the answer is a day-wide glob, not the glob asked
// about: the unpruned fallback answers, as it did when the glob was dropped.
func TestNewestPartitionShortcutIgnoresAWidenedDay(t *testing.T) {
	now := time.Date(2025, 1, 17, 12, 30, 0, 0, time.UTC)
	b := &levelBackend{
		mockS3Backend: &mockS3Backend{},
		t:             t,
		levels: map[string]level{
			"db/cpu/":      {dirs: []string{"2025"}},
			"db/cpu/2025/": {dirs: []string{"01"}},
			lvlMonth:       {dirs: []string{"15"}},
			lvlDay:         {files: []string{"cpu_daily.parquet"}},
		},
		fail:      map[string]error{},
		failFirst: map[string]int{},
	}
	p := NewPartitionPruner(zerolog.Nop())
	p.nowFn = func() time.Time { return now }
	p.SetStorageBackend(b)

	glob := "s3://bucket/db/cpu/**/*.parquet"
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"

	// Precondition: with the day listable, the shortcut reads it alone.
	ctx := WithCompleteSchemaAnchor(context.Background())
	if r, ok := p.OptimizeTablePath(ctx, glob, sql); !ok || !resultHasPath(r, lvlDayG) {
		t.Fatalf("precondition: got %v %v, want the newest day glob %s", r, ok, lvlDayG)
	}

	// The directory listings stay remembered, so the resolver still finds
	// the newest day; only the day's "has daily files" answer is gone (it
	// can expire separately), and the listing that would restore it fails.
	p.InvalidatePartitionCache()
	p.globCache.mu.Lock()
	delete(p.globCache.entries, "remote:dayfiles:"+lvlDay)
	p.globCache.mu.Unlock()
	b.fail[lvlDay] = errListing
	before := b.callsFor(lvlDay)
	if r, ok := p.OptimizeTablePath(WithCompleteSchemaAnchor(context.Background()), glob, sql); ok || r != glob {
		t.Fatalf("got %v %v, want the unpruned glob", r, ok)
	}
	if b.callsFor(lvlDay) == before {
		t.Fatal("the day was not listed again: this did not exercise the shortcut with a failed day listing")
	}
}

// The open-ended resolver lists a day's directory too; its listing answers
// the day-level check, which then makes no request.
func TestOpenEndResolverListingAnswersTheDayCheck(t *testing.T) {
	now := time.Date(2025, 1, 17, 12, 30, 0, 0, time.UTC)
	b := &levelBackend{
		mockS3Backend: &mockS3Backend{},
		t:             t,
		levels: map[string]level{
			"db/cpu/":      {dirs: []string{"2025"}},
			"db/cpu/2025/": {dirs: []string{"01"}},
			lvlMonth:       {dirs: []string{"15"}},
			lvlDay:         {files: []string{"cpu_daily.parquet"}},
		},
		fail:      map[string]error{},
		failFirst: map[string]int{},
	}
	p := NewPartitionPruner(zerolog.Nop())
	p.nowFn = func() time.Time { return now }
	p.SetStorageBackend(b)

	r, ok := p.OptimizeTablePath(WithCompleteSchemaAnchor(context.Background()),
		"s3://bucket/db/cpu/**/*.parquet", "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'")
	if !ok || !resultHasPath(r, lvlDayG) {
		t.Fatalf("got %v %v, want the newest day glob %s", r, ok, lvlDayG)
	}
	if n := b.callsFor(lvlDay); n != 1 {
		t.Errorf("the day directory was listed %d times, want 1", n)
	}
}

func TestIsDayWideGlob(t *testing.T) {
	const root = "s3://bucket/tenant-a/"
	for _, c := range []struct {
		pattern, root string
		want          bool
	}{
		{root + "db/cpu/2025/01/15/**/*.parquet", root, true},
		{root + "spoke/child/cpu/2025/01/15/**/*.parquet", root, true},
		{root + "db/cpu/**/*.parquet", root, false},
		{root + "spoke/child/cpu/**/*.parquet", root, false},
		// Names that look like a date: under the known root this is an
		// unpruned glob (database "01", measurement "15", prefix ending 2025).
		{"s3://bucket/2025/01/15/**/*.parquet", "s3://bucket/2025/", false},
		{"s3://bucket/2025/spoke/01/15/**/*.parquet", "s3://bucket/", false},
		// Root unknown or not matching: the date shape decides, towards day-wide.
		{"s3://bucket/2025/01/15/**/*.parquet", "", true},
		{"s3://other/db/cpu/2025/01/15/**/*.parquet", root, true},
		{root + "db/cpu/2025/01/15/*.parquet", root, false},
		{root + "db/cpu/2025/01/**/*.parquet", root, false},
	} {
		if got := IsDayWideGlob(c.pattern, c.root); got != c.want {
			t.Errorf("IsDayWideGlob(%q, %q) = %v, want %v", c.pattern, c.root, got, c.want)
		}
	}
}

// File-level time pruning leaves a day-wide glob as it is.
func TestFileTimePassLeavesADayWideGlobAlone(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	tr := &TimeRange{Start: time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC), End: time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC)}
	in := []string{lvlDayWide, lvlHour16G}
	got, _, _ := p.fileTimePass(context.Background(), in, tr)
	if !reflect.DeepEqual(got, in) {
		t.Errorf("fileTimePass = %v, want %v unchanged", got, in)
	}
}
