package compaction

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1775: a day past daily_skip_file_age_check_days that already has a daily
// file folds its stragglers into another daily file once it is quiet, and
// never rewrites the one it has. A younger day keeps the rewrite.

func TestDailyTierStragglerFold1775(t *testing.T) {
	now := time.Now().UTC()
	day := func(daysAgo int) time.Time {
		d := now.AddDate(0, 0, -daysAgo)
		return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	}
	old, young := day(30), day(3)
	oldWritten := old.Add(26 * time.Hour)
	youngWritten := now.Add(-30 * time.Hour) // older than the 24 h cutoff

	hours := func(d, written time.Time, n int) []string {
		var out []string
		for h := 0; h < n; h++ {
			out = append(out, hourFile("db", "cpu", d, h, written))
		}
		return out
	}
	oldDaily := dailyFile("db", "cpu", old, oldWritten)
	oldDaily2 := fmt.Sprintf("%s/cpu_%s_%09d_b2_daily.parquet", dayDir("db", "cpu", old), oldWritten.Format("20060102_150405"), 7)
	oldDailyRewritten := oldDaily[:len(oldDaily)-len(".parquet")] + "_rewrite_1700000000000000000.parquet"
	youngDaily := dailyFile("db", "cpu", young, youngWritten)
	oldTwelve := hours(old, oldWritten, 12)
	youngTwelve := hours(young, youngWritten, 12)
	fresh := hourFile("db", "cpu", old, 20, now.Add(-time.Hour)) // still landing

	type want struct {
		settled bool
		files   []string // nil = no candidate
	}
	cases := []struct {
		name    string
		tier    *DailyTier
		objects []string
		want    want
	}{
		{"old quiet day: stragglers fold, daily left out", settledTier(12, 7, 24),
			append([]string{oldDaily}, oldTwelve...), want{true, oldTwelve}},
		{"old day, several daily files incl. a DELETE rewrite: none in the job", settledTier(12, 7, 24),
			append([]string{oldDaily2, oldDailyRewritten}, oldTwelve...), want{true, oldTwelve}},
		{"old day with a daily file still receiving files: waits", settledTier(12, 7, 24),
			append(append([]string{oldDaily}, oldTwelve...), fresh), want{}},
		{"old day without a daily file, backfill landing: regular first compaction", settledTier(12, 7, 24),
			append(append([]string{}, oldTwelve...), fresh), want{false, append(append([]string{}, oldTwelve...), fresh)}},
		{"young day keeps the rewrite (daily included, dedup across it)", settledTier(12, 7, 24),
			append([]string{youngDaily}, youngTwelve...), want{false, append([]string{youngDaily}, youngTwelve...)}},
		{"young day, daily_min_files=1: rewrite of daily + 1 straggler, two inputs", settledTier(1, 7, 24),
			[]string{youngDaily, youngTwelve[0]}, want{false, []string{youngDaily, youngTwelve[0]}}},
		// Non-default skip_file_age_check_days moves the line.
		{"skip_file_age_check_days=1: a 3-day-old day folds", settledTier(12, 1, 24),
			append([]string{youngDaily}, youngTwelve...), want{true, youngTwelve}},
		{"skip_file_age_check_days=40: a 30-day-old day keeps the rewrite", settledTier(12, 40, 24),
			append([]string{oldDaily}, oldTwelve...), want{false, append([]string{oldDaily}, oldTwelve...)}},
		// Non-default daily_min_age_hours sets the quiet period.
		{"daily_min_age_hours=1: a file written 2h ago is quiet", settledTier(12, 7, 1),
			[]string{oldDaily, hourFile("db", "cpu", old, 4, now.Add(-2*time.Hour))},
			want{true, []string{hourFile("db", "cpu", old, 4, now.Add(-2*time.Hour))}}},
		{"daily_min_age_hours=1: still waits on a file written 10m ago", settledTier(12, 7, 1),
			append([]string{oldDaily, hourFile("db", "cpu", old, 4, now.Add(-10*time.Minute))}, oldTwelve...), want{}},
		{"a fold's own fresh daily file holds the day for min_age_hours", settledTier(12, 7, 24),
			append([]string{dailyFile("db", "cpu", old, now.Add(-time.Hour))}, oldTwelve...), want{}},
		{"lone daily files: nothing", settledTier(12, 7, 24), []string{oldDaily, oldDaily2}, want{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.tier.FindCandidatesFromListing("db", "cpu", tc.objects)
			if tc.want.files == nil {
				if len(got) != 0 {
					t.Fatalf("got candidates %+v, want none", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("got %d candidates %+v, want 1", len(got), got)
			}
			c := got[0]
			gotFiles := append([]string(nil), c.Files...)
			wantFiles := append([]string(nil), tc.want.files...)
			sort.Strings(gotFiles)
			sort.Strings(wantFiles)
			if c.Settled != tc.want.settled || fmt.Sprint(gotFiles) != fmt.Sprint(wantFiles) || c.FileCount != len(wantFiles) {
				t.Fatalf("candidate settled=%v files=%v count=%d, want settled=%v files=%v",
					c.Settled, gotFiles, c.FileCount, tc.want.settled, wantFiles)
			}
		})
	}
}

// Through a real cycle: the job the manager starts for an old day with a
// daily file does not read the daily file.
func TestStragglerFoldJobLeavesDailyOut1775(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	now := time.Now().UTC()
	d := now.AddDate(0, 0, -30)
	old := time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	written := old.Add(26 * time.Hour)
	daily := dailyFile("db", "cpu", old, written)
	files := []string{daily}
	for h := 0; h < 12; h++ {
		files = append(files, hourFile("db", "cpu", old, h, written))
	}
	for _, f := range files {
		if err := b.Write(ctx, f, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	m.Tiers = []Tier{NewDailyTier(&DailyTierConfig{StorageBackend: b, Enabled: true, Logger: zerolog.Nop()})}

	old2 := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old2 })
	var mu sync.Mutex
	var inputs [][]string
	runJobInSubprocess = func(_ context.Context, cfg *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		mu.Lock()
		inputs = append(inputs, append([]string(nil), cfg.Files...))
		mu.Unlock()
		return &SubprocessJobResult{Success: true}, nil
	}
	if _, err := m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"daily"}); err != nil {
		t.Fatal(err)
	}
	if len(inputs) == 0 {
		t.Fatal("no job ran")
	}
	n := 0
	for _, in := range inputs {
		for _, f := range in {
			if f == daily {
				t.Fatalf("job read the existing daily file: %v", in)
			}
			n++
		}
	}
	if n != 12 {
		t.Fatalf("jobs read %d files, want the 12 stragglers: %v", n, inputs)
	}
}

// A too-wide entry recorded while the day's daily file was an input does not
// hold the smaller fold job; a failure of that job memoizes it afresh.
func TestTooWideEntryWithDailyFileRetriesTheFold1775(t *testing.T) {
	tier := settledTier(12, 7, 24)
	mem := &tooWideMemo{entries: map[string]*TooWideEntry{}, logger: zerolog.Nop()}
	part := "db/cpu/2026/10/01"
	daily := part + "/cpu_20261002_020000_000000001_b1_daily.parquet"
	s1, s2 := part+"/03/cpu_20261002_030000_000000001.parquet", part+"/04/cpu_20261002_040000_000000001.parquet"
	const settings = "s"

	mem.strike("daily", part, []string{daily, s1, s2}, "memory_error", settings)
	if !mem.skip(tier, part, []string{daily, s1, s2}, settings) {
		t.Fatal("the rewrite job that failed is not held")
	}
	if !mem.skip(tier, part, []string{daily, s1}, settings) {
		t.Fatal("a subset that still reads the daily file is not held")
	}
	if mem.skip(tier, part, []string{s1, s2}, settings) {
		t.Fatal("the fold job without the daily file is held by the rewrite's failure")
	}
	if len(mem.list()) != 0 {
		t.Fatalf("stale entry kept: %+v", mem.list())
	}
	mem.strike("daily", part, []string{s1, s2}, "memory_error", settings)
	for i := 0; i < 2; i++ {
		if !mem.skip(tier, part, []string{s1, s2}, settings) {
			t.Fatalf("the fold job's own failure is not held (pass %d)", i)
		}
	}
	// A new daily output beside it (a sibling batch) still does not count.
	if !mem.skip(tier, part, []string{s1, s2, part + "/cpu_20261119_000000_000000002_b2_daily.parquet"}, settings) {
		t.Fatal("a sibling daily output released the memo")
	}
}

// A sibling batch that consumed the recorded daily file and wrote another
// does not release the memo: the batch that failed is the same job.
func TestTooWideSiblingConsumedOutputKeepsEntry1775(t *testing.T) {
	daily := settledTier(12, 7, 24)
	mem := &tooWideMemo{entries: map[string]*TooWideEntry{}, logger: zerolog.Nop()}
	part := "db/cpu/2026/11/16"
	d1 := part + "/cpu_20261117_020000_000000001_b1_daily.parquet"
	d2 := part + "/cpu_20261119_020000_000000002_b1_daily.parquet"
	s1 := part + "/03/cpu_20261117_030000_000000001.parquet"
	mem.strike("daily", part, []string{d1, s1}, "memory_error", "s")
	if !mem.skip(daily, part, []string{d2, s1}, "s") {
		t.Fatal("a sibling's new daily output released the memo")
	}

	hourly := NewHourlyTier(&HourlyTierConfig{Enabled: true, Logger: zerolog.Nop()})
	hp := "db/cpu/2026/11/16/03"
	c1 := hp + "/cpu_20261117_030000_000000001_compacted.parquet"
	c2 := hp + "/cpu_20261119_030000_000000002_compacted.parquet"
	r := hp + "/cpu_20261117_031000_000000001.parquet"
	mem.strike("hourly", hp, []string{c1, r}, "memory_error", "s")
	if !mem.skip(hourly, hp, []string{c2, r}, "s") {
		t.Fatal("hourly: a sibling's new compacted output released the memo")
	}
}

// A settled fold split into batches: every batch that ran is remembered, so
// the day is not run again while its files stay the ones that failed.
func TestSettledMemoAcrossBatches1775(t *testing.T) {
	s := newSettledMemo()
	b1, b2 := []string{"a", "b"}, []string{"c", "d"}
	s.record("daily", "p", b1)
	s.record("daily", "p", b2)
	if !s.skip("daily", "p", []string{"a", "b", "c", "d"}) {
		t.Fatal("both batches failed: the day is run again")
	}
	if !s.skip("daily", "p", b2) {
		t.Fatal("batch 1 compacted, batch 2 failed: the rest is run again")
	}
	if s.skip("daily", "p", []string{"c", "d", "e"}) {
		t.Fatal("a new file did not make a new attempt")
	}
	if s.skip("daily", "p", b2) {
		t.Fatal("the entry outlived the new attempt")
	}
}
