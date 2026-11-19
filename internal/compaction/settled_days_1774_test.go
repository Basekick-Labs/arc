package compaction

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1774: a settled day with fewer hour-level files than daily_min_files gets a
// daily file, so tiering (which moves only daily files) can take it to cold.

func dayDir(db, meas string, day time.Time) string {
	return fmt.Sprintf("%s/%s/%04d/%02d/%02d", db, meas, day.Year(), day.Month(), day.Day())
}

// hourFile names an hour-level raw file written at `written`.
func hourFile(db, meas string, day time.Time, hour int, written time.Time) string {
	return fmt.Sprintf("%s/%02d/%s_%s_%09d.parquet", dayDir(db, meas, day), hour, meas,
		written.UTC().Format("20060102_150405"), written.Nanosecond())
}

// dailyFile names a daily output written at `written`.
func dailyFile(db, meas string, day time.Time, written time.Time) string {
	return fmt.Sprintf("%s/%s_%s_%09d_b1_daily.parquet", dayDir(db, meas, day), meas,
		written.UTC().Format("20060102_150405"), written.Nanosecond())
}

func settledTier(minFiles, skipDays, minAgeHours int) *DailyTier {
	return NewDailyTier(&DailyTierConfig{
		MinAgeHours:          minAgeHours,
		MinFiles:             minFiles,
		SkipFileAgeCheckDays: skipDays,
		Enabled:              true,
		Logger:               zerolog.Nop(),
	})
}

func TestDailyTierSettledSparseDays1774(t *testing.T) {
	now := time.Now().UTC()
	day := func(daysAgo int) time.Time {
		d := now.AddDate(0, 0, -daysAgo)
		return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
	}
	old := day(30)
	written := old.Add(26 * time.Hour) // a day after the data, long settled

	type want struct {
		settled bool
		files   []string // exact Files of the one candidate; nil = no candidate
	}
	lone := hourFile("db", "cpu", old, 3, written)
	two := hourFile("db", "cpu", old, 5, written)
	daily := dailyFile("db", "cpu", old, written)

	var twelve []string
	for h := 0; h < 12; h++ {
		twelve = append(twelve, hourFile("db", "cpu", old, h, written))
	}

	cases := []struct {
		name    string
		tier    *DailyTier
		objects []string
		want    want
	}{
		{"one hour file on a settled day", settledTier(12, 7, 24), []string{lone}, want{true, []string{lone}}},
		{"stragglers next to a daily file: only the stragglers", settledTier(12, 7, 24),
			[]string{daily, lone, two}, want{true, []string{lone, two}}},
		{"lone daily file: nothing", settledTier(12, 7, 24), []string{daily}, want{}},
		{"file written within min_age_hours: a backfill still landing", settledTier(12, 7, 24),
			[]string{hourFile("db", "cpu", old, 3, now.Add(-time.Hour))}, want{}},
		{"recent daily file holds the day back too", settledTier(12, 7, 24),
			[]string{lone, dailyFile("db", "cpu", old, now.Add(-time.Hour))}, want{}},
		{"day younger than skip_file_age_check_days", settledTier(12, 7, 24),
			[]string{hourFile("db", "cpu", day(3), 3, day(3).Add(26*time.Hour))}, want{}},
		{"unreadable file time: not settled", settledTier(12, 7, 24),
			[]string{dayDir("db", "cpu", old) + "/03/foreign.parquet"}, want{}},
		// #1775: MinFiles or more stragglers next to a daily file on a
		// settled day fold separately too; the daily file is not rewritten.
		{"MinFiles or more next to a daily file: only the stragglers", settledTier(12, 7, 24),
			append([]string{daily}, twelve...), want{true, twelve}},
		{"MinFiles or more without a daily file keeps the regular case", settledTier(12, 7, 24),
			twelve, want{false, twelve}},
		{"daily_min_files=1: a lone straggler is settled, not a rewrite of the daily", settledTier(1, 7, 24),
			[]string{daily, lone}, want{true, []string{lone}}},
		{"daily_min_files=1: two stragglers fold without the daily (#1775)", settledTier(1, 7, 24),
			[]string{daily, lone, two}, want{true, []string{lone, two}}},
		{"DELETE rewrite of an hour file counts as hour-level", settledTier(12, 7, 24),
			[]string{daily, lone[:len(lone)-len(".parquet")] + "_rewrite_1700000000000000000.parquet"},
			want{true, []string{lone[:len(lone)-len(".parquet")] + "_rewrite_1700000000000000000.parquet"}}},
		// Non-default skip_file_age_check_days: the settled age follows it.
		{"skip_file_age_check_days=40 holds a 30-day-old day", settledTier(12, 40, 24), []string{lone}, want{}},
		{"skip_file_age_check_days=1 settles a 3-day-old day", settledTier(12, 1, 24),
			[]string{hourFile("db", "cpu", day(3), 3, day(3).Add(26*time.Hour))},
			want{true, []string{hourFile("db", "cpu", day(3), 3, day(3).Add(26*time.Hour))}}},
		// Non-default daily_min_age_hours: the quiet period follows it.
		{"daily_min_age_hours=1 accepts a file written 2h ago", settledTier(12, 7, 1),
			[]string{hourFile("db", "cpu", old, 3, now.Add(-2*time.Hour))},
			want{true, []string{hourFile("db", "cpu", old, 3, now.Add(-2*time.Hour))}}},
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
			if c.Settled != tc.want.settled {
				t.Fatalf("Settled = %v, want %v", c.Settled, tc.want.settled)
			}
			if fmt.Sprint(c.Files) != fmt.Sprint(tc.want.files) || c.FileCount != len(tc.want.files) || c.Tier != "daily" {
				t.Fatalf("candidate %+v, want files %v", c, tc.want.files)
			}
		})
	}
}

func TestDailyTierSettledSpokeNamespace1774(t *testing.T) {
	old := time.Now().UTC().AddDate(0, 0, -30)
	old = time.Date(old.Year(), old.Month(), old.Day(), 0, 0, 0, 0, time.UTC)
	f := hourFile("rocket-01/db", "cpu", old, 7, old.Add(26*time.Hour))
	got := settledTier(12, 7, 24).FindCandidatesFromListing("rocket-01/db", "cpu", []string{f})
	if len(got) != 1 || !got[0].Settled || got[0].Files[0] != f {
		t.Fatalf("spoke-namespace settled day: %+v", got)
	}
}

func TestSplitCandidateKeepsSettled1774(t *testing.T) {
	c := Candidate{Settled: true, Files: []string{"a", "b", "c", "d", "e"}}
	batches := SplitCandidateIntoBatches(c, 2)
	if len(batches) < 2 {
		t.Fatalf("expected a split, got %d batches", len(batches))
	}
	for _, b := range batches {
		if !b.Settled {
			t.Fatalf("batch %d lost Settled", b.BatchNumber)
		}
	}
}

func TestSingleFileJobFloor1774(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	ctx := context.Background()
	var ran []string
	m.attemptForTest = func(_ context.Context, c Candidate) error {
		ran = append(ran, c.Files...)
		return nil
	}
	hour := "db/cpu/2026/01/01/03/cpu_20260102_020000_000000001.parquet"
	daily := "db/cpu/2026/01/01/cpu_20260102_020000_000000001_b1_daily.parquet"

	if err := m.compactFilesAdaptively(ctx, Candidate{Settled: true, Files: []string{hour}}, []string{hour}, 0, "", 0); err != nil || len(ran) != 1 {
		t.Fatalf("settled one-file job: err %v ran %v", err, ran)
	}
	ran = nil
	// A regular candidate shrunk to one file by a filter stays refused.
	if err := m.compactFilesAdaptively(ctx, Candidate{Files: []string{hour}}, []string{hour}, 0, "", 0); err == nil || len(ran) != 0 {
		t.Fatalf("regular one-file job ran: err %v ran %v", err, ran)
	}
	// Never rewrite a lone daily file, even on a settled candidate.
	if err := m.compactFilesAdaptively(ctx, Candidate{Settled: true, Files: []string{daily}}, []string{daily}, 0, "", 0); err == nil || len(ran) != 0 {
		t.Fatalf("lone daily rewritten: err %v ran %v", err, ran)
	}
}

type dailyTierStub struct{ cycleTierIssue915 }

func (dailyTierStub) GetMinFiles() int { return 12 }

func TestFiltersUseSettledFloor1774(t *testing.T) {
	m := activeJobsRig(t, localBackend(t))
	a, b := "db/cpu/2026/01/01/03/a.parquet", "db/cpu/2026/01/01/04/b.parquet"
	m.SetPendingFilter(func(p string) bool { return p == b })
	tier := dailyTierStub{}

	got, ok := m.filterPending(Candidate{Settled: true, Files: []string{a, b}}, tier)
	if !ok || len(got.Files) != 1 || got.Files[0] != a {
		t.Fatalf("settled candidate after pending filter: ok %v %+v", ok, got)
	}
	if _, ok := m.filterPending(Candidate{Files: []string{a, b}}, tier); ok {
		t.Fatal("regular candidate kept with 1 file under MinFiles 12")
	}
	m.SetSyncEligibility(func(_ context.Context, paths []string) (map[string]bool, error) {
		return map[string]bool{a: true}, nil
	})
	got, ok, _ = m.filterSyncEligibilityWithError(context.Background(), Candidate{Settled: true, Files: []string{a, b}}, tier)
	if !ok || len(got.Files) != 1 {
		t.Fatalf("settled candidate after sync filter: ok %v %+v", ok, got)
	}
}

func TestStagedSettledOneFile1774(t *testing.T) {
	backend := localBackend(t)
	m := activeJobsRig(t, backend)
	ctx := context.Background()
	hour := "db/cpu/2026/01/01/03/cpu_20260102_020000_000000001.parquet"
	other := "db/cpu/2026/01/01/04/cpu_20260102_020000_000000002.parquet"
	daily := "db/cpu/2026/01/01/cpu_20260102_020000_000000001_b1_daily.parquet"
	for _, f := range []string{hour, other, daily} {
		if err := backend.Write(ctx, f, []byte("12345")); err != nil {
			t.Fatal(err)
		}
	}
	m.SetStagedCommit(func() bool { return true },
		func(p string) (int64, string, bool) {
			if p == other {
				return 5, "", false // not listed unchanged: the staged filter drops it
			}
			return 5, "sha", true
		},
		func(string) bool { return false })

	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	var mu sync.Mutex
	var jobs [][]string
	runJobInSubprocess = func(_ context.Context, cfg *SubprocessJobConfig, _ zerolog.Logger) (*SubprocessJobResult, error) {
		mu.Lock()
		jobs = append(jobs, cfg.Files)
		mu.Unlock()
		return &SubprocessJobResult{Success: true}, nil
	}
	run := func(c Candidate) {
		t.Helper()
		c.Database, c.PartitionPath, c.BatchNumber = "db", "cpu/2026/01/01", 1
		_ = m.compactPartition(ctx, c, jobAttribution{})
	}

	run(Candidate{Settled: true, Files: []string{hour}})
	if len(jobs) != 1 || len(jobs[0]) != 1 || jobs[0][0] != hour {
		t.Fatalf("settled one-file staged job: %v", jobs)
	}
	jobs = nil
	run(Candidate{Files: []string{hour, other}}) // regular, reduced to one
	run(Candidate{Settled: true, Files: []string{other}})
	run(Candidate{Settled: true, Files: []string{daily}})
	if len(jobs) != 0 {
		t.Fatalf("staged jobs ran that must not: %v", jobs)
	}
}

// Settled candidates run after every regular candidate of the tier, capped.
func TestSettledCandidatesRunLastAndCapped1774(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.MaxConcurrent = 1
	var cands []Candidate
	settled := func(i int) Candidate {
		c := candidateIssue915(fmt.Sprintf("cpu/2026/01/%02d/settled%d", 1+i%28, i))
		c.Settled = true
		return c
	}
	cands = append(cands, settled(0))
	cands = append(cands, candidateIssue915("cpu/2026/02/01/regular"))
	for i := 1; i <= maxSettledCandidatesPerCycle; i++ { // one over the cap in total
		cands = append(cands, settled(i))
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return cands, nil }}}
	var mu sync.Mutex
	var order []Candidate
	m.compactBatchForTest = func(_ context.Context, c Candidate) error {
		mu.Lock()
		order = append(order, c)
		mu.Unlock()
		return nil
	}
	if _, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if len(order) != 1+maxSettledCandidatesPerCycle {
		t.Fatalf("ran %d batches, want %d", len(order), 1+maxSettledCandidatesPerCycle)
	}
	if order[0].Settled {
		t.Fatalf("a settled candidate ran before the regular one: %+v", order[0])
	}
	for _, c := range order[1:] {
		if !c.Settled {
			t.Fatalf("regular candidate ran after settled ones")
		}
	}
}

func TestSettledMemo1774(t *testing.T) {
	s := newSettledMemo()
	clock := time.Unix(1_800_000_000, 0)
	s.now = func() time.Time { return clock }
	s.record("daily", "db/cpu/2026/01/01", []string{"b", "a"})
	if !s.skip("daily", "db/cpu/2026/01/01", []string{"a", "b"}) {
		t.Fatal("same file set, any order: want skip")
	}
	if s.skip("hourly", "db/cpu/2026/01/01", []string{"a", "b"}) {
		t.Fatal("other tier skipped")
	}
	clock = clock.Add(settledRetryAfter)
	if s.skip("daily", "db/cpu/2026/01/01", []string{"a", "b"}) {
		t.Fatal("skipped past settledRetryAfter")
	}
	s.record("daily", "db/cpu/2026/01/01", []string{"a"})
	if s.skip("daily", "db/cpu/2026/01/01", []string{"a", "c"}) {
		t.Fatal("a new file in the day: want a new attempt")
	}
	if s.skip("daily", "db/cpu/2026/01/01", []string{"a"}) {
		t.Fatal("a different file set must drop the entry")
	}
	var nilMemo *settledMemo
	nilMemo.record("daily", "p", []string{"a"})
	if nilMemo.skip("daily", "p", []string{"a"}) {
		t.Fatal("nil memo skipped")
	}
}

// A settled day whose job ran and left its files is not run again next
// cycle while they stay the same; new files make it a new attempt. A
// compactPartition that returned without running a job records nothing.
func TestSettledDayThatFailsIsNotRetriedEveryCycle1774(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	part := "db/cpu/2026/01/01"
	c := candidateIssue915(part)
	c.Settled, c.Files, c.FileCount = true, []string{part + "/03/corrupt.parquet"}, 1
	if err := b.Write(ctx, c.Files[0], []byte("not parquet")); err != nil {
		t.Fatal(err)
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		return []Candidate{c}, nil
	}}}
	old := runJobInSubprocess
	t.Cleanup(func() { runJobInSubprocess = old })
	runs := 0
	runJobInSubprocess = func(context.Context, *SubprocessJobConfig, zerolog.Logger) (*SubprocessJobResult, error) {
		runs++
		return &SubprocessJobResult{Success: false, Error: "no valid parquet files found"}, fmt.Errorf("no valid parquet files found")
	}

	// The partition lock is held elsewhere: no job runs, nothing is recorded.
	lockKey := filepath.Join(c.Database, c.PartitionPath)
	if !m.LockManager.AcquireLock(lockKey) {
		t.Fatal("lock")
	}
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	m.LockManager.ReleaseLock(lockKey)
	if runs != 0 || m.settled.skip(c.Tier, c.PartitionPath, c.Files) {
		t.Fatalf("lock-busy return was recorded: runs %d", runs)
	}

	for i := 0; i < 3; i++ {
		_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	}
	if runs != 1 {
		t.Fatalf("job ran %d times over 3 cycles, want 1", runs)
	}
	c.Files = append(c.Files, part+"/04/new.parquet")
	_, _ = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if runs != 2 {
		t.Fatalf("a changed file set did not run: %d", runs)
	}
}

// Settled candidates the filters defer do not use a slot of the cap.
func TestSettledCapCountsStartedJobsOnly1774(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.MaxConcurrent = 1
	const deferred = 50
	var cands []Candidate
	for i := 0; i < deferred+maxSettledCandidatesPerCycle+1; i++ {
		c := candidateIssue915(fmt.Sprintf("db/cpu/2026/01/01/s%d", i))
		c.Settled = true
		c.Files = []string{fmt.Sprintf("db/cpu/2026/01/01/%d.parquet", i)}
		if i < deferred {
			c.Files[0] = fmt.Sprintf("db/cpu/2026/01/01/pending%d.parquet", i)
		}
		cands = append(cands, c)
	}
	m.SetPendingFilter(func(p string) bool { return strings.Contains(p, "/pending") })
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return cands, nil }}}
	var mu sync.Mutex
	ran := 0
	m.compactBatchForTest = func(context.Context, Candidate) error {
		mu.Lock()
		ran++
		mu.Unlock()
		return nil
	}
	if _, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	if ran != maxSettledCandidatesPerCycle {
		t.Fatalf("ran %d, want %d: deferred candidates used slots", ran, maxSettledCandidatesPerCycle)
	}
}
