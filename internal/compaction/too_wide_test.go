package compaction

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// wideTier is an hourly tier whose candidates the test sets.
type wideTier struct {
	Tier
	candidates func() []Candidate
}

func (wideTier) GetTierName() string              { return "hourly" }
func (wideTier) IsEnabled() bool                  { return true }
func (wideTier) GetMinFiles() int                 { return 2 }
func (wideTier) GetStats() map[string]interface{} { return map[string]interface{}{} }
func (wideTier) IsCompactedFile(filename string) bool {
	return strings.HasSuffix(filename, "_compacted.parquet")
}
func (t wideTier) FindCandidates(context.Context, string, string) ([]Candidate, error) {
	return t.candidates(), nil
}

func wideCandidate(files ...string) Candidate {
	return Candidate{Database: "db", Measurement: "wide", PartitionPath: "db/wide/2026/10/25/10", Tier: "hourly", Files: files, FileCount: len(files)}
}

func errMemory() error {
	return errors.New("Out of Memory Error: failed to pin block of size 256.0 KiB (1.8 GiB/1.8 GiB used)")
}

// wideCycle runs one hourly cycle over db/wide and returns its record.
func wideCycle(t *testing.T, m *Manager) CycleRecord {
	t.Helper()
	id, _ := m.RunCompactionCycleForMeasurement(context.Background(), "db", "wide", []string{"hourly"})
	rec, found, _, _, _ := m.CycleLookup(id)
	if !found {
		t.Fatalf("cycle %d not recorded", id)
	}
	return rec
}

func wideManager(t *testing.T, files *[]string, attempt func(Candidate) error) (*Manager, *atomic.Int32) {
	t.Helper()
	m, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	m.ManifestManager = nil
	m.MaxConcurrent = 1
	m.Tiers = []Tier{wideTier{candidates: func() []Candidate { return []Candidate{wideCandidate(*files...)} }}}
	var attempts atomic.Int32
	m.attemptForTest = func(_ context.Context, c Candidate) error {
		attempts.Add(1)
		return attempt(c)
	}
	return m, &attempts
}

func TestCompactionRemembersAPartitionThatRunsOutOfMemoryAtTheSmallestBatch(t *testing.T) {
	files := []string{"a.parquet", "b.parquet", "c.parquet", "d.parquet"}
	m, attempts := wideManager(t, &files, func(Candidate) error { return errMemory() })

	rec := wideCycle(t, m)
	// 4 files: tried at 4, split into 2 + 2; the first 2 fails unsplittable.
	if rec.Failed != 1 || attempts.Load() != 2 {
		t.Fatalf("first cycle: failed %d, attempts %d", rec.Failed, attempts.Load())
	}
	entries := m.TooWidePartitions()
	if len(entries) != 1 || !entries[0].Memoized() || entries[0].Reason != "memory_error" || len(entries[0].Files) != 4 {
		t.Fatalf("memo after one out-of-memory: %+v", entries)
	}

	rec = wideCycle(t, m)
	if rec.SkippedTooWide != 1 || rec.Discovered != 0 || attempts.Load() != 2 {
		t.Fatalf("second cycle: skipped %d, discovered %d, attempts %d", rec.SkippedTooWide, rec.Discovered, attempts.Load())
	}
	if got := m.Stats()["last_cycle"].(map[string]interface{})["skipped_too_wide"]; got != int64(1) {
		t.Fatalf("stats last_cycle skipped_too_wide %v", got)
	}

	// The tier's own output (a sibling batch that compacted) is no new input.
	files = append(files, "x_compacted.parquet")
	if rec = wideCycle(t, m); rec.SkippedTooWide != 1 {
		t.Fatalf("an output of this tier retried the partition")
	}
	// A new input file retries it once.
	files = append(files, "e.parquet")
	if rec = wideCycle(t, m); rec.SkippedTooWide != 0 || rec.Failed != 1 {
		t.Fatalf("a new file did not retry: %+v", rec)
	}
	if got := m.TooWidePartitions()[0].Files; len(got) != 6 {
		t.Fatalf("memo files after the retry: %v", got)
	}
}

func TestCompactionNeedsTwoKillsInARowToRememberAPartition(t *testing.T) {
	files := []string{"a.parquet", "b.parquet"}
	m, attempts := wideManager(t, &files, func(Candidate) error {
		return errors.New("subprocess failed: signal: killed (stderr: )")
	})
	wideCycle(t, m)
	if e := m.TooWidePartitions(); len(e) != 1 || e[0].Memoized() {
		t.Fatalf("one kill memoized it: %+v", e)
	}
	wideCycle(t, m)
	if e := m.TooWidePartitions(); len(e) != 1 || !e[0].Memoized() || e[0].Reason != "killed" {
		t.Fatalf("two kills did not memoize it: %+v", e)
	}
	if rec := wideCycle(t, m); rec.SkippedTooWide != 1 || attempts.Load() != 2 {
		t.Fatalf("third cycle ran it: %+v, attempts %d", rec, attempts.Load())
	}
}

func TestCompactionForgetsAPartitionThatCompacts(t *testing.T) {
	files := []string{"a.parquet", "b.parquet"}
	var fail atomic.Bool
	fail.Store(true)
	m, _ := wideManager(t, &files, func(Candidate) error {
		if fail.Load() {
			return errors.New("subprocess failed: signal: killed")
		}
		return nil
	})
	wideCycle(t, m)
	fail.Store(false)
	if rec := wideCycle(t, m); rec.Succeeded != 1 {
		t.Fatalf("second cycle %+v", rec)
	}
	if e := m.TooWidePartitions(); len(e) != 0 {
		t.Fatalf("a compacted partition is still remembered: %+v", e)
	}
}

func TestCompactionDoesNotRememberAPermanentError(t *testing.T) {
	files := []string{"a.parquet", "b.parquet"}
	m, _ := wideManager(t, &files, func(Candidate) error {
		return errors.New("Binder Error: Referenced column \"time\" not found")
	})
	wideCycle(t, m)
	wideCycle(t, m)
	if e := m.TooWidePartitions(); len(e) != 0 {
		t.Fatalf("a SQL error was remembered: %+v", e)
	}
}

func TestCompactionRetriesARememberedPartitionWhenTheJobSizingChanges(t *testing.T) {
	files := []string{"a.parquet", "b.parquet"}
	m, attempts := wideManager(t, &files, func(Candidate) error { return errMemory() })
	wideCycle(t, m)
	m.MemoryLimit = "8GB"
	if rec := wideCycle(t, m); rec.SkippedTooWide != 0 || attempts.Load() != 2 {
		t.Fatalf("more memory per job did not retry: %+v", rec)
	}
}

func TestClearTooWide(t *testing.T) {
	files := []string{"a.parquet", "b.parquet"}
	m, attempts := wideManager(t, &files, func(Candidate) error { return errMemory() })
	wideCycle(t, m)
	if n := m.ClearTooWide("daily", ""); n != 0 {
		t.Fatalf("cleared %d of another tier", n)
	}
	if n := m.ClearTooWide("hourly", "db/wide/2026/10/25/10"); n != 1 {
		t.Fatalf("cleared %d", n)
	}
	if rec := wideCycle(t, m); rec.SkippedTooWide != 0 || attempts.Load() != 2 {
		t.Fatalf("a cleared partition was skipped: %+v", rec)
	}
}

func TestTooWideMemoSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	tier := wideTier{}
	a := newTooWideMemo(dir, zerolog.Nop())
	a.strike("hourly", "p", []string{"a.parquet", "b.parquet"}, "memory_error", "s")
	b := newTooWideMemo(dir, zerolog.Nop())
	if !b.skip(tier, "p", []string{"b.parquet", "a.parquet"}, "s") {
		t.Fatalf("not remembered after reload: %+v", b.list())
	}
	info, err := os.Stat(filepath.Join(dir, tooWideFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("memo file %v, %v", info, err)
	}
	// An unreadable file starts empty rather than failing.
	if err := os.WriteFile(filepath.Join(dir, tooWideFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if c := newTooWideMemo(dir, zerolog.Nop()); len(c.list()) != 0 {
		t.Fatal("garbage loaded")
	}
}

func TestTooWideMemoPrunesPartitionsAFullCycleNoLongerSees(t *testing.T) {
	m := newTooWideMemo("", zerolog.Nop())
	m.strike("hourly", "gone", []string{"a"}, "memory_error", "s")
	m.strike("hourly", "kept", []string{"a"}, "memory_error", "s")
	m.strike("daily", "other-tier", []string{"a"}, "memory_error", "s")
	m.prune(map[string]bool{"hourly": true}, map[string]bool{tooWideKey("hourly", "kept"): true})
	var left []string
	for _, e := range m.list() {
		left = append(left, e.Tier+"/"+e.Partition)
	}
	if strings.Join(left, ",") != "daily/other-tier,hourly/kept" {
		t.Fatalf("left %v", left)
	}
}

func TestTooWideMemoIsSafeWhenNil(t *testing.T) {
	var m *tooWideMemo
	if m.skip(wideTier{}, "p", nil, "") || m.strike("hourly", "p", nil, "memory_error", "") || m.memoized() != 0 || m.clearAll("", "") != 0 || m.list() != nil {
		t.Fatal("nil memo remembered something")
	}
	m.clear("hourly", "p")
	m.prune(nil, nil)
}

// The splitter's terminal cases all carry the attempt's reason to the memo.
func TestSplitterEndsWithAnUnsplittableError(t *testing.T) {
	for _, n := range []int{2, 3, 4, 7, 30, 40} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			m, _, cleanup := setupTestManager(t)
			defer cleanup()
			var sizes []int
			m.attemptForTest = func(_ context.Context, c Candidate) error {
				sizes = append(sizes, len(c.Files))
				return errMemory()
			}
			files := make([]string, n)
			for i := range files {
				files[i] = fmt.Sprintf("f%02d.parquet", i)
			}
			err := m.compactFilesAdaptively(context.Background(), wideCandidate(files...), files, 0, "", 1)
			if tooWideReason(err) != "memory_error" {
				t.Fatalf("ladder %v ended with %v", sizes, err)
			}
			for _, s := range sizes {
				if s < MinFilesPerBatch {
					t.Fatalf("attempted a batch of %d (ladder %v)", s, sizes)
				}
			}
			if !strings.Contains(err.Error(), "Out of Memory Error") {
				t.Fatalf("the attempt's error is lost: %v", err)
			}
		})
	}
}

func TestSplitterMarksAnAttemptTheDeadlineCutOffLate(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.CycleTimeout = 40 * time.Millisecond
	m.attemptForTest = func(ctx context.Context, _ Candidate) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.CycleTimeout)
	defer cancel()
	files := []string{"a", "b"}
	err := m.compactFilesAdaptively(ctx, wideCandidate(files...), files, 0, "", 1)
	if tooWideReason(err) != "timeout" || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late deadline: %v", err)
	}

	// Cut off early (another batch used the budget): not marked.
	m.CycleTimeout = time.Hour
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if err := m.compactFilesAdaptively(ctx2, wideCandidate(files...), files, 0, "", 1); tooWideReason(err) != "" {
		t.Fatalf("an early cut-off was marked: %v", err)
	}
	// Cancelled (shutdown, pause): never marked.
	m.CycleTimeout = time.Millisecond
	ctx3, cancel3 := context.WithCancel(context.Background())
	cancel3()
	if err := m.compactFilesAdaptively(ctx3, wideCandidate(files...), files, 0, "", 1); tooWideReason(err) != "" {
		t.Fatalf("a cancellation was marked: %v", err)
	}
}

func TestClassifyMemoryBeforeSQLErrors(t *testing.T) {
	for _, c := range []struct {
		err, stderr, reason string
	}{
		// A Go runtime out-of-memory whose stderr quotes an earlier warn
		// line with SQL error text.
		{"subprocess failed: exit status 2", `{"level":"warn","error":"Binder Error: x"} fatal error: runtime: out of memory`, "memory_error"},
		{"subprocess failed: signal: aborted", "terminate called after throwing an instance of 'std::bad_alloc'", "memory_error"},
		{"subprocess failed: signal: aborted", "", "aborted"},
		{"Binder Error: Referenced column", "", "sql_error"},
	} {
		_, reason := ClassifySubprocessError(errors.New(c.err), c.stderr)
		if reason != c.reason {
			t.Errorf("%q / %q: %s, want %s", c.err, c.stderr, reason, c.reason)
		}
	}
}
