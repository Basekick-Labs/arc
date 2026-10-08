package compaction

// Job-level cycle attribution (#1162 follow-up). #1162 shipped a 10-entry
// sample of failed partitions on the cycle record, on the assumption that job
// history could answer the rest once it carried a cycle id. It cannot -- see
// cycleFailedPartitionLimit -- so this change does three things: attributes
// jobs to their cycle, makes the history limit actually work above 10, and
// makes the per-cycle partition list the complete answer instead of a sample.

import (
	"fmt"
	"testing"
	"time"
)

// seedJob appends one job record the way the compaction path does, going
// through the production attribution helper rather than reimplementing it.
//
// Seeded directly because CompactPartition -- the only thing that writes job
// history -- runs a real compaction subprocess and cannot be called from a
// unit test. That is also why job recording has never had coverage, and why
// the end-to-end assertion for this change is a live binary run.
func seedJob(m *Manager, attr jobAttribution, partition string, success bool) {
	job := map[string]interface{}{
		"database":       "db_d",
		"measurement":    "wide_table",
		"partition_path": partition,
		"tier":           "hourly",
		"success":        success,
	}
	attributeJobToCycle(job, attr)
	m.mu.Lock()
	m.jobHistory = appendJobHistory(m.jobHistory, job)
	m.mu.Unlock()
}

// The limit actually works above 10. Regression test for a shipped bug: Stats
// truncated recent_jobs to the newest 10 and getHistory applied ?limit= to
// that already-truncated slice, so ?limit=50 returned 10.
func TestJobHistoryHonoursLimitsAboveTenIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	for i := 0; i < 40; i++ {
		seedJob(manager, jobAttribution{CycleID: 7}, fmt.Sprintf("db_d/wide_table/2026/10/08/%02d", i), false)
	}

	page := manager.JobHistory(50, 0)
	if page.Matched != 40 {
		t.Errorf("Matched = %d, want 40", page.Matched)
	}
	if len(page.Jobs) != 40 {
		t.Fatalf("JobHistory(50, 0) returned %d jobs, want 40 -- a limit above 10 must not be capped", len(page.Jobs))
	}

	page = manager.JobHistory(10, 0)
	if len(page.Jobs) != 10 || page.Matched != 40 {
		t.Errorf("JobHistory(10, 0) = %d jobs, Matched %d; want 10 and 40", len(page.Jobs), page.Matched)
	}
	// Newest ten, chronological.
	if got := page.Jobs[len(page.Jobs)-1]["partition_path"]; got != "db_d/wide_table/2026/10/08/39" {
		t.Errorf("last job = %v, want the newest partition", got)
	}
	if got := page.Jobs[0]["partition_path"]; got != "db_d/wide_table/2026/10/08/30" {
		t.Errorf("first job = %v, want the 10th-newest partition", got)
	}
}

// Matched is the pre-limit count: it distinguishes "10 failures" from "10 of
// 40", which is the difference between finishing a migration and silently
// skipping three quarters of the retries.
func TestJobHistoryReportsThePreLimitMatchCountIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	for i := 0; i < 25; i++ {
		seedJob(manager, jobAttribution{CycleID: 7}, fmt.Sprintf("p%02d", i), false)
	}

	page := manager.JobHistory(10, 7)
	if page.Matched != 25 {
		t.Errorf("Matched = %d, want 25 (the pre-limit total)", page.Matched)
	}
	if len(page.Jobs) != 10 {
		t.Errorf("returned %d jobs, want the limit of 10", len(page.Jobs))
	}
	if page.Matched <= len(page.Jobs) {
		t.Error("Matched must exceed the page size here, or the response cannot signal truncation")
	}
}

// Jobs are separable by cycle. Without this an operator's trigger and the
// hourly scheduler's cycle in between are indistinguishable.
func TestJobHistoryFiltersByCycleIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	seedJob(manager, jobAttribution{CycleID: 11}, "mine-a", false)
	seedJob(manager, jobAttribution{CycleID: 12}, "schedulers-a", true)
	seedJob(manager, jobAttribution{CycleID: 11}, "mine-b", false)
	seedJob(manager, jobAttribution{CycleID: 12}, "schedulers-b", true)
	seedJob(manager, jobAttribution{}, "no-cycle", true)

	mine := manager.JobHistory(100, 11)
	if mine.Matched != 2 || len(mine.Jobs) != 2 {
		t.Fatalf("cycle 11: %d jobs, Matched %d; want 2 and 2", len(mine.Jobs), mine.Matched)
	}
	for _, job := range mine.Jobs {
		if job["cycle_id"] != int64(11) {
			t.Errorf("cycle 11 filter returned a job from cycle %v", job["cycle_id"])
		}
	}
	if mine.Jobs[0]["partition_path"] != "mine-a" || mine.Jobs[1]["partition_path"] != "mine-b" {
		t.Errorf("got %v then %v, want mine-a then mine-b (chronological)",
			mine.Jobs[0]["partition_path"], mine.Jobs[1]["partition_path"])
	}

	all := manager.JobHistory(100, 0)
	if all.Matched != 5 || len(all.Jobs) != 5 {
		t.Errorf("unfiltered: %d jobs, Matched %d; want 5 and 5", len(all.Jobs), all.Matched)
	}

	// A cycle with no jobs is empty, not everything -- the failure mode that
	// would make a filter read as "this cycle touched the whole store".
	none := manager.JobHistory(100, 99)
	if none.Matched != 0 || len(none.Jobs) != 0 {
		t.Errorf("cycle 99: %d jobs, Matched %d; want 0 and 0", len(none.Jobs), none.Matched)
	}
}

// A job recorded outside any cycle omits the key rather than reporting cycle 0.
func TestJobWithoutACycleOmitsTheKeyIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	seedJob(manager, jobAttribution{}, "orphan", true)

	page := manager.JobHistory(10, 0)
	if len(page.Jobs) != 1 {
		t.Fatalf("returned %d jobs, want 1", len(page.Jobs))
	}
	if raw, present := page.Jobs[0]["cycle_id"]; present {
		t.Errorf("cycle_id present on a job that ran outside any cycle: %v", raw)
	}
}

// attributeJobToCycle is the production guard, asserted directly.
//
// It is a function rather than inlined lines precisely so this test can exist:
// its only call site is inside CompactPartition, which runs a real compaction
// subprocess and is unreachable from a unit test. While the guard was inlined
// there, a mutation recording cycle_id unconditionally passed the entire
// suite, because the fixture above seeded history through its own copy of the
// guard and never executed the real one.
func TestAttributeJobToCycleOmitsZeroIssue1162(t *testing.T) {
	for _, tc := range []struct {
		cycleID int64
		want    interface{}
		present bool
	}{
		{cycleID: 412, want: int64(412), present: true},
		{cycleID: 1, want: int64(1), present: true},
		{cycleID: 0, present: false},
		{cycleID: -5, present: false},
	} {
		job := map[string]interface{}{"partition_path": "p"}
		attributeJobToCycle(job, jobAttribution{CycleID: tc.cycleID, AttemptDepth: 2})
		got, present := job["cycle_id"]
		if present != tc.present {
			t.Errorf("cycleID %d: cycle_id present = %v, want %v", tc.cycleID, present, tc.present)
			continue
		}
		if present && got != tc.want {
			t.Errorf("cycleID %d: cycle_id = %v, want %v", tc.cycleID, got, tc.want)
		}
		// attempt_depth is always recorded: a record with no depth cannot be
		// told apart from an outer-level attempt, which is the only depth
		// whose failures match the cycle's failed_batches.
		if got := job["attempt_depth"]; got != 2 {
			t.Errorf("cycleID %d: attempt_depth = %v, want 2", tc.cycleID, got)
		}
	}
}

// Returned records are copies: the retained ring must not escape the lock.
func TestJobHistoryReturnsCopiesIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	seedJob(manager, jobAttribution{CycleID: 7}, "original", true)

	page := manager.JobHistory(10, 0)
	page.Jobs[0]["partition_path"] = "mutated-by-caller"
	page.Jobs[0]["injected"] = true

	again := manager.JobHistory(10, 0)
	if got := again.Jobs[0]["partition_path"]; got != "original" {
		t.Errorf("retained record was mutated through a returned map: %v", got)
	}
	if _, present := again.Jobs[0]["injected"]; present {
		t.Error("a key injected into a returned map reached the retained record")
	}
}

// Stats keeps its own 10-entry view and no longer hands out a slice of the
// live ring's backing array.
func TestStatsRecentJobsStaysCappedAndCopiedIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	for i := 0; i < 30; i++ {
		seedJob(manager, jobAttribution{CycleID: 7}, fmt.Sprintf("p%02d", i), true)
	}

	recent, ok := manager.Stats()["recent_jobs"].([]map[string]interface{})
	if !ok {
		t.Fatal("recent_jobs is not a slice of maps")
	}
	if len(recent) != 10 {
		t.Errorf("recent_jobs holds %d entries, want the unchanged cap of 10", len(recent))
	}

	manager.mu.Lock()
	ring := manager.jobHistory
	manager.mu.Unlock()
	if len(recent) > 0 && len(ring) >= 10 && &recent[0] == &ring[len(ring)-10] {
		t.Error("recent_jobs shares the ring's backing array; a caller reads it with no lock")
	}
}

// The ring bound the API clamps to must match what the ring retains, and a
// full ring must say so -- that flag is the only thing standing between an
// operator and reading an evicted page as complete.
func TestJobHistoryReportsAFullRingIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	notFull := manager.JobHistory(10, 0)
	if notFull.RingFull {
		t.Error("an empty ring reported itself full")
	}

	for i := 0; i < JobHistoryLimit+25; i++ {
		seedJob(manager, jobAttribution{CycleID: 7}, fmt.Sprintf("p%03d", i), true)
	}

	page := manager.JobHistory(JobHistoryLimit, 0)
	if page.Matched != JobHistoryLimit {
		t.Errorf("Matched = %d, want the ring bound %d", page.Matched, JobHistoryLimit)
	}
	if page.Retained != JobHistoryLimit {
		t.Errorf("Retained = %d, want %d", page.Retained, JobHistoryLimit)
	}
	if !page.RingFull {
		t.Error("a full ring did not report RingFull; an evicted page would read as complete")
	}
	if got := page.Jobs[len(page.Jobs)-1]["partition_path"]; got != fmt.Sprintf("p%03d", JobHistoryLimit+24) {
		t.Errorf("newest retained = %v, want the last seeded job", got)
	}
}

// A non-positive limit is clamped, not turned into "nothing matched". Matched
// must never lie.
func TestJobHistoryClampsNonPositiveLimitIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)
	seedJob(manager, jobAttribution{CycleID: 7}, "p", true)

	for _, limit := range []int{0, -1, -100} {
		page := manager.JobHistory(limit, 0)
		if page.Matched != 1 {
			t.Errorf("JobHistory(%d, 0): Matched = %d, want 1 -- a bad limit must not report nothing matched", limit, page.Matched)
		}
		if len(page.Jobs) != 1 {
			t.Errorf("JobHistory(%d, 0) returned %d jobs, want the clamped 1", limit, len(page.Jobs))
		}
	}
}

// The cycle cross-check. The job ring holds attempts and gets evicted; the
// cycle record holds the authoritative outer-level counts and is retained far
// longer, so a filtered page must carry it. And when the cycle is gone
// entirely, zero rows must not be reported as "nothing failed".
func TestJobHistoryCrossChecksTheCycleRecordIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	// A retained cycle with known outer-level outcomes.
	manager.mu.Lock()
	manager.cycleHistory = appendCycleHistory(manager.cycleHistory, CycleRecord{
		CycleID: 50, Status: "failed", Started: 80, Failed: 80,
		FinishedAt:       time.Now().UTC(),
		FailedPartitions: map[string]int{"db_d/wide_table/2026/09/14/08": 19},
	})
	manager.mu.Unlock()
	seedJob(manager, jobAttribution{CycleID: 50}, "db_d/wide_table/2026/09/14/08", false)

	page := manager.JobHistory(10, 50)
	if !page.CycleFound {
		t.Fatal("a retained cycle was not cross-checked")
	}
	if page.CycleFailedBatches != 80 || page.CycleStartedBatches != 80 {
		t.Errorf("cycle counters = failed %d started %d, want 80 and 80",
			page.CycleFailedBatches, page.CycleStartedBatches)
	}
	if page.CycleStatus != "failed" {
		t.Errorf("cycle status = %q, want failed", page.CycleStatus)
	}
	if got := page.CycleFailedPartitions["db_d/wide_table/2026/09/14/08"]; got != 19 {
		t.Errorf("cycle failed partitions = %v, want the partition with 19 batches", page.CycleFailedPartitions)
	}
	// The page shows 1 attempt; the cycle says 80 batches failed. That gap is
	// the whole point of carrying both.
	if page.Matched >= int(page.CycleFailedBatches) {
		t.Errorf("Matched %d vs cycle failed %d: the fixture no longer demonstrates the gap",
			page.Matched, page.CycleFailedBatches)
	}

	// An unretained cycle is reported as such, with the range.
	gone := manager.JobHistory(10, 9999)
	if gone.CycleFound {
		t.Error("an unretained cycle reported as found")
	}
	if !gone.HasCycleRange || gone.NewestRetainedCycle != 50 {
		t.Errorf("range = %d..%d (has=%v), want it to name 50 as newest",
			gone.OldestRetainedCycle, gone.NewestRetainedCycle, gone.HasCycleRange)
	}
}

// Truncated must be true when the ring is full and the read is cycle-filtered,
// even though every retained match fits on the page.
//
// This is the case that lies if Truncated is just Matched > len(Jobs): 320
// attempts happened, 100 survive, the operator asks for 100, gets 100, and
// without this would be told the page is complete while 220 are gone. It is
// decided in the manager precisely so this test can exist -- a handler-side
// computation passed a mutation that dropped exactly this half, because
// nothing outside this package can seed job history.
func TestFullRingFlagsTruncationEvenWhenThePageFitsIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	for i := 0; i < JobHistoryLimit+220; i++ {
		seedJob(manager, jobAttribution{CycleID: 50}, fmt.Sprintf("p%04d", i), false)
	}

	page := manager.JobHistory(JobHistoryLimit, 50)
	if page.Matched != JobHistoryLimit || len(page.Jobs) != JobHistoryLimit {
		t.Fatalf("Matched %d, page %d; want both %d (fixture must fill the ring exactly)",
			page.Matched, len(page.Jobs), JobHistoryLimit)
	}
	if page.Matched > len(page.Jobs) {
		t.Fatal("fixture no longer exercises the case: the page must hold every retained match")
	}
	if !page.Truncated {
		t.Error("a full, cycle-filtered ring reported the page as complete while 220 attempts were evicted")
	}

	// An unfiltered read of a full ring is NOT flagged: without a cycle filter
	// there is no claim about a particular cycle's completeness to get wrong.
	if unfiltered := manager.JobHistory(JobHistoryLimit, 0); unfiltered.Truncated {
		t.Error("an unfiltered full-ring read was flagged truncated; it makes no completeness claim")
	}
}

// And the ordinary case still works: more matched than the page holds.
func TestTruncatedWhenThePageIsShorterThanTheMatchIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	for i := 0; i < 25; i++ {
		seedJob(manager, jobAttribution{CycleID: 7}, fmt.Sprintf("p%02d", i), false)
	}

	page := manager.JobHistory(10, 7)
	if page.RingFull {
		t.Fatal("fixture must not fill the ring, or this asserts the other branch")
	}
	if !page.Truncated {
		t.Errorf("Matched %d with a page of %d was not flagged truncated", page.Matched, len(page.Jobs))
	}

	full := manager.JobHistory(100, 7)
	if full.Truncated {
		t.Errorf("a complete page on a non-full ring was flagged truncated (Matched %d, page %d)", full.Matched, len(full.Jobs))
	}
}

// A full ring must NOT flag truncation for a cycle that is entirely retained.
//
// The first version of the eviction term was just "the ring is full", which
// made the flag constant-true for every cycle-filtered read on any busy node
// -- a flag that never varies carries no information, which defeats the
// purpose it was introduced for. Cycles are serialized with monotonic ids, so
// the oldest retained cycle id decides it.
func TestFullRingDoesNotFlagAFullyRetainedCycleIssue1162(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	t.Cleanup(cleanup)

	// Cycle 1 fills the ring; cycle 2's five records evict five of cycle 1's.
	for i := 0; i < JobHistoryLimit; i++ {
		seedJob(manager, jobAttribution{CycleID: 1}, fmt.Sprintf("old%04d", i), false)
	}
	for i := 0; i < 5; i++ {
		seedJob(manager, jobAttribution{CycleID: 2}, fmt.Sprintf("new%04d", i), false)
	}

	recent := manager.JobHistory(10, 2)
	if !recent.RingFull {
		t.Fatal("fixture must fill the ring, or this asserts the wrong branch")
	}
	if recent.Matched != 5 || len(recent.Jobs) != 5 {
		t.Fatalf("cycle 2: Matched %d, page %d; want 5 and 5", recent.Matched, len(recent.Jobs))
	}
	if recent.Truncated {
		t.Error("a fully-retained cycle was flagged truncated on a full ring; the flag would be constant-true on any busy node")
	}
	if recent.MayHaveEvicted {
		t.Error("a fully-retained cycle was flagged as possibly evicted")
	}

	// Cycle 1 genuinely lost records, and must still be flagged.
	old := manager.JobHistory(JobHistoryLimit, 1)
	if !old.Truncated || !old.MayHaveEvicted {
		t.Errorf("cycle 1 lost records to eviction but was not flagged (truncated=%v evicted=%v)",
			old.Truncated, old.MayHaveEvicted)
	}
}

// The reconciliation rule that actually holds.
//
// NOT "depth-0 failures == failed_batches": recordFailure and failed.Add fire
// together and only when the whole adaptive ladder failed, so a batch rescued
// by splitting counts as succeeded and contributes no partition, while still
// leaving a depth-0 success=false record in job history. What is exact is the
// sum of the per-partition counts.
func TestFailedPartitionCountsSumToFailedBatchesIssue1162(t *testing.T) {
	const partitions = 7
	const batchesEach = 3

	progress := &cycleProgress{}
	var failed int
	for p := 0; p < partitions; p++ {
		for b := 0; b < batchesEach; b++ {
			// Mirrors the dispatch loop: the two happen together.
			failed++
			progress.recordFailure(fmt.Sprintf("db/m/2026/10/08/%02d", p))
		}
	}

	counts, truncated := progress.snapshotFailedPartitions()
	if truncated {
		t.Fatal("fixture is under the cap; truncation would void the invariant")
	}
	if len(counts) != partitions {
		t.Errorf("tracked %d distinct partitions, want %d (dedup by partition)", len(counts), partitions)
	}
	sum := 0
	for _, n := range counts {
		sum += n
	}
	if sum != failed {
		t.Errorf("sum(failed_partitions) = %d, want failed_batches = %d -- this is the one exact reconciliation rule", sum, failed)
	}
}

// Past the distinct-partition cap the sum no longer reconciles, and the flag
// is the only thing that says so.
func TestFailedPartitionsTruncationIsFlaggedIssue1162(t *testing.T) {
	progress := &cycleProgress{}
	for p := 0; p < cycleFailedPartitionLimit+40; p++ {
		progress.recordFailure(fmt.Sprintf("p%05d", p))
	}

	counts, truncated := progress.snapshotFailedPartitions()
	if len(counts) != cycleFailedPartitionLimit {
		t.Errorf("tracked %d partitions, want the cap of %d", len(counts), cycleFailedPartitionLimit)
	}
	if !truncated {
		t.Error("the cap was exceeded but truncation was not flagged; a capped list would read as complete")
	}

	// An already-tracked partition keeps counting past the cap: the cap bounds
	// distinct partitions, not failures per partition.
	before := counts["p00000"]
	progress.recordFailure("p00000")
	after, _ := progress.snapshotFailedPartitions()
	if after["p00000"] != before+1 {
		t.Errorf("an already-tracked partition stopped counting at the cap: %d then %d", before, after["p00000"])
	}
}
