package cluster

// #1207 S4c: the outbox tells the WAL which covered batches are still waiting
// for their files to register.

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/rs/zerolog"
)

func holds(r *RegistrationOutbox, inst uint64, key string, lo, hi uint64) bool {
	return r.CoverageHold().HoldsAny(inst, key, lo, hi)
}

func TestCoverageHoldIntervals(t *testing.T) {
	h := coverageHold{}
	h.add(1, "k", 10, 12)
	h.add(1, "k", 13, 15) // adjacent: merged
	h.add(1, "k", 20, 20)
	h.add(2, "k", 5, 5)
	h.merge()
	if got := h[holdKey{1, "k"}]; len(got) != 2 || got[0] != [2]uint64{10, 15} {
		t.Fatalf("merged = %v", got)
	}
	for _, c := range []struct {
		inst   uint64
		key    string
		lo, hi uint64
		want   bool
	}{
		{1, "k", 1, 9, false}, {1, "k", 9, 10, true}, {1, "k", 15, 19, true}, {1, "k", 16, 19, false},
		{1, "k", 20, 20, true}, {1, "k", 21, 99, false}, {1, "other", 10, 10, false}, {2, "k", 5, 5, true}, {3, "k", 5, 5, false},
	} {
		if got := h.HoldsAny(c.inst, c.key, c.lo, c.hi); got != c.want {
			t.Errorf("HoldsAny(%d,%s,%d,%d) = %v, want %v", c.inst, c.key, c.lo, c.hi, got, c.want)
		}
	}
}

// Held while pending, across a restart (the journal), and released once the
// registration is acknowledged durably.
func TestCoverageHoldFollowsTheJournal(t *testing.T) {
	dir := t.TempDir()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	registerCovered(first, outPathA, 10, testCov(&settleLog{}))
	register(first, outPathB, 20) // uncovered: holds nothing
	if !holds(first, 9, "db/cpu", 5, 5) || holds(first, 9, "db/cpu", 8, 99) || holds(first, 9, "db/mem", 5, 5) {
		t.Fatal("pending coverage not held as journaled")
	}
	if err := first.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(first)

	r := newTestOutbox(t, dir)
	if !holds(r, 9, "db/cpu", 3, 3) || !r.HoldsCoverage() {
		t.Fatal("a loaded journal record must hold its batches: the next start registers it")
	}
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}, "writer-1")
	waitOutbox(t, "registration", func() bool { return r.Pending() == 0 })
	waitOutbox(t, "hold release", func() bool { return !holds(r, 9, "db/cpu", 3, 7) })
	if r.HoldsCoverage() {
		t.Fatal("nothing should be held once everything registered")
	}
}

// An acknowledgement that is not durable yet keeps the hold: a crash would
// re-register the file on the next start, and a refusal then needs the WAL.
func TestCoverageHoldWaitsForTheAckToBeDurable(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	registerCovered(r, outPathA, 10, testCov(&settleLog{}))
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	origSync, origRetry := fsyncFile, outboxBrokenRetry
	t.Cleanup(func() { fsyncFile, outboxBrokenRetry = origSync, origRetry })
	var syncCalls atomic.Int64
	fsyncFile = func(f *os.File) error {
		if syncCalls.Add(1) == 1 {
			return origSync(f)
		}
		return errors.New("disk gone")
	}
	outboxBrokenRetry = 0

	r.HandOff(newFakeOutboxManifest(), &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "registration", func() bool { return r.Pending() == 0 })
	if !holds(r, 9, "db/cpu", 3, 7) {
		t.Fatal("released on an ack that never reached the disk")
	}
	fsyncFile = origSync
	if err := r.SyncRegistrations(); err != nil { // rewrites the broken journal
		t.Fatal(err)
	}
	if holds(r, 9, "db/cpu", 3, 7) {
		t.Fatal("still held after the ack is durable")
	}
}

// Abandon keeps the hold (user decision, #1207 S4c): the journal keeps the
// records and the next start registers them, or refuses and re-flushes them
// from these entries.
func TestCoverageHoldSurvivesAbandon(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	log := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(log))
	if !r.Abandon() {
		t.Fatal("Abandon")
	}
	if got := log.wait(t, outPathA); got != ingest.CoverageAbandoned {
		t.Fatalf("settled %v", got)
	}
	if !holds(r, 9, "db/cpu", 3, 7) {
		t.Fatal("Abandon released the hold")
	}
}

// A rewrite carries the group's coverage, so the hold moves with it; a
// withdrawal (rows deleted on purpose) releases it once durable.
func TestCoverageHoldAcrossReplaceAndWithdraw(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	registerCovered(r, outPathA, 10, testCov(&settleLog{}))
	if outcome, _, err := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new"); err != nil || outcome != ReplaceSwapped {
		t.Fatalf("ReplacePending: %v %v", outcome, err)
	}
	if !holds(r, 9, "db/cpu", 3, 7) {
		t.Fatal("the rewrite dropped the hold")
	}
	if ok, _, err := r.Withdraw(context.Background(), outPathB); err != nil || !ok {
		t.Fatalf("Withdraw: %v %v", ok, err)
	}
	if holds(r, 9, "db/cpu", 3, 7) {
		t.Fatal("a withdrawn file still holds its batches")
	}
}

// A coverage the journal truncated holds its untruncated bounds after a
// restart; one truncated by a release without bounds holds nothing.
func TestCoverageHoldOfATruncatedRecord(t *testing.T) {
	dir := t.TempDir()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	cov := ingest.NewGroupCoverage("g1", "db/cpu", 1, nil)
	for i := uint64(0); i < 5000; i++ { // over the journal cap, never contiguous
		cov.Ranges = append(cov.Ranges, ingest.CoverageRange{Instance: 9, Lo: 2 * i, Hi: 2 * i})
	}
	registerCovered(first, outPathA, 10, cov)
	old := ingest.NewGroupCoverage("g2", "db/mem", 1, nil)
	old.Ranges = []ingest.CoverageRange{{Instance: 9, Lo: 1, Hi: 1}}
	old.RangesOmitted = 3 // a pre-S4c truncated copy: no bounds
	registerCovered(first, outPathB, 10, old)
	if err := first.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(first)

	r := newTestOutbox(t, dir)
	if !holds(r, 9, "db/cpu", 9997, 9997) {
		t.Fatal("a batch past the journal cap is not held: its bounds were lost")
	}
	if !holds(r, 9, "db/mem", 1, 1) {
		t.Fatal("the listed part of an old truncated copy is still held")
	}
	r.mu.Lock()
	unknown := r.unknownHoldsLocked()
	r.mu.Unlock()
	if unknown != 1 {
		t.Fatalf("unknown holds = %d, want 1", unknown)
	}
}

// A snapshot is cached between changes; every change to what is held must
// invalidate it: a new covered registration, and an acknowledgement.
func TestCoverageHoldSnapshotFollowsChanges(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	if r.HoldsCoverage() {
		t.Fatal("empty outbox holds")
	}
	registerCovered(r, outPathA, 10, testCov(&settleLog{}))
	if !holds(r, 9, "db/cpu", 3, 3) {
		t.Fatal("a registration after a snapshot is not held: the cached snapshot was reused")
	}
	r.HandOff(newFakeOutboxManifest(), &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "registration", func() bool { return r.Pending() == 0 })
	waitOutbox(t, "release", func() bool { return !holds(r, 9, "db/cpu", 3, 7) })
	r.mu.Lock()
	left := len(r.unsyncedAcks)
	r.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d durable acks still tracked", left)
	}
}
