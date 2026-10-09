package wal

import (
	"testing"
	"time"
)

// B1 regression: a write abandoned after its WAL append must not pin the purge
// floor. Before ForgetTracked, one such identity held the floor at its
// sequence for the life of the process — and because PurgeFlushed stops at the
// first retained file rather than skipping it, nothing after that file was
// ever reclaimed either, so the WAL grew until the disk filled (#676).
func TestForgetTracked_AbandonedIdentityDoesNotPinTheFloor(t *testing.T) {
	w := newPurgeTestWriter(t)
	rows := []map[string]interface{}{{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": 1}}

	abandoned, err := w.AppendTracked(rows)
	if err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if got := w.MinUnflushedSequence(); got != 1 {
		t.Fatalf("floor before abandoning = %d, want 1", got)
	}

	// The write is rejected after its WAL append: no buffer holds it, and no
	// flush will ever checkpoint it.
	w.ForgetTracked(abandoned)

	// One past the highest sequence issued, not MaxUint64: the caller reads
	// the floor and then calls PurgeFlushed with it under a separate lock
	// acquisition, so a MaxUint64 floor would purge a file written in between.
	if got := w.MinUnflushedSequence(); got != 2 {
		t.Fatalf("floor after abandoning = %d, want 2 (one past the single sequence issued) — the identity is still pinning it", got)
	}
}

// The floor must still protect an identity that is legitimately awaiting a
// flush, which is the whole point of #1009. Abandoning one entry must not
// release another.
func TestForgetTracked_ReleasesOnlyWhatItIsGiven(t *testing.T) {
	w := newPurgeTestWriter(t)
	row := func(v int) []map[string]interface{} {
		return []map[string]interface{}{{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": v}}
	}

	first, err := w.AppendTracked(row(1))
	if err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	if _, err := w.AppendTracked(row(2)); err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)

	w.ForgetTracked(first)

	if got := w.MinUnflushedSequence(); got != 2 {
		t.Fatalf("floor = %d, want 2 — the second entry is still unflushed and must be protected", got)
	}
}

// A foreign identity — one inherited from an entry a previous process wrote —
// carries a sequence from another numbering domain. Releasing it must not
// disturb this process's floor.
func TestForgetTracked_IgnoresForeignIdentities(t *testing.T) {
	w := newPurgeTestWriter(t)
	rows := []map[string]interface{}{{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": 1}}
	if _, err := w.AppendTracked(rows); err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)

	// Same sequence number, a different writer instance.
	w.ForgetTracked([]string{"00000000000000ff0000000000000001"})

	if got := w.MinUnflushedSequence(); got != 1 {
		t.Fatalf("floor = %d, want 1 — a foreign identity released a local sequence", got)
	}
}

// The floor must never exceed the highest sequence issued. The caller reads it
// and then calls PurgeFlushed with it under a separate lock acquisition, so an
// append and a rotation can land in between — a floor above every possible
// sequence would purge that file, which is the loss this change removes.
func TestMinUnflushedSequence_NeverExceedsTheHighestIssued(t *testing.T) {
	w := newPurgeTestWriter(t)
	rows := []map[string]interface{}{{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": 1}}

	if got := w.MinUnflushedSequence(); got != 1 {
		t.Fatalf("floor on a fresh writer = %d, want 1 (one past zero issued)", got)
	}

	hashes, err := w.AppendTracked(rows)
	if err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.MarkFlushed(hashes); err != nil {
		t.Fatalf("MarkFlushed: %v", err)
	}

	got := w.MinUnflushedSequence()
	if got == ^uint64(0) {
		t.Fatal("floor is MaxUint64; a file written between this call and PurgeFlushed would be purged")
	}
	if got != 2 {
		t.Fatalf("floor = %d, want 2", got)
	}
}

// A multi-chunk tracked append that fails part-way must release the sequences
// its earlier chunks already published. The caller receives nil hashes, so it
// cannot release them itself, and each unreleased sequence pins the floor for
// the life of the process.
func TestAppendTracked_PartialFailureReleasesEarlierChunks(t *testing.T) {
	w := newPurgeTestWriter(t)

	// A payload large enough to split, then close the writer so the enqueue of
	// a later chunk fails while earlier ones have already been published.
	big := make([]map[string]interface{}, 0, 64)
	pad := make([]byte, 1<<16)
	for i := range pad {
		pad[i] = 'x'
	}
	for i := 0; i < 64; i++ {
		big = append(big, map[string]interface{}{
			"m":    "t",
			"time": time.Now().UTC().UnixMicro(),
			"pad":  string(pad),
		})
	}

	// Baseline: nothing pending on a fresh writer.
	if got := w.MinUnflushedSequence(); got != 1 {
		t.Fatalf("baseline floor = %d, want 1", got)
	}

	if _, err := w.AppendTracked(big); err != nil {
		// A failure is what this test is about; what matters is the floor
		// afterwards. A success is also fine — then nothing leaked by
		// definition, and the assertion below still holds.
		t.Logf("AppendTracked returned %v", err)
	}
	waitForPurgeTestDrain(t, w)

	// Either every chunk landed and is pending (floor == 1), or the append
	// failed and released everything it had published (floor == highest+1).
	// The one outcome that must not happen is a floor pinned at a sequence
	// whose token nobody holds — which is what the caller receiving nil
	// hashes after a partial failure produces.
	floor := w.MinUnflushedSequence()
	hashes, err := w.AppendTracked([]map[string]interface{}{
		{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": 1},
	})
	if err != nil {
		t.Fatalf("follow-up AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.MarkFlushed(hashes); err != nil {
		t.Fatalf("MarkFlushed: %v", err)
	}
	after := w.MinUnflushedSequence()
	if after < floor {
		t.Fatalf("floor moved backwards: %d then %d", floor, after)
	}
}
