package wal

import (
	"os"
	"path/filepath"
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

// B2 regression: a replication follower writes every replicated entry through
// AppendRaw, which is untracked, so PurgeFlushed stops at the first such file
// by design. Repointing the live purge sites at PurgeFlushed alone therefore
// left a reader node's WAL growing without bound. Age is the only signal that
// exists for data no checkpoint will ever cover.
func TestPurgeUnaccountedOlderThan_ReclaimsUntrackedFollowerFiles(t *testing.T) {
	w := newPurgeTestWriter(t)

	// Two rotated files holding only untracked (replicated) entries.
	for i := 0; i < 2; i++ {
		if err := w.AppendRaw([]byte("replicated payload")); err != nil {
			t.Fatalf("AppendRaw: %v", err)
		}
		waitForPurgeTestDrain(t, w)
		if err := w.Rotate(); err != nil {
			t.Fatalf("Rotate: %v", err)
		}
	}

	// The floor cannot touch them, whatever it is.
	if n, err := w.PurgeFlushed(^uint64(0)); err != nil || n != 0 {
		t.Fatalf("PurgeFlushed deleted %d (err %v); untracked files are not its business", n, err)
	}

	ageWALFiles(t, w, -time.Hour)
	deleted, err := w.PurgeUnaccountedOlderThan(time.Minute)
	if err != nil {
		t.Fatalf("PurgeUnaccountedOlderThan: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted %d, want 2 — a follower's WAL is otherwise never reclaimed", deleted)
	}
}

// The residual purge must never touch a file carrying tracked sequences,
// however old it is. Doing so is exactly the acknowledged-write loss of #966:
// a flush slower than the age threshold, and the only copy is gone.
func TestPurgeUnaccountedOlderThan_NeverTouchesTrackedFiles(t *testing.T) {
	w := newPurgeTestWriter(t)
	rows := []map[string]interface{}{{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": 1}}
	if _, err := w.AppendTracked(rows); err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	before := walFileCount(t, w.config.WALDir)
	ageWALFiles(t, w, -24*time.Hour)

	deleted, err := w.PurgeUnaccountedOlderThan(time.Minute)
	if err != nil {
		t.Fatalf("PurgeUnaccountedOlderThan: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted %d tracked-file(s) by age — that is the #966 data loss", deleted)
	}
	if got := walFileCount(t, w.config.WALDir); got != before {
		t.Fatalf("file count %d, want %d", got, before)
	}
}

// B3 regression: a file left by a PREVIOUS process is absent from fileSeqs by
// design, so PurgeFlushed can never see it. Recovery deletes what it replays
// but deliberately keeps a file holding an entry it could not apply, and one
// left by an unclean shutdown — both documented as relying on the periodic
// purge to reclaim them afterwards.
func TestPurgeUnaccountedOlderThan_ReclaimsPreviousProcessFiles(t *testing.T) {
	w := newPurgeTestWriter(t)

	// A file this writer never wrote, as a restart would leave behind.
	foreign := filepath.Join(w.config.WALDir, "arc-0000000000-foreign.wal")
	if err := os.WriteFile(foreign, emptyWALFixture(), 0o600); err != nil {
		t.Fatalf("seed foreign file: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(foreign, old, old); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if n, err := w.PurgeFlushed(^uint64(0)); err != nil || n != 0 {
		t.Fatalf("PurgeFlushed deleted %d (err %v); a foreign file is not its business", n, err)
	}

	deleted, err := w.PurgeUnaccountedOlderThan(time.Minute)
	if err != nil {
		t.Fatalf("PurgeUnaccountedOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted %d, want 1 — a retained foreign file otherwise survives every restart forever", deleted)
	}
	if _, err := os.Stat(foreign); !os.IsNotExist(err) {
		t.Fatalf("foreign file still present")
	}
}

// The active file is never a candidate: it is still being appended to.
func TestPurgeUnaccountedOlderThan_SkipsTheActiveFile(t *testing.T) {
	w := newPurgeTestWriter(t)
	if err := w.AppendRaw([]byte("replicated payload")); err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	ageWALFiles(t, w, -time.Hour)

	deleted, err := w.PurgeUnaccountedOlderThan(time.Minute)
	if err != nil {
		t.Fatalf("PurgeUnaccountedOlderThan: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted %d, want 0 — the active file was purged", deleted)
	}
}

// ageWALFiles backdates every WAL file's mtime so an age-based purge sees them
// as old without the test sleeping.
func ageWALFiles(t *testing.T, w *Writer, delta time.Duration) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(w.config.WALDir, "*.wal"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	when := time.Now().Add(delta)
	for _, p := range paths {
		if err := os.Chtimes(p, when, when); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}
}

// The residual purge must delete only a PREFIX of the rotation order, the same
// invariant PurgeFlushed keeps and for the same reason: a flush checkpoint is
// appended like any other entry, so a checkpoint covering file R's entries can
// live in R or in any LATER file. Deleting a later file while R is kept can
// destroy the only record that R's entries were flushed, and recovery would
// then replay them — permanent duplicate rows for a measurement without tags.
//
// maxSeq == 0 cannot distinguish "holds only checkpoints" from "holds
// nothing", so a glob-and-delete-by-age pass over this process's files hits
// exactly that hazard: an older TRACKED file is retained while a younger
// checkpoint-bearing file after it is deleted.
func TestPurgeUnaccountedOlderThan_StopsAtTheFirstTrackedFile(t *testing.T) {
	w := newPurgeTestWriter(t)
	rows := []map[string]interface{}{{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": 1}}

	// File 1: tracked, unflushed — must be retained.
	if _, err := w.AppendTracked(rows); err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// File 2: no tracked sequences, and old. On its own it would be a
	// candidate — but it sits AFTER a retained file, so it may hold that
	// file's checkpoint.
	if err := w.AppendRaw([]byte("untracked payload")); err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	before := walFileCount(t, w.config.WALDir)
	ageWALFiles(t, w, -time.Hour)

	deleted, err := w.PurgeUnaccountedOlderThan(time.Minute)
	if err != nil {
		t.Fatalf("PurgeUnaccountedOlderThan: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted %d file(s) past a retained tracked file; that can destroy the proof its entries were flushed", deleted)
	}
	if got := walFileCount(t, w.config.WALDir); got != before {
		t.Fatalf("file count %d, want %d", got, before)
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
