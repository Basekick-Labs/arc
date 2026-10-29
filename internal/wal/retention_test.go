package wal

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// fakeHold holds individual coverage sequences; safe to change mid-test.
type fakeHold struct {
	mu   sync.Mutex
	held map[CoverageSpan]bool // Lo == Hi
}

func newFakeHold() *fakeHold { return &fakeHold{held: map[CoverageSpan]bool{}} }

func (h *fakeHold) set(instance uint64, key string, cseq uint64, on bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	k := CoverageSpan{Instance: instance, Key: key, Lo: cseq, Hi: cseq}
	if on {
		h.held[k] = true
	} else {
		delete(h.held, k)
	}
}

func (h *fakeHold) HoldsAny(instance uint64, key string, lo, hi uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k := range h.held {
		if k.Instance == instance && k.Key == key && k.Lo >= lo && k.Lo <= hi {
			return true
		}
	}
	return false
}

func (h *fakeHold) source() func() CoverageHold { return func() CoverageHold { return h } }

// commitCovered appends one covered batch to the writer's current file and
// returns its tokens.
func commitCovered(t *testing.T, w *Writer, key string, cseq uint64) []string {
	t.Helper()
	p, err := w.PrepareCovered("db1", key, coveredTestPayload(t, 2))
	require.NoError(t, err)
	tokens, err := p.Commit(cseq)
	require.NoError(t, err)
	return tokens
}

func TestCoverageSetMergesAdjacentSequences(t *testing.T) {
	s := coverageSet{}
	for _, c := range []uint64{5, 3, 4, 9, 7, 8, 1} {
		s.add(1, "k", c)
	}
	s.add(2, "k", 4)
	require.Equal(t, []CoverageSpan{
		{Instance: 1, Key: "k", Lo: 1, Hi: 1},
		{Instance: 1, Key: "k", Lo: 3, Hi: 5},
		{Instance: 1, Key: "k", Lo: 7, Hi: 9},
		{Instance: 2, Key: "k", Lo: 4, Hi: 4},
	}, s.spans())
	s.add(1, "k", 6)
	s.add(1, "k", 2)
	require.Equal(t, []CoverageSpan{{Instance: 1, Key: "k", Lo: 1, Hi: 9}, {Instance: 2, Key: "k", Lo: 4, Hi: 4}}, s.spans())
}

// A file of this process holding a batch whose file is not registered stays,
// and so does every later file, even when the floor says flushed: recovery
// checkpoints and an abandoned outbox release the floor, only registration
// releases the hold (#1207 S4c).
func TestPurgeFlushedStopsAtAHeldOwnFile(t *testing.T) {
	dir := t.TempDir()
	w := pinTestWriter(t, dir)
	hold := newFakeHold()
	w.SetCoverageHold(hold.source())

	tokens := commitCovered(t, w, "db1/cpu", 1)
	held := w.CurrentFile()
	require.NoError(t, w.MarkFlushed(tokens)) // the floor is released
	hold.set(w.TrackedInstance(), "db1/cpu", 1, true)
	ingestAndFlush(t, w)
	later := w.CurrentFile()
	require.NoError(t, w.Rotate())

	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.FileExists(t, held)
	require.FileExists(t, later)

	hold.set(w.TrackedInstance(), "db1/cpu", 1, false)
	deleted, err = w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
}

// writeCoveredFiles writes one file per batch with an earlier process's
// writer, checkpointing the batches listed in flushed, and returns the paths
// and the instance.
func writeCoveredFiles(t *testing.T, dir string, n int, flushed map[int]bool) ([]string, uint64) {
	t.Helper()
	w := crashRecoveryWriter(t, dir)
	var files []string
	for i := 0; i < n; i++ {
		if i > 0 {
			require.NoError(t, w.Rotate())
		}
		tokens := commitCovered(t, w, "db1/cpu", uint64(i+1))
		if flushed[i] {
			require.NoError(t, w.MarkFlushed(tokens))
		}
		files = append(files, w.CurrentFile())
	}
	for i := 1; i < len(files); i++ {
		require.NotEqual(t, files[i-1], files[i])
	}
	inst := w.TrackedInstance()
	require.NoError(t, w.Close())
	return files, inst
}

func recoverWith(t *testing.T, dir string, w *Writer, hold *fakeHold, barrier bool) *RecoveryStats {
	t.Helper()
	opts := &RecoveryOptions{
		SkipActiveFile:   w.CurrentFile(),
		ColumnarCallback: func(context.Context, string, string, map[string][]interface{}, string) error { return nil },
		Held:             hold.source(),
	}
	if barrier {
		opts.BeforeDelete = func(context.Context) error { return nil }
	}
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, opts)
	require.NoError(t, err)
	return stats
}

// F1: a batch flushed before the crash (checkpointed) whose file never
// registered. Recovery skips its entry, and used to delete the file.
func TestRecoveryRetainsAFileWhoseCheckpointedBatchIsHeld(t *testing.T) {
	for _, barrier := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_barrier", true: "barrier"}[barrier], func(t *testing.T) {
			dir := t.TempDir()
			files, inst := writeCoveredFiles(t, dir, 1, map[int]bool{0: true})
			w := pinTestWriter(t, dir)
			hold := newFakeHold()
			hold.set(inst, "db1/cpu", 1, true)

			stats := recoverWith(t, dir, w, hold, barrier)
			require.FileExists(t, files[0])
			require.Equal(t, []RetainedFile{{Path: files[0], Coverage: []CoverageSpan{{Instance: inst, Key: "db1/cpu", Lo: 1, Hi: 1}}}}, stats.Retained)
			require.Zero(t, stats.KeptFiles, "retained is not a failure")

			hold.set(inst, "db1/cpu", 1, false)
			stats = recoverWith(t, dir, w, hold, barrier)
			require.Empty(t, stats.Retained)
			require.NoFileExists(t, files[0])
		})
	}
}

// A batch replayed now is held from the barrier on: its file was just
// journaled and has not registered.
func TestRecoveryRetainsAFileWhoseReplayedBatchIsHeld(t *testing.T) {
	dir := t.TempDir()
	files, inst := writeCoveredFiles(t, dir, 1, nil)
	w := pinTestWriter(t, dir)
	hold := newFakeHold()
	opts := &RecoveryOptions{
		SkipActiveFile: w.CurrentFile(),
		ColumnarCallback: func(ctx context.Context, _, _ string, _ map[string][]interface{}, _ string) error {
			rc, ok := ReplayCoverageFrom(ctx)
			require.True(t, ok)
			hold.set(rc.Coverage.Instance, rc.Coverage.Key, rc.Coverage.CSeq, true) // journaled by the flush
			return nil
		},
		BeforeDelete: func(context.Context) error { return nil },
		Held:         hold.source(),
	}
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, opts)
	require.NoError(t, err)
	require.FileExists(t, files[0])
	require.Len(t, stats.Retained, 1)
	require.Equal(t, inst, stats.Retained[0].Coverage[0].Instance)
}

// Once a file is retained, no later file of the pass is deleted, whether it
// is in the same barrier batch or a later one: its checkpoints can be the
// only proof for the retained file's entries.
func TestRecoveryKeepsEveryFileAfterARetainedOne(t *testing.T) {
	for _, barrier := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_barrier", true: "barrier"}[barrier], func(t *testing.T) {
			dir := t.TempDir()
			files, inst := writeCoveredFiles(t, dir, 3, map[int]bool{0: true, 1: true, 2: true})
			w := pinTestWriter(t, dir)
			hold := newFakeHold()
			hold.set(inst, "db1/cpu", 2, true) // the middle file

			stats := recoverWith(t, dir, w, hold, barrier)
			require.NoFileExists(t, files[0], "a file before the retained one is deleted")
			require.FileExists(t, files[1])
			require.FileExists(t, files[2], "kept for its position")
			require.Len(t, stats.Retained, 2)
			require.Equal(t, files[2], stats.Retained[1].Path)
		})
	}
}

// ReleaseRetained deletes adopted files in name order once they are not held,
// and stops at a held one or at an earlier-process file it was never given.
func TestReleaseRetainedDeletesTheReleasedPrefix(t *testing.T) {
	dir := t.TempDir()
	files, inst := writeCoveredFiles(t, dir, 3, map[int]bool{0: true, 1: true, 2: true})
	w := pinTestWriter(t, dir)
	hold := newFakeHold()
	w.SetCoverageHold(hold.source())
	hold.set(inst, "db1/cpu", 1, true)
	stats := recoverWith(t, dir, w, hold, false)
	require.Len(t, stats.Retained, 3)
	w.AdoptRetained(stats.Retained)
	w.AdoptRetained([]RetainedFile{{Path: w.CurrentFile()}}) // own files are never adopted
	require.Equal(t, 3, w.RetainedFiles())

	deleted, err := w.ReleaseRetained()
	require.NoError(t, err)
	require.Zero(t, deleted, "the first file is held")

	hold.set(inst, "db1/cpu", 1, false)
	hold.set(inst, "db1/cpu", 3, true)
	deleted, err = w.ReleaseRetained()
	require.NoError(t, err)
	require.Equal(t, 2, deleted, "the first two are free, the third held")
	require.NoFileExists(t, files[0])
	require.NoFileExists(t, files[1])
	require.FileExists(t, files[2])

	hold.set(inst, "db1/cpu", 3, false)
	deleted, err = w.ReleaseRetained()
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	require.Zero(t, w.RetainedFiles())
	require.FileExists(t, w.CurrentFile())
}

func TestReleaseRetainedStopsAtAnUnknownEarlierFile(t *testing.T) {
	dir := t.TempDir()
	files, _ := writeCoveredFiles(t, dir, 2, map[int]bool{0: true, 1: true})
	w := pinTestWriter(t, dir)
	// Only the second was given; the first is an earlier-process file
	// recovery kept for a failure and will process again.
	w.AdoptRetained([]RetainedFile{{Path: files[1]}})
	deleted, err := w.ReleaseRetained()
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.FileExists(t, files[1])

	// Recovery removes the first on its own: the second goes.
	require.NoError(t, os.Remove(files[0]))
	deleted, err = w.ReleaseRetained()
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
}

// Interaction with #1222, not AdoptRetained itself: an adopted file is an
// earlier-process file, so the checkpoints this process wrote for it stay.
func TestAdoptedRetainedFileKeepsItsProof(t *testing.T) {
	dir := t.TempDir()
	files, _ := writeCoveredFiles(t, dir, 1, nil)
	w := pinTestWriter(t, dir)
	w.AdoptRetained([]RetainedFile{{Path: files[0]}})
	require.NoError(t, w.MarkFlushed([]string{"ffffffffffffffff0000000000000007"}))
	proof := w.CurrentFile()
	require.NoError(t, w.Rotate())
	_, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.FileExists(t, proof)
	require.Equal(t, filepath.Dir(proof), dir)
}

// Two rotations in one clock tick, or a new writer just after a restart,
// must never append to the same file: a file another process wrote would be
// taken for this one's (and purged by its rules).
func TestRotateNeverReusesAFileName(t *testing.T) {
	frozen := walNow()
	orig := walNow
	walNow = func() time.Time { return frozen }
	t.Cleanup(func() { walNow = orig })
	dir := t.TempDir()
	first := pinTestWriter(t, dir)
	second := pinTestWriter(t, dir)
	require.NoError(t, second.Rotate())
	paths := map[string]bool{first.CurrentFile(): true}
	require.False(t, paths[second.CurrentFile()])
	require.Len(t, walFiles(t, dir), 3)
	require.Less(t, first.CurrentFile(), second.CurrentFile(), "a clash moves the name later: name order stays rotation order")
}
