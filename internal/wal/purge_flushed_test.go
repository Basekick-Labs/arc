package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newPurgeTestWriter(t *testing.T) *Writer {
	t.Helper()
	w, err := NewWriter(&WriterConfig{
		WALDir:     t.TempDir(),
		SyncMode:   SyncModeFdatasync,
		BufferSize: 1024,
		// Rotation is driven explicitly in these tests.
		MaxSizeBytes: 1 << 40,
		MaxAge:       time.Hour,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// appendAndSettle appends one tracked entry and waits for the writer loop to
// have written it, so the per-file accounting is observable.
func appendAndSettle(t *testing.T, w *Writer, records int) {
	t.Helper()
	rows := make([]map[string]interface{}, records)
	for i := range rows {
		rows[i] = map[string]interface{}{"m": "t", "time": time.Now().UTC().UnixMicro(), "v": i}
	}
	if _, err := w.AppendTracked(rows); err != nil {
		t.Fatalf("AppendTracked: %v", err)
	}
	waitForPurgeTestDrain(t, w)
}

func waitForPurgeTestDrain(t *testing.T, w *Writer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		drained := len(w.entryChan) == 0
		w.mu.Unlock()
		if drained {
			// One more beat for the in-flight entry to be accounted.
			time.Sleep(20 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("writer loop did not drain")
}

func walFileCount(t *testing.T, dir string) int {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	return len(m)
}

// TestPurgeFlushed_DeletesOnlyBelowTheFloor is the core of #1009: a rotated file
// goes away when every sequence it holds is flushed, and stays when any is not.
// No clock is involved.
func TestPurgeFlushed_DeletesOnlyBelowTheFloor(t *testing.T) {
	w := newPurgeTestWriter(t)

	appendAndSettle(t, w, 1) // seq 1 -> file A
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	appendAndSettle(t, w, 1) // seq 2 -> file B (active)

	w.mu.Lock()
	order := append([]string(nil), w.fileOrder...)
	stateA := *w.fileSeqs[order[0]]
	w.mu.Unlock()

	if len(order) != 2 {
		t.Fatalf("fileOrder = %v, want two files", order)
	}
	if stateA.maxSeq != 1 {
		t.Fatalf("file A maxSeq = %d, want 1", stateA.maxSeq)
	}

	// Floor 1: seq 1 is still unflushed, so A must stay.
	if deleted, err := w.PurgeFlushed(1); err != nil || deleted != 0 {
		t.Fatalf("PurgeFlushed(1) = (%d, %v), want (0, nil): seq 1 is unflushed", deleted, err)
	}

	// Floor 2: everything below 2 is flushed, so A goes and the active file stays.
	deleted, err := w.PurgeFlushed(2)
	if err != nil {
		t.Fatalf("PurgeFlushed(2): %v", err)
	}
	if deleted != 1 {
		t.Fatalf("PurgeFlushed(2) deleted %d, want 1", deleted)
	}
	if n := walFileCount(t, w.config.WALDir); n != 1 {
		t.Fatalf("%d WAL files remain, want 1 (the active file)", n)
	}
}

// TestPurgeFlushed_StopsAtTheFirstRetainedFile pins the rule that keeps #948
// closed, and it has to be set up deliberately.
//
// A flush checkpoint is appended like any other entry, so a checkpoint covering
// an earlier file's entries can live in a LATER file. Deleting the later file
// while the earlier one is retained destroys the only record that the earlier
// file's entries were flushed, and recovery replays them — permanent duplicate
// rows for a measurement without tags.
//
// The shape that tests this needs an older file whose bound is ABOVE the floor
// sitting in front of a newer file whose bound is below it. That is reachable in
// production precisely because sequences are assigned before entries are
// enqueued (a high sequence can be written to an older file while a low one
// lands in a newer), but it cannot be produced deterministically through the
// public API, so the recorded state is set directly. A naive first version of
// this test used a floor that retained the later file for its OWN reason and
// therefore passed even when the rule was replaced with "skip and keep going".
func TestPurgeFlushed_StopsAtTheFirstRetainedFile(t *testing.T) {
	w := newPurgeTestWriter(t)

	appendAndSettle(t, w, 1)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	appendAndSettle(t, w, 1)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	appendAndSettle(t, w, 1) // third file is active

	w.mu.Lock()
	order := append([]string(nil), w.fileOrder...)
	if len(order) == 3 {
		// Older file: a high sequence that is still unflushed.
		w.fileSeqs[order[0]].maxSeq = 7
		// Newer rotated file: every sequence in it is flushed, so on its own it
		// qualifies for deletion.
		w.fileSeqs[order[1]].maxSeq = 1
	}
	w.mu.Unlock()
	if len(order) != 3 {
		t.Fatalf("fileOrder = %v, want three files", order)
	}

	deleted, err := w.PurgeFlushed(2)
	if err != nil {
		t.Fatalf("PurgeFlushed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted %d files, want 0: the newer file qualifies on its own (maxSeq 1 < floor 2) but must be retained while the older file (maxSeq 7) is", deleted)
	}
	if _, err := os.Stat(order[1]); err != nil {
		t.Errorf("the newer rotated file was deleted ahead of the older retained one: %v", err)
	}

	// And once the older file's data is flushed, both go — oldest first.
	deleted, err = w.PurgeFlushed(8)
	if err != nil {
		t.Fatalf("PurgeFlushed: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted %d, want 2 once the floor clears both", deleted)
	}
}

// TestPurgeFlushed_NeverTouchesUntrackedOrForeignFiles: an untracked data entry
// is never reported flushed, and a file from a previous process has no
// accounting at all. Both must survive any floor.
func TestPurgeFlushed_NeverTouchesUntrackedOrForeignFiles(t *testing.T) {
	w := newPurgeTestWriter(t)

	// Untracked append: the path a replication follower uses.
	if err := w.AppendRaw([]byte{0x90}); err != nil {
		t.Fatalf("AppendRaw: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	appendAndSettle(t, w, 1)

	// A file this process did not create.
	foreign := filepath.Join(w.config.WALDir, "foreign-00000000.wal")
	if err := os.WriteFile(foreign, []byte("not ours"), 0600); err != nil {
		t.Fatal(err)
	}

	deleted, err := w.PurgeFlushed(^uint64(0))
	if err != nil {
		t.Fatalf("PurgeFlushed: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted %d files with an unbounded floor, want 0: the first rotated file holds an untracked entry", deleted)
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Errorf("a file from another process was removed: %v", err)
	}
}

// TestPurgeFlushed_IgnoresCheckpointOnlyFiles: checkpoints carry no sequence,
// but they must not make a file permanently unreclaimable the way an untracked
// DATA entry does — otherwise every file holds one and nothing is ever purged.
func TestPurgeFlushed_IgnoresCheckpointOnlyFiles(t *testing.T) {
	w := newPurgeTestWriter(t)

	appendAndSettle(t, w, 1)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	// A checkpoint lands in the second file, which holds no data at all.
	if err := w.MarkFlushed([]string{fmt.Sprintf("%016x%016x", w.trackedInstance, uint64(1))}); err != nil {
		t.Fatalf("MarkFlushed: %v", err)
	}
	waitForPurgeTestDrain(t, w)
	if err := w.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	appendAndSettle(t, w, 1)

	w.mu.Lock()
	second := w.fileSeqs[w.fileOrder[1]]
	checkpointOnly := second.maxSeq == 0 && !second.hasUntrackedData
	w.mu.Unlock()
	if !checkpointOnly {
		t.Fatalf("second file state = %+v, want no tracked data and no untracked data", second)
	}

	deleted, err := w.PurgeFlushed(2)
	if err != nil {
		t.Fatalf("PurgeFlushed: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted %d, want 2 (the data file and the checkpoint-only file)", deleted)
	}
}
