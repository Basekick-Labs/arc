package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func columnarPayload(t *testing.T, m string) []byte {
	t.Helper()
	raw, err := msgpack.Marshal(map[string]interface{}{
		"m":       m,
		"columns": map[string]interface{}{"time": []interface{}{int64(1700000000000000)}, "v": []interface{}{int64(1)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func appendTracked(t *testing.T, w *Writer, m string) []string {
	t.Helper()
	ids, err := w.AppendRawWithMetaTracked("db", columnarPayload(t, m))
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// #1225: a quarantined file of this process held an entry whose flush failed.
// Its sequence stayed pending for the life of the process, so the purge
// stopped at that file and never reclaimed another one.
func TestQuarantineReleasesTheFilesPendingEntries(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer w.Close()

	appendTracked(t, w, "lost") // its flush failed: pending
	p, err := w.PrepareCovered("db", "db/lost", columnarPayload(t, "lost"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Commit(1); err != nil { // a covered entry, pending too
		t.Fatal(err)
	}
	poisoned := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	flushed := appendTracked(t, w, "cpu")
	if err := w.MarkFlushed(flushed); err != nil {
		t.Fatal(err)
	}
	later := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if n, err := w.PurgeFlushed(w.MinUnflushedSequence()); err != nil || n != 0 {
		t.Fatalf("purge before quarantine = %d %v, want 0: the pending entry holds the floor", n, err)
	}

	releasedBefore := metrics.Get().Snapshot()["wal_quarantine_released_entries"].(int64)
	quarantinedBefore := metrics.Get().Snapshot()["wal_quarantined_files"].(int64)
	// Recovery quarantines the file after repeated failures.
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&RecoveryOptions{
			SkipActiveFile:    w.CurrentFile(),
			MaxReplayFailures: 1,
			ColumnarCallback: func(_ context.Context, _, m string, _ map[string][]interface{}, _ string) error {
				if m == "lost" {
					return errors.New("poison")
				}
				return errors.New("unexpected replay: the later entry is checkpointed")
			},
			OnQuarantine: func(path, as string) { w.Quarantined(path, as) },
		})
	if err != nil {
		t.Fatal(err)
	}
	if stats.QuarantinedFiles != 1 {
		t.Fatalf("stats %+v, want the poisoned file quarantined", stats)
	}
	if _, err := os.Stat(poisoned + ".failed"); err != nil {
		t.Fatalf("quarantined copy missing: %v", err)
	}
	if got := w.PendingUnflushedCount(); got != 0 {
		t.Fatalf("%d entries still pending after their file was quarantined", got)
	}
	if got := metrics.Get().Snapshot()["wal_quarantine_released_entries"].(int64) - releasedBefore; got != 2 {
		t.Fatalf("released metric moved by %d, want 2", got)
	}
	if got := metrics.Get().Snapshot()["wal_quarantined_files"].(int64) - quarantinedBefore; got != 1 {
		t.Fatalf("quarantined files metric moved by %d, want 1", got)
	}
	// Recovery replayed and deleted the later file itself. A file written
	// from now on must be reclaimable again.
	if _, err := os.Stat(later); !os.IsNotExist(err) {
		t.Fatalf("later file: %v", err)
	}
	next := appendTracked(t, w, "cpu")
	if err := w.MarkFlushed(next); err != nil {
		t.Fatal(err)
	}
	nextFile := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.PurgeFlushed(w.MinUnflushedSequence()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(nextFile); !os.IsNotExist(err) {
		t.Fatalf("a flushed file written after the quarantine was not reclaimed: %v", err)
	}
}

// Only the quarantined file's own sequences are released: sequences
// interleave across files, and a pending entry elsewhere still needs the WAL.
func TestQuarantineReleasesOnlyThatFilesSequences(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer w.Close()

	appendTracked(t, w, "a")
	first := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	appendTracked(t, w, "b") // pending in the second file
	second := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(first, first+".failed"); err != nil {
		t.Fatal(err)
	}
	if got := w.Quarantined(first, first+".failed"); got != 1 {
		t.Fatalf("released %d, want 1", got)
	}
	if got := w.PendingUnflushedCount(); got != 1 {
		t.Fatalf("pending %d, want the second file's entry still pending", got)
	}
	if n, _ := w.PurgeFlushed(w.MinUnflushedSequence()); n != 0 {
		t.Fatal("purged a file whose entry is still pending")
	}
	if _, err := os.Stat(second); err != nil {
		t.Fatal(err)
	}
	// A file another writer wrote releases nothing, even with a pending
	// sequence of the same number.
	other, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	appendTracked(t, other, "x")
	foreign := other.CurrentFile()
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if got := w.Quarantined(filepath.Join(dir, "other.wal"), foreign); got != 0 {
		t.Fatalf("released %d for a foreign file", got)
	}
	if got := w.PendingUnflushedCount(); got != 1 {
		t.Fatalf("pending %d after a foreign quarantine, want 1", got)
	}
}

// A lower pending sequence in an earlier file survives the quarantine of a
// later one.
func TestQuarantineKeepsALowerPendingSequenceElsewhere(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer w.Close()

	appendTracked(t, w, "a") // seq 1, pending, first file
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	appendTracked(t, w, "b") // seq 2, pending, second file
	second := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(second, second+".failed"); err != nil {
		t.Fatal(err)
	}
	if got := w.Quarantined(second, second+".failed"); got != 1 {
		t.Fatalf("released %d, want 1", got)
	}
	if got := w.MinUnflushedSequence(); got != 1 {
		t.Fatalf("floor %d, want the first file's entry still pending", got)
	}
}

// A quarantined own file is forgotten, not just released: a file holding an
// untracked entry stops the purge walk for as long as it is listed.
func TestQuarantineForgetsTheFile(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer w.Close()

	if err := w.AppendRawWithMeta("db", columnarPayload(t, "untracked")); err != nil {
		t.Fatal(err)
	}
	first := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	ids := appendTracked(t, w, "cpu")
	if err := w.MarkFlushed(ids); err != nil {
		t.Fatal(err)
	}
	second := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(first, first+".failed"); err != nil {
		t.Fatal(err)
	}
	w.Quarantined(first, first+".failed")
	if n, err := w.PurgeFlushed(w.MinUnflushedSequence()); err != nil || n != 1 {
		t.Fatalf("purge = %d %v, want the flushed second file reclaimed", n, err)
	}
	if _, err := os.Stat(second); !os.IsNotExist(err) {
		t.Fatalf("second file: %v", err)
	}
}

// A rename that worked but whose directory sync failed is still a
// quarantine: the file is out of the WAL directory, the writer is told, and
// the pass goes on to the next file.
func TestQuarantineWithAFailedDirectorySync(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer w.Close()

	appendTracked(t, w, "lost")
	poisoned := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	appendTracked(t, w, "next") // replayed by the same pass
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	orig := syncQuarantineDirFn
	syncQuarantineDirFn = func(string) error { return errors.New("sync failed") }
	t.Cleanup(func() { syncQuarantineDirFn = orig })

	var told []string
	var replayed []string
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&RecoveryOptions{
			SkipActiveFile:    w.CurrentFile(),
			MaxReplayFailures: 1,
			ColumnarCallback: func(_ context.Context, _, m string, _ map[string][]interface{}, _ string) error {
				replayed = append(replayed, m)
				if m == "lost" {
					return errors.New("poison")
				}
				return nil
			},
			OnQuarantine: func(path, as string) {
				told = append(told, path)
				w.Quarantined(path, as)
			},
		})
	if err != nil {
		t.Fatalf("a failed directory sync after the rename stopped the pass: %v", err)
	}
	if stats.QuarantinedFiles != 1 || len(told) != 1 || told[0] != poisoned {
		t.Fatalf("stats %+v, told %v", stats, told)
	}
	if len(replayed) != 2 || replayed[1] != "next" {
		t.Fatalf("replayed %v: the pass did not go on to the next file", replayed)
	}
	if _, err := os.Stat(poisoned + ".failed"); err != nil {
		t.Fatal(err)
	}
}

// A damaged entry's identity cannot be trusted (a flipped bit could name
// another file's pending entry), so it is not released.
func TestQuarantineDoesNotTrustADamagedEntry(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer w.Close()

	appendTracked(t, w, "lost")
	file := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff // damage the payload, not the identity
	if err := os.WriteFile(file+".failed", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if got := w.Quarantined(file, file+".failed"); got != 0 {
		t.Fatalf("released %d from a damaged entry", got)
	}
	if got := w.PendingUnflushedCount(); got != 1 {
		t.Fatalf("pending %d, want 1", got)
	}
}
