package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #676 part A: the writer tells the WAL maintenance loop when a purge could
// now delete something, so the WAL is reclaimed as it is flushed rather than
// once per periodic pass.

func drainPurgeHint(w *Writer) {
	select {
	case <-w.PurgeHint():
	default:
	}
}

func requirePurgeHint(t *testing.T, w *Writer, what string) {
	t.Helper()
	select {
	case <-w.PurgeHint():
	default:
		t.Fatalf("%s did not signal the purge hint", what)
	}
}

func requireNoPurgeHint(t *testing.T, w *Writer, what string) {
	t.Helper()
	select {
	case <-w.PurgeHint():
		t.Fatalf("%s signalled the purge hint", what)
	default:
	}
}

func TestPurgeHintOnRotationAndFloorRelease(t *testing.T) {
	t.Run("rotation", func(t *testing.T) {
		w := newPurgeTestWriter(t)
		requireNoPurgeHint(t, w, "the initial file")
		require.NoError(t, w.Rotate())
		requirePurgeHint(t, w, "a rotation")
	})
	t.Run("MarkFlushed", func(t *testing.T) {
		w := newPurgeTestWriter(t)
		tok := appendOne(t, w, 1)
		drainPurgeHint(w)
		require.NoError(t, w.MarkFlushed([]string{tok}))
		requirePurgeHint(t, w, "a flush checkpoint")
	})
	t.Run("ForgetTracked", func(t *testing.T) {
		w := newPurgeTestWriter(t)
		tok := appendOne(t, w, 1)
		drainPurgeHint(w)
		w.ForgetTracked([]string{tok})
		requirePurgeHint(t, w, "ForgetTracked")
		w.ForgetTracked([]string{tok})
		requireNoPurgeHint(t, w, "a release of nothing pending")
	})
	t.Run("ReleaseRegistered", func(t *testing.T) {
		w := newPurgeTestWriter(t)
		tok := appendOne(t, w, 1)
		require.NoError(t, w.MarkFlushedRetained([]string{tok}))
		drainPurgeHint(w)
		w.ReleaseRegistered([]string{tok})
		requirePurgeHint(t, w, "ReleaseRegistered")
	})
	t.Run("Abandon", func(t *testing.T) {
		w := newPurgeTestWriter(t)
		p, err := w.PrepareCovered("db", "k", []byte{0x90})
		require.NoError(t, err)
		drainPurgeHint(w)
		p.Abandon()
		requirePurgeHint(t, w, "an abandoned covered append")
	})
	t.Run("Quarantined", func(t *testing.T) {
		w := newPurgeTestWriter(t)
		appendOne(t, w, 1)
		path := w.CurrentFile()
		require.NoError(t, w.Rotate())
		require.NoError(t, os.Rename(path, path+".failed"))
		drainPurgeHint(w)
		w.Quarantined(path, path+".failed")
		requirePurgeHint(t, w, "a quarantined own file")
	})
	t.Run("ReleaseRetained", func(t *testing.T) {
		dir := t.TempDir()
		earlier := filepath.Join(dir, "arc-19700101_000000.000000000.wal")
		require.NoError(t, os.WriteFile(earlier, []byte("ARCW"), 0600))
		w := pinTestWriter(t, dir)
		w.AdoptRetained([]RetainedFile{{Path: earlier}})
		drainPurgeHint(w)
		deleted, err := w.ReleaseRetained()
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
		requirePurgeHint(t, w, "a released retained file")
		_, err = w.ReleaseRetained()
		require.NoError(t, err)
		requireNoPurgeHint(t, w, "a ReleaseRetained that deleted nothing")
	})
}

// Hints coalesce and never block a sender, so the writer loop (which signals
// under w.mu) cannot stall on a maintenance loop that is busy.
func TestPurgeHintCoalesces(t *testing.T) {
	w := newPurgeTestWriter(t)
	for i := 0; i < 5; i++ {
		require.NoError(t, w.Rotate())
	}
	requirePurgeHint(t, w, "rotations")
	requireNoPurgeHint(t, w, "a second read of coalesced hints")
}

// The sampled purge log keeps the running total.
func TestPurgeFlushedCountsEveryDeletedFile(t *testing.T) {
	w := newPurgeTestWriter(t)
	w.logger = zerolog.Nop()
	for i := 0; i < 3; i++ {
		tok := appendOne(t, w, i)
		require.NoError(t, w.MarkFlushed([]string{tok}))
		require.NoError(t, w.Rotate())
		_, err := w.PurgeFlushed(w.MinUnflushedSequence())
		require.NoError(t, err)
	}
	require.EqualValues(t, 3, w.purgedTotal.Load())
}

// A tracked append dropped on a full queue releases its sequence, which can
// raise the floor.
func TestPurgeHintOnADroppedTrackedAppend(t *testing.T) {
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeAsync, BufferSize: 1, Logger: zerolog.Nop()})
	require.NoError(t, err)
	// Stop the drain so the queue stays full (see wal_dropped_test.go).
	close(w.done)
	w.wg.Wait()
	t.Cleanup(func() { _ = w.currentFile.Close() })
	w.entryChan <- walEntry{data: []byte{0xff}}
	drainPurgeHint(w)
	_, err = w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.True(t, errors.Is(err, ErrWALDropped), "got %v", err)
	requirePurgeHint(t, w, "a dropped tracked append")
}
