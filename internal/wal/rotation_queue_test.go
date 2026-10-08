package wal

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Pause only the background drain, as in the deterministic append-drop tests,
// keeping a real open WAL file and real queued, encoded entries. Restarting the
// loop lets the same test verify ordering and file contents after admission.
func pausedRotationWriter(t *testing.T) (*Writer, func()) {
	t.Helper()
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeFsync,
		BufferSize: 2, MaxSizeBytes: 100 * 1024 * 1024, Logger: zerolog.Nop()})
	require.NoError(t, err)
	close(w.done)
	w.wg.Wait()
	w.done = make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { w.wg.Add(1); go w.writerLoop() }) }
	t.Cleanup(func() { resume(); require.NoError(t, w.Close()) })
	return w, resume
}

func testRotationWaitsForFullQueue(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	w, resume := pausedRotationWriter(t)
	first, err := w.AppendTracked(rows)
	require.NoError(t, err)
	second, err := w.AppendTracked([]map[string]interface{}{{"index": "queued-second"}})
	require.NoError(t, err)
	require.Equal(t, cap(w.entryChan), len(w.entryChan))
	oldPath := w.CurrentFile()
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); result <- w.Rotate() }()
	<-started
	select {
	case err := <-result:
		t.Fatalf("rotation returned before full queue could drain: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	resume()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("rotation never resumed after queue drained")
	}
	require.Zero(t, atomic.LoadInt64(&w.DroppedEntries), "maintenance rotation is not dropped data")
	require.NotEqual(t, oldPath, w.CurrentFile())
	entries, err := NewReader(oldPath, zerolog.Nop()).ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, first[0], entries[0].PayloadHash)
	require.Equal(t, second[0], entries[1].PayloadHash)
	require.Len(t, entries[0].Records, len(rows))
	require.Equal(t, 2, w.PendingUnflushedCount(), "rotation must not mark data flushed")
	newPath := w.CurrentFile()
	third, err := w.AppendTracked([]map[string]interface{}{{"index": "after-rotation"}})
	require.NoError(t, err)
	require.NoError(t, w.Rotate())
	entries, err = NewReader(newPath, zerolog.Nop()).ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, third[0], entries[0].PayloadHash)
}

func TestRotationWaitsForFullQueue(t *testing.T) {
	testRotationWaitsForFullQueue(t, []map[string]interface{}{{"index": "queued-first"}})
}
