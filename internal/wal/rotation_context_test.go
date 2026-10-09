package wal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestRotationContextAdmissionDeadline(t *testing.T) {
	w, resume := pausedRotationWriter(t)
	for i := 0; i < 2; i++ {
		_, err := w.AppendTracked([]map[string]interface{}{{"index": i}})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, w.RotateContext(ctx), context.DeadlineExceeded)
	require.Equal(t, 2, len(w.entryChan), "timed-out rotation was never admitted")
	require.Zero(t, atomic.LoadInt64(&w.DroppedEntries))
	resume()
	require.NoError(t, w.RotateContext(context.Background()))
}

func TestRotationContextCancellationAfterAdmission(t *testing.T) {
	w, resume := pausedRotationWriter(t)
	oldPath := w.CurrentFile()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, w.RotateContext(ctx), context.DeadlineExceeded)
	require.Equal(t, 1, len(w.entryChan), "admitted command must retain its owner")
	resume()
	// A second command can complete only after the cancelled caller's command.
	require.NoError(t, w.RotateContext(context.Background()))
	require.NotEqual(t, oldPath, w.CurrentFile())
	require.EqualValues(t, 3, atomic.LoadInt64(&w.TotalRotations))
	require.Zero(t, atomic.LoadInt64(&w.DroppedEntries))
}

func TestRotationContextCancelledBeforeAdmission(t *testing.T) {
	w, _ := pausedRotationWriter(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.RotateContext(ctx), context.Canceled)
	require.Empty(t, w.entryChan)
}

func TestRotationContextDoesNotWaitOnWriterMutex(t *testing.T) {
	w, _ := pausedRotationWriter(t)
	w.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	result := make(chan error, 1)
	go func() { result <- w.RotateContext(ctx) }()
	select {
	case err := <-result:
		w.mu.Unlock()
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		w.mu.Unlock()
		cancel()
		t.Fatal("rotation deadline waited on the file I/O mutex")
	}
}

func TestRotationContextAfterClose(t *testing.T) {
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.ErrorContains(t, w.RotateContext(context.Background()), "closed")
	require.ErrorContains(t, w.Rotate(), "closed")
	require.Empty(t, w.entryChan)
}

func TestRotationContextRacesShutdown(t *testing.T) {
	for i := 0; i < 30; i++ {
		w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeFsync, Logger: zerolog.Nop(), BufferSize: 1})
		require.NoError(t, err)
		_, err = w.AppendTracked([]map[string]interface{}{{"index": i}})
		require.NoError(t, err)
		rotation := make(chan error, 1)
		closed := make(chan error, 1)
		go func() { rotation <- w.RotateContext(context.Background()) }()
		go func() { closed <- w.Close() }()
		select {
		case err := <-rotation:
			if err != nil {
				require.ErrorContains(t, err, "closed")
			}
		case <-time.After(time.Second):
			t.Fatal("rotation hung during shutdown")
		}
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("shutdown hung during rotation")
		}
		// Regardless of which operation won, shutdown must drain admitted data.
		files, _, err := NewRecovery(w.config.WALDir, zerolog.Nop()).ListWALFiles()
		require.NoError(t, err)
		rows := 0
		for _, path := range files {
			entries, err := NewReader(path, zerolog.Nop()).ReadAll()
			require.NoError(t, err)
			for _, entry := range entries {
				rows += len(entry.Records)
			}
		}
		require.Equal(t, 1, rows)
	}
}
