package wal

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func newTimeoutWriter(t *testing.T, bufferSize int, ackTimeout time.Duration) *Writer {
	t.Helper()
	w, err := NewWriter(&WriterConfig{
		WALDir: t.TempDir(), SyncMode: SyncModeAsync, MaxSizeBytes: 64 << 20, MaxAge: time.Hour,
		SyncInterval: time.Hour, SyncBytes: 1 << 30, BufferSize: bufferSize,
		AckMode: AckModeDurable, AckTimeout: ackTimeout, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	return w
}

// stallWriterLoop parks the writer loop on its first data write until release
// is closed. Installed after NewWriter, which writes the file header.
func stallWriterLoop(t *testing.T) (release chan struct{}, held *atomic.Bool) {
	t.Helper()
	release = make(chan struct{})
	held = &atomic.Bool{}
	prevWrite := writeFileFn
	writeFileFn = func(f *os.File, b []byte) (int, error) {
		if held.CompareAndSwap(false, true) {
			<-release
		}
		return prevWrite(f, b)
	}
	t.Cleanup(func() { writeFileFn = prevWrite })
	return release, held
}

func awaitResult(t *testing.T, ch chan appendResult) appendResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("append never returned")
		return appendResult{}
	}
}

// #1501: every durable append parked on a stalled device is refused within
// AckTimeout, whether it waits for its sync (the queue has room) or for
// admission (the queue is full); none keeps its sequence pinned, and once
// the device recovers the retractions cover whatever reached the file, so a
// recovery pass replays nothing the clients were refused.
func TestDurableAckTimesOutOnAStalledDevice(t *testing.T) {
	for name, bufferSize := range map[string]int{"queue has room (reply path)": 16, "queue full (admission path)": 1} {
		t.Run(name, func(t *testing.T) {
			w := newTimeoutWriter(t, bufferSize, 200*time.Millisecond)
			release, held := stallWriterLoop(t)
			released := false
			defer func() {
				if !released {
					close(release)
				}
				_ = w.Close()
			}()
			payload := durablePayload(t)

			first := appendAsync(w, payload) // the loop takes it and parks in the device write
			for !held.Load() {
				time.Sleep(time.Millisecond)
			}
			var second chan appendResult
			if bufferSize == 1 {
				second = appendAsync(w, payload) // fills the one slot
				for len(w.entryChan) < 1 {
					time.Sleep(time.Millisecond)
				}
			}
			start := time.Now()
			_, err := w.AppendRawWithMetaTracked("iot", payload)
			elapsed := time.Since(start)
			require.ErrorIs(t, err, ErrNotDurable)
			require.ErrorIs(t, err, errAckTimeout)
			require.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
			require.Less(t, elapsed, 2*time.Second, "the refusal must not wait on the device")

			// The device recovers. The parked appends timed out the same way
			// (their retractions were waiting for admission to the full
			// queue; had the device stayed stalled past retractTimeout they
			// would have been released without one and replayed, the
			// at-least-once case) and now their retractions land.
			released = true
			close(release)
			require.ErrorIs(t, awaitResult(t, first).err, errAckTimeout)
			want := int64(2)
			if second != nil {
				require.ErrorIs(t, awaitResult(t, second).err, errAckTimeout)
				want = 3
			}
			require.Equal(t, want, atomic.LoadInt64(&w.DurableAckTimeouts))
			require.Equal(t, int64(0), atomic.LoadInt64(&w.RetractionsFailed))
			require.Equal(t, 0, w.PendingUnflushedCount(), "a timed-out entry must not pin the floor")
			deadline := time.Now().Add(5 * time.Second)
			for len(w.entryChan) > 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			require.NoError(t, w.Rotate())
			replayed := 0
			_, err = NewRecovery(w.config.WALDir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
				SkipActiveFile: w.CurrentFile(),
				TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
					replayed++
					return nil
				},
			})
			require.NoError(t, err)
			require.Equal(t, 0, replayed, "a write the client was refused must not be replayed")
		})
	}
}

// AckTimeout negative: no bound, the wait ends with the sync, as before.
func TestDurableAckUnboundedWhenConfigured(t *testing.T) {
	w := newTimeoutWriter(t, 16, -1)
	release, held := stallWriterLoop(t)
	defer func() { _ = w.Close() }()
	payload := durablePayload(t)
	first := appendAsync(w, payload)
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	second := appendAsync(w, payload)
	select {
	case r := <-second:
		t.Fatalf("unbounded append returned %v during the stall", r)
	case <-time.After(500 * time.Millisecond):
	}
	close(release)
	for _, ch := range []chan appendResult{first, second} {
		require.NoError(t, awaitResult(t, ch).err)
	}
	require.Equal(t, int64(0), atomic.LoadInt64(&w.DurableAckTimeouts))
}

// The default is DefaultAckTimeout; queued mode never waits, so the timeout
// does not apply to it.
func TestDurableAckTimeoutDefaults(t *testing.T) {
	w := newDurableWriter(t, SyncModeAsync, 16)
	defer w.Close()
	require.Equal(t, DefaultAckTimeout, w.config.AckTimeout)
	q, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeAsync, AckTimeout: time.Millisecond, Logger: zerolog.Nop()})
	require.NoError(t, err)
	defer q.Close()
	release, held := stallWriterLoop(t)
	defer close(release)
	_, err = q.AppendRawWithMetaTracked("iot", durablePayload(t))
	require.NoError(t, err, "queued mode acknowledges on enqueue")
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	_, err = q.AppendRawWithMetaTracked("iot", durablePayload(t))
	require.False(t, errors.Is(err, errAckTimeout))
}
