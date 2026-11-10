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
	"github.com/vmihailenco/msgpack/v5"
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

// A multi-chunk append whose later chunk times out at admission retracts the
// admitted chunks without waiting on the device either: the earlier form took
// the writer mutex the stalled loop holds, for as long as the device did.
func TestDurableAckTimeoutOnAMultiChunkAppendDoesNotWaitOnTheDevice(t *testing.T) {
	// Built before the stall: marshalling it under -race takes longer than
	// the timeout, and the parked append must not time out before the first
	// chunk has taken the queue slot.
	value := string(make([]byte, 16<<20))
	records := make([]map[string]interface{}, 0, 8)
	for i := 0; i < 8; i++ {
		records = append(records, map[string]interface{}{"v": value})
	}
	big, err := msgpack.Marshal(records)
	require.NoError(t, err)
	require.Greater(t, len(big), MaxWALPayloadSize)

	w := newTimeoutWriter(t, 1, 200*time.Millisecond)
	release, held := stallWriterLoop(t)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = w.Close()
	}()
	// An untracked write parks the loop: it never waits, so nothing but the
	// append under test times out and competes for the queue slot.
	require.NoError(t, w.AppendRawWithMeta("iot", durablePayload(t)))
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	// Two chunks (an array of records over MaxWALPayloadSize is split): the
	// first takes the one queue slot, the second waits for admission and
	// times out.
	start := time.Now()
	_, err = w.AppendRawWithMetaTracked("iot", big)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrNotDurable)
	require.ErrorIs(t, err, errAckTimeout)
	// The bound guards against the unbounded wait on the writer mutex the
	// stalled loop holds (the mutation runs to the test timeout); the time
	// measured also includes splitting the payload into chunks, which under
	// -race on a slow runner takes seconds by itself.
	require.Less(t, elapsed, 10*time.Second, "the admitted chunk's retraction must not wait on the device")
	require.Equal(t, int64(1), atomic.LoadInt64(&w.DurableAckTimeouts), "one timeout per append")
	require.Equal(t, int64(1), atomic.LoadInt64(&w.RetractionsFailed), "the full queue refused the retraction: counted, the chunk may replay")
	require.Equal(t, 0, w.PendingUnflushedCount(), "the admitted chunk was released")
	released = true
	close(release)
}

// A durable entry is admitted when the queue has room, even with its deadline
// already past: the deadline can run out while a large entry is built, and a
// select with both cases ready picks one at random, refusing the write as a
// stalled device and suspending MQTT for nothing (#1577). Repeated, so the
// random pick cannot pass by luck.
func TestDurableAdmissionTakesRoomPastTheDeadline(t *testing.T) {
	w := newTimeoutWriter(t, 1, time.Hour)
	release, held := stallWriterLoop(t)
	defer func() {
		close(release)
		_ = w.Close()
	}()
	require.NoError(t, w.AppendRawWithMeta("iot", durablePayload(t)))
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	fired := make(chan time.Time)
	close(fired)
	for i := 0; i < 64; i++ {
		entry := walEntry{data: []byte{1}, durable: true, done: make(chan error, 1)}
		require.NoError(t, w.enqueueDurable(entry, fired), "attempt %d: refused with room in the queue", i)
		<-w.entryChan // the stalled loop never takes it
	}
	require.False(t, w.ackFailing.Load(), "acknowledgements reported failing with room in the queue")
}
