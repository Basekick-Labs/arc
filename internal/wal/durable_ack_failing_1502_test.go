package wal

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type ackListener struct {
	mu     sync.Mutex
	states []bool
}

func (l *ackListener) fn(failing bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.states = append(l.states, failing)
}

func (l *ackListener) snapshot() []bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]bool(nil), l.states...)
}

// #1502: a timed-out durable append turns the state on; a sync that succeeds
// turns it off, and with every writer held the writer's own probe sync is
// what succeeds. Registration delivers the current state.
func TestDurableAckFailingFlipsOnTimeoutAndOffOnTheNextGoodSync(t *testing.T) {
	w := newTimeoutWriter(t, 16, 200*time.Millisecond)
	l := &ackListener{}
	w.OnDurableAckChange(l.fn)
	require.Equal(t, []bool{false}, l.snapshot(), "registration delivers the current state")
	release, held := stallWriterLoop(t)
	released := false
	defer func() {
		if !released {
			close(release)
		}
		_ = w.Close()
	}()
	first := appendAsync(w, durablePayload(t))
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	require.ErrorIs(t, awaitResult(t, first).err, errAckTimeout)
	require.Equal(t, []bool{false, true}, l.snapshot(), "set at the first refusal")
	require.True(t, w.ackFailing.Load())

	// The device recovers; no writer sends anything (they are held). The
	// probe sync clears the state within about a second.
	released = true
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for w.ackFailing.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.False(t, w.ackFailing.Load(), "no write came, so only the probe sync can clear the state")
	require.Equal(t, []bool{false, true, false}, l.snapshot())
}

// A failed sync turns the state on; the writer rotates away from the file
// that reported it, and the probe on the new file turns it off.
func TestDurableAckFailingFlipsOnAFailedSync(t *testing.T) {
	rec := installSyncRecorder(t, 1)
	rec.errOnce = errors.New("injected sync failure")
	// A short tick: the writer loop's tick is what rotates away from the
	// failed file and requests the probe sync.
	w, err := NewWriter(&WriterConfig{
		WALDir: t.TempDir(), SyncMode: SyncModeFsync, MaxSizeBytes: 64 << 20, MaxAge: time.Hour,
		SyncInterval: 50 * time.Millisecond, SyncBytes: 1 << 30, BufferSize: 16,
		AckMode: AckModeDurable, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	defer w.Close()
	l := &ackListener{}
	w.OnDurableAckChange(l.fn)
	res := appendAsync(w, durablePayload(t))
	<-rec.started
	close(rec.release)
	require.ErrorIs(t, awaitResult(t, res).err, ErrNotDurable)
	require.Equal(t, []bool{false, true}, l.snapshot())
	deadline := time.Now().Add(5 * time.Second)
	for w.ackFailing.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.False(t, w.ackFailing.Load(), "the new file's probe sync clears it")
	// And a durable write is accepted again.
	_, err = w.AppendRawWithMetaTracked("iot", durablePayload(t))
	require.NoError(t, err)
}

// Queued mode never waits, so it never reports the state, whatever the device
// does; and a per-write refusal (an oversized payload) is not a device state.
func TestDurableAckFailingNeverSetInQueuedModeOrByAPerWriteError(t *testing.T) {
	q, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeAsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	defer q.Close()
	_, err = q.AppendRawWithMetaTracked("iot", durablePayload(t))
	require.NoError(t, err)
	require.False(t, q.ackFailing.Load())

	w := newDurableWriter(t, SyncModeAsync, 16)
	defer w.Close()
	_, err = w.AppendRawWithMetaTracked(string(make([]byte, 300)), durablePayload(t)) // database name over 255 bytes
	require.ErrorIs(t, err, ErrNotDurable)
	require.False(t, w.ackFailing.Load(), "a per-write error is not the device's state")
	require.Equal(t, int64(0), atomic.LoadInt64(&w.DurableAckTimeouts))
}
