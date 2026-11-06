package wal

import (
	"errors"
	"sync"
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

	// The device recovers: the parked write lands and its sync, the first to
	// succeed, clears the state. (The idle-node probe is covered by
	// TestDurableAckFailingFlipsOnAFailedSync and the async probe test.)
	released = true
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for w.ackFailing.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.False(t, w.ackFailing.Load())
	require.Equal(t, []bool{false, true, false}, l.snapshot())
}

// The admission path sets the state too: a full queue that refuses admission
// for the whole timeout is the device not keeping up.
func TestDurableAckFailingSetByAnAdmissionTimeout(t *testing.T) {
	w := newTimeoutWriter(t, 1, 200*time.Millisecond)
	l := &ackListener{}
	w.OnDurableAckChange(l.fn)
	release, held := stallWriterLoop(t)
	defer func() { close(release); _ = w.Close() }()
	require.NoError(t, w.AppendRawWithMeta("iot", durablePayload(t))) // untracked: parks the loop, never waits
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, w.AppendRawWithMeta("iot", durablePayload(t))) // fills the one slot
	for len(w.entryChan) < 1 {
		time.Sleep(time.Millisecond)
	}
	_, err := w.AppendRawWithMetaTracked("iot", durablePayload(t)) // waits for admission, times out
	require.ErrorIs(t, err, errAckTimeout)
	require.Equal(t, []bool{false, true}, l.snapshot())
}

// A durable append refused because the writer closed is shutdown, not the
// device: the state stays off.
func TestDurableAckFailingNotSetByAClosedWriter(t *testing.T) {
	w := newDurableWriter(t, SyncModeAsync, 16)
	l := &ackListener{}
	w.OnDurableAckChange(l.fn)
	require.NoError(t, w.Close())
	_, err := w.AppendRawWithMetaTracked("iot", durablePayload(t))
	require.ErrorIs(t, err, ErrNotDurable)
	require.Equal(t, []bool{false}, l.snapshot())
	require.False(t, w.ackFailing.Load())
}

// In async mode the probe is a real data sync: a probe that did not touch the
// device would clear the state whatever the device does.
func TestDurableAckFailingProbeSyncsInAsyncMode(t *testing.T) {
	rec := installSyncRecorder(t, 0)
	w, err := NewWriter(&WriterConfig{
		WALDir: t.TempDir(), SyncMode: SyncModeAsync, MaxSizeBytes: 64 << 20, MaxAge: time.Hour,
		SyncInterval: 20 * time.Millisecond, SyncBytes: 1 << 30, BufferSize: 16,
		AckMode: AckModeDurable, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	defer w.Close()
	before := rec.callCount()
	w.setAckFailing(true)
	deadline := time.Now().Add(5 * time.Second)
	for w.ackFailing.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.False(t, w.ackFailing.Load(), "the probe cleared it")
	// async mode's no-op path returns without calling syncFileFn at all.
	require.GreaterOrEqual(t, rec.callCount()-before, 1, "the probe performed a sync")
}

// Probes are paced: at most one a second while the state holds.
func TestDurableAckFailingProbesAtMostOncePerSecond(t *testing.T) {
	rec := installSyncRecorder(t, 0)
	rec.errAlways = errors.New("device keeps failing")
	w, err := NewWriter(&WriterConfig{
		WALDir: t.TempDir(), SyncMode: SyncModeFsync, MaxSizeBytes: 64 << 20, MaxAge: time.Hour,
		SyncInterval: 20 * time.Millisecond, SyncBytes: 1 << 30, BufferSize: 16,
		AckMode: AckModeDurable, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	defer w.Close()
	w.setAckFailing(true)
	time.Sleep(2500 * time.Millisecond)
	// Each probe fails, sticks and rotates (one sync in the rotation): a
	// handful of calls, not one per 20 ms tick.
	require.LessOrEqual(t, rec.callCount(), 8, "probes are paced to one a second")
	require.True(t, w.ackFailing.Load(), "a failing device keeps the state on")
}
