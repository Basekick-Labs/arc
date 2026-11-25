package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setResetSeams installs ResetFlushFailure's commit seams for one test.
func setResetSeams(t *testing.T, before, after func(*ArrowBuffer)) {
	t.Helper()
	resetBeforeCommit, resetAfterCommit = before, after
	t.Cleanup(func() { resetBeforeCommit, resetAfterCommit = nil, nil })
}

// #1938 W2: a flush failing while a recovery pass resets the latch must never
// be observable as a clear latch. The reset used to store false into both
// flags, re-check the generation and restore them; a DELETE reading the flags
// between the clear and the restore went ahead. The seams put the failure
// between the reset's generation check and its commit, and read the latch
// right after the commit, on the refused path this test takes.
func TestResetFlushFailureNeverShowsAClearLatchOverAConcurrentFailure1938(t *testing.T) {
	b := &ArrowBuffer{}
	b.markFlushFailure()
	generation := b.FlushFailureGeneration()

	var seen []bool
	setResetSeams(t,
		func(b *ArrowBuffer) { b.markFlushFailure() },
		func(b *ArrowBuffer) { seen = append(seen, b.HasFlushFailure()) })

	require.False(t, b.ResetFlushFailure(generation), "a failure during the reset must refuse it")
	require.Equal(t, []bool{true}, seen, "the latch read clear right after the reset's commit while a failure was outstanding")
	require.True(t, b.flushFailed())
	require.Equal(t, generation+1, b.FlushFailureGeneration())
}

// The #1170 split survives a refused reset in both directions: a pending
// recovery is not turned into a flush failure, and a flush failure is not lost.
func TestRefusedResetKeepsEachReasonAsItWas1938(t *testing.T) {
	t.Run("a recovery mark during the reset of a pending recovery", func(t *testing.T) {
		b := &ArrowBuffer{}
		b.MarkWALRecoveryPending()
		generation := b.FlushFailureGeneration()
		setResetSeams(t, func(b *ArrowBuffer) { b.MarkWALRecoveryPending() }, nil)

		require.False(t, b.ResetFlushFailure(generation))
		require.True(t, b.recoveryPending())
		require.False(t, b.flushFailed(), "a kept file is not a flush failure (#1170)")
	})
	t.Run("a flush failure during the reset of a pending recovery", func(t *testing.T) {
		b := &ArrowBuffer{}
		b.MarkWALRecoveryPending()
		generation := b.FlushFailureGeneration()
		setResetSeams(t, func(b *ArrowBuffer) { b.markFlushFailure() }, nil)

		require.False(t, b.ResetFlushFailure(generation))
		require.True(t, b.recoveryPending())
		require.True(t, b.flushFailed())
	})
}

// FlushFailureState reports the two reasons apart, with the generation, from
// one word.
func TestFlushFailureStateReportsBothReasons1938(t *testing.T) {
	b := &ArrowBuffer{}
	failed, pending, gen := b.FlushFailureState()
	require.Equal(t, []any{false, false, uint64(0)}, []any{failed, pending, gen})

	b.MarkWALRecoveryPending()
	failed, pending, gen = b.FlushFailureState()
	require.Equal(t, []any{false, true, uint64(1)}, []any{failed, pending, gen})

	b.markFlushFailure()
	failed, pending, gen = b.FlushFailureState()
	require.Equal(t, []any{true, true, uint64(2)}, []any{failed, pending, gen})

	require.True(t, b.ResetFlushFailure(2))
	failed, pending, gen = b.FlushFailureState()
	require.Equal(t, []any{false, false, uint64(2)}, []any{failed, pending, gen}, "a reset keeps the generation")
}

// #1938 W1: FlushScopeAndWaitSince refuses when the generation moved after its
// caller's read, even with nothing buffered; FlushScopeAndWait, which reads its
// own, does not.
func TestFlushScopeAndWaitSinceRefusesAMovedBaseline1938(t *testing.T) {
	b := newReplayTestBuffer(t, t.TempDir())
	_, _, baseline := b.FlushFailureState()
	b.markFlushFailure()
	b.ResetFlushFailure(b.FlushFailureGeneration()) // even once the latch is clear again

	_, err := b.FlushScopeAndWaitSince(context.Background(), "db", "cpu", baseline)
	require.Error(t, err)
	_, err = b.FlushScopeAndWait(context.Background(), "db", "cpu")
	require.NoError(t, err)
	_, err = b.FlushScopeAndWaitSince(context.Background(), "db", "cpu", b.FlushFailureGeneration())
	require.NoError(t, err)
}

// The latch-age gauge, driven tick by tick (the sampler calls sampleLatchAge
// once a second).
func TestLatchAgeGauge1938(t *testing.T) {
	b := &ArrowBuffer{}
	b.hasWAL.Store(true)
	t0 := time.Unix(1_000_000, 0)
	at := func(d time.Duration) time.Time { return t0.Add(d) }

	require.Zero(t, b.sampleLatchAge(at(0)), "clear")

	b.markFlushFailure()
	require.Zero(t, b.sampleLatchAge(at(time.Second)), "stamped on the first tick that sees it set")
	require.Equal(t, 3*time.Second, b.sampleLatchAge(at(4*time.Second)))

	b.markFlushFailure() // a second failure is the same episode
	require.Equal(t, 5*time.Second, b.sampleLatchAge(at(6*time.Second)))
	b.MarkWALRecoveryPending()
	require.Equal(t, 6*time.Second, b.sampleLatchAge(at(7*time.Second)))

	require.True(t, b.ResetFlushFailure(b.FlushFailureGeneration()))
	require.Zero(t, b.sampleLatchAge(at(8*time.Second)), "clear again")

	b.MarkWALRecoveryPending() // a new episode starts from its own first tick
	require.Zero(t, b.sampleLatchAge(at(20*time.Second)))
	require.Equal(t, 2*time.Second, b.sampleLatchAge(at(22*time.Second)))

	// Documented limit: a clear and a new failure between two ticks read as
	// one continuing episode.
	require.True(t, b.ResetFlushFailure(b.FlushFailureGeneration()))
	b.markFlushFailure()
	require.Equal(t, 3*time.Second, b.sampleLatchAge(at(23*time.Second)))
}

// Without a WAL nothing clears the latch and a DELETE ignores it, so the gauge
// stays 0 rather than counting up for the life of the process.
func TestLatchAgeGaugeIsZeroWithoutAWAL1938(t *testing.T) {
	b := &ArrowBuffer{}
	b.markFlushFailure()
	t0 := time.Unix(1_000_000, 0)
	require.Zero(t, b.sampleLatchAge(t0))
	require.Zero(t, b.sampleLatchAge(t0.Add(time.Hour)))

	b.SetWAL(nil)
	require.Zero(t, b.sampleLatchAge(t0.Add(2*time.Hour)))
}
