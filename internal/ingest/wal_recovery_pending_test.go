package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// MarkWALRecoveryPending used to set the flush-failure flag, whose other reader
// is the shutdown WAL purge. So any startup that kept a WAL file made every later
// shutdown log, at Error, "buffer flush did not complete cleanly on shutdown …
// Investigate the buffer flush errors logged above" — pointing the operator at
// logs that do not exist. Retention was right; the attribution was not.
//
// These assert the flag algebra directly: retention is unchanged, recovery is
// still armed, and the two reasons are now distinguishable.
func TestWALRecoveryPendingRetainsWithoutClaimingAFlushFailure(t *testing.T) {
	b := &ArrowBuffer{}
	// A Close that completed cleanly, which is the precondition for the
	// retention reason to be attributable at all.
	b.closeFlushClean.Store(true)
	require.True(t, b.CloseFlushedCleanly())
	require.False(t, b.RetainedForWALRecoveryOnly())
	require.False(t, b.HasFlushFailure())

	b.MarkWALRecoveryPending()

	require.True(t, b.HasFlushFailure(), "the maintenance tick must still arm recovery")
	require.False(t, b.CloseFlushedCleanly(), "the WAL must still be retained")
	require.True(t, b.RetainedForWALRecoveryOnly(), "the shutdown purge must be able to name the real cause")
	require.False(t, b.flushFailed(), "a kept file is not a flush failure")
}

// A genuine flush failure keeps the Error: it is the one that means records are
// at risk, and it wins over a concurrent pending recovery.
func TestFlushFailureOutranksAPendingRecovery(t *testing.T) {
	b := &ArrowBuffer{}
	b.closeFlushClean.Store(true)
	b.MarkWALRecoveryPending()
	b.markFlushFailure()

	require.True(t, b.HasFlushFailure())
	require.False(t, b.CloseFlushedCleanly())
	require.False(t, b.RetainedForWALRecoveryOnly(),
		"a real flush failure must keep the investigate-the-flush-errors Error")
}

// Every other way records can be lost also outranks it, or the shutdown purge
// would downgrade a data-loss Error to a Warn.
func TestRetainedForWALRecoveryOnlyIsNotClaimedWhenDataWasLost(t *testing.T) {
	t.Run("close failed", func(t *testing.T) {
		b := &ArrowBuffer{}
		b.closeFlushClean.Store(true)
		b.MarkWALRecoveryPending()
		b.closeFailed.Store(true)
		require.False(t, b.RetainedForWALRecoveryOnly())
	})
	t.Run("wal-only records", func(t *testing.T) {
		b := &ArrowBuffer{}
		b.closeFlushClean.Store(true)
		b.MarkWALRecoveryPending()
		b.walOnlyRecords.Store(1)
		require.False(t, b.RetainedForWALRecoveryOnly())
	})
	t.Run("close never ran", func(t *testing.T) {
		b := &ArrowBuffer{}
		b.MarkWALRecoveryPending()
		require.False(t, b.RetainedForWALRecoveryOnly(),
			"a shutdown that never reached the buffer cannot attribute its retention")
	})
}

// The generation CAS clears both reasons together, or the tick would spin on a
// recovery that already succeeded.
func TestResetFlushFailureClearsBothReasons(t *testing.T) {
	b := &ArrowBuffer{}
	b.closeFlushClean.Store(true)
	b.MarkWALRecoveryPending()
	generation := b.FlushFailureGeneration()

	require.True(t, b.ResetFlushFailure(generation))
	require.False(t, b.HasFlushFailure())
	require.True(t, b.CloseFlushedCleanly())
	require.False(t, b.RetainedForWALRecoveryOnly())
}

// A reset pinned to a stale generation must change nothing. The generation is
// what makes the recovery latch safe to clear: a flush worker that failed while
// the pass was running bumps it, and the reset then refuses rather than
// declaring the WAL disposable.
//
// A refused reset keeps each reason exactly as it was. The reset used to clear
// both flags, re-check the generation and restore the flush flag to TRUE
// rather than to a snapshot, since a mismatch there meant a real flush failure
// (#1170); a reader between the clear and the restore saw the latch clear
// (#1938). The reset is now one swap of the latch word, so there is no clear
// to restore from; the race is pinned deterministically by
// TestResetFlushFailureNeverShowsAClearLatchOverAConcurrentFailure1938.
func TestResetFlushFailureRefusesAStaleGeneration(t *testing.T) {
	b := &ArrowBuffer{}
	b.closeFlushClean.Store(true)
	b.MarkWALRecoveryPending()
	stale := b.FlushFailureGeneration()

	// A flush worker fails while the recovery pass was running.
	b.markFlushFailure()

	require.False(t, b.ResetFlushFailure(stale))
	require.True(t, b.flushFailed(),
		"a flush failure that arrived during recovery must survive a stale reset")
	require.True(t, b.recoveryPending(),
		"the recovery reason was still outstanding and must survive too")
	require.False(t, b.CloseFlushedCleanly())
	require.False(t, b.RetainedForWALRecoveryOnly())
}
