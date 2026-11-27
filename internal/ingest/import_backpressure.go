package ingest

import (
	"context"
	"errors"
	"time"
)

// ErrBufferedBytesStuck is returned by WaitBufferedBelow when the bytes held
// for flushing stayed above the limit for the whole wait: flushes are not
// keeping up (storage slow or down), or other writers keep the buffer full.
var ErrBufferedBytesStuck = errors.New("buffered rows stayed above the limit for the whole wait: flushes are not keeping up")

// BufferedBytes is the estimated size of the rows written to the buffer and
// not yet released by their flush: buffered, queued or being written.
func (b *ArrowBuffer) BufferedBytes() int64 { return b.bufferBytesBuffered.Load() }

// MemoryLimitBytes is the memory limit this process runs under (cgroup or
// physical), 0 when unknown.
func (b *ArrowBuffer) MemoryLimitBytes() uint64 { return b.memoryLimitBytes }

// waitBufferedPoll is how often WaitBufferedBelow rechecks. A var for tests.
var waitBufferedPoll = 10 * time.Millisecond

// WaitBufferedBelow blocks while BufferedBytes is above limit(), for at most
// maxWait. A writer that produces rows faster than they flush (a bulk import
// parses far faster than storage takes files) calls it between batches, so
// what it has written but not yet flushed stays bounded instead of growing to
// the size of its input (#1070 follow-up). Flushes are asynchronous, so
// writing alone never slows such a writer down.
//
// limit is read again on every recheck: concurrent imports share one limit
// that moves as they start and finish, and a waiter holding the value it
// started with would wait for a level the others no longer pace to (#1429).
// While still above it, nudge (when not nil) runs every nudgeEvery: rows
// written after the waiter's own flush need flushing too. A nudge's error
// ends the wait with it: a failed flush can drop the rows it took (with the
// WAL off they are gone), which would otherwise read as the wait succeeding.
func (b *ArrowBuffer) WaitBufferedBelow(ctx context.Context, limit func() int64, maxWait time.Duration, nudge func() error, nudgeEvery time.Duration) error {
	if b.bufferBytesBuffered.Load() <= limit() {
		return nil
	}
	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	tick := time.NewTicker(waitBufferedPoll)
	defer tick.Stop()
	lastNudge := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return ErrBufferedBytesStuck
		case <-tick.C:
			if b.closing.Load() {
				return ErrBufferClosing
			}
			if b.bufferBytesBuffered.Load() <= limit() {
				return nil
			}
			if nudge != nil && nudgeEvery > 0 && time.Since(lastNudge) >= nudgeEvery {
				if err := nudge(); err != nil {
					return err
				}
				lastNudge = time.Now()
			}
		}
	}
}
