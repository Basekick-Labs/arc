package filereplication

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// RunCatchUp walks a snapshot of the cluster file manifest and enqueues every
// entry the local node should hold but doesn't. It is the Phase 3 mechanism
// that brings a node with a stale or empty local backend back into sync with
// the manifest: on startup (and only on startup — periodic reconciliation is
// not in scope for Phase 3), the coordinator hands us the output of
// fsm.GetAllFiles() and we feed each entry through Enqueue so the regular
// worker pool pulls the missing bytes from a peer.
//
// RunCatchUp does NOT itself talk to peers or verify files — it relies on
// Enqueue's existing origin-is-self check, the inflight dedup set (so
// reactive FSM callbacks can race without double-pulling), and the workers'
// backend.Exists pre-check. All the actual fetch work flows through the
// same code path that Phase 2 reactive pulls use, which means the same
// retry/backoff/checksum/metrics apply automatically.
//
// To avoid a thundering-herd drop storm on large manifests, the feeder sleeps
// briefly whenever the queue is above CatchUpQueueHighWater (default 80%).
// This gives the workers time to drain and keeps reactive drops rare. The
// walker is NOT a hard gate on queries — the release notes document the
// eventual-consistency window between startup and full drain.
//
// RunCatchUp is safe to call at most once per puller lifecycle: the
// catchupStartedAt atomic is CAS-guarded so a second call short-circuits.
// The method returns when all entries have been processed (enqueued or
// skipped) or ctx is cancelled; it does not wait for the actual pulls to
// complete.
func (p *Puller) RunCatchUp(ctx context.Context, entries []*raft.FileEntry) {
	// Single-shot guard: only one catch-up per puller lifetime.
	if !p.catchupStartedAt.CompareAndSwap(0, time.Now().Unix()) {
		p.logger.Debug().Msg("File puller catch-up already ran, skipping")
		return
	}

	total := len(entries)
	p.logger.Info().
		Int("manifest_entries", total).
		Msg("File puller catch-up started")

	// Pre-capture stats counters so we can log the delta at the end. Using
	// the Stats() map would pull in catch-up counters and confuse the
	// summary — we want "what did catch-up cause".
	startEnqueued := p.totalEnqueued.Load()
	startPulled := p.totalPulled.Load()
	startSkippedLocal := p.totalSkippedLocal.Load()
	startSkippedDup := p.totalSkippedDup.Load()
	startDropped := p.totalDropped.Load()

	// High-water mark in absolute queue slots. capFraction < 1 is clamped in
	// New(); below we just multiply and floor.
	queueCap := cap(p.queue)
	highWater := int(float64(queueCap) * p.cfg.CatchUpQueueHighWater)
	if highWater < 1 {
		highWater = 1
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			p.logger.Warn().
				Int64("walked", p.catchupEntriesWalked.Load()).
				Int("total", total).
				Msg("File puller catch-up cancelled")
			return
		}
		p.catchupEntriesWalked.Add(1)
		if entry == nil {
			continue
		}

		// Backpressure: if the queue is above the high-water mark, sleep
		// briefly to let workers drain. This loop is intentionally simple
		// (no exponential backoff) because the expected case is that the
		// walker catches up within a few hundred ms.
		for len(p.queue) >= highWater {
			select {
			case <-ctx.Done():
				return
			case <-p.ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}

		// Snapshot Enqueue-side counters so we can tell why this entry was
		// accepted, deduped, or dropped. Counters are monotonic atomics so a
		// simple before/after comparison is race-free.
		beforeEnqueued := p.totalEnqueued.Load()
		beforeSkippedDup := p.totalSkippedDup.Load()
		beforeSkippedSelf := p.totalSkippedSelf.Load()
		beforeDropped := p.totalDropped.Load()

		p.Enqueue(entry)

		switch {
		case p.totalEnqueued.Load() > beforeEnqueued:
			p.catchupEnqueued.Add(1)
			// Tag this path as catch-up so FullyCaughtUp can scope its
			// "still draining" check to the cold-start batch and ignore
			// steady-state ingest. Tag fires only when Enqueue accepted
			// the entry (it's now in inflight); workers clear the tag on
			// processEntry's defer.
			p.markCatchUp(entry.Path)
		case p.totalSkippedDup.Load() > beforeSkippedDup:
			// Already enqueued (by a reactive callback or a prior walker
			// entry with the same path) — count as skipped so the caller
			// can see it happened. The path is already in inflight so
			// FullyCaughtUp will see it through inflightCount; no extra
			// tagging needed.
			p.catchupSkippedLocal.Add(1)
		case p.totalSkippedSelf.Load() > beforeSkippedSelf:
			// Origin is self — file is already local by construction.
			p.catchupSkippedLocal.Add(1)
		case p.totalDropped.Load() > beforeDropped:
			// Queue full even after backpressure sleep — give up on this
			// entry; a future restart or reactive callback will retry.
			// Bump the catch-up-scoped counter so FullyCaughtUp sees the
			// gap (steady-state drops don't count against the gate).
			p.catchupDropped.Add(1)
		}
	}

	p.catchupCompletedAt.Store(time.Now().Unix())

	p.logger.Info().
		Int("manifest_entries", total).
		Int64("catchup_walked", p.catchupEntriesWalked.Load()).
		Int64("catchup_enqueued", p.catchupEnqueued.Load()).
		Int64("catchup_skipped_local", p.catchupSkippedLocal.Load()).
		Int64("enqueued_delta", p.totalEnqueued.Load()-startEnqueued).
		Int64("pulled_so_far_delta", p.totalPulled.Load()-startPulled).
		Int64("skipped_local_delta", p.totalSkippedLocal.Load()-startSkippedLocal).
		Int64("skipped_dup_delta", p.totalSkippedDup.Load()-startSkippedDup).
		Int64("dropped_delta", p.totalDropped.Load()-startDropped).
		Msg("File puller catch-up completed")
}
