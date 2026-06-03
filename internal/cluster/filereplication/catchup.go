package filereplication

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// RunCatchUp walks the cluster file manifest in pages and enqueues every
// entry the local node should hold but doesn't. It is the Phase 3 mechanism
// that brings a node with a stale or empty local backend back into sync with
// the manifest: on startup (and only on startup — periodic reconciliation is
// not in scope for Phase 3), the coordinator hands us a paginated fetch
// function (backed by fsm.GetFilesPaginated) and we feed each page through
// Enqueue so the regular worker pool pulls the missing bytes from a peer.
//
// The fetch function follows cursor-based pagination: cursor="" for the first
// page, and each call returns (page, nextCursor, error). An empty nextCursor
// means no more pages. This avoids allocating a full O(N) snapshot of the
// manifest — for 1M+ files the old GetAllFiles() path caused ~50ms apply-path
// spikes and ~50MB transient memory.
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
func (p *Puller) RunCatchUp(ctx context.Context, fetch func(cursor string, limit int) ([]*raft.FileEntry, string, error)) {
	// Single-shot guard: only one catch-up per puller lifetime.
	if !p.catchupStartedAt.CompareAndSwap(0, time.Now().Unix()) {
		p.logger.Debug().Msg("File puller catch-up already ran, skipping")
		return
	}

	p.logger.Info().Msg("File puller catch-up started (paginated)")

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

	const pageSize = 1000
	cursor := ""
	for {
		if ctx.Err() != nil {
			p.logger.Warn().
				Int64("walked", p.catchupEntriesWalked.Load()).
				Msg("File puller catch-up cancelled")
			return
		}

		page, nextCursor, err := fetch(cursor, pageSize)
		if err != nil {
			p.logger.Error().Err(err).Str("cursor", cursor).Msg("Catch-up: page fetch failed, aborting")
			return
		}
		if len(page) == 0 {
			break // no more entries
		}

		for _, entry := range page {
			if ctx.Err() != nil {
				return
			}
			p.catchupEntriesWalked.Add(1)
			if entry == nil {
				continue
			}

			// Backpressure: if the queue is above the high-water mark, sleep
			// briefly to let workers drain.
			for len(p.queue) >= highWater {
				select {
				case <-ctx.Done():
					return
				case <-p.ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}

			// Fast-path: if origin is self, Enqueue would skip and we don't
			// want to leak a catch-up tag.
			if entry.OriginNodeID == p.cfg.SelfNodeID {
				p.totalSkippedSelf.Add(1)
				p.catchupSkippedLocal.Add(1)
				continue
			}

			marked := p.markCatchUp(entry.Path)

			beforeEnqueued := p.totalEnqueued.Load()
			beforeSkippedDup := p.totalSkippedDup.Load()
			beforeDropped := p.totalDropped.Load()

			p.Enqueue(entry)

			switch {
			case p.totalEnqueued.Load() > beforeEnqueued:
				p.catchupEnqueued.Add(1)
			case p.totalSkippedDup.Load() > beforeSkippedDup:
				p.catchupSkippedLocal.Add(1)
			case p.totalDropped.Load() > beforeDropped:
				if marked {
					p.unmarkCatchUp(entry.Path)
				}
				p.recordCatchUpDrop(entry.Path)
			}
		}

		cursor = nextCursor
		if cursor == "" {
			break
		}
	}

	p.catchupCompletedAt.Store(time.Now().Unix())

	p.logger.Info().
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
