package filereplication

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

type manifestWalkStatus uint8

const (
	manifestWalkCompleted manifestWalkStatus = iota
	manifestWalkAborted
	manifestWalkGated
)

type manifestWalkResult struct {
	status        manifestWalkStatus
	gated         bool
	entriesWalked int64
	enqueued      int64
	skipped       int64
	dropped       int64
}

// RunCatchUp walks the cluster file manifest in pages and enqueues every
// entry the local node should hold but doesn't. It is the Phase 3 mechanism
// that brings a node with a stale or empty local backend back into sync with
// the manifest: on startup (and only on startup), the coordinator hands us a
// paginated fetch function (backed by fsm.GetFilesPaginated) and we feed each
// page through Enqueue so the regular worker pool pulls the missing bytes from
// a peer.
//
// The fetch function follows cursor-based pagination: cursor="" for the first
// page, and each call returns (page, nextCursor, error). An empty nextCursor
// means no more pages. This avoids allocating a full O(N) snapshot of the
// manifest.
//
// RunCatchUp does NOT itself talk to peers or verify files. It relies on
// Enqueue's existing origin-is-self check, the inflight dedup set (so reactive
// FSM callbacks can race without double-pulling), and the workers' backend
// checks. All actual fetch work flows through the same code path that Phase 2
// reactive pulls use, which means the same retry, backoff, checksum, and
// metrics behavior applies automatically.
//
// To avoid a thundering-herd drop storm on large manifests, the feeder sleeps
// briefly whenever the queue is above CatchUpQueueHighWater (default 80%).
// The walker is not a hard gate on queries — the release notes document the
// eventual-consistency window between startup and full drain.
//
// RunCatchUp is safe to call at most once per puller lifecycle: the
// catchupStartedAt atomic is CAS-guarded so a second call short-circuits. The
// method returns when all entries have been processed (enqueued or skipped) or
// ctx is cancelled; it does not wait for actual pulls to complete.
func (p *Puller) RunCatchUp(ctx context.Context, fetch func(cursor string, limit int) ([]*raft.FileEntry, string, error)) {
	// Single-shot guard: only one catch-up per puller lifetime.
	if !p.catchupStartedAt.CompareAndSwap(0, time.Now().Unix()) {
		p.logger.Debug().Msg("File puller catch-up already ran, skipping")
		return
	}
	defer close(p.catchupFinished)

	p.reconciliationMu.Lock()
	defer p.reconciliationMu.Unlock()
	p.walkManifest(ctx, fetch, true)
}

// RunReconciliation walks the current manifest and enqueues entries through
// the normal pull path. Unlike RunCatchUp, it is repeatable and does not
// create startup catch-up state or remove startup tags. Its failures do not
// reopen readiness, while successful pulls may heal existing path-scoped
// catch-up failure/drop state. It returns false when another manifest walk is
// already in progress.
func (p *Puller) RunReconciliation(ctx context.Context, fetch func(cursor string, limit int) ([]*raft.FileEntry, string, error)) bool {
	if !p.reconciliationMu.TryLock() {
		p.recheckBusy.Add(1)
		return false
	}
	defer p.reconciliationMu.Unlock()

	p.recheckStarted.Add(1)
	result := p.walkManifest(ctx, fetch, false)
	p.recheckEntriesWalked.Add(result.entriesWalked)
	p.recheckEnqueued.Add(result.enqueued)
	p.recheckSkipped.Add(result.skipped)
	p.recheckDropped.Add(result.dropped)
	if result.gated {
		p.recheckGated.Add(1)
	}
	switch result.status {
	case manifestWalkCompleted:
		p.recheckCompleted.Add(1)
	case manifestWalkGated:
		// A gate rejection is reported separately from an aborted pass.
	default:
		p.recheckAborted.Add(1)
	}
	return true
}

func lifecycleDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

// walkManifest is the shared paginated manifest feeder. Startup walks retain
// the #392 metrics and path bookkeeping; periodic walks use separate metrics,
// never create or remove startup tags, and can heal existing path-scoped
// failure/drop state only when their pull succeeds.
func (p *Puller) walkManifest(ctx context.Context, fetch func(cursor string, limit int) ([]*raft.FileEntry, string, error), startup bool) manifestWalkResult {
	result := manifestWalkResult{status: manifestWalkCompleted}
	if startup {
		p.logger.Info().Msg("File puller catch-up started (paginated)")
	}
	p.mu.Lock()
	lifecycleCtx := p.ctx
	p.mu.Unlock()

	// Pre-capture stats counters so startup logging reports what the walk
	// caused. Periodic passes use their local result counters instead.
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
	reconciliationGateRejected := func(midPass bool) bool {
		if startup || p.reconciliationAllowed() {
			return false
		}
		result.gated = true
		if midPass {
			result.status = manifestWalkAborted
			p.logger.Warn().
				Str("replication_recheck_status", "aborted").
				Str("reason", "eligibility changed").
				Msg("Periodic file reconciliation aborted")
		} else {
			result.status = manifestWalkGated
			p.logger.Info().
				Str("replication_recheck_status", "gated").
				Msg("Periodic file reconciliation gated")
		}
		return true
	}

	const pageSize = 1000
	cursor := ""
	manifestFetchStarted := false
	for {
		if ctx.Err() != nil || (lifecycleCtx != nil && lifecycleCtx.Err() != nil) {
			if startup {
				p.logger.Warn().
					Int64("walked", p.catchupEntriesWalked.Load()).
					Msg("File puller catch-up cancelled")
			} else {
				p.logger.Warn().
					Str("replication_recheck_status", "aborted").
					Str("reason", "context cancelled").
					Msg("Periodic file reconciliation aborted")
			}
			result.status = manifestWalkAborted
			return result
		}
		if reconciliationGateRejected(manifestFetchStarted) {
			return result
		}

		page, nextCursor, err := fetch(cursor, pageSize)
		if err != nil {
			if startup {
				p.logger.Error().Err(err).Str("cursor", cursor).Msg("Catch-up: page fetch failed, aborting")
			} else {
				p.logger.Error().
					Err(err).
					Str("cursor", cursor).
					Str("replication_recheck_status", "aborted").
					Msg("Periodic file reconciliation page fetch failed")
			}
			result.status = manifestWalkAborted
			return result
		}
		manifestFetchStarted = true
		if ctx.Err() != nil || (lifecycleCtx != nil && lifecycleCtx.Err() != nil) {
			result.status = manifestWalkAborted
			return result
		}
		if reconciliationGateRejected(true) {
			return result
		}
		if len(page) == 0 {
			if nextCursor == "" {
				break
			}
			if nextCursor == cursor {
				if startup {
					p.logger.Error().Str("cursor", cursor).Msg("Catch-up: page cursor did not advance, aborting")
				} else {
					p.logger.Error().
						Str("cursor", cursor).
						Str("replication_recheck_status", "aborted").
						Msg("Periodic file reconciliation cursor did not advance")
				}
				result.status = manifestWalkAborted
				return result
			}
			cursor = nextCursor
			continue
		}

		for _, entry := range page {
			if ctx.Err() != nil || (lifecycleCtx != nil && lifecycleCtx.Err() != nil) {
				result.status = manifestWalkAborted
				return result
			}
			if reconciliationGateRejected(true) {
				return result
			}
			if startup {
				p.catchupEntriesWalked.Add(1)
			}
			if !startup {
				result.entriesWalked++
			}
			if entry == nil {
				continue
			}

			// Backpressure: if the queue is above the high-water mark, sleep
			// briefly to let workers drain.
			for len(p.queue) >= highWater {
				if reconciliationGateRejected(true) {
					return result
				}
				select {
				case <-ctx.Done():
					result.status = manifestWalkAborted
					return result
				case <-lifecycleDone(lifecycleCtx):
					result.status = manifestWalkAborted
					return result
				case <-time.After(50 * time.Millisecond):
				}
			}
			if reconciliationGateRejected(true) {
				return result
			}

			marked := false
			if startup {
				// Fast-path self-origin entries so Enqueue does not create a
				// catch-up tag for a file this node already owns.
				if entry.OriginNodeID == p.cfg.SelfNodeID {
					p.totalSkippedSelf.Add(1)
					p.catchupSkippedLocal.Add(1)
					continue
				}
				marked = p.markCatchUp(entry.Path)
			}

			source := enqueueSourceReconciliation
			if startup {
				source = enqueueSourceCatchUp
			}
			enqueueStatus := p.enqueue(entry, source)

			if startup {
				switch enqueueStatus {
				case enqueueResultEnqueued:
					p.catchupEnqueued.Add(1)
				case enqueueResultSkippedSelf, enqueueResultSkippedDuplicate:
					p.catchupSkippedLocal.Add(1)
				case enqueueResultDropped:
					if marked {
						// No worker will remove a tag when the queue rejects the
						// entry, so compensate for the pre-enqueue mark.
						p.unmarkCatchUp(entry.Path)
					}
					p.recordCatchUpDrop(entry.Path)
				}
			} else {
				switch enqueueStatus {
				case enqueueResultEnqueued:
					result.enqueued++
				case enqueueResultSkippedSelf, enqueueResultSkippedDuplicate:
					result.skipped++
				case enqueueResultDropped:
					result.dropped++
				}
			}
		}
		if ctx.Err() != nil || (lifecycleCtx != nil && lifecycleCtx.Err() != nil) {
			result.status = manifestWalkAborted
			return result
		}
		if reconciliationGateRejected(true) {
			return result
		}

		if nextCursor == "" {
			break
		}
		if nextCursor == cursor {
			if startup {
				p.logger.Error().Str("cursor", cursor).Msg("Catch-up: page cursor did not advance, aborting")
			} else {
				p.logger.Error().
					Str("cursor", cursor).
					Str("replication_recheck_status", "aborted").
					Msg("Periodic file reconciliation cursor did not advance")
			}
			result.status = manifestWalkAborted
			return result
		}
		cursor = nextCursor
	}

	if startup {
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
	} else {
		p.logger.Info().
			Str("replication_recheck_status", "completed").
			Int64("replication_recheck_entries_walked", result.entriesWalked).
			Int64("replication_recheck_enqueued", result.enqueued).
			Int64("replication_recheck_skipped", result.skipped).
			Int64("replication_recheck_dropped", result.dropped).
			Msg("Periodic file reconciliation completed")
	}
	return result
}
