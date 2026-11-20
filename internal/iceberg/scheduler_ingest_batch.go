package iceberg

import (
	"context"
	"sync"
	"time"
)

// Ingest appends of several tables at once (#1778).
//
// On object storage an append is a few hundred milliseconds of waiting on
// requests made one after another, and with one export at a time a node
// served under three tables a second. The waiting is per table, so the
// appends of different tables can overlap.
//
// What does not change is who touches the scheduler's state: the loop
// goroutine, alone. It takes a batch of due requests, decides what each is
// (prepareIngest), runs the appends at once and WAITS for all of them, and
// only then records what they did (finishAppend), one after another, in the
// order it took them. While a batch runs the loop runs nothing else: no
// reconcile pass, no drop, no other request. So, as before:
//
//   - a table is in one export at a time (a table has one due request, and
//     taking it removes it);
//   - a reconcile pass and an append never overlap, for any table;
//   - recentAdds, appendOK and the pending files are read before the appends
//     start and written after they end.
//
// Reconcile passes are not run at once. A request that needs one (a table's
// first export, a fallback, the pass after appendMaxBetweenPasses appends)
// runs it on the loop goroutine after the batch, as before.

// appendTimeout bounds one append of a batch. The loop waits for the whole
// batch, so a wedged append holds the others' bookkeeping and every other
// request for this long; an append is a handful of requests, and one that
// runs out of time falls back to a reconcile pass with its own budget
// (measurementTimeout).
var appendTimeout = 30 * time.Second // a var for tests

// batchFileReads is how many footers the appends of one batch read at once,
// all together: the object store client keeps 16 idle connections per host,
// and more than that at once means opening new ones.
const batchFileReads = 16

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// serviceIngestBatches serves the due ingest requests, the one due the
// longest first: at most maxRequests of them, of which at most maxPasses may
// run a reconcile pass. Appends run up to ingestConcurrency at once.
// Requests of the other kinds and database drops go first, before each batch
// and before each pass.
func (s *Scheduler) serviceIngestBatches(ctx context.Context, maxRequests, maxPasses int) {
	served, passes := 0, 0
	for ctx.Err() == nil && served < maxRequests {
		if s.otherRequestsQueued() {
			s.serviceDrops(ctx)
			s.serviceRequests(ctx)
		}
		if _, due := s.nextIngestDue(); !due {
			return
		}
		if !s.gateOpen() {
			s.dropIngest() // another node exports
			return
		}
		// Take requests until the batch is full or one needs a pass.
		var batch []*ingestJob
		var pass *ingestJob
		took, stop := 0, false
		for len(batch) < s.ingestConcurrency && served < maxRequests && pass == nil && !stop {
			m, key, ok := s.takeDueIngest(time.Now())
			if !ok {
				break // none due yet: the loop's timer comes back for it
			}
			took++
			served++
			if _, blocked := s.lastBlocked[key]; blocked {
				continue // awaiting a namespace migration: the pass handles it
			}
			// The names come from the flush, not from the listing a pass
			// filters: one a pass would never export is not exported here.
			if x, ok := s.source.(interface{ Exportable(Measurement) bool }); ok && !x.Exportable(m) {
				s.forgetIngest(key)
				continue
			}
			job := s.prepareIngest(m, key)
			switch job.kind {
			case ingestCovered:
				s.noteIngestCovered(key, job.takenAt, outcomeUnchanged, false, nil)
				s.afterIngest(m, key, outcomeUnchanged, nil, time.Since(job.began))
			case ingestAppend:
				batch = append(batch, job)
			default: // a pass
				if job.kind == ingestFallback {
					s.noteAppendFallback(job)
				}
				if passes >= maxPasses {
					// This service has run its passes: the request is due
					// at once, for the next one.
					s.ingestDueNow(m, key)
					stop = true
				} else {
					pass = job
				}
			}
		}
		if took == 0 {
			return
		}
		s.runAppends(ctx, batch)
		// Every table's bookkeeping, before anything else runs: a pass, a
		// drop or a request of another kind must find the scheduler's state
		// describing what the appends did.
		var passesDue []*ingestJob
		for _, job := range batch {
			outcome, needsPass := s.finishAppend(job)
			if needsPass {
				s.noteAppendFallback(job)
				passesDue = append(passesDue, job)
				continue
			}
			s.afterIngest(job.m, job.key, outcome, nil, job.took)
		}
		if pass != nil {
			passesDue = append(passesDue, pass)
		}
		for i, job := range passesDue {
			if ctx.Err() != nil || !s.gateOpen() {
				// Shutting down, or another node exports now (an append that
				// lost the writer role is one way to get here): its passes
				// converge the tables.
				if ctx.Err() == nil {
					s.dropIngest()
				}
				return
			}
			if passes >= maxPasses {
				// No more passes in this service (between two measurements
				// of a timed pass, one at most): they are due at once.
				for _, later := range passesDue[i:] {
					s.ingestDueNow(later.m, later.key)
				}
				return
			}
			passes++
			if i > 0 && s.otherRequestsQueued() {
				s.serviceDrops(ctx)
				s.serviceRequests(ctx)
			}
			mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
			began := time.Now()
			outcome, err := s.reconcileOneRecovered(mctx, job.m, job.key)
			cancel()
			s.afterIngest(job.m, job.key, outcome, err, job.took+time.Since(began))
		}
		if stop || passes >= maxPasses {
			return
		}
	}
}

// runAppends runs the batch's appends, each on its own goroutine with its
// own exporter state and deadline, and returns when every one has ended,
// whatever ctx says: the caller's bookkeeping, and a shutdown that closes
// the catalog after the loop exits, both need them finished.
func (s *Scheduler) runAppends(ctx context.Context, batch []*ingestJob) {
	if len(batch) == 0 {
		return
	}
	fileReads := maxInt(2, batchFileReads/len(batch))
	run := func(job *ingestJob) {
		actx, cancel := context.WithTimeout(ctx, appendTimeout)
		defer cancel()
		s.runAppend(actx, job, s.exporter.forAppend(), fileReads)
	}
	if len(batch) == 1 {
		run(batch[0])
		return
	}
	var wg sync.WaitGroup
	for _, job := range batch {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run(job)
		}()
	}
	wg.Wait()
}

// ingestDueNow puts back a taken ingest request so that the next service
// takes it first: one whose reconcile pass could not run in this service.
func (s *Scheduler) ingestDueNow(m Measurement, key string) {
	s.reqMu.Lock()
	if _, queued := s.ingestDue[key]; !queued {
		s.ingestDue[key] = ingestRequest{m: m, due: time.Now()}
	}
	s.reqMu.Unlock()
}
