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
// Reconcile passes are never part of a batch of appends. A request that
// needs one (a table's first export, a fallback, the pass after
// appendMaxBetweenPasses appends) has it run after the batch: the passes one
// batch of appends leaves run at once among themselves (scheduler_pass.go,
// #1786), which in practice is that batch's fallbacks, since the collection
// above stops at the first request that needs a pass.

// appendTimeout bounds one append of a batch. The loop waits for the whole
// batch, so a wedged append holds the others' bookkeeping and every other
// request for this long. An append is a handful of requests plus one read per
// new file, appendFileReads at a time: the most files one append takes
// (appendMaxFiles, 256) at 200 ms a read is 13 s. One that runs out of time
// falls back to a reconcile pass with its own budget (measurementTimeout).
var appendTimeout = 60 * time.Second // a var for tests

// batchFileReads is how many footers the appends of one batch aim to read at
// once, all together: the object store client keeps 16 idle connections per
// host, and more than that at once means opening new ones. appendFileReads
// is the least one append reads at once whatever the batch's size, so a full
// batch of 8 reads up to 32 at once and one of 32 up to 128.
const (
	batchFileReads  = 16
	appendFileReads = 4
)

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
		took := 0
		for len(batch) < s.ingestConcurrency && served < maxRequests && pass == nil {
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
				// Within its budget: the service returns as soon as it has
				// run maxPasses passes (below), so one is left here.
				pass = job
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
		for len(passesDue) > 0 {
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
				for _, later := range passesDue {
					s.ingestDueNow(later.m, later.key)
				}
				return
			}
			// What was asked for while the batch or the last passes ran: a
			// compaction's request must not wait behind these passes.
			if s.otherRequestsQueued() {
				s.serviceDrops(ctx)
				s.serviceRequests(ctx)
			}
			// Up to passConcurrency of them at once (scheduler_pass.go),
			// within this service's budget of passes.
			n := min(len(passesDue), s.passConcurrency, maxPasses-passes)
			group := passesDue[:n]
			passesDue = passesDue[n:]
			passes += n
			if n == 1 {
				job := group[0]
				mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
				began := time.Now()
				outcome, err := s.reconcileOneRecovered(mctx, job.m, job.key)
				cancel()
				s.afterIngest(job.m, job.key, outcome, err, job.took+time.Since(began))
				continue
			}
			// Prepared after the services above: one of them can have
			// reconciled a table of the group.
			jobs := make([]*passJob, n)
			for i, job := range group {
				jobs[i] = s.preparePass(job.m, job.key)
			}
			s.runPasses(ctx, jobs)
			for i, pj := range jobs {
				s.finishPass(pj)
				s.afterIngest(pj.m, pj.key, pj.outcome, pj.err, group[i].took+pj.took)
			}
		}
		if passes >= maxPasses {
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
	fileReads := max(appendFileReads, batchFileReads/len(batch))
	run := func(job *ingestJob) {
		actx, cancel := context.WithTimeout(ctx, appendTimeout)
		defer cancel()
		s.runAppend(actx, job, s.exporter.forExport(), fileReads)
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

// ingestDueNow puts back a taken ingest request, due at once: one whose
// reconcile pass could not run in this service. A flush that reported a file
// for the table meanwhile has queued a request one interval out (the spacing
// was stamped when this one was taken); that one is brought forward, or the
// table, which takes no append until its pass, would wait a whole interval.
func (s *Scheduler) ingestDueNow(m Measurement, key string) {
	now := time.Now()
	s.reqMu.Lock()
	if r, queued := s.ingestDue[key]; !queued || r.due.After(now) {
		s.ingestDue[key] = ingestRequest{m: m, due: now}
	}
	s.reqMu.Unlock()
}
