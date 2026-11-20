package iceberg

import (
	"context"
	"errors"
	"fmt"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"golang.org/x/sync/errgroup"
)

// The scheduler's side of the append path (#1751; the commit is append.go).
//
// An ingest flush reports each file it registers (RequestIngestAppend). When
// the table's ingest request comes due, the files reported for it are added
// in one commit, with no listing of the measurement and no read of the
// table's manifests. Anything that is not plainly an append runs the
// reconcile pass the request ran before, which re-derives the table from a
// listing.
//
// The append commits with iceberg-go's duplicate check off, so THIS file is
// what keeps a file from being added twice:
//
//   - One export of a table at a time, and never a pass while an append
//     runs. Appends of different tables run at once on other goroutines
//     (#1778, scheduler_ingest_batch.go), and so do reconcile passes of
//     different tables (#1786, scheduler_pass.go), in batches of one kind:
//     the loop goroutine starts a batch and waits for all of it before it
//     does anything else, a table is in a batch once at most, and everything
//     below is read before a batch starts and written after it ends, by the
//     loop.
//   - A table takes appends only between reconcile passes that settled
//     (appendOK). A settled pass means every listed file is in the table, and
//     that every rule a listing enforces as a whole held: no hidden data
//     files, no listing that kept moving, no table still to create.
//   - recentAdds holds, per table, every recent file that is in the table or
//     that a pass has seen: the files a pass listed and the files an append
//     added. A reported file found there is dropped. A pass can list a file
//     between its write and its registration (an hour file that waits for the
//     rest of its flush is registered minutes after it is stored), and that
//     file is then reported after the pass that added it.
//   - Only files named after this process started, and less than
//     appendMaxFileAge ago, are appended; recentAdds keeps an entry for
//     recentAddsKeep, three times as long. An older file is the pass's.
//   - A pass drops the pending files reported before it started, whatever its
//     outcome: one that reached its listing listed after they were stored,
//     and one that did not leaves the table taking no append.
//
// After an append the cached fingerprint no longer describes the table, so it
// is replaced by one no listing produces: the next pass of the measurement
// always runs. That pass removes an appended file that has since left storage,
// and expires, prunes and sweeps what the appends left behind (finishPass).

const (
	// appendMaxFiles is the most files one append commits. More than that
	// waiting is a backlog (the export was off, or far behind), and a pass
	// registers a backlog in chunks, within its own time budget.
	appendMaxFiles = 256
	// appendMaxPending bounds the files remembered per table between two
	// services of its request; past it the table takes a pass.
	appendMaxPending = 4 * appendMaxFiles
	// appendMaxFileAge and recentAddsKeep: see the file comment.
	appendMaxFileAge = 30 * time.Minute
	recentAddsKeep   = 90 * time.Minute
	// appendedFingerprint replaces a table's cached fingerprint after an
	// append. fingerprint() returns a hex digest, so no listing equals it.
	appendedFingerprint = "appended since the last pass"
	// appendMaxBetweenPasses is the most appends a table takes before its
	// ingest request runs a pass instead. Appends expire, prune and sweep
	// nothing; the timed pass does, every iceberg.reconcile_interval, and
	// this bounds what a table carries when that interval is long: its
	// snapshots, published versions and manifests.
	appendMaxBetweenPasses = 120
)

// pendingFile is one file an ingest flush reported and no export has covered.
type pendingFile struct {
	key  string // storage key
	size int64  // bytes the flush wrote; 0 when the reporter did not say
	at   time.Time
}

// pendingFiles is a table's reported files; overflow means some were not
// remembered (appendMaxPending) and only a pass can find them.
type pendingFiles struct {
	files    []pendingFile
	overflow bool
	// overflowAt is when a file was last left out of files.
	overflowAt time.Time
}

// RequestIngestAppend reports a file an ingest flush has stored and kept, and
// asks for an export of its table as RequestIngestReconcile does. When the
// request comes due the reported files are appended to the table; without the
// append path (iceberg.ingest_append_enabled off) it is RequestIngestReconcile.
// It never blocks and is safe on a nil scheduler.
func (s *Scheduler) RequestIngestAppend(database, measurement, storageKey string, sizeBytes int64) {
	if s == nil || s.ingestInterval <= 0 || database == "" || measurement == "" {
		return
	}
	// The file and the report under one lock and one clock reading: the loop
	// takes the files and judges which reports that covered by time, and a
	// report dated after its own file was taken would ask for a pass.
	now := time.Now()
	s.reqMu.Lock()
	if s.appendEnabled && storageKey != "" {
		key := database + "\x00" + measurement
		p := s.appendPending[key]
		if len(p.files) >= appendMaxPending {
			p.overflow, p.overflowAt = true, now
		} else {
			p.files = append(p.files, pendingFile{key: storageKey, size: sizeBytes, at: now})
		}
		s.appendPending[key] = p
	}
	queued := s.requestIngestLocked(database, measurement, now)
	s.reqMu.Unlock()
	if queued {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// takeAppendPending removes and returns the files reported for the table,
// and the time it took them: every report before it has its file among them.
func (s *Scheduler) takeAppendPending(key string) (pendingFiles, time.Time) {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	p := s.appendPending[key]
	delete(s.appendPending, key)
	return p, time.Now()
}

// dropAppendPendingBefore forgets the files reported for the table before t:
// a pass that started at t listed after they were stored.
func (s *Scheduler) dropAppendPendingBefore(key string, t time.Time) {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	p, ok := s.appendPending[key]
	if !ok {
		return
	}
	kept := p.files[:0]
	for _, f := range p.files {
		if !f.at.Before(t) {
			kept = append(kept, f)
		}
	}
	// A file left out of the list (overflow) after the pass started was not
	// listed by it either: the overflow stands, and the table takes a pass.
	p.files = kept
	p.overflow = p.overflow && !p.overflowAt.Before(t)
	if len(p.files) == 0 && !p.overflow {
		delete(s.appendPending, key)
		return
	}
	s.appendPending[key] = p
}

// forgetAppend drops everything the append path remembers about a table.
func (s *Scheduler) forgetAppend(key string) {
	s.reqMu.Lock()
	delete(s.appendPending, key)
	s.reqMu.Unlock()
	delete(s.appendOK, key)
	delete(s.recentAdds, key)
	delete(s.appendFallback, key)
	delete(s.appendsSince, key)
}

// noteRecent records storage keys as in the table (or seen by a pass), and
// drops the entries that have aged out.
func (s *Scheduler) noteRecent(key string, storageKeys []string) {
	now := time.Now()
	set := s.recentAdds[key]
	for k, at := range set {
		if now.Sub(at) > recentAddsKeep {
			delete(set, k)
		}
	}
	if len(storageKeys) == 0 {
		return
	}
	if set == nil {
		set = make(map[string]time.Time)
		s.recentAdds[key] = set
	}
	for _, k := range storageKeys {
		set[k] = now
	}
}

// notePassEnded is the append path's bookkeeping after a reconcile pass of the
// measurement: whether the table may take appends, and which reported files
// the pass covered.
func (s *Scheduler) notePassEnded(key string, startedAt time.Time, outcome reconcileOutcome, err error) {
	if !s.appendEnabled || s.ingestInterval <= 0 {
		return
	}
	s.dropAppendPendingBefore(key, startedAt)
	_, settled := s.state[key]
	switch {
	case err != nil || !settled:
		// Failed, busy, partial or unsettled (reconcileOne drops the state for
		// each), or a listing error that kept it: the table is not known to
		// hold what a listing would.
		delete(s.appendOK, key)
	case outcome == outcomeReconciled:
		s.appendOK[key] = true
		delete(s.appendFallback, key)
		delete(s.appendsSince, key)
	}
	// outcomeUnchanged with the state kept: the listing is the one of the
	// last settled pass, so whatever appendOK says still holds.
}

// appendable splits the reported files into the ones an append may add and a
// reason it may not run at all ("" when it may).
func (s *Scheduler) appendable(key string, p pendingFiles, now time.Time) (keys []string, reason string) {
	if p.overflow {
		return nil, "more files were reported than are remembered"
	}
	recent := s.recentAdds[key]
	seen := make(map[string]struct{}, len(p.files))
	for _, f := range p.files {
		if _, dup := seen[f.key]; dup {
			continue
		}
		seen[f.key] = struct{}{}
		if _, in := recent[f.key]; in {
			continue // in the table already, or a pass has it
		}
		name := path.Base(f.key)
		t, ok := storage.IngestFileTime(name)
		if !ok || storage.StripRewriteSuffix(name) != name {
			return nil, "a reported file is not an ingest file"
		}
		if t.Before(s.started) {
			return nil, "a reported file was written before this process started"
		}
		if now.Sub(t) >= appendMaxFileAge {
			return nil, "a reported file was written too long ago"
		}
		keys = append(keys, f.key)
	}
	if len(keys) > appendMaxFiles {
		return nil, "too many files waiting"
	}
	return keys, ""
}

// An ingest request is served in three steps, so that the middle one can run
// for several tables at once (#1778, scheduler_ingest_batch.go):
//
//   - prepareIngest, on the loop goroutine: what the request is. Every read of
//     the scheduler's maps that decides it happens here.
//   - runAppend, on any goroutine: the storage and catalog work of an append.
//     It uses the scheduler's source, exporter, gate and logger, which do not
//     change after construction, and nothing else of the scheduler.
//   - finishAppend, on the loop goroutine: every write to the scheduler's maps.
//
// One table is in at most one of these at a time, and the loop runs no
// reconcile pass while any append is running, so the file comment's argument
// holds as it did with one export at a time.

// ingestKind is what serving an ingest request takes.
type ingestKind int

const (
	ingestPass     ingestKind = iota // a reconcile pass of the measurement
	ingestFallback                   // a pass, because the reported files cannot be appended (reason)
	ingestCovered                    // nothing: every reported file is in the table
	ingestAppend                     // an append of keys
)

// ingestJob is one ingest request taken from the queue.
type ingestJob struct {
	m     Measurement
	key   string
	kind  ingestKind
	began time.Time
	// An append: the files, and when they were taken from the pending list.
	resolver appendResolver
	keys     []string
	sizes    map[string]int64
	takenAt  time.Time
	// reason is why a pass is needed: set by prepareIngest (ingestFallback)
	// or by runAppend.
	reason string
	// What runAppend did.
	done  []string // storage keys now in the table, or left out for good
	added int
	took  time.Duration
}

// prepareIngest decides how the table's due ingest request is served. Loop
// goroutine only.
func (s *Scheduler) prepareIngest(m Measurement, key string) *ingestJob {
	job := &ingestJob{m: m, key: key, began: time.Now()}
	// With the append path off no table is ever marked as taking appends
	// (notePassEnded), so every request is the pass.
	resolver, canResolve := s.source.(appendResolver)
	_, settled := s.state[key]
	if !canResolve || !settled || !s.appendOK[key] || s.appendsSince[key] >= appendMaxBetweenPasses {
		// The pending files stay: the pass drops the ones it covers.
		return job
	}
	pending, takenAt := s.takeAppendPending(key)
	if len(pending.files) == 0 && !pending.overflow {
		// A request with no file behind it (the trigger without a report).
		return job
	}
	job.takenAt = takenAt
	keys, reason := s.appendable(key, pending, takenAt)
	if reason != "" {
		job.kind, job.reason = ingestFallback, reason
		return job
	}
	if len(keys) == 0 {
		// Everything reported is in the table already.
		job.kind = ingestCovered
		return job
	}
	job.kind, job.resolver, job.keys = ingestAppend, resolver, keys
	job.sizes = make(map[string]int64, len(pending.files))
	for _, f := range pending.files {
		job.sizes[f.key] = f.size
	}
	return job
}

// finishAppend records what an append did, and returns the outcome and
// whether a reconcile pass must follow (job.reason says why). Loop goroutine
// only.
func (s *Scheduler) finishAppend(job *ingestJob) (outcome reconcileOutcome, needsPass bool) {
	if job.reason != "" {
		return outcomeUnchanged, true
	}
	s.noteRecent(job.key, job.done)
	outcome = outcomeUnchanged
	if job.added > 0 {
		// The cached listing no longer describes the table: make the next
		// pass of the measurement run (see the file comment).
		s.markAppended(job.key)
		s.appendsSince[job.key]++
		metrics.Get().AddIcebergIngestAppend(job.added)
		outcome = outcomeReconciled
	}
	s.noteIngestCovered(job.key, job.takenAt, outcome, job.added > 0, nil)
	return outcome, false
}

// noteAppendFallback is the bookkeeping for reported files that were not
// appended, before the reconcile pass that follows. The pass decides what the
// table holds. Until one settles, the table takes no append. And the cached
// listing may no longer be the table: an append that failed can still have
// committed (a publish that failed, a commit whose answer was lost). If the
// pass then fails at its listing the cache is kept, and it must not match a
// later listing. Loop goroutine only.
func (s *Scheduler) noteAppendFallback(job *ingestJob) {
	delete(s.appendOK, job.key)
	s.markAppended(job.key)
	metrics.Get().IncIcebergIngestAppendFallback()
	if s.appendFallback[job.key] != job.reason {
		// Once per reason per table: a table that keeps falling back would
		// otherwise say so at every interval.
		s.appendFallback[job.key] = job.reason
		s.logger.Info().Str("database", job.m.Database).Str("measurement", job.m.Measurement).Str("reason", job.reason).
			Msg("Iceberg: ingest files not appended; running a reconcile pass of the measurement instead")
	}
}

// exportIngest serves a due ingest request for the table on the loop
// goroutine, start to end: an append of the files reported for it when that
// is possible, a reconcile pass otherwise.
func (s *Scheduler) exportIngest(ctx context.Context, m Measurement, key string) (reconcileOutcome, error) {
	job := s.prepareIngest(m, key)
	switch job.kind {
	case ingestPass:
		return s.reconcileOneRecovered(ctx, m, key)
	case ingestCovered:
		s.noteIngestCovered(key, job.takenAt, outcomeUnchanged, false, nil)
		return outcomeUnchanged, nil
	case ingestAppend:
		s.runAppend(ctx, job, s.exporter, unionSchemaConcurrency)
		outcome, needsPass := s.finishAppend(job)
		if !needsPass {
			return outcome, nil
		}
	}
	s.noteAppendFallback(job)
	return s.reconcileOneRecovered(ctx, m, key)
}

// markAppended replaces the table's cached fingerprint with one no listing
// produces, so its next pass runs whatever the listing is: after an append.
// (A table whose published versions wait out their minimum age gets its pass
// when they are due: measurementState.pruneAt.)
func (s *Scheduler) markAppended(key string) {
	if st, ok := s.state[key]; ok {
		st.fingerprint = appendedFingerprint
		s.state[key] = st
	}
}

// appendResolver is the source's per-file resolution (StorageWalkSource.AppendRefs).
type appendResolver interface {
	AppendRefs(ctx context.Context, m Measurement, keys []string, sizes map[string]int64) ([]FileRef, bool, error)
}

// runAppend appends the job's files with exp and records in the job what it
// did, or the reason a reconcile pass is needed. A panic is a reason too, as
// in reconcileOneRecovered, and is recovered here: this may be its own
// goroutine. fileReads bounds the footers read at once.
//
// It touches nothing of the scheduler but its source, gate and logger (see
// prepareIngest).
func (s *Scheduler) runAppend(ctx context.Context, job *ingestJob, exp *Exporter, fileReads int) {
	m := job.m
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Str("database", m.Database).Str("measurement", m.Measurement).
				Str("panic", fmt.Sprint(r)).Str("stack", string(debug.Stack())).
				Msg("Iceberg append: recovered a panic")
			job.done, job.added, job.reason = nil, 0, fmt.Sprintf("the append panicked: %v", r)
		}
		job.took = time.Since(job.began)
	}()
	// What this export reads of the new files is read once (export_reads.go).
	ctx = withExportReads(ctx)
	refs, ok, err := job.resolver.AppendRefs(ctx, m, job.keys, job.sizes)
	if err != nil {
		job.reason = err.Error()
		return
	}
	if !ok {
		job.reason = "this storage layout has no per-file check"
		return
	}
	// The footers, read here and not in appendFiles so that a file that has
	// left storage since it was reported is left out, as the stat in
	// AppendRefs leaves one out on local storage (#1778). When the append
	// then cannot run, the pass that follows reads them again.
	refs, sc, err := s.presentFileSchemas(ctx, refs, fileReads)
	if err != nil {
		job.reason = err.Error()
		return
	}
	if len(refs) == 0 {
		return // gone or left out since they were reported: nothing to add
	}
	readSchemas := func(context.Context, []FileRef) (ArcSchema, error) { return sc, nil }
	res, err := exp.appendFiles(ctx, m.Database, m.Measurement, refs, readSchemas, s.canRun)
	if err != nil {
		if errors.Is(err, errNeedsFullPass) {
			job.reason = "the table does not exist or lacks a column of the new files"
		} else {
			job.reason = err.Error()
		}
		return
	}
	// Added, and left out for good (rows across a UTC day): neither is
	// offered again.
	keyOf := make(map[string]string, len(refs))
	for _, f := range refs {
		keyOf[f.PhysicalPath] = f.Key
	}
	job.done = make([]string, 0, len(res.added)+len(res.skipped))
	for _, p := range res.added {
		job.done = append(job.done, keyOf[p])
	}
	for _, p := range res.skipped {
		job.done = append(job.done, keyOf[p])
	}
	job.added = len(res.added)
	if len(res.skipped) > 0 {
		s.logger.Warn().Str("database", m.Database).Str("measurement", m.Measurement).
			Int("skipped", len(res.skipped)).Str("sample", strings.Join(res.skipped[:min(3, len(res.skipped))], ", ")).
			Msg("Iceberg: ingest files left out of the table (their rows span more than one UTC day); the reconcile pass reports them")
	}
}

// presentFileSchemas reads the schemas of reported files, as
// unionFileSchemas does, leaving out the files the store no longer has. It
// returns the files left and their merged schema.
func (s *Scheduler) presentFileSchemas(ctx context.Context, files []FileRef, limit int) ([]FileRef, ArcSchema, error) {
	schemas := make([]ArcSchema, len(files))
	gone := make([]bool, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, limit))
	for i, f := range files {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			sc, err := s.source.FileSchema(gctx, f)
			if err != nil {
				if storage.IsNotFound(err) {
					gone[i] = true
					return nil
				}
				return fmt.Errorf("file %q: %w", f.PhysicalPath, err)
			}
			schemas[i] = sc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, ArcSchema{}, err
	}
	present := make([]FileRef, 0, len(files))
	var out ArcSchema
	for i, sc := range schemas {
		if gone[i] {
			continue
		}
		present = append(present, files[i])
		// No column sources, unlike unionFileSchemas: a conflict here makes
		// the export a pass, and the pass is what names the files (#1744).
		var err error
		if out, err = MergeSchemas(out, sc); err != nil {
			return nil, ArcSchema{}, markConflict(err, conflictWithFiles)
		}
	}
	return present, out, nil
}
