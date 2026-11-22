package iceberg

import (
	"context"
	"errors"
	"fmt"
	"github.com/basekick-labs/arc/internal/metrics"
	"path"
	"runtime/debug"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// Reconcile passes of several tables at once (#1786).
//
// A reconcile pass of a table is that table's own work: its listing, its
// metadata and manifests, its commits, its prune and sweep. On object storage
// it is about half a second of waiting on requests, and the timed pass used
// to walk the tables one after another: 200 tables were minutes, every
// iceberg.reconcile_interval, during which their appends waited.
//
// The shape is the one the appends have (scheduler_ingest_batch.go). The loop
// goroutine takes a batch of tables, copies what the scheduler remembers of
// each (preparePass), runs the passes at once and WAITS for all of them, and
// only then records what each did (finishPass), one after another. A pass
// running on another goroutine has a passRunner and a passJob, not the
// scheduler: it cannot reach the scheduler's maps.
//
//   - A table is in one export at a time: a batch holds distinct tables, and
//     the loop runs nothing else until the batch has ended.
//   - What the scheduler remembers (state, recentAdds, appendOK, the pending
//     files) is read before the passes start and written after they end.

// stateOp is what a pass decided about its table's cached state.
type stateOp int

const (
	stateKeep stateOp = iota // leave it as it was
	stateSet                 // replace it with passJob.newState
	stateDrop                // forget it: the next pass starts from the table
)

// passJob is one reconcile pass of one table: what the scheduler remembered
// of it when the pass started, and what the pass did.
type passJob struct {
	m   Measurement
	key string
	// prev is the scheduler's cached state of the table and cached whether
	// there was one, copied by the loop before the pass. Its maps are only
	// read: nothing writes a measurementState after it is built.
	prev      measurementState
	cached    bool
	startedAt time.Time
	// drops is the table's stamp (Exporter.tableDropStamp), read by the pass
	// once it holds the database and again after it settled the table
	// against its database's drop record.
	drops uint64

	// What the pass did.
	op          stateOp
	newState    measurementState
	recent      []string // listed files an append could still be offered (noteRecent)
	droppedCold bool     // a cold file vanished: the cold listing is stale
	commits     int
	outcome     reconcileOutcome
	err         error
	took        time.Duration
}

func (j *passJob) setState(st measurementState) {
	st.drops = j.drops
	j.op, j.newState = stateSet, st
}
func (j *passJob) dropState() { j.op = stateDrop }

// passRunner is what a reconcile pass uses besides its job and its exporter.
// None of it changes after the scheduler is built.
type passRunner struct {
	source FileSetSourceWithSchema
	canRun func() bool
	logger zerolog.Logger
	// fileReads bounds the footers one pass reads at once.
	fileReads int
}

// passRunner returns the runner of a pass that reads at most fileReads
// footers at once.
func (s *Scheduler) passRunner(fileReads int) *passRunner {
	return &passRunner{source: s.source, canRun: s.canRun, logger: s.logger, fileReads: fileReads}
}

// fileExists is the source's existence check for the reconcile, or nil when it has none.
func (r *passRunner) fileExists() func(context.Context, FileRef) (bool, error) {
	if fe, ok := r.source.(fileExister); ok {
		return fe.FileExists
	}
	return nil
}

// unionFileSchemas derives the union schema of files through the source's FileSchema, reading
// footers in parallel (bounded, as UnionSchema does) and folding them in the files' order so
// column order and conflict detection do not depend on scheduling.
func (r *passRunner) unionFileSchemas(ctx context.Context, files []FileRef) (ArcSchema, error) {
	schemas := make([]ArcSchema, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, r.fileReads))
	for i, f := range files {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			release := acquireRead(gctx)
			sc, err := r.source.FileSchema(gctx, f)
			release()
			if err != nil {
				return fmt.Errorf("file %q: %w", f.PhysicalPath, err)
			}
			schemas[i] = sc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return ArcSchema{}, err
	}
	var out ArcSchema
	for i, sc := range schemas {
		// Each column remembers the file it came from, so a conflict, here or when the caller
		// merges this schema with the table's, names the files (#1744).
		sc.sources = make(map[string]string, len(sc.Fields))
		for _, f := range sc.Fields {
			sc.sources[f.Name] = fileRefKey(files[i])
		}
		var err error
		if out, err = MergeSchemas(out, sc); err != nil {
			return ArcSchema{}, markConflict(err, conflictWithFiles)
		}
	}
	return out, nil
}

// recentIngestKeys returns the listed files an append could still be offered:
// ingest files written less than recentAddsKeep ago.
func recentIngestKeys(files []FileRef) []string {
	now := time.Now()
	var recent []string
	for _, f := range files {
		if t, ok := storage.IngestFileTime(path.Base(f.Key)); ok && now.Sub(t) < recentAddsKeep {
			recent = append(recent, f.Key)
		}
	}
	return recent
}

// preparePass copies what the scheduler remembers of the table for a pass.
// Loop goroutine only, and right before the pass runs: anything the loop
// does to the table's state in between would be lost to the pass.
func (s *Scheduler) preparePass(m Measurement, key string) *passJob {
	job := &passJob{m: m, key: key, startedAt: time.Now()}
	job.prev, job.cached = s.state[key]
	return job
}

// applyPassEffects records what a pass did that holds whatever became of
// it: the commits it made, and a cold listing it found stale. Loop goroutine
// only.
func (s *Scheduler) applyPassEffects(job *passJob) {
	s.passCommits = job.commits
	if job.droppedCold {
		if ci, ok := s.source.(coldInvalidator); ok {
			ci.InvalidateCold(job.m.Database)
		}
	}
}

// applyPassState records what a pass that returned decided about its table's
// cached state, and the files it listed. Loop goroutine only.
func (s *Scheduler) applyPassState(job *passJob) {
	// What this pass has seen is not offered to an append again (#1751).
	if s.appendEnabled && s.ingestInterval > 0 {
		s.noteRecent(job.key, job.recent)
	}
	switch job.op {
	case stateSet:
		s.state[job.key] = job.newState
	case stateDrop:
		delete(s.state, job.key)
	}
	if job.op != stateKeep {
		s.reportLeftOut()
	}
}

// reportLeftOut sets arc_iceberg_files_left_out to the files left out of the tables whose
// last reconcile settled. Loop goroutine only.
func (s *Scheduler) reportLeftOut() {
	n := 0
	for _, st := range s.state {
		n += st.leftOut
	}
	metrics.Get().SetIcebergFilesLeftOut(n)
}

// finishPass records a pass that ran on another goroutine: everything
// reconcileOneRecovered does around a pass on the loop goroutine. Loop
// goroutine only.
func (s *Scheduler) finishPass(job *passJob) {
	s.applyPassEffects(job)
	s.applyPassState(job)
	s.notePassEnded(job.key, job.startedAt, job.outcome, job.err)
	s.noteIngestCovered(job.key, job.startedAt, job.outcome, job.commits > 0, job.err)
}

// run runs the job's pass with exp and records the result in the job. A
// panic is the table's error, as in reconcileOneRecovered, and leaves its
// cached state as it was.
func (r *passRunner) run(ctx context.Context, job *passJob, exp *Exporter) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error().Str("database", job.m.Database).Str("measurement", job.m.Measurement).
				Str("panic", fmt.Sprint(rec)).Str("stack", string(debug.Stack())).
				Msg("Iceberg reconcile: recovered a panic")
			// A guard: reconcile sets these last, with nothing after that can
			// panic. A pass that panicked leaves its table's cached state alone.
			job.op = stateKeep
			job.outcome, job.err = outcomeUnchanged, fmt.Errorf("reconcile panicked: %v", rec)
		}
		job.took = time.Since(job.startedAt)
	}()
	job.outcome, job.err = r.reconcile(ctx, job, exp)
}

// runPasses runs the batch's passes, each on its own goroutine with its own
// exporter state and deadline, and returns when every one has ended,
// whatever ctx says (runAppends). The tables must be distinct.
func (s *Scheduler) runPasses(ctx context.Context, batch []*passJob) {
	if len(batch) == 0 {
		return
	}
	// The storage reads the whole batch has in flight, and the tails it
	// holds, are shared (batchReads): a pass with a backlog beside tables
	// with nothing new reads as fast as it would alone.
	runner := s.passRunner(unionSchemaConcurrency)
	shared := newBatchReads()
	run := func(job *passJob) {
		pctx, cancel := context.WithTimeout(withBatchReads(ctx, shared), measurementTimeout)
		defer cancel()
		runner.run(pctx, job, s.exporter.forExport())
	}
	// A table twice in a batch must not be two exports of it at once. No
	// caller builds such a batch (a listing names a measurement once, and a
	// table has one ingest request); if one ever does, the repeats run after
	// the batch, one by one, as they would have before passes ran at once.
	seen := make(map[string]struct{}, len(batch))
	var repeats []*passJob
	var wg sync.WaitGroup
	for _, job := range batch {
		if _, dup := seen[job.key]; dup {
			repeats = append(repeats, job)
			continue
		}
		seen[job.key] = struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			run(job)
		}()
	}
	wg.Wait()
	for _, job := range repeats {
		s.logger.Error().Str("database", job.m.Database).Str("measurement", job.m.Measurement).
			Msg("Iceberg reconcile: a table was twice in one batch of passes; its second pass ran after the batch")
		run(job)
	}
}

// reconcile is one reconcile pass of the job's table: list, compare with
// what is cached or in the table, and have the exporter register the
// difference. It records in the job what the scheduler should remember.
func (r *passRunner) reconcile(ctx context.Context, job *passJob, exp *Exporter) (outcome reconcileOutcome, err error) {
	m := job.m
	// From before the listing to the end: the database's tables are not
	// dropped while this pass works on one (database_lock.go, #1789). While
	// they are being dropped this pass gives way at once, like one whose
	// files are changing under it: nothing was read or committed, what is
	// cached is kept, and the table is tried again soon.
	release, err := exp.holdDatabase(m.Database)
	if err != nil {
		return outcomeBusy, err
	}
	defer release()
	// The same on a shared bucket, for a drop running on another node, and a
	// table that drop meant to remove is removed: before anything of the
	// table is read, and only in a pass that will read it, not in one that
	// finds its files unchanged (shared_drop.go, #1792).
	// A table it removes is gone from what was cached of it too: the stamp
	// moved, and the pass starts from the table as on first sight, not from
	// the removed table's schema and footers.
	settle := func() (reconcileOutcome, error) {
		err := exp.settleDroppedTable(ctx, m.Database, m.Measurement, r.canRun)
		if errors.Is(err, errDatabaseBeingDropped) {
			return outcomeBusy, err
		}
		if err != nil {
			return outcomeUnchanged, err
		}
		if stamp := exp.tableDropStamp(m.Database, m.Measurement); stamp != job.drops {
			job.drops = stamp
			job.prev, job.cached = measurementState{}, false
		}
		return outcomeUnchanged, nil
	}
	// What the scheduler remembered of the table was copied before this pass
	// had the database. If the database was dropped since it was cached (at
	// any time before the loop got to forget it, a drop that ended a moment
	// ago included), it describes a table that is gone: its schema
	// and its files' footers are not those of a table recreated under the
	// name, and a pass that trusted them built the new table with the old
	// columns. Start from the table, as on first sight.
	job.drops = exp.tableDropStamp(m.Database, m.Measurement)
	if job.cached && job.prev.drops != job.drops {
		job.prev, job.cached = measurementState{}, false
	}
	// What this pass reads of the store is not read twice (export_reads.go, #1778).
	ctx = withExportReads(ctx)
	// One backend listing yields both the data-file URIs and the local paths (Files + LocalFiles
	// would list the same prefix twice).
	files, localFiles, err := r.source.FilesAndLocal(ctx, m)
	if err != nil {
		if errors.Is(err, errSourcesMoving) {
			// The listing never held still: nothing was read or committed, and the cached state
			// (if any) still describes the table, so it is kept.
			return outcomeBusy, err
		}
		return outcomeUnchanged, err
	}
	// What this pass has seen is not offered to an append again (#1751).
	job.recent = recentIngestKeys(files)
	if len(files) == 0 {
		// Every data file is gone (retention/compaction/manual delete). We still reach this code
		// because Arc's Delete only os.Remove()s the file — the {db}/{measurement}/… directory
		// tree survives, so Measurements() keeps yielding this measurement. Returning early here
		// would leave the Iceberg table pointing at deleted files forever, and external engines
		// fail on the missing paths. Reconcile the table to EMPTY instead.
		//
		// Fingerprint-gate it like any other state so a permanently-empty measurement (an emptied
		// directory Arc never prunes) is emptied ONCE and skipped every pass after — otherwise
		// every tick re-runs the catalog load + commit and re-logs forever.
		fp := fingerprint(files, localFiles)
		if job.cached && job.prev.unchanged(fp, time.Now()) {
			return outcomeUnchanged, nil
		}
		// Only for a table that already exists: an empty directory that never held data must not
		// mint a zero-column table (EnsureTable would create one with no fields and no partition
		// spec). Schema is irrelevant when emptying — the existing table's schema is preserved.
		if out, err := settle(); err != nil {
			return out, err
		}
		exists, err := exp.tableExists(ctx, m.Database, m.Measurement)
		if err != nil {
			return outcomeUnchanged, err
		}
		if !exists {
			// Cache the negative result too: an empty directory with no table has no
			// catalog state to reconcile, so unchanged passes need no catalog lookup.
			job.setState(measurementState{fingerprint: fp})
			return outcomeUnchanged, nil
		}
		res, err := exp.reconcile(ctx, reconcileRequest{database: m.Database, measurement: m.Measurement, canCommit: r.canRun, dropChecked: true})
		job.commits = res.commits
		if err != nil {
			return outcomeUnchanged, err
		}
		settled := res.settled
		if !res.writerLost && !res.partial {
			r.logger.Info().Str("database", m.Database).Str("measurement", m.Measurement).
				Msg("Iceberg reconcile: all data files gone — table reconciled to empty")
		}
		// Cache the empty state (nil schema/localFiles) so a permanently-empty
		// measurement is emptied once, but only once the discovery files are
		// published — see the equivalent guard on the main path below.
		if !settled {
			job.dropState()
			return outcomeReconciled, nil
		}
		job.setState(measurementState{fingerprint: fp, pruneAt: res.pruneAgainAt})
		return outcomeReconciled, nil
	}
	// Fingerprint the file set. Unchanged fingerprint + a cached schema => guaranteed no-op
	// (same files in the table already), so skip the footer reads and the diff.
	fp := fingerprint(files, localFiles)
	prev, cached := job.prev, job.cached
	if cached && prev.unchanged(fp, time.Now()) {
		return outcomeUnchanged, nil
	}
	if out, err := settle(); err != nil {
		return out, err
	}
	prev, cached = job.prev, job.cached

	// The set changed (or first sight): work out which files' footers are not already accounted
	// for — avoiding an O(N) re-read of every footer every time the set grows, local or remote.
	// The exporter reads those, a chunk at a time, as it registers them. (Removed files can't
	// remove columns: the table schema is a superset and all fields are optional, so dropping a
	// file never narrows the schema.)
	//
	// What is accounted for: with a cache, the files of the last reconcile. Without one (first
	// sight, a restart), the table's own schema and the files its current snapshot holds at
	// their size: a restart then reads no footer for an unchanged table, and on an object store
	// it does not GET the tail of every file the measurement has ever exported.
	base, known := prev.schema, prev.schemaFiles
	if !cached || known == nil {
		tableSc, tableFiles, ok, err := exp.tableSeed(ctx, m.Database, m.Measurement)
		if err != nil {
			return outcomeUnchanged, err
		}
		base, known = ArcSchema{}, nil
		if ok {
			base = tableSc
			known = make(map[string]struct{}, len(tableFiles))
			keyer, _ := r.source.(keyOfURIer)
			for uri, size := range tableFiles {
				key := uri
				if keyer != nil {
					if k, ok := keyer.KeyOf(uri); ok {
						key = k
					}
				}
				known[schemaKey(key, size)] = struct{}{}
			}
		}
	}
	var unread []FileRef
	for _, f := range files {
		if _, ok := known[refSchemaKey(f)]; !ok {
			unread = append(unread, f)
		}
	}

	// The unread footers are NOT all read here. The exporter reads them a chunk at a time, only
	// for the files each commit registers, so a measurement with more new files than fit in one
	// deadline still commits what it gets through (#1686). A file that is unread but already in
	// the table at its size (a KeyOf miss) is never read: the table's schema already covers it.
	res, err := exp.reconcile(ctx, reconcileRequest{
		database:    m.Database,
		measurement: m.Measurement,
		dropChecked: true,
		schema:      base,
		current:     files,
		unread:      unread,
		readSchemas: r.unionFileSchemas,
		canCommit:   r.canRun,
		exists:      r.fileExists(),
	})
	job.commits = res.commits // before the error check: a pass can commit chunks and then fail
	if res.droppedCold {
		// A cold file vanished: its cached cold listing is stale until invalidated. Before the
		// error check: a pass can drop a cold file and then fail a later chunk.
		// Done by the loop once the pass has ended (applyPassEffects), like
		// everything else a pass changes outside its own table (#1786). Nothing
		// in this pass lists the cold tier again.
		job.droppedCold = true
	}
	if err != nil {
		// Drop the entry: this pass may have committed chunks before failing, and a stale entry's
		// schemaFiles would make the next pass re-read their footers. A cold entry seeds from the
		// table instead (tableSeed), which has them.
		job.dropState()
		return outcomeUnchanged, err
	}
	if res.moved {
		// Files vanished and were left for the next pass. Not cached: the table is not the listing.
		job.dropState()
		if res.commits > 0 {
			return outcomeInProgress, nil
		}
		return outcomeBusy, nil
	}
	if res.partial {
		// Not cached, for the same reason: the next pass seeds from the table and continues.
		job.dropState()
		return outcomeInProgress, nil
	}

	// Cache only when the pass fully settled. The snapshot is committed either way, but
	// caching an unsettled pass would gate this measurement out of every later one — and if
	// its file set then goes quiet (the steady state) it would stay unsettled indefinitely.
	// Leaving the entry uncached costs one repeated reconcile per tick until it succeeds.
	//
	// Three things can leave a pass unsettled, and the exporter has already logged which:
	// unpublished reader discovery files, a snapshot expiry that failed (by retention, or the
	// floor of a pass whose removals made the older snapshots unreadable, #1092), or a heal commit (name mapping,
	// retention properties) that failed (#1098). All need the same retry, so this does not name
	// one — a message claiming the hint when the expiry failed would send an operator to the
	// wrong place.
	if res.writerLost {
		// Not a failed step: the exporter logged that the writer role moved; nothing to retry here.
		job.dropState()
		return outcomeReconciled, nil
	}
	if !res.settled {
		job.dropState()
		r.logger.Warn().
			Str("database", m.Database).Str("measurement", m.Measurement).
			Msg("Iceberg reconcile: pass did not fully settle (see the preceding error for which step); will retry next pass")
		return outcomeReconciled, nil
	}

	schemaSet := make(map[string]struct{}, len(files))
	for _, f := range files {
		schemaSet[refSchemaKey(f)] = struct{}{}
	}
	// The table's schema, not a union of footers: every listed file is now in the table, so it
	// covers all of them, and it is exactly what the next pass's EnsureTable compares against.
	// pruneAt: published versions are waiting out iceberg.min_version_age, or
	// the prune could not finish. The table's pass runs again then, whatever
	// the listing is, and not before (measurementState.pruneAt).
	job.setState(measurementState{fingerprint: fp, schema: res.schema, schemaFiles: schemaSet, pruneAt: res.pruneAgainAt, leftOut: res.leftOut})
	return outcomeReconciled, nil
}
