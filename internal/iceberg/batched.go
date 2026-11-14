package iceberg

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
)

// A first registration used to be one all-or-nothing step: read every new file's footer for
// the schema, read every footer again to build its DataFile, then commit once — all inside the
// scheduler's per-measurement deadline. A measurement whose footers took longer than that to read
// never committed anything, and every pass started again from zero (#1686).
//
// reconcile commits new files in chunks instead. Each chunk reads the schemas of its own files,
// evolves the table, builds the DataFiles and commits, so what a pass gets through is durable in
// the table and the next pass starts after it. Progress therefore survives a timeout and a
// restart: a cold scheduler seeds itself from the table (tableSeed) and only the unregistered
// files are read again.
//
// What is NOT chunked is anything a reader must see change atomically. Every removal, every tier
// move (the table's own entry cloned under its new URI), every rewrite (a path whose bytes
// changed) and every new file in a UTC day that also loses a file land in the first commit,
// together. That is what keeps a compaction (N inputs out, one output in, same day) a single
// snapshot, as it was before: split across two, a reader between them would see that day's rows
// missing. Only new files with no removal in their day are chunked, and they are the ones a
// reader has never seen, so registering them a chunk at a time only ever adds rows.

const (
	// dataFileChunkMax is the most new files one chunk registers. A chunk reads up to two
	// footers per file (schema, then DataFile) at unionSchemaConcurrency, so at ~200 ms per
	// object-store round trip 256 files take about 30 s including the commit — several chunks per
	// measurement deadline, and few enough snapshots per pass that retain_snapshots and the
	// manifest collapse are not stressed.
	dataFileChunkMax = 256
	// dataFileChunkMin is the floor the adaptive chunk size shrinks to on a store so slow that a
	// whole chunk does not fit in the time left (see shrinkChunk).
	dataFileChunkMin = 8
	// chunkGrowBelow: a chunk that completes faster than this lets the next one double, back
	// towards dataFileChunkMax.
	chunkGrowBelow = 15 * time.Second
	// defaultCommitReserve is the part of the measurement deadline a chunk's footer reads may
	// not use: it is left for the commit, the discovery files and the end-of-pass expiry, prune
	// and sweep, which are a handful of object-store round trips each.
	defaultCommitReserve = 20 * time.Second
)

// stopWriterLost is the reason a pass stops when canCommit turns false.
const stopWriterLost = "writer role lost"

// reconcileRequest is one measurement's reconcile input.
type reconcileRequest struct {
	database, measurement string
	// schema covers every file in current except those in unread.
	schema ArcSchema
	// current is the measurement's full listing. It is always the whole listing, never the
	// part of it this pass will register: a table path missing from current is REMOVED.
	current []FileRef
	// unread are listed files whose footer schema is not folded into schema yet. They are read
	// a chunk at a time through readSchemas, only when the chunk registers them.
	unread      []FileRef
	readSchemas func(ctx context.Context, files []FileRef) (ArcSchema, error)
	// canCommit is checked immediately before every commit (#939: the writer lease can move
	// during a long pass). nil means always.
	canCommit func() bool
}

// reconcileResult reports what one reconcile did.
type reconcileResult struct {
	// settled: the table equals the listing, the discovery files are published and any history
	// floor owed has landed. Only then may the scheduler cache the fingerprint.
	settled bool
	// partial: new files remain unregistered because the pass ran out of time or lost the
	// writer role. Not an error: what was committed stays, and the next pass continues.
	partial bool
	// registered and remaining count new files (rewrites and tier moves excluded).
	registered, remaining int
	// schema is the table's schema when the pass ended.
	schema ArcSchema
}

// ReconcileMeasurementWithHint is ReconcileMeasurement plus whether the pass fully
// SETTLED: the reader discovery files (version-hint.text, v<N>.metadata.json) were
// published, AND any snapshot-history floor this pass owed actually committed.
//
// The scheduler needs this separately from the error. Both steps are best-effort with
// respect to the committed snapshot, but if either fails the caller must NOT cache the
// measurement's fingerprint, or a file set that then goes quiet is never revisited: the
// hint would stay stale forever, and metadata that names deleted data files would stay
// published (#1092).
//
// The return is true when there was nothing to do for either step, which is the ordinary
// converged case. sc must cover every file in current; a pass that runs out of time (ctx has a
// deadline) commits what it got through and returns false.
func (e *Exporter) ReconcileMeasurementWithHint(ctx context.Context, database, measurement string, sc ArcSchema, current []FileRef) (settled bool, err error) {
	res, err := e.reconcile(ctx, reconcileRequest{database: database, measurement: measurement, schema: sc, current: current})
	return res.settled, err
}

func (e *Exporter) reconcile(ctx context.Context, req reconcileRequest) (reconcileResult, error) {
	ctx = e.withAWS(ctx)
	database, measurement := req.database, req.measurement
	tkey := database + "\x00" + measurement
	canCommit := req.canCommit
	if canCommit == nil {
		canCommit = func() bool { return true }
	}
	// EnsureTable publishes the discovery files itself when it creates or evolves
	// the table, so start from whether those writes succeeded. Otherwise a
	// creation-time failure would be invisible here and the fingerprint would be
	// cached over an unpublished hint.
	e.resetHintFailure()

	sc := req.schema
	unread := make(map[string]FileRef, len(req.unread))
	for _, f := range req.unread {
		unread[f.PhysicalPath] = f
	}
	if len(unread) > 0 && req.readSchemas == nil {
		return reconcileResult{}, fmt.Errorf("reconcile %s.%s: %d files have no schema and no reader", database, measurement, len(unread))
	}

	// A table that does not exist yet is created from sc, and its day(time) partition spec is
	// taken from sc's time column (EnsureTable). Never create it from a schema that has read no
	// footer at all: a table created without its spec cannot get one later, and cloneDataFile
	// refuses entries across specs. So read the first chunk's schemas here, synchronously, and
	// fail the pass (creating nothing) if that does not finish.
	if len(unread) > 0 && !schemaHasField(sc, "time") {
		exists, err := e.tableExists(ctx, database, measurement)
		if err != nil {
			return reconcileResult{}, err
		}
		if !exists {
			first := append([]FileRef(nil), req.unread...)
			sortNewestFirst(first, func(f FileRef) string { return chunkSortKey(f.Key, f.PhysicalPath) })
			first = first[:min(e.chunkSizeFor(tkey), len(first))]
			// Under the chunk budget like any other chunk, and shrunk the same way when it does
			// not fit: a new table on a store too slow for a full first chunk must converge too.
			cctx, cancel := e.chunkContext(ctx)
			add, err := req.readSchemas(cctx, first)
			expired := errors.Is(cctx.Err(), context.DeadlineExceeded)
			cancel()
			if err != nil {
				if ctx.Err() == nil && expired && errors.Is(err, context.DeadlineExceeded) {
					e.shrinkChunk(tkey)
				}
				return reconcileResult{}, err
			}
			if sc, err = MergeSchemas(sc, add); err != nil {
				return reconcileResult{}, err
			}
			for _, f := range first {
				delete(unread, f.PhysicalPath)
			}
		}
	}

	tbl, err := e.EnsureTable(ctx, database, measurement, sc)
	if err != nil {
		return reconcileResult{}, err
	}
	if err := e.fencedErr(); err != nil {
		return reconcileResult{}, err // EnsureTable's create or evolve lost a publish race (adopt.go)
	}
	hintOK := !e.hintFailed()

	want := make(map[string]int64, len(req.current))
	keyOf := make(map[string]string, len(req.current))
	for _, f := range req.current {
		want[f.PhysicalPath] = f.SizeBytes
		keyOf[f.PhysicalPath] = f.Key
	}
	live, err := e.tableLiveFiles(ctx, tbl)
	if err != nil {
		return reconcileResult{}, fmt.Errorf("read current iceberg data files: %w", err)
	}

	// The diff key is (path, size). A path on both sides with a different size is a file
	// whose bytes changed without changing its path — for example, a backup restore replacing
	// an existing object. The manifest entry still describes the old content (record_count,
	// file_size_in_bytes, column bounds), so external engines keep counting deleted rows and
	// can mis-plan reads against the shorter file. Such a path is dropped and re-registered
	// in the same commit.
	var toAdd, toRemove, rewritten []string
	for p, size := range want {
		df, ok := live[p]
		switch {
		case !ok:
			toAdd = append(toAdd, p)
		case df.FileSizeBytes() != size:
			rewritten = append(rewritten, p)
			toRemove = append(toRemove, p)
			toAdd = append(toAdd, p)
		}
	}
	for p := range live {
		if _, ok := want[p]; !ok {
			toRemove = append(toRemove, p)
		}
	}
	// Deterministic order (stable snapshots, easier debugging).
	sort.Strings(toAdd)
	sort.Strings(toRemove)

	if len(toAdd) == 0 && len(toRemove) == 0 {
		// Already converged — no new snapshot. Still republish the discovery
		// files: a previous pass may have committed the snapshot but failed to
		// write them, and this is the path that pass's retry lands on.
		//
		// Sweep here too. A measurement whose file set has stopped changing is
		// skipped by the scheduler's fingerprint gate before it ever reaches this
		// function, so for such a table THIS is the pass that reclaims: the first
		// one after a restart, whose fingerprint cache is cold.
		tbl, ok, err := e.finishPass(ctx, tbl, tkey, database, measurement, 0, -1)
		if err != nil {
			return reconcileResult{}, err
		}
		return reconcileResult{settled: ok && hintOK, schema: arcSchemaOf(tbl)}, nil
	}

	// Metadata-only: files are dropped and added as DataFiles. (Transaction.Delete is a
	// ROW-level predicate that rewrites partially-matching files — wrong here; we drop whole
	// files that Arc already removed from storage.) The removes are the table's own entries;
	// the adds are built from their footers first, which is also where a file whose rows
	// straddle a UTC day is found. A rewritten path appears in both lists and is removed before
	// it is added, inside the same commit.
	removes := make([]iceberg.DataFile, 0, len(toRemove))
	for _, p := range toRemove {
		removes = append(removes, live[p])
	}
	// A file that moved tiers is the same file under another URI: its DataFile is the table's
	// own entry with the new path, and the moved object is never read (cold_export.go).
	cloned, toBuild, err := cloneFromTable(tbl, live, toAdd, keyOf, want)
	if err != nil {
		return reconcileResult{}, err
	}
	anchored, free := splitAnchored(toBuild, toRemove, database, measurement)
	sortNewestFirst(free, func(p string) string { return chunkSortKey(keyOf[p], p) })
	newFiles := len(toAdd) - len(rewritten) - len(cloned)

	var (
		commits    int
		registered int
		skippedAll []string
		loopErr    error
		stopReason string
	)
	liveNow := len(live)
	passStart := -1 // the first version this pass committed; finishPass's prune keeps it and later ones
	commit := func(rm, adds []iceberg.DataFile) error {
		if commits > 0 {
			// Claim the previous commit's version before making another (adopt.go): a pass of
			// several commits otherwise publishes only its last version, and a stale writer is
			// fenced only by colliding with a published one. A single-commit pass claims nothing
			// here, exactly as before #1686.
			if err := e.claimVersion(ctx, tbl); err != nil {
				return err
			}
		}
		// The manifest collapse (#1106) is decided per commit, before it stages anything (it
		// reads the CURRENT snapshot and the transaction cannot be re-driven once the diff has
		// staged), against the live count THIS commit drives the table to — not the pass's
		// final one. A commit that empties the table must not collapse; see shouldCollapse.
		after := liveNow - len(rm) + len(adds)
		collapse := e.shouldCollapse(ctx, tbl, after, database, measurement)
		committed, err := e.commitDiff(ctx, tbl, rm, adds, database, measurement, collapse)
		if err != nil {
			return err
		}
		tbl, liveNow = committed, after
		if commits == 0 {
			passStart = e.metadataVersion(tbl)
		}
		commits++
		// version-hint.text moves once per pass, in finishPass, whether the pass ended complete,
		// partial or on a chunk error. The v<N> copy of this commit is claimed by the next
		// commit (above), or published with the hint if this was the last.
		return nil
	}

	// First commit: removals, tier moves, rewrites and new files sharing a day with a removal,
	// all together (see the comment at the top of this file). Not chunked and not time-budgeted:
	// splitting it is exactly what must not happen, and it is the commit the pass was making
	// before #1686, for the part of the diff it covers.
	if len(removes) > 0 || len(cloned) > 0 || len(anchored) > 0 {
		var built []iceberg.DataFile
		var skipped []string
		tbl, sc, built, skipped, err = e.prepareChunk(ctx, ctx, tbl, sc, anchored, unread, req.readSchemas, want, canCommit)
		if errors.Is(err, errWriterLost) {
			stopReason = stopWriterLost
			err = nil
		} else if err != nil {
			return reconcileResult{}, err
		}
		skippedAll = append(skippedAll, skipped...)
		adds := append(cloned, built...)
		// len(adds)+len(removes) > 0 here: an anchored add or a clone exists only beside a removal.
		if !canCommit() {
			stopReason = stopWriterLost
		} else {
			if err := commit(removes, adds); err != nil {
				return reconcileResult{}, err
			}
			// Floor history now, straight after the commit that removed the paths, as before
			// #1686 — not at the end of the pass, which a node that loses the writer role
			// mid-pass never reaches, and the next writer, seeing nothing to remove, would never
			// floor at all. finishPass retries a failed floor.
			var floorOK bool
			tbl, floorOK = e.expireSnapshots(ctx, tbl, database, measurement, expireModeFor(len(removes)))
			if len(removes) > 0 {
				e.setFloorOwed(tkey, !floorOK)
			}
			rewrote := make(map[string]struct{}, len(rewritten))
			for _, p := range rewritten {
				rewrote[p] = struct{}{}
			}
			for _, df := range built {
				if _, ok := rewrote[df.FilePath()]; !ok {
					registered++
				}
			}
		}
	}

	// Then the new files no removal touches, newest first, a chunk at a time, while there is
	// time left. A chunk that runs out of time commits nothing and ends the pass; the next one
	// continues with a smaller chunk.
	var lastDur time.Duration
	for len(free) > 0 && stopReason == "" {
		if !e.timeForChunk(ctx, lastDur) {
			stopReason = "measurement deadline"
			break
		}
		batch := free[:min(e.chunkSizeFor(tkey), len(free))]
		cctx, cancel := e.chunkContext(ctx)
		start := time.Now()
		var built []iceberg.DataFile
		var skipped []string
		tbl, sc, built, skipped, err = e.prepareChunk(cctx, ctx, tbl, sc, batch, unread, req.readSchemas, want, canCommit)
		// Read before cancel(), which would set it on every path.
		chunkExpired := errors.Is(cctx.Err(), context.DeadlineExceeded)
		cancel()
		if errors.Is(err, errWriterLost) {
			stopReason = stopWriterLost
			break
		}
		if err != nil {
			if fe := e.fencedErr(); fe != nil {
				return reconcileResult{}, fe
			}
			// Only a deadline error is "did not fit"; a type conflict that happens to surface
			// after the chunk's deadline is still an error.
			if ctx.Err() == nil && chunkExpired && errors.Is(err, context.DeadlineExceeded) {
				// The chunk's own budget ran out, not the pass's: commit nothing for it, shrink
				// the next one and stop. Not an error — earlier chunks stand.
				e.shrinkChunk(tkey)
				stopReason = "chunk did not fit in the time left"
				break
			}
			loopErr = err
			break
		}
		skippedAll = append(skippedAll, skipped...)
		if len(built) > 0 {
			if !canCommit() {
				stopReason = stopWriterLost
				break
			}
			if err := commit(nil, built); err != nil {
				if fe := e.fencedErr(); fe != nil {
					return reconcileResult{}, fe // forked: no hint, no prune, no sweep (adopt.go)
				}
				loopErr = err
				break
			}
			registered += len(built)
		}
		free = free[len(batch):]
		lastDur = time.Since(start)
		e.growChunk(tkey, lastDur)
	}

	if len(skippedAll) > 0 {
		// Some files could not be partitioned (day-straddling data — see day() spec). They are
		// omitted from the table but logged loudly; the rest of the measurement still exports.
		e.logger.Error().
			Str("database", database).Str("measurement", measurement).
			Int("skipped", len(skippedAll)).Strs("files", skippedAll).
			Msg("Iceberg: skipped files that could not be partition-mapped (day-straddling time range)")
	}

	// A node that lost the writer role mid-pass writes nothing more: no expiry commit, no
	// discovery files, no prune or sweep. The new writer's pass reconciles from the table.
	finishedOK := false
	if stopReason != stopWriterLost {
		tbl, finishedOK, err = e.finishPass(ctx, tbl, tkey, database, measurement, commits, passStart)
		if err != nil {
			return reconcileResult{}, err
		}
	}
	partial := len(free) > 0 && loopErr == nil
	remaining := len(free)
	if partial && commits == 0 && stopReason != stopWriterLost {
		// Nothing landed: no time was left for even one chunk (the listing and the table seed
		// took the deadline), or the smallest chunk still did not fit. That is not progress, and
		// reporting it as in_progress would also bring every later pass forward to the short
		// retry interval. A failure, on the normal interval, with the chunk already shrunk.
		loopErr = fmt.Errorf("no time left in this pass to register any of %d new files (%s); the next pass retries with chunks of %d", remaining, stopReason, e.chunkSizeFor(tkey))
		partial = false
	}
	if stopReason != "" && commits == 0 && len(removes)+len(cloned)+len(anchored) > 0 {
		// The first commit itself was refused (writer role): nothing of this pass landed.
		partial, remaining = true, newFiles
	}
	if commits > 0 {
		e.logger.Info().
			Str("database", database).Str("measurement", measurement).
			Int("added", registered).Int("removed", len(toRemove)-len(rewritten)).
			Int("reregistered", len(rewritten)).Int("moved", len(cloned)).
			Int("commits", commits).Int("remaining", remaining).
			Bool("hint_published", hintOK).
			Bool("history_settled", finishedOK).
			Msg("Reconciled Iceberg table")
	}
	if partial {
		e.logger.Info().
			Str("database", database).Str("measurement", measurement).
			Int("registered", registered).Int("new_files", newFiles).Int("remaining", remaining).
			Str("stopped", stopReason).
			Msg("Iceberg: registered part of this measurement's new files; the next pass continues from the table")
	}
	res := reconcileResult{
		partial:    partial,
		registered: registered,
		remaining:  remaining,
		schema:     arcSchemaOf(tbl),
	}
	if loopErr != nil {
		return res, loopErr
	}
	// A failed floor must not be cached as settled. The scheduler keys its skip gate on this
	// value; reporting not-settled keeps the fingerprint uncached so the next tick re-drives the
	// measurement, and finishPass retries the owed floor there even if that pass removes nothing.
	res.settled = !partial && hintOK && finishedOK
	return res, nil
}

// finishPass runs the once-per-pass steps after the pass's commits: snapshot expiry, the
// discovery files, version-file pruning and the orphan sweep. Once per pass, not per commit:
// expiring and pruning after every chunk would add a commit per chunk and walk readers' version
// cushion (pruneOldVersionFiles keeps retain+1) forward several times within one pass.
//
// Expire old snapshots so snapshot history and metadata don't grow unbounded, and floor
// history to the current snapshot when this pass removed any path. A removed path is one
// Arc already deleted from the primary backend, so every older snapshot names a file that
// is gone and time travel to it fails on the read (#1092). Flooring does not lose
// readable history: it drops snapshots that were already unreadable. A rewritten path is in
// the removal list too: it is still present in storage, but with different bytes, so the
// older snapshots' record_count and bounds describe content that no longer exists there — the
// original #1092 symptom.
//
// Best-effort for the retention case: a failure doesn't undo the successful reconcile, and the
// next pass retries. NOT best-effort for the floor: only a pass that removes something asks to
// floor, so the next pass may well remove nothing. A failed floor is remembered (floorOwed) and
// retried by every later pass until it lands, including a pass with nothing to commit; before
// #1686 the retry pass took the converged branch, which never expired, and the floor was lost.
// The memory is per process: a restart in between forgets it. The floor itself is taken
// straight after the commit that removed the paths (reconcile), so here it is only the retry.
//
// passStart is the first version this pass committed (-1: none). Pruning keeps it and every
// later v<N> copy, so the versions a multi-commit pass claimed (claimVersion) stay on disk for
// the fence until the next pass.
//
// ok reports that the discovery files were published and any owed floor landed.
func (e *Exporter) finishPass(ctx context.Context, tbl *icetable.Table, tkey, database, measurement string, commits, passStart int) (*icetable.Table, bool, error) {
	floor := e.floorOwed(tkey)
	expireOK := true
	before := tbl.MetadataLocation()
	switch {
	case floor:
		tbl, expireOK = e.expireSnapshots(ctx, tbl, database, measurement, floorHistory)
		e.setFloorOwed(tkey, !expireOK)
	case commits > 0:
		tbl, expireOK = e.expireSnapshots(ctx, tbl, database, measurement, retainConfigured)
	}
	published := e.writeVersionHint(ctx, tbl) && !e.hintFailed()
	if err := e.fencedErr(); err != nil {
		return tbl, false, err
	}
	if commits > 0 || tbl.MetadataLocation() != before {
		e.pruneOldVersionFilesFrom(ctx, tbl, passStart)
	}
	// After pruneOldVersionFiles, so the on-disk metadata set the sweep reads for
	// reachability is the one this pass settled on, not one version stale.
	e.sweepOrphanMetadata(ctx, tbl)
	return tbl, published && expireOK, nil
}

// errWriterLost is prepareChunk's report that canCommit turned false before a schema commit.
var errWriterLost = errors.New("iceberg: this node is no longer the writer")

// prepareChunk reads the schemas of the chunk's unread files (readCtx), evolves the table to
// cover them (commitCtx: a schema commit is not abandoned halfway because the read budget
// ended), and builds the chunk's DataFiles (readCtx). Files whose schema is read are removed
// from unread. On error the returned table and schema are still the current ones.
//
// A schema evolution is a commit, and it publishes the discovery files (evolveSchema), so it is
// gated like the data commits: errWriterLost when canCommit is false, and the fence is checked
// straight after it.
func (e *Exporter) prepareChunk(readCtx, commitCtx context.Context, tbl *icetable.Table, sc ArcSchema, paths []string, unread map[string]FileRef, readSchemas func(context.Context, []FileRef) (ArcSchema, error), want map[string]int64, canCommit func() bool) (*icetable.Table, ArcSchema, []iceberg.DataFile, []string, error) {
	if len(paths) == 0 {
		return tbl, sc, nil, nil, nil
	}
	var toRead []FileRef
	for _, p := range paths {
		if f, ok := unread[p]; ok {
			toRead = append(toRead, f)
		}
	}
	if len(toRead) > 0 {
		add, err := readSchemas(readCtx, toRead)
		if err != nil {
			return tbl, sc, nil, nil, err
		}
		merged, err := MergeSchemas(sc, add)
		if err != nil {
			return tbl, sc, nil, nil, err
		}
		if !schemaCovers(tbl, merged) && !canCommit() {
			return tbl, sc, nil, nil, errWriterLost
		}
		evolved, err := e.evolveSchema(commitCtx, tbl, merged)
		if err != nil {
			return tbl, sc, nil, nil, err
		}
		if err := e.fencedErr(); err != nil {
			return evolved, sc, nil, nil, err
		}
		tbl, sc = evolved, merged
		for _, f := range toRead {
			delete(unread, f.PhysicalPath)
		}
	}
	built, skipped, err := e.buildDataFiles(readCtx, tbl, paths, want)
	if err != nil {
		return tbl, sc, nil, nil, err
	}
	return tbl, sc, built, skipped, nil
}

// splitAnchored separates the paths to build into those that must commit with this pass's
// removals (anchored) and those that may be registered a chunk at a time (free). A path is
// anchored when its UTC day directory also loses a file this pass — a compaction output, a
// rewrite, a tier move whose clone did not resolve. A path or removal whose day cannot be read
// from its key is treated as touching every day: with any removal at all, everything is
// anchored, which is the single commit Arc made before #1686.
func splitAnchored(toBuild, toRemove []string, database, measurement string) (anchored, free []string) {
	if len(toRemove) == 0 {
		return nil, append([]string(nil), toBuild...)
	}
	days := make(map[string]struct{}, len(toRemove))
	for _, p := range toRemove {
		d := dayOf(p, database, measurement)
		if d == "" {
			return append([]string(nil), toBuild...), nil
		}
		days[d] = struct{}{}
	}
	for _, p := range toBuild {
		d := dayOf(p, database, measurement)
		if _, hit := days[d]; hit || d == "" {
			anchored = append(anchored, p)
		} else {
			free = append(free, p)
		}
	}
	return anchored, free
}

// dayOf returns "YYYY/MM/DD" from a data file's path or URI, by the storage layout
// {database}/{measurement}/{year}/{month}/{day}/[{hour}/]{file}. Daily-compacted files have
// no hour directory, which is why the day — not the hour — is the unit: it is also the
// table's day() partition. "" when the path does not have that shape.
func dayOf(p, database, measurement string) string {
	p = filepath.ToSlash(p)
	marker := database + "/" + measurement + "/"
	var rest string
	if i := strings.LastIndex(p, "/"+marker); i >= 0 {
		rest = p[i+1+len(marker):]
	} else if strings.HasPrefix(p, marker) {
		rest = p[len(marker):]
	} else {
		return ""
	}
	segs := strings.SplitN(rest, "/", 4)
	if len(segs) < 4 || segs[0] == "" || segs[1] == "" || segs[2] == "" {
		return ""
	}
	return segs[0] + "/" + segs[1] + "/" + segs[2]
}

// chunkSortKey orders files by storage key, which sorts by time across tiers (a URI does not:
// a cold s3:// URI and a hot local path sort by scheme). Falls back to the path.
func chunkSortKey(key, p string) string {
	if key != "" {
		return key
	}
	return p
}

// sortNewestFirst sorts descending by key: a dashboard reads recent data, so a measurement that
// is still being registered answers recent queries first.
func sortNewestFirst[T any](s []T, key func(T) string) {
	sort.SliceStable(s, func(i, j int) bool { return key(s[i]) > key(s[j]) })
}

// schemaCovers reports whether the table already has every column of sc (evolveSchema would be
// a no-op).
func schemaCovers(tbl *icetable.Table, sc ArcSchema) bool {
	for _, f := range sc.Fields {
		if _, ok := tbl.Schema().FindFieldByName(f.Name); !ok {
			return false
		}
	}
	return true
}

// metadataVersion is the number N of the table's current metadata file (the v<N> it is
// published as), -1 when the location cannot be parsed.
func (e *Exporter) metadataVersion(tbl *icetable.Table) int {
	v, _, ok := e.parseVersionAndMetaDir(tbl.MetadataLocation())
	if !ok {
		return -1
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return n
}

// claimVersion publishes the v<N>.metadata.json copy of a commit in the middle of a pass,
// without moving version-hint.text (finishPass does that once). It is the stale-writer fence
// (adopt.go) for the versions a multi-commit pass would otherwise never publish: errFenced when
// another writer already holds this version. Any other failure is logged and ignored; the
// copy is a claim, not the publish readers use.
func (e *Exporter) claimVersion(ctx context.Context, tbl *icetable.Table) error {
	if e.backend == nil {
		return nil
	}
	metaLoc := tbl.MetadataLocation()
	version, dirKey, ok := e.parseVersionAndMetaDir(metaLoc)
	if !ok {
		return nil
	}
	metaKey, ok := e.warehouseRelKey(metaLoc)
	if !ok {
		return nil
	}
	body, err := e.backend.Read(ctx, metaKey)
	if err != nil {
		e.logger.Debug().Err(err).Str("key", metaKey).Msg("Iceberg: could not read a mid-pass version to claim it (non-fatal)")
		return nil
	}
	vKey := path.Join(dirKey, "v"+version+".metadata.json")
	if err := e.publishMetadataCopy(ctx, vKey, body); err != nil {
		if errors.Is(err, errFenced) {
			e.logger.Error().Err(err).Str("key", vKey).Bool("adopts_next_pass", e.warehouseAuthoritative).
				Msg("Iceberg: another writer published this table version first; this pass stops (the next one adopts the published version where the warehouse is authoritative)")
			e.hintFailMu.Lock()
			e.fenced = err
			e.hintFailMu.Unlock()
			return err
		}
		e.logger.Debug().Err(err).Str("key", vKey).Msg("Iceberg: could not claim a mid-pass version (non-fatal)")
	}
	return nil
}

func schemaHasField(sc ArcSchema, name string) bool {
	for _, f := range sc.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

// arcSchemaOf is the table's current schema as an ArcSchema.
func arcSchemaOf(tbl *icetable.Table) ArcSchema {
	var sc ArcSchema
	for _, f := range tbl.Schema().Fields() {
		sc.Fields = append(sc.Fields, ArcField{Name: f.Name, Type: f.Type})
	}
	return sc
}

// timeForChunk reports whether another chunk should start: the deadline leaves the commit
// reserve plus about as long as the last chunk took. No deadline, always.
func (e *Exporter) timeForChunk(ctx context.Context, last time.Duration) bool {
	dl, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(dl) > e.commitReserve+last
}

// chunkContext is the context a chunk's footer reads run under: the pass's, ending the commit
// reserve before its deadline, so a chunk that does not fit stops with time left to finish the
// pass cleanly instead of failing it at the hard deadline.
func (e *Exporter) chunkContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if dl, ok := ctx.Deadline(); ok {
		return context.WithDeadline(ctx, dl.Add(-e.commitReserve))
	}
	return context.WithCancel(ctx)
}

// chunkSizeFor is the measurement's current chunk size: dataFileChunkMax (e.chunkMax) unless a
// chunk timed out, in which case it halves down to dataFileChunkMin and doubles back after
// chunks that finish quickly. Remembered per process; a restart starts at the maximum again.
func (e *Exporter) chunkSizeFor(tkey string) int {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	if n, ok := e.chunkSize[tkey]; ok {
		return n
	}
	return e.maxChunk()
}

// maxChunk is e.chunkMax, or dataFileChunkMax for an Exporter not built by NewExporter: a zero
// chunk size would never advance the chunk loop. Called with chunkMu held or from it.
func (e *Exporter) maxChunk() int {
	if e.chunkMax <= 0 {
		return dataFileChunkMax
	}
	return e.chunkMax
}

func (e *Exporter) shrinkChunk(tkey string) {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	n, ok := e.chunkSize[tkey]
	if !ok {
		n = e.maxChunk()
	}
	n = max(n/2, min(dataFileChunkMin, e.maxChunk()))
	if e.chunkSize == nil {
		e.chunkSize = make(map[string]int)
	}
	e.chunkSize[tkey] = n
}

func (e *Exporter) growChunk(tkey string, took time.Duration) {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	n, ok := e.chunkSize[tkey]
	if !ok || took >= chunkGrowBelow {
		return
	}
	if n *= 2; n >= e.maxChunk() {
		delete(e.chunkSize, tkey) // back at the default; keep the map to shrunk tables only
		return
	}
	e.chunkSize[tkey] = n
}

func (e *Exporter) floorOwed(tkey string) bool {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	_, owed := e.floorDue[tkey]
	return owed
}

func (e *Exporter) setFloorOwed(tkey string, owed bool) {
	e.chunkMu.Lock()
	defer e.chunkMu.Unlock()
	if owed {
		if e.floorDue == nil {
			e.floorDue = make(map[string]struct{})
		}
		e.floorDue[tkey] = struct{}{}
	} else {
		delete(e.floorDue, tkey)
	}
}
