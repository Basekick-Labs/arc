package ingest

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
)

// A storage write that fails can still land (#1635).
//
// A flush's storage.Write runs under ingest.flush_timeout_seconds. When the
// client gives up — a deadline, a cancel, a reset connection — an object
// store that already holds the body can still complete the PUT. The flush is
// failed all the same: the file is not registered, the WAL entries are not
// checkpointed, and the next recovery pass replays the rows into a new file
// with a new name. Both files are in storage and queries read both.
//
// The replay cannot simply reuse the failed key: replayed entries merge with
// live writes in the shared buffer and are split again by hour and coverage,
// so the replayed file does not hold the same rows as the failed one.
//
// So the failed key is kept here, with the WAL identities of the rows it was
// meant to hold. Once another flush has stored every one of those rows — the
// evidence the WAL checkpoint itself relies on to stop replaying them — the
// failed key holds nothing that is not stored elsewhere, and a copy that
// landed there is removed. It is checked more than once, because the
// abandoned PUT can land after the first look.
//
// Deleting is only ever safe on that proof, so anything that cannot carry it
// is not recorded and keeps today's behaviour (a possible duplicate, never a
// loss): no WAL, rows without a WAL copy, identities that replay does not
// inherit (content hashes), or a full ledger.

// failedStorageWrite marks a flush error raised by storage.Write, naming the
// key that may still be written.
type failedStorageWrite struct {
	err  error
	path string
	size int64
	// rows names the rows of a per-hour file of a multi-hour flush (row-range
	// identities, rangesIn); empty when they cannot all be named. nil: the
	// flush's need covers the file.
	rows []string
}

func (e *failedStorageWrite) Error() string { return e.err.Error() }
func (e *failedStorageWrite) Unwrap() error { return e.err }

const (
	// maxAmbiguousWrites and maxAmbiguousParents bound the ledger, in files
	// and in WAL entries named. Past either a failed write is not recorded:
	// its possible duplicate stays, as before #1635.
	maxAmbiguousWrites  = 10000
	maxAmbiguousParents = 200000
	// ambiguousWriteMaxAge drops an entry still held after a day: rows never
	// stored by another flush (replayed under an identity it does not name,
	// or not replayed by this process at all), or a key storage kept refusing
	// to answer for.
	ambiguousWriteMaxAge = 24 * time.Hour
	// ambiguousSweepInterval paces the sweeper. It does nothing while the
	// ledger is empty.
	ambiguousSweepInterval = 15 * time.Second
	// ambiguousStorageTimeout bounds each stat and delete.
	ambiguousStorageTimeout = 30 * time.Second
)

// ambiguousCheckAfter is when a satisfied entry is looked at, measured from
// the moment its rows were all stored elsewhere. The first look usually finds
// the late copy (storage was answering again, or the replay could not have
// been stored); the later ones catch a PUT the store finished after it.
var ambiguousCheckAfter = []time.Duration{0, time.Minute, 5 * time.Minute, 15 * time.Minute}

// rowRange is rows [start, end) of one WAL entry.
type rowRange struct{ start, end int }

// ambiguousNeed is what must be stored elsewhere for one parent entry: the
// whole entry, or row ranges of it.
type ambiguousNeed struct {
	whole  bool
	ranges []rowRange // sorted, disjoint; used when !whole
}

type ambiguousWrite struct {
	path        string
	size        int64
	need        map[string]*ambiguousNeed // parent identity -> rows still unstored
	recordedAt  time.Time
	satisfiedAt time.Time // zero until need is empty
	checks      int       // looks taken since satisfied
	parents     int       // len(need) when recorded, for the ledger's bound
	// A deferred hour file (#1697) is not a failed write: it is stored,
	// unregistered, and possibly the only copy of its rows (a WAL that
	// accepted an entry and then failed to write it). If its need is not met
	// within maxAge, or by Close, register is called instead of letting it
	// go untracked, which on a cluster would leave it an orphan the
	// reconciliation sweep can delete. nil for a failed write.
	register func(context.Context)
	maxAge   time.Duration // 0: ambiguousWriteMaxAge
}

// unmetDeferred is a deferred file whose need was not met in time.
type unmetDeferred struct {
	path     string
	size     int64
	register func(context.Context)
}

type ambiguousLedger struct {
	mu       sync.Mutex
	entries  map[string]*ambiguousWrite     // by path
	byParent map[string]map[string]struct{} // parent -> paths needing it
	count    atomic.Int64                   // len(entries), read without mu
	parents  int                            // sum of the entries' parents
	now      func() time.Time               // tests
	warned   atomic.Int64                   // unix nanos of the last "not tracked" warning
}

func newAmbiguousLedger() *ambiguousLedger {
	return &ambiguousLedger{
		entries:  make(map[string]*ambiguousWrite),
		byParent: make(map[string]map[string]struct{}),
		now:      time.Now,
	}
}

// needFromIDs parses the identities a failed file's rows are named by. ok is
// false when one of them can never be matched by a replay (a content hash, or
// anything else that is not a tracked token or a row range).
func needFromIDs(ids []string) (map[string]*ambiguousNeed, bool) {
	if len(ids) == 0 {
		return nil, false
	}
	need := make(map[string]*ambiguousNeed, len(ids))
	for _, id := range ids {
		if isTrackedIdentity(id) {
			need[id] = &ambiguousNeed{whole: true}
			continue
		}
		parent, start, end, ok := wal.ParseRecoveryRowIdentity(id)
		if !ok {
			return nil, false
		}
		n := need[parent]
		if n == nil {
			n = &ambiguousNeed{}
			need[parent] = n
		}
		if !n.whole {
			n.ranges = addRange(n.ranges, rowRange{start, end})
		}
	}
	return need, true
}

func isTrackedIdentity(id string) bool {
	parent, _ := rowSpanParent(id, 0)
	return parent != "" && parent == id
}

// rangeRows counts the rows row-range identities name; -1 if one is not a
// row range.
func rangeRows(ids []string) int {
	n := 0
	for _, id := range ids {
		_, start, end, ok := wal.ParseRecoveryRowIdentity(id)
		if !ok {
			return -1
		}
		n += end - start
	}
	return n
}

// addRange inserts r into sorted disjoint ranges, merging overlaps.
func addRange(ranges []rowRange, r rowRange) []rowRange {
	if n := len(ranges); n == 0 || ranges[n-1].end < r.start {
		// After every range: the usual case, as rangesIn emits them in order
		// (#1697: a deferred file's need can hold thousands).
		return append(ranges, r)
	}
	out := make([]rowRange, 0, len(ranges)+1)
	placed := false
	for _, x := range ranges {
		switch {
		case x.end < r.start:
			out = append(out, x)
		case r.end < x.start:
			if !placed {
				out = append(out, r)
				placed = true
			}
			out = append(out, x)
		default:
			if x.start < r.start {
				r.start = x.start
			}
			if x.end > r.end {
				r.end = x.end
			}
		}
	}
	if !placed {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	return out
}

// subtractRange removes r from sorted disjoint ranges.
func subtractRange(ranges []rowRange, r rowRange) []rowRange {
	out := ranges[:0:0]
	for _, x := range ranges {
		if x.end <= r.start || x.start >= r.end {
			out = append(out, x)
			continue
		}
		if x.start < r.start {
			out = append(out, rowRange{x.start, r.start})
		}
		if x.end > r.end {
			out = append(out, rowRange{r.end, x.end})
		}
	}
	return out
}

// record adds a failed write. Reports whether it was recorded.
func (l *ambiguousLedger) record(path string, size int64, ids []string) bool {
	return l.recordWith(path, size, ids, 0, nil)
}

// recordDeferred adds a deferred hour file of a part-failed flush (#1697),
// with what registers it if its need is not met within maxAge.
func (l *ambiguousLedger) recordDeferred(path string, size int64, ids []string, maxAge time.Duration, register func(context.Context)) bool {
	return l.recordWith(path, size, ids, maxAge, register)
}

func (l *ambiguousLedger) recordWith(path string, size int64, ids []string, maxAge time.Duration, register func(context.Context)) bool {
	if l == nil {
		return false
	}
	need, ok := needFromIDs(ids)
	if !ok {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, dup := l.entries[path]; dup {
		return true
	}
	if len(l.entries) >= maxAmbiguousWrites || l.parents+len(need) > maxAmbiguousParents {
		return false
	}
	e := &ambiguousWrite{path: path, size: size, need: need, recordedAt: l.now(), parents: len(need), register: register, maxAge: maxAge}
	l.parents += len(need)
	l.entries[path] = e
	for parent := range need {
		paths := l.byParent[parent]
		if paths == nil {
			paths = make(map[string]struct{})
			l.byParent[parent] = paths
		}
		paths[path] = struct{}{}
	}
	l.count.Store(int64(len(l.entries)))
	return true
}

// stored notes identities whose rows a flush has stored and registered. A
// whole identity covers every row of its entry; a row range covers its rows.
// A whole need is not met by ranges: the entry's length is not known here.
func (l *ambiguousLedger) stored(ids []string) {
	if l == nil || l.count.Load() == 0 || len(ids) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for _, id := range ids {
		parent, start, end, isRange := wal.ParseRecoveryRowIdentity(id)
		if !isRange {
			parent = id
		}
		paths := l.byParent[parent]
		for path := range paths {
			e := l.entries[path]
			if e == nil {
				continue
			}
			n := e.need[parent]
			if n == nil {
				continue
			}
			if isRange {
				if n.whole {
					continue
				}
				n.ranges = subtractRange(n.ranges, rowRange{start, end})
				if len(n.ranges) > 0 {
					continue
				}
			}
			delete(e.need, parent)
			delete(paths, path)
			if len(e.need) == 0 && e.satisfiedAt.IsZero() {
				e.satisfiedAt = now
			}
		}
		if len(paths) == 0 {
			delete(l.byParent, parent)
		}
	}
}

// forget drops the entry for path, if any, without removing anything: its
// file was adopted as the stored copy of its rows (#1646).
func (l *ambiguousLedger) forget(path string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.entries[path]; e != nil {
		l.dropLocked(e)
	}
}

func (l *ambiguousLedger) dropLocked(e *ambiguousWrite) {
	delete(l.entries, e.path)
	l.parents -= e.parents
	for parent := range e.need {
		if paths := l.byParent[parent]; paths != nil {
			delete(paths, e.path)
			if len(paths) == 0 {
				delete(l.byParent, parent)
			}
		}
	}
	l.count.Store(int64(len(l.entries)))
}

// ambiguousCheck is one look the sweeper is due to take.
type ambiguousCheck struct {
	path string
	size int64
}

// due returns the looks due now, and drops entries that expired unsatisfied.
func (l *ambiguousLedger) due() (checks []ambiguousCheck, expired []string, unmet []unmetDeferred) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for _, e := range l.entries {
		maxAge := ambiguousWriteMaxAge
		if e.maxAge > 0 {
			maxAge = e.maxAge
		}
		if e.satisfiedAt.IsZero() && now.Sub(e.recordedAt) >= maxAge || now.Sub(e.recordedAt) >= ambiguousWriteMaxAge {
			if e.register != nil && e.satisfiedAt.IsZero() {
				unmet = append(unmet, unmetDeferred{path: e.path, size: e.size, register: e.register})
			} else {
				expired = append(expired, e.path)
			}
			l.dropLocked(e)
			continue
		}
		if e.satisfiedAt.IsZero() {
			continue
		}
		if e.checks < len(ambiguousCheckAfter) && now.Sub(e.satisfiedAt) >= ambiguousCheckAfter[e.checks] {
			checks = append(checks, ambiguousCheck{path: e.path, size: e.size})
		}
	}
	return checks, expired, unmet
}

// checked advances an entry past one look; drop ends it.
func (l *ambiguousLedger) checked(path string, drop bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entries[path]
	if e == nil {
		return
	}
	e.checks++
	if drop || e.checks >= len(ambiguousCheckAfter) {
		l.dropLocked(e)
	}
}

// walNeedOf names every row of records by an identity replay inherits, for
// a failed write's ledger entry. ok is false when a row cannot be named, and
// then nothing may be recorded: "every identity was stored again" would not
// mean "every row was". Read before the records are released.
//
// A batch with one identity names its rows as a row range of the entry, by
// the rule walRowSpans relies on for the row checkpoints of #1492 (row r of
// the batch is row r of a tracked entry, or row s+r of a range P:s:e of the
// batch's length). A range need is met by the whole entry or by the ranges
// recovery splits a large row-format entry into (wal.recovery_batch_size),
// where a whole-entry need would only be met by the whole entry. A chunked
// payload (several identities) needs each chunk entry whole.
func walNeedOf(records []interface{}) (need []string, ok bool) {
	return walNeedParts(walNeedPartsOf(records))
}

// walNeedPart is what walNeedOf reads of one record, kept so a flush can
// release its records and still name their rows if it fails. ok false: the
// record is not a batch with a WAL copy and rows.
type walNeedPart struct {
	ids  []string
	rows int
	ok   bool
}

// walNeedPartsOf is the cheap half of walNeedOf: no identity is formatted
// until a write has failed.
func walNeedPartsOf(records []interface{}) []walNeedPart {
	parts := make([]walNeedPart, len(records))
	for i, record := range records {
		batch, isBatch := record.(*TypedColumnBatch)
		if !isBatch || batch == nil || !batch.hasWALCopy || len(batch.WALHashes) == 0 {
			continue
		}
		times, hasTime := batch.Data["time"].([]int64)
		if !hasTime || len(times) == 0 {
			continue
		}
		parts[i] = walNeedPart{ids: batch.WALHashes, rows: len(times), ok: true}
	}
	return parts
}

func walNeedParts(parts []walNeedPart) (need []string, ok bool) {
	if len(parts) == 0 {
		return nil, false
	}
	for _, part := range parts {
		if !part.ok {
			return nil, false
		}
		if len(part.ids) == 1 {
			parent, base := rowSpanParent(part.ids[0], part.rows)
			if parent == "" {
				return nil, false
			}
			need = append(need, wal.RecoveryRowIdentity(parent, base, base+part.rows))
			continue
		}
		for _, id := range part.ids {
			if !isTrackedIdentity(id) {
				return nil, false
			}
			need = append(need, id)
		}
	}
	return need, true
}

// noteFailedWrite records the key of a failed storage write, which can still
// land (#1635). need is walNeedOf of the flush's records.
func (b *ArrowBuffer) noteFailedWrite(err error, need []string, ok bool) {
	var fw *failedStorageWrite
	if b.wal == nil || b.ambiguous == nil || !errors.As(err, &fw) {
		return
	}
	ids, named := need, ok
	if fw.rows != nil {
		ids, named = fw.rows, len(fw.rows) > 0
	}
	if named && b.ambiguous.record(fw.path, fw.size, ids) {
		metrics.Get().IncIngestAmbiguousWritesRecorded()
		return
	}
	now := time.Now().UnixNano()
	if last := b.ambiguous.warned.Load(); now-last >= int64(time.Minute) && b.ambiguous.warned.CompareAndSwap(last, now) {
		b.logger.Warn().Str("storage_path", fw.path).
			Msg("Failed flush write not tracked for late-landing cleanup (rows replay cannot name, or the tracker is full): if the write lands after all, its rows are stored twice after WAL replay")
	}
}

// deferredHandOver is a stored hour file a multi-hour flush deferred and then
// left unregistered when a later hour failed (#1697), with the WAL identities
// of every row it holds.
type deferredHandOver struct {
	path     string
	size     int64
	indices  []int                 // its rows: positions of the merged batch
	register func(context.Context) // registers it, as the post-loop pass would
}

// deferredWriteMaxAge is how long a deferred file waits for replay to store
// its rows before it is registered instead (#1697): several WAL recovery
// passes (wal.recovery_interval_seconds, default 300) and well inside the
// reconciliation sweep's default 24 h grace, so a file whose rows were never
// replayed is registered before the sweep could take it for an orphan.
const deferredWriteMaxAge = time.Hour

// registerUnmetDeferred registers a deferred file whose rows were not seen
// stored elsewhere, if it is still in storage at its size: it may be their
// only copy. The cost is a possible duplicate, which is what registering it
// as stored did before #1697.
func (b *ArrowBuffer) registerUnmetDeferred(ctx context.Context, u unmetDeferred) {
	sctx, cancel := context.WithTimeout(ctx, ambiguousStorageTimeout)
	size, err := b.storage.StatFile(sctx, u.path)
	cancel()
	if err != nil && !storage.IsNotFound(err) {
		b.logger.Error().Err(err).Str("storage_path", u.path).
			Msg("Could not check a deferred hour file whose rows were not seen replayed; registering it, as it may be their only copy")
	} else if size != u.size {
		return // gone, or not this flush's bytes
	}
	rctx, cancel := context.WithTimeout(ctx, ambiguousStorageTimeout)
	u.register(rctx)
	cancel()
	metrics.Get().IncIngestAmbiguousWritesAbandoned()
	b.logger.Warn().Str("storage_path", u.path).
		Msg("Registered a deferred hour file whose rows were not seen stored by WAL replay: it is kept as their copy; if replay stored them under another identity, they are stored twice")
}

// handOverDeferred gives a part-failed flush's deferred files to the ledger.
// Each is unregistered and its rows are replayed into new files; the ledger
// removes it once those rows are stored elsewhere, the same proof a
// late-landing failed write needs (#1635). Never removed on less: the WAL may
// have accepted an entry it then failed to write, and this file would be the
// only copy. One that is not recorded (full ledger) stays: its rows are
// stored twice after replay, as before #1697.
func (b *ArrowBuffer) handOverDeferred(ctx context.Context, bufferKey string, spans *walRowSpans, files []deferredHandOver) {
	if len(files) == 0 || b.wal == nil || b.ambiguous == nil {
		return
	}
	recorded := 0
	for _, f := range files {
		need, ok := spans.deferredNeed(f.indices)
		if ok && b.ambiguous.recordDeferred(f.path, f.size, need, deferredWriteMaxAge, f.register) {
			metrics.Get().IncIngestAmbiguousWritesRecorded()
			recorded++
			continue
		}
		// Not tracked (the tracker is full): register it now, as before
		// #1697, rather than leave a stored file unregistered.
		f.register(ctx)
		b.logger.Warn().Str("buffer_key", bufferKey).Str("storage_path", f.path).
			Msg("Deferred hour file of a failed flush not tracked for cleanup (the tracker is full): registered instead; its rows are stored twice after WAL replay")
	}
	b.logger.Info().Str("buffer_key", bufferKey).Int("files", len(files)).Int("tracked", recorded).
		Msg("Flush failed after storing deferred hour files: they stay unregistered and are removed once WAL replay has stored their rows")
}

// ambiguousSweeper removes late-landed copies of failed writes (#1635).
func (b *ArrowBuffer) ambiguousSweeper() {
	defer b.wg.Done()
	ticker := time.NewTicker(ambiguousSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
		}
		if b.ambiguous.count.Load() == 0 {
			continue
		}
		b.sweepAmbiguousWrites(b.ctx)
	}
}

// sweepAmbiguousWrites takes the looks that are due.
func (b *ArrowBuffer) sweepAmbiguousWrites(ctx context.Context) {
	checks, expired, unmet := b.ambiguous.due()
	for _, u := range unmet {
		b.registerUnmetDeferred(ctx, u)
	}
	for _, path := range expired {
		metrics.Get().IncIngestAmbiguousWritesAbandoned()
		b.logger.Warn().Str("storage_path", path).
			Msg("Stopped tracking a failed flush write after a day (its rows were not seen stored by another flush, or storage did not answer for the key): if the file is in storage, its rows are stored twice")
	}
	for _, c := range checks {
		if ctx.Err() != nil {
			return
		}
		b.checkAmbiguousWrite(ctx, c)
	}
}

func (b *ArrowBuffer) checkAmbiguousWrite(ctx context.Context, c ambiguousCheck) {
	sctx, cancel := context.WithTimeout(ctx, ambiguousStorageTimeout)
	size, err := b.storage.StatFile(sctx, c.path)
	cancel()
	switch {
	case (err == nil && size < 0) || storage.IsNotFound(err): // StatFile: -1, nil is absent
		// Not there (yet). Look again later; the last look ends it.
		b.ambiguous.checked(c.path, false)
		return
	case err != nil:
		// Storage not answering: the look is not taken, try next sweep.
		b.logger.Debug().Err(err).Str("storage_path", c.path).Msg("Could not check a failed flush's key; retrying")
		return
	case size != c.size:
		// Not the bytes this flush sent: never delete what we did not write.
		metrics.Get().IncIngestAmbiguousWritesAbandoned()
		b.logger.Error().Str("storage_path", c.path).Int64("size", size).Int64("expected_size", c.size).
			Msg("Key of a failed flush holds an object of another size; leaving it in place")
		b.ambiguous.checked(c.path, true)
		return
	}
	dctx, cancel := context.WithTimeout(ctx, ambiguousStorageTimeout)
	err = b.storage.Delete(dctx, c.path)
	cancel()
	if err != nil && !storage.IsNotFound(err) {
		b.logger.Warn().Err(err).Str("storage_path", c.path).Msg("Could not remove the late-landed copy of a failed flush; retrying")
		return
	}
	metrics.Get().IncIngestAmbiguousWritesRemoved()
	b.logger.Warn().Str("storage_path", c.path).Int64("size_bytes", size).
		Msg("Removed a file a failed flush wrote after all: its rows were replayed from the WAL into another file, and keeping both would store them twice")
	b.ambiguous.checked(c.path, false)
}

// closeAmbiguousWrites runs at Close, after the last flushes: their replayed
// rows can have satisfied entries the sweeper will not get to. One bounded
// pass of the looks due now, then every entry still held is named, since the
// ledger does not outlive the process.
func (b *ArrowBuffer) closeAmbiguousWrites(budget time.Duration) {
	if b.ambiguous == nil || b.ambiguous.count.Load() == 0 {
		return
	}
	if budget > 0 {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(b.ctx), budget)
		b.sweepAmbiguousWrites(ctx)
		cancel()
	}
	// A deferred file still waiting for its rows is registered now: the
	// ledger does not outlive the process, and unregistered it would be an
	// orphan after the restart (#1697).
	b.ambiguous.mu.Lock()
	var unmet []unmetDeferred
	for _, e := range b.ambiguous.entries {
		if e.register != nil && e.satisfiedAt.IsZero() {
			unmet = append(unmet, unmetDeferred{path: e.path, size: e.size, register: e.register})
			b.ambiguous.dropLocked(e)
		}
	}
	b.ambiguous.mu.Unlock()
	if len(unmet) > 0 {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(b.ctx), ambiguousStorageTimeout)
		for _, u := range unmet {
			b.registerUnmetDeferred(ctx, u)
		}
		cancel()
	}
	b.ambiguous.mu.Lock()
	held := len(b.ambiguous.entries)
	var samples []string
	for _, e := range b.ambiguous.entries {
		if len(samples) == 5 {
			break
		}
		samples = append(samples, e.path)
	}
	b.ambiguous.mu.Unlock()
	if held > 0 {
		b.logger.Warn().Int("tracked", held).Strs("sample_paths", samples).
			Msg("Shutting down with failed flush writes still tracked: any of them in storage holds rows that are stored twice once they are replayed")
	}
}
