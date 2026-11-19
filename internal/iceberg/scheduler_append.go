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
//   - One goroutine (the loop) owns every commit: a pass and an append never
//     interleave.
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
//     outcome: it listed after they were stored.
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
)

// pendingFile is one file an ingest flush reported and no export has covered.
type pendingFile struct {
	key string // storage key
	at  time.Time
}

// pendingFiles is a table's reported files; overflow means some were not
// remembered (appendMaxPending) and only a pass can find them.
type pendingFiles struct {
	files    []pendingFile
	overflow bool
}

// RequestIngestAppend reports a file an ingest flush has stored and kept, and
// asks for an export of its table as RequestIngestReconcile does. When the
// request comes due the reported files are appended to the table; without the
// append path (iceberg.ingest_append_enabled off) it is RequestIngestReconcile.
// It never blocks and is safe on a nil scheduler.
func (s *Scheduler) RequestIngestAppend(database, measurement, storageKey string) {
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
			p.overflow = true
		} else {
			p.files = append(p.files, pendingFile{key: storageKey, at: now})
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
	if len(kept) == 0 {
		// Nothing reported since it started. An overflow before it is covered
		// too; one after it left files behind, which stay.
		delete(s.appendPending, key)
		return
	}
	p.files, p.overflow = kept, false
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

// noteListed records the recent files of a pass's listing in recentAdds. Only
// files an append could still take: the rest cannot be reported in time.
func (s *Scheduler) noteListed(key string, files []FileRef) {
	if !s.appendEnabled || s.ingestInterval <= 0 {
		return
	}
	now := time.Now()
	var recent []string
	for _, f := range files {
		if t, ok := storage.IngestFileTime(path.Base(f.Key)); ok && now.Sub(t) < recentAddsKeep {
			recent = append(recent, f.Key)
		}
	}
	s.noteRecent(key, recent)
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

// exportIngest serves a due ingest request for the table: an append of the
// files reported for it when that is possible, a reconcile pass otherwise.
func (s *Scheduler) exportIngest(ctx context.Context, m Measurement, key string) (reconcileOutcome, error) {
	if !s.appendEnabled {
		return s.reconcileOneRecovered(ctx, m, key)
	}
	resolver, canResolve := s.source.(appendResolver)
	_, settled := s.state[key]
	if !canResolve || !settled || !s.appendOK[key] {
		// The pending files stay: the pass drops the ones it covers.
		return s.reconcileOneRecovered(ctx, m, key)
	}
	pending, takenAt := s.takeAppendPending(key)
	if len(pending.files) == 0 && !pending.overflow {
		// A request with no file behind it (the trigger without a report).
		return s.reconcileOneRecovered(ctx, m, key)
	}
	keys, reason := s.appendable(key, pending, takenAt)
	if reason == "" {
		if len(keys) == 0 {
			// Everything reported is in the table already.
			s.noteIngestCovered(key, takenAt, outcomeUnchanged, nil)
			return outcomeUnchanged, nil
		}
		var added int
		added, reason = s.appendRecovered(ctx, resolver, m, key, keys)
		if reason == "" {
			outcome := outcomeUnchanged
			if added > 0 {
				outcome = outcomeReconciled
			}
			s.noteIngestCovered(key, takenAt, outcome, nil)
			return outcome, nil
		}
	}
	// Not an append: the pass decides what the table holds. Until one
	// settles, the table takes no append.
	delete(s.appendOK, key)
	metrics.Get().IncIcebergIngestAppendFallback()
	if s.appendFallback[key] != reason {
		// Once per reason per table: a table that keeps falling back would
		// otherwise say so at every interval.
		s.appendFallback[key] = reason
		s.logger.Info().Str("database", m.Database).Str("measurement", m.Measurement).Str("reason", reason).
			Msg("Iceberg: ingest files not appended; running a reconcile pass of the measurement instead")
	}
	return s.reconcileOneRecovered(ctx, m, key)
}

// appendResolver is the source's per-file resolution (StorageWalkSource.AppendRefs).
type appendResolver interface {
	AppendRefs(ctx context.Context, m Measurement, keys []string) ([]FileRef, bool, error)
}

// appendRecovered appends the files and returns how many it added, or the
// reason a reconcile pass is needed. A panic is a reason too, as in
// reconcileOneRecovered.
func (s *Scheduler) appendRecovered(ctx context.Context, resolver appendResolver, m Measurement, key string, keys []string) (added int, reason string) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Str("database", m.Database).Str("measurement", m.Measurement).
				Str("panic", fmt.Sprint(r)).Str("stack", string(debug.Stack())).
				Msg("Iceberg append: recovered a panic")
			added, reason = 0, fmt.Sprintf("the append panicked: %v", r)
		}
	}()
	refs, ok, err := resolver.AppendRefs(ctx, m, keys)
	if err != nil {
		return 0, err.Error()
	}
	if !ok {
		return 0, "this storage layout has no per-file check"
	}
	if len(refs) == 0 {
		return 0, "" // gone or left out since they were reported: nothing to add
	}
	res, err := s.exporter.appendFiles(ctx, m.Database, m.Measurement, refs, s.unionFileSchemas, s.canRun)
	if err != nil {
		if errors.Is(err, errNeedsFullPass) {
			return 0, "the table does not exist or lacks a column of the new files"
		}
		return 0, err.Error()
	}
	// Added, and left out for good (rows across a UTC day): neither is
	// offered again.
	keyOf := make(map[string]string, len(refs))
	for _, f := range refs {
		keyOf[f.PhysicalPath] = f.Key
	}
	done := make([]string, 0, len(res.added)+len(res.skipped))
	for _, p := range res.added {
		done = append(done, keyOf[p])
	}
	for _, p := range res.skipped {
		done = append(done, keyOf[p])
	}
	s.noteRecent(key, done)
	if len(res.skipped) > 0 {
		s.logger.Warn().Str("database", m.Database).Str("measurement", m.Measurement).
			Int("skipped", len(res.skipped)).Str("sample", strings.Join(res.skipped[:min(3, len(res.skipped))], ", ")).
			Msg("Iceberg: ingest files left out of the table (their rows span more than one UTC day); the reconcile pass reports them")
	}
	if len(res.added) > 0 {
		// The cached listing no longer describes the table: make the next
		// pass of the measurement run (see the file comment).
		if st, ok := s.state[key]; ok {
			st.fingerprint = appendedFingerprint
			s.state[key] = st
		}
		metrics.Get().AddIcebergIngestAppend(len(res.added))
	}
	return len(res.added), ""
}
