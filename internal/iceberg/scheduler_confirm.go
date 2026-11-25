package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	iofs "io/fs"
	"net/url"
	"strings"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
)

// A re-home job replaces one data file with files in other directories and
// keeps the old file in storage, left out of reads, until the table has
// stopped naming it (#1825). It asks here: a pass of the table that starts
// after the question, then whether the published table names the file.
//
// The pass runs on the loop goroutine like every other reconcile, so "after
// the question" needs no clock: the loop takes the question, then runs the
// pass. A listing that began before the job switched its reads has ended by
// then.

// PassAnswer is what ReconcileAndConfirm found.
type PassAnswer struct {
	// Blocked is why no pass of the table runs now ("" when one ran): the
	// export is paused after a restore, another node exports, the table
	// awaits a namespace migration. Nothing is listing the measurement.
	Blocked string
	// NotExported: the measurement has no table and gets none (its name is
	// not one the export publishes).
	NotExported bool
	// Outcome and Err are the pass's.
	Outcome string
	Err     error
	// Published: the table's current version is the one the discovery files
	// name. Named: that version names the file asked about.
	Published bool
	Named     bool
	// WriteErr: on a question that names no file, the table's metadata
	// directory could not be written (#1845 item 3).
	WriteErr error
}

// Clear reports that the table, as published, does not name the file, or
// that there is no table to name it.
func (a PassAnswer) Clear() bool {
	return a.NotExported || (a.Blocked == "" && a.Published && !a.Named)
}

// NotExportingReason is PassAnswer.Blocked on a node whose export gate is
// closed: another node exports (or none does).
const NotExportingReason = "this node is not the one exporting to Iceberg"

// Verdict is the answer as a re-home job uses it: whether it may go on, and
// if not, what is in the way. asked is false for the question a job puts
// before it changes anything, which names no file: there the pass itself
// must have gone through, not only an earlier one's publish, since the job
// is about to depend on the next one.
func (a PassAnswer) Verdict(asked bool) (clear bool, why string) {
	if !asked && a.WriteErr != nil {
		return false, a.WriteErr.Error()
	}
	if a.Clear() && (asked || a.NotExported || a.Err == nil) {
		return true, ""
	}
	switch {
	case a.Blocked != "":
		return false, a.Blocked
	case a.Err != nil:
		return false, "the last pass of the table failed: " + a.Err.Error()
	case !a.Published:
		return false, "the last pass of the table (" + a.Outcome + ") did not publish its version"
	}
	return false, "the published table still names the file (last pass: " + a.Outcome + ")"
}

type confirmWaiter struct {
	m   Measurement
	key string // the storage key asked about; "" asks only for a pass
	ch  chan PassAnswer
	// readOnly: no pass, only whether the published table names key;
	// answered whatever the gate (CheckPublished, #1870).
	readOnly bool
}

// CheckPublished reports, without running a pass and whichever node exports,
// whether the published table of the measurement names the data file at
// storage key fileKey: on a shared warehouse every node reads the version the
// exporting node published (#1870). Safe from any goroutine; answered on the
// scheduler's loop.
func (s *Scheduler) CheckPublished(ctx context.Context, database, measurement, fileKey string) (PassAnswer, error) {
	w := &confirmWaiter{m: Measurement{Database: database, Measurement: measurement}, key: fileKey, ch: make(chan PassAnswer, 1), readOnly: true}
	return s.awaitConfirm(ctx, w)
}

// ReconcileAndConfirm runs one pass of the table on the scheduler's
// goroutine, started after this call, and reports whether the published
// table names the data file at storage key fileKey. It returns when the pass
// and the check have ended, when the scheduler says no pass runs, or when ctx
// ends. Safe from any goroutine.
func (s *Scheduler) ReconcileAndConfirm(ctx context.Context, database, measurement, fileKey string) (PassAnswer, error) {
	w := &confirmWaiter{m: Measurement{Database: database, Measurement: measurement}, key: fileKey, ch: make(chan PassAnswer, 1)}
	return s.awaitConfirm(ctx, w)
}

func (s *Scheduler) awaitConfirm(ctx context.Context, w *confirmWaiter) (PassAnswer, error) {
	s.reqMu.Lock()
	s.confirms = append(s.confirms, w)
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	select {
	case a := <-w.ch:
		return a, nil
	case <-ctx.Done():
		s.reqMu.Lock()
		for i, x := range s.confirms {
			if x == w {
				s.confirms = append(s.confirms[:i], s.confirms[i+1:]...)
				break
			}
		}
		s.reqMu.Unlock()
		return PassAnswer{}, ctx.Err()
	}
}

func (s *Scheduler) takeConfirms() []*confirmWaiter {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	out := s.confirms
	s.confirms = nil
	return out
}

// blockConfirms answers every waiting question with why no pass runs. Loop
// goroutine only.
func (s *Scheduler) blockConfirms(why string) {
	for _, w := range s.takeConfirms() {
		w.ch <- PassAnswer{Blocked: why}
	}
}

// serviceConfirms answers the waiting questions, one pass each. Loop
// goroutine only; called where requests are served.
func (s *Scheduler) serviceConfirms(ctx context.Context) {
	waiters := s.takeConfirms()
	if len(waiters) == 0 {
		return
	}
	// The loop checks the pause before it serves anything, and a restore can
	// set it between that check and here.
	if s.exporter != nil && s.exporter.exportPausedForRestore() {
		for _, w := range waiters {
			w.ch <- PassAnswer{Blocked: "the Iceberg export is paused until the server restarts (a restore staged a catalog)"}
		}
		return
	}
	// Read-only questions: answered whatever the gate, with no pass.
	rest := waiters[:0]
	for _, w := range waiters {
		if !w.readOnly {
			rest = append(rest, w)
			continue
		}
		a := PassAnswer{Outcome: "checked"}
		if s.exporter != nil {
			mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
			var exists, published, named bool
			err := s.exporter.adoptForCheck(s.exporter.withAWS(mctx), w.m.Database, w.m.Measurement)
			if err == nil {
				exists, published, named, err = s.exporter.publishedNames(mctx, w.m.Database, w.m.Measurement, w.key)
			}
			cancel()
			switch {
			case err != nil:
				a.Err = err
			case !exists:
				a.Published = true // no table: nothing names the file
			default:
				a.Published, a.Named = published, named
			}
		} else {
			a.Err = errors.New("the Iceberg export is not running on this node")
		}
		w.ch <- a
	}
	waiters = rest
	if len(waiters) == 0 {
		return
	}
	if !s.gateOpen() {
		for _, w := range waiters {
			w.ch <- PassAnswer{Blocked: NotExportingReason}
		}
		return
	}
	for i, w := range waiters {
		if ctx.Err() != nil {
			// Shutdown: the asker's own context ends its wait.
			s.reqMu.Lock()
			s.confirms = append(s.confirms, waiters[i:]...)
			s.reqMu.Unlock()
			return
		}
		key := w.m.Database + "\x00" + w.m.Measurement
		if x, ok := s.source.(interface{ Exportable(Measurement) bool }); ok && !x.Exportable(w.m) {
			w.ch <- PassAnswer{NotExported: true}
			continue
		}
		if _, blocked := s.lastBlocked[key]; blocked {
			w.ch <- PassAnswer{Blocked: "the table awaits a namespace migration (iceberg.namespace_migration_dry_run)"}
			continue
		}
		if spacing := s.requestSpacing(); spacing > 0 {
			if since := time.Since(s.lastReconciled[key]); since < spacing {
				// Not now (requestSpacing): ask again when the spacing has
				// passed, without holding the loop.
				// One wake-up at a time, however often the loop comes by
				// here before it fires.
				s.reqMu.Lock()
				s.confirms = append(s.confirms, w)
				arm := !s.confirmWake
				s.confirmWake = true
				s.reqMu.Unlock()
				if arm {
					time.AfterFunc(spacing-since, func() {
						s.reqMu.Lock()
						s.confirmWake = false
						s.reqMu.Unlock()
						select {
						case s.wake <- struct{}{}:
						default:
						}
					})
				}
				continue
			}
		}
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		outcome, err := s.reconcileOneRecovered(mctx, w.m, key)
		cancel()
		a := PassAnswer{Outcome: outcome.String(), Err: err}
		s.noteOutcome(key, outcome)
		// The check has its own time: a pass that used all of its own must
		// not leave none to find out what it published.
		mctx, cancel = context.WithTimeout(ctx, measurementTimeout)
		if s.exporter != nil {
			exists, published, named, cerr := s.exporter.publishedNames(mctx, w.m.Database, w.m.Measurement, w.key)
			switch {
			case cerr != nil:
				if a.Err == nil {
					a.Err = cerr
				}
			case !exists:
				// No table: nothing names the file. A pass that failed
				// before it could create one is reported by Err.
				a.Published = err == nil
			default:
				a.Published, a.Named = published, named
			}
			// A ready question (no key, #1845 item 3): a pass that needed no
			// change wrote nothing, so a metadata directory that cannot be
			// written would only show once the job waits for the export.
			if w.key == "" && a.Err == nil && exists {
				a.WriteErr = s.exporter.probeMetadataWrite(mctx, w.m.Database, w.m.Measurement)
			}
		}
		cancel()
		s.logger.Debug().Str("database", w.m.Database).Str("measurement", w.m.Measurement).Str("outcome", a.Outcome).
			Bool("published", a.Published).Bool("names_file", a.Named).AnErr("pass_error", a.Err).
			Msg("Iceberg reconcile for a re-home job")
		w.ch <- a
	}
}

// probeMetadataWrite writes and deletes a small object in the table's
// metadata directory through the table's own file IO, the writer and
// credentials its next commit uses there (#1845 item 3). Loop goroutine only.
func (e *Exporter) probeMetadataWrite(ctx context.Context, database, measurement string) error {
	ctx = e.withAWS(ctx)
	tbl, err := e.loadTableRecovering(ctx, e.tableIdent(database, measurement))
	if err != nil {
		return fmt.Errorf("could not check that the table's metadata directory can be written: %w", err)
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return fmt.Errorf("could not check that the table's metadata directory can be written: %w", err)
	}
	w, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return nil // a read-only IO: nothing this check can try
	}
	metaLoc := tbl.MetadataLocation()
	dir := metaLoc[:strings.LastIndex(metaLoc, "/")+1]
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	loc := dir + ".arc-write-probe-" + hex.EncodeToString(nonce[:])
	werr := w.WriteFile(loc, []byte("probe"))
	// Deleted even after a failed write: an object store may have kept it.
	if derr := fs.Remove(loc); derr != nil && werr == nil && !errors.Is(derr, iofs.ErrNotExist) {
		e.logger.Warn().Err(derr).Str("location", loc).Msg("Iceberg: could not delete the write probe in the table's metadata directory; it is safe to remove by hand")
	}
	if werr != nil {
		return fmt.Errorf("the table's metadata directory cannot be written (%s): %w", dir, werr)
	}
	return nil
}

// publishedNames reports whether the table exists, whether its current
// version is the one the discovery files name, and whether that version
// names the data file at storage key fileKey. Loop goroutine only: it loads
// the table as a pass does.
func (e *Exporter) publishedNames(ctx context.Context, database, measurement, fileKey string) (exists, published, named bool, err error) {
	ctx = e.withAWS(ctx)
	exists, err = e.tableExists(ctx, database, measurement)
	if err != nil || !exists {
		return false, false, false, err
	}
	tbl, err := e.loadTableRecovering(ctx, e.tableIdent(database, measurement))
	if err != nil {
		return true, false, false, err
	}
	published, err = e.publishedIsCurrent(ctx, database, measurement, tbl)
	if err != nil {
		return true, false, false, err
	}
	if fileKey == "" {
		return true, published, false, nil
	}
	live, err := e.tableLiveFiles(ctx, tbl)
	if err != nil {
		return true, published, false, err
	}
	for p := range live {
		if pathIsKey(p, fileKey) {
			return true, published, true, nil
		}
	}
	return true, published, false, nil
}

// publishedIsCurrent reports whether the version the discovery files name
// has the same current snapshot as tbl, read from storage: the files a
// reader of the table sees are that snapshot's. (alreadyPublished answers
// from a pass's own memory, and only inside one.)
func (e *Exporter) publishedIsCurrent(ctx context.Context, database, measurement string, tbl *icetable.Table) (bool, error) {
	pub, found, err := e.LoadPublished(ctx, database, measurement)
	if err != nil || !found {
		return false, err
	}
	var meta struct {
		CurrentSnapshotID *int64 `json:"current-snapshot-id"`
	}
	if err := json.Unmarshal(pub.Metadata, &meta); err != nil {
		return false, fmt.Errorf("published Iceberg metadata %s: %w", pub.MetadataLocation, err)
	}
	cur := tbl.CurrentSnapshot()
	// An empty table: no snapshot, written as absent or as -1.
	if cur == nil {
		return meta.CurrentSnapshotID == nil || *meta.CurrentSnapshotID < 0, nil
	}
	return meta.CurrentSnapshotID != nil && *meta.CurrentSnapshotID == cur.SnapshotID, nil
}

// pathIsKey reports whether a table path (a URI or an absolute path) is the
// file at storage key key: the key itself, or the key after a "/". A file
// URI percent-encodes what a key does not (a space, a non-ASCII letter), so
// the path is compared decoded too.
func pathIsKey(p, key string) bool {
	if p == key || strings.HasSuffix(p, "/"+key) {
		return true
	}
	if u, err := url.PathUnescape(p); err == nil && u != p {
		return u == key || strings.HasSuffix(u, "/"+key)
	}
	return false
}

// isOneOf reports whether the file at table path p, whose storage key is
// key when the listing gave one, is one of keys.
func isOneOf(p, key string, keys []string) bool {
	for _, k := range keys {
		if key == k || pathIsKey(p, k) {
			return true
		}
	}
	return false
}

// SetReplaced installs the files a re-home job is replacing in a
// measurement: each old file's storage key with the keys of the files that
// hold its rows now, for the jobs whose reads already use the new files
// (#1825). A pass that removes such an old file commits those new files in
// the same commit, and commits neither if one of them cannot be registered
// or a file of that commit vanished. nil: no such jobs.
//
// It is asked when the pass computes its diff, so it holds only while the
// job is recorded: a job ended without the export's commit (its wait waived)
// leaves its old file's removal to a pass that commits it like any other.
func (e *Exporter) SetReplaced(f func(database, measurement string) map[string][]string) {
	e.replaced = f
}

// replacedOutputs returns the keys of the new files of every re-home job
// whose old file is among toRemove, and whether there is such a job.
func (e *Exporter) replacedOutputs(database, measurement string, toRemove []string) (outputs []string, hit bool) {
	if e.replaced == nil || len(toRemove) == 0 {
		return nil, false
	}
	for old, outs := range e.replaced(database, measurement) {
		for _, p := range toRemove {
			if pathIsKey(p, old) {
				outputs, hit = append(outputs, outs...), true
				break
			}
		}
	}
	return outputs, hit
}
