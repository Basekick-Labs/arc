package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/colname"
	"github.com/basekick-labs/arc/internal/metrics"
)

// measurementState caches per-measurement work between passes so an unchanged measurement is
// skipped without re-reading every Parquet footer, and a grown measurement re-reads only the
// footers of its newly-added local files (incremental schema derivation).
type measurementState struct {
	fingerprint string              // hash of the sorted file set + local-file set at last successful reconcile
	schema      ArcSchema           // schema derived at that reconcile (reused/extended while files persist)
	schemaFiles map[string]struct{} // schemaKey of every file at that reconcile — to diff for newly-added files
	// pruneAt, when set, is when the table's pass runs though its listing has
	// not changed: that reconcile left published versions on disk that only
	// time or a retry removes (reconcileResult.pruneAgainAt). Until then an
	// unchanged table is skipped like any other; without it a table that
	// goes quiet would keep them for good (#1751, #1787).
	pruneAt time.Time
	// drops is the table's stamp when this was cached
	// (Exporter.tableDropStamp: drops of its database in this process, and
	// changes of the table's lineage under it). A pass that finds another
	// number starts from the table, not from this (#1789, #1801).
	drops uint64
}

// unchanged reports that a pass that listed fp has nothing to do: the
// listing is the one this state was cached for, and no prune is due.
func (st measurementState) unchanged(fp string, now time.Time) bool {
	return st.fingerprint == fp && (st.pruneAt.IsZero() || now.Before(st.pruneAt))
}

// schemaKey identifies a file for schema derivation: its storage key and size. A different file
// written at a known path (a restore, an edge-sync promote) has its footer read again rather
// than inherit the old file's types. The key, not the URI: a file migrated to the cold tier keeps
// its key and bytes and changes only its URI, and must not cost a cold GET (#939 B2).
func schemaKey(key string, size int64) string {
	return key + "\x00" + strconv.FormatInt(size, 10)
}

// refSchemaKey is schemaKey for a listed file; a source that sets no Key falls back to the URI.
func refSchemaKey(f FileRef) string {
	if f.Key != "" {
		return schemaKey(f.Key, f.SizeBytes)
	}
	return schemaKey(f.PhysicalPath, f.SizeBytes)
}

// keyOfURIer is a source that can map a URI it produced back to its storage key.
type keyOfURIer interface {
	KeyOf(uri string) (string, bool)
}

// WriterGate gates the reconciler to a single node in cluster mode. nil means "no gate,
// allow" (OSS/standalone — single node, always runs). CanRun is checked every
// tick (not once at Start) so a lease change takes effect without a restart.
// Compaction's ClusterGate now does the same; it used to check only at Start,
// which is how two nodes ended up compacting the same partitions.
type WriterGate interface {
	// CanRun reports whether the local node may run the Iceberg reconcile pass.
	CanRun() bool
}

// Scheduler runs the Iceberg reconcile pass on a fixed interval. Each pass walks the current
// file set (via the FileSetSource) and reconciles every measurement's Iceberg table. It is
// safe to miss or fail a pass — the next pass re-derives the diff from durable state and
// converges (the reconciler is idempotent).
type Scheduler struct {
	exporter *Exporter
	source   FileSetSourceWithSchema
	interval time.Duration
	gate     WriterGate
	logger   zerolog.Logger

	// state caches per-measurement fingerprint+schema so unchanged measurements are skipped
	// without re-reading footers. Accessed only from the single loop() goroutine — no lock.
	state map[string]measurementState

	// gatedWarnOnce fires the first-gated WARN a single time per process
	// (#639 item 2): the per-tick Debug is invisible at default log levels,
	// so a cluster with no compactor-role node silently never exported.
	// Only the loop() goroutine touches it. A node demoted after having run
	// does not re-warn; the startup case is the observability gap.
	gatedWarnOnce bool
	// pauseWarned: the restore pause was reported (pausedForRestore).
	pauseWarned bool
	// wasGated: the gate was closed at the last check (loop goroutine). While
	// another node exports, this node's cached cold listings miss every
	// cold-removal notice (the requests are dropped at the gate), so they are
	// dropped when the gate opens again (#1728).
	wasGated bool

	// busySince is when each measurement last stopped making progress while busy: the first busy
	// pass after one that committed or settled. Loop goroutine only, like state.
	busySince map[string]time.Time
	// vanishTries counts a table's passes in a row that left files because
	// some vanished (noteVanished). Loop goroutine only.
	vanishTries map[string]int
	// leftOut is, per table, how many files its last pass that went through every new file
	// left out because no day could be given to them (file_day.go); tables with none have no
	// entry. Kept apart from state, which a pass that did not settle drops: the files are
	// still left out then. Loop goroutine only (reportLeftOut).
	leftOut map[string]int

	// requested holds the measurements whose files a compaction just replaced
	// (RequestReconcile), coalesced, until the loop goroutine reconciles
	// them: at idle, and between measurements of a running pass. Without it
	// the table kept naming the deleted inputs until the next timed pass,
	// up to the reconcile interval of failed external reads (#1722). The
	// loop, not the caller, runs them: state, busySince and the exporter's
	// per-pass version claims assume one reconcile at a time.
	reqMu     sync.Mutex
	requested map[string]Measurement
	// confirms are the questions of re-home jobs waiting for a pass
	// (scheduler_confirm.go), under reqMu.
	confirms []*confirmWaiter
	// confirmWake: a wake-up for a question held back by requestSpacing is
	// pending. Under reqMu.
	confirmWake bool
	// requestedPaths holds storage keys or directory prefixes another Arc
	// path removed, moved or rewrote (RequestReconcilePaths, #1727). The loop
	// maps them onto measurements the last pass listed: callers name data the
	// way storage lays it out, which is not always how the export names
	// measurements (an edge-sync spoke's "{spoke}/{db}" database, #634).
	requestedPaths map[string]struct{}
	// requestedHot holds path requests that need no cold re-list (a hot-only
	// removal on a shared bucket, RequestReconcilePathsLater); merged into a
	// pass's requests without dropping the cold listing.
	requestedHot map[string]struct{}
	// later is the delayed path requests, by delay bucket: one timer per
	// bucket however many keys arrive (RequestReconcilePathsLater).
	later [len(laterDelays)]laterBucket
	// retrying holds the measurements with a later request already
	// scheduled (requestAfter), so a burst does not start a timer each.
	retrying map[string]bool
	wake     chan struct{}
	// Ingest-triggered export (scheduler_ingest.go, #1750), all under reqMu:
	// ingestDue is the requests waiting and when each may run, ingestNext
	// the earliest next attempt per table, ingestPending when a file was
	// first reported for a table no reconcile has covered since.
	// ingestInterval is the spacing; 0 turns the trigger off.
	ingestInterval time.Duration
	ingestDue      map[string]ingestRequest
	ingestNext     map[string]time.Time
	ingestPending  map[string]ingestReports
	// The append path (scheduler_append.go, #1751). appendPending is the
	// files ingest reported per table (under reqMu). The rest is the loop
	// goroutine's: appendOK the tables whose last reconcile pass settled,
	// recentAdds the recent files in each table or seen by a pass,
	// appendFallback the last reason a table's append ran a pass instead.
	appendEnabled bool
	// ingestConcurrency is how many tables' appends run at once (1: one
	// request at a time, start to end, on the loop goroutine).
	ingestConcurrency int
	// passConcurrency is how many tables' reconcile passes run at once in
	// a timed pass and after an ingest batch (1: one at a time).
	passConcurrency int
	started         time.Time
	appendPending   map[string]pendingFiles
	appendOK        map[string]bool
	recentAdds      map[string]map[string]time.Time
	appendFallback  map[string]string
	// otherExporter: the tables another process was found committing to, on
	// a node with no election; warned about once each (#1795). Loop goroutine.
	otherExporter map[string]struct{}
	// passCommits is how many commits the reconcile pass in progress (or the
	// last one) made, a repair of entries held twice included (#1795): reconcileOne sets it, reconcileOneRecovered reads it.
	passCommits int
	// appendsSince counts each table's appends since its last settled pass.
	appendsSince map[string]int
	// lastReconciled is when each measurement's table last changed, by a
	// pass or a request. Loop goroutine only.
	lastReconciled map[string]time.Time
	// lastBlocked is the last pass's measurements awaiting a namespace
	// migration; a requested one is left to the pass. Loop goroutine only.
	lastBlocked map[string]struct{}
	// known is the measurements the last pass listed, the only ones a path
	// request can reach. Loop goroutine only.
	known []Measurement

	// Database drops (#1731, scheduler_drop.go): requested names (reqMu),
	// their retry state, the guard, and the databases dropped during the
	// running pass. Loop goroutine only, except drops.
	drops map[string]struct{}
	// dropped is the databases whose tables were dropped since the loop last
	// forgot what it remembers of them (noteDatabaseDropped; under reqMu).
	dropped     map[string]struct{}
	dropRetry   map[string]dropRetry
	dropGuard   func(ctx context.Context, database string) (bool, error)
	passDrops   map[string]struct{}
	backstopDue time.Time

	cancel context.CancelFunc
	done   chan struct{}
}

// measurementTimeout bounds a single measurement's reconcile so one slow/wedged measurement
// (e.g. a huge footer scan) can't block the whole pass or graceful shutdown indefinitely. It is a
// bound on PROGRESS, not on success: a measurement with more new files than fit in it registers
// them a chunk at a time and the next pass continues (#1686, batched.go).
const measurementTimeout = 2 * time.Minute

// partialRetryInterval is how soon the next pass starts after one that left a measurement
// partly registered, instead of the full reconcile interval: a first registration of a large
// measurement otherwise takes interval x passes. Capped at the interval itself.
func (s *Scheduler) partialRetryInterval() time.Duration {
	return min(s.interval, max(30*time.Second, s.interval/10))
}

// vanishRetryFirst is the first retry of a table whose pass left files for
// the next one because a file vanished under it (#1755): a compaction, a
// DELETE or retention deleting a file between the listing and the read.
// Such a race is usually over within seconds, and every file of the day it
// held back, the ones ingest wrote just now among them, waits for the retry.
// It doubles on each further such pass, up to partialRetryInterval, which
// is where the ordinary retry takes over.
const vanishRetryFirst = 5 * time.Second

// noteVanished asks for a table again soon after a pass that left files
// because some vanished, sooner each time than the ordinary retry, and
// forgets the count once a pass did not. Loop goroutine only.
func (s *Scheduler) noteVanished(job *passJob) {
	if !job.vanished {
		delete(s.vanishTries, job.key)
		return
	}
	metrics.Get().IncIcebergVanishedPass()
	// A table escalated as busy is left to the timed passes (serviceRequests).
	if since, started := s.busySince[job.key]; started && time.Since(since) >= busyEscalateAfter {
		return
	}
	n := s.vanishTries[job.key]
	d := vanishRetryFirst << min(n, 8)
	// Counted only when asked: a retry already pending (the ordinary one, or
	// a spaced request) leaves this step of the ladder for the next pass.
	if d < s.partialRetryInterval() && s.requestAfter(job.m, job.key, d) {
		s.vanishTries[job.key] = n + 1
	}
}

// reconcileOutcome is what reconcileOne did for one measurement.
type reconcileOutcome int

const (
	outcomeUnchanged  reconcileOutcome = iota // fingerprint unchanged, nothing to do
	outcomeReconciled                         // did work (settled or not)
	outcomeInProgress                         // registered part of the new files; continues next pass
	outcomeBusy                               // source files changed under the pass; nothing committed (vanished.go)
)

func (o reconcileOutcome) String() string {
	switch o {
	case outcomeUnchanged:
		return "unchanged"
	case outcomeReconciled:
		return "reconciled"
	case outcomeInProgress:
		return "in_progress"
	case outcomeBusy:
		return "busy"
	}
	return "unknown"
}

// busyEscalateAfter is how long a measurement may go on being busy — source files changing
// under every pass, nothing committed and nothing settled — before it is reported as failed. A
// daily compaction cycle over a large backlog is minutes; this is long enough not to call that a
// failure and short enough that an export blocked for good is seen the same day. The clock is per
// process: a restart or a writer failover starts it again.
const busyEscalateAfter = 30 * time.Minute

// fileExister is a source that can say whether a listed file is still in storage, through the
// file's own backend. The reconcile needs it to tell a file deleted mid-pass from a read failure.
type fileExister interface {
	FileExists(ctx context.Context, f FileRef) (bool, error)
}

// coldInvalidator is a source with a cached cold-tier listing.
type coldInvalidator interface {
	InvalidateCold(database string)
}

// FileSetSourceWithSchema is a FileSetSource that can also list the local files of a
// measurement for schema derivation, and (via FilesAndLocal) return both views from a single
// storage listing so a reconcile pass doesn't double-list every measurement. StorageWalkSource
// satisfies it.
type FileSetSourceWithSchema interface {
	FileSetSource
	LocalFiles(ctx context.Context, m Measurement) ([]string, error)
	// FilesAndLocal returns the measurement's data-file URIs and on-disk local paths from ONE
	// backend listing (Files + LocalFiles would list twice).
	FilesAndLocal(ctx context.Context, m Measurement) ([]FileRef, []string, error)
	// FileSchema derives one listed file's schema from its footer, wherever the file is.
	FileSchema(ctx context.Context, f FileRef) (ArcSchema, error)
}

// SchedulerConfig configures the reconcile scheduler.
type SchedulerConfig struct {
	Exporter *Exporter
	Source   FileSetSourceWithSchema
	Interval time.Duration
	Gate     WriterGate // nil = always run (OSS)
	Logger   zerolog.Logger
	// IngestInterval is the least time between two reconciles of one table
	// that ingest flushes cause (iceberg.ingest_export_interval). Zero or
	// less: RequestIngestReconcile does nothing.
	IngestInterval time.Duration
	// IngestAppend lets an ingest request add only the reported files
	// (iceberg.ingest_append_enabled). Off: every ingest request is a
	// reconcile pass of the measurement.
	IngestAppend bool
	// IngestConcurrency is how many tables' ingest appends run at once
	// (iceberg.ingest_export_concurrency). One or less: one ingest request
	// at a time, as before #1778.
	IngestConcurrency int
	// ReconcileConcurrency is how many tables' reconcile passes run at once
	// (iceberg.reconcile_concurrency). One or less: one at a time, on the
	// loop goroutine, as before #1786.
	ReconcileConcurrency int
}

// NewScheduler builds a reconcile scheduler.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	s := &Scheduler{
		exporter:          cfg.Exporter,
		source:            cfg.Source,
		interval:          cfg.Interval,
		gate:              cfg.Gate,
		logger:            cfg.Logger.With().Str("component", "iceberg-scheduler").Logger(),
		state:             make(map[string]measurementState),
		leftOut:           make(map[string]int),
		busySince:         make(map[string]time.Time),
		vanishTries:       make(map[string]int),
		requested:         make(map[string]Measurement),
		requestedPaths:    make(map[string]struct{}),
		requestedHot:      make(map[string]struct{}),
		retrying:          make(map[string]bool),
		wake:              make(chan struct{}, 1),
		ingestInterval:    cfg.IngestInterval,
		ingestDue:         make(map[string]ingestRequest),
		ingestNext:        make(map[string]time.Time),
		ingestPending:     make(map[string]ingestReports),
		appendEnabled:     cfg.IngestAppend,
		ingestConcurrency: max(1, cfg.IngestConcurrency),
		passConcurrency:   max(1, cfg.ReconcileConcurrency),
		started:           time.Now(),
		appendPending:     make(map[string]pendingFiles),
		appendOK:          make(map[string]bool),
		recentAdds:        make(map[string]map[string]time.Time),
		appendFallback:    make(map[string]string),
		otherExporter:     make(map[string]struct{}),
		appendsSince:      make(map[string]int),
		lastReconciled:    make(map[string]time.Time),
		drops:             make(map[string]struct{}),
		dropped:           make(map[string]struct{}),
		dropRetry:         make(map[string]dropRetry),
	}
	if cfg.Exporter != nil {
		// A database's tables dropped by anyone (the API's DELETE runs on
		// its own goroutine): the loop forgets what it remembers of them.
		cfg.Exporter.SetOnDatabaseDropped(s.noteDatabaseDropped)
	}
	return s
}

// requestMinSpacing is the least time between two reconciles of one table
// that a request can cause. Each reconcile that removes files publishes
// about two versions, and the exporter keeps retain+1 v<N> files for a
// reader that resolved version-hint.text and has not opened its version
// yet: back-to-back reconciles of a busy compaction cycle (one request per
// job) would prune that version from under it at retain_snapshots=1. A
// request inside the spacing waits out the rest of it. Short, because it is
// also how long a later job's deleted inputs can stay in the table: it only
// has to outlast a reader's hint-to-open step, not a query. Applied only at
// retain_snapshots 1 and 2 (requestSpacing): at the default 10 the 11 kept
// versions outlast any burst of compactions, and at 0 nothing is pruned.
var requestMinSpacing = 2 * time.Second // a variable so tests can shorten it

// spacedRetain is the largest retain_snapshots a request is spaced at.
const spacedRetain = 2

// requestSpacing is requestMinSpacing where back-to-back reconciles could
// prune a version a reader just resolved, and none elsewhere.
func (s *Scheduler) requestSpacing() time.Duration {
	if s.exporter == nil || s.exporter.retain < 1 || s.exporter.retain > spacedRetain {
		return 0
	}
	return requestMinSpacing
}

// requestAfter asks for m again after d, once per measurement at a time. It
// reports whether it did: false while another such request is pending.
func (s *Scheduler) requestAfter(m Measurement, key string, d time.Duration) bool {
	s.reqMu.Lock()
	if s.retrying[key] {
		s.reqMu.Unlock()
		return false
	}
	s.retrying[key] = true
	s.reqMu.Unlock()
	time.AfterFunc(d, func() {
		s.reqMu.Lock()
		delete(s.retrying, key)
		s.reqMu.Unlock()
		s.RequestReconcile(m.Database, m.Measurement)
	})
	return true
}

// RequestReconcile asks for one measurement to be reconciled soon, outside the
// timer: a compaction has just replaced files in it. Safe from any goroutine
// and never blocks; repeated requests before the loop gets to them coalesce. A
// node that may not export (the gate) drops them, and the timed passes still
// converge on their own.
func (s *Scheduler) RequestReconcile(database, measurement string) {
	if s == nil || database == "" || measurement == "" {
		return
	}
	s.reqMu.Lock()
	s.requested[database+"\x00"+measurement] = Measurement{Database: database, Measurement: measurement}
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// RequestReconcilePaths asks for the measurements holding the given storage
// keys (a file such as "db/cpu/2026/11/16/14/f.parquet") or directory
// prefixes ("db/", "db/cpu/") to be reconciled soon: a row DELETE, retention,
// a tiering move or another Arc path just removed, moved or rewrote them
// (#1727). Like RequestReconcile it never blocks and coalesces. The keys are
// mapped onto the measurements the last pass listed, at segment boundaries,
// on the loop goroutine; a key under no known measurement is dropped and the
// timed passes cover it. A caller never names a measurement itself: storage
// names an edge-sync spoke's data "{spoke}/{db}/{measurement}", and a request
// for the wrong (database, measurement) split would export a new table over
// the wrong prefix.
func (s *Scheduler) RequestReconcilePaths(keys []string) {
	if s == nil || len(keys) == 0 {
		return
	}
	norm := normalizeKeys(keys)
	if len(norm) == 0 {
		return
	}
	s.reqMu.Lock()
	for _, k := range norm {
		s.requestedPaths[k] = struct{}{}
	}
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// laterDelays are when RequestReconcilePathsLater asks: a writer deletes a
// shared bucket's objects after their manifest delete, one at a time, so the
// first ask can still list some. Each key waits at least each delay. A
// reconcile that sees files vanish under it is busy and asks again on its
// own; a deletion slower than the last delay leaves the object in the table
// until the timed pass. A variable so tests can shorten it.
var laterDelays = [3]time.Duration{2 * time.Second, 10 * time.Second, 30 * time.Second}

// laterBucket collects keys for one delay. Guarded by reqMu. Keys that
// arrive while its timer runs wait in next for a timer of their own, so
// every key waits at least the bucket's delay.
type laterBucket struct {
	keys  map[string]struct{}
	next  map[string]struct{}
	armed bool
}

// RequestReconcilePathsLater asks, like RequestReconcilePaths, for the
// measurements holding keys, but after each of laterDelays and without
// dropping the cold listing: for a change this node learns of before it
// happens in storage (a shared bucket's manifest delete, #1728). Never
// blocks; keys coalesce per bucket.
func (s *Scheduler) RequestReconcilePathsLater(keys []string) {
	if s == nil || len(keys) == 0 {
		return
	}
	norm := normalizeKeys(keys)
	if len(norm) == 0 {
		return
	}
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	for i := range s.later {
		b := &s.later[i]
		if b.armed {
			if b.next == nil {
				b.next = make(map[string]struct{})
			}
			for _, k := range norm {
				b.next[k] = struct{}{}
			}
			continue
		}
		if b.keys == nil {
			b.keys = make(map[string]struct{})
		}
		for _, k := range norm {
			b.keys[k] = struct{}{}
		}
		b.armed = true
		time.AfterFunc(laterDelays[i], func() { s.fireLater(i) })
	}
}

// RequestReconcilePathsHot is RequestReconcilePaths for a change that left
// the hot tier only (a local unlink, a rewrite's original): served at once,
// without dropping the cold listing, which a hot removal does not change and
// whose re-list would cost every cold object of the database (#1728).
func (s *Scheduler) RequestReconcilePathsHot(keys []string) {
	norm := normalizeKeys(keys)
	if s == nil || len(norm) == 0 {
		return
	}
	s.reqMu.Lock()
	for _, k := range norm {
		s.requestedHot[k] = struct{}{}
	}
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// normalizeKeys turns storage keys into the form measurementsFor matches at
// least cost: slash-separated, no leading "/", a file collapsed to its
// directory (a batch of files in one hour is one key), deduplicated.
func normalizeKeys(keys []string) []string {
	seen := make(map[string]bool, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.TrimLeft(filepath.ToSlash(k), "/")
		if strings.HasSuffix(k, ".parquet") {
			if i := strings.LastIndexByte(k, '/'); i >= 0 {
				k = k[:i+1]
			}
		}
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	return out
}

// fireLater moves a delay bucket's keys into the hot-only requests.
func (s *Scheduler) fireLater(i int) {
	s.reqMu.Lock()
	b := &s.later[i]
	for k := range b.keys {
		s.requestedHot[k] = struct{}{}
	}
	clear(b.keys)
	b.armed = false
	if len(b.next) > 0 {
		// Keys that came in while this timer ran get their full delay.
		b.keys, b.next = b.next, nil
		b.armed = true
		time.AfterFunc(laterDelays[i], func() { s.fireLater(i) })
	}
	s.reqMu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// takeHotPathRequests empties the hot-only path requests.
func (s *Scheduler) takeHotPathRequests() []string {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	out := make([]string, 0, len(s.requestedHot))
	for k := range s.requestedHot {
		out = append(out, k)
	}
	clear(s.requestedHot)
	return out
}

// takePathRequests empties the requested paths.
func (s *Scheduler) takePathRequests() []string {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	if len(s.requestedPaths) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.requestedPaths))
	for k := range s.requestedPaths {
		out = append(out, k)
	}
	clear(s.requestedPaths)
	return out
}

// measurementsFor maps storage keys onto the measurements the last pass
// listed or that were reconciled since (state): a key inside a measurement's directory reaches that measurement
// (the longest such, should two nest), and a directory key reaches every
// measurement under it. Matching is at "/" boundaries, so "db/cpu2/..." is
// not "db/cpu", nor "wh-other/" "wh". Loop goroutine only.
func (s *Scheduler) measurementsFor(keys []string) []Measurement {
	// The last pass's list, and the tables reconciled since (a compaction's
	// request can create one between passes).
	cands := make([]Measurement, 0, len(s.known)+len(s.state))
	dirs := make([]string, 0, cap(cands))
	inKnown := make(map[string]bool, len(s.known))
	for _, m := range s.known {
		inKnown[m.Database+"\x00"+m.Measurement] = true
		cands = append(cands, m)
	}
	for k := range s.state {
		if inKnown[k] {
			continue
		}
		if db, meas, ok := strings.Cut(k, "\x00"); ok {
			cands = append(cands, Measurement{Database: db, Measurement: meas})
		}
	}
	if len(keys) == 0 || len(cands) == 0 {
		return nil
	}
	for _, m := range cands {
		dirs = append(dirs, m.Database+"/"+m.Measurement+"/")
	}
	seen := map[string]bool{}
	var out []Measurement
	add := func(m Measurement) {
		k := m.Database + "\x00" + m.Measurement
		if !seen[k] {
			seen[k] = true
			out = append(out, m)
		}
	}
	for _, key := range keys {
		dir := strings.TrimSuffix(key, "/") + "/"
		best, bestLen := Measurement{}, 0
		for i, m := range cands {
			mdir := dirs[i]
			switch {
			case strings.HasPrefix(key, mdir):
				// The key is in this measurement's directory.
				if len(mdir) > bestLen {
					best, bestLen = m, len(mdir)
				}
			case strings.HasPrefix(mdir, dir):
				// The key is a directory holding this measurement.
				add(m)
			}
		}
		if bestLen > 0 {
			add(best)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Database != out[j].Database {
			return out[i].Database < out[j].Database
		}
		return out[i].Measurement < out[j].Measurement
	})
	return out
}

// takeRequests empties the requested set.
func (s *Scheduler) takeRequests() []Measurement {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	if len(s.requested) == 0 {
		return nil
	}
	out := make([]Measurement, 0, len(s.requested))
	for _, m := range s.requested {
		out = append(out, m)
	}
	clear(s.requested)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Database != out[j].Database {
			return out[i].Database < out[j].Database
		}
		return out[i].Measurement < out[j].Measurement
	})
	return out
}

// serviceRequests reconciles the requested measurements now, on the loop
// goroutine. A measurement reconciled less than requestSpacing ago, or
// left busy or partly registered, is asked for again later rather than
// bringing the whole next pass forward. Busy here does not start a
// measurement's busy clock (escalation stays the timed passes' call), and an
// escalated one is left to them. The gate is checked once per batch; the
// reconcile's own commit fence (canCommit) covers a lease lost meanwhile.
func (s *Scheduler) serviceRequests(ctx context.Context) {
	s.serviceConfirms(ctx)
	reqs := s.takeRequests()
	byPath := s.measurementsFor(s.takePathRequests())
	hotOnly := s.measurementsFor(s.takeHotPathRequests())
	if len(reqs) == 0 && len(byPath) == 0 && len(hotOnly) == 0 {
		return
	}
	if !s.gateOpen() {
		return // another node exports; its own passes converge
	}
	// A path request follows a change Arc made to the files, the cold tier's
	// included (cold retention, a cold DELETE, a tiering move): the cached
	// cold listing must not hide it from this reconcile.
	if ci, ok := s.source.(coldInvalidator); ok {
		dbs := map[string]bool{}
		for _, m := range byPath {
			if !dbs[m.Database] {
				dbs[m.Database] = true
				ci.InvalidateCold(m.Database)
			}
		}
	}
	queued := make(map[string]bool, len(reqs))
	for _, m := range reqs {
		queued[m.Database+"\x00"+m.Measurement] = true
	}
	for _, m := range append(byPath, hotOnly...) {
		if k := m.Database + "\x00" + m.Measurement; !queued[k] {
			queued[k] = true
			reqs = append(reqs, m)
		}
	}
	for i, m := range reqs {
		if ctx.Err() != nil {
			// Stopping (#1886): the shutdown drain serves the rest.
			s.requeueRequests(reqs[i:])
			return
		}
		key := m.Database + "\x00" + m.Measurement
		if _, blocked := s.lastBlocked[key]; blocked {
			continue // awaiting a namespace migration: the pass handles it
		}
		if spacing := s.requestSpacing(); spacing > 0 {
			if since := time.Since(s.lastReconciled[key]); since < spacing {
				s.requestAfter(m, key, spacing-since)
				continue
			}
		}
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		outcome, err := s.reconcileOneRecovered(mctx, m, key)
		cancel()
		s.noteOutcome(key, outcome)
		event := s.logger.Debug()
		if err != nil && outcome != outcomeBusy {
			event = logSchemaConflict(s.logger.Warn().Err(err), err)
		}
		event.Str("database", m.Database).Str("measurement", m.Measurement).Str("outcome", outcome.String()).
			Msg("Iceberg reconcile on request (a compaction, DELETE, retention or tiering change)")
		switch outcome {
		case outcomeInProgress:
			s.requestAfter(m, key, s.partialRetryInterval())
		case outcomeBusy:
			if since, started := s.busySince[key]; started && time.Since(since) >= busyEscalateAfter {
				continue // escalated by the passes: they own it now
			}
			s.requestAfter(m, key, s.partialRetryInterval())
		}
	}
}

// requeueRequests puts measurements taken by serviceRequests back in the
// queue, unserved.
func (s *Scheduler) requeueRequests(ms []Measurement) {
	s.reqMu.Lock()
	for _, m := range ms {
		s.requested[m.Database+"\x00"+m.Measurement] = m
	}
	s.reqMu.Unlock()
}

// noteOutcome records what a reconcile of key did: when the table last
// changed (requestSpacing), and the end of a busy stretch.
func (s *Scheduler) noteOutcome(key string, outcome reconcileOutcome) {
	if outcome == outcomeReconciled || outcome == outcomeInProgress {
		s.lastReconciled[key] = time.Now()
	}
	if outcome != outcomeBusy {
		delete(s.busySince, key)
	}
}

// gateOpen reports whether this node may export now. On the first open check
// after a closed one it drops every cached cold listing: one kept from an
// earlier turn as exporter has missed the cold removals since. It also takes
// every table off the append path (#1903): while another node exported, this
// node dropped its ingest reports and files its tables name may have gone
// (compaction inputs, retention, DELETE), so an append would publish the table
// as this node left it plus the new files. The next export of each table is a
// pass, which diffs it against the listing; one that settles puts it back.
func (s *Scheduler) gateOpen() bool {
	if s.gate != nil && !s.gate.CanRun() {
		s.wasGated = true
		return false
	}
	if s.wasGated {
		s.wasGated = false
		if ci, ok := s.source.(coldInvalidator); ok {
			ci.InvalidateCold("")
		}
		clear(s.appendOK)
	}
	return true
}

// Start launches the reconcile loop in the background. Stop cancels it.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go s.loop(ctx)
	s.logger.Info().Dur("interval", s.interval).Msg("Iceberg reconcile scheduler started")
}

// Stop halts the loop and waits for the in-flight pass to finish.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
}

func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	// Run one pass shortly after start so a fresh deployment publishes promptly.
	partial := s.runPass(ctx)
	// A Timer reset AFTER each pass (rather than a Ticker) guarantees a full interval of idle
	// between passes. With a Ticker, a pass that runs longer than the interval (slow disk, many
	// files) would queue ticks and cause back-to-back passes with no breathing room — starving
	// I/O and, in cluster mode, competing with the write path. The reconciler is idempotent, so
	// a slightly-later next pass is always safe.
	next := s.interval
	if partial {
		next = s.partialRetryInterval()
	}
	timer := time.NewTimer(next)
	defer timer.Stop()
	// One timer for every waiting ingest request (scheduler_ingest.go): armed
	// at the top of each turn to the earliest one due.
	ingestTimer := time.NewTimer(time.Hour)
	defer ingestTimer.Stop()
	for {
		s.armIngestTimer(ingestTimer)
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			// A request between passes: serve it now. The timer is not
			// touched, so the next timed pass keeps its time however many
			// requests come in. Drops first: they run whatever the gate.
			// Ingest requests last, and only the ones already due.
			if s.pausedForRestore() {
				continue
			}
			s.serviceDrops(ctx)
			s.serviceRequests(ctx)
			s.serviceIngest(ctx, ingestBatch)
			continue
		case <-ingestTimer.C:
			if s.pausedForRestore() {
				continue
			}
			s.serviceIngest(ctx, ingestBatch)
			continue
		case <-timer.C:
			partial = s.runPass(ctx)
		}
		next = s.interval
		if partial {
			next = s.partialRetryInterval()
		}
		timer.Reset(next)
	}
}

// pausedForRestore reports that a restore holds the export paused until the
// restart (restore_pause.go, #1813). The loop then does nothing: the waiting
// ingest requests are dropped, since the restart reconciles every table, and
// the pause is reported once rather than as every table being busy.
func (s *Scheduler) pausedForRestore() bool {
	if s.exporter == nil || !s.exporter.exportPausedForRestore() {
		return false
	}
	s.dropIngest()
	s.blockConfirms("the Iceberg export is paused until the server restarts (a restore staged a catalog)")
	clear(s.busySince)
	s.exporter.clearLastLag()
	if !s.pauseWarned {
		s.pauseWarned = true
		s.logger.Warn().Msg("Iceberg export is paused until the server restarts: a restore staged a catalog that replaces this one at the next start, and nothing is exported or dropped until then")
	}
	return true
}

// runPass reconciles every measurement once. Gated per-tick so cluster failover/demotion
// takes effect without a restart (nil gate = always run). It reports whether any measurement
// was left partly registered, so the loop can come back sooner.
func (s *Scheduler) runPass(ctx context.Context) (partial bool) {
	if s.pausedForRestore() {
		return false
	}
	// A deleted database's tables are dropped on every node, exporting or not
	// (#1731): this node's warehouse is its own.
	s.passDrops = nil
	s.serviceDrops(ctx)
	if !s.gateOpen() {
		if !s.gatedWarnOnce {
			s.gatedWarnOnce = true
			s.logger.Warn().Msg("Iceberg reconcile is gated off on this node (not the active compactor). If no node in the cluster holds a compactor role, Iceberg export never runs; assign one to enable it")
		}
		s.logger.Debug().Msg("Iceberg reconcile skipped: node is not the active writer")
		clear(s.busySince) // another node exports meanwhile; a later turn starts its own clock
		if s.exporter != nil {
			s.exporter.clearLastLag() // its publishes count now, not this node's (#1772)
		}
		metrics.Get().SetIcebergFilesLeftOut(0) // the exporting node reports them
		return false
	}
	measurements, err := s.source.Measurements(ctx)
	if err == nil {
		s.known = measurements
	}
	if err != nil {
		s.logger.Error().Err(err).Msg("Iceberg reconcile: failed to enumerate measurements")
		return false
	}
	// Bounded like a measurement, and for the same reason: this reads the shared SQLite catalog
	// (auth, audit and tiering write to it too), and an unbounded call here would hold the whole
	// pass open until shutdown rather than until the next tick (#1154).
	nsctx, nscancel := context.WithTimeout(ctx, measurementTimeout)
	blocked, err := s.exporter.MigrateDottedNamespaces(nsctx, measurements)
	nscancel()
	if err != nil {
		// Do NOT return: the only error source is reading the catalog table list, and on a
		// SQLITE_BUSY against the shared DB that would skip every measurement on the node rather
		// than the few a migration concerns. An empty blocked set is the safe fallback -- a
		// legacy dotted table is then reconciled, which at worst creates the duplicate the
		// migration would have prevented, so the warning names that risk explicitly.
		s.logger.Warn().Err(err).
			Msg("Iceberg reconcile: could not read the catalog to plan dotted-namespace migration; continuing without it. A database whose Iceberg namespace contains a dot may get a second table this pass")
		blocked = nil
	}
	s.lastBlocked = blocked
	var ok, failed, skipped, awaitingMigration, inProgress, busy int
	seen := make(map[string]struct{}, len(measurements))
	// What follows a table's pass: when it last changed, its busy stretch,
	// the pass's counters and the log line.
	afterPass := func(m Measurement, key string, outcome reconcileOutcome, err error) {
		if outcome == outcomeReconciled || outcome == outcomeInProgress {
			s.lastReconciled[key] = time.Now()
		}
		if outcome == outcomeBusy {
			since, started := s.busySince[key]
			if !started {
				since = time.Now()
				s.busySince[key] = since
			}
			if time.Since(since) < busyEscalateAfter {
				// Not a failure: another Arc writer path is rewriting this measurement, and the
				// next pass, which lists again, sees what it left. Soon, on the short interval.
				s.logger.Info().Err(err).
					Str("database", m.Database).Str("measurement", m.Measurement).
					Dur("retry_in", s.partialRetryInterval()).
					Msg("Iceberg reconcile: this measurement's files are changing under the export (compaction, delete or retention running); retrying soon")
				busy++
				return
			}
			// Escalated: still not an error in the export itself, but nothing has landed for this
			// measurement for too long to keep calling it transient. Counted as failed, and it no
			// longer brings passes forward.
			s.logger.Warn().Err(err).
				Str("database", m.Database).Str("measurement", m.Measurement).
				Time("busy_since", since).Dur("busy_for", time.Since(since).Round(time.Second)).
				Msg("Iceberg reconcile: this measurement's files have kept changing under the export without anything committed or settled; counting it as failed")
			failed++
			return
		} else {
			delete(s.busySince, key)
		}
		switch {
		case err != nil:
			// Log and continue — one bad measurement must not block the rest; next pass retries.
			logSchemaConflict(s.logger.Error().Err(err), err).
				Str("database", m.Database).Str("measurement", m.Measurement).
				Msg("Iceberg reconcile: measurement failed")
			failed++
		case outcome == outcomeInProgress:
			// Not a failure: what was registered is committed, and the next pass continues.
			// The exporter logged the counts.
			inProgress++
		case outcome == outcomeReconciled:
			ok++
		default:
			skipped++ // fingerprint unchanged — nothing to do (the cheap common case)
		}
	}
	for next := 0; next < len(measurements); {
		if ctx.Err() != nil { // shutdown mid-pass
			return false
		}
		// A compaction that finished while this pass runs is served before the
		// next measurement, not after the whole pass (#1722). A database
		// dropped in this pass (#1731) is not reconciled again in it: a drop
		// between measurements happened after the listing, and one at pass
		// start leaves nothing to list.
		s.serviceDrops(ctx)
		s.serviceRequests(ctx)
		// One ingest request that needs a pass at most (and one batch of
		// appends), so a backlog of them cannot hold the pass
		// (scheduler_ingest.go).
		s.serviceIngest(ctx, 1)
		// The next tables to reconcile, up to passConcurrency of them. Taken
		// after the services above and right before they run: a service can
		// reconcile one of them, and what the pass starts from must be what
		// that left (preparePass).
		var batch []Measurement
		for next < len(measurements) && len(batch) < s.passConcurrency {
			m := measurements[next]
			next++
			if s.droppedThisPass(m.Database) {
				continue
			}
			key := m.Database + "\x00" + m.Measurement
			seen[key] = struct{}{}
			if _, blocked := blocked[key]; blocked {
				// Counted apart from `skipped`, which means "fingerprint unchanged, nothing to do".
				// Folding the two together would report a permanently unexported measurement in the
				// same number as a healthy no-op, and the pass summary would look clean. The
				// per-measurement reason is logged by MigrateDottedNamespaces, which has it; this
				// layer only counts (one log line per fact -- SQLite checklist item 7).
				awaitingMigration++
				continue
			}
			batch = append(batch, m)
		}
		if len(batch) == 0 {
			continue
		}
		// Re-checked per batch, not only per pass: a pass over many measurements on
		// object storage takes minutes, and a lease that moved meanwhile would otherwise leave
		// two nodes writing the same tables until this one finished (#939).
		if s.gate != nil && !s.gate.CanRun() {
			s.logger.Info().Msg("Iceberg reconcile: this node stopped being the active writer mid-pass; stopping the pass")
			clear(s.busySince)
			return false
		}
		if len(batch) == 1 {
			// One table: on this goroutine, start to end, as always.
			m := batch[0]
			key := m.Database + "\x00" + m.Measurement
			mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
			outcome, err := s.reconcileOneRecovered(mctx, m, key)
			cancel()
			afterPass(m, key, outcome, err)
			continue
		}
		// Several at once (scheduler_pass.go): this goroutine waits for all
		// of them, then records each in the order of the listing.
		jobs := make([]*passJob, len(batch))
		for i, m := range batch {
			jobs[i] = s.preparePass(m, m.Database+"\x00"+m.Measurement)
		}
		s.runPasses(ctx, jobs)
		for _, job := range jobs {
			s.finishPass(job)
			afterPass(job.m, job.key, job.outcome, job.err)
		}
	}
	// Drop cache entries for measurements that no longer exist (bounded memory).
	for k := range s.state {
		if _, ok := seen[k]; !ok {
			delete(s.state, k)
		}
	}
	for k := range s.busySince {
		if _, ok := seen[k]; !ok {
			delete(s.busySince, k)
		}
	}
	for k := range s.vanishTries {
		if _, ok := seen[k]; !ok {
			delete(s.vanishTries, k)
		}
	}
	for k := range s.appendOK {
		if _, ok := seen[k]; !ok {
			s.forgetAppend(k)
		}
	}
	for k := range s.recentAdds {
		if _, ok := seen[k]; !ok {
			s.forgetAppend(k)
		}
	}
	for k := range s.lastReconciled {
		if _, ok := seen[k]; !ok {
			delete(s.lastReconciled, k)
		}
	}
	for k := range s.leftOut {
		if _, ok := seen[k]; !ok {
			delete(s.leftOut, k)
		}
	}
	s.reportLeftOut()
	event := s.logger.Info().Int("reconciled", ok).Int("unchanged", skipped).Int("failed", failed)
	if awaitingMigration > 0 {
		event = event.Int("awaiting_namespace_migration", awaitingMigration)
	}
	if inProgress > 0 {
		event = event.Int("in_progress", inProgress)
	}
	if busy > 0 {
		event = event.Int("busy", busy)
	}
	event.Msg("Iceberg reconcile pass complete")
	// An escalated measurement counts as failed and does not bring the next pass forward.
	return inProgress > 0 || busy > 0
}

// reconcileOneRecovered runs reconcileOne and returns a panic on this
// goroutine as the measurement's error, so one measurement whose files trip a
// bug fails its pass instead of ending the process (a pass that runs beside
// others recovers its own, passRunner.run). Footer reads recover their own panics too (schemaFromReader),
// including on worker goroutines this cannot reach.
func (s *Scheduler) reconcileOneRecovered(ctx context.Context, m Measurement, key string) (outcome reconcileOutcome, err error) {
	// Deferred first, so it runs last and sees what a recovered panic turned
	// the result into: whichever kind of reconcile this is, one that went
	// through covers the ingest files reported before it started (#1750).
	startedAt := time.Now()
	defer func() {
		s.notePassEnded(key, startedAt, outcome, err)
		s.noteIngestCovered(key, startedAt, outcome, s.passCommits > 0, err)
	}()
	s.passCommits = 0
	defer func() {
		if r := recover(); r != nil {
			// The stack is logged here, where it is; the returned error is
			// logged by runPass as the measurement's failure.
			s.logger.Error().Str("database", m.Database).Str("measurement", m.Measurement).
				Str("panic", fmt.Sprint(r)).Str("stack", string(debug.Stack())).
				Msg("Iceberg reconcile: recovered a panic")
			outcome, err = outcomeUnchanged, fmt.Errorf("reconcile panicked: %v", r)
		}
	}()
	return s.reconcileOne(ctx, m, key)
}

// reconcileOne reconciles one measurement, returning whether it did work (changed=true) or was
// skipped because its file set is unchanged since the last successful pass. The fingerprint of
// the (data-file set + local-file set) lets us skip the expensive per-file footer reads and the
// no-op diff entirely when nothing changed — the common steady-state case.
func (s *Scheduler) reconcileOne(ctx context.Context, m Measurement, key string) (outcome reconcileOutcome, err error) {
	// One table, start to end, on the loop goroutine: the three steps a
	// batch runs for several tables at once (scheduler_pass.go, #1786).
	job := s.preparePass(m, key)
	// Deferred so that reconcileOneRecovered's own deferred bookkeeping,
	// which reads the pass's commits, always runs after it.
	defer s.applyPassEffects(job)
	job.outcome, job.err = s.passRunner(unionSchemaConcurrency).reconcile(ctx, job, s.exporter)
	s.applyPassState(job)
	return job.outcome, job.err
}

// canRun reports whether this node may still write Iceberg metadata (nil gate = always).
func (s *Scheduler) canRun() bool {
	return s.gate == nil || s.gate.CanRun()
}

// fingerprint hashes the sorted union of the data files (path + size) and the local-file
// paths. Any add, remove, or compaction changes the set and thus the hash; an unchanged set
// hashes identically. Size is part of the key because a file can be rewritten at its own path:
// the delete API's partial-match branch did that before #1073 publishes a fresh path (#633),
// and restores and edge-sync still do. With paths alone such a measurement looked unchanged
// forever and the Iceberg manifest kept describing the old content.
func fingerprint(files []FileRef, localFiles []string) string {
	parts := make([]string, 0, len(files)+len(localFiles))
	for _, f := range files {
		parts = append(parts, f.PhysicalPath+"\x00"+strconv.FormatInt(f.SizeBytes, 10))
	}
	parts = append(parts, localFiles...)
	sort.Strings(parts)
	h := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(h[:])
}

// unionFileSchemas reads and merges the files' schemas (passRunner.unionFileSchemas).
func (s *Scheduler) unionFileSchemas(ctx context.Context, files []FileRef) (ArcSchema, error) {
	return s.passRunner(unionSchemaConcurrency).unionFileSchemas(ctx, files)
}

// fileRefKey is the storage key of a file, or its physical path when the source has none.
func fileRefKey(f FileRef) string {
	if f.Key != "" {
		return f.Key
	}
	return f.PhysicalPath
}

// logSchemaConflict adds a SchemaConflictError's fields to a log event (#1744). The error text
// is sanitized like every logged error, which masks the quoted column name; these structured
// fields carry it, the two types and the files, so the operator can tell what to fix.
func logSchemaConflict(ev *zerolog.Event, err error) *zerolog.Event {
	var c *SchemaConflictError
	if !errors.As(err, &c) {
		return ev
	}
	ev = ev.Str("column", colname.Quote(c.Column)).Str("type_first", c.First).Str("type_then", c.Then).Str("conflict_with", c.With)
	if c.FirstFile != "" {
		ev = ev.Str("file_first", c.FirstFile)
	}
	if c.File != "" {
		ev = ev.Str("file_then", c.File)
	}
	return ev
}
