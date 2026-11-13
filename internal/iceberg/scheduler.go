package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

// measurementState caches per-measurement work between passes so an unchanged measurement is
// skipped without re-reading every Parquet footer, and a grown measurement re-reads only the
// footers of its newly-added local files (incremental schema derivation).
type measurementState struct {
	fingerprint string              // hash of the sorted file set + local-file set at last successful reconcile
	schema      ArcSchema           // schema derived at that reconcile (reused/extended while files persist)
	schemaFiles map[string]struct{} // schemaKey of every file at that reconcile — to diff for newly-added files
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

	cancel context.CancelFunc
	done   chan struct{}
}

// measurementTimeout bounds a single measurement's reconcile so one slow/wedged measurement
// (e.g. a huge footer scan) can't block the whole pass or graceful shutdown indefinitely.
const measurementTimeout = 2 * time.Minute

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
}

// NewScheduler builds a reconcile scheduler.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	return &Scheduler{
		exporter: cfg.Exporter,
		source:   cfg.Source,
		interval: cfg.Interval,
		gate:     cfg.Gate,
		logger:   cfg.Logger.With().Str("component", "iceberg-scheduler").Logger(),
		state:    make(map[string]measurementState),
	}
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
	s.runPass(ctx)
	// A Timer reset AFTER each pass (rather than a Ticker) guarantees a full interval of idle
	// between passes. With a Ticker, a pass that runs longer than the interval (slow disk, many
	// files) would queue ticks and cause back-to-back passes with no breathing room — starving
	// I/O and, in cluster mode, competing with the write path. The reconciler is idempotent, so
	// a slightly-later next pass is always safe.
	timer := time.NewTimer(s.interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.runPass(ctx)
			timer.Reset(s.interval)
		}
	}
}

// runPass reconciles every measurement once. Gated per-tick so cluster failover/demotion
// takes effect without a restart (nil gate = always run).
func (s *Scheduler) runPass(ctx context.Context) {
	if s.gate != nil && !s.gate.CanRun() {
		if !s.gatedWarnOnce {
			s.gatedWarnOnce = true
			s.logger.Warn().Msg("Iceberg reconcile is gated off on this node (not the active compactor). If no node in the cluster holds a compactor role, Iceberg export never runs; assign one to enable it")
		}
		s.logger.Debug().Msg("Iceberg reconcile skipped: node is not the active writer")
		return
	}
	measurements, err := s.source.Measurements(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Iceberg reconcile: failed to enumerate measurements")
		return
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
	var ok, failed, skipped, awaitingMigration int
	seen := make(map[string]struct{}, len(measurements))
	for _, m := range measurements {
		if ctx.Err() != nil { // shutdown mid-pass
			return
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
		// Re-checked per measurement, not only per pass: a pass over many measurements on
		// object storage takes minutes, and a lease that moved meanwhile would otherwise leave
		// two nodes writing the same tables until this one finished (#939).
		if s.gate != nil && !s.gate.CanRun() {
			s.logger.Info().Msg("Iceberg reconcile: this node stopped being the active writer mid-pass; stopping the pass")
			return
		}
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		changed, err := s.reconcileOneRecovered(mctx, m, key)
		cancel()
		switch {
		case err != nil:
			// Log and continue — one bad measurement must not block the rest; next pass retries.
			s.logger.Error().Err(err).
				Str("database", m.Database).Str("measurement", m.Measurement).
				Msg("Iceberg reconcile: measurement failed")
			failed++
		case changed:
			ok++
		default:
			skipped++ // fingerprint unchanged — nothing to do (the cheap common case)
		}
	}
	// Drop cache entries for measurements that no longer exist (bounded memory).
	for k := range s.state {
		if _, ok := seen[k]; !ok {
			delete(s.state, k)
		}
	}
	event := s.logger.Info().Int("reconciled", ok).Int("unchanged", skipped).Int("failed", failed)
	if awaitingMigration > 0 {
		event = event.Int("awaiting_namespace_migration", awaitingMigration)
	}
	event.Msg("Iceberg reconcile pass complete")
}

// reconcileOneRecovered runs reconcileOne and returns a panic on this
// goroutine as the measurement's error, so one measurement whose files trip a
// bug fails its pass instead of ending the process (the reconcile loop has no
// other recover). Footer reads recover their own panics too (schemaFromReader),
// including on worker goroutines this cannot reach.
func (s *Scheduler) reconcileOneRecovered(ctx context.Context, m Measurement, key string) (changed bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			// The stack is logged here, where it is; the returned error is
			// logged by runPass as the measurement's failure.
			s.logger.Error().Str("database", m.Database).Str("measurement", m.Measurement).
				Str("panic", fmt.Sprint(r)).Str("stack", string(debug.Stack())).
				Msg("Iceberg reconcile: recovered a panic")
			changed, err = false, fmt.Errorf("reconcile panicked: %v", r)
		}
	}()
	return s.reconcileOne(ctx, m, key)
}

// reconcileOne reconciles one measurement, returning whether it did work (changed=true) or was
// skipped because its file set is unchanged since the last successful pass. The fingerprint of
// the (data-file set + local-file set) lets us skip the expensive per-file footer reads and the
// no-op diff entirely when nothing changed — the common steady-state case.
func (s *Scheduler) reconcileOne(ctx context.Context, m Measurement, key string) (changed bool, err error) {
	// One backend listing yields both the data-file URIs and the local paths (Files + LocalFiles
	// would list the same prefix twice).
	files, localFiles, err := s.source.FilesAndLocal(ctx, m)
	if err != nil {
		return false, err
	}
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
		if prev, cached := s.state[key]; cached && prev.fingerprint == fp {
			return false, nil
		}
		// Only for a table that already exists: an empty directory that never held data must not
		// mint a zero-column table (EnsureTable would create one with no fields and no partition
		// spec). Schema is irrelevant when emptying — the existing table's schema is preserved.
		exists, err := s.exporter.tableExists(ctx, m.Database, m.Measurement)
		if err != nil {
			return false, err
		}
		if !exists {
			// Cache the negative result too: an empty directory with no table has no
			// catalog state to reconcile, so unchanged passes need no catalog lookup.
			s.state[key] = measurementState{fingerprint: fp}
			return false, nil
		}
		settled, err := s.exporter.ReconcileMeasurementWithHint(ctx, m.Database, m.Measurement, ArcSchema{}, nil)
		if err != nil {
			return false, err
		}
		s.logger.Info().Str("database", m.Database).Str("measurement", m.Measurement).
			Msg("Iceberg reconcile: all data files gone — table reconciled to empty")
		// Cache the empty state (nil schema/localFiles) so a permanently-empty
		// measurement is emptied once, but only once the discovery files are
		// published — see the equivalent guard on the main path below.
		if !settled {
			delete(s.state, key)
			return true, nil
		}
		s.state[key] = measurementState{fingerprint: fp}
		return true, nil
	}
	// Fingerprint the file set. Unchanged fingerprint + a cached schema => guaranteed no-op
	// (same files in the table already), so skip the footer reads and the diff.
	fp := fingerprint(files, localFiles)
	prev, cached := s.state[key]
	if cached && prev.fingerprint == fp {
		return false, nil
	}

	// The set changed (or first sight): derive the union schema, reading only the footers of
	// files not already accounted for — avoiding an O(N) re-read of every footer every time the
	// set grows, local or remote. (Removed files can't remove columns: the table schema is a
	// superset and all fields are optional, so dropping a file never narrows the schema.)
	//
	// What is accounted for: with a cache, the files of the last reconcile. Without one (first
	// sight, a restart), the table's own schema and the files its current snapshot holds at
	// their size: a restart then reads no footer for an unchanged table, and on an object store
	// it does not GET the tail of every file the measurement has ever exported.
	base, known := prev.schema, prev.schemaFiles
	if !cached || known == nil {
		tableSc, tableFiles, ok, err := s.exporter.tableSeed(ctx, m.Database, m.Measurement)
		if err != nil {
			return false, err
		}
		base, known = ArcSchema{}, nil
		if ok {
			base = tableSc
			known = make(map[string]struct{}, len(tableFiles))
			keyer, _ := s.source.(keyOfURIer)
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
	sc := base
	if len(unread) > 0 {
		addSc, err := s.unionFileSchemas(ctx, unread)
		if err != nil {
			return false, err
		}
		if sc, err = MergeSchemas(base, addSc); err != nil {
			return false, err
		}
	}

	settled, err := s.exporter.ReconcileMeasurementWithHint(ctx, m.Database, m.Measurement, sc, files)
	if err != nil {
		return false, err
	}

	// Cache only when the pass fully settled. The snapshot is committed either way, but
	// caching an unsettled pass would gate this measurement out of every later one — and if
	// its file set then goes quiet (the steady state) it would stay unsettled indefinitely.
	// Leaving the entry uncached costs one repeated reconcile per tick until it succeeds.
	//
	// Two things can leave a pass unsettled, and the exporter has already logged which:
	// unpublished reader discovery files, or a snapshot expiry that failed on a pass whose
	// removals made the older snapshots unreadable (#1092). Both need the same retry, so this
	// does not name either — a message claiming the hint when the expiry failed would send an
	// operator to the wrong place.
	if !settled {
		delete(s.state, key)
		s.logger.Warn().
			Str("database", m.Database).Str("measurement", m.Measurement).
			Msg("Iceberg reconcile: pass did not fully settle (see the preceding error for which step); will retry next pass")
		return true, nil
	}

	schemaSet := make(map[string]struct{}, len(files))
	for _, f := range files {
		schemaSet[refSchemaKey(f)] = struct{}{}
	}
	s.state[key] = measurementState{fingerprint: fp, schema: sc, schemaFiles: schemaSet}
	return true, nil
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

// unionFileSchemas derives the union schema of files through the source's FileSchema, reading
// footers in parallel (bounded, as UnionSchema does) and folding them in the files' order so
// column order and conflict detection do not depend on scheduling.
func (s *Scheduler) unionFileSchemas(ctx context.Context, files []FileRef) (ArcSchema, error) {
	schemas := make([]ArcSchema, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(unionSchemaConcurrency)
	for i, f := range files {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			sc, err := s.source.FileSchema(gctx, f)
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
	for _, sc := range schemas {
		var err error
		if out, err = MergeSchemas(out, sc); err != nil {
			return ArcSchema{}, err
		}
	}
	return out, nil
}
