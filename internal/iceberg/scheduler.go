package iceberg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// measurementState caches per-measurement work between passes so an unchanged measurement is
// skipped without re-reading every Parquet footer, and a grown measurement re-reads only the
// footers of its newly-added local files (incremental schema derivation).
type measurementState struct {
	fingerprint string              // hash of the sorted file set + local-file set at last successful reconcile
	schema      ArcSchema           // schema derived at that reconcile (reused/extended while files persist)
	localFiles  map[string]struct{} // local-file set at that reconcile — to diff for newly-added files
}

// WriterGate gates the reconciler to a single node in cluster mode. nil means "no gate,
// allow" (OSS/standalone — single node, always runs). Mirrors compaction's ClusterGate:
// CanRun is checked every tick (not once at Start) so failover takes effect without restart.
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
		s.logger.Debug().Msg("Iceberg reconcile skipped: node is not the active writer")
		return
	}
	measurements, err := s.source.Measurements(ctx)
	if err != nil {
		s.logger.Error().Err(err).Msg("Iceberg reconcile: failed to enumerate measurements")
		return
	}
	var ok, failed, skipped int
	seen := make(map[string]struct{}, len(measurements))
	for _, m := range measurements {
		if ctx.Err() != nil { // shutdown mid-pass
			return
		}
		key := m.Database + "\x00" + m.Measurement
		seen[key] = struct{}{}
		mctx, cancel := context.WithTimeout(ctx, measurementTimeout)
		changed, err := s.reconcileOne(mctx, m, key)
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
	s.logger.Info().Int("reconciled", ok).Int("unchanged", skipped).Int("failed", failed).
		Msg("Iceberg reconcile pass complete")
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
		if !s.exporter.TableExists(ctx, m.Database, m.Measurement) {
			return false, nil
		}
		if err := s.exporter.ReconcileMeasurement(ctx, m.Database, m.Measurement, ArcSchema{}, nil); err != nil {
			return false, err
		}
		s.logger.Info().Str("database", m.Database).Str("measurement", m.Measurement).
			Msg("Iceberg reconcile: all data files gone — table reconciled to empty")
		// Cache the empty state (nil schema/localFiles): a later re-ingest changes the
		// fingerprint and falls through to a full re-derivation below.
		s.state[key] = measurementState{fingerprint: fp}
		return true, nil
	}
	if len(localFiles) == 0 {
		// Files exist but none are local, so we cannot derive a schema (cold-only measurement).
		// Skip in v1 — documented limitation.
		s.logger.Debug().Str("database", m.Database).Str("measurement", m.Measurement).
			Msg("Iceberg reconcile: no local file for schema derivation, skipping")
		return false, nil
	}

	// Fingerprint the file set. Unchanged fingerprint + a cached schema => guaranteed no-op
	// (same files in the table already), so skip the footer reads and the diff.
	fp := fingerprint(files, localFiles)
	prev, cached := s.state[key]
	if cached && prev.fingerprint == fp {
		return false, nil
	}

	// The set changed (or first sight): derive the union schema. When we have a cached schema,
	// only read footers of the LOCAL files that are new since last reconcile and merge them into
	// the cached schema — avoiding an O(N) re-read of every footer every time the set grows.
	// (Removed files can't remove columns: the table schema is a superset and all fields are
	// optional, so dropping a file never narrows the schema.) First sight, or a cache with no
	// remembered local set, falls back to a full UnionSchema.
	var sc ArcSchema
	if cached && prev.localFiles != nil {
		var newLocal []string
		for _, lp := range localFiles {
			if _, ok := prev.localFiles[lp]; !ok {
				newLocal = append(newLocal, lp)
			}
		}
		if len(newLocal) == 0 {
			sc = prev.schema // only removals — schema unchanged
		} else {
			addSc, err := UnionSchema(newLocal)
			if err != nil {
				return false, err
			}
			sc, err = MergeSchemas(prev.schema, addSc)
			if err != nil {
				return false, err
			}
		}
	} else {
		sc, err = UnionSchema(localFiles)
		if err != nil {
			return false, err
		}
	}

	if err := s.exporter.ReconcileMeasurement(ctx, m.Database, m.Measurement, sc, files); err != nil {
		return false, err
	}

	localSet := make(map[string]struct{}, len(localFiles))
	for _, lp := range localFiles {
		localSet[lp] = struct{}{}
	}
	s.state[key] = measurementState{fingerprint: fp, schema: sc, localFiles: localSet}
	return true, nil
}

// fingerprint hashes the sorted union of the data-file paths and local-file paths. Any add,
// remove, or compaction changes the set and thus the hash; an unchanged set hashes identically.
func fingerprint(files []FileRef, localFiles []string) string {
	parts := make([]string, 0, len(files)+len(localFiles))
	for _, f := range files {
		parts = append(parts, f.PhysicalPath)
	}
	parts = append(parts, localFiles...)
	sort.Strings(parts)
	h := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(h[:])
}
