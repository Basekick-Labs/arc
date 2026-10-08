package tiering

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Manager orchestrates tiered storage operations
type Manager struct {
	// Storage backends
	hotBackend  storage.Backend
	coldBackend storage.Backend

	// onMigrationComplete, when set, runs after a migration cycle that moved
	// or deleted at least one tier file. The query layer uses it to invalidate
	// pruner and SQL transform caches, whose cached partition paths go stale
	// the moment a file changes tier (a cached hot hour glob that no longer
	// matches any file makes DuckDB error on the next query within the cache
	// TTL). Guarded by callbackMu: it is wired from main after the scheduler
	// goroutine already runs.
	callbackMu          sync.Mutex
	onMigrationComplete func()

	// onHotFilesRemoved, when set, runs with the storage paths of hot files
	// the tiering subsystem is about to permanently remove (migration source
	// deletes, orphan reconciliation). The edge-sync hub wires it to receipt
	// marking (#687): a spoke-synced file's receipt must be marked before its
	// hot copy disappears, or confirmPresent forgets the receipt and the
	// spoke re-uploads a duplicate. An error return means the caller MUST NOT
	// delete the files. Guarded by callbackMu.
	onHotFilesRemoved func(paths []string) error

	// scanRunning serializes ScanTiers across its exported callers: the
	// startup scan and POST /api/v1/tiering/scan. The endpoint has no
	// handler deadline and fasthttp does not cancel a request when the
	// client disconnects, so an operator who gives up on a slow scan and
	// retries would otherwise start a second full one over the same
	// storage root, against the same SQLite handle, each invalidating the
	// tier cache the query path reads (#1154).
	//
	// Deliberately NOT on the unexported scanTiers: a migration cycle that
	// lands on a running API scan must still run its own pre-migration
	// scan, or it would migrate from stale hot rows.
	scanRunning atomic.Bool

	// lastScan is a copy of the most recent scan result and when it
	// finished, written by scanTiers on every path including truncation.
	// The startup scan has no HTTP response to carry its result, and a
	// health check cannot ask the scan endpoint without starting a scan, so
	// this is the only way a partial startup scan is observable after the
	// fact. Guarded by scanStateMu: scanRunning serializes the startup and
	// API scans and cycleRunning serializes the cycle and migrate paths, so
	// two scans can still be in flight at once while the status handler
	// reads.
	scanStateMu sync.RWMutex
	lastScan    *ScanResult
	lastScanAt  time.Time

	// Data stores
	metadata *MetadataStore
	policies *PolicyStore

	// Configuration
	config *config.TieredStorageConfig

	// License client for feature gating
	licenseClient *license.Client

	// clusterGate, when non-nil, limits storage mutation to the primary
	// writer and switches on the cold-tier metadata sync. See ManagerConfig.
	clusterGate ClusterGate

	// manifest, when non-nil, is told about every hot copy tiering removes,
	// before it is removed. See ManagerConfig. manifestMu serializes
	// proposals and guards lastManifestProposal, which paces them.
	manifest             ManifestCoordinator
	manifestMu           sync.Mutex
	lastManifestProposal time.Time

	// tierEvent* carry the registration reports the cluster layer makes for
	// files this node did not write itself — pulled from a peer, or unlinked
	// because another node migrated them. One drainer applies them so a
	// catch-up burst cannot pile writers onto the single shared SQLite
	// connection; see internal/tiering/replicated.go for why.
	//
	// The queue and the goroutine are created by NewManager, not Start, so
	// the seam is live for the whole life of the manager, and tierEventWG
	// lets Stop join the drainer before the shared handle can be closed.
	tierEvents        *tierEventQueue
	tierEventStop     chan struct{}
	tierEventWG       sync.WaitGroup
	tierEventStopOnce sync.Once
	// draining is set once Stop has signalled the drainer: its chunks then
	// share what is left of tierEventStopDeadline (unix nanos) and start no
	// cold-tier existence probe, because Stop is on the shared shutdown
	// budget.
	draining              atomic.Bool
	tierEventStopDeadline atomic.Int64
	// tierEventsProcessed counts events the drainer has finished with,
	// whatever the outcome — the barrier a test waits on, since an event that
	// correctly writes nothing moves none of the three counters below.
	tierEventsProcessed atomic.Int64
	tierEventsApplied   atomic.Int64
	tierEventsDropped   atomic.Int64
	tierEventsFailed    atomic.Int64

	// Components
	migrator  *Migrator
	scheduler *Scheduler
	router    *Router

	// State
	running atomic.Bool
	// cycleRunning serializes runCycle: a scheduled tick and a manual
	// trigger must not overlap (see ErrMigrationCycleRunning).
	cycleRunning atomic.Bool
	stopCh       chan struct{}

	logger zerolog.Logger
	mu     sync.RWMutex
}

// ManagerConfig holds configuration for creating a tiering manager
type ManagerConfig struct {
	// Storage backends
	HotBackend  storage.Backend // Required: local storage for hot tier
	ColdBackend storage.Backend // Optional: S3/Azure for cold tier

	// Database connection for metadata
	DB *sql.DB

	// Configuration
	Config *config.TieredStorageConfig

	// License client
	LicenseClient *license.Client

	// ClusterGate, when non-nil, restricts storage mutation (migrate, delete,
	// orphan reconciliation) to the primary writer. main.go wires it where
	// nodes share data — shared-storage mode, and per-node storage with file
	// replication: there the primary's manifest deletes unlink every
	// replica, so only one node may migrate. A non-nil gate also switches on
	// the cold-tier metadata sync in ScanTiers, since other nodes' migrations
	// are then visible only by listing cold storage. A local-storage cluster
	// without replication shares nothing and stays ungated.
	ClusterGate ClusterGate

	// Manifest, when non-nil, keeps the cluster file manifest in step with
	// the hot tier: every hot copy tiering removes is first removed from the
	// manifest (one batched proposal per chunk), which is what stops peer
	// replication from pulling the file back. main.go wires it whenever the
	// cluster coordinator exists.
	Manifest ManifestCoordinator

	// Logger
	Logger zerolog.Logger
}

// NewManager creates a new tiering manager
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	logger := cfg.Logger.With().Str("component", "tiering-manager").Logger()

	// Validate configuration
	if cfg.HotBackend == nil {
		return nil, fmt.Errorf("hot backend is required")
	}
	if cfg.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}
	if cfg.Config == nil {
		return nil, fmt.Errorf("configuration is required")
	}

	// Validate license
	if cfg.LicenseClient == nil {
		return nil, fmt.Errorf("license client is required for tiered storage")
	}
	if !cfg.LicenseClient.CanUseTieredStorage() {
		return nil, fmt.Errorf("valid license with tiered_storage feature required")
	}

	// Create metadata store
	metadata, err := NewMetadataStore(cfg.DB, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	// Create policy store
	policies, err := NewPolicyStore(cfg.DB, cfg.Config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create policy store: %w", err)
	}

	m := &Manager{
		hotBackend:    cfg.HotBackend,
		coldBackend:   cfg.ColdBackend,
		metadata:      metadata,
		policies:      policies,
		config:        cfg.Config,
		licenseClient: cfg.LicenseClient,
		clusterGate:   cfg.ClusterGate,
		manifest:      cfg.Manifest,
		stopCh:        make(chan struct{}),
		tierEvents:    newTierEventQueue(),
		tierEventStop: make(chan struct{}),
		logger:        logger,
	}

	// Started here rather than in Start so the cluster layer can report pulls
	// and unlinks from the moment the manager exists, and so a manager whose
	// Start is refused still has a drainer that Stop can join.
	m.tierEventWG.Add(1)
	go m.tierEventLoop()

	// Create migrator
	m.migrator = NewMigrator(&MigratorConfig{
		Manager:       m,
		MaxConcurrent: cfg.Config.MigrationMaxConcurrent,
		BatchSize:     cfg.Config.MigrationBatchSize,
		Logger:        logger,
	})

	// Create scheduler
	m.scheduler = NewScheduler(&SchedulerConfig{
		Manager:  m,
		Schedule: cfg.Config.MigrationSchedule,
		Logger:   logger,
	})

	// Create router for query routing across tiers
	m.router = NewRouter(m, logger)

	logger.Info().
		Bool("cold_enabled", cfg.ColdBackend != nil && cfg.Config.Cold.Enabled).
		Str("schedule", cfg.Config.MigrationSchedule).
		Msg("Tiering manager created")

	return m, nil
}

// Start starts the tiering manager and scheduler
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running.Load() {
		return fmt.Errorf("tiering manager already running")
	}

	// Verify license before starting
	if !m.licenseClient.CanUseTieredStorage() {
		m.logger.Warn().Msg("Valid license required for tiered storage - not starting scheduler")
		return nil
	}

	// Start scheduler
	if err := m.scheduler.Start(); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	m.running.Store(true)
	m.logger.Info().Msg("Tiering manager started")
	return nil
}

// Stop stops the tiering manager and scheduler.
//
// The tier-event drainer is joined FIRST and outside m.mu: its batches write
// through the shared SQLite handle, which the caller closes once Stop returns,
// and holding an application mutex across that I/O is the deadlock this
// codebase has been bitten by before — anything the drainer ever comes to call
// that takes m.mu would wedge here. The join also happens before the
// not-running early return, so a manager whose Start was refused (an expired
// license, an unparseable migration schedule) still has its goroutine
// collected rather than leaked against a closed handle.
func (m *Manager) Stop() error {
	// Nil only on a Manager built by hand rather than by NewManager, which
	// the package's own tests do because NewManager needs a real license
	// client. Closing a nil channel panics, and a Stop that panics on a test
	// double is a trap for the next person, not a caught bug.
	if m.tierEventStop != nil {
		m.tierEventStopOnce.Do(func() { close(m.tierEventStop) })
	}
	m.tierEventWG.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running.Load() {
		return nil
	}

	close(m.stopCh)
	m.scheduler.Stop()
	m.running.Store(false)

	m.logger.Info().Msg("Tiering manager stopped")
	return nil
}

// IsRunning returns true if the manager is running
func (m *Manager) IsRunning() bool {
	return m.running.Load()
}

// RunMigrationCycle runs a single migration cycle. On a node a cluster gate
// excludes it syncs tier metadata and returns ErrMigrationRoleGated.
func (m *Manager) RunMigrationCycle(ctx context.Context) error {
	// Check license before each cycle
	if !m.licenseClient.CanUseTieredStorage() {
		m.logger.Warn().Msg("Valid license required - skipping migration cycle")
		return nil
	}
	return m.runCycle(ctx)
}

// runCycle is RunMigrationCycle after the license check. Every node brings
// its tier metadata in line with storage; only the primary writer (or an
// ungated node) goes on to move and delete files.
func (m *Manager) runCycle(ctx context.Context) error {
	if !m.cycleRunning.CompareAndSwap(false, true) {
		return ErrMigrationCycleRunning
	}
	defer m.cycleRunning.Store(false)

	if m.roleGated() {
		m.logger.Debug().Msg("Starting tiering cycle: metadata sync only, node is not the primary writer")
	} else {
		m.logger.Info().Msg("Starting migration cycle")
	}
	startTime := time.Now()

	// Scan and register any new files before migration. The result is never
	// nil: a cold sync that succeeded before the hot scan failed still
	// changed what this node reads.
	// The scan gets its own budget, derived from the cycle's rather than
	// replacing it: whichever is shorter bounds the scan, and migration — the
	// step that actually moves files — keeps the remainder of the cycle. A
	// scan_timeout larger than the cycle budget is capped by this parent.
	scanResult, coldRows, err := func() (*ScanResult, []FileMetadata, error) {
		scanCtx, cancelScan := context.WithTimeout(ctx, m.scanBudget())
		defer cancelScan()
		return m.scanTiers(scanCtx)
	}()
	switch {
	case err != nil && errors.Is(ctx.Err(), context.Canceled):
		// The cycle was cancelled rather than timed out — a shutdown, or the
		// request context behind TriggerMigration. Neither budget is at
		// fault, so name neither.
		m.logger.Warn().Err(err).
			Msg("File scan stopped because the migration cycle was cancelled, continuing with existing metadata")
	case err != nil && ctx.Err() != nil:
		// The CYCLE ran out, not the scan. Say which, so an operator does
		// not raise tiered_storage.scan_timeout at a cycle that was already
		// out of time.
		m.logger.Warn().Err(err).
			Msg("File scan stopped because the migration cycle ran out of time, continuing with existing metadata")
	case err != nil:
		m.logger.Warn().Err(err).
			Dur("scan_timeout", m.scanBudget()).
			Bool("truncated", scanResult.Truncated).
			Int("scanned", scanResult.FilesScanned).
			Msg("File scan failed, continuing with existing metadata")
	case scanResult.Truncated:
		m.logger.Warn().
			Dur("scan_timeout", m.scanBudget()).
			Int("scanned", scanResult.FilesScanned).
			Int("registered", scanResult.FilesRegistered).
			Msg("Pre-migration scan ran out of budget and stopped part way; no hot rows were retired this cycle. Raise tiered_storage.scan_timeout above the time a full scan takes")
	default:
		m.logger.Info().
			Int("scanned", scanResult.FilesScanned).
			Int("registered", scanResult.FilesRegistered).
			Int("hot_retired", scanResult.HotRetired).
			Int("cold_synced", scanResult.ColdSynced).
			Msg("Pre-migration scan completed")
	}

	// Checked every cycle, not at Start: a leader change must take effect
	// without a restart. A gated node stops here — the sync above is its
	// whole job — but a sync that flipped rows changed which tiers this
	// node reads for a measurement, so the query caches need the same
	// invalidation a migration would trigger (#662).
	if m.roleGated() {
		// Hot rows the scan wrote count too: a measurement this node had
		// only cold rows for may have just gained its first hot one.
		m.notifyMigrationComplete(scanResult.ColdSynced+scanResult.HotRowsWritten, 0)
		return ErrMigrationRoleGated
	}

	var totalMigrated int
	var totalErrors int
	var orphansFound, orphansDeleted, orphanErrors int

	// A primary whose cold listing just failed still holds whatever stale
	// hot rows the sync would have flipped, so migrating now would select
	// files that are already in cold and fail on every one of them; and
	// reconciliation cannot verify cold copies against a backend that
	// cannot be listed. Both wait for a cycle whose listing succeeds.
	if m.clusterGate != nil && scanResult.ColdSyncFailed {
		m.logger.Warn().Msg("Skipping migration and reconciliation this cycle: the cold tier could not be listed")
		totalErrors++
	} else {
		// Hot -> Cold migrations (2-tier system)
		if m.coldTierUsable() {
			migrated, errors := m.migrator.MigrateTier(ctx, TierHot, TierCold)
			totalMigrated += migrated
			totalErrors += errors
		}

		// Reconcile orphaned hot files (files tracked as cold but still in
		// hot storage) — and ONLY with a usable cold tier (#1143).
		//
		// Both of these verify a cold copy before acting, so without one
		// there is nothing they can soundly do: GetBackendForTier answers
		// nil, the orphan sweep keeps every hot copy it examines and the
		// manifest sweep returns before doing any work at all. Skipped
		// rather than left to those branches because the orphan sweep logs
		// an error and counts a failure per orphan, so a node with cold off
		// reports errors every cycle for work it was never going to do.
		// (For the manifest sweep the gate is a pure no-op; it already
		// returned early on a nil cold backend.)
		//
		// This is the gate whose absence was #1143. Its cost was noise
		// rather than data: a disabled cold tier has no backend (main.go
		// builds one only when the flag is on), so the sweep took its
		// keep-the-file branch — but it took it per ORPHAN, every cycle,
		// with an Error line and a counted failure each time. Per orphan,
		// not per cold row: a row whose hot copy is gone costs one silent
		// existence check and never reaches the cold step.
		if !m.coldTierUsable() {
			// One line per cycle, not one per row. Named because skipping
			// costs something besides the noise: the sweep quarantines a
			// permanently unusable storage key (#758) BEFORE it looks at the
			// cold tier, so that mark is now deferred on such a node until
			// cold comes back — and the rows age out of the 48-hour window
			// meanwhile, so in practice it is not taken at all. Harmless
			// while the sweep itself is not running (the mark exists to stop
			// the sweep retrying that key), and worth saying out loud.
			m.logger.Debug().Msg("Cold tier is not usable on this node; orphan and manifest reconciliation are skipped this cycle")
		} else {
			orphansFound, orphansDeleted, orphanErrors = m.migrator.ReconcileOrphanedFiles(ctx)
			totalErrors += orphanErrors

			// Manifest entries for files already in cold — migrated before the
			// manifest was kept in step, or whose manifest step failed — keep
			// peer replication pulling their replicas back. coldRows (every
			// cold row this node knows) is held across the whole cycle for
			// this; a few hundred bytes a row.
			if _, err := m.migrator.ReconcileManifest(ctx, coldRows); err != nil {
				m.logger.Warn().Err(err).Msg("Manifest reconciliation stopped; remaining entries are retried next cycle")
				totalErrors++
			}
		}
	}
	if orphansFound > 0 || orphanErrors > 0 {
		m.logger.Info().
			Int("found", orphansFound).
			Int("deleted", orphansDeleted).
			Int("errors", orphanErrors).
			Msg("Orphaned hot file reconciliation completed")
	}

	// Cleanup old migration history records. Reached only past the gate, so
	// a node that never leads keeps whatever history it wrote before it was
	// gated until it next leads; bounded by that history's size.
	if err := m.cleanupOldMigrations(ctx); err != nil {
		m.logger.Warn().Err(err).Msg("Migration history cleanup failed")
	}

	duration := time.Since(startTime)
	m.logger.Info().
		Int("migrated", totalMigrated).
		Int("errors", totalErrors).
		Dur("duration", duration).
		Msg("Migration cycle completed")

	// Cold rows the sync added or flipped, and hot rows the scan wrote, count
	// as moved from this node's point of view: they change which globs a
	// query reads.
	m.notifyMigrationComplete(totalMigrated+scanResult.ColdSynced+scanResult.HotRowsWritten, orphansDeleted)

	return nil
}

// deleteFromManifest removes hot paths from the cluster manifest, when there
// is one; a nil manifest — no cluster — is a no-op. Callers chunk to
// manifestChunk; this paces: no two proposals from this node, whatever
// their source, are closer than manifestChunkPause, so every peer's unlink
// queue drains between them. The pause therefore never applies where there
// is nothing to pace.
func (m *Manager) deleteFromManifest(ctx context.Context, paths []string, reason string) error {
	if m.manifest == nil || len(paths) == 0 {
		return nil
	}
	m.manifestMu.Lock()
	defer m.manifestMu.Unlock()
	if wait := manifestChunkPause - time.Since(m.lastManifestProposal); wait > 0 {
		if err := sleepCtx(ctx, wait); err != nil {
			return err
		}
	}
	err := m.manifest.DeleteFilesFromManifest(ctx, paths, reason)
	m.lastManifestProposal = time.Now()
	return err
}

// roleGated reports whether a cluster gate is wired and denies this node.
func (m *Manager) roleGated() bool {
	return m.clusterGate != nil && !m.clusterGate.IsPrimaryWriter()
}

// MigrationGate reports whether migration is role-gated on this node right
// now and, when a gate is wired, the node's role for messages.
func (m *Manager) MigrationGate() (gated bool, role string) {
	if m.clusterGate == nil {
		return false, ""
	}
	return !m.clusterGate.IsPrimaryWriter(), m.clusterGate.Role()
}

// notifyMigrationComplete fires the registered callback when a cycle changed
// which files live on which tier. Orphan deletions count: crash-recovery
// reconciliation can delete hot files in a cycle that migrated nothing, and a
// cached pruned hot glob matching zero files makes DuckDB error until TTL.
func (m *Manager) notifyMigrationComplete(migrated, orphansDeleted int) {
	if migrated <= 0 && orphansDeleted <= 0 {
		return
	}
	m.callbackMu.Lock()
	fn := m.onMigrationComplete
	m.callbackMu.Unlock()
	if fn != nil {
		fn()
	}
}

// SetOnHotFilesRemoved registers a callback invoked BEFORE tiering deletes
// hot files (see the field comment). Safe to call after Start.
func (m *Manager) SetOnHotFilesRemoved(fn func(paths []string) error) {
	m.callbackMu.Lock()
	m.onHotFilesRemoved = fn
	m.callbackMu.Unlock()
}

// notifyHotFilesRemoved invokes the removal callback; a nil callback allows
// the removal (non-hub deployments have no receipts to protect).
func (m *Manager) notifyHotFilesRemoved(paths []string) error {
	m.callbackMu.Lock()
	fn := m.onHotFilesRemoved
	m.callbackMu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(paths)
}

// hasHotFileRemovalHook reports whether a removal callback is wired; the
// migrator only admits spoke-namespace candidates when it is (#687).
func (m *Manager) hasHotFileRemovalHook() bool {
	m.callbackMu.Lock()
	defer m.callbackMu.Unlock()
	return m.onHotFilesRemoved != nil
}

// SetOnMigrationComplete registers a callback invoked after any migration
// cycle that moved or deleted tier files. See the field comment for why the
// query caches need it. Safe to call after Start: the scheduler goroutine
// reads the callback under the same lock.
func (m *Manager) SetOnMigrationComplete(fn func()) {
	m.callbackMu.Lock()
	m.onMigrationComplete = fn
	m.callbackMu.Unlock()
}

// cleanupOldMigrations deletes migration history records older than the configured retention.
//
// This layer owns retention *policy*: a value of 0 (unset) defaults to 90 days,
// and a negative value explicitly disables cleanup. MetadataStore.CleanupOldMigrations
// applies its own <= 0 guard as a defensive contract, but by the time we call it here
// retentionDays is always > 0.
func (m *Manager) cleanupOldMigrations(ctx context.Context) error {
	retentionDays := m.config.MigrationHistoryRetentionDays
	if retentionDays == 0 {
		retentionDays = 90 // Default: keep 90 days of migration history
	} else if retentionDays < 0 {
		return nil // Negative value explicitly disables cleanup
	}

	deleted, err := m.metadata.CleanupOldMigrations(ctx, retentionDays)
	if err != nil {
		return err
	}

	if deleted > 0 {
		m.logger.Debug().
			Int64("deleted", deleted).
			Int("retention_days", retentionDays).
			Msg("Migration history cleanup completed")
	}

	return nil
}

// TriggerMigration triggers a manual migration cycle. A gated node refuses
// up front rather than running the metadata sync and then declining: an
// operator who hit a non-primary wants the answer now, and the scheduled
// cycle already keeps every node's metadata converging.
func (m *Manager) TriggerMigration(ctx context.Context) error {
	if m.roleGated() {
		return ErrMigrationRoleGated
	}
	return m.RunMigrationCycle(ctx)
}

// coldTierUsable reports whether this node has a cold tier it may actually
// read or write: a backend was built AND the operator has it enabled.
//
// The single definition of that question (#1143). It used to be spelled out
// inline at every consumer — the migration gate, the stats, the cold-metadata
// sync, the query glob, two drainer paths — and orphan reconciliation was the
// one that forgot. A conjunction remembered at six call sites is not a design;
// there is now one predicate, and the only conjunction still spelled out is
// NewManager's own startup log line, which runs before there is an m to ask.
//
// What the omission did and did not cost, because the comment this replaces
// overstated it: the sweep confirms a cold copy before deleting a hot one, so
// taking an unflagged backend as proof would have had it delete the copy the
// query path was actually reading. But a disabled cold tier has no backend at
// all — cmd/arc/main.go builds one only inside "if cold.Enabled", nothing
// assigns the flag after load, and there is no reload — so the sweep met nil
// and already kept the file. The real cost was noise: it ran every cycle on
// such a node and logged an error per orphan row for work it could never do.
// The invariant is enforced here anyway, so moving construction out of that
// "if" cannot quietly turn a latent violation into a live one.
//
// config is a POINTER and is checked before Cold.Enabled is read: NewManager
// always sets it, but a partly-built manager is the shape a caller holding
// this as an interface can be handed. Nil-receiver safe for the same reason.
func (m *Manager) coldTierUsable() bool {
	return m != nil && m.coldBackend != nil && m.config != nil && m.config.Cold.Enabled
}

// GetBackendForTier returns the storage backend for a tier, or nil when this
// node has no usable one.
//
// The cold answer ANDs cold.enabled (#1143). It did not until that issue, and
// what that cost is narrower than it looks: a caller taking a non-nil answer
// as "cold is usable" WOULD have been wrong on a node that kept a cold backend
// with the flag off, but no such node exists — cmd/arc/main.go builds one only
// inside "if cold.Enabled", so disabling cold leaves this nil either way. The
// flag is ANDed here so the two cannot drift if construction ever moves.
//
// Callers that need the configured backend regardless of the flag do not
// exist; the one that looked like it did, the migration source cleanup, always
// asks for the HOT tier.
func (m *Manager) GetBackendForTier(tier Tier) storage.Backend {
	if m == nil {
		return nil
	}
	switch tier {
	case TierHot:
		return m.hotBackend
	case TierCold:
		if !m.coldTierUsable() {
			return nil
		}
		return m.coldBackend
	default:
		return m.hotBackend
	}
}

// GetMetadata returns the metadata store
func (m *Manager) GetMetadata() *MetadataStore {
	return m.metadata
}

// GetPolicies returns the policy store
func (m *Manager) GetPolicies() *PolicyStore {
	return m.policies
}

// GetConfig returns the tiered storage configuration
func (m *Manager) GetConfig() *config.TieredStorageConfig {
	return m.config
}

// GetRouter returns the tier router for query routing
func (m *Manager) GetRouter() *Router {
	return m.router
}

// RecordRewrittenFile updates tier metadata for an immutable rewrite.
// The old path is retired and the new path is recorded as hot. It is used by
// DELETE because rewritten files bypass the normal ingest/replication writers.
func (m *Manager) RecordRewrittenFile(ctx context.Context, oldPath, newPath string, sizeBytes int64) error {
	if m == nil || m.metadata == nil {
		return nil
	}
	info, err := m.parseFilePath(newPath)
	if err != nil {
		return fmt.Errorf("parse rewritten file path %q: %w", newPath, err)
	}
	if err := m.metadata.DeleteFile(ctx, oldPath); err != nil {
		return fmt.Errorf("retire rewritten source %q from tier metadata: %w", oldPath, err)
	}
	if err := m.RecordNewFile(ctx, &FileMetadata{
		Path:          newPath,
		Database:      info.Database,
		Measurement:   info.Measurement,
		PartitionTime: info.PartitionTime,
		SizeBytes:     sizeBytes,
		CreatedAt:     time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("record rewritten file %q in tier metadata: %w", newPath, err)
	}
	return nil
}

// RecordNewFile records a newly ingested file in the hot tier
func (m *Manager) RecordNewFile(ctx context.Context, file *FileMetadata) error {
	file.Tier = TierHot
	if file.CreatedAt.IsZero() {
		file.CreatedAt = time.Now().UTC()
	}
	return m.metadata.RecordFile(ctx, file)
}

// DeleteFile removes a file from tier metadata (called when file is deleted)
func (m *Manager) DeleteFile(ctx context.Context, path string) error {
	return m.metadata.DeleteFile(ctx, path)
}

// GetStatus returns the current tiering status
func (m *Manager) GetStatus(ctx context.Context) (*StatusResponse, error) {
	status := &StatusResponse{
		Enabled:      m.config.Enabled,
		LicenseValid: m.licenseClient.CanUseTieredStorage(),
	}
	if last, at := m.LastScan(); last != nil {
		status.LastScan = last
		scannedAt := at
		status.LastScanAt = &scannedAt
	}

	if !status.LicenseValid {
		status.Reason = "license required"
		return status, nil
	}

	// Get tier stats
	tierStats, err := m.metadata.GetTierStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tier stats: %w", err)
	}

	status.Tiers = make(map[string]TierStats)

	// Hot tier
	hotStats := tierStats[TierHot]
	hotStats.Enabled = true
	// The hot tier is whatever the primary storage backend is; in
	// shared-storage mode that is an object store, not local disk.
	hotStats.Backend = m.hotBackend.Type()
	status.Tiers["hot"] = hotStats

	// Cold tier
	coldStats := tierStats[TierCold]
	coldStats.Enabled = m.coldTierUsable()
	coldStats.Backend = m.config.Cold.Backend
	status.Tiers["cold"] = coldStats

	quarantined, err := m.metadata.CountQuarantinedFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to count quarantined files: %w", err)
	}
	status.QuarantinedFiles = quarantined

	// Scheduler status
	status.Scheduler = m.scheduler.Status()

	// Omitted entirely when nothing has ever been reported, so a standalone
	// node's status is not cluttered with three zeroes.
	if applied, dropped, failed := m.TierEventStats(); applied|dropped|failed != 0 {
		status.ReplicationEvents = &TierEventCounts{Applied: applied, Dropped: dropped, Failed: failed}
	}

	return status, nil
}

// GetEffectivePolicy returns the effective policy for a database
func (m *Manager) GetEffectivePolicy(ctx context.Context, database string) *EffectivePolicy {
	return m.policies.GetEffective(ctx, database)
}

// IsHotOnly returns true if the database should stay in hot tier only
func (m *Manager) IsHotOnly(ctx context.Context, database string) bool {
	return m.policies.IsHotOnly(ctx, database)
}

// ScanResult holds the results of a file scan operation
type ScanResult struct {
	FilesScanned    int `json:"files_scanned"`
	FilesRegistered int `json:"files_registered"`
	FilesSkipped    int `json:"files_skipped"`
	Errors          int `json:"errors"`
	// ColdSynced counts rows the cold-tier sync added or flipped to cold.
	// Always zero without a cluster gate (see ScanTiers).
	ColdSynced int `json:"cold_synced"`
	// ColdSyncFailed is set when the cold tier could not be listed; the
	// hot scan still ran, but rows this node holds for files other nodes
	// moved are stale until a listing succeeds.
	ColdSyncFailed bool `json:"cold_sync_failed,omitempty"`
	// HotRetired counts hot rows removed because their file is no longer
	// in hot storage.
	HotRetired int `json:"hot_retired"`
	// HotRowsWritten counts hot rows the walk actually inserted or changed.
	// FilesRegistered counts every hot file the walk accounted for, written
	// or not, so on a node whose rows already match its disk this is zero
	// while FilesRegistered is the file count.
	HotRowsWritten int `json:"hot_rows_written"`
	// Truncated reports that the scan ran out of budget and stopped part
	// way. The counts beside it are real but incomplete, and in particular
	// hot-row retirement runs only after the whole walk, so a truncated
	// scan has retired nothing: stale rows for files that are gone from hot
	// storage survive to the next complete scan.
	//
	// Set on every path that can run out of budget — the cold sync, the hot
	// walk, and the retire tail, which returns no error of its own.
	Truncated bool `json:"truncated,omitempty"`
}

// ScanTiers brings this node's tier metadata in line with storage: the cold
// listing first (only when a cluster gate is wired — see
// syncColdTierMetadata), then the hot scan, whose never-downgrade guard
// (#683) must see the rows the cold pass flipped. The result is never nil,
// so a caller can act on a partial pass when the hot scan fails.
func (m *Manager) ScanTiers(ctx context.Context) (*ScanResult, error) {
	if !m.scanRunning.CompareAndSwap(false, true) {
		return &ScanResult{}, ErrScanRunning
	}
	defer m.scanRunning.Store(false)
	if !m.cycleRunning.CompareAndSwap(false, true) {
		return nil, ErrMigrationCycleRunning
	}
	defer m.cycleRunning.Store(false)

	result, _, err := m.scanTiers(ctx)
	return result, err
}

// defaultScanTimeout is the budget for one tier scan when no configuration
// supplies one. It matches the tiered_storage.scan_timeout default rather than
// restating it: a Manager built in a test, or the partly-built shape
// NewMetadataStore callers are handed, has no config at all, and a zero
// duration would make context.WithTimeout return an already-expired context
// that truncates every scan on its first object.
const defaultScanTimeout = 2 * time.Hour

// scanBudget is how long one scan may take. Read through here, never off the
// config struct: m.config is a pointer and nil is a supported shape.
func (m *Manager) scanBudget() time.Duration {
	if m.config != nil && m.config.ScanTimeout > 0 {
		return m.config.ScanTimeout
	}
	return defaultScanTimeout
}

// ScanBudget is scanBudget for callers outside the package: the API handler
// bounds its own scan with the same number the background paths use, read from
// the manager rather than plumbed through a constructor so a hand-built handler
// cannot carry a zero.
func (m *Manager) ScanBudget() time.Duration { return m.scanBudget() }

// recordScan stores a COPY of the result. The caller keeps the pointer it
// passed and hands it to HTTP responses and callbacks; storing that pointer
// would let a later in-place annotation rewrite what this node reports as its
// last scan.
func (m *Manager) recordScan(result *ScanResult) {
	if result == nil {
		return
	}
	snapshot := *result
	m.scanStateMu.Lock()
	m.lastScan = &snapshot
	m.lastScanAt = time.Now().UTC()
	m.scanStateMu.Unlock()
}

// LastScan returns a copy of the most recent scan result and when it finished,
// or nil if this node has not scanned since it started.
func (m *Manager) LastScan() (*ScanResult, time.Time) {
	m.scanStateMu.RLock()
	defer m.scanStateMu.RUnlock()
	if m.lastScan == nil {
		return nil, time.Time{}
	}
	snapshot := *m.lastScan
	return &snapshot, m.lastScanAt
}

// scanTiers is ScanTiers that also hands back the cold rows the sync worked
// from (those it loaded plus those it recorded), so the primary's manifest
// sweep in the same cycle does not load them a third time.
func (m *Manager) scanTiers(ctx context.Context) (*ScanResult, []FileMetadata, error) {
	result := &ScanResult{}
	var coldRows []FileMetadata
	var coldSyncErr error
	if m.clusterGate != nil && m.coldTierUsable() {
		synced, rows, err := m.syncColdTierMetadata(ctx)
		result.ColdSynced = synced
		coldRows = rows
		coldSyncErr = err
		if err != nil {
			// The hot scan still matters on its own: the primary migrates
			// from hot rows and every node routes reads from them.
			result.ColdSyncFailed = true
			m.logger.Warn().Err(err).Msg("Cold tier metadata sync failed, continuing with hot scan")
		}
	}
	hot, err := m.ScanAndRegisterFiles(ctx)
	// Copy whatever the walk accounted for whether or not it finished. A
	// truncated walk returns real counts on a non-nil result, and dropping
	// them reported files_scanned 0 after a walk over tens of thousands of
	// files (#1154). HotRowsWritten was missing from this copy entirely, so
	// every consumer read zero: the cache invalidations keyed on it never
	// fired, including the one a standalone node needs on its first boot.
	if hot != nil {
		result.FilesScanned = hot.FilesScanned
		result.FilesRegistered = hot.FilesRegistered
		result.FilesSkipped = hot.FilesSkipped
		result.HotRetired = hot.HotRetired
		result.HotRowsWritten = hot.HotRowsWritten
		result.Errors = hot.Errors
		if hot.Truncated {
			result.Truncated = true
		}
	}
	// A budget that expired anywhere in the scan is a truncation, including
	// in the cold sync above, whose error this function deliberately folds
	// into ColdSyncFailed so the hot scan still runs.
	if isBudgetExpiry(err) || isBudgetExpiry(coldSyncErr) {
		result.Truncated = true
	}
	m.recordScan(result)
	if err != nil {
		return result, coldRows, err
	}
	return result, coldRows, nil
}

// isBudgetExpiry reports whether err is a scan that ran out of time or was
// cancelled, rather than a scan that failed on its own terms.
func isBudgetExpiry(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// syncColdTierMetadata makes this node's metadata reflect what is in cold
// storage. In a gated cluster — shared storage, or per-node storage with
// file replication — only the primary writer migrates, but the query layer
// routes each node from its OWN SQLite (buildMultiTierReadParquet includes
// the cold tier only for measurements with a cold row here), so a node that
// never migrated would never read the cold tier. Listing cold and recording
// what is there is how every node — readers included — learns about the
// primary's moves, and how a newly elected primary stops treating
// already-migrated files as candidates.
//
// Rows are only ever added or flipped to cold. A cold row whose object is
// gone is reported, never reverted, for the same reason the hot scan never
// downgrades (#683): the object may be mid-move, and a hot copy is still
// read through the hot glob. Returns how many rows changed and the cold rows
// it worked from — those loaded plus those recorded — for the manifest
// sweep.
func (m *Manager) syncColdTierMetadata(ctx context.Context) (int, []FileMetadata, error) {
	lister, ok := m.coldBackend.(storage.ObjectLister)
	if !ok {
		return 0, nil, fmt.Errorf("cold backend does not support ListObjects")
	}
	objects, err := lister.ListObjects(ctx, "")
	if err != nil {
		return 0, nil, fmt.Errorf("failed to list cold objects: %w", err)
	}

	// One query for the cold row set, as the hot scan does. Skipping rows
	// that are already cold is what keeps this pass from writing at all in
	// steady state: every write invalidates the tier cache a query-serving
	// node is using.
	coldRows, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to load cold tier paths: %w", err)
	}
	unseen := make(map[string]bool, len(coldRows))
	for _, f := range coldRows {
		// A quarantined row's key is one no backend will list (#758), so
		// it would show up as "missing" on every node, every cycle.
		if f.QuarantinedAt != nil {
			continue
		}
		unseen[f.Path] = true
	}

	synced, unparseable, quarantined := 0, 0, 0
	for _, obj := range objects {
		// Past the deadline every remaining upsert would fail and log; stop
		// with what was recorded — the next cycle continues from there.
		if err := ctx.Err(); err != nil {
			return synced, coldRows, err
		}
		if !strings.HasSuffix(obj.Path, ".parquet") {
			continue
		}
		if first, _, _ := strings.Cut(obj.Path, "/"); storage.IsReservedRootDir(first) {
			continue
		}
		if unseen[obj.Path] {
			delete(unseen, obj.Path)
			continue
		}
		info, err := m.parseFilePath(obj.Path)
		if err != nil {
			// Counted, not logged per file: a cold bucket shared with
			// something else (an Iceberg warehouse, say) would fire on
			// every object, every cycle.
			unparseable++
			continue
		}
		file := &FileMetadata{
			Path:          obj.Path,
			Database:      info.Database,
			Measurement:   info.Measurement,
			PartitionTime: info.PartitionTime,
			SizeBytes:     obj.Size,
			CreatedAt:     obj.LastModified,
		}
		wrote, err := m.metadata.RecordColdFile(ctx, file, obj.LastModified)
		if err != nil {
			m.logger.Warn().Str("path", obj.Path).Err(err).Msg("Failed to record cold file, skipping")
			continue
		}
		if !wrote {
			// Quarantined (#1086 stage C added that guard). The row is
			// already the record that this key is unusable, and before the
			// guard every cycle re-upserted it for as long as the listing
			// kept returning the object. Not counted as synced and not
			// appended to coldRows: the sweep must not act on it either.
			quarantined++
			continue
		}
		synced++
		// As recorded: the sweep needs the tier and the migration stamp.
		migratedAt := obj.LastModified
		if migratedAt.IsZero() {
			migratedAt = time.Now()
		}
		file.Tier = TierCold
		file.MigratedAt = &migratedAt
		coldRows = append(coldRows, *file)
	}

	if len(unseen) > 0 {
		sample := make([]string, 0, 5)
		for p := range unseen {
			if len(sample) == cap(sample) {
				break
			}
			sample = append(sample, p)
		}
		m.logger.Warn().
			Int("count", len(unseen)).
			Strs("sample", sample).
			Msg("Cold tier rows have no object in cold storage; left as-is (a hot copy, if any, is still read)")
	}

	// Debug: the cycle summary already reports cold_synced.
	m.logger.Debug().
		Int("objects", len(objects)).
		Int("synced", synced).
		Int("unparseable", unparseable).
		Int("quarantined_skipped", quarantined).
		Msg("Cold tier metadata sync completed")
	return synced, coldRows, nil
}

// ScanAndRegisterFiles scans the hot tier storage and registers all existing parquet files
// Path format: {database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet
func (m *Manager) ScanAndRegisterFiles(ctx context.Context) (*ScanResult, error) {
	result := &ScanResult{}

	// Check if hot backend supports ListObjects
	objectLister, ok := m.hotBackend.(storage.ObjectLister)
	if !ok {
		return nil, fmt.Errorf("hot backend does not support ListObjects")
	}

	m.logger.Info().Msg("Starting file scan for tiering registration")

	// List all objects in the storage root
	listStart := time.Now()
	objects, err := objectLister.ListObjects(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// One query for the cold path set instead of a point lookup per scanned
	// file: the scan walks every hot file, while the cold set holds only
	// migrated daily files. Fail the scan on error rather than risk the
	// downgrade the check exists to prevent.
	coldFiles, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		return nil, fmt.Errorf("failed to load cold tier paths: %w", err)
	}
	// Measurements whose rows this walk actually changed. Invalidating the
	// tier cache per file would bump the process-wide generation once per
	// file, and every bump makes concurrent GetTiersForMeasurement fills
	// discard their result — so a first scan of a large node would leave the
	// query path re-running SELECT DISTINCT tier for its whole duration.
	touched := make(map[string][2]string)

	coldPaths := make(map[string]bool, len(coldFiles))
	for _, f := range coldFiles {
		coldPaths[f.Path] = true
	}

	for _, obj := range objects {
		// Only process parquet files
		if !strings.HasSuffix(obj.Path, ".parquet") {
			continue
		}
		// Reserved roots hold Arc's own state (_schema anchors are Parquet
		// but never tiered data); not a parse error.
		if first, _, _ := strings.Cut(obj.Path, "/"); storage.IsReservedRootDir(first) {
			continue
		}

		if err := ctx.Err(); err != nil {
			// Cancelled — the shutdown hook, or the scan's own deadline.
			// Every write from here on would fail and log once per remaining
			// file, and a partial walk must not reach retireVanishedHotRows.
			//
			// Returning from inside the loop skips the batched invalidation
			// below, so do it here: the rows written before this point are
			// real, and leaving the tier cache on its pre-scan answer makes
			// the query path contradict the database it just wrote (#1154).
			result.Truncated = true
			for _, dm := range touched {
				m.metadata.invalidateTierCache(dm[0], dm[1])
			}
			return result, err
		}

		result.FilesScanned++

		// Parse the path to extract database, measurement, and partition time
		// Format: {database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}.parquet
		fileInfo, err := m.parseFilePath(obj.Path)
		if err != nil {
			m.logger.Warn().
				Str("path", obj.Path).
				Err(err).
				Msg("Failed to parse file path, skipping")
			result.Errors++
			continue
		}

		// Never downgrade a cold row back to hot (#683): the scan lists the
		// HOT backend, and a hot file whose row already says cold is exactly
		// the orphan that ReconcileOrphanedFiles deletes after a failed
		// post-migration cleanup. Re-registering it as hot would reset
		// migrated_at, hide it from reconciliation, and re-upload it to cold
		// every cycle.
		//
		// coldPaths is a snapshot taken before the walk, so it answers for
		// the rows that existed when the scan started. The write below is
		// conditional on the stored tier for the rest: the replication
		// drainer flips rows to cold while this walk is in progress, and an
		// unconditional upsert would undo those — permanently, since nothing
		// reverts a cold row and retireVanishedHotRows skips any path in this
		// same stale listing.
		if coldPaths[obj.Path] {
			result.FilesSkipped++
			continue
		}

		// Create file metadata
		file := &FileMetadata{
			Path:          obj.Path,
			Database:      fileInfo.Database,
			Measurement:   fileInfo.Measurement,
			PartitionTime: fileInfo.PartitionTime,
			Tier:          TierHot,
			SizeBytes:     obj.Size,
			CreatedAt:     obj.LastModified,
		}

		// Conditional on the stored row still being hot, and silent when the
		// row is already what it would write — so a steady-state rescan does
		// no writes at all, and invalidates no cache entries.
		wrote, err := m.metadata.recordHotFileIfNotCold(ctx, file)
		if err != nil {
			m.logger.Warn().
				Str("path", obj.Path).
				Err(err).
				Msg("Failed to record file, skipping")
			result.Errors++
			continue
		}
		if wrote {
			touched[fileInfo.Database+"\x00"+fileInfo.Measurement] = [2]string{fileInfo.Database, fileInfo.Measurement}
			result.HotRowsWritten++
		}

		result.FilesRegistered++

		// Log progress every 100 files
		if result.FilesScanned%100 == 0 {
			m.logger.Info().
				Int("scanned", result.FilesScanned).
				Int("registered", result.FilesRegistered).
				Msg("Scan progress")
		}
	}

	// One invalidation per measurement the walk changed, after the walk.
	for _, dm := range touched {
		m.metadata.invalidateTierCache(dm[0], dm[1])
	}

	result.HotRetired = m.retireVanishedHotRows(ctx, objects, listStart, result)

	m.logScanOutcome(result)
	return result, nil
}

// logScanOutcome reports a finished walk. Split out because the decision it
// makes is the testable part: the retirement pass returns a count and no
// error, so a truncation in its tail arrives here with err nil, and calling
// that "completed" is the success-with-the-wrong-answer shape #1154 is about
// — the walk ran, but stale hot rows were left behind.
func (m *Manager) logScanOutcome(result *ScanResult) {
	event := m.logger.Info()
	msg := "File scan completed"
	if result.Truncated {
		event = m.logger.Warn()
		msg = "File scan ran out of budget and stopped part way; stale hot rows were not retired"
	}
	event.
		Int("scanned", result.FilesScanned).
		Int("registered", result.FilesRegistered).
		Int("skipped", result.FilesSkipped).
		Int("retired", result.HotRetired).
		Int("errors", result.Errors).
		Msg(msg)
}

// retireVanishedHotRowMargin is how recently a hot row may have been created
// and still be judged by a listing: a file flushed just before the listing
// began may not be visible to it yet, and one flushed during the scan
// registers after it. Neither is stale.
const retireVanishedHotRowMargin = 5 * time.Minute

// retireVanishedHotRows removes hot rows whose file the listing did not
// return — compaction consumed it, retention or an operator removed it. A
// stale hot row keeps the hot tier in every multi-tier read of its
// measurement, and once the tier's glob matches nothing the whole read
// comes back empty; it also keeps the tier counts wrong and lets
// FindCandidates select a file that is not there. Cold rows are never
// touched (#683) and quarantined rows keep their record (#758). Returns
// how many rows were retired.
func (m *Manager) retireVanishedHotRows(ctx context.Context, objects []storage.ObjectInfo, listStart time.Time, result *ScanResult) int {
	hotRows, err := m.metadata.GetFilesInTier(ctx, TierHot)
	if err != nil {
		// A budget that expired here is a truncation, not a database
		// failure: without the flag the scan reports Errors++ and still
		// returns a nil error, so it reads as a complete scan that had a
		// hiccup rather than one that never retired anything (#1154).
		if isBudgetExpiry(err) {
			result.Truncated = true
		}
		m.logger.Warn().Err(err).Msg("Failed to load hot rows; stale rows are retried next scan")
		result.Errors++
		return 0
	}
	seen := make(map[string]bool, len(objects))
	for _, obj := range objects {
		seen[obj.Path] = true
	}
	cutoff := listStart.Add(-retireVanishedHotRowMargin)
	retired := 0
	for _, row := range hotRows {
		if seen[row.Path] || row.QuarantinedAt != nil || !row.CreatedAt.Before(cutoff) {
			continue
		}
		if err := ctx.Err(); err != nil {
			// This exit returns a count and no error, so without the flag
			// the caller logs "File scan completed" and the endpoint
			// answers 200 on a scan that retired only part of what it
			// should have (#1154). result is the same object the caller
			// reads.
			//
			// Reaching here needs the budget to expire BETWEEN the load
			// above and this check, which no test can arrange without a
			// seam; the load-failure path above is the covered one. Kept
			// because a budget that expires mid-loop is the likelier case
			// on a node with many stale rows, one StatFile each.
			result.Truncated = true
			return retired
		}
		// Tier-conditional: a row that changed tier since the listing is
		// not ours to remove.
		// Confirm the file really is gone, now, rather than trusting the
		// listing this decision came from. Between that listing and here the
		// replication drainer can have pulled the path back and re-registered
		// it — and re-registration keeps the row's original created_at, so
		// neither the margin above nor an age condition on the delete can
		// tell the refreshed row from the stale one. Retiring it anyway
		// leaves a file on disk with no hot row, which costs its measurement
		// the local glob entirely once its other rows are cold.
		//
		// One stat per row judged stale, not per file scanned: a steady-state
		// node reaches this loop with nothing to retire.
		n, statErr := m.hotBackend.StatFile(ctx, row.Path)
		if statErr != nil {
			// The stat confirmed nothing, so the listing is not enough on its
			// own. Keep the row: one kept a scan too long costs an empty glob,
			// one retired for a file still on disk costs the measurement its
			// local reads once its other rows are cold.
			m.logger.Warn().Err(statErr).Str("path", row.Path).Msg("Could not confirm a vanished hot file is gone; keeping its row for the next scan")
			result.Errors++
			continue
		}
		if n >= 0 {
			continue
		}

		// createdBefore as a second condition, for the case the stat cannot
		// see: a row the drainer deleted and a later pull re-INSERTED carries
		// a fresh created_at, and this decision was not taken about that row.
		removed, err := m.metadata.DeleteFileInTier(ctx, row.Path, TierHot, cutoff)
		if err != nil {
			m.logger.Warn().Err(err).Str("path", row.Path).Msg("Failed to retire hot row for a vanished file")
			result.Errors++
			continue
		}
		if removed {
			retired++
		}
	}
	return retired
}

// filePathInfo holds parsed information from a file path
type filePathInfo struct {
	Database      string
	Measurement   string
	PartitionTime time.Time
	// SpokeNamespaced marks a path with an extra edge-sync namespace level
	// ({spoke}/{db}/{meas}/...). Such files register for visibility but are
	// gated out of cold migration until receipt-aware handling exists (#687).
	SpokeNamespaced bool
}

// parseFilePath parses a storage path to extract database, measurement, and
// partition time. Accepted shapes (hour- and day-level, plain and
// spoke-namespaced): {db}/{meas}/Y/M/D[/H]/{file}.parquet and
// {spoke}/{db}/{meas}/Y/M/D[/H]/{file}.parquet.
func (m *Manager) parseFilePath(path string) (*filePathInfo, error) {
	// Normalize path separators
	path = filepath.ToSlash(path)
	parts := strings.Split(path, "/")

	// Parse by TAIL shape, not absolute segment counts (#619 precedent:
	// compaction's isHourLevelFile) — a spoke-namespace pseudo-database adds
	// a path level, so hour-level files have 7 parts plain and 8 under a
	// spoke, day-level 6 and 7. Validation thresholds mirror
	// internal/compaction/daily.go isHourLevelFile; keep them in sync.
	//   hour-level tail: {year}/{month}/{day}/{hour}/{file}.parquet
	//   day-level tail:  {year}/{month}/{day}/{file}.parquet
	validDate := func(y, mo, d string) (time.Time, bool) {
		yn, err := strconv.Atoi(y)
		if err != nil || len(y) != 4 || yn < 1970 {
			return time.Time{}, false
		}
		mn, err := strconv.Atoi(mo)
		if err != nil || mn < 1 || mn > 12 {
			return time.Time{}, false
		}
		dn, err := strconv.Atoi(d)
		if err != nil || dn < 1 || dn > 31 {
			return time.Time{}, false
		}
		return time.Date(yn, time.Month(mn), dn, 0, 0, 0, 0, time.UTC), true
	}

	var partitionTime time.Time
	var prefix []string
	n := len(parts)
	if n >= 7 {
		if day, ok := validDate(parts[n-5], parts[n-4], parts[n-3]); ok {
			if hn, err := strconv.Atoi(parts[n-2]); err == nil && hn >= 0 && hn <= 23 {
				partitionTime = day.Add(time.Duration(hn) * time.Hour)
				prefix = parts[:n-5]
			}
		}
	}
	if prefix == nil && n >= 6 {
		if day, ok := validDate(parts[n-4], parts[n-3], parts[n-2]); ok {
			// Day-level files carry no hour segment; their partition time is
			// the start of the day, matching hourly rows' hour-start convention.
			partitionTime = day
			prefix = parts[:n-4]
		}
	}
	if prefix == nil {
		return nil, fmt.Errorf("no year/month/day[/hour] partition tail in path: %s", path)
	}

	// The prefix is {database}/{measurement} (2 parts) or a spoke namespace
	// {spoke}/{db}/{measurement} (3 parts). For spoke files, register
	// (database=spoke, measurement=spoke-db): that is the split the QUERY
	// layer produces for spoke data (FROM "rocket-01".telemetry globs
	// rocket-01/telemetry/**), so tier metadata stays query-visible. Deeper
	// nesting is not a feature (edge-sync forbids relaying); reject it.
	var database, measurement string
	spokeNamespaced := false
	switch len(prefix) {
	case 2:
		database, measurement = prefix[0], prefix[1]
	case 3:
		database, measurement = prefix[0], prefix[1]
		spokeNamespaced = true
	default:
		return nil, fmt.Errorf("unsupported path depth (%d prefix segments): %s", len(prefix), path)
	}

	// Validate no path traversal in database or measurement names
	if strings.Contains(database, "..") || strings.ContainsAny(database, "\\") {
		return nil, fmt.Errorf("invalid database name in path: %s", database)
	}
	if strings.Contains(measurement, "..") || strings.ContainsAny(measurement, "\\") {
		return nil, fmt.Errorf("invalid measurement name in path: %s", measurement)
	}

	return &filePathInfo{
		Database:        database,
		Measurement:     measurement,
		PartitionTime:   partitionTime,
		SpokeNamespaced: spokeNamespaced,
	}, nil
}
