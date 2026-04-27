// Package reconciliation implements Phase 5 of Arc's cluster file-tracking
// stack: periodic detection and repair of drift between the Raft-replicated
// file manifest and physical storage.
//
// Two kinds of drift are repaired:
//
//   - Orphan manifest entries: the manifest references a file path that no
//     longer exists in storage. Caused by retention/compaction/delete
//     succeeding storage-side then losing Raft quorum before the manifest
//     update commits.
//   - Orphan storage files: a file exists in storage but no manifest entry
//     references it. Caused by a crash between storage.Write and the
//     file-registrar Raft propose, or by files predating Phase 1.
//
// The reconciler is opt-in (default off) and, once enabled, auto-acts on
// drift older than a conservative grace window (default 24h). Per-run blast
// radius is capped (default 10k deletions) so a buggy run can't nuke the
// cluster. The manifest-before-storage invariant from CLAUDE.md is upheld
// at all times: orphan-manifest cleanup only writes the manifest, and
// orphan-storage cleanup only writes storage.
//
// See docs/progress/2026-04-27-phase5-manifest-reconciliation.md for the
// full design rationale and competitor analysis.
package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// BackendKind selects the gating + filtering rules for the reconciler.
type BackendKind string

const (
	// BackendShared: S3, Azure, MinIO — one node sweeps the bucket.
	BackendShared BackendKind = "shared"
	// BackendLocal: every node walks its own disks; per-file
	// OriginNodeID filter scopes the work.
	BackendLocal BackendKind = "local"
	// BackendStandalone: no cluster, no Raft. Reconciler runs unconditionally
	// when enabled; manifest writes are no-ops because there is no manifest.
	BackendStandalone BackendKind = "standalone"
)

// Errors returned by the reconciler.
var (
	// ErrManifestTooLarge indicates the manifest exceeds MaxManifestSize.
	// The non-paginated FSM walk would allocate too much for a safe run;
	// the operator must either increase the cap or wait for the paginated
	// FSM walk follow-up at internal/cluster/raft/fsm.go:800-806.
	ErrManifestTooLarge = errors.New("reconciliation: manifest size exceeds max_manifest_size")
	// ErrAlreadyRunning indicates a reconcile run is already in progress;
	// the caller should retry later.
	ErrAlreadyRunning = errors.New("reconciliation: a run is already in progress")
	// ErrGated indicates the local node's role does not permit running
	// reconciliation this tick (e.g. shared-storage cluster on a node that
	// is not the active compactor).
	ErrGated = errors.New("reconciliation: node role is not permitted to run")
	// ErrDisabled indicates the reconciler was constructed with cfg.Enabled=false.
	ErrDisabled = errors.New("reconciliation: feature is disabled")
	// ErrGateRevoked is wrapped by both sweeps when the cluster gate
	// returns false at a chunk boundary mid-run (e.g. compactor lease
	// transferred, primary writer demoted). Used by markAborted to
	// classify this as AbortLeaseLost without depending on log-string
	// matching.
	ErrGateRevoked = errors.New("reconciliation: gate revoked mid-run")
)

// Coordinator is the minimal interface the reconciler needs from the
// cluster package. Defined here (not imported from cluster) to avoid a
// cluster→reconciliation→cluster import cycle and to make the reconciler
// trivially mockable in unit tests.
type Coordinator interface {
	// GetFileManifest returns a snapshot of the cluster-wide manifest.
	// The reconciler treats this as authoritative for orphan-manifest
	// detection; concurrent registers/deletes are tolerated via re-checks
	// at apply time.
	GetFileManifest() []*raft.FileEntry

	// GetFileEntry returns the manifest entry for a single path, or
	// (nil, false) if the entry has been deleted. Used to re-check before
	// orphan-storage deletes so a concurrent register isn't racing.
	GetFileEntry(path string) (*raft.FileEntry, bool)

	// BatchFileOpsInManifest applies a batch of register/delete operations
	// as a single Raft log entry. Used for orphan-manifest cleanup. Already
	// leader-forwards from non-leader callers (Phase 4) so the reconciler
	// can run on any node without an extra "is leader?" check.
	BatchFileOpsInManifest(ops []raft.BatchFileOp) error
}

// Config holds reconciler configuration.
type Config struct {
	// Enabled toggles the entire feature. When false, NewReconciler still
	// returns a usable struct but Reconcile/TriggerNow return ErrDisabled.
	Enabled bool

	// BackendKind drives gating + filtering rules.
	BackendKind BackendKind

	// LocalNodeID is the local node's cluster ID. Used in BackendLocal
	// mode to filter orphan-storage candidates to files this node owns.
	LocalNodeID string

	// GraceWindow: orphan storage files younger than this are NEVER deleted.
	// Default 24h. Protects in-flight writer flushes + Raft propose +
	// replication races.
	GraceWindow time.Duration

	// ClockSkewAllowance is added to GraceWindow to absorb local-vs-storage
	// clock drift. Default 5m.
	ClockSkewAllowance time.Duration

	// PerPrefixTimeout is the per-prefix List timeout. Default 5m.
	PerPrefixTimeout time.Duration

	// MaxRunDuration is the overall run timeout. Default 30m.
	MaxRunDuration time.Duration

	// MaxManifestSize is the largest manifest the reconciler will operate
	// on. Larger manifests cause an explicit ErrManifestTooLarge so the
	// non-paginated walk's memory cost is bounded. Default 200_000.
	MaxManifestSize int

	// MaxDeletesPerRun caps the combined manifest+storage deletes in a
	// single run. Default 10_000.
	MaxDeletesPerRun int

	// BatchSize is the chunk size for both BatchFileOpsInManifest calls
	// and BatchDeleter calls. Default 1000 (matches retention).
	BatchSize int

	// DeletePreManifestOrphans controls whether orphan-storage files
	// outside the standard Arc layout (`db/m/yyyy/mm/dd/hh/file.parquet`,
	// 7 segments) are eligible for deletion. Default false — secure by
	// default for shared-bucket deployments where stray non-Arc files
	// must be left untouched. Operators who deliberately want
	// pre-Phase-1 cleanup or migration-residual cleanup must explicitly
	// opt in.
	DeletePreManifestOrphans bool

	// ManifestOnlyDryRun forces every cron run to be dry-run regardless
	// of the cron's normal act-on-completion behavior. Default true —
	// even though the feature itself is opt-in via cfg.Enabled,
	// operators get a clean dry-run audit on the first cron tick so
	// they can verify the diff before flipping this to false to allow
	// real deletes. Mirrors Druid's coordinator kill task and Iceberg's
	// remove_orphan_files posture.
	ManifestOnlyDryRun bool

	// SamplePathsCap caps the number of sample paths included in audit
	// events and Run summaries. Default 10.
	SamplePathsCap int

	// MaxRootWalkDatabases caps the number of unknown databases the
	// root-walk fallback will descend into when searching for files in
	// databases the manifest doesn't know about. Each unknown database
	// triggers a second-level ListDirectories to enumerate its
	// measurements. On clusters with thousands of databases (or a
	// shared bucket leaking unrelated top-level dirs), an unbounded
	// walk would issue thousands of metadata calls per tick. Default
	// 1000; cluster operators with very wide tenancy can raise it.
	// Set to 0 to disable the root walk entirely.
	MaxRootWalkDatabases int

	// RecheckConcurrency is the worker count for the per-file
	// storage.Exists re-check during the orphan-manifest sweep. Each
	// re-check is one HEAD request on S3/Azure; sequential is slow
	// (one RTT per file) and parallel is bounded so we can't overrun
	// the backend's per-second rate limit. Default 8 is conservative
	// for both cloud rate limits and local-disk syscall thrash. Set
	// to 1 to force sequential.
	RecheckConcurrency int
}

// applyDefaults fills in zero-value config fields with the documented
// defaults AND clamps operator-tunable knobs to sane bounds.
//
// One field has shape-#1 "0 is meaningful" semantics:
//
//   - MaxRootWalkDatabases: 0 explicitly disables the root-walk
//     fallback. The downstream guard at walk.go's derivePrefixes uses
//     `<= 0` to skip the walk, and operators set `0` in arc.toml to
//     reach that state. Negative values mean "operator did not set
//     this, fill default."
//
// All other fields: zero/negative folds into the default. The audit-
// only / report-only operating mode is reachable via
// ManifestOnlyDryRun=true (which also matches the production
// secure-by-default posture), so MaxDeletesPerRun=0 is NOT overloaded
// to mean "audit-only" — that would diverge from the behavioral knob
// already documented for that purpose.
//
// Over-large inputs are clamped at documented ceilings to defend
// against integer overflow (MaxRootWalkDatabases × 4 in walk.go) and
// goroutine fan-out (RecheckConcurrency).
func (c Config) applyDefaults() Config {
	if c.GraceWindow <= 0 {
		c.GraceWindow = 24 * time.Hour
	}
	if c.ClockSkewAllowance <= 0 {
		c.ClockSkewAllowance = 5 * time.Minute
	}
	if c.PerPrefixTimeout <= 0 {
		c.PerPrefixTimeout = 5 * time.Minute
	}
	if c.MaxRunDuration <= 0 {
		c.MaxRunDuration = 30 * time.Minute
	}
	if c.MaxManifestSize <= 0 {
		c.MaxManifestSize = 200_000
	}
	if c.MaxDeletesPerRun <= 0 {
		c.MaxDeletesPerRun = 10_000
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.SamplePathsCap <= 0 {
		c.SamplePathsCap = 10
	}
	if c.RecheckConcurrency <= 0 {
		c.RecheckConcurrency = 8
	} else if c.RecheckConcurrency > maxRecheckConcurrency {
		// Clamp the goroutine fan-out per chunk. Going above 64 trades
		// cloud per-second rate-limit budget for negligible latency
		// improvement and risks SDK connection-pool exhaustion.
		c.RecheckConcurrency = maxRecheckConcurrency
	}

	// MaxRootWalkDatabases: shape-#1 "0 is meaningful" — only this one
	// field uses the `< 0 = default, 0 = disabled, > 0 = use as cap`
	// convention. The downstream guard at walk.go's derivePrefixes
	// gates root walk on `MaxRootWalkDatabases > 0`.
	if c.MaxRootWalkDatabases < 0 {
		c.MaxRootWalkDatabases = 1000
	} else if c.MaxRootWalkDatabases > maxRootWalkDatabasesCap {
		// Defend against integer overflow in the `× 4` scan-limit math
		// in walk.go. The realistic upper bound for "unknown databases
		// inspected per tick" is well below this ceiling.
		c.MaxRootWalkDatabases = maxRootWalkDatabasesCap
	}
	return c
}

// Tunable upper bounds. Exported as private constants because operators
// who legitimately need to override them should be raising these in
// source rather than via runtime config — both ceilings exist to defend
// against accidental misconfiguration, not as policy limits.
const (
	maxRootWalkDatabasesCap = 1_000_000
	maxRecheckConcurrency   = 64
)

// AbortReason categorizes early-exit conditions for a run. Stable string
// values are documented because audit consumers depend on them.
type AbortReason string

const (
	AbortRaftQuorumLoss   AbortReason = "raft_quorum_loss"
	AbortLeaseLost        AbortReason = "lease_lost"
	AbortCtxCanceled      AbortReason = "ctx_canceled"
	AbortManifestTooLarge AbortReason = "manifest_too_large"
	AbortDisabled         AbortReason = "disabled"
	// AbortUnknown is the catch-all for errors that don't match a more
	// specific reason. Audit consumers should treat it as "investigate"
	// rather than rolling it into Raft dashboards.
	AbortUnknown AbortReason = "unknown"
)

// Run is a single reconcile-cycle summary. Held in the reconciler's ring
// buffer of recent runs and surfaced via Status().
type Run struct {
	ID         string    `json:"id"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	DryRun     bool      `json:"dry_run"`
	BackendKind BackendKind `json:"backend_kind"`
	Role       string    `json:"role"`

	// Counts
	ManifestFileCount   int `json:"manifest_file_count"`
	StorageFileCount    int `json:"storage_file_count"`
	OrphanManifestCount int `json:"orphan_manifest_count"`
	OrphanStorageCount  int `json:"orphan_storage_count"`
	ManifestDeletes     int `json:"manifest_deletes"`
	StorageDeletes      int `json:"storage_deletes"`
	SkippedGrace        int `json:"skipped_grace"`
	SkippedRecheck      int `json:"skipped_recheck"`

	// Bounded samples for operator visibility
	OrphanManifestSample []string `json:"orphan_manifest_sample,omitempty"`
	OrphanStorageSample  []string `json:"orphan_storage_sample,omitempty"`

	// Errors collected during the run, capped to keep memory bounded.
	Errors []string `json:"errors,omitempty"`

	// Aborted indicates the run did not complete its scheduled work.
	Aborted      bool        `json:"aborted"`
	AbortReason  AbortReason `json:"abort_reason,omitempty"`
	AbortMessage string      `json:"abort_message,omitempty"`

	// CapHit indicates the run stopped early because MaxDeletesPerRun was reached.
	CapHit bool `json:"cap_hit"`

	// WalkPartial indicates the storage walk could not list one or more
	// prefixes (transient backend failure or list timeout). Operators
	// reading "0 orphans found" runs need to know whether that's the
	// truth or a side-effect of a blind walk.
	WalkPartial bool `json:"walk_partial"`
}

// Reconciler is the core reconciliation engine. It is independent of the
// cron scheduler so it can be unit-tested by calling Reconcile directly.
type Reconciler struct {
	cfg         Config
	coord       Coordinator
	storage     storage.Backend
	gate        Gate
	auditLogger *audit.Logger
	logger      zerolog.Logger

	// runState ensures only one Reconcile is in flight at a time. The
	// scheduler also serializes via its own mutex but Reconcile is
	// callable directly (e.g. from tests, or from a future async
	// trigger), so we belt-and-braces here.
	runState atomic.Bool

	// History is a small fixed-size ring buffer of recent runs. All
	// access goes through mu. Layout: history is allocated to
	// runHistoryCap at construction; writes land at history[head] and
	// head advances modulo cap. size tracks how many slots are
	// populated (saturates at cap). This gives O(1) record + O(N)
	// read where N <= cap, vs the previous O(N) prepend.
	mu          sync.RWMutex
	history     []*Run
	historyHead int
	historySize int
	lastRun     *Run
}

const runHistoryCap = 10

// NewReconciler constructs a reconciler. coord may be nil only for
// BackendStandalone — every other mode requires a real coordinator.
// auditLogger may be nil; audit events are silently dropped in that case.
func NewReconciler(
	cfg Config,
	coord Coordinator,
	store storage.Backend,
	gate Gate,
	auditLogger *audit.Logger,
	logger zerolog.Logger,
) (*Reconciler, error) {
	cfg = cfg.applyDefaults()

	if store == nil {
		return nil, fmt.Errorf("reconciliation: storage backend is required")
	}
	if cfg.BackendKind == "" {
		return nil, fmt.Errorf("reconciliation: backend_kind must be set")
	}
	if cfg.BackendKind != BackendStandalone && coord == nil {
		return nil, fmt.Errorf("reconciliation: coordinator is required for backend kind %q", cfg.BackendKind)
	}
	if cfg.BackendKind == BackendLocal && cfg.LocalNodeID == "" {
		return nil, fmt.Errorf("reconciliation: local_node_id is required for backend kind %q", cfg.BackendKind)
	}

	return &Reconciler{
		cfg:         cfg,
		coord:       coord,
		storage:     store,
		gate:        gate,
		auditLogger: auditLogger,
		logger:      logger.With().Str("component", "reconciliation").Logger(),
	}, nil
}

// Config returns a copy of the active configuration. Useful for the
// scheduler wrapper and the API status endpoint.
func (r *Reconciler) Config() Config {
	return r.cfg
}

// LastRun returns the most recent completed run, or nil if no run has
// finished yet.
func (r *Reconciler) LastRun() *Run {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastRun
}

// RecentRuns returns up to runHistoryCap recent runs in reverse chronological
// order (newest first). The result is a freshly allocated slice; callers may
// safely retain it.
func (r *Reconciler) RecentRuns() []*Run {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotHistoryLocked()
}

// FindRun returns the run with the given ID from the recent-runs ring
// buffer, or (nil, false) if not found.
func (r *Reconciler) FindRun(id string) (*Run, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// Walk the ring newest-first so a callback that happens to know the
	// id of a recent run finds it on the first slot.
	for i := 0; i < r.historySize; i++ {
		idx := (r.historyHead - 1 - i + runHistoryCap) % runHistoryCap
		if r.history[idx] != nil && r.history[idx].ID == id {
			return r.history[idx], true
		}
	}
	return nil, false
}

// IsRunning reports whether a Reconcile call is currently in progress.
func (r *Reconciler) IsRunning() bool {
	return r.runState.Load()
}

// recordRun writes the run into the ring buffer and updates lastRun.
// O(1): one slot write, two integer updates. The previous prepend
// implementation was O(N).
func (r *Reconciler) recordRun(run *Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastRun = run
	if r.history == nil {
		r.history = make([]*Run, runHistoryCap)
	}
	r.history[r.historyHead] = run
	r.historyHead = (r.historyHead + 1) % runHistoryCap
	if r.historySize < runHistoryCap {
		r.historySize++
	}
}

// snapshotHistoryLocked materializes the ring as a newest-first slice.
// Caller must hold mu (read or write).
func (r *Reconciler) snapshotHistoryLocked() []*Run {
	out := make([]*Run, 0, r.historySize)
	for i := 0; i < r.historySize; i++ {
		idx := (r.historyHead - 1 - i + runHistoryCap) % runHistoryCap
		if r.history[idx] != nil {
			out = append(out, r.history[idx])
		}
	}
	return out
}

// Reconcile runs a single reconciliation cycle. Steps 2–7 of the algorithm
// land in subsequent commits; this stub returns ErrDisabled when disabled
// and an empty Run otherwise so lifecycle tests can pin down the contract.
func (r *Reconciler) Reconcile(ctx context.Context, dryRun bool) (*Run, error) {
	if !r.cfg.Enabled {
		return nil, ErrDisabled
	}
	if !r.runState.CompareAndSwap(false, true) {
		return nil, ErrAlreadyRunning
	}
	defer r.runState.Store(false)

	if r.gate != nil && !r.gate.ShouldRunStorageScan() && !r.gate.ShouldRunManifestSweep() {
		return nil, ErrGated
	}

	// Bound the run by ctx + MaxRunDuration. The scheduler also passes
	// a context with timeout but we apply our own cap here so direct
	// callers (tests, future async trigger) get the same protection.
	runCtx, cancel := context.WithTimeout(ctx, r.cfg.MaxRunDuration)
	defer cancel()

	run := &Run{
		ID:          uuid.NewString(),
		StartedAt:   time.Now().UTC(),
		DryRun:      dryRun || r.cfg.ManifestOnlyDryRun,
		BackendKind: r.cfg.BackendKind,
	}
	if r.gate != nil {
		run.Role = r.gate.Role()
	}

	r.logger.Info().
		Str("run_id", run.ID).
		Bool("dry_run", run.DryRun).
		Str("backend_kind", string(run.BackendKind)).
		Str("role", run.Role).
		Msg("Reconciliation run started")

	r.emitAudit("reconcile.run_started", run, map[string]string{
		"run_id":       run.ID,
		"dry_run":      boolStr(run.DryRun),
		"backend_kind": string(run.BackendKind),
	})

	// Step 1: snapshot the manifest.
	manifest, abortErr := r.snapshotManifest(runCtx)
	if abortErr != nil {
		r.markAborted(run, abortErr)
		r.finalizeRun(run)
		return run, abortErr
	}
	run.ManifestFileCount = len(manifest)

	// In BackendLocal mode the per-node walk is conceptually scoped to
	// "files this node owns". Reflect that in the manifest set used for
	// diff so a file owned by node-B (and physically present on node-B's
	// disk) doesn't show up as orphan-storage on node-A.
	keys := r.manifestToKeys(fileEntriesToKeys(manifest))
	// Drop the full FileEntry slice now that we've reduced to ObjectKeys.
	// At MaxManifestSize=200k entries this releases ~40 MB of pinned
	// FileEntry data (SHA256, SizeBytes, CreatedAt, etc.) for GC during
	// the rest of the run. The walk + sweeps only need the lighter
	// ObjectKey form going forward.
	manifest = nil

	// Step 2-3: walk storage by per-prefix listing, then compute the diff.
	walkRes, walkErr := r.walkStorage(runCtx, keys)
	if walkErr != nil {
		r.markAborted(run, walkErr)
		r.finalizeRun(run)
		return run, walkErr
	}
	run.StorageFileCount = len(walkRes.records)
	run.WalkPartial = walkRes.partial
	for _, e := range walkRes.prefixErrors {
		run.Errors = appendBounded(run.Errors, e, 32)
	}

	graceTotal := r.cfg.GraceWindow + r.cfg.ClockSkewAllowance
	diff := computeDiff(keys, walkRes.records, time.Now().UTC(), graceTotal)
	run.OrphanManifestCount = len(diff.orphanManifest)
	run.OrphanStorageCount = len(diff.orphanStorage)
	run.SkippedGrace = diff.skippedGraceCount
	run.OrphanManifestSample = sampleStrings(diff.orphanManifest, r.cfg.SamplePathsCap)
	run.OrphanStorageSample = sampleCandidatePaths(diff.orphanStorage, r.cfg.SamplePathsCap)

	// Step 4: orphan-manifest sweep. Cheap, retryable, no risk of data
	// loss — runs first so a Raft quorum loss aborts the cycle BEFORE
	// we touch any storage bytes.
	if sweepErr := r.sweepOrphanManifest(runCtx, run, diff.orphanManifest, run.DryRun); sweepErr != nil {
		r.markAborted(run, sweepErr)
		r.finalizeRun(run)
		return run, sweepErr
	}

	// Step 5: orphan-storage sweep. Only runs if step 4 succeeded —
	// manifest-before-storage ordering for the whole cycle. The cap
	// flag may already be set by step 4, in which case sweepOrphanStorage
	// short-circuits immediately on the first chunk.
	if sweepErr := r.sweepOrphanStorage(runCtx, run, diff.orphanStorage, run.DryRun); sweepErr != nil {
		r.markAborted(run, sweepErr)
		r.finalizeRun(run)
		return run, sweepErr
	}

	r.finalizeRun(run)
	return run, nil
}

// fileEntriesToKeys reduces full FileEntry records to the lighter ObjectKey
// form the walk/diff use. Path / Database / Measurement / OriginNodeID
// are the only fields the post-snapshot stages need, and reducing early
// frees the rest of the entry struct (SHA256, SizeBytes, CreatedAt, …)
// for GC during long runs.
func fileEntriesToKeys(entries []*raft.FileEntry) []*ObjectKey {
	out := make([]*ObjectKey, 0, len(entries))
	for _, e := range entries {
		if e == nil {
			continue
		}
		out = append(out, &ObjectKey{
			Path:         e.Path,
			Database:     e.Database,
			Measurement:  e.Measurement,
			OriginNodeID: e.OriginNodeID,
		})
	}
	return out
}

// manifestToKeys filters the manifest to files this node is responsible
// for. In BackendShared and BackendStandalone the local node owns
// nothing exclusively and the full manifest is returned. In
// BackendLocal mode it filters to files this node owns — a file owned
// by another node lives on that node's disk and is invisible to our
// walk anyway, so leaving it in the manifest set would produce a false
// orphan-manifest signal. NewReconciler rejects BackendLocal with an
// empty LocalNodeID, so we don't need to defend against that here.
func (r *Reconciler) manifestToKeys(manifest []*ObjectKey) []*ObjectKey {
	if r.cfg.BackendKind != BackendLocal {
		return manifest
	}
	out := make([]*ObjectKey, 0, len(manifest))
	for _, e := range manifest {
		if e.OriginNodeID == "" || e.OriginNodeID == r.cfg.LocalNodeID {
			out = append(out, e)
		}
	}
	return out
}

// snapshotManifest is step 1: read the manifest, build the path→entry map,
// and bail out if the manifest exceeds MaxManifestSize. Returned as a slice
// to keep the door open for sorted iteration in step 4 if useful, but
// callers that need O(1) membership build their own map.
func (r *Reconciler) snapshotManifest(ctx context.Context) ([]*raft.FileEntry, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if r.cfg.BackendKind == BackendStandalone || r.coord == nil {
		// Standalone: no manifest. The "diff" degenerates to "every storage
		// file is an orphan" — but standalone has no consensus to reconcile
		// against, so subsequent commits will short-circuit the run at this
		// point. For now return an empty slice so step 1 still completes.
		return nil, nil
	}
	manifest := r.coord.GetFileManifest()
	if len(manifest) > r.cfg.MaxManifestSize {
		return nil, fmt.Errorf("%w: have %d, max %d", ErrManifestTooLarge, len(manifest), r.cfg.MaxManifestSize)
	}
	return manifest, nil
}

// finalizeRun stamps FinishedAt, logs a summary, emits the run_completed
// audit event, and persists the run into the ring buffer.
func (r *Reconciler) finalizeRun(run *Run) {
	run.FinishedAt = time.Now().UTC()
	duration := run.FinishedAt.Sub(run.StartedAt)

	logEv := r.logger.Info().
		Str("run_id", run.ID).
		Bool("dry_run", run.DryRun).
		Bool("aborted", run.Aborted).
		Int("manifest_file_count", run.ManifestFileCount).
		Int("storage_file_count", run.StorageFileCount).
		Int("orphan_manifest", run.OrphanManifestCount).
		Int("orphan_storage", run.OrphanStorageCount).
		Int("manifest_deletes", run.ManifestDeletes).
		Int("storage_deletes", run.StorageDeletes).
		Int("skipped_grace", run.SkippedGrace).
		Dur("duration", duration)
	if run.Aborted {
		logEv = logEv.Str("abort_reason", string(run.AbortReason))
	}
	logEv.Msg("Reconciliation run completed")

	// SIEM/dashboard consumers depend on these audit fields to tell
	// "0 orphans found, all clean" apart from "blind walk produced 0
	// candidates" or "we hit the per-run cap and stopped early."
	// Threading walk_partial / cap_hit / backend_kind through both
	// completed and aborted detail maps keeps the two run states
	// queryable on the same shape.
	if run.Aborted {
		r.emitAudit("reconcile.run_aborted", run, map[string]string{
			"run_id":           run.ID,
			"backend_kind":     string(run.BackendKind),
			"abort_reason":    string(run.AbortReason),
			"abort_message":   run.AbortMessage,
			"manifest_deletes": strconv.Itoa(run.ManifestDeletes),
			"storage_deletes":  strconv.Itoa(run.StorageDeletes),
			"walk_partial":    boolStr(run.WalkPartial),
			"cap_hit":         boolStr(run.CapHit),
			"duration_ms":     strconv.FormatInt(duration.Milliseconds(), 10),
		})
	} else {
		r.emitAudit("reconcile.run_completed", run, map[string]string{
			"run_id":           run.ID,
			"backend_kind":     string(run.BackendKind),
			"manifest_deletes": strconv.Itoa(run.ManifestDeletes),
			"storage_deletes":  strconv.Itoa(run.StorageDeletes),
			"skipped_grace":    strconv.Itoa(run.SkippedGrace),
			"skipped_recheck":  strconv.Itoa(run.SkippedRecheck),
			"walk_partial":     boolStr(run.WalkPartial),
			"cap_hit":          boolStr(run.CapHit),
			"duration_ms":      strconv.FormatInt(duration.Milliseconds(), 10),
		})
	}

	r.recordRun(run)
}

// markAborted classifies an error into an AbortReason and stamps the run.
// Defaults to AbortUnknown rather than Raft — misclassification on
// audit dashboards is worse than admitting we don't know. Specific
// classifications come from typed errors / errors.Is checks.
func (r *Reconciler) markAborted(run *Run, err error) {
	run.Aborted = true
	run.AbortMessage = err.Error()
	switch {
	case errors.Is(err, ErrManifestTooLarge):
		run.AbortReason = AbortManifestTooLarge
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		run.AbortReason = AbortCtxCanceled
	case isLeaseLostError(err):
		// Lease loss is a normal failover event, not a quorum loss.
		// Distinguishing it keeps lease-handoff noise off the Raft
		// dashboards.
		run.AbortReason = AbortLeaseLost
	case isRaftError(err):
		run.AbortReason = AbortRaftQuorumLoss
	default:
		run.AbortReason = AbortUnknown
	}
}

// isLeaseLostError reports whether the error came from the per-chunk
// gate re-check inside a sweep (lease handoff mid-run). Both sweeps
// wrap ErrGateRevoked, so errors.Is is robust to log-string changes.
func isLeaseLostError(err error) bool {
	return errors.Is(err, ErrGateRevoked)
}

// isRaftError reports whether an error originated from a Raft manifest
// apply failure. Uses errors.Is against raft.ErrManifestApply, which
// the cluster.Coordinator wraps into every manifest-write error
// (RegisterFileInManifest, DeleteFileFromManifest, BatchFileOpsInManifest,
// UpdateFileInManifest). Lives in the leaf raft package so consumers
// can import it without an import cycle.
func isRaftError(err error) bool {
	return errors.Is(err, raft.ErrManifestApply)
}

// emitAudit sends an audit event if the audit logger is configured. The
// detail map is JSON-encoded by the audit package.
func (r *Reconciler) emitAudit(eventType string, run *Run, detail map[string]string) {
	if r.auditLogger == nil {
		return
	}
	r.auditLogger.LogEvent(&audit.AuditEvent{
		EventType:  eventType,
		Actor:      "reconciler",
		Method:     "INTERNAL",
		Path:       "/reconciliation/" + run.ID,
		StatusCode: 200,
		Detail:     detail,
	})
}
