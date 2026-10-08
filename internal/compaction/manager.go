package compaction

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ErrCycleAlreadyRunning is returned when attempting to start a compaction cycle while one is already in progress
var ErrCycleAlreadyRunning = errors.New("compaction cycle already running")

// ErrCompactionPaused is returned by a cycle that did not start, or stopped
// at a batch boundary, because compaction is paused cluster-wide (#1087): a
// cluster restore holds the pause while it rewrites the manifest. Batches
// already running finish first; the cycle status is "paused". Not an error
// for the scheduler, which logs it at Info.
var ErrCompactionPaused = errors.New("compaction is paused cluster-wide")

// Where a cycle came from, recorded on its outcome so an operator can tell
// their own trigger's cycle from the scheduler's. "unspecified" is the honest
// label for the public RunCompactionCycle* family: the scheduler reaches the
// cycle body through runCycleInternal directly, so those exported entries have
// no production caller today and guessing "scheduler" for them would mislabel
// whichever caller is added next.
const (
	cycleSourceAPI         = "api"
	cycleSourceScheduler   = "scheduler"
	cycleSourceUnspecified = "unspecified"
)

// CycleHistoryLimit bounds the retained cycle outcomes (#1162). Exported so
// the API can clamp its limit parameter to it.
//
// The default schedules are hourly "5 * * * *" plus daily "0 3 * * *", so a
// completely idle node still burns 25 ids a day -- every tick claims a cycle
// and records a slot even with zero candidates. 500 is therefore ~20 days at
// the defaults, and ~41 hours at the "*/5 * * * *" an operator reconciling a
// large migration might set. 100 (the bound appendJobHistory uses) would have
// been 4 days and 8.3 hours respectively: short enough that the scheduler's
// own empty cycles would evict an operator's trigger overnight.
const CycleHistoryLimit = 500

// cycleFailedSampleLimit bounds the per-cycle sample of failed partitions.
// The counters say how many batches failed; without the sample an operator has
// no way to learn which, because job history carries no cycle id and is capped
// at 10 entries in Stats. Mirrors skipped_sample (#977).
const cycleFailedSampleLimit = 10

// cycleProgress holds a running cycle's live counters. These were locals in
// runClaimed, which meant a lookup of a running cycle could report its scope
// and start time but nothing about progress -- useless for the multi-hour
// cycles this endpoint exists to observe. The outcome record holds a pointer
// while the cycle runs; the finalizer copies the values out and clears it.
type cycleProgress struct {
	discovered      atomic.Int64
	started         atomic.Int64
	succeeded       atomic.Int64
	failed          atomic.Int64
	interrupted     atomic.Int64
	discoveryErrors atomic.Int64

	mu           sync.Mutex
	failedSample []string
}

// recordFailure samples the partition of a failed batch, up to the cap.
func (p *cycleProgress) recordFailure(partitionPath string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.failedSample) >= cycleFailedSampleLimit {
		return
	}
	p.failedSample = append(p.failedSample, partitionPath)
}

// snapshotFailedSample copies the sample out from under the mutex.
func (p *cycleProgress) snapshotFailedSample() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.failedSample) == 0 {
		return nil
	}
	out := make([]string, len(p.failedSample))
	copy(out, p.failedSample)
	return out
}

// CycleRecord is the outcome of one compaction cycle. It records only eligible
// batches actually discovered during this cycle; unvisited measurements are
// never counted as unstarted work.
//
// Exported because the API serves it per cycle id (#1162). Retained in a
// bounded in-memory ring -- see CycleHistoryLimit.
type CycleRecord struct {
	CycleID int64
	Status  string
	Source  string

	// Requested scope. Databases is nil when the cycle covered every database
	// and Measurement is empty when it covered every measurement -- the cycle
	// body works on its own expanded copy, so these stay what the caller asked
	// for, which is what an operator is trying to match against.
	Databases   []string
	Measurement string
	Tiers       []string

	StartedAt  time.Time
	FinishedAt time.Time // zero while the cycle runs

	Discovered      int64
	Started         int64
	Succeeded       int64
	Failed          int64
	Interrupted     int64
	Unstarted       int64
	DiscoveryErrors int64

	// Err is the cycle error, truncated. A discovery-error count with no
	// reason attached is a dead end for whoever has to act on it.
	Err          string
	FailedSample []string

	// progress is non-nil only while the cycle runs, and is read only when
	// FinishedAt is zero. The finalizer copies the counters into the fields
	// above and clears it, so a finished outcome never follows the pointer.
	progress *cycleProgress
}

// counters returns the outcome with its live counters resolved. A running
// cycle's numbers come from progress; a finished one already has them.
func (o CycleRecord) counters() CycleRecord {
	p := o.progress
	if p == nil {
		return o
	}
	o.progress = nil
	o.Discovered = p.discovered.Load()
	o.Started = p.started.Load()
	o.Succeeded = p.succeeded.Load()
	o.Failed = p.failed.Load()
	o.Interrupted = p.interrupted.Load()
	o.DiscoveryErrors = p.discoveryErrors.Load()
	o.Unstarted = o.Discovered - o.Started
	o.FailedSample = p.snapshotFailedSample()
	return o
}

// clone deep-copies the slices so a retained outcome never shares backing
// storage with a caller outside the lock.
func (o CycleRecord) clone() CycleRecord {
	if o.Databases != nil {
		dbs := make([]string, len(o.Databases))
		copy(dbs, o.Databases)
		o.Databases = dbs
	}
	if o.Tiers != nil {
		tiers := make([]string, len(o.Tiers))
		copy(tiers, o.Tiers)
		o.Tiers = tiers
	}
	if o.FailedSample != nil {
		fs := make([]string, len(o.FailedSample))
		copy(fs, o.FailedSample)
		o.FailedSample = fs
	}
	return o
}

// Manager orchestrates compaction jobs across all measurements
type Manager struct {
	StorageBackend  storage.Backend
	LockManager     *LockManager
	ManifestManager *ManifestManager

	// Configuration
	MinAgeHours int
	MinFiles    int
	// MaxFilesPerBatch bounds how many files one compaction job feeds to a
	// single DuckDB read_parquet() call. Already clamped to the supported
	// range by NewManager.
	MaxFilesPerBatch int
	MaxConcurrent    int
	CycleTimeout     time.Duration // Shared budget for scheduled and manual cycles
	TempDirectory    string        // Temp directory for compaction files
	MemoryLimit      string        // DuckDB memory limit for EACH subprocess (e.g., "8GB")
	Threads          int           // DuckDB thread count for EACH subprocess (0 = DuckDB decides: the container CPU quota, or all cores when unlimited)

	// excludeDatabases holds compaction.exclude_databases as a set, built
	// once by NewManager from the normalized excludeList. Immutable after
	// construction, so readers need no lock. Consulted only by
	// filterExcludedDatabases — see that method for the semantics.
	excludeDatabases map[string]struct{}
	// excludeList is the normalized, de-duplicated configured order, kept
	// for startup logging and Stats().
	excludeList []string
	// Phase 4: local-disk directory where compaction subprocesses write
	// completion manifests for the parent-side CompletionWatcher to pick
	// up. Empty means "OSS mode, no completion-manifest handoff". Set by
	// the main wiring to filepath.Join(TempDirectory, ".completion", "pending")
	// when clustering + replication are enabled.
	CompletionDir string

	// Sort key configuration (from ingest config)
	SortKeysConfig  map[string][]string // measurement -> sort keys
	DefaultSortKeys []string            // default sort keys

	// Tiers
	Tiers []Tier

	// Job history (jobs run in subprocess, so no activeJobs tracking)
	jobHistory []map[string]interface{}

	// Cycle management - prevents concurrent compaction cycles.
	//
	// claimMu makes taking the claim and publishing its id one step (#1153).
	// Without it there is a window between the winner's CompareAndSwap and
	// its cycleID.Add in which another caller reads the PREVIOUS, finished
	// cycle's id -- and the trigger endpoint reports that id to an operator,
	// who has no way to tell it apart from the cycle that is actually
	// running. Held only around the claim and by the id accessors, never
	// across cycle work or any I/O.
	//
	// It closes that window only for callers that take it. Stats() still
	// reads cycleRunning and the id as two independent loads, so
	// /api/v1/compaction/stats can pair cycle_running true with the previous
	// cycle's id for an instant. Narrow, pre-existing, and not worth
	// serialising a stats read behind the claim.
	claimMu      sync.Mutex
	cycleRunning atomic.Bool
	cycleID      atomic.Int64

	// Metrics
	totalJobsCompleted   int
	totalJobsFailed      int
	totalJobsInterrupted int
	totalFilesCompacted  int
	totalBytesSaved      int64
	totalManifestsRecov  int // Number of manifests recovered

	// Callback invoked after a successful compaction job (in parent process).
	// Used to invalidate DuckDB and query caches after files are deleted.
	onCompactionComplete func()

	// syncEligibility, when set, restricts compaction inputs to files the
	// edge sync ledger reports delivered (issue #610): compacting an
	// undelivered raw would destroy the only copy of rows the hub never
	// received. nil means unrestricted (edge sync absent or the operator
	// opted out via edge_sync.spoke.defer_compaction_until_synced=false).
	// Guarded by mu, like onCompactionComplete.
	syncEligibility func(ctx context.Context, paths []string) (map[string]bool, error)

	// namespaceExpander, when set, returns the top-level segments this node
	// holds as an edge-sync HUB (the registered spoke IDs). The cycle
	// replaces each matching top-level directory with {spoke}/{child}
	// pseudo-databases so received data compacts like any other (#619);
	// candidates from pseudo-databases are SyncExempt. nil means no
	// expansion. Guarded by mu.
	namespaceExpander func(ctx context.Context) (map[string]struct{}, error)

	// onConsumedInputs, when set, receives the storage keys of the SOURCE
	// files each successful compaction actually consumed and deleted —
	// j.compactedFiles across IPC, or manifest.InputFiles on the recovery
	// path. The edge-sync hub marks the matching receipts compacted so the
	// sync protocol keeps answering "present" for content that now lives
	// inside an output (#619 / the #611 gate). FALLIBLE: a non-nil error
	// means the marks did not all commit, and the caller must KEEP the
	// retained crash-recovery manifest so recovery re-fires them — deleting
	// it anyway would silently lose the marks (deep-review B1). Guarded by mu.
	onConsumedInputs func(inputs []string) error

	// onCompactedOutput, when set, receives the storage key of every
	// compacted output the parent learns about — from a subprocess result
	// AND from manifest crash-recovery — so the edge sync ledger can mark
	// it as already-delivered content that must never sync (issue #610).
	onCompactedOutput func(storageKey string)

	// Nil in production. Deterministic cycle tests may inject a batch
	// runner without starting an external compaction subprocess.
	compactBatchForTest func(context.Context, Candidate) error

	// pauseGate, when set, reports whether compaction is paused cluster-wide
	// (#1087); main.go wires it to the cluster coordinator. Consulted at
	// cycle start, before every worker launch and between the batches of a
	// partition. nil (OSS, no cluster) means never paused. Lock-free so the
	// hot loop and Stats (which holds mu) can both read it.
	pauseGate atomic.Pointer[func() bool]

	lastCycle CycleRecord

	// cycleHistory retains the newest CycleHistoryLimit outcomes so a cycle id
	// can be looked up after the fact (#1162). In memory only, like
	// jobHistory: this package has no persistence, so the window dies with the
	// process. Append-only and monotonic in CycleID, which is what lets the
	// finalizer update the newest entry in place and the lookup treat the first
	// and last entries as the retained range.
	cycleHistory []CycleRecord

	logger zerolog.Logger
	mu     sync.Mutex
}

// SetPauseGate wires the cluster-wide compaction pause (#1087). The gate is
// read before every batch, so it must be cheap: the coordinator's is one FSM
// read and a clock comparison.
func (m *Manager) SetPauseGate(gate func() bool) {
	if gate == nil {
		m.pauseGate.Store(nil)
		return
	}
	m.pauseGate.Store(&gate)
}

// Paused reports whether compaction is paused cluster-wide. False when no
// gate is wired (OSS, standalone, a cluster without a Raft manifest).
func (m *Manager) Paused() bool {
	gate := m.pauseGate.Load()
	return gate != nil && (*gate)()
}

// ManagerConfig holds configuration for creating a compaction manager
type ManagerConfig struct {
	StorageBackend storage.Backend
	LockManager    *LockManager
	MinAgeHours    int
	MinFiles       int
	// MaxFilesPerBatch bounds files per compaction job. Out-of-range values
	// (including 0 from an unset field) are clamped by NewManager — see
	// clampFilesPerBatch.
	MaxFilesPerBatch int
	MaxConcurrent    int
	CycleTimeout     time.Duration // Zero selects the backward-compatible 30m default
	// ExcludeDatabases is compaction.exclude_databases: databases that
	// scheduled (unscoped) cycles skip during candidate discovery.
	// Database-scoped cycles bypass it. Normalized by NewManager.
	ExcludeDatabases []string
	TempDirectory    string              // Temp directory for compaction files
	MemoryLimit      string              // DuckDB memory limit for EACH subprocess (e.g., "8GB")
	Threads          int                 // DuckDB thread count for EACH subprocess (0 = DuckDB decides: the container CPU quota, or all cores when unlimited)
	CompletionDir    string              // Phase 4: local-disk completion-manifest dir (empty = OSS mode)
	SortKeysConfig   map[string][]string // Per-measurement sort keys from ingest config
	DefaultSortKeys  []string            // Default sort keys from ingest config
	Tiers            []Tier
	Logger           zerolog.Logger
}

// NewManager creates a new compaction manager
func NewManager(cfg *ManagerConfig) *Manager {
	// Set defaults
	if cfg.MinAgeHours == 0 {
		cfg.MinAgeHours = 1
	}
	if cfg.MinFiles == 0 {
		cfg.MinFiles = 10
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 2
	}
	if cfg.CycleTimeout <= 0 {
		cfg.CycleTimeout = 30 * time.Minute
	}
	// Clamp the batch size once here rather than per-partition, so an
	// out-of-range configured value produces exactly one warning at startup.
	// SplitCandidateIntoBatches clamps again defensively.
	maxFilesPerBatch, batchSizeAdjusted := clampFilesPerBatch(cfg.MaxFilesPerBatch)
	if cfg.TempDirectory == "" {
		cfg.TempDirectory = "./data/compaction"
	}

	// Set default sort keys if not provided
	sortKeysConfig := cfg.SortKeysConfig
	if sortKeysConfig == nil {
		sortKeysConfig = make(map[string][]string)
	}

	defaultSortKeys := cfg.DefaultSortKeys
	if defaultSortKeys == nil {
		defaultSortKeys = []string{"time"} // Default to time-only sorting
	}

	logger := cfg.Logger.With().Str("component", "compaction-manager").Logger()

	excludeList := normalizeExcludeDatabases(cfg.ExcludeDatabases)
	excludeSet := make(map[string]struct{}, len(excludeList))
	for _, db := range excludeList {
		excludeSet[db] = struct{}{}
	}

	m := &Manager{
		StorageBackend:   cfg.StorageBackend,
		LockManager:      cfg.LockManager,
		ManifestManager:  NewManifestManager(cfg.StorageBackend, logger),
		MinAgeHours:      cfg.MinAgeHours,
		MinFiles:         cfg.MinFiles,
		MaxFilesPerBatch: maxFilesPerBatch,
		MaxConcurrent:    cfg.MaxConcurrent,
		CycleTimeout:     cfg.CycleTimeout,
		TempDirectory:    cfg.TempDirectory,
		MemoryLimit:      cfg.MemoryLimit,
		Threads:          cfg.Threads,
		CompletionDir:    cfg.CompletionDir,
		SortKeysConfig:   sortKeysConfig,
		DefaultSortKeys:  defaultSortKeys,
		Tiers:            cfg.Tiers,
		excludeDatabases: excludeSet,
		excludeList:      excludeList,
		jobHistory:       make([]map[string]interface{}, 0),
		logger:           logger,
	}

	if len(excludeList) > 0 {
		m.logger.Info().
			Strs("exclude_databases", excludeList).
			Msg("Compaction exclusion list active: scheduled cycles skip these databases; database-scoped manual triggers bypass the list")
		for _, db := range excludeList {
			if excludeEntryCanNeverMatch(db) {
				m.logger.Warn().
					Str("entry", db).
					Msg("compaction.exclude_databases entry can never match a discovered database; check for a typo")
			}
		}
	}

	if batchSizeAdjusted {
		m.logger.Warn().
			Int("configured", cfg.MaxFilesPerBatch).
			Int("effective", maxFilesPerBatch).
			Int("min", MinFilesPerBatch).
			Int("max", MaxAllowedFilesPerBatch).
			Msg("compaction.max_files_per_batch out of range; using adjusted value")
	}

	// Log tier information
	if len(m.Tiers) > 0 {
		var enabledTiers []string
		for _, tier := range m.Tiers {
			if tier.IsEnabled() {
				enabledTiers = append(enabledTiers, tier.GetTierName())
			}
		}
		m.logger.Info().
			Strs("tiers", enabledTiers).
			Str("subprocess_memory_limit", m.MemoryLimit).
			Int("subprocess_threads", m.Threads).
			Int("max_concurrent", m.MaxConcurrent).
			Msg("Compaction manager initialized with tiers")
	} else {
		m.logger.Info().
			Str("subprocess_memory_limit", m.MemoryLimit).
			Int("subprocess_threads", m.Threads).
			Int("max_concurrent", m.MaxConcurrent).
			Msg("Compaction manager initialized (no tiers)")
	}

	return m
}

// appendJobHistory keeps the newest 100 job-stat maps. When trimming is
// required, copy the retained map references into fresh slice storage so the
// current history does not keep an older backing array that may retain evicted
// pointer-containing entries. The maps themselves are intentionally shared.
func appendJobHistory(history []map[string]interface{}, jobStats map[string]interface{}) []map[string]interface{} {
	history = append(history, jobStats)
	if len(history) <= 100 {
		return history
	}

	retained := make([]map[string]interface{}, 100)
	copy(retained, history[len(history)-100:])
	return retained
}

// appendCycleHistory keeps the newest CycleHistoryLimit cycle outcomes. Same
// trim-from-the-front shape as appendJobHistory, so the newest entry stays
// last -- the invariant the in-place finalizer update relies on.
func appendCycleHistory(history []CycleRecord, outcome CycleRecord) []CycleRecord {
	history = append(history, outcome)
	if len(history) <= CycleHistoryLimit {
		return history
	}

	retained := make([]CycleRecord, CycleHistoryLimit)
	copy(retained, history[len(history)-CycleHistoryLimit:])
	return retained
}

// SetOnCompactionComplete sets the callback invoked after each successful compaction job.
// This is used to invalidate DuckDB and query caches in the parent process after
// the compaction subprocess deletes old parquet files.
// Safe to call concurrently with running compaction jobs (the setter takes
// m.mu): main.go wires this callback after the schedulers have already
// started, so a job may complete concurrently (#351).
func (m *Manager) SetOnCompactionComplete(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onCompactionComplete = fn
}

// SetSyncEligibility installs the edge sync delivery gate (issue #610).
// A fail-closed placeholder may be installed at construction time and
// swapped for the ledger-backed hook once the spoke ledger exists — a
// scheduler tick during slow startup must never run unfiltered.
func (m *Manager) SetSyncEligibility(fn func(ctx context.Context, paths []string) (map[string]bool, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncEligibility = fn
}

// SetOnCompactedOutput installs the compacted-output observer (issue #610),
// covering both delivery paths: subprocess results (CompactPartition) and
// manifest crash-recovery (notifyCompactedOutput is handed to
// RecoverOrphanedManifests per call, copied under mu there).
func (m *Manager) SetOnCompactedOutput(fn func(storageKey string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onCompactedOutput = fn
}

// filterSyncEligibility drops candidate files the edge sync ledger has not
// confirmed delivered, re-applying the TIER's MinFiles threshold to what
// remains. Returns false when the partition should be deferred this cycle.
// A hook error defers the whole candidate: the fail-safe direction is to
// compact nothing rather than risk consuming undelivered rows.
func (m *Manager) filterSyncEligibility(ctx context.Context, candidate Candidate, tier Tier) (Candidate, bool) {
	filtered, eligible, _ := m.filterSyncEligibilityWithError(ctx, candidate, tier)
	return filtered, eligible
}

func (m *Manager) filterSyncEligibilityWithError(ctx context.Context, candidate Candidate, tier Tier) (Candidate, bool, error) {
	if candidate.SyncExempt {
		// Expander-produced spoke-namespace candidates: received data is
		// never owed upstream (the node's own sync discovery excludes these
		// namespaces for exactly that reason), so the delivery gate does not
		// apply — without this bypass a dual-role node would defer every
		// received partition forever (#619 review F2).
		return candidate, true, nil
	}
	m.mu.Lock()
	fn := m.syncEligibility
	m.mu.Unlock()
	if fn == nil {
		return candidate, true, nil
	}

	eligible, err := fn(ctx, candidate.Files)
	if err != nil {
		if ctx.Err() == nil {
			m.logger.Warn().Err(err).
				Str("partition", candidate.PartitionPath).
				Msg("Sync eligibility lookup failed; deferring this partition (fail-safe)")
		}
		return candidate, false, err
	}

	kept := make([]string, 0, len(candidate.Files))
	for _, f := range candidate.Files {
		if eligible[f] {
			kept = append(kept, f)
		}
	}
	deferred := len(candidate.Files) - len(kept)
	if deferred > 0 {
		m.logger.Info().
			Str("partition", candidate.PartitionPath).
			Str("tier", candidate.Tier).
			Int("deferred", deferred).
			Int("eligible", len(kept)).
			Msg("Edge sync deferral: files await delivery before compaction")
	}
	if len(kept) < tier.GetMinFiles() {
		return candidate, false, nil
	}
	candidate.Files = kept
	candidate.FileCount = len(kept)
	return candidate, true, nil
}

// SetNamespaceExpander installs the hub's spoke-namespace expansion (#619).
func (m *Manager) SetNamespaceExpander(fn func(ctx context.Context) (map[string]struct{}, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.namespaceExpander = fn
}

// SetOnConsumedInputs installs the consumed-inputs observer (#619).
func (m *Manager) SetOnConsumedInputs(fn func(inputs []string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onConsumedInputs = fn
}

// notifyConsumedInputs invokes the consumed-inputs observer, if any. A
// returned error means the receipt marks did not all commit.
func (m *Manager) notifyConsumedInputs(inputs []string) error {
	if len(inputs) == 0 {
		return nil
	}
	m.mu.Lock()
	fn := m.onConsumedInputs
	m.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(inputs)
}

// expandNamespaces replaces registered spoke namespaces in a database list
// with {spoke}/{child} pseudo-databases (#619). A bare spoke directory
// yields no candidates on its own (its children are databases, not
// partition years), so it is dropped once expanded. Expander errors are
// fail-safe: spoke top-level dirs are SKIPPED for this cycle (never
// compacted unexpanded) and everything else proceeds.
func (m *Manager) expandNamespaces(ctx context.Context, databases []string) []string {
	m.mu.Lock()
	fn := m.namespaceExpander
	m.mu.Unlock()
	if fn == nil {
		return databases
	}

	spokes, err := fn(ctx)
	if err != nil {
		m.logger.Warn().Err(err).
			Msg("Spoke-namespace lookup failed; skipping received namespaces this cycle (fail-safe)")
		spokes = nil // fall through: unknown set, treat nothing as a spoke
	}
	if len(spokes) == 0 {
		return databases
	}

	out := make([]string, 0, len(databases))
	for _, db := range databases {
		if _, isSpoke := spokes[db]; !isSpoke {
			out = append(out, db)
			continue
		}
		children, err := m.listMeasurements(ctx, db)
		if err != nil {
			m.logger.Warn().Err(err).Str("spoke", db).
				Msg("Could not list a spoke namespace; skipping it this cycle")
			continue
		}
		for _, child := range children {
			out = append(out, db+"/"+child)
		}
	}
	return out
}

// normalizeExcludeDatabases canonicalizes compaction.exclude_databases:
// entries are whitespace-trimmed, de-duplicated, and empties dropped.
// Nothing else is rewritten — matching is exact and case-sensitive, with no
// prefixes, globs, or separator splitting, so an entry can never bleed onto
// a sibling database ("wh" must not match "wh-other"; see the #534 class),
// and a spoke namespace whose ID legally contains a comma or dot is
// excluded verbatim rather than silently split into fragments that exclude
// unrelated databases. Environment overrides need no splitting here either:
// viper delivers ARC_COMPACTION_EXCLUDE_DATABASES="a b" as separate
// whitespace-separated entries already; a name the environment form cannot
// express belongs in the arc.toml array.
func normalizeExcludeDatabases(entries []string) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, entry := range entries {
		name := strings.TrimSpace(entry)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

// excludeEntryCanNeverMatch reports whether an exclusion entry can never
// equal a database name that candidate discovery produces: listDatabases
// skips reserved root directories (leading "_" or ".") and compaction's
// scratch root; discovered names carry no path escapes or control
// characters; and slashes appear only in the exactly-one-slash
// "spoke/child" form that expandNamespaces builds — so a leading or
// trailing slash ("staging/") or two-plus slashes is always a typo, never
// a match. Warning on these catches configuration mistakes without ever
// rejecting a legal spoke namespace, whose IDs may contain dots, commas,
// or leading digits (validateSpokeID is a blocklist, not a database-name
// allowlist).
func excludeEntryCanNeverMatch(name string) bool {
	if storage.IsReservedRootDir(name) || name == "compaction" {
		return true
	}
	if strings.Contains(name, "\\") || strings.Contains(name, "..") {
		return true
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") ||
		strings.Count(name, "/") >= 2 {
		return true
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// filterExcludedDatabases drops databases named in
// compaction.exclude_databases. Callers apply it to unscoped discovery only
// — an operator explicitly scoping a cycle to one database bypasses the
// list — and apply it on both sides of namespace expansion, so an excluded
// spoke parent ("spoke1") never has its children listed, and a single
// received pseudo-database ("spoke1/telemetry") can be excluded on its own.
// Manifest recovery is deliberately NOT filtered: exclusion gates new
// candidate discovery, never the completion of work a previous cycle
// already started.
func (m *Manager) filterExcludedDatabases(databases []string) []string {
	if len(m.excludeDatabases) == 0 {
		return databases
	}
	out := make([]string, 0, len(databases))
	var skipped []string
	for _, db := range databases {
		if _, excluded := m.excludeDatabases[db]; excluded {
			skipped = append(skipped, db)
			continue
		}
		out = append(out, db)
	}
	if len(skipped) > 0 {
		m.logger.Debug().
			Strs("excluded", skipped).
			Msg("Skipping databases excluded from compaction")
	}
	return out
}

// ExcludedDatabases returns the normalized compaction.exclude_databases
// list, for operator-facing surfaces (trigger responses, stats). The
// returned slice is a copy; the configuration itself is immutable after
// construction.
func (m *Manager) ExcludedDatabases() []string {
	if len(m.excludeList) == 0 {
		return nil
	}
	return append([]string(nil), m.excludeList...)
}

// sanitizeDBForName replaces slashes with dots so the database portion of
// a job ID is one path segment. Job IDs also name temporary directories and
// completion-manifest files; validateJobID rejects path separators.
//
// This is NOT a unique database encoding. Spoke IDs may contain dots, so
// "rocket.01/telemetry" and "rocket/01.telemetry" both become
// "rocket.01.telemetry". Never use this token alone as a database identity.
// Manager-generated job IDs also include the folded partition path, which
// differs for this example. The original database name is not changed.
func sanitizeDBForName(database string) string {
	return strings.ReplaceAll(database, "/", ".")
}

// cleanupSubprocessTempDir removes the exact working directory owned by a
// subprocess. The JobID validation prevents an empty or path-like ID from
// resolving to the temp-directory base or escaping it.
func cleanupSubprocessTempDir(tempDirectory, jobID string) error {
	if tempDirectory == "" {
		return errors.New("cannot cleanup subprocess temp directory with an empty base directory")
	}
	if err := validateJobID(jobID); err != nil {
		return fmt.Errorf("cannot cleanup subprocess temp directory: %w", err)
	}
	return os.RemoveAll(jobTempDir(tempDirectory, jobID))
}

// notifyCompactedOutput invokes the compacted-output observer, if any.
func (m *Manager) notifyCompactedOutput(storageKey string) {
	if storageKey == "" {
		return
	}
	m.mu.Lock()
	fn := m.onCompactedOutput
	m.mu.Unlock()
	if fn != nil {
		fn(storageKey)
	}
}

// recoverOrphanedManifests wires cluster completion updates into storage recovery.
func (m *Manager) recoverOrphanedManifests(ctx context.Context, scope recoveryScope) (int, error) {
	hooks := recoveryHooks{}
	if m.CompletionDir != "" {
		hooks.beforeInputDelete = m.writeRecoveredOutputWrittenManifest
		hooks.afterInputDelete = m.writeRecoveredSourcesDeletedManifest
	}
	return m.ManifestManager.recoverOrphanedManifestsWithHooks(ctx, scope, m.notifyCompactedOutput, m.notifyConsumedInputs, hooks)
}

// FindCandidates finds partitions that are candidates for compaction across all databases
func (m *Manager) FindCandidates(ctx context.Context) ([]Candidate, error) {
	var candidates []Candidate

	m.logger.Info().Msg("Scanning for compaction candidates")

	tiers := enabledTiers(m.Tiers)
	if len(tiers) == 0 {
		m.logger.Info().Int("candidates", 0).Msg("Found compaction candidates")
		return nil, nil
	}

	// Discover all databases. Namespace expansion here too, so the
	// candidates listing endpoint previews the same partitions a cycle
	// would actually process (#619 review F4).
	databases, err := m.listDatabases(ctx)
	if err != nil {
		return nil, err
	}
	// The exclusion list applies here exactly as in a scheduled cycle, on
	// both sides of the expansion, so this endpoint keeps previewing what
	// a cycle would actually process (#619 review F4).
	databases = m.filterExcludedDatabases(databases)
	databases = m.expandNamespaces(ctx, databases)
	databases = m.filterExcludedDatabases(databases)

	m.logger.Info().Strs("databases", databases).Msg("Discovered databases for compaction")

	// Process each database
	for _, database := range databases {
		// List all measurements in this database
		measurements, err := m.listMeasurements(ctx, database)
		if err != nil {
			m.logger.Error().Err(err).Str("database", database).Msg("Failed to list measurements")
			continue
		}

		// Find candidates for each measurement across all tiers. Nothing
		// changes the store between tiers here, so one listing serves them
		// all (#316).
		for _, meas := range measurements {
			objects, err := m.StorageBackend.List(ctx, measurementPrefix(database, meas))
			if err != nil {
				m.logger.Error().Err(err).
					Str("database", database).
					Str("measurement", meas).
					Msg("Failed to list measurement objects")
				continue
			}
			for _, tier := range tiers {
				candidates = append(candidates, tier.FindCandidatesFromListing(database, meas, objects)...)
			}
		}
	}

	m.logger.Info().Int("candidates", len(candidates)).Msg("Found compaction candidates")
	return candidates, nil
}

// CompactPartition compacts a single partition using subprocess isolation.
// Running compaction in a subprocess ensures that DuckDB's jemalloc memory
// is fully released when the subprocess exits, preventing memory retention.
func (m *Manager) CompactPartition(ctx context.Context, candidate Candidate) error {
	lockKey := filepath.Join(candidate.Database, candidate.PartitionPath)

	// Try to acquire lock
	if !m.LockManager.AcquireLock(lockKey) {
		m.logger.Info().Str("partition", lockKey).Msg("Partition already locked, skipping")
		return nil
	}
	defer m.LockManager.ReleaseLock(lockKey)

	// Build subprocess config. JobID is generated here (not inside NewJob)
	// so the parent and subprocess agree on the completion-manifest filename.
	//
	// BatchNumber is included because sibling batches of one partition differ
	// in nothing else: same database, same partition path, and a wall-clock
	// nanosecond that is not guaranteed distinct (macOS returns a coarse
	// clock). A JobID collision would make two batches share a
	// completion-manifest filename (completion.go writes {JobID}.json), so one
	// batch's manifest would overwrite the other's and its output would never
	// be registered in the Raft manifest. Batches run sequentially today, which
	// makes this remote — BatchNumber makes it impossible.
	//
	// This assumes the candidate came from SplitCandidateIntoBatches, which
	// always sets a 1-based BatchNumber (1 of 1 for an unsplit partition).
	// Today that holds: the only caller is compactFilesAdaptively, which is
	// only ever reached from runCycleInternal via the split. A candidate
	// constructed directly would carry BatchNumber 0 and yield a "_b0" suffix
	// — still unique against real batches, but a signal the invariant was
	// bypassed.
	jobID := fmt.Sprintf("%s_%s_%d_b%d",
		sanitizeDBForName(candidate.Database),
		strings.ReplaceAll(candidate.PartitionPath, "/", "_"),
		time.Now().UnixNano(),
		candidate.BatchNumber,
	)
	config := &SubprocessJobConfig{
		Database:      candidate.Database,
		Measurement:   candidate.Measurement,
		PartitionPath: candidate.PartitionPath,
		Files:         candidate.Files,
		Tier:          candidate.Tier,
		BatchNumber:   candidate.BatchNumber,
		TempDirectory: m.TempDirectory,
		MemoryLimit:   m.MemoryLimit,
		Threads:       m.Threads,
		SortKeys:      m.GetSortKeys(candidate.Measurement),
		StorageType:   m.StorageBackend.Type(),
		StorageConfig: m.StorageBackend.ConfigJSON(),
		// Phase 4: cluster-mode fields. CompletionDir is empty in OSS
		// (Manager.CompletionDir is never set), so the subprocess's
		// clusterMode() returns false and no completion manifest is
		// written — behavior is byte-identical to pre-Phase-4.
		JobID:         jobID,
		CompletionDir: m.CompletionDir,
		PartitionTime: candidate.PartitionTime,
	}
	// When the hub observes consumed inputs (#619), the subprocess leaves
	// the crash-recovery manifest in place and THIS process deletes it after
	// receipt marking commits — the manifest-before-storage discipline
	// applied to bookkeeping: a crash in the window re-fires the marks via
	// recovery instead of silently losing them.
	m.mu.Lock()
	config.ParentFinalizesManifest = m.onConsumedInputs != nil
	m.mu.Unlock()

	// Build extra environment variables for subprocess (storage credentials)
	var extraEnv []string
	if azureBackend, ok := m.StorageBackend.(*storage.AzureBlobBackend); ok {
		if key := azureBackend.GetAccountKey(); key != "" {
			extraEnv = append(extraEnv, "AZURE_STORAGE_KEY="+key)
		}
	}
	if s3Backend, ok := m.StorageBackend.(*storage.S3Backend); ok {
		if accessKey := s3Backend.GetAccessKey(); accessKey != "" {
			extraEnv = append(extraEnv, "AWS_ACCESS_KEY_ID="+accessKey)
		}
		if secretKey := s3Backend.GetSecretKey(); secretKey != "" {
			extraEnv = append(extraEnv, "AWS_SECRET_ACCESS_KEY="+secretKey)
		}
	}

	// Run compaction in subprocess for memory isolation
	result, err := RunJobInSubprocess(ctx, config, m.logger, extraEnv...)

	// Always clean up temp directories for this partition after subprocess completes.
	// The subprocess has its own defer cleanup, but if it crashes or gets OOM-killed,
	// the defer never runs. This ensures cleanup happens from the parent process.
	// The parent already generated config.JobID, so remove only that exact
	// job-owned directory and never infer identity from partition names.
	if removeErr := cleanupSubprocessTempDir(config.TempDirectory, config.JobID); removeErr != nil {
		m.logger.Debug().Err(removeErr).
			Str("dir", jobTempDir(config.TempDirectory, config.JobID)).
			Msg("Failed to cleanup subprocess temp directory")
	} else {
		m.logger.Debug().
			Str("dir", jobTempDir(config.TempDirectory, config.JobID)).
			Msg("Cleaned up subprocess temp directory")
	}

	// Update metrics. Cache invalidation is gated on FilesCompacted > 0, not
	// just success: a zero-file completion (all files already compacted, or a
	// no-time-column skip) changed nothing on storage, and dropping the parquet
	// metadata + query caches for it would cost every in-flight query a cold
	// re-read — every cycle, for a partition that skips every cycle.
	jobSucceeded := err == nil && result != nil && result.Success
	jobInterrupted := ctx.Err() != nil && errors.Is(err, ctx.Err())
	shouldInvalidateCache := jobSucceeded && result.FilesCompacted > 0

	m.mu.Lock()
	if jobSucceeded {
		m.totalJobsCompleted++
		m.totalFilesCompacted += result.FilesCompacted
		m.totalBytesSaved += (result.BytesBefore - result.BytesAfter)
	} else if jobInterrupted {
		m.totalJobsInterrupted++
	} else {
		m.totalJobsFailed++
	}

	// Build job stats for history
	jobStats := map[string]interface{}{
		"database":       candidate.Database,
		"measurement":    candidate.Measurement,
		"partition_path": candidate.PartitionPath,
		"tier":           candidate.Tier,
	}

	if result != nil {
		jobStats["files_compacted"] = result.FilesCompacted
		jobStats["bytes_before"] = result.BytesBefore
		jobStats["bytes_after"] = result.BytesAfter
		jobStats["success"] = result.Success
		if result.BytesBefore > 0 {
			jobStats["compression_ratio"] = 1 - float64(result.BytesAfter)/float64(result.BytesBefore)
		}
		if result.Error != "" {
			jobStats["error"] = result.Error
		}
	}
	if err != nil {
		jobStats["error"] = err.Error()
		jobStats["success"] = false
	}

	// Add to history while holding m.mu so Stats sees a consistent history.
	m.jobHistory = appendJobHistory(m.jobHistory, jobStats)
	// Copy the callback while still holding the lock — main.go wires it via
	// SetOnCompactionComplete after the schedulers have started (#351).
	onComplete := m.onCompactionComplete
	m.mu.Unlock()

	// Invalidate caches outside the lock — the callback performs IO (DuckDB Exec)
	// and should not block stat reads or other concurrent compaction goroutines.
	// The subprocess deleted old parquet files from storage, but DuckDB's
	// cache_httpfs and parquet_metadata_cache still reference them.
	// Edge sync compacted-output observer (issue #610): record the output
	// in the sync ledger before anything can discover it as a new file.
	if jobSucceeded && result != nil {
		m.notifyCompactedOutput(result.OutputFile)
	}

	// Consumed-inputs observer (#619): mark edge-sync receipts for the
	// sources this job deleted, THEN finalize the retained crash-recovery
	// manifest — and ONLY if every mark committed. The surviving manifest is
	// what makes the marks durable: recovery re-fires them idempotently, so
	// on a mark failure (or a failed delete) the manifest must stay put
	// (deep-review B1).
	if jobSucceeded && result != nil {
		markErr := m.notifyConsumedInputs(result.CompactedInputs)
		if result.ManifestPath != "" && m.ManifestManager != nil {
			if markErr != nil {
				m.logger.Warn().Err(markErr).Str("manifest", result.ManifestPath).
					Msg("Receipt marking incomplete; keeping the compaction manifest so recovery re-fires the marks")
			} else if err := m.ManifestManager.DeleteManifest(ctx, result.ManifestPath); err != nil {
				m.logger.Warn().Err(err).Str("manifest", result.ManifestPath).
					Msg("Could not finalize the compaction manifest; recovery re-fires the marks and retries the delete")
			}
		}
	}

	if shouldInvalidateCache && onComplete != nil {
		onComplete()
	}

	// Return error if subprocess failed
	if err != nil {
		return err
	}
	if result != nil && !result.Success {
		return errors.New(result.Error)
	}
	return nil
}

// compactFilesAdaptively attempts compaction with adaptive batch sizing.
// If compaction fails with a recoverable error (segfault, OOM), it splits the batch
// in half and retries each half recursively. This allows large compactions to succeed
// by automatically finding a batch size that fits in memory.
//
// Parameters:
//   - ctx: context for cancellation
//   - candidate: the original candidate (used for metadata)
//   - files: the current subset of files to compact
//   - depth: recursion depth (0 = original batch, 1+ = split retries)
//   - stderr: stderr output from last failed attempt (for error classification)
//
// The algorithm:
//  1. Try to compact all files in the batch
//  2. If it fails with a recoverable error and batch size > minBatchSize:
//     - Split files in half
//     - Recursively compact each half
//  3. If batch size <= minBatchSize and still failing, give up
func (m *Manager) compactFilesAdaptively(ctx context.Context, candidate Candidate, files []string, depth int, stderr string) error {
	// Maximum split depth. Each level halves the batch, so the ladder starts at
	// whatever compaction.max_files_per_batch produced and bottoms out at
	// MinFilesPerBatch — e.g. at the default 30: 30 → 15 → 7 → 3 → stop.
	// A larger configured batch size does not get proportionally more retries,
	// which is part of why MaxAllowedFilesPerBatch caps it.
	const maxDepth = 4
	// Don't split below MinFilesPerBatch files. This is the same floor
	// SplitCandidateIntoBatches clamps to, so a configured batch size can never
	// land below it and fail here on the first attempt.
	const minBatchSize = MinFilesPerBatch

	// Check context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Safety check: too deep or too few files
	if depth > maxDepth {
		return fmt.Errorf("compaction failed: exceeded max retry depth %d", maxDepth)
	}
	if len(files) < minBatchSize {
		return fmt.Errorf("compaction failed: batch size %d below minimum %d", len(files), minBatchSize)
	}

	// Create candidate for this batch
	batchCandidate := candidate
	batchCandidate.Files = files

	// Log retry attempts
	if depth > 0 {
		m.logger.Info().
			Int("depth", depth).
			Int("file_count", len(files)).
			Str("partition", candidate.PartitionPath).
			Msg("Retrying compaction with reduced batch size")
	}

	// Attempt compaction
	err := m.CompactPartition(ctx, batchCandidate)
	if err == nil {
		// Success!
		if depth > 0 {
			m.logger.Info().
				Int("depth", depth).
				Int("file_count", len(files)).
				Str("partition", candidate.PartitionPath).
				Msg("Compaction succeeded after batch size reduction")
		}
		return nil
	}

	// Cancellation is not a recoverable resource failure. Preserve the
	// cause without logging a failed batch or allocating split retries.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	// Extract stderr from error message for classification
	// Error format from RunJobInSubprocess: "subprocess failed: %w (stderr: %s)"
	errStderr := stderr
	if errStr := err.Error(); strings.Contains(errStr, "(stderr:") {
		parts := strings.SplitN(errStr, "(stderr:", 2)
		if len(parts) > 1 {
			errStderr = strings.TrimSuffix(strings.TrimSpace(parts[1]), ")")
		}
	}

	// Classify the error
	recoverable, reason := ClassifySubprocessError(err, errStderr)

	if !recoverable {
		m.logger.Error().
			Err(err).
			Str("reason", reason).
			Str("partition", candidate.PartitionPath).
			Msg("Compaction failed with non-recoverable error")
		return err
	}

	// Check if we can split further
	if len(files) <= minBatchSize {
		m.logger.Error().
			Err(err).
			Str("reason", reason).
			Int("batch_size", len(files)).
			Str("partition", candidate.PartitionPath).
			Msg("Compaction failed at minimum batch size, cannot split further")
		return err
	}

	// Split batch in half and try each half (copy to independent backing arrays)
	mid := len(files) / 2
	firstHalf := make([]string, mid)
	copy(firstHalf, files[:mid])
	secondHalf := make([]string, len(files)-mid)
	copy(secondHalf, files[mid:])

	m.logger.Warn().
		Int("depth", depth).
		Int("original_size", len(files)).
		Int("first_half", len(firstHalf)).
		Int("second_half", len(secondHalf)).
		Str("reason", reason).
		Str("partition", candidate.PartitionPath).
		Msg("Splitting batch after recoverable failure")

	// Try first half
	if err := m.compactFilesAdaptively(ctx, candidate, firstHalf, depth+1, errStderr); err != nil {
		return fmt.Errorf("first half failed: %w", err)
	}

	// Try second half
	if err := m.compactFilesAdaptively(ctx, candidate, secondHalf, depth+1, errStderr); err != nil {
		return fmt.Errorf("second half failed: %w", err)
	}

	return nil
}

// CycleClaim is the exclusive right to run one compaction cycle, with the id
// that cycle will report. Taking the claim and learning its id is one step, so
// a caller can answer an operator with the real id before the cycle body
// starts (#1153) -- the trigger endpoint used to answer with
// GetCurrentCycleID()+1, a guess made in the handler, while the id was
// actually assigned later inside the cycle goroutine.
//
// The holder MUST call Release, and must arrange for it before any path that
// can leave the claim behind -- a return OR a recovered panic. An unreleased
// claim leaves cycleRunning true forever, which stops compaction on the node
// until it is restarted. Further Release calls are no-ops.
type CycleClaim struct {
	// ID is the cycle id this claim will run under. Already published, so
	// RunningCycleID returns it the moment ClaimCycle returns.
	ID int64

	m        *Manager
	released atomic.Bool
}

// Release gives up the claim. Idempotent: a second call is a no-op rather
// than clearing cycleRunning under a cycle that claimed it later, which would
// be a worse bug than the one CycleClaim exists to fix.
func (c *CycleClaim) Release() {
	if c == nil || !c.released.CompareAndSwap(false, true) {
		return
	}
	c.m.cycleRunning.Store(false)
}

// ClaimCycle takes the exclusive right to run one cycle and assigns its id.
// Returns ErrCycleAlreadyRunning when a cycle already holds the claim.
//
// Deliberately silent: the caller decides whether a refusal is worth a log
// line. runCycleInternalFiltered keeps the scheduler's Warn, and the API
// handler logs its own refusal, so moving a log in here would double it.
func (m *Manager) ClaimCycle() (*CycleClaim, error) {
	m.claimMu.Lock()
	defer m.claimMu.Unlock()

	if !m.cycleRunning.CompareAndSwap(false, true) {
		return nil, ErrCycleAlreadyRunning
	}
	return &CycleClaim{ID: m.cycleID.Add(1), m: m}, nil
}

// RunningCycleID is the id of the cycle holding the claim, or of the most
// recent one when none is held. Takes claimMu, so it never observes the
// instant between a claim being taken and its id being assigned -- that is
// the whole point of the mutex, and the reason callers answering an operator
// should prefer this over GetCurrentCycleID.
func (m *Manager) RunningCycleID() int64 {
	m.claimMu.Lock()
	defer m.claimMu.Unlock()
	return m.cycleID.Load()
}

// errInvalidClaim rejects a claim that cannot safely run a cycle. Running on a
// released claim is the dangerous case: cycleRunning is false, so another
// cycle can claim and run concurrently -- exactly the exclusivity CycleClaim
// exists to provide. A claim from another Manager would run this manager's
// cycle under that one's exclusion.
var errInvalidClaim = errors.New("compaction: nil, already-released, or foreign cycle claim")

func (m *Manager) checkClaim(claim *CycleClaim) error {
	if claim == nil || claim.m != m || claim.released.Load() {
		return errInvalidClaim
	}
	return nil
}

// RunClaimedCycleForTiers runs a cycle the caller already claimed, for the
// given tiers across all databases. The caller owns the claim and releases
// it -- including when this returns an error; this never releases it.
func (m *Manager) RunClaimedCycleForTiers(ctx context.Context, claim *CycleClaim, tierNames []string) error {
	if err := m.checkClaim(claim); err != nil {
		return err
	}
	return m.runClaimed(ctx, claim.ID, cycleSourceAPI, nil, tierNames, "")
}

// RunClaimedCycleForDatabase is RunClaimedCycleForTiers scoped to one database.
func (m *Manager) RunClaimedCycleForDatabase(ctx context.Context, claim *CycleClaim, database string, tierNames []string) error {
	if err := m.checkClaim(claim); err != nil {
		return err
	}
	return m.runClaimed(ctx, claim.ID, cycleSourceAPI, []string{database}, tierNames, "")
}

// RunClaimedCycleForMeasurement is RunClaimedCycleForTiers scoped to one
// measurement of one database. Validates before touching the claim, matching
// RunCompactionCycleForMeasurement, which validates before claiming.
func (m *Manager) RunClaimedCycleForMeasurement(ctx context.Context, claim *CycleClaim, database, measurement string, tierNames []string) error {
	if database == "" || measurement == "" {
		return fmt.Errorf("database and measurement are required")
	}
	if err := m.checkClaim(claim); err != nil {
		return err
	}
	return m.runClaimed(ctx, claim.ID, cycleSourceAPI, []string{database}, tierNames, measurement)
}

// RunCompactionCycle runs one compaction cycle for all enabled tiers.
// Returns the cycle ID and an error if the cycle couldn't be started.
// Returns ErrCycleAlreadyRunning if a cycle is already in progress.
func (m *Manager) RunCompactionCycle(ctx context.Context) (int64, error) {
	// Collect all enabled tier names
	var tierNames []string
	for _, tier := range m.Tiers {
		if tier.IsEnabled() {
			tierNames = append(tierNames, tier.GetTierName())
		}
	}
	return m.RunCompactionCycleForTiers(ctx, tierNames)
}

// RunCompactionCycleForTiers runs a complete compaction cycle for specific tiers across all databases.
// tierNames must be non-empty - specify which tiers to run explicitly.
//
// Cycles started through this family are recorded with source "unspecified"
// (#1162). The scheduler passes cycleSourceScheduler through runCycleInternal
// directly, so labelling this family "scheduler" would mislabel whichever
// caller is added next -- today it has none in production.
func (m *Manager) RunCompactionCycleForTiers(ctx context.Context, tierNames []string) (int64, error) {
	return m.runCycleInternal(ctx, cycleSourceUnspecified, nil, tierNames)
}

// RunCompactionCycleForDatabase runs a compaction cycle for a single database.
// tierNames must be non-empty - specify which tiers to run explicitly.
func (m *Manager) RunCompactionCycleForDatabase(ctx context.Context, database string, tierNames []string) (int64, error) {
	return m.runCycleInternal(ctx, cycleSourceUnspecified, []string{database}, tierNames)
}

// RunCompactionCycleForMeasurement restricts discovery to one measurement.
func (m *Manager) RunCompactionCycleForMeasurement(ctx context.Context, database, measurement string, tierNames []string) (int64, error) {
	if database == "" || measurement == "" {
		return 0, fmt.Errorf("database and measurement are required")
	}
	return m.runCycleInternalFiltered(ctx, cycleSourceUnspecified, []string{database}, tierNames, measurement)
}

// runCycleInternal is the shared implementation for compaction cycles.
// If filterDatabases is non-nil, only those databases are compacted; otherwise all databases are discovered.
func (m *Manager) runCycleInternal(ctx context.Context, source string, filterDatabases []string, tierNames []string) (int64, error) {
	return m.runCycleInternalFiltered(ctx, source, filterDatabases, tierNames, "")
}

// runCycleInternalFiltered claims a cycle and runs it to completion. Every
// caller that wants the whole operation in one call -- both schedulers and the
// three RunCompactionCycleFor* entry points -- goes through here, so their
// behaviour is unchanged by the claim split: same loss Warn, same (0, error)
// on a lost claim, same release-after-the-finalizer ordering.
func (m *Manager) runCycleInternalFiltered(ctx context.Context, source string, filterDatabases []string, tierNames []string, filterMeasurement string) (int64, error) {
	claim, err := m.ClaimCycle()
	if err != nil {
		m.logger.Warn().Msg("Compaction cycle already running, skipping")
		return 0, err
	}
	// Registered before runClaimed is called, and runClaimed's own finalizer
	// (which waits for active workers) completes before it returns -- so the
	// release still happens strictly after the last worker, exactly as the
	// previous defer ordering guaranteed. No worker can outlive the claim.
	defer claim.Release()

	return claim.ID, m.runClaimed(ctx, claim.ID, source, filterDatabases, tierNames, filterMeasurement)
}

// runClaimed is the cycle body. The caller holds the claim and owns its
// release; runErr is named because the finalizer mutates it.
func (m *Manager) runClaimed(ctx context.Context, cycleID int64, source string, filterDatabases []string, tierNames []string, filterMeasurement string) (runErr error) {
	var active sync.WaitGroup
	progress := &cycleProgress{}
	discovered := &progress.discovered
	started := &progress.started
	succeeded := &progress.succeeded
	failed := &progress.failed
	interrupted := &progress.interrupted
	discoveryErrors := &progress.discoveryErrors
	// pausedSkipped is set by a partition goroutine that stopped between two
	// of its batches because compaction was paused (#1087). The goroutine
	// cannot set runErr, and the dispatch loop may already be waiting at the
	// end of the tier, so it is checked there.
	var pausedSkipped atomic.Bool

	// The cycle is recorded as "running" before it starts, carrying its scope
	// and a pointer to the live counters (#1162). Recording only at completion
	// would make the endpoint 404 in exactly the window it exists for: the
	// operator triggers, is handed a cycle id, and asks about it immediately.
	// Nothing can return between here and the finalizer below -- only variable
	// declarations sit in between -- so the finalizer always finds this entry.
	startRecord := CycleRecord{
		CycleID:     cycleID,
		Status:      "running",
		Source:      source,
		Databases:   filterDatabases,
		Measurement: filterMeasurement,
		Tiers:       tierNames,
		StartedAt:   time.Now().UTC(),
		progress:    progress,
	}
	m.mu.Lock()
	m.cycleHistory = appendCycleHistory(m.cycleHistory, startRecord.clone())
	m.mu.Unlock()

	// This finalizer also covers every early return. Active workers finish
	// before the cycle is recorded and before cycleRunning is released.
	defer func() {
		active.Wait()

		if cause := ctx.Err(); cause != nil && !errors.Is(runErr, cause) {
			if runErr == nil {
				runErr = cause
			} else {
				runErr = errors.Join(runErr, cause)
			}
		}

		if runErr == nil && (failed.Load() > 0 || discoveryErrors.Load() > 0) {
			runErr = fmt.Errorf(
				"compaction cycle: %d batch failures, %d discovery failures",
				failed.Load(), discoveryErrors.Load(),
			)
		}

		status := "completed"
		switch {
		case errors.Is(ctx.Err(), context.DeadlineExceeded):
			status = "timed_out"
		case errors.Is(ctx.Err(), context.Canceled):
			status = "cancelled"
		case errors.Is(runErr, ErrCompactionPaused):
			status = "paused"
		case runErr != nil:
			status = "failed"
		}

		outcome := CycleRecord{
			CycleID:         cycleID,
			Status:          status,
			Source:          source,
			Databases:       filterDatabases,
			Measurement:     filterMeasurement,
			Tiers:           tierNames,
			StartedAt:       startRecord.StartedAt,
			FinishedAt:      time.Now().UTC(),
			Discovered:      discovered.Load(),
			Started:         started.Load(),
			Succeeded:       succeeded.Load(),
			Failed:          failed.Load(),
			Interrupted:     interrupted.Load(),
			DiscoveryErrors: discoveryErrors.Load(),
			FailedSample:    progress.snapshotFailedSample(),
		}
		outcome.Unstarted = outcome.Discovered - outcome.Started
		if runErr != nil {
			outcome.Err = truncateCycleError(runErr.Error())
		}

		m.mu.Lock()
		m.lastCycle = outcome
		// Replace the "running" entry this cycle appended at its start. It is
		// the newest entry, because ClaimCycle's CAS admits one cycle at a time
		// and the claim is released only after this finalizer returns. The
		// append is defensive: it must not be reachable, and a test asserts the
		// history grows by exactly one entry per cycle.
		if n := len(m.cycleHistory); n > 0 && m.cycleHistory[n-1].CycleID == cycleID {
			m.cycleHistory[n-1] = outcome.clone()
		} else {
			m.cycleHistory = appendCycleHistory(m.cycleHistory, outcome.clone())
			m.logger.Warn().
				Int64("cycle_id", cycleID).
				Msg("Compaction cycle history lost its running entry; appended the outcome instead")
		}
		m.mu.Unlock()

		event := m.logger.Info()
		if runErr != nil {
			event = m.logger.Warn()
		}
		event.Err(runErr).
			Int64("cycle_id", cycleID).
			Str("status", status).
			Int64("discovered_batches", outcome.Discovered).
			Int64("started_batches", outcome.Started).
			Int64("succeeded_batches", outcome.Succeeded).
			Int64("failed_batches", outcome.Failed).
			Int64("interrupted_batches", outcome.Interrupted).
			Int64("unstarted_batches", outcome.Unstarted).
			Int64("discovery_errors", outcome.DiscoveryErrors).
			Msg("Compaction cycle finished")
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	// The cluster-wide compaction pause (#1087): a cycle that starts while a
	// restore holds it would stop at its first batch boundary anyway, so it
	// does not start. Recorded as a "paused" cycle so Stats shows why.
	if m.Paused() {
		m.logger.Info().Int64("cycle_id", cycleID).Msg("Compaction cycle not started: compaction is paused cluster-wide")
		return ErrCompactionPaused
	}

	// Require explicit tier names
	if len(tierNames) == 0 {
		m.logger.Debug().Int64("cycle_id", cycleID).Msg("No tiers specified, skipping cycle")
		return nil
	}

	logEvent := m.logger.Info().
		Int64("cycle_id", cycleID).
		Strs("tiers", tierNames)
	if filterDatabases != nil {
		logEvent = logEvent.Strs("databases", filterDatabases)
	}
	logEvent.Msg("Starting compaction cycle")

	// Build tier filter map for quick lookup
	tierFilter := make(map[string]bool)
	for _, name := range tierNames {
		tierFilter[name] = true
	}

	// Determine databases to compact. Namespace expansion applies to BOTH
	// arms: a scheduled cycle expands what listDatabases found, and an
	// operator triggering ?database=<spoke-id> gets that spoke's children
	// expanded the same way (#619 review F4).
	var databases []string
	if filterDatabases != nil {
		databases = filterDatabases
	} else {
		// Pre-discover databases ONCE before processing tiers
		// This avoids redundant storage API calls when multiple tiers are enabled
		var err error
		databases, err = m.listDatabases(ctx)
		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to list databases for compaction cycle")
			return err
		}
		// The exclusion list applies to unscoped discovery only — an
		// explicit filterDatabases scope is operator intent and bypasses
		// it. Filtering here, before expansion, drops excluded real
		// databases and whole spoke namespaces without listing their
		// children; the post-expansion pass below catches individual
		// pseudo-databases ("spoke1/telemetry").
		databases = m.filterExcludedDatabases(databases)
	}

	databases = m.expandNamespaces(ctx, databases)
	if filterDatabases == nil {
		databases = m.filterExcludedDatabases(databases)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	recoveryDatabases := databases
	if filterDatabases == nil {
		recoveryDatabases = nil
	}
	// Run manifest recovery before starting new compactions. This ensures
	// interrupted compactions from previous cycles are completed. Recovery
	// honors the manual database/measurement scope but spans every tier: see
	// recoveryScope for why a tier-scoped recovery is wrong for the
	// single-tier schedulers.
	if m.ManifestManager != nil {
		recovered, err := m.recoverOrphanedManifests(ctx, recoveryScope{Databases: recoveryDatabases, Measurement: filterMeasurement})
		if recovered > 0 {
			m.mu.Lock()
			m.totalManifestsRecov += recovered
			m.mu.Unlock()
			m.logger.Info().Int("recovered", recovered).Msg("Recovered orphaned compaction manifests")
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			discoveryErrors.Add(1)
			m.logger.Warn().Err(err).Msg("Manifest recovery encountered errors")
		}
	}

	// Build database -> measurements map to avoid repeated lookups
	dbMeasurements := make(map[string][]string)
	for _, database := range databases {
		if err := ctx.Err(); err != nil {
			return err
		}
		if filterMeasurement != "" {
			dbMeasurements[database] = []string{filterMeasurement}
			continue
		}
		measurements, err := m.listMeasurements(ctx, database)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			discoveryErrors.Add(1)
			m.logger.Error().Err(err).
				Str("database", database).
				Msg("Failed to list measurements")
			continue
		}
		dbMeasurements[database] = measurements
	}

	// Process tiers sequentially to maintain hierarchy (hourly -> daily)
	// This ensures lower tiers complete before higher tiers run
	// Listings are deliberately NOT shared across tiers here, unlike in
	// FindCandidates: hourly jobs delete their inputs and write their
	// `_compacted` outputs, and the cycle waits for the whole tier before
	// daily starts, so a listing taken for hourly is stale by the time daily
	// would read it. Each tier lists the measurement itself (#316).
	for _, tier := range m.Tiers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !tier.IsEnabled() {
			continue
		}

		tierName := tier.GetTierName()

		// Skip tier if not in filter
		if !tierFilter[tierName] {
			m.logger.Debug().
				Int64("cycle_id", cycleID).
				Str("tier", tierName).
				Msg("Skipping tier (not in filter)")
			continue
		}
		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Msg("Processing tier")

		// MEMORY OPTIMIZATION: Process candidates as they're found instead of accumulating all.
		// This prevents unbounded memory growth when there are millions of files.
		// Each measurement's candidates are processed and then eligible for GC.
		sem := make(chan struct{}, m.MaxConcurrent)
		var wg sync.WaitGroup
		var tierCandidateCount int
		tierStartStarted := started.Load()
		tierStartSucceeded := succeeded.Load()
		tierStartFailed := failed.Load()
		tierStartInterrupted := interrupted.Load()

		for _, database := range databases {
			measurements := dbMeasurements[database]

			for _, meas := range measurements {
				// Check for cancellation between measurements
				select {
				case <-ctx.Done():
					m.logger.Info().
						Int64("cycle_id", cycleID).
						Str("tier", tierName).
						Msg("Tier processing cancelled")
					wg.Wait()
					return ctx.Err()
				default:
				}
				// The cluster-wide compaction pause (#1087) also ends
				// discovery: the node acks the pause only once this cycle
				// has ended, and listing every remaining measurement of a
				// large store first would hold that ack for minutes.
				if m.Paused() {
					m.logger.Info().
						Int64("cycle_id", cycleID).
						Str("tier", tierName).
						Str("database", database).
						Msg("Compaction cycle stopping between measurements: compaction is paused cluster-wide")
					wg.Wait()
					return ErrCompactionPaused
				}

				candidates, err := tier.FindCandidates(ctx, database, meas)
				if err != nil {
					if ctx.Err() != nil {
						wg.Wait()
						return ctx.Err()
					}
					discoveryErrors.Add(1)
					m.logger.Error().Err(err).
						Str("database", database).
						Str("measurement", meas).
						Str("tier", tierName).
						Msg("Failed to find candidates")
					continue
				}

				// Process this measurement's candidates immediately
				for _, candidate := range candidates {
					if err := ctx.Err(); err != nil {
						wg.Wait()
						return err
					}
					// A slash-carrying database is by construction a
					// pseudo-database from the namespace expander — received
					// data the delivery gate must not defer (#619 F2).
					if strings.ContainsRune(candidate.Database, '/') {
						candidate.SyncExempt = true
					}

					// Filter out files that are tracked by manifests (pending compaction)
					filteredCandidate, shouldProcess, manifestErr := m.filterCandidateFilesWithError(ctx, candidate)
					if err := ctx.Err(); err != nil {
						wg.Wait()
						return err
					}
					if manifestErr != nil {
						// The candidate stays excluded. Record genuine manifest
						// failures without counting expired-context errors.
						discoveryErrors.Add(1)
					}
					if !shouldProcess {
						m.logger.Debug().
							Str("partition", candidate.PartitionPath).
							Msg("Skipping candidate: all files are tracked by manifests")
						continue
					}

					// Edge sync delivery gate (issue #610): drop files the
					// sync ledger has not confirmed delivered and re-check
					// the tier's MinFiles on what remains. Deferring here is
					// the design, not a failure — the partition compacts
					// once its files have synced.
					filteredCandidate, shouldProcess, eligibilityErr := m.filterSyncEligibilityWithError(ctx, filteredCandidate, tier)
					if eligibilityErr != nil && ctx.Err() == nil {
						discoveryErrors.Add(1)
					}
					if err := ctx.Err(); err != nil {
						wg.Wait()
						return err
					}
					if !shouldProcess {
						continue
					}

					// Split large candidates into batches to prevent DuckDB segfaults
					// when processing too many files in a single read_parquet() call
					batches := SplitCandidateIntoBatches(filteredCandidate, m.MaxFilesPerBatch)
					if len(batches) > 1 {
						m.logger.Info().
							Str("partition", filteredCandidate.PartitionPath).
							Int("total_files", len(filteredCandidate.Files)).
							Int("batches", len(batches)).
							Msg("Splitting large candidate into batches")
					}

					tierCandidateCount += len(batches)
					discovered.Add(int64(len(batches)))

					// Capacity acquisition must not outlive the cycle budget.
					select {
					case sem <- struct{}{}:
					case <-ctx.Done():
						wg.Wait()
						return ctx.Err()
					}
					// If both select arms were ready, cancellation still wins
					// before a new worker can be launched.
					if err := ctx.Err(); err != nil {
						<-sem
						wg.Wait()
						return err
					}
					// The cluster-wide compaction pause (#1087): no new worker
					// while a restore holds it. Checked here, with capacity in
					// hand, because the pause lands while this waits behind a
					// running batch. Workers already running finish their
					// current batch (a subprocess is never killed for the
					// pause: a kill between its two commit phases leaves the
					// manifest and storage disagreeing), so the cycle waits for
					// them and ends "paused"; the batch just discovered counts
					// as unstarted. Sub-batch splitting inside
					// compactFilesAdaptively does not see the gate: those are
					// one job's inputs and splitting them is part of running it.
					if m.Paused() {
						<-sem
						m.logger.Info().
							Int64("cycle_id", cycleID).
							Str("tier", tierName).
							Str("partition", candidate.PartitionPath).
							Msg("Compaction cycle stopping at a batch boundary: compaction is paused cluster-wide")
						wg.Wait()
						return ErrCompactionPaused
					}
					wg.Add(1)
					active.Add(1)

					// Run all batches for the same partition sequentially within a single goroutine.
					// This prevents race conditions where batch N tries to compact files that were
					// already deleted by batch N-1. Different partitions can still run in parallel.
					go func(partitionBatches []Candidate, partition string) {
						defer wg.Done()
						defer active.Done()
						defer func() { <-sem }() // Release semaphore

						for _, batch := range partitionBatches {
							if ctx.Err() != nil {
								return
							}
							// Between two batches of one partition the pause
							// (#1087) applies too; the remaining batches stay
							// unstarted and the tier end turns this into
							// ErrCompactionPaused.
							if m.Paused() {
								pausedSkipped.Store(true)
								return
							}
							// Count a batch only when its execution actually starts.
							started.Add(1)

							var err error
							if m.compactBatchForTest != nil {
								err = m.compactBatchForTest(ctx, batch)
							} else {
								err = m.compactFilesAdaptively(ctx, batch, batch.Files, 0, "")
							}

							if err != nil {
								if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
									interrupted.Add(1)
									return
								}
								failed.Add(1)
								progress.recordFailure(batch.PartitionPath)
								m.logger.Error().Err(err).
									Str("partition", batch.PartitionPath).
									Str("tier", tierName).
									Int("batch", batch.BatchNumber).
									Int("total_batches", batch.TotalBatches).
									Int64("cycle_id", cycleID).
									Msg("Compaction failed")
								// Continue after genuine failures, preserving existing
								// per-partition batch behavior.
							} else {
								succeeded.Add(1)
							}
						}
					}(batches, candidate.PartitionPath)
				}
				// candidates slice is now eligible for GC after this iteration
			}
		}

		// Wait for all jobs in this tier to complete before moving to next tier
		wg.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}
		if pausedSkipped.Load() {
			m.logger.Info().
				Int64("cycle_id", cycleID).
				Str("tier", tierName).
				Msg("Compaction cycle ended at a batch boundary: compaction is paused cluster-wide")
			return ErrCompactionPaused
		}

		if tierCandidateCount == 0 {
			m.logger.Info().
				Int64("cycle_id", cycleID).
				Str("tier", tierName).
				Msg("No candidates found for tier")
			continue
		}

		tierStarted := started.Load() - tierStartStarted

		m.logger.Info().
			Int64("cycle_id", cycleID).
			Str("tier", tierName).
			Int("discovered", tierCandidateCount).
			Int64("started", tierStarted).
			Int64("succeeded", succeeded.Load()-tierStartSucceeded).
			Int64("failed", failed.Load()-tierStartFailed).
			Int64("interrupted", interrupted.Load()-tierStartInterrupted).
			Int64("unstarted", int64(tierCandidateCount)-tierStarted).
			Msg("Tier processing complete")
	}

	// The finalizer records the terminal outcome and complete counters.
	return nil
}

// IsCycleRunning returns true if a compaction cycle is currently in progress
func (m *Manager) IsCycleRunning() bool {
	return m.cycleRunning.Load()
}

// GetCurrentCycleID returns the current or most recent cycle ID.
//
// Takes claimMu, so it is identical to RunningCycleID -- kept as the older
// name because callers outside this package may use it, and deliberately NOT
// left as a bare atomic load: a caller that picked this one over
// RunningCycleID would silently reintroduce the window the mutex exists to
// close (#1153).
func (m *Manager) GetCurrentCycleID() int64 {
	return m.RunningCycleID()
}

// listDatabases discovers all databases in storage
func (m *Manager) listDatabases(ctx context.Context) ([]string, error) {
	// MEMORY OPTIMIZATION: Use ListDirectories instead of List to avoid loading all file paths.
	// ListDirectories only reads top-level directory entries, not all files recursively.
	// This reduces memory from O(millions of files) to O(number of databases).
	if dirLister, ok := m.StorageBackend.(storage.DirectoryLister); ok {
		dirs, err := dirLister.ListDirectories(ctx, "")
		if err != nil {
			return nil, err
		}

		// Filter out hidden directories and special directories
		databases := make([]string, 0, len(dirs))
		for _, dir := range dirs {
			// Reserved root directories (_compaction_state, _schema, dot
			// prefixed) are Arc's own state, never databases.
			if dir != "" && !storage.IsReservedRootDir(dir) && dir != "compaction" {
				databases = append(databases, dir)
			}
		}
		return databases, nil
	}

	// Fallback for backends that don't implement DirectoryLister
	objects, err := m.StorageBackend.List(ctx, "")
	if err != nil {
		return nil, err
	}

	// Extract unique database names from paths
	databaseSet := make(map[string]struct{})
	for _, obj := range objects {
		// Path format: database/measurement/year/month/day/hour/file.parquet
		parts := strings.Split(obj, "/")
		if len(parts) >= 2 {
			database := parts[0]
			// Skip hidden directories and special files
			if database != "" && !storage.IsReservedRootDir(database) && database != "compaction" {
				databaseSet[database] = struct{}{}
			}
		}
	}

	// Convert to slice
	databases := make([]string, 0, len(databaseSet))
	for db := range databaseSet {
		databases = append(databases, db)
	}

	return databases, nil
}

// listMeasurements lists all measurements in storage for a given database
func (m *Manager) listMeasurements(ctx context.Context, database string) ([]string, error) {
	// MEMORY OPTIMIZATION: Use ListDirectories instead of List to avoid loading all file paths.
	// ListDirectories only reads directory entries at one level, not all files recursively.
	// This reduces memory from O(files in database) to O(number of measurements).
	if dirLister, ok := m.StorageBackend.(storage.DirectoryLister); ok {
		measurements, err := dirLister.ListDirectories(ctx, database+"/")
		if err != nil {
			return nil, err
		}

		m.logger.Debug().
			Str("database", database).
			Strs("measurements", measurements).
			Msg("Found measurements")

		return measurements, nil
	}

	// Fallback for backends that don't implement DirectoryLister
	prefix := database + "/"
	objects, err := m.StorageBackend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Extract unique measurement names from paths — PREFIX-RELATIVE, like
	// the tier scanners: parts[1] would be wrong for a slash-carrying
	// pseudo-database ("rocket-01/telemetry"), silently no-oping the whole
	// #619 feature on a non-DirectoryLister backend (review M1).
	measurementSet := make(map[string]struct{})
	for _, obj := range objects {
		rel, ok := strings.CutPrefix(obj, prefix)
		if !ok {
			continue
		}
		if i := strings.IndexByte(rel, '/'); i > 0 {
			measurement := rel[:i]
			if measurement != "" && measurement != "." {
				measurementSet[measurement] = struct{}{}
			}
		}
	}

	// Convert to slice
	measurements := make([]string, 0, len(measurementSet))
	for meas := range measurementSet {
		measurements = append(measurements, meas)
	}

	m.logger.Debug().
		Str("database", database).
		Strs("measurements", measurements).
		Msg("Found measurements")

	return measurements, nil
}

// GetSortKeys returns sort keys for a measurement.
// Checks measurement-specific config first, then falls back to default.
// Always ensures "time" is the last sort key, mirroring the ingest path's
// getSortKeys in arrow_writer.go, so compacted files keep the same time
// ordering within each sort group that ingest established.
func (m *Manager) GetSortKeys(measurement string) []string {
	// Check measurement-specific config
	var keys []string
	if measurementKeys, exists := m.SortKeysConfig[measurement]; exists {
		keys = measurementKeys
	} else {
		keys = m.DefaultSortKeys
	}

	// Always ensure "time" is the last sort key.
	// Skip adding if already present (legacy configs may include it explicitly).
	for _, k := range keys {
		if k == "time" {
			return keys
		}
	}

	// Append "time" - users configure ADDITIONAL sort keys only.
	return append(keys, "time")
}

// filterCandidateFiles removes files that are tracked by manifests from a candidate.
// Returns the filtered candidate and whether it should still be processed.
// filterCandidateFiles preserves the existing two-value helper API.
func (m *Manager) filterCandidateFiles(
	ctx context.Context, candidate Candidate,
) (Candidate, bool) {
	filtered, ok, _ := m.filterCandidateFilesWithError(ctx, candidate)
	return filtered, ok
}

func (m *Manager) filterCandidateFilesWithError(ctx context.Context, candidate Candidate) (Candidate, bool, error) {
	if ctx.Err() != nil {
		return candidate, false, ctx.Err()
	}
	if m.ManifestManager == nil {
		return candidate, len(candidate.Files) > 0, nil
	}

	filesInManifests, err := m.ManifestManager.GetFilesInManifests(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return candidate, false, ctx.Err()
		}
		m.logger.Warn().Err(err).Msg("Failed to get files in manifests, skipping partition to avoid re-compaction")
		return candidate, false, err
	}

	if len(filesInManifests) == 0 {
		return candidate, len(candidate.Files) > 0, nil
	}

	// Filter out files that are in manifests
	filteredFiles := make([]string, 0, len(candidate.Files))
	var skipped int
	for _, f := range candidate.Files {
		if _, inManifest := filesInManifests[f]; inManifest {
			skipped++
			continue
		}
		filteredFiles = append(filteredFiles, f)
	}

	if skipped > 0 {
		m.logger.Debug().
			Str("partition", candidate.PartitionPath).
			Int("skipped", skipped).
			Int("remaining", len(filteredFiles)).
			Msg("Filtered files tracked by manifests")
	}

	candidate.Files = filteredFiles
	candidate.FileCount = len(filteredFiles)

	return candidate, len(filteredFiles) > 0, nil
}

// Stats returns compaction statistics
func (m *Manager) Stats() map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	// A copy, coerced so JSON consumers always see an array (the nil slice
	// would marshal as null), matching the trigger-response echo surface.
	excluded := m.ExcludedDatabases()
	if excluded == nil {
		excluded = []string{}
	}

	stats := map[string]interface{}{
		"total_jobs_completed":    m.totalJobsCompleted,
		"total_jobs_failed":       m.totalJobsFailed,
		"total_jobs_interrupted":  m.totalJobsInterrupted,
		"total_files_compacted":   m.totalFilesCompacted,
		"total_bytes_saved":       m.totalBytesSaved,
		"total_bytes_saved_mb":    float64(m.totalBytesSaved) / 1024 / 1024,
		"total_manifests_recover": m.totalManifestsRecov,
		"cycle_running":           m.cycleRunning.Load(),
		"current_cycle_id":        m.cycleID.Load(),
		"exclude_databases":       excluded,
		"paused":                  m.Paused(),
		"last_cycle": map[string]interface{}{
			"cycle_id":            m.lastCycle.CycleID,
			"status":              m.lastCycle.Status,
			"discovered_batches":  m.lastCycle.Discovered,
			"started_batches":     m.lastCycle.Started,
			"succeeded_batches":   m.lastCycle.Succeeded,
			"failed_batches":      m.lastCycle.Failed,
			"interrupted_batches": m.lastCycle.Interrupted,
			"unstarted_batches":   m.lastCycle.Unstarted,
			"discovery_errors":    m.lastCycle.DiscoveryErrors,
		},
	}

	// Add recent jobs (last 10)
	recentJobs := m.jobHistory
	if len(recentJobs) > 10 {
		recentJobs = recentJobs[len(recentJobs)-10:]
	}
	stats["recent_jobs"] = recentJobs

	// Add tier stats
	if len(m.Tiers) > 0 {
		tierStats := make([]map[string]interface{}, len(m.Tiers))
		for i, tier := range m.Tiers {
			tierStats[i] = tier.GetStats()
		}
		stats["tiers"] = tierStats
	}

	return stats
}

// reservedTempSubdirs are subdirectory names under TempDirectory that
// CleanupOrphanedTempDirs must NOT delete. Phase 4 reserves ".completion"
// for the local-disk CompletionManifest handoff: if the pod crashes
// between a successful compaction and the watcher's Raft apply, the
// completion manifest is the ONLY record that ties the compacted output
// to the sources it replaced. Nuking it on restart would leak manifest
// entries in the Raft FSM until operator intervention.
var reservedTempSubdirs = map[string]struct{}{
	".completion": {},
}

// CleanupOrphanedTempDirs removes orphaned temp directories from previous runs.
// This handles cleanup after pod crashes where the defer cleanup didn't run.
// Call this on startup before running compaction cycles.
//
// Phase 4: skips reserved subdirectories (see reservedTempSubdirs). Those are
// managed by their own cleanup paths (e.g. CleanupOrphanedCompletionManifests)
// and must not be treated as stale temp dirs.
func (m *Manager) CleanupOrphanedTempDirs() error {
	entries, err := os.ReadDir(m.TempDirectory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist yet
		}
		return fmt.Errorf("failed to read temp directory: %w", err)
	}

	var cleaned int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, reserved := reservedTempSubdirs[entry.Name()]; reserved {
			continue
		}
		path := filepath.Join(m.TempDirectory, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			m.logger.Warn().Err(err).Str("dir", entry.Name()).Msg("Failed to cleanup orphaned temp directory")
		} else {
			m.logger.Info().Str("dir", entry.Name()).Msg("Cleaned up orphaned temp directory")
			cleaned++
		}
	}

	if cleaned > 0 {
		m.logger.Info().Int("count", cleaned).Msg("Orphaned temp directories cleaned")
	}
	return nil
}

// CleanupOrphanedCompletionManifests sweeps the Phase 4 completion-manifest
// directory for entries in state "writing_output" that are older than
// orphanTimeout. These represent compaction subprocesses that started
// writing a manifest but crashed before advancing to output_written — the
// watcher will never pick them up (they're still in the initial state),
// and leaving them on disk would cause confusing "stuck in writing_output"
// log noise on every startup.
//
// Manifests in state output_written or sources_deleted are left alone no
// matter how old they are — those are valid pending work that the watcher
// needs to process, and aging them out would silently drop Raft manifest
// updates the cluster needs.
//
// A zero or negative orphanTimeout means "use 10 minutes" so tests and
// production agree on a safe default unless the operator explicitly tunes it.
//
// Safe to call multiple times. Safe when CompletionDir is empty (no-op).
func (m *Manager) CleanupOrphanedCompletionManifests(orphanTimeout time.Duration) error {
	if m.CompletionDir == "" {
		return nil
	}
	if orphanTimeout <= 0 {
		orphanTimeout = 10 * time.Minute
	}

	paths, err := listPendingCompletionManifests(m.CompletionDir)
	if err != nil {
		return fmt.Errorf("list pending completion manifests: %w", err)
	}

	cutoff := time.Now().Add(-orphanTimeout)
	var swept, kept int
	for _, path := range paths {
		manifest, err := readCompletionManifest(path)
		if err != nil {
			// Unreadable manifests are suspicious but not this function's
			// problem — log and leave them for operator inspection.
			m.logger.Warn().Err(err).Str("path", path).Msg("Unreadable completion manifest during orphan sweep")
			kept++
			continue
		}
		// Only sweep manifests that are BOTH still in writing_output AND
		// stale. Manifests in later states are pending watcher work.
		if manifest.State != CompletionStateWritingOutput {
			kept++
			continue
		}
		if manifest.UpdatedAt.After(cutoff) {
			kept++
			continue
		}
		if err := deleteCompletionManifest(path); err != nil {
			m.logger.Warn().Err(err).Str("path", path).Msg("Failed to delete orphaned completion manifest")
			kept++
			continue
		}
		m.logger.Info().
			Str("job_id", manifest.JobID).
			Time("updated_at", manifest.UpdatedAt).
			Dur("age", time.Since(manifest.UpdatedAt)).
			Msg("Swept orphaned completion manifest (stuck in writing_output)")
		swept++
	}

	if swept > 0 {
		m.logger.Info().
			Int("swept", swept).
			Int("kept", kept).
			Msg("Orphaned completion manifest sweep completed")
	}
	return nil
}

// LockManager manages locks for compaction partitions
type LockManager struct {
	locks map[string]bool
	mu    sync.Mutex
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]bool),
	}
}

// AcquireLock attempts to acquire a lock for a partition
func (l *LockManager) AcquireLock(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.locks[key] {
		return false // Already locked
	}

	l.locks[key] = true
	return true
}

// ReleaseLock releases a lock for a partition
func (l *LockManager) ReleaseLock(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.locks, key)
}

// IsLocked checks if a partition is locked
func (l *LockManager) IsLocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.locks[key]
}

// enabledTiers returns the enabled tiers, in order.
func enabledTiers(tiers []Tier) []Tier {
	var out []Tier
	for _, tier := range tiers {
		if tier.IsEnabled() {
			out = append(out, tier)
		}
	}
	return out
}

// truncateCycleError bounds a retained cycle error. The history holds up to
// CycleHistoryLimit entries, so an unbounded error string is an unbounded
// retention cost.
func truncateCycleError(msg string) string {
	const limit = 512
	if len(msg) <= limit {
		return msg
	}
	return msg[:limit] + "... (truncated)"
}

// CycleByID returns the retained outcome for a cycle id. A running cycle's
// counters are resolved live. Returns false when no record is retained --
// which does not mean the cycle never ran: ids restart at 1 on restart, the
// ring is bounded, and cycles run on the node holding the compactor lease.
func (m *Manager) CycleByID(id int64) (CycleRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(m.cycleHistory) - 1; i >= 0; i-- {
		if m.cycleHistory[i].CycleID == id {
			return m.cycleHistory[i].counters().clone(), true
		}
	}
	return CycleRecord{}, false
}

// RetainedCycleRange returns the oldest and newest retained cycle ids. ok is
// false when nothing is retained -- callers must not report 0/0 as a range,
// because ids start at 1 and every id would then look newer than the newest.
//
// Safe to read as the ends of the slice: the history is appended in cycle
// order and the finalizer only ever updates the entry it appended.
func (m *Manager) RetainedCycleRange() (oldest int64, newest int64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.cycleHistory) == 0 {
		return 0, 0, false
	}
	return m.cycleHistory[0].CycleID, m.cycleHistory[len(m.cycleHistory)-1].CycleID, true
}

// CycleHistoryPage is a consistent snapshot of the retained cycle history:
// the page, the window it sits in, and which cycle is still open. Assembled
// under one lock acquisition so the fields cannot disagree -- reading them
// through separate accessors allowed a page that listed no running cycle
// beside a RunningCycleID naming one that had just started, and vice versa.
type CycleHistoryPage struct {
	Cycles   []CycleRecord
	Retained int

	// Oldest and Newest are meaningless unless HasRange; callers must not
	// report 0/0 as a range, because ids start at 1.
	Oldest   int64
	Newest   int64
	HasRange bool

	// RunningCycleID is the cycle whose record is still open, if any.
	RunningCycleID int64
	HasRunning     bool
}

// CyclePage returns up to limit retained cycles, newest first, together with
// the window and running-cycle facts that describe them.
func (m *Manager) CyclePage(limit int) CycleHistoryPage {
	m.mu.Lock()
	defer m.mu.Unlock()

	page := CycleHistoryPage{Retained: len(m.cycleHistory)}
	if len(m.cycleHistory) == 0 {
		return page
	}

	page.Oldest = m.cycleHistory[0].CycleID
	page.Newest = m.cycleHistory[len(m.cycleHistory)-1].CycleID
	page.HasRange = true

	if newest := m.cycleHistory[len(m.cycleHistory)-1]; newest.FinishedAt.IsZero() {
		page.RunningCycleID = newest.CycleID
		page.HasRunning = true
	}

	if limit < 1 {
		return page
	}
	if limit > len(m.cycleHistory) {
		limit = len(m.cycleHistory)
	}
	page.Cycles = make([]CycleRecord, 0, limit)
	for i := len(m.cycleHistory) - 1; i >= len(m.cycleHistory)-limit; i-- {
		page.Cycles = append(page.Cycles, m.cycleHistory[i].counters().clone())
	}
	return page
}
