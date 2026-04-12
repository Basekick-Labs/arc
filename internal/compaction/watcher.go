package compaction

// Phase 4 CompletionWatcher: the parent-side component that bridges the
// completion-manifest handoff into Raft commands.
//
// Responsibility: poll {CompletionDir} on a tick, read each pending manifest,
// and apply RegisterFile / DeleteFile via a ManifestBridge into the cluster
// coordinator. On success, remove the manifest from disk. On failure, leave
// it in place and retry next tick.
//
// The watcher is intentionally dumb — all the smarts live in:
//   - the subprocess (which writes the manifests atomically with state
//     transitions; see completion.go)
//   - the bridge (which owns the leader-check and the mapping from
//     CompactedOutput to the Raft command shape)
//
// This keeps the watcher single-purpose and lets both sides be tested in
// isolation with stubs.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// ErrNotLeader is returned by a ManifestBridge when the local node is not
// the Raft leader and therefore cannot append to the cluster manifest.
// The watcher treats this as a transient retry condition: the completion
// manifest is kept on disk, no counters are incremented, and the next poll
// cycle tries again. By the time Raft leadership stabilizes (sub-second in
// practice) the retry succeeds.
//
// Why a sentinel instead of a generic retryable error: we want the watcher
// to distinguish leader flap (expected, noisy, don't log at Error) from a
// real bridge failure (unexpected, should surface loudly). errors.Is against
// ErrNotLeader is the cheapest way to make that distinction explicit.
var ErrNotLeader = errors.New("compaction bridge: not the Raft leader")

// CompactedFile is the cluster-agnostic shape the bridge uses to register
// a compacted output in the Raft manifest. It intentionally excludes
// raft.FileEntry.LSN (the FSM stamps it at apply time) and
// raft.FileEntry.OriginNodeID (the bridge sets it to the local node ID
// because the compactor IS the origin from Phase 2/3's perspective).
//
// The bridge converts CompactedFile to raft.FileEntry inside the cluster
// package, keeping the compaction package free of any raft.* imports.
type CompactedFile struct {
	Path          string    // storage-relative path
	SHA256        string    // hex-encoded 64 chars
	SizeBytes     int64
	Database      string
	Measurement   string
	PartitionTime time.Time
	Tier          string
	CreatedAt     time.Time
}

// ManifestBridge is the narrow interface the compaction package imports
// from the cluster package. It abstracts "append to the cluster manifest"
// without pulling in the Raft types or the coordinator struct directly.
//
// Both methods MUST return ErrNotLeader (via %w wrapping) when the local
// node is not the Raft leader — the watcher relies on errors.Is to keep
// the manifest on disk for retry. Any other error is treated as a hard
// failure and the manifest is kept for inspection but the failure is
// logged at Error.
//
// Context is accepted so the watcher can bound each apply attempt by its
// poll interval; implementations typically pass it through to the Raft
// Apply call's timeout budget.
type ManifestBridge interface {
	RegisterCompactedFile(ctx context.Context, file CompactedFile) error
	DeleteCompactedSource(ctx context.Context, path, reason string) error
}

// CompletionWatcherConfig bundles the watcher's dependencies and tunables.
// All fields are required unless explicitly marked optional.
type CompletionWatcherConfig struct {
	// Dir is the local-disk directory scanned for pending completion manifests.
	// Matches the compaction Manager's CompletionDir field. Must not be empty.
	Dir string

	// Bridge is the cluster-side manifest appender. Required.
	Bridge ManifestBridge

	// PollInterval is the time between scan cycles. Default: 1s. Smaller
	// values catch up faster but add load; larger values delay visibility
	// of compacted files in the cluster manifest.
	PollInterval time.Duration

	// ApplyTimeout bounds a single bridge call. Default: 5s. The watcher
	// derives a fresh context from its own context with this deadline for
	// each Register/Delete call so a stuck bridge call doesn't pin the
	// poll loop.
	ApplyTimeout time.Duration

	// Logger receives structured log output. Defaults to Nop if zero.
	Logger zerolog.Logger
}

// CompletionWatcher polls the completion-manifest directory and applies
// pending manifests via the ManifestBridge. Run one per compactor node.
//
// Lifecycle:
//   w := NewCompletionWatcher(cfg)
//   w.Start(ctx)  // kicks off the poll loop in a background goroutine
//   ...
//   w.Stop()      // signals stop, waits for the loop to drain one final poll
//
// The watcher is safe to Start/Stop multiple times. It is NOT safe for
// concurrent Start calls from different goroutines.
type CompletionWatcher struct {
	cfg    CompletionWatcherConfig
	logger zerolog.Logger

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	started bool

	// registeredOutputs tracks job IDs whose RegisterCompactedFile has
	// already been applied while the manifest was in output_written state.
	// This prevents redundant Raft traffic on every poll tick while
	// waiting for the subprocess to advance the manifest to
	// sources_deleted. The set is cleared when the manifest is removed
	// (sources_deleted applied) or on watcher restart.
	registeredOutputs map[string]struct{}

	// Metrics (atomic for lock-free observability via Stats)
	pollsTotal         atomic.Int64 // total scan cycles attempted
	manifestsSeen      atomic.Int64 // manifests observed across all polls
	manifestsApplied   atomic.Int64 // successful full applies (removed after)
	manifestsNotLeader atomic.Int64 // skipped because we're not the Raft leader
	applyErrors        atomic.Int64 // bridge errors that are NOT ErrNotLeader
	registerCalls      atomic.Int64 // total RegisterCompactedFile calls
	deleteCalls        atomic.Int64 // total DeleteCompactedSource calls
	lastPollAt         atomic.Int64 // unix nanos of most recent poll end
}

// NewCompletionWatcher constructs a watcher. Returns an error if any
// required dependency is missing.
func NewCompletionWatcher(cfg CompletionWatcherConfig) (*CompletionWatcher, error) {
	if cfg.Dir == "" {
		return nil, errors.New("completion watcher: Dir is required")
	}
	if cfg.Bridge == nil {
		return nil, errors.New("completion watcher: Bridge is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = 5 * time.Second
	}
	return &CompletionWatcher{
		cfg:               cfg,
		logger:            cfg.Logger.With().Str("component", "compaction-completion-watcher").Logger(),
		registeredOutputs: make(map[string]struct{}),
	}, nil
}

// Start launches the poll loop in a background goroutine. Safe to call
// multiple times — second and subsequent calls are no-ops.
func (w *CompletionWatcher) Start(parentCtx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return
	}
	w.ctx, w.cancel = context.WithCancel(parentCtx)
	w.started = true
	w.wg.Add(1)
	go w.loop()
	w.logger.Info().
		Str("dir", w.cfg.Dir).
		Dur("poll_interval", w.cfg.PollInterval).
		Dur("apply_timeout", w.cfg.ApplyTimeout).
		Msg("Phase 4 completion watcher started")
}

// Stop signals the poll loop to exit and waits for it to drain. Safe to
// call multiple times; second and subsequent calls are no-ops. After Stop,
// the watcher can be Started again with a fresh context if desired.
func (w *CompletionWatcher) Stop() {
	w.mu.Lock()
	if !w.started {
		w.mu.Unlock()
		return
	}
	w.cancel()
	w.mu.Unlock()
	w.wg.Wait()
	w.mu.Lock()
	w.started = false
	w.mu.Unlock()
	w.logger.Info().
		Int64("polls_total", w.pollsTotal.Load()).
		Int64("manifests_applied", w.manifestsApplied.Load()).
		Int64("manifests_not_leader", w.manifestsNotLeader.Load()).
		Int64("apply_errors", w.applyErrors.Load()).
		Msg("Phase 4 completion watcher stopped")
}

// Stats returns a point-in-time snapshot of the watcher's metrics, suitable
// for /api/v1/cluster/status. The returned map is safe to marshal as JSON
// directly — all values are int64 or strings.
func (w *CompletionWatcher) Stats() map[string]int64 {
	return map[string]int64{
		"polls_total":          w.pollsTotal.Load(),
		"manifests_seen":       w.manifestsSeen.Load(),
		"manifests_applied":    w.manifestsApplied.Load(),
		"manifests_not_leader": w.manifestsNotLeader.Load(),
		"apply_errors":         w.applyErrors.Load(),
		"register_calls":       w.registerCalls.Load(),
		"delete_calls":         w.deleteCalls.Load(),
		"last_poll_at":         w.lastPollAt.Load(),
	}
}

// loop is the background poll loop. Ticks on cfg.PollInterval, honors
// ctx cancellation, runs a final poll on shutdown to drain any manifests
// that accumulated during the shutdown grace period.
func (w *CompletionWatcher) loop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			// Final drain pass with a fresh context so bridge calls don't
			// fail immediately with "context canceled". The 10s timeout
			// bounds how long shutdown waits for the drain to finish. If
			// this pass hits ErrNotLeader or an error, the manifests stay
			// on disk and the NEXT compactor start picks them up.
			drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
			w.poll(drainCtx)
			drainCancel()
			return
		case <-ticker.C:
			w.poll(w.ctx)
		}
	}
}

// poll runs a single scan cycle: list pending manifests, apply each in
// order, remove on success. Never panics; internal errors are logged and
// counted but do not propagate out. The ctx argument is used for bridge
// calls — during normal operation it's w.ctx; during shutdown drain it's
// a fresh context with a bounded timeout.
func (w *CompletionWatcher) poll(ctx context.Context) {
	w.pollsTotal.Add(1)
	defer w.lastPollAt.Store(time.Now().UnixNano())

	paths, err := listPendingCompletionManifests(w.cfg.Dir)
	if err != nil {
		w.logger.Warn().Err(err).Str("dir", w.cfg.Dir).Msg("Failed to list pending completion manifests")
		return
	}
	if len(paths) == 0 {
		return
	}

	for _, path := range paths {
		// Check ctx between manifests so shutdown is responsive even on a
		// large backlog. We don't check mid-apply because a single apply
		// is bounded by ApplyTimeout and is expected to be sub-second.
		if ctx.Err() != nil {
			return
		}
		w.applyOne(ctx, path)
	}
}

// applyOne processes a single completion manifest: read, apply via bridge,
// remove on success. On bridge error leaves the manifest in place.
func (w *CompletionWatcher) applyOne(ctx context.Context, path string) {
	manifest, err := readCompletionManifest(path)
	if err != nil {
		w.logger.Warn().Err(err).Str("path", path).Msg("Failed to read completion manifest; leaving in place")
		w.applyErrors.Add(1)
		return
	}
	w.manifestsSeen.Add(1)

	// writing_output manifests are still in progress inside the subprocess.
	// The watcher ignores them until the subprocess advances the state.
	// Orphan cleanup for stuck writing_output is handled separately by
	// Manager.CleanupOrphanedCompletionManifests at startup.
	if manifest.State == CompletionStateWritingOutput {
		return
	}

	// Skip RegisterCompactedFile if we already applied it for this job on
	// a previous tick. This happens when the manifest is in output_written
	// state: the register succeeds, but we leave the manifest in place
	// waiting for the subprocess to advance to sources_deleted. Without
	// this check, every poll tick would re-issue the same RegisterFile
	// commands (idempotent but wasteful Raft traffic).
	_, alreadyRegistered := w.registeredOutputs[manifest.JobID]

	// Apply RegisterCompactedFile for every output. We do these first because
	// DeleteFile for sources is only safe AFTER the replacement is durable
	// in the cluster manifest — otherwise a reader could see the source as
	// deleted before it sees the compacted replacement, and a mid-flight
	// query could return incomplete results.
	//
	// IMPORTANT — idempotency contract: if one output's RegisterCompactedFile
	// succeeds and the next one's Apply times out (or we crash), the watcher
	// will retry from the beginning of the Outputs slice on the next tick.
	// The bridge and the Raft FSM MUST treat duplicate RegisterFile commands
	// for the same path as idempotent. This is enforced by:
	//
	//   - raft.applyRegisterFile (fsm.go:433) — overwrites the existing entry
	//     unconditionally, so re-registration of the same file is a no-op.
	//   - raft.applyDeleteFile (fsm.go:487-489) — explicitly returns nil on
	//     "file not existed", so re-deletion is also a no-op.
	//
	// If either handler is refactored to become non-idempotent, the partial-
	// apply retry path here will start producing errors. Integration test
	// TestPhase4_CompletionManifestToFSM exercises the single-apply happy
	// path; the leader-flap test exercises the retry path with the same
	// FSM. Both would catch an accidental break of this contract.
	for _, output := range manifest.Outputs {
		if alreadyRegistered {
			break // Already applied on a previous tick; skip to delete phase.
		}
		applyCtx, cancel := context.WithTimeout(ctx, w.cfg.ApplyTimeout)
		err := w.cfg.Bridge.RegisterCompactedFile(applyCtx, CompactedFile{
			Path:          output.Path,
			SHA256:        output.SHA256,
			SizeBytes:     output.SizeBytes,
			Database:      output.Database,
			Measurement:   output.Measurement,
			PartitionTime: output.PartitionTime,
			Tier:          output.Tier,
			CreatedAt:     output.CreatedAt,
		})
		cancel()
		w.registerCalls.Add(1)
		if err != nil {
			if errors.Is(err, ErrNotLeader) {
				// Expected on non-leader compactors during leader flap.
				// Silent at Info level, leave the manifest for next tick.
				w.manifestsNotLeader.Add(1)
				w.logger.Debug().
					Str("path", output.Path).
					Str("job_id", manifest.JobID).
					Msg("Not leader; will retry completion manifest next tick")
				return
			}
			w.applyErrors.Add(1)
			w.logger.Error().
				Err(err).
				Str("path", output.Path).
				Str("job_id", manifest.JobID).
				Msg("Bridge RegisterCompactedFile failed; leaving manifest for retry")
			return
		}
	}

	// Mark that we've applied RegisterFile for this job so subsequent
	// ticks skip the redundant Raft calls.
	if !alreadyRegistered {
		w.registeredOutputs[manifest.JobID] = struct{}{}
	}

	// Only advance to DeleteFile once the subprocess has confirmed the
	// source files are actually gone from storage. In state output_written
	// the sources still exist on disk — issuing DeleteFile here would make
	// the manifest claim they're gone while they're still readable,
	// breaking the "manifest is source of truth" invariant.
	if manifest.State != CompletionStateSourcesDeleted {
		// RegisterFile has been applied. Leave the manifest in place: the
		// subprocess will rewrite it in state sources_deleted shortly, and
		// the next tick will pick up that version and issue the deletes.
		// This is NOT an error — it's the normal two-step progression.
		return
	}

	for _, source := range manifest.DeletedSources {
		applyCtx, cancel := context.WithTimeout(ctx, w.cfg.ApplyTimeout)
		err := w.cfg.Bridge.DeleteCompactedSource(applyCtx, source, fmt.Sprintf("compaction:%s", manifest.JobID))
		cancel()
		w.deleteCalls.Add(1)
		if err != nil {
			if errors.Is(err, ErrNotLeader) {
				w.manifestsNotLeader.Add(1)
				return
			}
			w.applyErrors.Add(1)
			w.logger.Error().
				Err(err).
				Str("source", source).
				Str("job_id", manifest.JobID).
				Msg("Bridge DeleteCompactedSource failed; leaving manifest for retry")
			return
		}
	}

	// All bridge calls succeeded. Remove the manifest so the next tick
	// doesn't re-apply them (even though the FSM handlers are idempotent,
	// re-applying is wasted Raft traffic).
	if err := deleteCompletionManifest(path); err != nil {
		w.logger.Warn().
			Err(err).
			Str("path", path).
			Msg("Failed to remove completion manifest after successful apply; will retry cleanup next tick")
		w.applyErrors.Add(1)
		return
	}
	// Clean up the in-memory dedup tracker — the manifest is gone from
	// disk so no future tick will see it.
	delete(w.registeredOutputs, manifest.JobID)
	w.manifestsApplied.Add(1)
	w.logger.Info().
		Str("job_id", manifest.JobID).
		Int("outputs", len(manifest.Outputs)).
		Int("deleted_sources", len(manifest.DeletedSources)).
		Msg("Phase 4 completion manifest applied and removed")
}
