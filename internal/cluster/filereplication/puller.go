// Package filereplication implements peer-to-peer Parquet file replication
// for Arc Enterprise clusters without shared storage. It is the byte-level
// counterpart to Phase 1's cluster-wide file manifest: when a new file is
// announced on the Raft log, the puller downloads the bytes from the origin
// peer over the coordinator TCP protocol, verifies the SHA-256, and writes
// the file to the local storage backend.
//
// The puller is gated by the Enterprise license (FeatureClustering) and
// wired by the coordinator when cluster.replication_enabled is true.
package filereplication

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Fetcher is the contract the puller uses to download a single file from a
// peer. It's an interface rather than a concrete type so the puller can be
// unit-tested with a fake that returns deterministic bytes/errors without
// opening real TCP connections.
type Fetcher interface {
	// Fetch downloads the file (or a tail of it) identified by entry from the
	// given peer address and writes body bytes into dst.
	//
	// byteOffset is the byte position to resume from (0 = full fetch). When
	// byteOffset > 0, prefixHasher must be a sha256.Hash pre-fed with bytes
	// [0, byteOffset) from the partial local file. Fetch streams bytes
	// [byteOffset, entry.SizeBytes) through the same hasher and verifies the
	// final hash against entry.SHA256. When byteOffset == 0, prefixHasher must
	// be nil; Fetch creates a fresh hasher internally.
	//
	// Returns (bytesWritten, error). bytesWritten counts only the tail bytes
	// received in this call (not the prefix already on disk).
	Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error)
}

// PeerResolver returns an ordered list of peer coordinator addresses that
// can serve a given file. The puller tries each address in order until one
// responds with the file bytes. The first address is typically the origin
// node (if still healthy) followed by any other healthy peers — that way
// catch-up after a Kubernetes pod rotation still works when the original
// writer is gone.
//
// The resolver takes (originNodeID, path) rather than the full FileEntry so
// the interface stays decoupled from the raft package — any future
// implementation that wants richer routing (health-aware, latency-aware,
// shard-aware) only needs these two fields to make its decision.
//
// The puller looks up addresses fresh on every attempt (no caching) so
// topology changes are picked up automatically. An empty slice means "no
// known peers" and is treated as a transient failure the puller can retry.
type PeerResolver interface {
	ResolvePeers(originNodeID, path string) []string
}

// Config bundles the puller's dependencies and tunables.
type Config struct {
	// SelfNodeID is the ID of the local node. Files whose OriginNodeID matches
	// are skipped (the origin already has the bytes).
	SelfNodeID string

	// Backend is the local storage backend. The puller calls Exists to skip
	// already-local files and WriteReader to stream pulled bytes onto disk.
	Backend storage.Backend

	// Fetcher is the network client that actually downloads file bytes from
	// a peer. Injected so tests can use a fake.
	Fetcher Fetcher

	// PeerResolver looks up the coordinator address for a node ID.
	PeerResolver PeerResolver

	// Workers is the number of concurrent pull goroutines. Default: 4.
	Workers int

	// QueueSize is the buffered channel capacity. Enqueues past this limit
	// are dropped and counted. Default: 1024.
	QueueSize int

	// RetryMaxAttempts is the number of immediate retry attempts for a single
	// pull failure before the entry is given up on. Further recovery happens
	// via a later FSM callback or the Phase 3 catch-up scanner. Default: 3.
	RetryMaxAttempts int

	// RetryInitialBackoff is the first retry delay. Doubles on each attempt.
	// Default: 500ms.
	RetryInitialBackoff time.Duration

	// FetchTimeout bounds a single Fetcher.Fetch call. Default: 60s.
	FetchTimeout time.Duration

	// CatchUpQueueHighWater is the queue-depth fraction above which the
	// Phase 3 catch-up walker pauses enqueueing. Keeps the walker from
	// racing ahead of workers and causing drop storms on large manifests.
	// Default: 0.8 (sleep when > 80% full).
	CatchUpQueueHighWater float64

	// Logger receives structured log output.
	Logger zerolog.Logger
}

// DefaultConfig returns sensible defaults. Callers typically override only
// the dependencies (Backend, Fetcher, PeerResolver, SelfNodeID, Logger).
func DefaultConfig() Config {
	return Config{
		Workers:               4,
		QueueSize:             1024,
		RetryMaxAttempts:      3,
		RetryInitialBackoff:   500 * time.Millisecond,
		FetchTimeout:          60 * time.Second,
		CatchUpQueueHighWater: 0.8,
	}
}

// Puller is the background worker pool that drains FSM file-registration
// callbacks and pulls missing files from their origin peers.
type Puller struct {
	cfg    Config
	queue  chan *raft.FileEntry
	logger zerolog.Logger

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	started bool

	// inflight tracks paths currently enqueued or being processed. Enqueue
	// consults it to dedup reactive callbacks against the Phase 3 catch-up
	// walker (both can enqueue the same path during a race), and workers
	// remove the entry via defer in processEntry so the set stays bounded
	// even on panic.
	//
	// inflightCount mirrors len(inflight) as an atomic so hot-path readers
	// (FullyCaughtUp, Stats during a 503 storm) don't take inflightMu and
	// contend with the workers. The map is the source of truth for dedup;
	// the counter is updated under inflightMu in the same critical section
	// so they cannot diverge.
	inflightMu    sync.Mutex
	inflight      map[string]struct{}
	inflightCount atomic.Int64

	// Metrics (atomic for lock-free observability)
	totalEnqueued               atomic.Int64
	totalSkippedSelf            atomic.Int64 // origin is self — no pull needed
	totalSkippedLocal           atomic.Int64 // file fully present locally
	totalSkippedDup             atomic.Int64 // already enqueued / in-flight
	totalPulled                 atomic.Int64 // successful pulls
	totalFailed                 atomic.Int64 // gave up after retries
	totalDropped                atomic.Int64 // queue full
	totalChecksumMismatch       atomic.Int64 // bytes didn't match manifest SHA256
	totalPeerLookupFailure      atomic.Int64 // no candidate peers available
	totalBadOffsetServer        atomic.Int64 // server rejected resume offset (AckCodeBadOffset)
	totalBadOffsetBackend       atomic.Int64 // backend can't append (ErrResumeNotSupported)

	// Catch-up metrics (Phase 3). Populated by RunCatchUp and read via Stats.
	catchupStartedAt     atomic.Int64 // unix seconds; 0 if never started
	catchupCompletedAt   atomic.Int64 // unix seconds; 0 if still running or never ran
	catchupEntriesWalked atomic.Int64 // entries the walker iterated
	catchupEnqueued      atomic.Int64 // entries successfully enqueued by the walker
	// catchupSkippedLocal counts entries the walker chose NOT to enqueue
	// because Enqueue already had a reason to skip — either origin==self
	// (totalSkippedSelf bump) or the path was already in-flight via a
	// reactive callback (totalSkippedDup bump). It does NOT include entries
	// skipped because backend.Exists(path) was already true — that check
	// happens downstream inside processEntry and is tracked by the global
	// totalSkippedLocal counter.
	catchupSkippedLocal atomic.Int64

	// catchupPaths tracks files the walker specifically enqueued. The puller
	// uses it to distinguish "catch-up batch still draining" from "steady-
	// state ingest is in flight." Steady-state pulls never enter this set, so
	// the query gate doesn't fire on every new flush in a busy cluster — only
	// while the cold-start batch is settling. Workers remove paths from this
	// set when they finish processing (success or failure); catchupInflight
	// is the atomic counterpart for lock-free reads.
	//
	// catchupFailedPaths holds catch-up paths whose pull permanently gave up
	// after retries. catchupDroppedPaths holds catch-up paths the walker
	// could not enqueue because the queue was full. Both are tracked so a
	// later successful pull (reactive FSM callback after the underlying
	// issue clears, or a subsequent catch-up scan) can decrement the
	// corresponding scoped counter and let the gate self-heal without a
	// process restart. All three sets share catchupPathsMu — they're all
	// catch-up bookkeeping and the lock is only ever held for tiny critical
	// sections.
	catchupPathsMu      sync.Mutex
	catchupPaths        map[string]struct{}
	catchupFailedPaths  map[string]struct{}
	catchupDroppedPaths map[string]struct{}
	catchupInflight     atomic.Int64

	// catchupFailed / catchupDropped count failures and drops scoped to the
	// catch-up batch only. FullyCaughtUp uses these (not the cumulative
	// totalFailed / totalDropped) so transient steady-state failures don't
	// keep the gate red forever. Both self-heal when a later pull succeeds
	// for the affected path — see clearCatchUpFailure / clearCatchUpDrop.
	catchupFailed  atomic.Int64
	catchupDropped atomic.Int64
}

// New constructs a Puller. Does not start background workers — call Start.
func New(cfg Config) (*Puller, error) {
	if cfg.Backend == nil {
		return nil, errors.New("filereplication: Backend is required")
	}
	if cfg.Fetcher == nil {
		return nil, errors.New("filereplication: Fetcher is required")
	}
	if cfg.PeerResolver == nil {
		return nil, errors.New("filereplication: PeerResolver is required")
	}
	if cfg.SelfNodeID == "" {
		return nil, errors.New("filereplication: SelfNodeID is required")
	}
	// Fill defaults
	defaults := DefaultConfig()
	if cfg.Workers <= 0 {
		cfg.Workers = defaults.Workers
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaults.QueueSize
	}
	if cfg.RetryMaxAttempts <= 0 {
		cfg.RetryMaxAttempts = defaults.RetryMaxAttempts
	}
	if cfg.RetryInitialBackoff <= 0 {
		cfg.RetryInitialBackoff = defaults.RetryInitialBackoff
	}
	if cfg.FetchTimeout <= 0 {
		cfg.FetchTimeout = defaults.FetchTimeout
	}
	// Clamp catch-up high-water to a sane range. Values <=0 or >=1 would
	// either disable throttling entirely or starve the walker, so fall back
	// to the default.
	if cfg.CatchUpQueueHighWater <= 0 || cfg.CatchUpQueueHighWater >= 1 {
		cfg.CatchUpQueueHighWater = defaults.CatchUpQueueHighWater
	}

	return &Puller{
		cfg:                 cfg,
		queue:               make(chan *raft.FileEntry, cfg.QueueSize),
		inflight:            make(map[string]struct{}),
		catchupPaths:        make(map[string]struct{}),
		catchupFailedPaths:  make(map[string]struct{}),
		catchupDroppedPaths: make(map[string]struct{}),
		logger:              cfg.Logger.With().Str("component", "file-puller").Logger(),
	}, nil
}

// inflightAdd records a path as in-flight (enqueued or being processed).
// Returns false if the path was already in the set — caller should treat
// this as "already handled, skip". Updates inflightCount in the same
// critical section as the map so atomic readers cannot observe a state
// where the count and the map disagree.
func (p *Puller) inflightAdd(path string) bool {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	if _, ok := p.inflight[path]; ok {
		return false
	}
	p.inflight[path] = struct{}{}
	p.inflightCount.Add(1)
	return true
}

// inflightRemove clears a path from the in-flight set. Safe to call on a
// path that's not in the set (no-op). Called from processEntry via defer so
// it runs on panic unwind too. Updates inflightCount only when an entry is
// actually deleted to keep the counter exact. Also clears the catch-up tag
// if the path was specifically enqueued by RunCatchUp, so FullyCaughtUp can
// distinguish "cold-start batch still draining" from "steady-state ingest."
func (p *Puller) inflightRemove(path string) {
	p.inflightMu.Lock()
	if _, ok := p.inflight[path]; ok {
		delete(p.inflight, path)
		p.inflightCount.Add(-1)
	}
	p.inflightMu.Unlock()

	p.catchupPathsMu.Lock()
	if _, ok := p.catchupPaths[path]; ok {
		delete(p.catchupPaths, path)
		p.catchupInflight.Add(-1)
	}
	p.catchupPathsMu.Unlock()
}

// markCatchUp records that a path is being enqueued by the catch-up walker
// (not by a steady-state FSM callback). Must be called BEFORE p.Enqueue —
// Enqueue is non-blocking and a fast worker can complete the pull and call
// inflightRemove (which clears the tag) before the walker's call to
// markCatchUp ran. Marking after Enqueue would leak a stale tag and
// permanently bump catchupInflight, bricking the gate.
//
// Returns true when this call actually added the tag (and incremented
// catchupInflight). Returns false when the path was already tagged
// (idempotent re-mark). RunCatchUp uses the return value to compensate
// the increment if Enqueue ends up dropping the entry.
func (p *Puller) markCatchUp(path string) bool {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	if _, ok := p.catchupPaths[path]; ok {
		return false
	}
	p.catchupPaths[path] = struct{}{}
	p.catchupInflight.Add(1)
	return true
}

// unmarkCatchUp removes a catch-up tag and decrements catchupInflight.
// Called by RunCatchUp to compensate when Enqueue drops an entry that was
// pre-marked: with no inflight slot taken, no future inflightRemove will
// run for this path, so the tag would otherwise leak.
func (p *Puller) unmarkCatchUp(path string) {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	if _, ok := p.catchupPaths[path]; !ok {
		return
	}
	delete(p.catchupPaths, path)
	p.catchupInflight.Add(-1)
}

// isCatchUpPath reports whether a path is currently tagged as catch-up-
// enqueued. Used by processEntry to decide whether a permanent failure
// should bump catchupFailed (and therefore keep the query gate red until
// a successful retry resolves the missing file).
func (p *Puller) isCatchUpPath(path string) bool {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	_, ok := p.catchupPaths[path]
	return ok
}

// recordCatchUpFailure adds a path to the failed-catch-up set and increments
// the catch-up failure counter. Called from processEntry's defer when a
// catch-up-tagged path gives up after retries. Idempotent: a duplicate
// failure for the same path (which would only happen via re-enqueue +
// re-failure) does not double-count, so the gate's self-heal accounting
// stays correct.
func (p *Puller) recordCatchUpFailure(path string) {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	if _, ok := p.catchupFailedPaths[path]; ok {
		return
	}
	p.catchupFailedPaths[path] = struct{}{}
	p.catchupFailed.Add(1)
}

// clearCatchUpFailure decrements the catch-up failure counter if the given
// path was previously recorded as failed. Called from processEntry's
// success path so a reactive FSM callback (or any later successful pull)
// can heal the gate without a process restart. No-op if the path was not
// previously failed.
func (p *Puller) clearCatchUpFailure(path string) {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	if _, ok := p.catchupFailedPaths[path]; !ok {
		return
	}
	delete(p.catchupFailedPaths, path)
	p.catchupFailed.Add(-1)
}

// recordCatchUpDrop adds a path to the dropped-catch-up set and increments
// the catch-up drop counter. Called from RunCatchUp when the queue is full
// and the walker can't enqueue an entry — there's no inflight slot to
// later decrement, so we track the path here and rely on a reactive FSM
// callback (or subsequent catch-up scan) to eventually pull it. Idempotent.
func (p *Puller) recordCatchUpDrop(path string) {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	if _, ok := p.catchupDroppedPaths[path]; ok {
		return
	}
	p.catchupDroppedPaths[path] = struct{}{}
	p.catchupDropped.Add(1)
}

// clearCatchUpDrop decrements the catch-up drop counter if the given path
// was previously recorded as dropped. Called from processEntry's success
// path so a reactive FSM callback succeeding for a previously-dropped
// path can heal the gate without a process restart. No-op if the path
// was not previously dropped.
func (p *Puller) clearCatchUpDrop(path string) {
	p.catchupPathsMu.Lock()
	defer p.catchupPathsMu.Unlock()
	if _, ok := p.catchupDroppedPaths[path]; !ok {
		return
	}
	delete(p.catchupDroppedPaths, path)
	p.catchupDropped.Add(-1)
}

// Start launches the worker pool. Safe to call multiple times — subsequent
// calls are no-ops.
func (p *Puller) Start(parentCtx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.ctx, p.cancel = context.WithCancel(parentCtx)
	p.started = true
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	p.logger.Info().
		Int("workers", p.cfg.Workers).
		Int("queue_size", p.cfg.QueueSize).
		Msg("File puller started")
}

// Stop signals all workers to exit and waits for them to finish. In-flight
// pulls are cancelled via the shared context. Pending queue entries are
// dropped (Phase 3 catch-up or a later FSM callback will re-discover them).
func (p *Puller) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.cancel()
	p.mu.Unlock()
	p.wg.Wait()
	p.logger.Info().
		Int64("total_enqueued", p.totalEnqueued.Load()).
		Int64("total_pulled", p.totalPulled.Load()).
		Int64("total_failed", p.totalFailed.Load()).
		Int64("total_dropped", p.totalDropped.Load()).
		Int64("total_checksum_mismatch", p.totalChecksumMismatch.Load()).
		Msg("File puller stopped")
}

// Enqueue submits a file entry for pulling. Non-blocking: if the queue is
// full, the entry is dropped and totalDropped is incremented. If origin is
// self, the file already exists locally, or the same path is already
// enqueued / in-flight (via the inflight set), the entry is counted as a
// skip and never reaches a worker.
//
// Enqueue is safe to call from the Raft FSM apply callback (which must
// return quickly): all checks here are O(1) and no I/O happens inline. It
// is also safe to call from the Phase 3 catch-up walker concurrently with
// reactive callbacks — the inflight set dedups cross-path races.
func (p *Puller) Enqueue(entry *raft.FileEntry) {
	if entry == nil {
		return
	}
	// Fast-path: if origin is self there's nothing to pull.
	if entry.OriginNodeID == p.cfg.SelfNodeID {
		p.totalSkippedSelf.Add(1)
		return
	}
	// Dedup: if the path is already enqueued or being processed, don't add
	// it again. The inflight slot is released by processEntry via defer.
	if !p.inflightAdd(entry.Path) {
		p.totalSkippedDup.Add(1)
		return
	}
	// Copy so the caller can't mutate the entry out from under the worker.
	entryCopy := *entry
	select {
	case p.queue <- &entryCopy:
		p.totalEnqueued.Add(1)
	default:
		// Queue full — release the inflight slot so a future retry can
		// re-enqueue this path, and count the drop.
		p.inflightRemove(entry.Path)
		dropped := p.totalDropped.Add(1)
		// Power-of-2 rate limiting, same pattern as CoordinatorFileRegistrar.
		if dropped&(dropped-1) == 0 {
			p.logger.Warn().
				Str("path", entry.Path).
				Int64("total_dropped", dropped).
				Msg("File puller queue full, dropping entry (will be recovered by catch-up scanner)")
		}
	}
}

// Stats returns a point-in-time snapshot of the puller's metrics,
// including Phase 3 catch-up counters and the live queue / inflight depths
// the catch-up gate (#392) consumes. Lock-free: inflight count is read from
// inflightCount, queue depth from len() on the buffered channel.
func (p *Puller) Stats() map[string]int64 {
	return map[string]int64{
		"enqueued":               p.totalEnqueued.Load(),
		"skipped_self":           p.totalSkippedSelf.Load(),
		"skipped_local":          p.totalSkippedLocal.Load(),
		"skipped_dup":            p.totalSkippedDup.Load(),
		"pulled":                 p.totalPulled.Load(),
		"failed":                 p.totalFailed.Load(),
		"dropped":                p.totalDropped.Load(),
		"checksum_mismatch":        p.totalChecksumMismatch.Load(),
		"peer_lookup_failure":      p.totalPeerLookupFailure.Load(),
		"bad_offset_server":        p.totalBadOffsetServer.Load(),
		"bad_offset_backend":       p.totalBadOffsetBackend.Load(),
		"queue_depth":            int64(len(p.queue)),
		"inflight_count":         p.inflightCount.Load(),
		"catchup_started_at":     p.catchupStartedAt.Load(),
		"catchup_completed_at":   p.catchupCompletedAt.Load(),
		"catchup_entries_walked": p.catchupEntriesWalked.Load(),
		"catchup_enqueued":       p.catchupEnqueued.Load(),
		"catchup_skipped_local":  p.catchupSkippedLocal.Load(),
		"catchup_inflight":       p.catchupInflight.Load(),
		"catchup_failed":         p.catchupFailed.Load(),
		"catchup_dropped":        p.catchupDropped.Load(),
	}
}

// CatchUpCompleted reports whether the startup catch-up walker has finished
// enqueueing. Preserved for operators reading puller stats directly; the
// query gate uses the stronger FullyCaughtUp predicate, which also requires
// the queue/inflight to be drained AND no failed/dropped pulls outstanding.
func (p *Puller) CatchUpCompleted() bool {
	return p.catchupCompletedAt.Load() > 0
}

// FullyCaughtUp reports whether the startup catch-up batch has fully
// converged on this node: the walker has finished its pass, every entry it
// specifically enqueued has settled, no catch-up failures are outstanding,
// and no catch-up entries were dropped. This is the signal the query gate
// consumes — it scopes "ready" to the cold-start window, not to whatever
// is in flight at any given moment.
//
// The predicate is deliberately scoped to the catch-up batch, not to all
// inflight pulls. Steady-state ingest (a writer flushes a file → FSM
// callback → reactive Enqueue) constantly puts entries in flight in any
// healthy cluster. Gating queries on those would mean the reader returns
// 503 every few seconds in normal operation, which defeats the gate's
// purpose and breaks query availability. The gate's job is "the reader
// has finished bootstrapping its view of the cluster manifest," not
// "no pulls are happening anywhere right now."
//
// Self-heal: catchupFailed and catchupDropped both decrement when a later
// pull (reactive FSM callback or otherwise) succeeds for a previously-
// affected path, so transient peer outages or queue-saturation events
// during catch-up don't require a process restart to clear the gate. The
// puller tracks the affected paths in catchupFailedPaths and
// catchupDroppedPaths respectively; processEntry's success path calls
// clearCatchUpFailure and clearCatchUpDrop unconditionally (both are no-
// ops when the path was never recorded), so any successful pull is a
// recovery event regardless of the original failure mode.
//
// Returns false (not ready) if:
//   - the catch-up walker never ran or hasn't completed, OR
//   - any path the walker enqueued is still in flight, OR
//   - a catch-up-tagged pull failed (catchupFailed > 0) and hasn't been
//     resolved by a later success, OR
//   - a catch-up entry was dropped (catchupDropped > 0).
//
// Failures and drops outside the catch-up window do NOT keep the gate red.
// They're operational concerns surfaced via Stats() but not correctness
// blockers — by the time the catch-up batch has settled, this reader has
// reconciled its view of the manifest as of walker start. Steady-state
// failures are handled by reactive FSM callbacks (which re-enqueue), the
// Phase 5 reconciler (sweeps drift between manifest and storage), and
// operator alerting via the cumulative failed/dropped counters.
//
// Returning true on a node where the puller never ran (OSS / standalone) is
// the caller's responsibility — see Coordinator.ReplicationReady, which
// short-circuits when the puller is nil. When the catch-up walker is
// disabled via cluster.replication_catchup_enabled=false the operator has
// explicitly opted out of the bootstrap safety net; in that case the
// coordinator should treat ReplicationReady() as always true (the
// configuration check in main.go enforces this).
func (p *Puller) FullyCaughtUp() bool {
	return p.catchupCompletedAt.Load() != 0 &&
		p.catchupInflight.Load() == 0 &&
		p.catchupFailed.Load() == 0 &&
		p.catchupDropped.Load() == 0
}

// CatchUpStatus returns the subset of puller metrics that operators and the
// query gate's 503 body care about. Built directly rather than projecting
// Stats() so the gate path doesn't allocate a full Stats() map on every
// blocked request just to extract a handful of fields. Returns a fresh map
// per call so the caller can mutate the result without affecting subsequent
// callers.
//
// Key semantics: cumulative counters (failed, dropped, pulled, skipped_dup)
// keep their original whole-puller-lifetime meaning — operators have been
// monitoring those keys since pre-#392 and changing semantics under them
// would silently hide steady-state problems. The new catchup_* keys carry
// the catch-up-batch-scoped values FullyCaughtUp consumes; dashboards that
// want gate-relevant numbers consume those explicitly.
func (p *Puller) CatchUpStatus() map[string]int64 {
	return map[string]int64{
		// Catch-up walker progress.
		"started_at":     p.catchupStartedAt.Load(),
		"completed_at":   p.catchupCompletedAt.Load(),
		"entries_walked": p.catchupEntriesWalked.Load(),
		"enqueued":       p.catchupEnqueued.Load(),
		"skipped_local":  p.catchupSkippedLocal.Load(),

		// Cumulative whole-puller-lifetime counters (unchanged semantics
		// since the original /api/v1/cluster/status surface). Keep these
		// for steady-state observability — a non-zero rate here means the
		// puller has been having problems regardless of where in the
		// puller's life they happened.
		"skipped_dup": p.totalSkippedDup.Load(),
		"pulled":      p.totalPulled.Load(),
		"failed":      p.totalFailed.Load(),
		"dropped":     p.totalDropped.Load(),

		// Catch-up-batch-scoped counters (added in 26.06.1 for the query
		// gate). Non-zero means the gate is closed for a reason FullyCaughtUp
		// can attribute to the cold-start batch specifically.
		"catchup_inflight": p.catchupInflight.Load(),
		"catchup_failed":   p.catchupFailed.Load(),
		"catchup_dropped":  p.catchupDropped.Load(),

		// Live queue/inflight depth.
		"queue_depth":    int64(len(p.queue)),
		"inflight_count": p.inflightCount.Load(),
	}
}

func (p *Puller) worker(id int) {
	defer p.wg.Done()
	workerLog := p.logger.With().Int("worker_id", id).Logger()
	workerLog.Debug().Msg("File puller worker started")

	for {
		select {
		case <-p.ctx.Done():
			workerLog.Debug().Msg("File puller worker exiting")
			return
		case entry, ok := <-p.queue:
			if !ok {
				return
			}
			p.processEntry(workerLog, entry)
		}
	}
}

// processEntry pulls a single file with bounded retries. Each retry re-checks
// backend.Exists (in case a concurrent worker or external process put the
// file in place) and re-resolves the peer list (in case of topology change).
// Within a single attempt, the resolver returns an ordered list of candidate
// peers and we fall through to the next candidate on any per-peer failure
// EXCEPT checksum mismatch — a corrupt body from one peer is a real data
// integrity problem and shouldn't trigger pull-and-corrupt from every other
// healthy peer in turn.
func (p *Puller) processEntry(log zerolog.Logger, entry *raft.FileEntry) {
	// failed and succeeded are local to this worker so a concurrent worker
	// processing a different entry doesn't trip our defer — using a global
	// counter delta would cross-pollinate outcomes across workers. Set by
	// the give-up path (after RetryMaxAttempts) and the success path
	// (pulledFromPeer) respectively. They're never both true.
	var failed, succeeded bool

	// Remove from the inflight set when we're done, whether success or failure.
	// This keeps the set bounded even if a worker panics — Go's defer runs on
	// panic unwind — and makes repeated catch-up walks idempotent with the
	// reactive enqueue path.
	//
	// The catch-up tag check happens INSIDE the defer (not snapshotted at
	// function entry) to close a race: a reactive pull may already be in
	// flight when RunCatchUp starts, in which case the walker tags the
	// path AFTER this worker began processing it. Reading isCatchUpPath
	// at entry would miss that late tag and we'd silently drop the
	// catch-up-failure signal — opening the gate while the file is
	// missing. Reading it inside the defer (before inflightRemove clears
	// the tag) sees whatever state the walker has converged on.
	defer func() {
		switch {
		case failed && p.isCatchUpPath(entry.Path):
			// Catch-up-tagged pull gave up after all retries. Record the
			// failure; the gate stays red until a later successful pull
			// (reactive or otherwise) calls clearCatchUpFailure.
			p.recordCatchUpFailure(entry.Path)
		case succeeded:
			// Pull succeeded. If this path had a prior catch-up failure or
			// drop, heal it so the gate can clear without a process restart.
			// Both are no-ops when the path was never in the corresponding
			// set, so safe to call on every success regardless of history.
			p.clearCatchUpFailure(entry.Path)
			p.clearCatchUpDrop(entry.Path)
		}
		p.inflightRemove(entry.Path)
	}()

	for attempt := 1; attempt <= p.cfg.RetryMaxAttempts; attempt++ {
		if p.ctx.Err() != nil {
			return
		}

		// Pre-pull check: skip only if the file is fully present locally
		// (size matches the manifest). A partial file (size < SizeBytes) should
		// fall through so pullOnce can resume from the byte offset.
		statCtx, statCancel := context.WithTimeout(p.ctx, 5*time.Second)
		localSize, statErr := p.cfg.Backend.StatFile(statCtx, entry.Path)
		statCancel()
		if statErr == nil && localSize == entry.SizeBytes {
			p.totalSkippedLocal.Add(1)
			succeeded = true // File is already here — same as a fresh pull from the gate's perspective.
			return
		}

		// Resolve candidate peers fresh on each attempt so topology changes
		// (node failover, rescheduling) are picked up automatically.
		peers := p.cfg.PeerResolver.ResolvePeers(entry.OriginNodeID, entry.Path)
		if len(peers) == 0 {
			p.totalPeerLookupFailure.Add(1)
			log.Warn().
				Str("path", entry.Path).
				Str("origin_node_id", entry.OriginNodeID).
				Int("attempt", attempt).
				Msg("No candidate peers available for fetch, deferring pull")
			p.sleepBackoff(attempt)
			continue
		}

		var lastErr error
		var lastPeer string
		pulledFromPeer := false
		checksumMismatch := false
		for _, peerAddr := range peers {
			if p.ctx.Err() != nil {
				return
			}
			err := p.pullOnce(log, entry, peerAddr, attempt)
			if err == nil {
				p.totalPulled.Add(1)
				log.Info().
					Str("path", entry.Path).
					Str("peer", peerAddr).
					Int64("size_bytes", entry.SizeBytes).
					Int("attempts", attempt).
					Msg("File pulled from peer")
				pulledFromPeer = true
				succeeded = true // Signal to processEntry's defer to clear any prior catch-up failure for this path.
				break
			}
			lastErr = err
			lastPeer = peerAddr
			// Fast-path shutdown: if the puller is stopping, pullOnce will
			// return a context.Canceled-wrapped error. Without this check
			// the loop would iterate every remaining candidate, logging a
			// debug "trying next" line and issuing a dial attempt for each,
			// before the top-of-loop ctx check finally caught it. On a
			// 10-peer cluster that's 10 wasted dials during shutdown.
			if errors.Is(err, context.Canceled) {
				return
			}
			// Checksum mismatch is a data-integrity signal, not a "try next
			// peer" signal. A peer served bytes that didn't match the manifest
			// SHA-256 — corrupt manifest, corrupt peer, or adversarial peer.
			// Do NOT fall through to other peers; let the attempt-level retry
			// handle it (delete-and-redownload semantics already in pullOnce).
			if errors.Is(err, ErrChecksumMismatch) {
				checksumMismatch = true
				break
			}
			// File-not-on-peer and transport errors both fall through to the
			// next candidate. Log at Debug so operators can see the fallback
			// in action without drowning in noise when most peers have the
			// file.
			log.Debug().
				Err(err).
				Str("path", entry.Path).
				Str("peer", peerAddr).
				Int("attempt", attempt).
				Msg("Peer fetch failed, trying next candidate")
		}
		if pulledFromPeer {
			return
		}

		log.Warn().
			Err(lastErr).
			Str("path", entry.Path).
			Str("last_peer", lastPeer).
			Int("peers_tried", len(peers)).
			Int("attempt", attempt).
			Int("max_attempts", p.cfg.RetryMaxAttempts).
			Bool("checksum_mismatch", checksumMismatch).
			Msg("File pull attempt failed on all candidate peers")

		if attempt >= p.cfg.RetryMaxAttempts {
			p.totalFailed.Add(1)
			failed = true // Signal to processEntry's defer that THIS entry failed.
			log.Error().
				Err(lastErr).
				Str("path", entry.Path).
				Str("last_peer", lastPeer).
				Int("peers_tried", len(peers)).
				Msg("File pull giving up after max attempts (will be retried by next FSM callback or catch-up scan)")
			return
		}
		p.sleepBackoff(attempt)
	}
}

// pullOnce performs a single fetch attempt end-to-end. On attempt > 1 it
// checks for a partial file and resumes from the byte offset already written,
// avoiding re-transferring bytes already on disk. The Fetcher verifies SHA-256
// across the full file (prefix + tail); this function only tracks counters.
func (p *Puller) pullOnce(log zerolog.Logger, entry *raft.FileEntry, peerAddr string, attempt int) error {
	fetchCtx, cancel := context.WithTimeout(p.ctx, p.cfg.FetchTimeout)
	defer cancel()

	// On retries, attempt to resume from a partial file already on disk.
	// byteOffset > 0 means [0, byteOffset) is written and only the tail is needed.
	var byteOffset int64
	var prefixHasher hash.Hash
	if attempt > 1 {
		byteOffset, prefixHasher = p.tryResumeFromPartial(log, entry)
	}

	tailBytes := entry.SizeBytes - byteOffset

	// Pipe: Fetch writes tail bytes into pw; the write goroutine reads from pr
	// and commits to the backend. Using a pipe keeps memory flat regardless of
	// file size — bytes flow directly from the network connection to disk.
	pr, pw := io.Pipe()

	var (
		writeErr error
		wg       sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		writeErr = p.writeFileTail(fetchCtx, entry, pr, byteOffset, tailBytes)
		if writeErr != nil {
			// Signal the fetch side to abort; it will stop writing into pw.
			_ = pr.CloseWithError(writeErr)
		}
	}()

	written, fetchErr := p.cfg.Fetcher.Fetch(fetchCtx, peerAddr, entry, pw, byteOffset, prefixHasher)
	// CloseWithError(nil) is equivalent to Close() — safe in both success and
	// error paths. Closing pw unblocks the write goroutine's next read.
	_ = pw.CloseWithError(fetchErr)
	wg.Wait()

	// ErrResumeNotSupported from the write goroutine is the root cause even when
	// fetchErr is also set (the write goroutine closed the pipe, which caused the
	// fetch side to see a broken-pipe error). Handle it before fetchErr so the
	// puller deletes the partial and retries from zero rather than treating this
	// as a generic transport failure.
	if errors.Is(writeErr, storage.ErrResumeNotSupported) {
		p.totalBadOffsetBackend.Add(1)
		p.deleteFile(log, entry.Path)
		return fmt.Errorf("backend append not supported, will retry from zero: %w", ErrBadOffset)
	}

	if fetchErr != nil {
		if errors.Is(fetchErr, ErrChecksumMismatch) {
			p.totalChecksumMismatch.Add(1)
			p.deleteFile(log, entry.Path)
		}
		if errors.Is(fetchErr, ErrBadOffset) {
			// Server rejected our resume offset — delete partial, retry from zero.
			p.totalBadOffsetServer.Add(1)
			p.deleteFile(log, entry.Path)
		}
		return fetchErr
	}
	if writeErr != nil {
		return fmt.Errorf("backend write: %w", writeErr)
	}
	if written != tailBytes {
		return fmt.Errorf("short body: wrote %d tail bytes, expected %d", written, tailBytes)
	}
	return nil
}

// writeFileTail commits tail bytes from r to the backend. When byteOffset > 0
// it appends to the partial file via AppendingBackend; when byteOffset == 0 it
// calls WriteReader for a fresh full-file write.
//
// The type-assertion to AppendingBackend is intentional: S3 and Azure Blob do
// not implement AppendingBackend, so a non-zero offset on those backends
// returns ErrResumeNotSupported and the puller falls back to a full re-fetch.
func (p *Puller) writeFileTail(ctx context.Context, entry *raft.FileEntry, r io.Reader, byteOffset, tailBytes int64) error {
	if byteOffset > 0 {
		ab, ok := p.cfg.Backend.(storage.AppendingBackend)
		if !ok {
			return storage.ErrResumeNotSupported
		}
		return ab.AppendReader(ctx, entry.Path, r, tailBytes)
	}
	return p.cfg.Backend.WriteReader(ctx, entry.Path, r, entry.SizeBytes)
}

// tryResumeFromPartial checks whether a partial file exists on disk for entry
// and, if so, hashes its bytes so the fetch client can continue the SHA-256
// chain over the tail. Returns (offset, hasher) on success, or (0, nil) if
// there is no usable partial file (not found, too large, or hash failed).
//
// Note: for backends that do not implement AppendingBackend, writeFileTail will
// return ErrResumeNotSupported when called with a non-zero offset. This is
// intentional — the puller increments bad_offset_backend and retries from zero.
func (p *Puller) tryResumeFromPartial(log zerolog.Logger, entry *raft.FileEntry) (int64, hash.Hash) {
	statCtx, statCancel := context.WithTimeout(p.ctx, 5*time.Second)
	partial, statErr := p.cfg.Backend.StatFile(statCtx, entry.Path)
	statCancel()
	if statErr != nil || partial <= 0 || partial >= entry.SizeBytes {
		return 0, nil
	}

	h := sha256.New()
	hashCtx, hashCancel := context.WithTimeout(p.ctx, 30*time.Second)
	hashErr := p.cfg.Backend.ReadToAt(hashCtx, entry.Path, h, 0)
	hashCancel()
	if hashErr != nil {
		log.Debug().Err(hashErr).Str("path", entry.Path).
			Msg("Failed to hash partial file prefix; retrying from zero")
		p.deleteFile(log, entry.Path)
		return 0, nil
	}

	log.Debug().
		Str("path", entry.Path).
		Int64("byte_offset", partial).
		Int64("total_bytes", entry.SizeBytes).
		Msg("Resuming partial file transfer")
	return partial, h
}

// deleteFile is a helper that deletes a file from the local backend, logging
// a warning if the deletion fails. Used after checksum mismatches and bad offsets.
func (p *Puller) deleteFile(log zerolog.Logger, path string) {
	delCtx, delCancel := context.WithTimeout(p.ctx, 5*time.Second)
	if delErr := p.cfg.Backend.Delete(delCtx, path); delErr != nil {
		log.Warn().Err(delErr).Str("path", path).Msg("Failed to delete file")
	}
	delCancel()
}

// sleepBackoff sleeps for an exponential backoff interval, honoring context
// cancellation. attempt is 1-indexed; the first retry uses the base delay,
// each subsequent retry doubles it.
func (p *Puller) sleepBackoff(attempt int) {
	delay := p.cfg.RetryInitialBackoff << uint(attempt-1)
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	select {
	case <-p.ctx.Done():
	case <-time.After(delay):
	}
}

// ErrChecksumMismatch is returned by Fetcher implementations when the bytes
// pulled from a peer don't match the expected SHA-256 from the manifest.
// The puller tracks this as a distinct metric and deletes the partial local
// file before retrying. Unlike ErrFileNotOnPeer, this error does NOT trigger
// the multi-peer fallback — a corrupt body is a data integrity signal.
var ErrChecksumMismatch = errors.New("filereplication: checksum mismatch")

// ErrFileNotOnPeer is returned by Fetcher implementations when a peer
// explicitly reports that it does not hold the requested file (via the ack
// header Code field, or via a known error string from a Phase 2 peer). The
// puller treats this as a fallback trigger: the next candidate in the
// resolver's list is tried before the attempt is considered failed. This is
// essential for Phase 3 catch-up after a Kubernetes pod rotation where the
// original writer is gone but other peers still hold the file.
var ErrFileNotOnPeer = errors.New("filereplication: file not on peer")

// ErrBadOffset is returned by Fetcher implementations when the server rejects
// the requested byte offset (negative, >= file size, or backend doesn't
// support seeks). The puller should delete any partial file and retry from
// zero — not fall through to another peer, since the file exists there and
// the offset is simply invalid or stale.
var ErrBadOffset = errors.New("filereplication: bad byte offset")
