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
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

const defaultReconciliationInterval = 5 * time.Minute

// maxQuarantineLogPaths bounds the set of paths remembered for log
// deduplication of permanently unusable keys (#747). A healthy cluster keeps
// this set empty; the cap exists so a manifest full of bad entries costs a
// bounded amount of memory rather than one map entry per distinct path.
const maxQuarantineLogPaths = 1024

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

type enqueueSource uint8

const (
	enqueueSourceReactive enqueueSource = iota
	enqueueSourceCatchUp
	enqueueSourceReconciliation
)

type enqueueResult uint8

const (
	enqueueResultInvalid enqueueResult = iota
	enqueueResultEnqueued
	enqueueResultSkippedSelf
	enqueueResultSkippedDuplicate
	enqueueResultDropped
)

// maxContentMismatchPeers bounds how many candidate peers one attempt will ask
// after they reject on checksum. Rejection at the ack header costs only a
// round trip, but rejection on the computed digest costs a full body transfer,
// and the puller cannot tell which it will be before asking — so the fall
// through has to be bounded by peer count rather than by bytes.
//
// Three is enough for the case this exists for: a stale OriginNodeID first in
// the candidate list with a healthy replica behind it, which succeeds on the
// second ask. The attempt loop is unchanged, so the worst case for an entry no
// peer can serve is maxContentMismatchPeers * RetryMaxAttempts full transfers
// — 9 at the defaults, against 3 before #999. That is the price of not having
// one stale peer strand a file permanently, and it is paid only when every
// peer asked disagrees with the manifest, which the
// checksum_mismatch_exhausted counter reports.
const maxContentMismatchPeers = 3

type pullRequest struct {
	entry  *raft.FileEntry
	source enqueueSource
}

// Config bundles the puller's dependencies and tunables.
type Config struct {
	// SelfNodeID is the ID of the local node. A reactive Enqueue of a file
	// whose OriginNodeID matches is skipped: this node just wrote it. The
	// catch-up and reconciliation walks skip such a file only when it is
	// present on disk (see RepullMissingSelfOrigin), or always when that is
	// off.
	SelfNodeID string

	// RepullMissingSelfOrigin makes the walks check a self-origin entry on
	// disk instead of assuming this node still holds what it once wrote,
	// and pull it from a peer's replica when it is missing or short. On per-
	// node storage a node restored with an empty data disk otherwise pulls
	// every other node's files back and never its own (#959). Off for shared
	// backends: a missing own object there is not on any peer either, and
	// the check would be one HEAD per own entry per walk.
	RepullMissingSelfOrigin bool

	// Backend is the local storage backend. The puller calls StatFile (and,
	// on a backend that stages writes, StagedSize and Exists) to skip
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

	// ReconciliationInterval controls the delay between periodic manifest
	// rechecks. Default: 5 minutes.
	ReconciliationInterval time.Duration

	// ReconciliationGate is evaluated before and during each periodic manifest
	// walk. A nil gate allows reconciliation unconditionally.
	ReconciliationGate func() bool

	// ManifestHas reports whether path is still in the cluster manifest. The
	// puller consults it before each pull attempt and before recording a
	// catch-up failure or drop, so an entry deleted from the manifest
	// (retention, compaction, the reconciliation sweep, an operator) while its
	// pull was queued or in flight is dropped from the catch-up batch instead
	// of counted against the query gate (#759, #795). It is never invoked with
	// inflightMu held. nil means "always present", today's behaviour.
	//
	// Path membership only, not entry identity: a path deleted and
	// re-registered with a new checksum while the old pull is in flight still
	// reads as present; that stale pull fails its checksum and the path heals
	// on the next successful pull, as before.
	ManifestHas func(path string) bool

	// RecordPulledFile, when set, is called once for every file this node
	// pulled and kept, so the node's tier metadata describes the file the way
	// it would a file this node flushed itself. Without it a replicated file
	// has no tier row until the next tier scan, and the query layer routes
	// reads from those rows: a measurement with no row loses partition
	// pruning, and one with a cold row but no hot row loses its local hot
	// glob altogether.
	//
	// Called on a pull worker, so it must not block — the implementation is
	// expected to queue, as ingest.FileRegistrar does on the flush path.
	// nil means no tiering on this node.
	//
	// Returns whether anything accepted the report. The hook itself is wired
	// for the life of the coordinator, but what it reports to is attached
	// later and can be absent entirely, so the return value — not the hook
	// being non-nil — is what tier_registered counts.
	RecordPulledFile func(path string, sizeBytes int64) bool

	// RecordAbandonedFile, when set, is called for a file this node had just
	// finished pulling when it found the path gone from the manifest and
	// removed its copy. The delete worker handling that manifest delete may
	// stat the path before the bytes landed and so find nothing of its own to
	// report; without this, a hot row this node held for an earlier
	// generation of the path would stand until the next tier scan. Must not
	// block. nil means no tiering on this node.
	RecordAbandonedFile func(path string, sizeBytes int64)

	// Logger receives structured log output.
	Logger zerolog.Logger
}

// DefaultConfig returns sensible defaults. Callers typically override only
// the dependencies (Backend, Fetcher, PeerResolver, SelfNodeID, Logger).
func DefaultConfig() Config {
	return Config{
		Workers:                4,
		QueueSize:              1024,
		RetryMaxAttempts:       3,
		RetryInitialBackoff:    500 * time.Millisecond,
		FetchTimeout:           60 * time.Second,
		CatchUpQueueHighWater:  0.8,
		ReconciliationInterval: defaultReconciliationInterval,
	}
}

// Puller is the background worker pool that drains FSM file-registration
// callbacks and pulls missing files from their origin peers.
type Puller struct {
	cfg    Config
	queue  chan *pullRequest
	logger zerolog.Logger

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	mu      sync.Mutex
	started bool

	// reconciliationMu serializes manifest walks. The startup catch-up and
	// periodic reconciliation share the same walker, so a periodic pass can
	// never overlap another pass even if it is triggered concurrently.
	reconciliationMu      sync.Mutex
	reconciliationStarted bool
	catchupFinished       chan struct{}

	// inflight tracks paths currently enqueued or being processed. Enqueue
	// consults it to dedup reactive callbacks against manifest walkers (both
	// startup and periodic passes can enqueue the same path during a race).
	// Workers
	// remove the entry via defer in processEntry so the set stays bounded
	// even on panic.
	//
	// inflightCount mirrors len(inflight) as an atomic so hot-path readers
	// (FullyCaughtUp, Stats during a 503 storm) don't take inflightMu and
	// contend with the workers. The map is the source of truth for dedup;
	// the counter is updated under inflightMu in the same critical section
	// so they cannot diverge. The same lock protects catch-up path tags so a
	// worker finishing while the startup walker marks a path cannot lose the
	// catch-up failure signal.
	inflightMu    sync.Mutex
	inflight      map[string]struct{}
	inflightCount atomic.Int64

	// Metrics (atomic for lock-free observability)
	totalEnqueued         atomic.Int64
	totalSkippedSelf      atomic.Int64 // origin is self — no pull needed
	totalSkippedLocal     atomic.Int64 // file fully present locally
	totalSkippedDup       atomic.Int64 // already enqueued / in-flight
	totalPulled           atomic.Int64 // successful pulls
	totalFailed           atomic.Int64 // gave up after retries
	totalDropped          atomic.Int64 // queue full
	totalSkippedGone      atomic.Int64 // deleted from the manifest while queued or in flight
	totalChecksumMismatch atomic.Int64 // bytes didn't match manifest SHA256
	// Every candidate peer disagreed with the manifest's SHA256 for one entry.
	// Distinct from totalChecksumMismatch, which counts per-peer rejections:
	// one stale peer among several healthy ones is routine during propagation
	// and self-corrects, whereas this means no reachable peer holds the
	// generation the manifest names.
	totalChecksumMismatchExhausted atomic.Int64
	totalTierRegistered            atomic.Int64 // pulled files reported to tier metadata
	totalPeerLookupFailure         atomic.Int64 // no candidate peers available
	totalBadOffsetServer           atomic.Int64 // server rejected resume offset (AckCodeBadOffset)
	// Backend can't append (ErrResumeNotSupported). Zero in every shipping
	// configuration since #999: a resume is only attempted on a backend with a
	// staging area, and LocalBackend is both the only StagingInspector and the
	// only AppendingBackend. Kept because the two interfaces are independent
	// by contract, so a future backend could stage without appending.
	totalBadOffsetBackend atomic.Int64
	totalInvalidPath      atomic.Int64 // entry path is permanently unusable (storage.ErrInvalidPath)

	// Catch-up metrics (Phase 3). Populated by RunCatchUp and read via Stats.
	catchupStartedAt     atomic.Int64 // unix seconds; 0 if never started
	catchupCompletedAt   atomic.Int64 // unix seconds; 0 if still running or never ran
	catchupEntriesWalked atomic.Int64 // entries the walker iterated
	catchupEnqueued      atomic.Int64 // entries successfully enqueued by the walker
	// catchupSkippedLocal counts entries the walker chose NOT to enqueue:
	// a self-origin entry (totalSkippedSelf bump — present on disk when
	// RepullMissingSelfOrigin is on, always otherwise) or a path already
	// in-flight via a reactive callback (totalSkippedDup bump). A foreign
	// entry that turns out to be present is not counted here — the worker's
	// pre-pull check inside processEntry finds it and bumps the global
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
	// later successful pull after the underlying issue clears, or the entry
	// being deleted from the manifest (OnManifestDelete), can decrement the
	// corresponding scoped counter and let the gate self-heal without a process
	// restart. All three sets share inflightMu with the in-flight map,
	// which keeps finish/tag bookkeeping atomic.
	catchupPaths        map[string]struct{}
	catchupFailedPaths  map[string]struct{}
	catchupDroppedPaths map[string]struct{}
	catchupInflight     atomic.Int64

	// quarantinedPaths holds paths already logged as permanently unusable, so
	// the Error line is emitted once per path per process rather than once per
	// arrival (#747). The same path is re-offered by three independent sources
	// — the reactive FSM callback, the startup catch-up walker and the periodic
	// reconciler — and none of them can know another already reported it.
	//
	// Bounded by maxQuarantineLogPaths. Past the cap the set stops growing and
	// every arrival logs again: noisier, but a log flood is recoverable and an
	// unbounded map on a path an adversary with Raft write access could feed is
	// not. Shares inflightMu with the sets above.
	quarantinedPaths map[string]struct{}

	// staleKeptPaths holds paths whose pull exhausted every candidate on
	// checksum and whose local copy was therefore left in place. The presence
	// check consults it once per path to force one re-pull attempt.
	//
	// Needed because presence is size-only (presentAtSize). Before #999 an
	// exhausted pull deleted the local file, and that delete was what
	// guaranteed a retry: the next arrival found nothing and re-enqueued.
	// Keeping the file is better for availability but, for a rewrite that did
	// not change the file's length, it makes the local copy read as present
	// forever — turning a self-healing state into a permanent one. Not
	// reachable today, because a same-size local file is skipped before any
	// pull can mismatch, but it goes live the moment presence gains a content
	// check, and the exhausted log line promises a retry either way.
	//
	// Bounded by maxQuarantineLogPaths, like quarantinedPaths: past the cap no
	// new path is remembered, which degrades to the pre-#999 "kept forever"
	// shape rather than growing a map an adversary with Raft write access
	// could feed. Shares inflightMu with the sets above.
	staleKeptPaths map[string]struct{}

	// catchupFailed / catchupDropped count failures and drops scoped to the
	// catch-up batch only. FullyCaughtUp uses these (not the cumulative
	// totalFailed / totalDropped) so transient steady-state failures don't
	// keep the gate red forever. Both self-heal when a later startup, reactive,
	// or reconciliation pull succeeds for the affected path, or when the entry
	// leaves the manifest — see clearCatchUpFailure / clearCatchUpDrop and
	// OnManifestDelete.
	catchupFailed  atomic.Int64
	catchupDropped atomic.Int64

	// Periodic reconciliation metrics. These are intentionally separate from
	// the startup catch-up counters because a recheck must never reopen or
	// otherwise change the #392 readiness scope.
	recheckStarted       atomic.Int64
	recheckCompleted     atomic.Int64
	recheckAborted       atomic.Int64
	recheckGated         atomic.Int64
	recheckBusy          atomic.Int64
	recheckEntriesWalked atomic.Int64
	recheckEnqueued      atomic.Int64
	recheckSkipped       atomic.Int64
	recheckDropped       atomic.Int64
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
	if cfg.ReconciliationInterval <= 0 {
		cfg.ReconciliationInterval = defaults.ReconciliationInterval
	}

	return &Puller{
		cfg:                 cfg,
		queue:               make(chan *pullRequest, cfg.QueueSize),
		inflight:            make(map[string]struct{}),
		catchupPaths:        make(map[string]struct{}),
		catchupFailedPaths:  make(map[string]struct{}),
		catchupDroppedPaths: make(map[string]struct{}),
		quarantinedPaths:    make(map[string]struct{}),
		staleKeptPaths:      make(map[string]struct{}),
		catchupFinished:     make(chan struct{}),
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

// inflightRemove clears a path from the in-flight set and any catch-up tag.
// Safe to call on a path that's not in either set (no-op). Updates
// inflightCount only when an entry is actually deleted to keep the counter
// exact.
func (p *Puller) inflightRemove(path string) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	p.removeInflightLocked(path)
}

func (p *Puller) removeInflightLocked(path string) {
	p.removeInflightOnlyLocked(path)
	p.removeCatchUpTagLocked(path)
}

func (p *Puller) removeInflightOnlyLocked(path string) {
	if _, ok := p.inflight[path]; ok {
		delete(p.inflight, path)
		p.inflightCount.Add(-1)
	}
}

func (p *Puller) removeCatchUpTagLocked(path string) {
	if _, ok := p.catchupPaths[path]; ok {
		delete(p.catchupPaths, path)
		p.catchupInflight.Add(-1)
	}
}

// finishEntry records a worker outcome and removes its in-flight state while
// holding the same lock used by markCatchUp. This closes the race where a
// startup walker adds a catch-up tag after a worker checks the tag but before
// the worker removes its in-flight entry. Reconciliation failures stay outside
// startup bookkeeping, while any successful pull can heal a prior path failure
// or drop.
//
// stillWanted is the caller's ManifestHas verdict, taken outside this lock. A
// failure is recorded only for an entry the manifest still contains: one that
// was deleted while the pull was queued or in flight can never be pulled and
// must not hold the query gate (#795). The two orderings against a concurrent
// delete both converge: if the delete lands after the check, OnManifestDelete
// either removed the tag before we got here (nothing recorded) or clears the
// failure right after it was recorded.
func (p *Puller) finishEntry(path string, source enqueueSource, failed, succeeded, stillWanted bool) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()

	if failed && stillWanted && source != enqueueSourceReconciliation && p.isCatchUpPathLocked(path) {
		p.recordCatchUpFailureLocked(path)
	}
	if succeeded {
		p.clearCatchUpFailureLocked(path)
		p.clearCatchUpDropLocked(path)
	}
	if source != enqueueSourceReconciliation {
		p.removeCatchUpTagLocked(path)
	}
	p.removeInflightOnlyLocked(path)
}

// markQuarantinedForLog records that a path has been reported as permanently
// unusable and returns true only for the first caller to do so, so the Error
// line is emitted once per path per process (#747).
//
// Returns true unconditionally once the set is at maxQuarantineLogPaths: the
// choice there is between repeating a log line and growing a map without a
// bound, and only one of those is recoverable.
func (p *Puller) markQuarantinedForLog(path string) bool {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	if _, ok := p.quarantinedPaths[path]; ok {
		return false
	}
	if len(p.quarantinedPaths) >= maxQuarantineLogPaths {
		return true
	}
	p.quarantinedPaths[path] = struct{}{}
	return true
}

// markStaleKept remembers that path's local copy was left in place after every
// candidate rejected it on checksum, so the next presence check pulls it again
// instead of trusting its size. No-op past maxQuarantineLogPaths.
func (p *Puller) markStaleKept(path string) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	if len(p.staleKeptPaths) >= maxQuarantineLogPaths {
		return
	}
	p.staleKeptPaths[path] = struct{}{}
}

// takeStaleKept reports whether path was left in place by an exhausted pull,
// clearing the marker so the forced re-pull happens once per exhaustion rather
// than on every arrival. A pull that exhausts again re-marks it.
func (p *Puller) takeStaleKept(path string) bool {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	if _, ok := p.staleKeptPaths[path]; !ok {
		return false
	}
	delete(p.staleKeptPaths, path)
	return true
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
// (idempotent re-mark). When Enqueue then drops the entry, its drop branch
// removes the tag itself, in the same critical section that records the
// catch-up drop, so the walker has nothing to compensate.
func (p *Puller) markCatchUp(path string) bool {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	if _, ok := p.catchupPaths[path]; ok {
		return false
	}
	p.catchupPaths[path] = struct{}{}
	p.catchupInflight.Add(1)
	return true
}

// unmarkCatchUp removes a catch-up tag and decrements catchupInflight.
// Enqueue's drop branch does this itself for a pre-marked entry the queue
// rejects, so production code no longer calls it; kept for tests that
// build catch-up state by hand.
func (p *Puller) unmarkCatchUp(path string) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
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
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	return p.isCatchUpPathLocked(path)
}

func (p *Puller) isCatchUpPathLocked(path string) bool {
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
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	p.recordCatchUpFailureLocked(path)
}

func (p *Puller) recordCatchUpFailureLocked(path string) {
	if _, ok := p.catchupFailedPaths[path]; ok {
		return
	}
	p.catchupFailedPaths[path] = struct{}{}
	p.catchupFailed.Add(1)
}

// clearCatchUpFailure decrements the catch-up failure counter if the given
// path was previously recorded as failed. Called from processEntry's success
// path, so any successful pull, including reconciliation, can heal the gate
// without a process restart, and from OnManifestDelete when the entry leaves
// the manifest. No-op if the path was not previously failed.
func (p *Puller) clearCatchUpFailure(path string) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	p.clearCatchUpFailureLocked(path)
}

func (p *Puller) clearCatchUpFailureLocked(path string) {
	if _, ok := p.catchupFailedPaths[path]; !ok {
		return
	}
	delete(p.catchupFailedPaths, path)
	p.catchupFailed.Add(-1)
}

// recordCatchUpDrop adds a path to the dropped-catch-up set and increments
// the catch-up drop counter. The Locked variant is called from enqueue's
// drop branch when the queue rejects a walker entry — there's no inflight
// slot to later decrement, so we track the path here and rely on a reactive
// FSM callback, a later pull, or the entry leaving the manifest to clear it.
// Idempotent. This wrapper is used by tests.
func (p *Puller) recordCatchUpDrop(path string) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	p.recordCatchUpDropLocked(path)
}

func (p *Puller) recordCatchUpDropLocked(path string) {
	if _, ok := p.catchupDroppedPaths[path]; ok {
		return
	}
	p.catchupDroppedPaths[path] = struct{}{}
	p.catchupDropped.Add(1)
}

// clearCatchUpDrop decrements the catch-up drop counter if the given path was
// previously recorded as dropped. Called from processEntry's success path, so
// any successful pull, including reconciliation, can heal the gate without a
// process restart, and from OnManifestDelete when the entry leaves the
// manifest. No-op if the path was not previously dropped.
func (p *Puller) clearCatchUpDrop(path string) {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	p.clearCatchUpDropLocked(path)
}

func (p *Puller) clearCatchUpDropLocked(path string) {
	if _, ok := p.catchupDroppedPaths[path]; !ok {
		return
	}
	delete(p.catchupDroppedPaths, path)
	p.catchupDropped.Add(-1)
}

// manifestHas is the nil-safe wrapper around cfg.ManifestHas. Fail-open: no
// hook means "present", which is today's behaviour. Must not be called with
// inflightMu held: the coordinator's hook takes the FSM read lock, and a
// manifest page fetch can hold the FSM write lock for a full key sort.
func (p *Puller) manifestHas(path string) bool {
	return p.cfg.ManifestHas == nil || p.cfg.ManifestHas(path)
}

// recordPulledFile is the nil-safe wrapper around cfg.RecordPulledFile. No
// hook means no tiering on this node, which is today's behaviour.
func (p *Puller) recordPulledFile(path string, sizeBytes int64) {
	if p.cfg.RecordPulledFile == nil {
		return
	}
	if p.cfg.RecordPulledFile(path, sizeBytes) {
		p.totalTierRegistered.Add(1)
	}
}

// recordAbandonedFile is the nil-safe wrapper around cfg.RecordAbandonedFile.
func (p *Puller) recordAbandonedFile(path string, sizeBytes int64) {
	if p.cfg.RecordAbandonedFile != nil {
		p.cfg.RecordAbandonedFile(path, sizeBytes)
	}
}

// reconcileReplicatedMaterialization retires the query-visible local materialisation
// created by WAL replication once the canonical primary file carrying the same
// logical payload identity has arrived. Replicated paths embed the identity so this
// remains a cheap directory listing rather than reading the canonical Parquet file.
func (p *Puller) reconcileReplicatedMaterialization(log zerolog.Logger, entry *raft.FileEntry) {
	if entry.ContentHash == "" {
		return
	}
	dir := path.Dir(entry.Path)
	prefix := dir + "/"
	paths, err := p.cfg.Backend.List(p.ctx, prefix)
	if err != nil {
		log.Warn().Err(err).Str("prefix", prefix).Msg("Failed to list replicated materialisations for reconciliation")
		return
	}
	marker := "_replicated_" + entry.ContentHash + "_"
	for _, candidate := range paths {
		base := path.Base(candidate)
		if !strings.Contains(base, marker) {
			continue
		}
		sizeBytes, statErr := p.cfg.Backend.StatFile(p.ctx, candidate)
		if statErr != nil {
			log.Warn().Err(statErr).Str("path", candidate).Msg("Failed to stat replicated materialisation during reconciliation")
			continue
		}
		if err := p.cfg.Backend.Delete(p.ctx, candidate); err != nil {
			log.Error().Err(err).Str("path", candidate).Str("canonical_path", entry.Path).Msg("Failed to retire replicated materialisation after canonical file arrived")
			continue
		}
		p.recordAbandonedFile(candidate, sizeBytes)
		log.Info().Str("path", candidate).Str("canonical_path", entry.Path).Msg("Retired replicated materialisation after canonical file arrived")
	}
}

// forgetCatchUpPathLocked drops every trace of path from the catch-up batch:
// a recorded failure, a recorded drop, the catch-up tag, and the once-per-
// process quarantine log marker (so a re-registered bad path logs again). Each
// step is membership-guarded, so it composes with the worker's own later
// finishEntry without double-decrementing. Reports what it actually removed so
// callers log only real changes.
func (p *Puller) forgetCatchUpPathLocked(path string) (hadFailure, hadDrop, hadTag bool) {
	_, hadFailure = p.catchupFailedPaths[path]
	_, hadDrop = p.catchupDroppedPaths[path]
	_, hadTag = p.catchupPaths[path]
	p.clearCatchUpFailureLocked(path)
	p.clearCatchUpDropLocked(path)
	p.removeCatchUpTagLocked(path)
	delete(p.quarantinedPaths, path)
	delete(p.staleKeptPaths, path)
	return hadFailure, hadDrop, hadTag
}

// OnManifestDelete is the FSM delete callback's hook (#759, #795). Removing an
// entry from the cluster manifest is the operator's remedy for a file no peer
// can serve, and retention, compaction and the reconciliation sweep remove
// entries the same way; from this node's point of view the pull is no longer
// wanted, so nothing about it may hold the query gate.
//
// Removing the catch-up tag, not only the recorded failure, is what makes the
// timing safe: if the pull is still queued or in flight, the worker's deferred
// finishEntry finds no tag and records nothing, and the gate reopens now
// rather than after the remaining retries. The in-flight slot itself is left
// to the worker, which owns it and checks ManifestHas before its next attempt.
//
// A delete that lands before the walker has tagged the entry (the walker can
// wait minutes mid-page at queue high water) finds nothing here; that ordering
// is covered by ManifestHas in processEntry, finishEntry and enqueue's drop
// branch.
//
// Called synchronously on the Raft apply goroutine and must not block: it only
// takes inflightMu, whose holders are all map-only sections. Safe on a stopped
// puller.
func (p *Puller) OnManifestDelete(path string) {
	p.inflightMu.Lock()
	hadFailure, hadDrop, hadTag := p.forgetCatchUpPathLocked(path)
	p.inflightMu.Unlock()

	switch {
	case hadFailure || hadDrop:
		p.logger.Info().
			Str("path", path).
			Bool("cleared_failure", hadFailure).
			Bool("cleared_drop", hadDrop).
			Int64("catchup_failed", p.catchupFailed.Load()).
			Int64("catchup_dropped", p.catchupDropped.Load()).
			Msg("Manifest entry deleted; its catch-up failure no longer holds the query gate")
	case hadTag:
		p.logger.Debug().
			Str("path", path).
			Msg("Manifest entry deleted while its catch-up pull was pending; dropped from the catch-up batch")
	}
}

// pruneStaleCatchUpState clears recorded catch-up failures and drops for paths
// the manifest no longer contains. OnManifestDelete does this synchronously
// for every delete that goes through the log and, since #962, for every path a
// snapshot restore drops from the manifest a running follower held. It is the
// backstop for what neither covers: a restart whose local snapshot predates the
// recorded path (#1071), or a path recorded before the callbacks were wired.
// Without it such a path would hold the gate red until restart. Runs on every
// periodic reconciliation tick, before the
// eligibility gate. Lookups happen outside inflightMu; each clear is
// membership-guarded, so a path OnManifestDelete already handled cannot be
// decremented twice. A path re-recorded between the lookup and the clear is
// cleared with the stale verdict; the FSM callback or the next tick settles it.
func (p *Puller) pruneStaleCatchUpState() {
	if p.cfg.ManifestHas == nil {
		return
	}
	p.inflightMu.Lock()
	candidates := make([]string, 0, len(p.catchupFailedPaths)+len(p.catchupDroppedPaths))
	for path := range p.catchupFailedPaths {
		candidates = append(candidates, path)
	}
	for path := range p.catchupDroppedPaths {
		if _, dup := p.catchupFailedPaths[path]; !dup {
			candidates = append(candidates, path)
		}
	}
	p.inflightMu.Unlock()

	pruned := 0
	var sample string
	for _, path := range candidates {
		if p.manifestHas(path) {
			continue
		}
		p.inflightMu.Lock()
		hadFailure, hadDrop, _ := p.forgetCatchUpPathLocked(path)
		p.inflightMu.Unlock()
		if hadFailure || hadDrop {
			pruned++
			if sample == "" {
				sample = path
			}
		}
	}
	if pruned > 0 {
		p.logger.Info().
			Int("pruned", pruned).
			Str("sample_path", sample).
			Int64("catchup_failed", p.catchupFailed.Load()).
			Int64("catchup_dropped", p.catchupDropped.Load()).
			Msg("Cleared catch-up failures for entries no longer in the cluster manifest")
	}
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

func (p *Puller) reconciliationAllowed() bool {
	return p.cfg.ReconciliationGate == nil || p.cfg.ReconciliationGate()
}

// StartPeriodicReconciliation starts the repeatable manifest reconciliation
// loop. The loop waits for the one-shot startup catch-up attempt to finish,
// then runs once per interval. It is owned by the puller so Stop waits for a
// pass that is already walking the manifest before returning.
func (p *Puller) StartPeriodicReconciliation(fetch func(cursor string, limit int) ([]*raft.FileEntry, string, error)) {
	p.startPeriodicReconciliation(fetch, p.cfg.ReconciliationInterval)
}

// startPeriodicReconciliation is split out so tests can use a short interval
// without adding a public configuration surface for a fixed lifecycle timer.
func (p *Puller) startPeriodicReconciliation(fetch func(cursor string, limit int) ([]*raft.FileEntry, string, error), interval time.Duration) {
	p.mu.Lock()
	if !p.started || p.reconciliationStarted {
		p.mu.Unlock()
		return
	}
	p.reconciliationStarted = true
	ctx := p.ctx
	p.wg.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.wg.Done()
		select {
		case <-p.catchupFinished:
		case <-ctx.Done():
			return
		}

		if interval <= 0 {
			interval = defaultReconciliationInterval
		}
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				p.pruneStaleCatchUpState()
				if !p.reconciliationAllowed() {
					p.recheckGated.Add(1)
					p.logger.Info().
						Str("replication_recheck_status", "gated").
						Msg("Periodic file reconciliation gated")
				} else if !p.RunReconciliation(ctx, fetch) {
					p.logger.Debug().Msg("File puller reconciliation already running, skipping periodic pass")
				}
				if ctx.Err() != nil {
					return
				}
				timer.Reset(interval)
			}
		}
	}()
}

// Stop signals all workers to exit and waits for them to finish. In-flight
// pulls are cancelled via the shared context. Pending queue entries are
// dropped (a later manifest pass or FSM callback will re-discover them).
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
		Int64("total_checksum_mismatch_exhausted", p.totalChecksumMismatchExhausted.Load()).
		Msg("File puller stopped")
}

// Enqueue submits a file entry for pulling. Non-blocking: if the queue is
// full, the entry is dropped and totalDropped is incremented. If origin is
// self (this node just wrote the file), or the same path is already
// enqueued / in-flight (via the inflight set), the entry is counted as a
// skip and never reaches a worker; a file already present on disk is
// skipped by the worker's own check.
//
// Enqueue is safe to call from the Raft FSM apply callback (which must
// return quickly): all checks here are O(1) and no I/O happens inline. It
// is also safe to call from the Phase 3 catch-up walker concurrently with
// reactive callbacks — the inflight set dedups cross-path races.
func (p *Puller) Enqueue(entry *raft.FileEntry) {
	p.enqueue(entry, enqueueSourceReactive)
}

// statLocal sizes the local copy of a path. The timeout bounds a backend
// whose StatFile honours the context (an object store HEAD); the calls a
// staging backend adds below are local stats that ignore it, and a hung local
// disk stalls this the way it stalls every other local I/O. The one presence
// rule the worker's pre-pull check and the walks' self-origin check share is
// presentAtSize.
func (p *Puller) statLocal(path string) (int64, error) {
	parent := p.ctx
	if parent == nil { // not started: tests, or a walk driven by hand
		parent = context.Background()
	}
	statCtx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	size, err := p.cfg.Backend.StatFile(statCtx, path)
	if err != nil || size < 0 {
		return size, err
	}
	// LocalBackend.StatFile falls back to the ".part" staging file when the
	// final file is absent (the resume path wants that). Presence must not:
	// a full-size .part left by a crash before the rename would read as
	// "already here" and never be finalised (#963). Where a staged partial
	// exists, confirm the final file too; without it the answer is "not
	// here", so the entry is pulled again. Backends that do not stage (S3,
	// Azure) fail the assertion, and for them StatFile's answer is final.
	if si, ok := p.cfg.Backend.(storage.StagingInspector); ok {
		staged, err := si.StagedSize(statCtx, path)
		if err != nil {
			return -1, err
		}
		if staged >= 0 {
			exists, err := p.cfg.Backend.Exists(statCtx, path)
			if err != nil {
				return -1, err
			}
			if !exists {
				p.logger.Debug().
					Str("path", path).
					Int64("staged_bytes", staged).
					Msg("Staging file present without its final file; treating the path as absent so it is pulled again")
				return -1, nil
			}
		}
	}
	return size, nil
}

// presentAtSize is the puller's definition of "already here": the stat
// succeeded and the size is exactly the manifest's. Not found, short (a
// partial), longer, or any stat error all mean the file must be pulled.
func presentAtSize(localSize int64, statErr error, want int64) bool {
	return statErr == nil && localSize == want
}

func (p *Puller) enqueue(entry *raft.FileEntry, source enqueueSource) enqueueResult {
	if entry == nil {
		return enqueueResultInvalid
	}
	// Fast-path: a reactive register of a self-origin file means this node
	// just wrote it; nothing to pull. The walks reach here for a self-origin
	// entry only after checking the disk (RepullMissingSelfOrigin), so the
	// fast path must not stop them — unless the re-pull is off, in which
	// case self-origin is never pulled, as before.
	if entry.OriginNodeID == p.cfg.SelfNodeID && (source == enqueueSourceReactive || !p.cfg.RepullMissingSelfOrigin) {
		p.totalSkippedSelf.Add(1)
		return enqueueResultSkippedSelf
	}
	// Dedup: if the path is already enqueued or being processed, don't add
	// it again. The inflight slot is released by processEntry via defer.
	if !p.inflightAdd(entry.Path) {
		p.totalSkippedDup.Add(1)
		return enqueueResultSkippedDuplicate
	}
	// Copy so the caller can't mutate the entry out from under the worker.
	entryCopy := *entry
	select {
	case p.queue <- &pullRequest{entry: &entryCopy, source: source}:
		p.totalEnqueued.Add(1)
		return enqueueResultEnqueued
	default:
		// Queue full — release the inflight slot so a future retry can
		// re-enqueue this path, and count the drop. A periodic request must
		// not clear startup catch-up tags while releasing its own slot.
		//
		// Only the walker's own drop touches catch-up state: the pre-enqueue
		// tag is removed and the catch-up drop recorded in the same critical
		// section, only while the tag is still present and only for an entry
		// the manifest still contains. That keeps catchup_dropped exact against
		// a concurrent manifest delete: delete first, the tag is gone and
		// nothing is recorded; delete after, OnManifestDelete clears the record
		// (#795). ManifestHas is evaluated before taking the lock. A reactive
		// drop leaves a pending walker tag alone: such a tag exists only inside
		// the walker's mark→enqueue window, and the walker's own enqueue always
		// resolves it (a worker's finishEntry, or this branch).
		stillWanted := source != enqueueSourceCatchUp || p.manifestHas(entry.Path)
		p.inflightMu.Lock()
		p.removeInflightOnlyLocked(entry.Path)
		if source == enqueueSourceCatchUp {
			_, tagged := p.catchupPaths[entry.Path]
			p.removeCatchUpTagLocked(entry.Path)
			if tagged && stillWanted {
				p.recordCatchUpDropLocked(entry.Path)
			}
		}
		p.inflightMu.Unlock()
		dropped := p.totalDropped.Add(1)
		// Power-of-2 rate limiting, same pattern as CoordinatorFileRegistrar.
		if dropped&(dropped-1) == 0 {
			p.logger.Warn().
				Str("path", entry.Path).
				Int64("total_dropped", dropped).
				Msg("File puller queue full, dropping entry (will be recovered by catch-up scanner)")
		}
		return enqueueResultDropped
	}
}

// Stats returns a point-in-time snapshot of the puller's metrics,
// including Phase 3 catch-up counters and the live queue / inflight depths
// the catch-up gate (#392) consumes. Lock-free: inflight count is read from
// inflightCount, queue depth from len() on the buffered channel.
func (p *Puller) Stats() map[string]int64 {
	return map[string]int64{
		"enqueued":                           p.totalEnqueued.Load(),
		"skipped_self":                       p.totalSkippedSelf.Load(),
		"skipped_local":                      p.totalSkippedLocal.Load(),
		"skipped_dup":                        p.totalSkippedDup.Load(),
		"skipped_gone":                       p.totalSkippedGone.Load(),
		"pulled":                             p.totalPulled.Load(),
		"tier_registered":                    p.totalTierRegistered.Load(),
		"failed":                             p.totalFailed.Load(),
		"dropped":                            p.totalDropped.Load(),
		"checksum_mismatch":                  p.totalChecksumMismatch.Load(),
		"checksum_mismatch_exhausted":        p.totalChecksumMismatchExhausted.Load(),
		"peer_lookup_failure":                p.totalPeerLookupFailure.Load(),
		"bad_offset_server":                  p.totalBadOffsetServer.Load(),
		"bad_offset_backend":                 p.totalBadOffsetBackend.Load(),
		"invalid_path":                       p.totalInvalidPath.Load(),
		"queue_depth":                        int64(len(p.queue)),
		"inflight_count":                     p.inflightCount.Load(),
		"catchup_started_at":                 p.catchupStartedAt.Load(),
		"catchup_completed_at":               p.catchupCompletedAt.Load(),
		"catchup_entries_walked":             p.catchupEntriesWalked.Load(),
		"catchup_enqueued":                   p.catchupEnqueued.Load(),
		"catchup_skipped_local":              p.catchupSkippedLocal.Load(),
		"catchup_inflight":                   p.catchupInflight.Load(),
		"catchup_failed":                     p.catchupFailed.Load(),
		"catchup_dropped":                    p.catchupDropped.Load(),
		"replication_recheck_started":        p.recheckStarted.Load(),
		"replication_recheck_completed":      p.recheckCompleted.Load(),
		"replication_recheck_aborted":        p.recheckAborted.Load(),
		"replication_recheck_gated":          p.recheckGated.Load(),
		"replication_recheck_busy":           p.recheckBusy.Load(),
		"replication_recheck_entries_walked": p.recheckEntriesWalked.Load(),
		"replication_recheck_enqueued":       p.recheckEnqueued.Load(),
		"replication_recheck_skipped":        p.recheckSkipped.Load(),
		"replication_recheck_dropped":        p.recheckDropped.Load(),
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
// successful pull resolves a previously-affected path, or when the entry is
// deleted from the cluster manifest (OnManifestDelete, fired by the FSM on
// every node; pruneStaleCatchUpState covers snapshot restores, which fire no
// callbacks), so transient peer outages, queue-saturation events, and entries
// no peer can serve don't require a process restart to clear the gate.
// Periodic reconciliation cannot create or remove startup tags, and its
// failures cannot reopen readiness, but its successful pulls can heal
// existing path state. The puller tracks affected paths in
// catchupFailedPaths and catchupDroppedPaths; worker success calls
// clearCatchUpFailure and clearCatchUpDrop (both are no-ops when the path was
// never recorded).
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

		// Pulled files handed to this node's tier metadata. It should track
		// "pulled" on a node with tiering on, and sit at zero on one without.
		// "pulled" climbing while this stays flat means replicated files are
		// not reaching tier_files, which costs the node partition pruning and,
		// for a measurement whose rows are cold-only, correct reads.
		"tier_registered": p.totalTierRegistered.Load(),

		// Subset of "failed" whose cause is permanent: the manifest entry names
		// a key no storage backend can address (#747). Surfaced here because it
		// lands in the query gate's 503 body, and "invalid_path > 0" is the one
		// reason a red gate will never go green on its own.
		"invalid_path": p.totalInvalidPath.Load(),
		// Pulls abandoned because their entry left the manifest: the one reason a
		// catchup_inflight drop has no matching pulled/failed (#795).
		"skipped_gone": p.totalSkippedGone.Load(),
		// Why a red gate may not go green on its own: an exhausted entry sets
		// failed -> catchupFailed, and no retry can fix it until some peer
		// holds the checksum the manifest names.
		"checksum_mismatch_exhausted": p.totalChecksumMismatchExhausted.Load(),

		// Catch-up-batch-scoped counters (added in 26.06.1 for the query
		// gate). Non-zero means the gate is closed for a reason FullyCaughtUp
		// can attribute to the cold-start batch specifically.
		"catchup_inflight": p.catchupInflight.Load(),
		"catchup_failed":   p.catchupFailed.Load(),
		"catchup_dropped":  p.catchupDropped.Load(),

		// Live queue/inflight depth.
		"queue_depth":    int64(len(p.queue)),
		"inflight_count": p.inflightCount.Load(),

		// Periodic reconciliation counters. These are prefixed so callers can
		// distinguish them from the startup catch-up contract above.
		"replication_recheck_started":        p.recheckStarted.Load(),
		"replication_recheck_completed":      p.recheckCompleted.Load(),
		"replication_recheck_aborted":        p.recheckAborted.Load(),
		"replication_recheck_gated":          p.recheckGated.Load(),
		"replication_recheck_busy":           p.recheckBusy.Load(),
		"replication_recheck_entries_walked": p.recheckEntriesWalked.Load(),
		"replication_recheck_enqueued":       p.recheckEnqueued.Load(),
		"replication_recheck_skipped":        p.recheckSkipped.Load(),
		"replication_recheck_dropped":        p.recheckDropped.Load(),
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
		case request, ok := <-p.queue:
			if !ok {
				return
			}
			p.processEntry(workerLog, request)
		}
	}
}

// processEntry pulls a single file with bounded retries. Each retry re-checks
// local presence via statLocal (in case a concurrent worker or external
// process put the file in place) and re-resolves the peer list (in case of
// topology change).
// Within a single attempt, the resolver returns an ordered list of candidate
// peers and we fall through to the next candidate on any per-peer failure
// EXCEPT checksum mismatch — a corrupt body from one peer is a real data
// integrity problem and shouldn't trigger pull-and-corrupt from every other
// healthy peer in turn.
func (p *Puller) processEntry(log zerolog.Logger, request *pullRequest) {
	entry := request.entry
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
	// finishEntry checks the catch-up tag and removes the in-flight state under
	// one lock. A reactive pull may already be in flight when RunCatchUp starts,
	// so the walker can add the tag after this worker began processing it.
	defer func() {
		// ManifestHas is consulted outside inflightMu (see manifestHas) and
		// only when there is a failure to record.
		stillWanted := !failed || p.manifestHas(entry.Path)
		p.finishEntry(entry.Path, request.source, failed, succeeded, stillWanted)
	}()

	for attempt := 1; attempt <= p.cfg.RetryMaxAttempts; attempt++ {
		if p.ctx.Err() != nil {
			return
		}

		// The manifest can drop this entry while it sits in the queue or
		// between attempts (retention, compaction, the reconciliation sweep,
		// an operator). Stop pulling: the bytes are unwanted, every peer will
		// answer not-found, and a failure here must not count against the
		// query gate. Neither failed nor succeeded is set, so finishEntry only
		// releases the tag and the inflight slot (#795).
		if !p.manifestHas(entry.Path) {
			p.totalSkippedGone.Add(1)
			log.Debug().
				Str("path", entry.Path).
				Int("attempt", attempt).
				Msg("Manifest entry deleted while its pull was pending; skipping")
			return
		}

		// Pre-pull check: skip only if the file is fully present locally
		// (size matches the manifest). A partial file (size < SizeBytes) should
		// fall through so pullOnce can resume from the byte offset.
		localSize, statErr := p.statLocal(entry.Path)
		if presentAtSize(localSize, statErr, entry.SizeBytes) {
			// One exception: a copy an exhausted pull deliberately left in
			// place. Presence is size-only, so a rewrite that did not change
			// the length makes such a copy read as present forever, and the
			// delete that used to guarantee a retry is gone (#999). Force one
			// pull; if it exhausts again it re-marks itself.
			if p.takeStaleKept(entry.Path) {
				log.Debug().
					Str("path", entry.Path).
					Int64("local_size", localSize).
					Msg("Local copy matches the manifest size but an earlier pull found no peer holding its checksum; pulling again rather than trusting the size")
			} else {
				p.totalSkippedLocal.Add(1)
				succeeded = true // File is already here — same as a fresh pull from the gate's perspective.
				return
			}
		}
		if errors.Is(statErr, storage.ErrInvalidPath) {
			// Permanent (#747). This is the first backend call an entry makes,
			// and every later one — WriteReader, the partial-file stat, the
			// delete of a corrupt partial — fails identically on the same key.
			// Without this branch the worker fetches the whole file body from a
			// peer and then fails writing it, once per candidate peer, once per
			// attempt, on every catch-up walk and every reconciliation pass,
			// forever.
			//
			// failed stays TRUE deliberately. The file really is absent, the
			// read path is a glob so a query over that partition silently
			// returns fewer rows, and an operator who enabled
			// query.gate_on_catchup asked for a 503 over exactly that. An entry
			// no peer holds is equally unsatisfiable and reds the gate today;
			// this one gets no exemption. What the quarantine removes is the
			// wasted peer traffic and the retry storm, not the signal.
			p.totalInvalidPath.Add(1)
			p.totalFailed.Add(1)
			metrics.Get().IncStorageInvalidPathQuarantined()
			failed = true
			if p.markQuarantinedForLog(entry.Path) {
				log.Error().
					Err(statErr).
					Str("path", entry.Path).
					Str("origin_node_id", entry.OriginNodeID).
					Msg("Manifest entry names a storage key no backend can address; not pulling it from any peer. It cannot be replicated here and, if the query gate is enabled, it holds the gate closed until the entry is removed from the cluster manifest, and reopens as soon as it is. Nothing removes an unaddressable key automatically: retention cannot read it and the reconciliation sweep only reports it; the operator delete endpoint is tracked in #794")
			}
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
		contentMismatches := 0
		peersTried := 0
		for _, peerAddr := range peers {
			peersTried++
			if p.ctx.Err() != nil {
				return
			}
			err := p.pullOnce(log, entry, peerAddr, attempt)
			if err == nil {
				// The entry may have left the manifest while the bytes were in
				// transit. This node's own delete worker may already have
				// unlinked the path (500 ms grace after the FSM callback), and
				// the finalize rename above would resurrect it as an orphan the
				// read glob serves. Remove it and count the pull as abandoned;
				// neither failed nor succeeded, so finishEntry only releases.
				if !p.manifestHas(entry.Path) {
					p.totalSkippedGone.Add(1)
					p.deleteFile(log, entry.Path)
					p.recordAbandonedFile(entry.Path, entry.SizeBytes)
					log.Debug().
						Str("path", entry.Path).
						Str("peer", peerAddr).
						Msg("Manifest entry deleted while its pull was in transit; local copy removed")
					return
				}
				p.totalPulled.Add(1)
				log.Info().
					Str("path", entry.Path).
					Str("peer", peerAddr).
					Int64("size_bytes", entry.SizeBytes).
					Int("attempts", attempt).
					Msg("File pulled from peer")
				// Only here, past the manifest re-check above: a file whose
				// entry left the manifest mid-pull was just unlinked and must
				// not get a tier row. entry.SizeBytes is the file's size on
				// this disk — WriteReader was given exactly that count,
				// AppendReader promotes a resume only on a full write,
				// pullOnce rejects a short body, and the bytes were verified
				// against the manifest checksum. A rewrite landing between
				// here and the write is corrected by the next pull.
				p.reconcileReplicatedMaterialization(log, entry)
				p.recordPulledFile(entry.Path, entry.SizeBytes)
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
			// A checksum mismatch says "THIS peer cannot serve the generation
			// the manifest names", not "the content is unavailable" — so it
			// falls through to the next candidate like any other per-peer
			// failure (#999).
			//
			// This reverses the original decision, which broke out of the loop
			// on the theory that falling through would "pull-and-corrupt from
			// every healthy peer in turn". That theory was wrong: every
			// candidate's bytes are verified against the manifest SHA-256
			// before they are accepted, so no bad bytes can ever be trusted no
			// matter how many peers are asked. Breaking instead made one stale
			// peer fatal — and the stale peer is routinely the FIRST one tried,
			// because the resolver puts OriginNodeID first and, until #976, a
			// rewrite in place kept the original origin (internal/api/delete.go)
			// while only the rewriting node had the new bytes. Re-resolving on the
			// next attempt yields the same ordering, so the attempt-level
			// retry this used to defer to could never rescue it.
			//
			// What falling through does cost is bandwidth: unlike a
			// file-not-on-peer ack, a mismatch is only known after the body
			// has transferred. Hence the bound below.
			if errors.Is(err, ErrChecksumMismatch) {
				contentMismatches++
				// Debug, not Warn: a peer that has not yet caught up to a
				// rewrite rejects routinely and the next candidate serves the
				// entry. The operator-visible event is the exhausted case
				// below, logged at Error.
				log.Debug().
					Err(err).
					Str("path", entry.Path).
					Str("peer", peerAddr).
					Str("manifest_sha256", entry.SHA256).
					Int("attempt", attempt).
					Int("content_mismatches", contentMismatches).
					Msg("Peer does not hold the checksum the manifest names; trying next candidate")
				if contentMismatches >= maxContentMismatchPeers {
					break
				}
				continue
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
			Int("peers_tried", peersTried).
			Int("attempt", attempt).
			Int("max_attempts", p.cfg.RetryMaxAttempts).
			Int("content_mismatches", contentMismatches).
			Msg("File pull attempt failed on all candidate peers")

		// Did EVERY candidate reject on checksum? Requires that the bound did
		// not cut the loop short — otherwise unasked candidates, one of which
		// may hold the entry, would be reported as having disagreed.
		allCandidatesRejected := contentMismatches > 0 &&
			contentMismatches == peersTried &&
			peersTried == len(peers)

		if attempt >= p.cfg.RetryMaxAttempts {
			p.totalFailed.Add(1)
			failed = true // Signal to processEntry's defer that THIS entry failed.
			if allCandidatesRejected {
				// Counted here rather than per attempt so the counter reads as
				// "entries no reachable peer could serve" rather than as a
				// multiple of RetryMaxAttempts. A mismatch on its own is NOT
				// this: a peer whose FSM has not yet converged on a rewrite
				// rejects now and serves the entry fine after the backoff,
				// which is why the attempt loop is left alone and
				// TestPullerChecksumMismatchDiscardsStagedAndRecoversOnRetry
				// pins that recovery.
				p.totalChecksumMismatchExhausted.Add(1)
				p.markStaleKept(entry.Path)
				log.Error().
					Err(lastErr).
					Str("path", entry.Path).
					Int("peers_tried", peersTried).
					Str("manifest_sha256", entry.SHA256).
					Msg("No candidate peer holds the checksum the manifest names; any local copy is left in place and the entry will be retried by the next FSM callback or catch-up scan")
				return
			}
			log.Error().
				Err(lastErr).
				Str("path", entry.Path).
				Str("last_peer", lastPeer).
				Int("peers_tried", peersTried).
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
			// Discard only the unverified bytes, never the committed file.
			// On a staging backend the failed WriteReader left the rejected
			// bytes in the ".part" and never renamed, so the committed file is
			// still the previous generation — readable, and the best copy this
			// node has until some peer can serve the one the manifest names.
			// Deleting it instead (as this did before #999) turned "serves
			// stale rows" into "has no file", which the read path cannot even
			// report as an error: it globs *.parquet, so the partition just
			// returns fewer rows.
			p.discardUnverified(log, entry.Path)
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
// Note: a non-zero offset is only ever returned for a backend with a staging
// area, and LocalBackend is both the only StagingInspector and the only
// AppendingBackend — so writeFileTail's ErrResumeNotSupported branch is
// unreachable in every shipping configuration. It is kept because the two
// interfaces are independent by contract.
//
// statErr here can no longer be storage.ErrInvalidPath, and the (0, nil) return
// is therefore not ambiguous between "no partial" and "unusable key": the same
// key already went through StatFile at the top of processEntry, which
// quarantines and returns before any of this runs (#747). The same invariant is
// why deleteFile below cannot be called with an unusable key. Both are pinned
// by a test asserting the backend sees no Delete for a quarantined entry.
//
// A resume is sized and hashed from the STAGED partial alone, never from a
// committed file, and is refused outright when a committed file exists. The
// refusal is what is load-bearing; sizing and hashing through the staging API
// is defence in depth, since StatFile and ReadToAt fall back to the staging
// file exactly when no committed file exists — the only state the refusal lets
// through. Going through StagedSize/ReadStaged additionally means a committed
// file appearing between the two calls cannot silently retarget the hash. StatFile prefers the committed object and only falls
// back to the staging file, and ReadToAt does the same; writeFileTail, by
// contrast, appends to the staging file (AppendReader). Sizing or hashing with
// the StatFile/ReadToAt pair therefore hashes the prefix of one file and
// appends the tail to a different one. Once a rejected fetch stops deleting the
// committed copy (#999), that is reachable: a committed previous generation
// shorter than the entry would be hashed as the "prefix", the new tail appended
// to the staging file, and — whenever the old generation happens to be a byte
// prefix of the new one — the combined digest VERIFIES and a tail-only file is
// renamed into place. Same hazard statLocal guards on the presence side (#963).
func (p *Puller) tryResumeFromPartial(log zerolog.Logger, entry *raft.FileEntry) (int64, hash.Hash) {
	si, ok := p.cfg.Backend.(storage.StagingInspector)
	if !ok {
		// No staging area, so there is no partial to resume from. writeFileTail
		// would reject a non-zero offset anyway (ErrResumeNotSupported).
		return 0, nil
	}

	statCtx, statCancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer statCancel()
	partial, statErr := si.StagedSize(statCtx, entry.Path)
	if statErr != nil || partial <= 0 || partial >= entry.SizeBytes {
		return 0, nil
	}
	// A committed file next to the partial means the partial is not a prefix of
	// what this fetch is building — it belongs to some earlier, abandoned
	// transfer. Pull from zero.
	committed, existsErr := p.cfg.Backend.Exists(statCtx, entry.Path)
	if existsErr != nil {
		log.Debug().Err(existsErr).Str("path", entry.Path).
			Msg("Could not confirm whether a committed file exists; pulling from zero rather than resuming")
		return 0, nil
	}
	if committed {
		return 0, nil
	}

	h := sha256.New()
	hashCtx, hashCancel := context.WithTimeout(p.ctx, 30*time.Second)
	hashErr := si.ReadStaged(hashCtx, entry.Path, h)
	hashCancel()
	if hashErr != nil {
		log.Debug().Err(hashErr).Str("path", entry.Path).
			Msg("Failed to hash partial file prefix; retrying from zero")
		p.discardUnverified(log, entry.Path)
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
// a warning if the deletion fails. Used after bad offsets.
//
// Note this removes the key's staged partial as well as the committed object
// (LocalBackend.Delete does both since #744), which is why a caller that only
// wants to discard unverified bytes must use discardUnverified instead.
func (p *Puller) deleteFile(log zerolog.Logger, path string) {
	delCtx, delCancel := context.WithTimeout(p.ctx, 5*time.Second)
	if delErr := p.cfg.Backend.Delete(delCtx, path); delErr != nil {
		log.Warn().Err(delErr).Str("path", path).Msg("Failed to delete file")
	}
	delCancel()
}

// discardUnverified throws away the bytes a rejected fetch wrote, without
// touching a committed object that passed verification earlier.
//
// On a staging backend the rejected bytes are in the key's staged partial and
// the committed object was never replaced, so discarding the staged partial is
// exactly right: this node keeps serving the generation it already had.
//
// Backends with no staging area (S3, Azure) need no cleanup at all, and must
// not get a Delete. They never commit the rejected bytes in the first place:
// the puller always hands WriteReader a pipe, so the body is not seekable and
// goes through spoolExact or the multipart uploader, both of which read one
// byte past the declared length to confirm the source ended (expectEOF,
// internal/storage/s3.go). pullOnce closes that pipe with the mismatch error,
// so the confirming read returns the error rather than io.EOF, PutObject is
// never sent and a multipart upload is aborted — leaving the previous
// generation intact. Deleting the object here would therefore destroy exactly
// the readable copy this function exists to protect.
func (p *Puller) discardUnverified(log zerolog.Logger, path string) {
	si, ok := p.cfg.Backend.(storage.StagingInspector)
	if !ok {
		log.Debug().Str("path", path).
			Msg("Rejected fetch on a backend with no staging area; nothing was committed, so nothing to discard")
		return
	}
	delCtx, delCancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer delCancel()
	if delErr := si.DeleteStaged(delCtx, path); delErr != nil {
		log.Warn().Err(delErr).Str("path", path).Msg("Failed to discard staged partial after a rejected fetch")
		return
	}
	log.Debug().Str("path", path).Msg("Discarded staged partial after a rejected fetch; committed file left in place")
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
//
// Like ErrFileNotOnPeer, this DOES trigger the multi-peer fallback, bounded by
// maxContentMismatchPeers. It says "this peer cannot serve the generation the
// manifest names", which is a per-peer fact, not a per-content one: a rewrite
// propagates through Raft before the bytes reach every replica, so a peer that
// is merely behind rejects on checksum while a healthy replica next in the
// candidate list serves the entry fine (#999).
//
// Asking more peers cannot cause bad bytes to be trusted — every candidate's
// body is verified against the manifest SHA-256 before it is accepted — so the
// only cost of the fallback is bandwidth, which the bound caps. The puller
// tracks each rejection as a distinct metric, discards the unverified bytes
// without touching the committed file (discardUnverified), and counts the
// case where every candidate rejected separately.
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
