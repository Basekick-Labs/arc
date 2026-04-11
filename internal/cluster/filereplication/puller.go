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
	"errors"
	"fmt"
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
	// Fetch downloads the file identified by entry from the given peer address
	// and writes the body bytes into dst. It MUST verify that the peer's
	// declared SHA-256 matches the expected value from the manifest and that
	// the body length matches the declared size. Returns (bytesWritten, error).
	Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer) (int64, error)
}

// PeerResolver maps an origin node ID to its coordinator TCP address. The
// puller looks up the address fresh on every enqueued entry (rather than
// caching) so that topology changes are picked up automatically.
type PeerResolver interface {
	// ResolvePeer returns the coordinator address for a node, or ("", false)
	// if the node is unknown. Callers treat "not found" as a transient error
	// and may re-enqueue.
	ResolvePeer(nodeID string) (string, bool)
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

	// Logger receives structured log output.
	Logger zerolog.Logger
}

// DefaultConfig returns sensible defaults. Callers typically override only
// the dependencies (Backend, Fetcher, PeerResolver, SelfNodeID, Logger).
func DefaultConfig() Config {
	return Config{
		Workers:             4,
		QueueSize:           1024,
		RetryMaxAttempts:    3,
		RetryInitialBackoff: 500 * time.Millisecond,
		FetchTimeout:        60 * time.Second,
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

	// Metrics (atomic for lock-free observability)
	totalEnqueued          atomic.Int64
	totalSkippedSelf       atomic.Int64 // origin is self — no pull needed
	totalSkippedLocal      atomic.Int64 // backend.Exists already true
	totalPulled            atomic.Int64 // successful pulls
	totalFailed            atomic.Int64 // gave up after retries
	totalDropped           atomic.Int64 // queue full
	totalChecksumMismatch  atomic.Int64 // bytes didn't match manifest SHA256
	totalPeerLookupFailure atomic.Int64 // origin node not in registry
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

	return &Puller{
		cfg:    cfg,
		queue:  make(chan *raft.FileEntry, cfg.QueueSize),
		logger: cfg.Logger.With().Str("component", "file-puller").Logger(),
	}, nil
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
// self or the file already exists locally, the entry is counted as a skip
// and never reaches a worker.
//
// Enqueue is safe to call from the Raft FSM apply callback (which must
// return quickly): all checks here are O(1) and no I/O happens inline.
func (p *Puller) Enqueue(entry *raft.FileEntry) {
	if entry == nil {
		return
	}
	// Fast-path: if origin is self there's nothing to pull.
	if entry.OriginNodeID == p.cfg.SelfNodeID {
		p.totalSkippedSelf.Add(1)
		return
	}
	// Copy so the caller can't mutate the entry out from under the worker.
	entryCopy := *entry
	select {
	case p.queue <- &entryCopy:
		p.totalEnqueued.Add(1)
	default:
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

// Stats returns a point-in-time snapshot of the puller's metrics.
func (p *Puller) Stats() map[string]int64 {
	return map[string]int64{
		"enqueued":             p.totalEnqueued.Load(),
		"skipped_self":         p.totalSkippedSelf.Load(),
		"skipped_local":        p.totalSkippedLocal.Load(),
		"pulled":               p.totalPulled.Load(),
		"failed":               p.totalFailed.Load(),
		"dropped":              p.totalDropped.Load(),
		"checksum_mismatch":    p.totalChecksumMismatch.Load(),
		"peer_lookup_failure":  p.totalPeerLookupFailure.Load(),
		"queue_depth":          int64(len(p.queue)),
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
// file in place) and re-resolves the peer address (in case of topology change).
func (p *Puller) processEntry(log zerolog.Logger, entry *raft.FileEntry) {
	for attempt := 1; attempt <= p.cfg.RetryMaxAttempts; attempt++ {
		if p.ctx.Err() != nil {
			return
		}

		// Pre-pull check: skip if already local.
		existsCtx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
		exists, existsErr := p.cfg.Backend.Exists(existsCtx, entry.Path)
		cancel()
		if existsErr == nil && exists {
			p.totalSkippedLocal.Add(1)
			return
		}

		// Resolve peer address fresh on each attempt.
		peerAddr, ok := p.cfg.PeerResolver.ResolvePeer(entry.OriginNodeID)
		if !ok {
			p.totalPeerLookupFailure.Add(1)
			log.Warn().
				Str("path", entry.Path).
				Str("origin_node_id", entry.OriginNodeID).
				Int("attempt", attempt).
				Msg("Origin node not found in registry, deferring pull")
			p.sleepBackoff(attempt)
			continue
		}

		// Attempt the pull.
		if err := p.pullOnce(log, entry, peerAddr, attempt); err != nil {
			log.Warn().
				Err(err).
				Str("path", entry.Path).
				Str("peer", peerAddr).
				Int("attempt", attempt).
				Int("max_attempts", p.cfg.RetryMaxAttempts).
				Msg("File pull attempt failed")

			if attempt >= p.cfg.RetryMaxAttempts {
				p.totalFailed.Add(1)
				log.Error().
					Err(err).
					Str("path", entry.Path).
					Str("peer", peerAddr).
					Msg("File pull giving up after max attempts (will be retried by next FSM callback or catch-up scan)")
				return
			}
			p.sleepBackoff(attempt)
			continue
		}

		// Success.
		p.totalPulled.Add(1)
		log.Info().
			Str("path", entry.Path).
			Str("peer", peerAddr).
			Int64("size_bytes", entry.SizeBytes).
			Int("attempts", attempt).
			Msg("File pulled from peer")
		return
	}
}

// pullOnce performs a single fetch attempt end-to-end: opens a pipe into the
// local backend writer, runs Fetcher.Fetch which streams bytes into the pipe,
// and verifies the total byte count on success. The Fetcher is expected to
// verify the SHA-256 itself and return an error on mismatch — this function
// only tracks the mismatch counter.
func (p *Puller) pullOnce(log zerolog.Logger, entry *raft.FileEntry, peerAddr string, attempt int) error {
	fetchCtx, cancel := context.WithTimeout(p.ctx, p.cfg.FetchTimeout)
	defer cancel()

	// Pipe: producer side (Fetcher writes) → consumer side (backend.WriteReader reads).
	// This lets us stream body bytes from the peer into the backend without
	// buffering the entire file in memory.
	pr, pw := io.Pipe()

	// Backend writer goroutine: reads from the pipe until EOF or error.
	// WriteReader is expected to read exactly entry.SizeBytes bytes and then
	// return.
	var writeErr error
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		writeErr = p.cfg.Backend.WriteReader(fetchCtx, entry.Path, pr, entry.SizeBytes)
		// Ensure any pending Fetch writes unblock if the backend aborts early.
		// (Normal case: backend drains the full body then returns nil.)
		if writeErr != nil {
			_ = pr.CloseWithError(writeErr)
		}
	}()

	// Fetch: streams the body bytes into the pipe writer. When the fetcher
	// finishes (or errors), we close the pipe writer so the backend goroutine
	// sees EOF.
	written, fetchErr := p.cfg.Fetcher.Fetch(fetchCtx, peerAddr, entry, pw)
	// Always close the pipe writer — if fetchErr, propagate so the backend
	// goroutine unblocks with an error instead of hanging on Read.
	if fetchErr != nil {
		_ = pw.CloseWithError(fetchErr)
	} else {
		_ = pw.Close()
	}

	// Wait for the backend writer to finish so we observe its error.
	<-writeDone

	if fetchErr != nil {
		// Track checksum mismatches separately so operators can see them in metrics.
		if errors.Is(fetchErr, ErrChecksumMismatch) {
			p.totalChecksumMismatch.Add(1)
			// On a checksum mismatch the local file (if any) is corrupt — try to
			// delete it so the next attempt writes from scratch.
			delCtx, delCancel := context.WithTimeout(p.ctx, 5*time.Second)
			if delErr := p.cfg.Backend.Delete(delCtx, entry.Path); delErr != nil {
				log.Warn().
					Err(delErr).
					Str("path", entry.Path).
					Msg("Failed to delete corrupt file after checksum mismatch")
			}
			delCancel()
		}
		return fetchErr
	}
	if writeErr != nil {
		return fmt.Errorf("backend write: %w", writeErr)
	}
	if written != entry.SizeBytes {
		return fmt.Errorf("short body: wrote %d bytes, expected %d", written, entry.SizeBytes)
	}
	return nil
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
// file before retrying.
var ErrChecksumMismatch = errors.New("filereplication: checksum mismatch")
