package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// CoordinatorFileRegistrar implements ingest.FileRegistrar by forwarding
// file registration events to the cluster coordinator's Raft manifest.
//
// The registration is fully async: RegisterFile enqueues the entry and
// returns immediately. A background worker drains the queue and calls
// coordinator.RegisterFileInManifest. This ensures the flush hot path
// is never blocked on Raft consensus, network I/O, or checksum compute.
//
// In Phase 1 we do NOT compute checksums yet — the manifest entry records
// the path, size, and origin for validation. Checksums will be added in
// Phase 2 when we start pulling files from peers.
type CoordinatorFileRegistrar struct {
	coordinator *Coordinator
	queue       chan fileRegistration
	logger      zerolog.Logger

	// Metrics (atomic for lock-free access)
	totalEnqueued  atomic.Int64 // files successfully enqueued
	totalApplied   atomic.Int64 // files successfully applied to Raft
	totalDropped   atomic.Int64 // files dropped due to full queue
	totalApplyErrs atomic.Int64 // files that failed to apply (includes non-leader skips)

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// Stats returns a snapshot of registrar metrics.
func (r *CoordinatorFileRegistrar) Stats() map[string]int64 {
	return map[string]int64{
		"enqueued":    r.totalEnqueued.Load(),
		"applied":     r.totalApplied.Load(),
		"dropped":     r.totalDropped.Load(),
		"apply_errs":  r.totalApplyErrs.Load(),
		"queue_depth": int64(len(r.queue)),
	}
}

type fileRegistration struct {
	database      string
	measurement   string
	path          string
	partitionTime time.Time
	sizeBytes     int64
}

// NewCoordinatorFileRegistrar creates a new registrar backed by the coordinator.
// The coordinator must be started before the registrar is used.
func NewCoordinatorFileRegistrar(coord *Coordinator, logger zerolog.Logger) *CoordinatorFileRegistrar {
	return &CoordinatorFileRegistrar{
		coordinator: coord,
		// Buffered queue — drops are preferable to blocking the flush path.
		// 4096 is well above the expected flush rate (~10-100/sec) with
		// headroom for bursts during backfill or catch-up.
		queue:  make(chan fileRegistration, 4096),
		logger: logger.With().Str("component", "file-registrar").Logger(),
	}
}

// Start launches the background worker that drains the registration queue.
func (r *CoordinatorFileRegistrar) Start(parentCtx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return
	}
	r.ctx, r.cancel = context.WithCancel(parentCtx)
	r.started = true
	r.wg.Add(1)
	go r.worker()
	r.logger.Info().Msg("File registrar background worker started")
}

// Stop signals the worker to exit, drains any pending entries, and waits for
// the worker to finish. Best-effort drain: entries still in-flight after the
// drain timeout are discarded (they can be recovered by anti-entropy in a
// future phase).
func (r *CoordinatorFileRegistrar) Stop() {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return
	}
	r.cancel()
	r.mu.Unlock()
	r.wg.Wait()

	// Drain remaining queue entries (best-effort, bounded by deadline).
	drainDeadline := time.Now().Add(2 * time.Second)
	drained := 0
DrainLoop:
	for time.Now().Before(drainDeadline) {
		select {
		case reg := <-r.queue:
			r.process(reg)
			drained++
		default:
			break DrainLoop
		}
	}

	r.logger.Info().
		Int("drained", drained).
		Int64("total_enqueued", r.totalEnqueued.Load()).
		Int64("total_applied", r.totalApplied.Load()).
		Int64("total_dropped", r.totalDropped.Load()).
		Msg("File registrar background worker stopped")
}

// RegisterFile implements ingest.FileRegistrar. Non-blocking: enqueues the
// registration and returns immediately. If the queue is full, the entry is
// dropped and a counter is incremented — peer replication will discover it on
// the next anti-entropy scan (Phase 3+).
func (r *CoordinatorFileRegistrar) RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64) {
	reg := fileRegistration{
		database:      database,
		measurement:   measurement,
		path:          path,
		partitionTime: partitionTime,
		sizeBytes:     sizeBytes,
	}
	select {
	case r.queue <- reg:
		r.totalEnqueued.Add(1)
	default:
		dropped := r.totalDropped.Add(1)
		// Rate-limit the warning to avoid log spam under sustained overflow.
		// Log on every power of 2 so we see bursts without flooding logs.
		if dropped&(dropped-1) == 0 {
			r.logger.Warn().
				Str("path", path).
				Int64("total_dropped", dropped).
				Msg("File registrar queue full, dropping manifest entry (will be recovered by anti-entropy)")
		}
	}
}

func (r *CoordinatorFileRegistrar) worker() {
	defer r.wg.Done()

	for {
		select {
		case <-r.ctx.Done():
			return
		case reg := <-r.queue:
			r.process(reg)
		}
	}
}

func (r *CoordinatorFileRegistrar) process(reg fileRegistration) {
	entry := raft.FileEntry{
		Path:          reg.path,
		SizeBytes:     reg.sizeBytes,
		Database:      reg.database,
		Measurement:   reg.measurement,
		PartitionTime: reg.partitionTime,
		OriginNodeID:  r.coordinator.localNode.ID,
		Tier:          "hot",
		CreatedAt:     time.Now().UTC(),
	}

	if err := r.coordinator.RegisterFileInManifest(entry); err != nil {
		r.totalApplyErrs.Add(1)
		r.logger.Debug().
			Err(err).
			Str("path", reg.path).
			Msg("Failed to register file in cluster manifest")
		return
	}
	r.totalApplied.Add(1)
}
