package cluster

import (
	"context"
	"encoding/json"
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
// The SHA-256 checksum is computed by the flush path on the in-memory
// Parquet buffer before the backend write and passed to RegisterFile.
// Peers use it to verify files pulled during Phase 2 replication.
type CoordinatorFileRegistrar struct {
	coordinator *Coordinator
	queue       chan fileRegistration
	logger      zerolog.Logger

	// Metrics (atomic for lock-free access)
	totalEnqueued  atomic.Int64 // files successfully enqueued
	totalApplied   atomic.Int64 // files successfully applied to Raft
	totalDropped   atomic.Int64 // files dropped due to full queue
	totalApplyErrs atomic.Int64 // files that failed to apply (includes non-leader skips)

	// ctx bounds the worker's applies and is cancelled only by the parent
	// (or at the very end of Stop). Stop itself signals through stopCh, so
	// an apply in flight when Stop is called runs to completion instead of
	// failing with context.Canceled — that file had already left the queue
	// and the drain below would never have seen it again.
	ctx     context.Context
	cancel  context.CancelFunc
	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
	stopped bool
	mu      sync.Mutex
	// stopping is read on the RegisterFile hot path without the mutex: once
	// Stop has collected the queue, a late RegisterFile is dropped and
	// counted rather than left in a channel nobody will read again.
	stopping atomic.Bool
}

// registrarDrainTimeout bounds the shutdown drain in Stop. The queue is
// applied in batches, so this is a budget for a handful of Raft entries, not
// one per file.
//
// A batch is capped two ways. registrarDrainBatch is the Raft log-entry cap
// every manifest batch in Arc uses. registrarDrainChunkBytes caps the sum of
// the per-file payloads in a batch, because on a non-leader the batch is
// forwarded to the leader inside a protocol frame of at most
// protocol.MaxMessageSize (1 MiB), and the payload is base64-encoded three
// times on the way (BatchFileOp.Payload, Command.Payload,
// ForwardApplyRequest.CommandJSON are all []byte) — about 2.4x — plus JSON
// framing per op. 256 KiB of payload is ~700 KiB on the wire with 1000 ops.
// TestCoordinatorFileRegistrar_DrainChunksFitTheForwardFrame pins it.
const (
	registrarDrainTimeout    = 2 * time.Second
	registrarDrainBatch      = 1000
	registrarDrainChunkBytes = 256 << 10
)

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
	sha256        string // hex-encoded SHA-256 of the Parquet bytes
	contentHash   string // logical identity of the WAL payload set represented by this file
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
	r.stopCh = make(chan struct{})
	r.started = true
	r.wg.Add(1)
	go r.worker()
	r.logger.Info().Msg("File registrar background worker started")
}

// Stop lets the worker finish the entry it is applying, stops it, then
// applies everything still queued as batched Raft entries. It is meant to run
// after the final ArrowBuffer flush and before the cluster coordinator stops
// Raft (both ordered in cmd/arc/main.go), so the last files a node writes are
// in the manifest when it exits.
//
// The drain is bounded by registrarDrainTimeout. Entries it cannot get
// confirmed in that time are logged at Warn with the count: nothing
// re-registers them later, and where the storage reconciliation sweep is
// enabled a file with no manifest entry is an orphan-storage delete candidate
// once it ages past the grace window.
func (r *CoordinatorFileRegistrar) Stop() {
	r.mu.Lock()
	if !r.started || r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stopCh)
	r.mu.Unlock()
	r.joinWorker()

	// From here RegisterFile drops instead of enqueueing. The flag is set
	// before the queue is collected so a caller that raced the collect is
	// counted, not stranded; the sweep after the drain catches the one that
	// passed the flag check just before it flipped.
	r.stopping.Store(true)
	pending := collectQueued(r.queue)
	drained, lost := r.drain(pending)
	if late := len(collectQueued(r.queue)); late > 0 {
		dropped := r.totalDropped.Add(int64(late))
		r.logger.Warn().
			Int("files", late).
			Int64("total_dropped", dropped).
			Msg("Files registered after the registrar stopped; cluster manifest entries dropped (nothing re-registers them)")
	}
	r.cancel()

	evt := r.logger.Info()
	msg := "File registrar background worker stopped"
	if lost > 0 {
		evt = r.logger.Warn().Int("lost", lost)
		msg = "File registrar stopped with final registrations not confirmed in the cluster manifest; nothing re-registers them, and a storage reconciliation sweep treats an unregistered file as an orphan"
	}
	evt.
		Int("drained", drained).
		Int64("total_enqueued", r.totalEnqueued.Load()).
		Int64("total_applied", r.totalApplied.Load()).
		Int64("total_dropped", r.totalDropped.Load()).
		Msg(msg)
}

// joinWorker waits for the worker to finish the entry it has in flight — a
// healthy apply takes milliseconds — but not past registrarDrainTimeout: a
// node whose leader is unreachable would otherwise hold the whole shutdown
// for the forward's own deadlines, and skip the WAL and coordinator steps
// behind it. Past the budget the worker's context is cancelled; the apply it
// was in fails, as it would have anyway where the leader is unreachable.
func (r *CoordinatorFileRegistrar) joinWorker() {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(registrarDrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		r.logger.Warn().
			Dur("waited", registrarDrainTimeout).
			Msg("File registrar worker did not finish its in-flight apply in time; cancelling it")
		r.cancel()
		<-done
	}
}

// collectQueued takes everything currently in the queue without blocking.
func collectQueued(queue chan fileRegistration) []fileRegistration {
	pending := make([]fileRegistration, 0, len(queue))
	for {
		select {
		case reg := <-queue:
			pending = append(pending, reg)
		default:
			return pending
		}
	}
}

// drainChunks splits pending into batches under both caps (see the
// constants) and returns them with the number of entries left out: a path
// the FSM would refuse — it refuses the WHOLE batch for one bad path, so the
// check the FSM applies per entry is applied here per entry first — or a
// payload that would not marshal, which cannot happen for a FileEntry built
// from strings, ints and a time and is counted rather than panicked on in a
// shutdown path.
func (r *CoordinatorFileRegistrar) drainChunks(pending []fileRegistration) (chunks [][]raft.BatchFileOp, invalid int) {
	var chunk []raft.BatchFileOp
	chunkBytes := 0
	flush := func() {
		if len(chunk) > 0 {
			chunks = append(chunks, chunk)
			chunk, chunkBytes = nil, 0
		}
	}
	for _, reg := range pending {
		if err := raft.ValidateManifestPath(reg.path); err != nil {
			invalid++
			r.logger.Warn().Err(err).Str("path", reg.path).Msg("Final file registration has a path the cluster manifest refuses; skipped")
			continue
		}
		payload, err := json.Marshal(raft.RegisterFilePayload{File: r.entry(reg)})
		if err != nil {
			invalid++
			continue
		}
		if len(chunk) > 0 && (len(chunk) >= registrarDrainBatch || chunkBytes+len(payload) > registrarDrainChunkBytes) {
			flush()
		}
		chunk = append(chunk, raft.BatchFileOp{Type: raft.CommandRegisterFile, Payload: payload})
		chunkBytes += len(payload)
	}
	flush()
	return chunks, invalid
}

// registrarDrainRetryDelay is the pause before re-sending a batch that
// failed only because no leader is known yet or its address is not in the
// registry — an election in progress on a rolling restart. Both are
// documented as retry conditions (forward_apply.go); the drain deadline bounds
// how long the retries go on.
const registrarDrainRetryDelay = 100 * time.Millisecond

// drain applies pending as batched Raft entries under one
// registrarDrainTimeout deadline and returns how many were confirmed and how
// many were not. A batch refused for a transient leader error is retried
// until the deadline; any other refusal — no quorum, a command the leader
// would not apply — ends the drain, since the next batch would meet the same
// and the shutdown budget is better spent on the steps that follow. On a
// non-leader the leader may still commit a batch after this node gave up on
// it, so an unconfirmed entry is not necessarily absent from the manifest.
func (r *CoordinatorFileRegistrar) drain(pending []fileRegistration) (applied, lost int) {
	if len(pending) == 0 {
		return 0, 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), registrarDrainTimeout)
	defer cancel()

	chunks, invalid := r.drainChunks(pending)
	r.totalApplyErrs.Add(int64(invalid))
	lost = invalid
	for i, ops := range chunks {
		err := r.coordinator.BatchFileOpsInManifestContext(ctx, ops)
		for err != nil && isTransientLeaderError(err) && ctx.Err() == nil {
			timer := time.NewTimer(registrarDrainRetryDelay)
			select {
			case <-ctx.Done():
			case <-timer.C:
			}
			timer.Stop()
			if ctx.Err() != nil {
				break
			}
			err = r.coordinator.BatchFileOpsInManifestContext(ctx, ops)
		}
		if err != nil {
			remaining := 0
			for _, rest := range chunks[i:] {
				remaining += len(rest)
			}
			r.totalApplyErrs.Add(int64(remaining))
			r.logger.Warn().
				Err(err).
				Int("files", remaining).
				Msg("Failed to register final files in cluster manifest")
			return applied, lost + remaining
		}
		r.totalApplied.Add(int64(len(ops)))
		applied += len(ops)
	}
	return applied, lost
}

// RegisterFile implements ingest.FileRegistrar. Non-blocking: enqueues the
// registration and returns immediately. If the queue is full, or the
// registrar has already stopped, the entry is dropped and counted; nothing
// re-registers a file whose manifest entry was dropped.
//
// sha256 is a hex-encoded SHA-256 of the Parquet file bytes. The caller
// (arrow_writer.go flush path) computes it on the in-memory buffer before the
// storage backend write, so it's effectively free.
type ContentHashFileRegistrar interface {
	RegisterFileWithContentHash(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256, contentHash string)
}

func (r *CoordinatorFileRegistrar) RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string) {
	r.RegisterFileWithContentHash(database, measurement, path, partitionTime, sizeBytes, sha256, "")
}

func (r *CoordinatorFileRegistrar) RegisterFileWithContentHash(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256, contentHash string) {
	reg := fileRegistration{
		database:      database,
		measurement:   measurement,
		path:          path,
		partitionTime: partitionTime,
		sizeBytes:     sizeBytes,
		sha256:        sha256,
		contentHash:   contentHash,
	}
	if r.stopping.Load() {
		r.dropped(path, "File registrar stopped; cluster manifest entry dropped (nothing re-registers it)")
		return
	}
	select {
	case r.queue <- reg:
		r.totalEnqueued.Add(1)
	default:
		r.dropped(path, "File registrar queue full; cluster manifest entry dropped (nothing re-registers it)")
	}
}

// dropped counts a registration that will never reach the manifest and logs
// it on every power of two, so a sustained overflow shows without flooding.
func (r *CoordinatorFileRegistrar) dropped(path, msg string) {
	dropped := r.totalDropped.Add(1)
	if dropped&(dropped-1) == 0 {
		r.logger.Warn().
			Str("path", path).
			Int64("total_dropped", dropped).
			Msg(msg)
	}
}

func (r *CoordinatorFileRegistrar) worker() {
	defer r.wg.Done()

	for {
		// Checked on its own first: a select with a ready queue and a
		// closed stopCh picks at random, and the entries Stop is about to
		// drain as one batch would otherwise trickle out one Raft entry at
		// a time until the coin came up.
		select {
		case <-r.stopCh:
			return
		case <-r.ctx.Done():
			return
		default:
		}
		select {
		case <-r.stopCh:
			return
		case <-r.ctx.Done():
			return
		case reg := <-r.queue:
			r.process(r.ctx, reg)
		}
	}
}

func (r *CoordinatorFileRegistrar) entry(reg fileRegistration) raft.FileEntry {
	return raft.FileEntry{
		Path:          reg.path,
		SHA256:        reg.sha256,
		ContentHash:   reg.contentHash,
		SizeBytes:     reg.sizeBytes,
		Database:      reg.database,
		Measurement:   reg.measurement,
		PartitionTime: reg.partitionTime,
		OriginNodeID:  r.coordinator.localNode.ID,
		Tier:          "hot",
		CreatedAt:     time.Now().UTC(),
	}
}

func (r *CoordinatorFileRegistrar) process(parent context.Context, reg fileRegistration) {
	// One deadline per apply. Without it a non-leader's forward spends
	// manifestApplyTimeout on each of dial, send and receive, and Stop —
	// which waits for the entry in flight — would inherit all of them.
	ctx, cancel := context.WithTimeout(parent, forwardApplyTimeout)
	defer cancel()
	if err := r.coordinator.RegisterFileInManifest(ctx, r.entry(reg)); err != nil {
		failed := r.totalApplyErrs.Add(1)
		// A file on this disk that peers will never hear about, and
		// nothing re-registers it. Same power-of-2 rate limit as the
		// queue-full drop so a Raft outage does not flood the log.
		if failed&(failed-1) == 0 {
			r.logger.Warn().
				Err(err).
				Str("path", reg.path).
				Int64("total_apply_errors", failed).
				Msg("Failed to register file in cluster manifest (nothing re-registers it)")
		}
		return
	}
	r.totalApplied.Add(1)
}
