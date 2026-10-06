package tiering

// Tier metadata for files this node did not write itself.
//
// Before this, a node's tier_files only ever described files it had ingested:
// the flush path registers what it writes (internal/ingest, RecordFile) and
// ScanAndRegisterFiles picks up the rest — but that scan runs only from
// POST /api/v1/tiering/scan, from the pre-migration scan inside a migration
// cycle, and (since this change) once at startup. At the default
// migration_schedule of 02:00 that is once a day.
//
// On a cluster node with per-node storage and file replication, most of what
// is on disk arrives from a peer, and most of what leaves does so because
// another node migrated it. Between daily scans the metadata and the disk
// disagree, and the query layer routes reads from the metadata:
//
//   - a measurement with no row at all falls back to an unpruned
//     {db}/{meas}/**/*.parquet glob (a performance cost), and
//   - a measurement with a cold row and no hot row loses its local hot glob
//     entirely, so files sitting on this node's disk are silently left out of
//     the answer (a correctness cost).
//
// This file closes both directions: the replication puller reports what it
// pulled, the local-delete worker reports what it unlinked, and a single
// drainer applies both to tier_files.
//
// Why a drainer rather than a write per event: tiering shares one *sql.DB with
// auth, audit, CQ and retention, and that handle is opened with
// SetMaxOpenConns(1). A write per pulled file, from the puller's workers and
// the delete workers at once, would queue on that single connection against
// live authentication, the audit writer and the ingest flush path's own
// registration. AuthManager.lastUsedLoop solved the same problem the same way;
// its comment is worth reading. A batching drainer also bounds how often the
// tier cache is invalidated: see the note above recordHotFileIfNotCold.

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// tierEventQueueMax bounds the drainer's queue in memory. Reports never block
// their caller and are never dropped while the node is running normally: the
// queue grows to whatever a burst needs and the drainer takes all of it. The
// cap is a memory bound for a drainer that has stopped making progress — a
// stalled SQLite handle, which also stalls authentication — not a throughput
// knob; at roughly 200 bytes an event it is on the order of 200 MB. Events it
// turns away are counted and reconciled by the next tier scan. A variable so
// tests can shrink it.
var tierEventQueueMax = 1 << 20

// unlinkReasonAbandonedPull is the reason the replication puller reports when
// it removes a copy it had just finished pulling because the path left the
// cluster manifest while the bytes were in transit. The puller does not know
// WHY the path left — that reason went to the delete worker, which may stat
// the path before the bytes landed and find nothing to report — so this
// reason is treated like a tiering one: the cold tier is asked. The literal is
// shared with internal/cluster (tierReasonAbandonedPull); neither package
// imports the other.
const unlinkReasonAbandonedPull = "replication:abandoned"

// unlinkReasonSnapshotRemoved is the reason the Raft FSM reports when a
// snapshot restore drops a path the previous manifest listed (#962). The
// cluster deleted it while this node was away, for a reason the snapshot does
// not carry, so it is treated like an abandoned pull: the cold tier is asked
// before the hot row is retired, and a measurement migrated during the outage
// reads from cold instead of vanishing from this node. Mirrored in
// internal/cluster/raft (UnlinkReasonSnapshotRemoved); neither package imports
// the other.
const unlinkReasonSnapshotRemoved = "snapshot:removed"

// tierEventStopDrainTimeout is the whole drain's budget once Stop has been
// signalled; each chunk gets what is left of it. Short on purpose: Stop runs
// on the shutdown timeout that every remaining hook and component shares, so a
// slow drain here is paid for by things that still have to run. What it does
// not cover is counted dropped and reconciled by the startup scan.
const tierEventStopDrainTimeout = 2 * time.Second

// tierEventDrainTimeout bounds one applied chunk of a batch. A chunk is
// applied with the process's one SQLite connection, so it must not be able to
// park the drainer behind a stalled handle indefinitely; what is left of the
// batch when a chunk runs out is counted failed and retried by the next scan.
// A variable so tests can shrink it.
var tierEventDrainTimeout = 30 * time.Second

// tierEventChunk is how many events share one deadline. The deadline is a
// stall detector, not a throughput budget: a batch is the whole queue, however
// large a burst made it, and under a migration chunk each unlink in it costs a
// cold-tier round trip — one deadline over the whole batch would be decided by
// the batch's size rather than by whether anything is stuck.
const tierEventChunk = 256

// tierEventProbeParallelism bounds the concurrent cold-tier existence checks
// per chunk. Sequential HEADs at tens of milliseconds each cannot keep up with
// the delete workers unlinking a migration's files at disk speed: a nightly
// migration of a few thousand files would outrun any deadline, and the
// measurements whose unlinks fell in the failed tail would have their hot
// files gone and no cold row until the next cold sync — which runs on the
// same schedule as the primary's migration and so lists cold BEFORE that
// night's uploads land: invisible on this node until the night after.
const tierEventProbeParallelism = 16

// tierEventKind distinguishes the two reports the cluster layer makes.
type tierEventKind int

const (
	// tierEventPulled: this node pulled the file from a peer and kept it, so
	// it is on local (hot) storage now.
	tierEventPulled tierEventKind = iota
	// tierEventUnlinked: this node removed its local copy because the file
	// left the cluster manifest. Whether that means "it is in cold now" or
	// "it is gone" is decided from evidence, not from the reason — see
	// applyUnlinked.
	tierEventUnlinked
)

// tierEvent is one queued report.
type tierEvent struct {
	kind      tierEventKind
	path      string
	reason    string
	sizeBytes int64
}

// RecordReplicatedFile reports a file this node pulled from a peer and kept,
// so it is registered in the hot tier the way a local flush would be.
//
// Never blocks: it runs on a replication pull worker. The report is appended
// to the drainer's queue, which grows as a burst needs (tierEventQueueMax).
//
// Safe on a nil receiver: the cluster layer holds this as an interface, and an
// interface holding a typed nil is not == nil (#713).
func (m *Manager) RecordReplicatedFile(path string, sizeBytes int64) {
	if m == nil {
		return
	}
	m.enqueueTierEvent(tierEvent{kind: tierEventPulled, path: path, sizeBytes: sizeBytes})
}

// RecordRestoredFile reports a data file a backup restore has just written to
// this node's hot storage (#1083). It is the same event as a replicated file:
// bytes are on hot storage and no flush registered them, so without the
// report the file has no tier row until the next tier scan, with the routing
// consequences the file comment describes. Implements backup.TierRecorder.
//
// Never blocks and nil-receiver safe, as RecordReplicatedFile: the backup
// manager holds this as an interface, and a restore writes files at disk
// speed.
func (m *Manager) RecordRestoredFile(path string, sizeBytes int64) {
	if m == nil {
		return
	}
	m.enqueueTierEvent(tierEvent{kind: tierEventPulled, path: path, sizeBytes: sizeBytes})
}

// RecordUnlinkedFile reports that this node removed its own local copy of a
// path because the path left the cluster manifest. sizeBytes is the size the
// caller stat'd before deleting, which is also the evidence that this node
// actually held the file.
//
// Never blocks and nil-receiver safe, as RecordReplicatedFile. It runs on a
// local-delete worker, and a migration chunk has those unlinking at disk
// speed while each unlink costs the drainer a cold-tier round trip; the queue
// absorbs that difference, so neither the worker nor the event pays for it.
func (m *Manager) RecordUnlinkedFile(path, reason string, sizeBytes int64) {
	if m == nil {
		return
	}
	m.enqueueTierEvent(tierEvent{kind: tierEventUnlinked, path: path, reason: reason, sizeBytes: sizeBytes})
}

func (m *Manager) enqueueTierEvent(ev tierEvent) {
	if m.tierEvents == nil {
		// A Manager built by hand without a queue, as some package tests do.
		// Nothing drains, so the event counts as dropped.
		m.tierEventsDropped.Add(1)
		return
	}
	if m.tierEvents.push(ev, tierEventQueueMax) {
		return
	}
	m.tierEventsDropped.Add(1)
	if m.tierEvents.warnOnce() {
		// Once per episode, not per event: a drainer that is not making
		// progress is one problem, however many reports pile up behind it.
		m.logger.Warn().
			Int("queued", tierEventQueueMax).
			Msg("Tier metadata event queue at its memory bound and the drainer is not keeping up — is the metadata database stalled? Dropping until it drains; the next tier scan will reconcile")
	}
}

// tierEventQueue is the drainer's inbox: an unbounded slice behind a mutex
// with a one-slot wake channel — the shape of the coordinator's pending local
// deletes. Producers append and return; the single consumer takes the whole
// slice at once.
type tierEventQueue struct {
	mu     sync.Mutex
	events []tierEvent
	warned bool
	wake   chan struct{}
}

func newTierEventQueue() *tierEventQueue {
	return &tierEventQueue{wake: make(chan struct{}, 1)}
}

// push appends ev unless the queue already holds max events; reports whether
// it was queued.
func (q *tierEventQueue) push(ev tierEvent, max int) bool {
	q.mu.Lock()
	if len(q.events) >= max {
		q.mu.Unlock()
		return false
	}
	q.events = append(q.events, ev)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}

// warnOnce reports true the first time it is called after the queue was last
// emptied, so a wedged drainer logs once per episode rather than per event.
func (q *tierEventQueue) warnOnce() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.warned {
		return false
	}
	q.warned = true
	return true
}

// take hands back everything queued and leaves the queue empty.
func (q *tierEventQueue) take() []tierEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	events := q.events
	q.events = nil
	q.warned = false
	return events
}

// pending is the number of queued events, for tests and the status endpoint.
func (q *tierEventQueue) pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.events)
}

// TierEventStats reports what the drainer has done, for the status endpoint
// and for tests: a non-zero dropped count with a flat applied count is what a
// wedged SQLite handle looks like from outside.
func (m *Manager) TierEventStats() (applied, dropped, failed int64) {
	if m == nil {
		return 0, 0, 0
	}
	return m.tierEventsApplied.Load(), m.tierEventsDropped.Load(), m.tierEventsFailed.Load()
}

// tierEventLoop is the single writer applying queued events.
//
// One wake parks until there is something to do, then everything queued is
// taken and applied as one batch — in chunks, each under its own deadline —
// before the tier cache is invalidated once per database/measurement the
// batch changed. Started by NewManager so the seam is live before Start, and
// joined by Stop before the shared SQLite handle can be closed.
func (m *Manager) tierEventLoop() {
	defer m.tierEventWG.Done()

	for {
		select {
		case <-m.tierEvents.wake:
			for {
				batch := m.tierEvents.take()
				if len(batch) == 0 {
					break
				}
				m.applyTierEventBatch(batch)
			}
		case <-m.tierEventStop:
			// Drain what is queued, within one budget, then stop. Stop is on
			// the shutdown timeout that every remaining hook and component
			// shares, so the drain writes rows but starts no network probe
			// (draining=true is read by the probe pre-pass and by
			// applyUnlinked), and what the budget does not cover is counted
			// dropped: the database is about to close, and the startup scan
			// on the next boot reconciles it.
			m.draining.Store(true)
			m.tierEventStopDeadline.Store(time.Now().Add(tierEventStopDrainTimeout).UnixNano())
			for {
				batch := m.tierEvents.take()
				if len(batch) == 0 {
					return
				}
				if time.Now().UnixNano() >= m.tierEventStopDeadline.Load() {
					m.tierEventsDropped.Add(int64(len(batch)))
					m.tierEventsProcessed.Add(int64(len(batch)))
					m.logger.Warn().
						Int("unapplied", len(batch)).
						Msg("Tier metadata events still queued when the shutdown drain budget ran out; the startup scan will reconcile")
					return
				}
				m.applyTierEventBatch(batch)
			}
		}
	}
}

// applyTierEventBatch applies one batch in chunks, invalidates the tier cache
// once per distinct database/measurement the batch changed, and drops the
// query layer's caches only if some measurement's SET of tiers changed.
func (m *Manager) applyTierEventBatch(batch []tierEvent) {
	if len(batch) == 0 {
		return
	}

	// Re-checked per batch rather than at startup: a license can expire while
	// the process runs, and RunMigrationCycle re-checks for the same reason.
	// NewManager refuses an unlicensed client outright, so in a real process
	// this only fires on a runtime expiry.
	//
	// Only a client that is present and says no blocks. The field is never
	// nil on a manager NewManager built, and leaving a nil client to mean
	// "unlicensed" would make this a silent no-op in the package's own tests,
	// which construct a Manager directly for exactly that reason.
	if m.licenseClient != nil && !m.licenseClient.CanUseTieredStorage() {
		// Counted, not silent: without this the status endpoint would report a
		// healthy drainer while every event was being discarded.
		m.tierEventsDropped.Add(int64(len(batch)))
		m.tierEventsProcessed.Add(int64(len(batch)))
		return
	}

	// Keyed by database/measurement only, not by tier: invalidateTierCache
	// takes no tier, so adding one would just bump the generation twice for a
	// batch that both registered a pulled file and flipped another cold in
	// the same measurement.
	touched := make(map[tierEventKey]struct{}, 4)
	// The tier set each measurement had before this batch's first write to
	// it, for the decision at the end. Read through the tier cache: every
	// writer that changes a set invalidates the entry (RecordFile, the cold
	// sync, DeleteFileInTier, this drainer at the end of each batch, the scan
	// at the end of its walk), so a cached entry is current up to this batch's
	// own writes — which the bookkeeping below accounts for. In steady state
	// that makes the snapshot a map lookup, not a SELECT per pulled file.
	before := make(map[tierEventKey]tierSetSnapshot, 4)
	// Measurements a write in this batch could have moved to a different set:
	// any unlink write, or a pulled file landing where no hot row was. A
	// pull into a measurement that already had a hot row cannot change the
	// set, and in steady state that is every event, so those skip the
	// after-read too.
	mayChange := make(map[tierEventKey]bool, 4)

	for start := 0; start < len(batch); start += tierEventChunk {
		end := min(start+tierEventChunk, len(batch))
		if unapplied := m.applyTierEventChunk(batch[start:end], touched, before, mayChange); unapplied > 0 {
			// The chunk ran out of its deadline: the SQLite handle is stalled,
			// the cold tier is, or the shutdown drain budget is spent. Each
			// remaining chunk would burn a full deadline against the same
			// stall, so the rest of the batch is given up and the next scan
			// reconciles it.
			rest := unapplied + (len(batch) - end)
			m.tierEventsFailed.Add(int64(rest))
			m.tierEventsProcessed.Add(int64(rest))
			m.logger.Warn().
				Int("unapplied", rest).
				Msg("Tier metadata batch abandoned after a chunk exceeded its deadline; the next tier scan will reconcile")
			break
		}
	}

	if len(touched) == 0 {
		return
	}
	for k := range touched {
		m.metadata.invalidateTierCache(k.database, k.measurement)
	}

	// The query layer's pruned-path and SQL-transform caches hold WHICH tiers
	// a measurement reads from — the thing a completed migration changes,
	// which is why that drops them (main.go wires notifyMigrationComplete to
	// QueryHandler.InvalidateCaches). A pulled file for a measurement that
	// already has hot rows, or a compaction unlink that leaves others, changes
	// nothing those caches hold — and on a replicating node that is every
	// batch in steady state, since every pulled file is a new path. Dropping
	// them per batch would disable both caches on every node for as long as
	// any peer is ingesting, and log two Info lines per batch doing it. So the
	// sets are compared, and a set that could not be read, before or after,
	// counts as changed. Without any notification on a genuine change, a node
	// that has just acquired its first hot row for a measurement it only
	// receives would keep serving a cached cold-only read for the cache TTL.
	changed := 0
	ctx, cancel := context.WithTimeout(context.Background(), tierEventStopDrainTimeout)
	defer cancel()
	for k := range touched {
		if !mayChange[k] {
			continue
		}
		prev, ok := before[k]
		if !ok || !prev.ok {
			changed++
			continue
		}
		after, err := m.metadata.readTierSet(ctx, k.database, k.measurement)
		if err != nil || !sameTierSet(prev.tiers, after) {
			changed++
		}
	}
	if changed > 0 {
		m.notifyMigrationComplete(changed, 0)
	}
}

// tierEventKey identifies a measurement for the drainer's cache bookkeeping.
type tierEventKey struct {
	database    string
	measurement string
}

// tierSetSnapshot is a measurement's tier set as read before a batch's first
// write to it; ok is false when the read failed.
type tierSetSnapshot struct {
	tiers map[Tier]bool
	ok    bool
}

func sameTierSet(a, b map[Tier]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for t := range a {
		if !b[t] {
			return false
		}
	}
	return true
}

// applyTierEventChunk applies one chunk under one deadline. It gathers the
// evidence the writes need first — a parse per event, the tier set of each
// measurement seen for the first time in this batch, and the cold-tier
// existence of every unlinked path whose row might flip, probed in parallel —
// and then writes sequentially, so the single SQLite connection never waits on
// the network. Returns how many of the chunk's events went unapplied because
// the deadline ran out; zero means the chunk completed.
func (m *Manager) applyTierEventChunk(chunk []tierEvent, touched map[tierEventKey]struct{}, before map[tierEventKey]tierSetSnapshot, mayChange map[tierEventKey]bool) int {
	budget := tierEventDrainTimeout
	if m.draining.Load() {
		// What is left of the whole drain's budget, not a fresh one per
		// chunk: Stop is on the shared shutdown timeout.
		budget = time.Until(time.Unix(0, m.tierEventStopDeadline.Load()))
		if budget <= 0 {
			return len(chunk)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	infos := make([]*FileMetadata, len(chunk))
	for i, ev := range chunk {
		info, ok := m.tierEventFileInfo(ev)
		if !ok {
			m.tierEventsProcessed.Add(1)
			continue
		}
		infos[i] = info
		k := tierEventKey{info.Database, info.Measurement}
		if _, seen := before[k]; !seen {
			tiers, err := m.metadata.GetTiersForMeasurement(ctx, k.database, k.measurement)
			before[k] = tierSetSnapshot{tiers: tiers, ok: err == nil}
		}
	}

	probes := m.probeColdForChunk(ctx, chunk, infos)

	for i, ev := range chunk {
		info := infos[i]
		if info == nil {
			continue
		}
		if ctx.Err() != nil {
			// Past the deadline the remaining writes would all fail and log.
			// Hand back what is left of this chunk; the caller counts it with
			// the rest of the batch it gives up.
			rest := 0
			for _, later := range infos[i:] {
				if later != nil {
					rest++
				}
			}
			return rest
		}

		var (
			wrote bool
			err   error
		)
		switch ev.kind {
		case tierEventPulled:
			wrote, err = m.applyPulled(ctx, info)
		case tierEventUnlinked:
			var probe *coldProbe
			if p, ok := probes[ev.path]; ok {
				probe = &p
			}
			wrote, err = m.applyUnlinked(ctx, info, ev.reason, probe)
		}

		switch {
		case err != nil:
			m.tierEventsFailed.Add(1)
			m.logger.Warn().Err(err).
				Str("path", ev.path).
				Msg("Failed to apply tier metadata event; the next tier scan will reconcile")
		case wrote:
			m.tierEventsApplied.Add(1)
			// Keyed after the write: applyUnlinked may have replaced the
			// path-derived pair with the row's own.
			k := tierEventKey{info.Database, info.Measurement}
			touched[k] = struct{}{}
			if prev, ok := before[k]; ev.kind != tierEventPulled || !ok || !prev.ok || !prev.tiers[TierHot] {
				mayChange[k] = true
			}
		}
		m.tierEventsProcessed.Add(1)
	}
	return 0
}

// reasonMayBeMigration reports whether an unlink with this reason is worth a
// cold-tier existence check: tiering's own reasons, and the puller's abandoned
// pull, whose real reason is unknown to it.
func reasonMayBeMigration(reason string) bool {
	return strings.HasPrefix(reason, manifestReasonPrefix) ||
		reason == unlinkReasonAbandonedPull ||
		reason == unlinkReasonSnapshotRemoved
}

// coldProbe is the answer of one cold-tier existence check.
type coldProbe struct {
	inCold bool
	err    error
}

// probeColdForChunk runs the cold-tier existence checks a chunk's unlinks
// will need, tierEventProbeParallelism at a time. Only an unlink with a
// tiering reason, on a node that can read cold and is not shutting down, for
// a path whose row is not already cold, costs a probe — the same conditions
// applyUnlinked applies, so the two agree on which paths needed one. Returns
// nil when nothing did.
func (m *Manager) probeColdForChunk(ctx context.Context, chunk []tierEvent, infos []*FileMetadata) map[string]coldProbe {
	cold := m.GetBackendForTier(TierCold)
	if cold == nil || !m.config.Cold.Enabled || m.draining.Load() {
		return nil
	}

	var paths []string
	seen := make(map[string]struct{})
	for i, ev := range chunk {
		if infos[i] == nil || ev.kind != tierEventUnlinked || !reasonMayBeMigration(ev.reason) {
			continue
		}
		if _, dup := seen[ev.path]; dup {
			continue
		}
		seen[ev.path] = struct{}{}
		if existing, err := m.metadata.GetFile(ctx, ev.path); err == nil && existing != nil && existing.Tier == TierCold {
			continue
		}
		paths = append(paths, ev.path)
	}
	if len(paths) == 0 {
		return nil
	}

	results := make(map[string]coldProbe, len(paths))
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, tierEventProbeParallelism)
	)
	for _, p := range paths {
		if ctx.Err() != nil || m.draining.Load() {
			// Past the deadline the write pass stops too, and the paths not
			// probed are among the ones it will not reach. Once Stop has been
			// signalled no further probe is started: an unprobed unlink
			// retires its hot row, which is the stop-drain behaviour anyway.
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(p string) {
			defer wg.Done()
			defer func() { <-sem }()
			inCold, err := cold.Exists(ctx, p)
			mu.Lock()
			results[p] = coldProbe{inCold: inCold, err: err}
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return results
}

// applyPulled registers a pulled file as hot, but only if it is still on this
// node's hot storage.
//
// The stat is not redundant with the puller's own checks. A pull worker can
// pass its in-transit manifest re-check and then be descheduled; the delete
// worker unlinks the same path and reports first; this event then arrives for
// a file that is no longer there. Inserting a hot row for it would make the
// measurement claim local data it does not have — and the stale row stands
// until the next tier scan.
func (m *Manager) applyPulled(ctx context.Context, info *FileMetadata) (bool, error) {
	if m.hotBackend != nil {
		n, err := m.hotBackend.StatFile(ctx, info.Path)
		if err != nil {
			return false, err
		}
		if n < 0 {
			// Gone between the pull and here. Not an error: the unlink that
			// removed it reported too, and that report is the authority.
			return false, nil
		}
	}
	return m.metadata.recordHotFileIfNotCold(ctx, info)
}

// applyUnlinked decides what the removal of this node's local copy means for
// the row, from evidence rather than from the reason string.
//
// The reason is a hint only, and must be: the operator manifest-delete
// endpoint passes whatever ?reason= it is given, so a reason that looks like a
// migration is not proof of one. Treating it as proof would insert a cold row
// for a path with no cold object — and nothing retires such a row, because
// orphan reconciliation only examines cold rows whose hot copy still exists
// and the cold sync reports a missing object rather than reverting the row.
// The measurement would then lose its hot glob: the very bug this file fixes.
//
// So a migration-shaped reason buys one existence check against the cold
// backend, and the check decides:
//
//   - the object is there — the file really did move to cold, and this node
//     can read it there. Flip (or insert) the cold row.
//   - it is not — the file is simply gone from this node. Retire the hot row.
//
// Every other reason retires the hot row with no check. Those are the
// high-volume ones, and the file is genuinely gone in each case: a compaction
// that consumed it (compaction:<jobID>), a retention or operator delete
// ("retention:<policyID>", "delete", "operator", anything an admin types) and
// the reconciler's orphan-manifest sweep ("reconcile-orphan-manifest"). The
// "delete" path rewrites files and can re-register a path, which is safe here
// because the delete worker re-checks the manifest before unlinking at all and
// skips a path that is back.
//
// There are three outcomes, not two: the cold check can also FAIL, and it
// then retires the hot row like the absent case — see the comment at that
// branch for why. Note the event is counted as applied in that case, because
// a row was written; the Warn is the signal that the cold tier was
// unreachable.
//
// probe is the chunk's pre-pass answer for this path when it ran one, and
// nil otherwise; a nil probe means a direct check here, unless the drainer is
// stopping.
//
// Returns whether a row was written.
func (m *Manager) applyUnlinked(ctx context.Context, info *FileMetadata, reason string, probe *coldProbe) (bool, error) {
	cold := m.GetBackendForTier(TierCold)
	if reasonMayBeMigration(reason) && cold != nil && m.config.Cold.Enabled {
		// A row that already says cold needs no probe and no write, and
		// skipping it here is what keeps the existence check off the
		// high-volume tiering reasons: the manifest sweep and orphan
		// reconciliation both act on paths this node has already recorded as
		// cold, and they arrive in chunks of up to a thousand. Only a path
		// whose row is missing or hot is worth a round trip. The read is
		// confined to this branch: for every other reason the tier-conditional
		// DELETE below already refuses a cold row, so a SELECT first would be
		// a second statement per compaction or retention unlink for nothing.
		if existing, err := m.metadata.GetFile(ctx, info.Path); err == nil && existing != nil && existing.Tier == TierCold {
			return false, nil
		}

		// Keyed on the backend that was actually constructed, not on
		// Cold.Enabled alone: a cold backend whose construction failed leaves
		// the feature enabled in config and this node unable to read cold.
		// (main.go assigns it through a typed local so a failed constructor
		// leaves a true nil here, not a typed one.)
		//
		// No NEW network probe during shutdown: retiring the hot row is the
		// same fallback the probe-failure branch below takes, and the
		// cold-tier metadata sync records the cold row on the next boot
		// either way. An answer the pre-pass already has is used regardless.
		var (
			inCold  bool
			err     error
			decided bool
		)
		switch {
		case probe != nil:
			inCold, err, decided = probe.inCold, probe.err, true
		case !m.draining.Load():
			inCold, err = cold.Exists(ctx, info.Path)
			decided = true
		}
		if decided {
			if err != nil {
				// Unreachable cold backend: this node cannot tell whether the
				// file moved or vanished, but it does know the local copy is
				// gone, so the one thing the row must not keep saying is "hot
				// here". Retire it and fall through. If the object really is
				// in cold, the cold-tier metadata sync records it on the next
				// cycle; the opposite choice — keeping the hot row — would
				// leave the row contradicting the disk with nothing but the
				// next scan to fix it.
				m.logger.Warn().Err(err).
					Str("path", info.Path).
					Msg("Could not check the cold tier for an unlinked file; retiring the hot row")
			} else if inCold {
				wrote, err := m.metadata.markFileCold(ctx, info)
				return wrote, err
			}
		}
	}

	wrote, database, measurement, err := m.metadata.retireHotRow(ctx, info.Path)
	if wrote {
		// retireHotRow read them off the row that existed; prefer those over
		// the path-derived pair in case an older row used another convention.
		info.Database = database
		info.Measurement = measurement
	}
	return wrote, err
}

// tierEventFileInfo turns a queued event into the row to write, using the same
// path parser and the same exclusions as the hot scan.
//
// Database and measurement come from the PATH, not from the cluster manifest
// entry — which carries its own copies — so that the row a replicated file
// produces names the same database and measurement the scan would name for
// that file. That matters because the scan is the other writer of these rows,
// and a disagreement would make each pass rewrite the other's work. (created_at
// does differ — the scan uses the object's mtime, this uses now — but neither
// writer updates it on a conflict, so whichever sees the path first wins and
// no pass rewrites it.)
//
// The two are not always the same: parseFilePath maps a spoke-namespaced
// {spoke}/{db}/{meas}/... to database=spoke, measurement=db, because that is
// the split the query layer produces for those paths, while the flush path and
// the manifest entry carry the spoke's own pre-namespaced names. There are
// three writers of tier_files and two conventions; this follows the scan's.
func (m *Manager) tierEventFileInfo(ev tierEvent) (*FileMetadata, bool) {
	if !strings.HasSuffix(ev.path, ".parquet") {
		return nil, false
	}
	// Reserved roots hold Arc's own state; _schema anchors are Parquet but
	// never tiered data. Not an error, same as the scan.
	if first, _, _ := strings.Cut(ev.path, "/"); storage.IsReservedRootDir(first) {
		return nil, false
	}

	info, err := m.parseFilePath(ev.path)
	if err != nil {
		m.logger.Debug().Err(err).
			Str("path", ev.path).
			Msg("Tier metadata event for an unparseable path, skipping")
		return nil, false
	}

	return &FileMetadata{
		Path:          ev.path,
		Database:      info.Database,
		Measurement:   info.Measurement,
		PartitionTime: info.PartitionTime,
		SizeBytes:     ev.sizeBytes,
		CreatedAt:     time.Now().UTC(),
	}, true
}
