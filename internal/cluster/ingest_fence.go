package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// ingestFence refuses live writes on a Pattern-1 writer that may no longer be
// the primary (#1207 S2).
//
// A primary cut off from the cluster used to keep acknowledging writes: ingest
// was never gated on being the primary, only /ready/write was. With automatic
// failover the cluster promotes another writer, and the isolated one's rows
// reach the others only once the partition heals — or never, if its disk dies
// first. A fenced writer answers 503 before anything is written, the WAL
// included, so the client retries against the primary.
//
// Two rules, on a writer in a cluster with a primary-election manager:
//   - designation (always): another writer is the designated primary in this
//     node's applied FSM. Needs no licence: it only reads the FSM, and covers a
//     manual hand-over as well as failover.
//   - contact (automatic failover, at least 3 voters, another writer on
//     record): this node is not the Raft leader and has not heard from it for
//     longer than fenceAfter, which is kept below the failover detection time.
//
// Leaving the contact fence needs both steady contact for exitHold AND a
// completed manifest barrier started after contact returned: a heartbeat
// carries no log entries, so contact alone does not mean this node has applied
// a promotion that happened while it was away.
type ingestFence struct {
	fenceAfter  time.Duration
	exitHold    time.Duration
	contactRule bool
	localID     string
	logger      zerolog.Logger

	// Inputs, functions so a test drives them.
	now          func() time.Time
	isWriter     func() bool
	voters       func() int
	otherWriters func() int
	designated   func() string
	isLeader     func() bool
	lastContact  func() time.Time
	// catchUp waits until this node's FSM has applied everything the leader
	// had committed. Run asynchronously, never under f.mu.
	catchUp func(ctx context.Context) error

	fenced atomic.Bool
	reason atomic.Value // string
	// refusal is the error a refused write carries: the reason text and its
	// class (contact or standby) stored together, so a write never pairs one
	// state's text with another's class (#1268).
	refusal atomic.Pointer[ingest.RefusalError]

	mu          sync.Mutex
	lostContact bool      // fenced for contact (as opposed to designation)
	freshSince  time.Time // contact steady since
	prevContact time.Time // last contact seen on the previous tick
	barrierGen  uint64    // bumped whenever contact is lost
	barrierOK   uint64    // generation whose barrier completed
	barrierRun  bool
	onChange    []func(fenced bool)
}

const (
	ingestFenceTick     = 250 * time.Millisecond
	ingestFenceExitHold = 2 * time.Second
	ingestFenceFloor    = 3 * time.Second
	ingestFenceBarrier  = 15 * time.Second
)

// ingestFenceAfter derives how long a writer may go without leader contact
// before it stops accepting writes: half of health_check_interval ×
// unhealthy_threshold. Automatic failover needs longer than that whole product
// — 3 intervals of missed heartbeats before the first failed check, then the
// failed checks, then the failover manager's own ticks, about 30 s at defaults
// — so the fence closes first. Never below three election timeouts (3 s at
// defaults), so an ordinary election does not fence the primary. An operator
// override is capped at the product.
func ingestFenceAfter(healthCheckSec, unhealthyThreshold, overrideMs int, electionTimeout time.Duration, logger zerolog.Logger) time.Duration {
	if healthCheckSec <= 0 {
		healthCheckSec = 5
	}
	if unhealthyThreshold <= 0 {
		unhealthyThreshold = 3
	}
	detection := time.Duration(healthCheckSec*unhealthyThreshold) * time.Second
	floor := 3 * electionTimeout
	if floor < ingestFenceFloor {
		floor = ingestFenceFloor
	}
	after := detection / 2
	if overrideMs > 0 {
		after = time.Duration(overrideMs) * time.Millisecond
		if after > detection {
			logger.Warn().Dur("requested", after).Dur("capped_at", detection).
				Msg("cluster.ingest_fence_after_ms exceeds the failover detection time; capped so an isolated primary stops accepting writes before a replacement is promoted")
			after = detection
		}
	}
	if after < floor {
		if overrideMs > 0 {
			logger.Warn().Dur("requested", after).Dur("raised_to", floor).
				Msg("cluster.ingest_fence_after_ms is below three election timeouts; raised so an ordinary election does not fence the primary")
		}
		after = floor
	}
	return after
}

// evaluate recomputes the fence. Called on a ticker, and once at start.
func (f *ingestFence) evaluate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	reason, contact := "", false
	if f.isWriter() {
		if d := f.designated(); d != "" && d != f.localID {
			reason = "another writer is the designated primary"
		}
		if f.contactRule && f.voters() >= 3 && f.otherWriters() > 0 {
			lc := f.lastContact()
			inContact := f.isLeader() || (!lc.IsZero() && now.Sub(lc) <= f.fenceAfter)
			// Steady contact keeps arriving: a leader refreshes followers far
			// faster than the tick, so a contact time that did not move since
			// the last tick is not steady.
			steady := f.isLeader() || (inContact && lc.After(f.prevContact))
			f.prevContact = lc
			switch {
			case !inContact:
				if !f.lostContact {
					f.barrierGen++
				}
				f.lostContact = true
				f.freshSince = time.Time{}
				reason, contact = fmt.Sprintf("no contact with the cluster leader for over %s", f.fenceAfter), true
			case f.lostContact:
				if !steady {
					f.freshSince = time.Time{}
				} else if f.freshSince.IsZero() {
					f.freshSince = now
				}
				caughtUp := f.barrierOK == f.barrierGen
				if !caughtUp {
					f.startBarrierLocked()
					caughtUp = f.barrierOK == f.barrierGen
				}
				if f.freshSince.IsZero() || now.Sub(f.freshSince) < f.exitHold || !caughtUp {
					if reason == "" {
						reason, contact = "regaining contact with the cluster leader", true
					}
				} else {
					f.lostContact = false
				}
			}
		}
	}
	f.setLocked(reason, contact)
}

// startBarrierLocked runs the catch-up barrier for the current generation in
// the background. A barrier that completes for an older generation (contact
// was lost again meanwhile) does not count.
func (f *ingestFence) startBarrierLocked() {
	if f.catchUp == nil {
		f.barrierOK = f.barrierGen
		return
	}
	if f.barrierRun {
		return
	}
	f.barrierRun = true
	gen := f.barrierGen
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), ingestFenceBarrier)
		err := f.catchUp(ctx)
		cancel()
		f.mu.Lock()
		f.barrierRun = false
		if err == nil && gen == f.barrierGen {
			f.barrierOK = gen
		}
		f.mu.Unlock()
	}()
}

func (f *ingestFence) setLocked(reason string, contact bool) {
	was := f.fenced.Load()
	prev, _ := f.reason.Load().(string)
	now := reason != ""
	f.reason.Store(reason)
	class := ingest.RefusalStandby
	if contact {
		class = ingest.RefusalContact
	}
	f.refusal.Store(&ingest.RefusalError{Reason: class, Text: reason})
	f.fenced.Store(now)
	// The alert gauge is for contact loss only: a standby writer refusing
	// writes is its normal state, not an incident.
	metrics.Get().SetIngestFenced(contact)
	if was == now && (!now || reason == prev) {
		return
	}
	switch {
	case now && contact:
		f.logger.Warn().Str("reason", reason).Dur("fence_after", f.fenceAfter).
			Msg("Writes refused: no steady contact with the cluster leader; clients get 503 and retry against the primary")
	case now:
		f.logger.Info().Str("reason", reason).Msg("Writes refused on this writer: another writer is the designated primary")
	default:
		f.logger.Info().Msg("Writes accepted: this node is the primary writer and in contact with the cluster leader")
	}
	if was != now {
		for _, fn := range f.onChange {
			go fn(now)
		}
	}
}

// inSteadyContact reports whether this writer is in contact with the Raft
// leader now and is not recovering from a lost contact (barrier and exit hold
// done): its view of the designation can be trusted to forward writes (#1270).
// Stricter than evaluate (no voters/other-writers condition: a forward is
// never needed for safety, so a doubt keeps the local 503). Without the
// contact rule (no automatic failover) contact is not tracked and the FSM's
// view is used as is; the primary re-checks its own gate on arrival.
func (f *ingestFence) inSteadyContact() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lostContact {
		return false
	}
	if !f.contactRule || f.isLeader() {
		return true
	}
	lc := f.lastContact()
	return !lc.IsZero() && f.now().Sub(lc) <= f.fenceAfter
}

// WriteForwardTarget names the primary writer a write that reaches this
// standby writer is forwarded to, instead of refusing it (#1270). It answers
// only when this node may trust its view: a Pattern 1 writer in steady contact
// with the leader, not held by a stream takeover gate (claimed, announcing, a
// DELETE acknowledgement) — those keep their 503 — and the designated primary
// is another node, healthy, with an API address. Never derived from a refusal
// error, and counts nothing.
func (c *Coordinator) WriteForwardTarget() (*Node, bool) {
	if c.cfg.SharedStorageMode || c.raftNode == nil || c.localNode == nil || c.localNode.Role != RoleWriter {
		return nil, false
	}
	// The designation first: on the primary (every write it takes) this is
	// the only check that runs.
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return nil, false
	}
	d := fsm.PrimaryWriterIDFast()
	if d == "" || d == c.localNode.ID {
		return nil, false
	}
	if c.claimedExit.Load() || c.takeoverGateClosed() || c.deleteAckOwed() {
		return nil, false
	}
	f := c.ingestFence.Load()
	if f == nil || !f.inSteadyContact() {
		return nil, false
	}
	n, ok := c.registry.Get(d)
	if !ok || n.Role != RoleWriter || !n.IsHealthy() || n.APIAddress == "" {
		return nil, false
	}
	return n, true
}

// gate is the ArrowBuffer ingest gate.
func (f *ingestFence) gate() error {
	if !f.fenced.Load() {
		return nil
	}
	r := f.refusal.Load()
	if r == nil { // fenced is stored after refusal; not reachable
		return ingest.ErrIngestFenced
	}
	return fmt.Errorf("%w: %w", ingest.ErrIngestFenced, r)
}

// Refusal reasons, the label of arc_cluster_ingest_fenced_writes_total and
// the key refusals are logged by (#1268); defined in ingest, which the API and
// MQTT share. A standby writer refusing is its normal state; contact, claimed
// and delete_ack on the primary are not.
const (
	RefusalContact       = ingest.RefusalContact
	RefusalStandby       = ingest.RefusalStandby
	RefusalClaimed       = ingest.RefusalClaimed
	RefusalAnnouncing    = ingest.RefusalAnnouncing
	RefusalNotDesignated = ingest.RefusalNotDesignated
	RefusalDeleteAck     = ingest.RefusalDeleteAck
)

// IngestRefusalReason is ingest.RefusalReason.
func IngestRefusalReason(err error) string { return ingest.RefusalReason(err) }

// startIngestFenceLocked builds and runs the fence where a primary is elected:
// Raft with local storage. The contact rule also needs automatic failover. The
// caller holds c.mu: it runs from Start, which holds it throughout.
func (c *Coordinator) startIngestFenceLocked() {
	if c.raftNode == nil || c.raftFSM == nil || c.cfg.SharedStorageMode || c.writerFailoverMgr == nil {
		return
	}
	f := &ingestFence{
		fenceAfter: ingestFenceAfter(c.cfg.HealthCheckInterval, c.cfg.UnhealthyThreshold, c.cfg.IngestFenceAfterMs,
			c.raftNode.ElectionTimeout(), c.logger),
		exitHold:    ingestFenceExitHold,
		contactRule: c.autoFailover,
		localID:     c.localNode.ID,
		logger:      c.logger.With().Str("component", "ingest-fence").Logger(),
		now:         time.Now,
		isWriter:    func() bool { return c.localNode.Role == RoleWriter },
		voters:      c.raftNode.VoterCount,
		otherWriters: func() int {
			n := 0
			for _, w := range c.raftFSM.GetNodesByRole(string(RoleWriter)) {
				if w.ID != c.localNode.ID {
					n++
				}
			}
			return n
		},
		designated:  c.designatedPrimaryWriterID,
		isLeader:    c.raftNode.IsLeader,
		lastContact: c.raftNode.LastContact,
		catchUp:     c.SyncManifest,
	}
	// Listeners hear the combined state (fence or takeover gate), in order.
	if len(c.ingestFenceListeners) > 0 {
		f.onChange = append(f.onChange, func(bool) { c.ingestPauseChanged() })
	}
	// Decide before publishing, so the fence never starts open on a node that
	// should be refusing. evaluate takes no coordinator lock.
	f.evaluate()
	c.ingestFence.Store(f)
	ctx := c.ctx
	f.logger.Info().Dur("fence_after", f.fenceAfter).Bool("contact_rule", f.contactRule).Msg("Ingest fencing enabled")
	go func() {
		t := time.NewTicker(ingestFenceTick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				f.evaluate()
			}
		}
	}()
}

// IngestGate returns the check the ArrowBuffer runs before every live write.
// It admits everything until (and unless) the fence starts. Lock-free.
func (c *Coordinator) IngestGate() func() error {
	return func() error {
		err := c.ingestRefusal()
		if err != nil {
			metrics.Get().IncIngestFencedWrites(IngestRefusalReason(err))
		}
		return err
	}
}

// ingestRefusal is IngestGate's check, uncounted.
func (c *Coordinator) ingestRefusal() error {
	if c.claimedExit.Load() {
		return fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errTakeoverClaimed)
	}
	if c.takeoverGateClosed() {
		return fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errTakeoverAnnouncing)
	}
	if c.takeoverDesignationRefuses() {
		return fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errNotDesignated)
	}
	if c.deleteAckOwed() {
		return fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errDeleteAckPending)
	}
	if f := c.ingestFence.Load(); f != nil {
		return f.gate()
	}
	return nil
}

// OnIngestFenceChange registers fn to run (in its own goroutine) whenever the
// fence closes or opens. Register before Start.
func (c *Coordinator) OnIngestFenceChange(fn func(fenced bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ingestFenceListeners = append(c.ingestFenceListeners, fn)
}

// ingestFenced reports whether the fence currently refuses writes.
func (c *Coordinator) ingestFenced() bool {
	f := c.ingestFence.Load()
	return f != nil && f.fenced.Load()
}
