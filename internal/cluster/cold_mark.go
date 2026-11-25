package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Cold mark, coordinator side (#1552). See raft/cold_mark.go.
//
// The flag follows the compaction commit pattern: every build that maintains
// the mark advertises the capability, the leader enables it once every node
// in the FSM table and every Raft server advertised it, and from then on (or
// while an enable proposal may still commit) a join without the capability
// is refused before the node enters the Raft configuration. As with the other
// capability flags, a member is not removed when it comes back without it:
// its join is refused and logged at Error.
//
// The fence: ColdWrite holds coldWriteMu shared across its decision and the
// write. A node acks Enable only after taking it exclusively once Enable is
// applied here (once; a sticky flag makes every later write raise the mark),
// so no write that began unmarked can still be running.
//
// The join gate's barrier ("an enable proposal may still commit") holds only
// on the leader that proposed it. A member that later runs an older build is
// covered by the liveness check instead (raft/cold_mark.go): it stops
// confirming, and readers stop trusting the mark.

const coldMarkEvery = 10 * time.Second

// coldMarkGate serializes the join gate with the enable proposal, as
// compactionCommitGate does.
type coldMarkGate struct {
	mu       sync.Mutex
	inFlight bool
	blockers atomic.Pointer[[]string]
}

// coldMarkJoinGate is compactionCommitJoinGate for the cold mark.
func (c *Coordinator) coldMarkJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.coldMarkGate
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityColdMark) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.ColdMarkStatus().Enabled
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its cold mark state; retry the join", nil
		}
		enabled = fsm != nil && fsm.ColdMarkStatus().Enabled
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "the cold mark is enabled in this cluster: the node must run a release that supports it", nil
}

// coldMarkLoop enables the mark from the leader once every node supports it,
// and acks it from every node once its unmarked cold writes drained.
func (c *Coordinator) coldMarkLoop(ctx context.Context) {
	t := time.NewTicker(coldMarkEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.maybeEnableColdMark()
			c.maybeAckColdMark(ctx)
			c.maybeColdMarkAlive(ctx, time.Now())
		}
	}
}

func (c *Coordinator) maybeEnableColdMark() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || fsm.ColdMarkStatus().Enabled || !c.raftNode.IsLeader() {
		return
	}
	g := &c.coldMarkGate
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityColdMark)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableColdMarkPayload{Nodes: nodes})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableColdMark, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing the cold mark failed; retrying")
	}
}

// maybeAckColdMark acks once Enable is applied here: taking coldWriteMu
// exclusively waits out every cold write that may have begun unmarked.
func (c *Coordinator) maybeAckColdMark(ctx context.Context) {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.ColdMarkStatus().Enabled {
		return
	}
	c.coldMarkSeenEnabled.Store(true)
	if fsm.ColdMarkAcked(c.localNode.ID) {
		return
	}
	if !c.coldMarkDrained.Load() {
		// Once: every later ColdWrite on this node sees the sticky flag and
		// raises the mark, so there is nothing more to drain.
		c.coldWriteMu.Lock()
		c.coldWriteMu.Unlock() //nolint:staticcheck // the drain is the point
		c.coldMarkDrained.Store(true)
	}
	raw, err := json.Marshal(raft.ColdMarkAckPayload{NodeID: c.localNode.ID})
	if err != nil {
		return
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandColdMarkAck, Payload: raw}); err != nil {
		c.logger.Warn().Err(err).Msg("Acking the cold mark failed; retrying")
	}
}

// maybeColdMarkAlive proposes Alive for this node once the mark is enabled
// and its last Alive in the FSM is raft.ColdMarkAliveEvery old. Read from the
// FSM, not a local timer, so a restart resumes on time.
func (c *Coordinator) maybeColdMarkAlive(ctx context.Context, now time.Time) {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.ColdMarkStatus().Enabled {
		return
	}
	// A last Alive in the future (this clock was ahead and was corrected)
	// is replaced at once: readers treat it as not alive.
	if last := fsm.ColdMarkLastAlive(c.localNode.ID); !last.After(now) && now.Sub(last) < raft.ColdMarkAliveEvery {
		return
	}
	raw, err := json.Marshal(raft.ColdMarkAlivePayload{NodeID: c.localNode.ID, At: now.UTC()})
	if err != nil {
		return
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandColdMarkAlive, Payload: raw}); err != nil {
		c.logger.Warn().Err(err).Msg("Confirming the cold mark failed; retrying")
	}
}

// ColdMarkTrusted reports whether a reader may use the cold mark at now:
// enabled, fenced, seeded from one cold store, and every node in the table
// and every Raft server confirmed it maintains the mark within maxAge
// (raft.ColdMarkAliveMaxAge in production).
//
// localIdentity is this node's cold store identity (ColdMarkIdentity).
func (c *Coordinator) ColdMarkTrusted(now time.Time, maxAge time.Duration, localIdentity string) (bool, string) {
	if c == nil || c.raftNode == nil {
		return false, "no cluster"
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false, "no cluster state"
	}
	servers, err := c.raftNode.ConfigurationServerIDs()
	if err != nil {
		return false, "raft configuration unavailable"
	}
	return fsm.ColdMarkTrustedFor(now, maxAge, servers, localIdentity)
}

// ColdMarkBackend is ClusterFSM.ColdMarkBackend; all zero without a cluster.
func (c *Coordinator) ColdMarkBackend() (backend string, seeded, conflict bool) {
	if c == nil || c.raftNode == nil {
		return "", false, false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return "", false, false
	}
	return fsm.ColdMarkBackend()
}

// ColdMarkDay returns scope's mark ("database/measurement") as days since the
// epoch and the proposer time of its last raise; has is false when no cold
// object of the scope was ever marked.
func (c *Coordinator) ColdMarkDay(scope string) (day int32, raisedAt time.Time, has bool) {
	if c == nil || c.raftNode == nil {
		return 0, time.Time{}, false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return 0, time.Time{}, false
	}
	day, has, _ = fsm.ColdMark(scope)
	return day, fsm.ColdMarkRaisedAt(scope), has
}

// ErrColdMarkNotEnabled: a reset of a cold mark this cluster never enabled.
var ErrColdMarkNotEnabled = errors.New("the cold mark is not enabled in this cluster, so there is nothing to reset")

// coldMarkReadBarrierTimeout bounds the catch-up check a read makes before it
// trusts the mark; only reads whose cold listing failed make it.
const coldMarkReadBarrierTimeout = 2 * time.Second

// ColdMarkReadBarrier returns once this node's FSM has applied everything the
// leader had committed when it was called, so a mark read afterwards is not
// older than the cluster's (a lagging reader would otherwise trust a mark
// below a cold write already committed).
func (c *Coordinator) ColdMarkReadBarrier(ctx context.Context) error {
	return c.readBarrier(ctx, coldMarkReadBarrierTimeout)
}

// ResetColdMark clears the mark's seed, identity and backend conflict (an
// admin action, after fixing every node's cold tier config). Marks are kept;
// the first complete cold listing after it, on any node whose tiering is not
// role-gated, seeds again.
func (c *Coordinator) ResetColdMark(ctx context.Context, by, reason string) error {
	if c == nil || c.raftNode == nil {
		return errors.New("clustering is not enabled on this node")
	}
	if fsm := c.raftNode.FSM(); fsm == nil || !fsm.ColdMarkStatus().Enabled {
		return ErrColdMarkNotEnabled
	}
	raw, err := json.Marshal(raft.ColdMarkResetPayload{By: by, Reason: reason})
	if err != nil {
		return err
	}
	return c.applyOrForward(ctx, &raft.Command{Type: raft.CommandColdMarkReset, Payload: raw})
}

// SetColdMarkIdentity wires this node's cold store identity (tiering's
// ColdBackendIdentity); "" while it has no cold tier.
func (c *Coordinator) SetColdMarkIdentity(fn func() string) {
	if c != nil {
		c.coldMarkIdentity.Store(&fn)
	}
}

// ColdMarkIdentity is this node's cold store identity, "" when unknown.
func (c *Coordinator) ColdMarkIdentity() string {
	if c == nil {
		return ""
	}
	if fn := c.coldMarkIdentity.Load(); fn != nil && *fn != nil {
		return (*fn)()
	}
	return ""
}

// coldMarkRequired reports whether a cold write must raise the mark first
// (with the sticky flag, see ColdWrite):
// the mark is enabled, or every node in the table supports it (Enable can
// then commit while the write runs, and only a node that was unmarked-capable
// before Enable is drained by the fence). Vacuously true on an empty table.
func coldMarkRequired(fsm *raft.ClusterFSM) bool {
	if fsm.ColdMarkStatus().Enabled {
		return true
	}
	for _, n := range fsm.GetAllNodes() {
		if !hasCapability(n.Capabilities, raft.CapabilityColdMark) {
			return false
		}
	}
	return true
}

// ColdWrite raises the cold mark of each scope to its day before running
// write, which writes cold objects of those days. When the mark is required
// and cannot be raised, write does not run. Scopes whose mark already covers
// the day are not proposed again.
func (c *Coordinator) ColdWrite(ctx context.Context, marks map[string]int32, write func() error) error {
	if c == nil || c.raftNode == nil {
		return write()
	}
	c.coldWriteMu.RLock()
	defer c.coldWriteMu.RUnlock()
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return write()
	}
	// Sticky: once this node has seen Enable applied, a restarted FSM still
	// catching up must not make the mark optional again.
	if fsm.ColdMarkStatus().Enabled {
		c.coldMarkSeenEnabled.Store(true)
	}
	if !c.coldMarkSeenEnabled.Load() && !coldMarkRequired(fsm) {
		// A node on an older build may still apply the log: proposing a
		// command it does not know would only diverge, and the fence drains
		// this write before any seed.
		return write()
	}
	if err := c.raiseColdMarks(ctx, fsm, marks); err != nil {
		return fmt.Errorf("cold mark not raised, cold write not started: %w", err)
	}
	return write()
}

func (c *Coordinator) raiseColdMarks(ctx context.Context, fsm *raft.ClusterFSM, marks map[string]int32) error {
	need := make(map[string]int32, len(marks))
	for scope, day := range marks {
		if !fsm.ColdMarkCovers(scope, day) {
			need[scope] = day
		}
	}
	return c.proposeColdMarkChunks(ctx, need, func(chunk map[string]int32, _ bool) (*raft.Command, error) {
		if len(chunk) == 0 {
			return nil, nil
		}
		raw, err := json.Marshal(raft.ColdMarkAddPayload{Marks: chunk, At: time.Now().UTC()})
		return &raft.Command{Type: raft.CommandColdMarkAdd, Payload: raw}, err
	})
}

// proposeColdMarkChunks proposes marks in sorted chunks of at most 1000; last
// is set on the final chunk (also when marks is empty and build accepts it).
func (c *Coordinator) proposeColdMarkChunks(ctx context.Context, marks map[string]int32, build func(chunk map[string]int32, last bool) (*raft.Command, error)) error {
	scopes := make([]string, 0, len(marks))
	for s := range marks {
		// A scope the FSM refuses would refuse the whole chunk; no query
		// reads it.
		if raft.ValidColdMarkScope(s) {
			scopes = append(scopes, s)
		}
	}
	sort.Strings(scopes)
	if len(scopes) == 0 {
		// Nothing to raise; a seed still needs its final chunk.
		cmd, err := build(nil, true)
		if err != nil || cmd == nil {
			return err
		}
		return c.applyOrForward(ctx, cmd)
	}
	const chunkSize = 1000
	for start := 0; start < len(scopes); start += chunkSize {
		end := min(start+chunkSize, len(scopes))
		chunk := make(map[string]int32, end-start)
		for _, s := range scopes[start:end] {
			chunk[s] = marks[s]
		}
		cmd, err := build(chunk, end == len(scopes))
		if err != nil {
			return err
		}
		if cmd == nil {
			continue
		}
		if err := c.applyOrForward(ctx, cmd); err != nil {
			return err
		}
	}
	return nil
}

// ColdMarkSeedState reports whether the mark is fenced (a listing started
// now may seed it) and seeded.
func (c *Coordinator) ColdMarkSeedState() (fenced, seeded bool) {
	if c == nil || c.raftNode == nil {
		return false, false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false, false
	}
	st := fsm.ColdMarkStatus()
	return st.Fenced, st.Seeded
}

// SeedColdMarks folds a complete cold listing, started after the fence, into
// the marks and marks them seeded from backend.
func (c *Coordinator) SeedColdMarks(ctx context.Context, marks map[string]int32, backend string) error {
	if c == nil || c.raftNode == nil {
		return errors.New("seed cold marks: no raft")
	}
	if backend == "" {
		return errors.New("seed cold marks: no backend identity")
	}
	build := func(chunk map[string]int32, last bool) (*raft.Command, error) {
		p := raft.ColdMarkSeedPayload{Marks: chunk, At: time.Now().UTC()}
		if last {
			p.Final, p.Backend = true, backend
		}
		raw, err := json.Marshal(p)
		return &raft.Command{Type: raft.CommandColdMarkSeed, Payload: raw}, err
	}
	return c.proposeColdMarkChunks(ctx, marks, build)
}

// ColdMarkStatus is the /cluster status view.
func (c *Coordinator) ColdMarkStatus() (st raft.ColdMarkStatus, blocking []string) {
	if c == nil || c.raftNode == nil {
		return st, nil
	}
	if fsm := c.raftNode.FSM(); fsm != nil {
		st = fsm.ColdMarkStatus()
	}
	if b := c.coldMarkGate.blockers.Load(); b != nil && !st.Enabled {
		blocking = append(blocking, *b...)
	}
	return st, blocking
}
