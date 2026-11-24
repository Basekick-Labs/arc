package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Read fences for staged compaction on a shared object store, coordinator
// side (#1852). See raft/compaction_fences.go.
//
// The flag follows the compaction commit pattern: every build advertises the
// capability, the leader enables it once compaction commit is on and every
// node and Raft server advertises it, and a join without it is refused once
// it is on.

const (
	compactionFencesEnableEvery = 10 * time.Second
	// readFenceTick: how often a node looks for jobs above its fence.
	readFenceTick = 500 * time.Millisecond
	// readFenceWait bounds one fence attempt (its wait for reads).
	readFenceWait = 15 * time.Minute
	// silentNodeAfter: a node the leader has not heard from for this long
	// cannot be reading (it lost its read lease 5 s after its last contact,
	// which cancelled its reads), and gets a waived fence.
	silentNodeAfter = 30 * time.Second
	// silentNodeTick: how often the leader looks for silent nodes.
	silentNodeTick = 5 * time.Second
)

// ReadFenceFunc drops this node's caches that may name outputs' files or
// remember their directories (when outputs is not empty), waits for the
// reads that entered before it was called, and drops the caches again. It
// runs off the apply goroutine and may block up to ctx.
type ReadFenceFunc func(ctx context.Context, outputs []raft.FileEntry) error

// compactionFencesGate serializes the join gate with the enable proposal.
type compactionFencesGate struct {
	mu       sync.Mutex
	inFlight bool
	blockers atomic.Pointer[[]string]
}

// compactionFencesJoinGate is compactionCommitJoinGate for the read fences.
func (c *Coordinator) compactionFencesJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.fencesGate
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityCompactionFences) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.CompactionFencesEnabled()
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its compaction read fence state; retry the join", nil
		}
		enabled = fsm != nil && fsm.CompactionFencesEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "compaction read fences are enabled in this cluster: the node must run a release that supports them", nil
}

func (c *Coordinator) maybeEnableCompactionFences() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || fsm.CompactionFencesEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.fencesGate
	if !fsm.CompactionCommitEnabled() {
		off := []string{"(compaction commit is off)"}
		g.blockers.Store(&off)
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityCompactionFences)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableCompactionFencesPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableCompactionFences, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing compaction read fences failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Compaction read fences enabled: every node supports them")
}

// SetReadFenceFunc sets this node's part of a fence. Until it is set the
// node fences nothing, and committed object jobs wait for it.
func (c *Coordinator) SetReadFenceFunc(fn ReadFenceFunc) {
	if c != nil && fn != nil {
		c.fenceFn.Store(&fn)
	}
}

// startCompactionFences starts the enable loop, this node's fence loop and,
// for whichever node leads, the silent-node waivers.
func (c *Coordinator) startCompactionFences(ctx context.Context) {
	if c.raftNode == nil {
		return
	}
	go func() {
		t := time.NewTicker(compactionFencesEnableEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.maybeEnableCompactionFences()
			}
		}
	}()
	go c.readFenceLoop(ctx)
	sn := &silentNodes{since: map[string]silentPeer{}}
	if fsm := c.raftNode.FSM(); fsm != nil {
		self := c.LocalNodeID()
		fsm.SetReadFenceWaivedCallback(func(node string) {
			if node == self {
				// On the apply goroutine: the lease drop cancels reads and
				// is quick.
				c.readLease.drop("the cluster leader has not heard from this node and waived its read fence; it syncs again before it reads")
			}
		})
	}
	stop := c.raftNode.HeartbeatFailures(sn.observe)
	go func() {
		<-ctx.Done()
		stop()
	}()
	go c.silentNodeLoop(ctx, sn)
}

func (c *Coordinator) readFenceLoop(ctx context.Context) {
	t := time.NewTicker(readFenceTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := c.readFencePass(ctx); err != nil {
			c.logger.Warn().Err(err).Msg("Compaction read fence: this node's fence did not complete; retrying")
		}
	}
}

// readFencePass raises this node's fence when an Acked job's phase is above
// it: the caches the phases need dropped, the reads from before the fence's
// index ended. Only while this node holds the read lease.
func (c *Coordinator) readFencePass(ctx context.Context) error {
	p := c.fenceFn.Load()
	if p == nil || c.raftNode == nil {
		return nil
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.CompactionFencesEnabled() {
		return nil
	}
	self := c.LocalNodeID()
	mine := fsm.ReadFence(self)
	if fsm.FenceTop() <= mine {
		return nil
	}
	if ok, _ := c.ReadReady(); !ok {
		return nil
	}
	// Everything at or below w is applied here: a read that enters the
	// gate from now on sees it.
	w := fsm.AppliedDone()
	if w <= mine {
		return nil
	}
	wctx, cancel := context.WithTimeout(ctx, readFenceWait)
	defer cancel()
	if err := (*p)(wctx, fsm.FenceCacheOutputs(mine, w)); err != nil {
		return err
	}
	// A node that lost the lease meanwhile may have been behind.
	if ok, _ := c.ReadReady(); !ok {
		return nil
	}
	raw, err := json.Marshal(raft.ReadFencePayload{Node: self, Index: w})
	if err != nil {
		return err
	}
	return c.applyOrForward(wctx, &raft.Command{Type: raft.CommandReadFence, Payload: raw})
}

// silentNodes is the leader's record of peers its heartbeats fail to reach:
// the last contact the latest failure reported, and when that failure was
// observed. A resumption, or the peer leaving or joining the configuration,
// clears it.
type silentNodes struct {
	mu    sync.Mutex
	since map[string]silentPeer
}

type silentPeer struct {
	lastContact time.Time // the peer's last contact, as the latest failure has it
	observed    time.Time // when that failure was observed
}

// silentObservedWithin: a failure observed longer ago than this is not
// evidence the peer is silent now (hashicorp/raft backs failed heartbeats
// off to about 41 s, and reports no resumption after a peer re-joins).
const silentObservedWithin = 60 * time.Second

func (s *silentNodes) observe(peer string, failed bool, lastContact time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !failed {
		delete(s.since, peer)
		return
	}
	now := time.Now()
	if lastContact.IsZero() {
		// Never heard from by this leader: from the first failure on.
		if prev, ok := s.since[peer]; ok {
			lastContact = prev.lastContact
		} else {
			lastContact = now
		}
	}
	s.since[peer] = silentPeer{lastContact: lastContact, observed: now}
}

// silent reports the peers whose latest failure is recent and reports a
// last contact at least after ago.
func (s *silentNodes) silent(now time.Time, after time.Duration) map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]bool{}
	for peer, p := range s.since {
		if now.Sub(p.observed) <= silentObservedWithin && now.Sub(p.lastContact) >= after {
			out[peer] = true
		}
	}
	return out
}

func (s *silentNodes) reset() {
	s.mu.Lock()
	s.since = map[string]silentPeer{}
	s.mu.Unlock()
}

// silentNodeLoop, on the leader: a node silent for silentNodeAfter (its
// heartbeats failing, or in the node table but not the Raft configuration)
// gets a waived fence at the leader's applied index, so jobs do not wait for
// it.
func (c *Coordinator) silentNodeLoop(ctx context.Context, sn *silentNodes) {
	t := time.NewTicker(silentNodeTick)
	defer t.Stop()
	wasLeader := false
	outOfConfig := map[string]time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		leader := c.raftNode.IsLeader()
		if !leader {
			if wasLeader {
				sn.reset()
				outOfConfig = map[string]time.Time{}
			}
			wasLeader = false
			continue
		}
		if !wasLeader {
			// Observations from before this leadership are not ours.
			sn.reset()
			outOfConfig = map[string]time.Time{}
		}
		wasLeader = true
		fsm := c.raftNode.FSM()
		if fsm == nil || !fsm.CompactionFencesEnabled() {
			continue
		}
		top := fsm.FenceTop()
		if top == 0 {
			continue
		}
		now := time.Now()
		silent := sn.silent(now, silentNodeAfter)
		if ids, err := c.raftNode.ConfigurationServerIDs(); err == nil {
			inConfig := map[string]bool{}
			for _, id := range ids {
				inConfig[id] = true
			}
			for _, n := range fsm.GetAllNodes() {
				if inConfig[n.ID] {
					delete(outOfConfig, n.ID)
					continue
				}
				first, ok := outOfConfig[n.ID]
				if !ok {
					outOfConfig[n.ID] = now
					continue
				}
				if now.Sub(first) >= silentNodeAfter {
					silent[n.ID] = true
				}
			}
		}
		self := c.LocalNodeID()
		for peer := range silent {
			if peer == self || fsm.ReadFence(peer) >= top {
				continue
			}
			raw, err := json.Marshal(raft.ReadFencePayload{Node: peer, Index: fsm.AppliedDone(), Waived: true})
			if err != nil {
				continue
			}
			if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandReadFence, Payload: raw}, 5*time.Second); err != nil {
				c.logger.Warn().Err(err).Str("node", peer).Msg("Waiving a silent node's read fence failed; retrying")
			}
		}
	}
}

// CompactionFencedOwns reports whether a compaction being finished under
// read fences owns path, in this node's record.
func (c *Coordinator) CompactionFencedOwns(path string) bool {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return false
	}
	return c.raftNode.FSM().CompactionFencedOwns(path)
}
