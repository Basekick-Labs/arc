package cluster

import (
	"sync"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// standbyEvictPolicy decides which instances the standby store may evict
// at its cap while stream takeover is on in the cluster (#1207 S4b1c): a copy
// a claim could still need is kept. Dropping new batches is recoverable;
// losing the only copy of a dead primary's acknowledged rows is not.
type standbyEvictPolicy struct {
	c *Coordinator

	mu     sync.Mutex
	gen    uint64
	built  bool
	states map[uint64]raft.InstanceState
}

// active reports whether the policy applies: the flag is on in this node's
// manifest. Off, the store keeps its own rule (evict replaced instances).
func (p *standbyEvictPolicy) active() bool {
	fsm := p.c.raftNode.FSM()
	return fsm != nil && fsm.StreamClaimsEnabled()
}

func (p *standbyEvictPolicy) snapshot(fsm *raft.ClusterFSM) map[uint64]raft.InstanceState {
	p.mu.Lock()
	defer p.mu.Unlock()
	if gen := fsm.TakeoverGeneration(); !p.built || gen != p.gen {
		p.states, p.gen, p.built = fsm.TakeoverInstanceStates(), gen, true
	}
	return p.states
}

func (p *standbyEvictPolicy) tier(in originstream.InstanceInfo) int {
	fsm := p.c.raftNode.FSM()
	if fsm == nil {
		return originstream.EvictNever
	}
	st, known := p.snapshot(fsm)[in.Instance]
	switch {
	case known && st.Claimed && !st.ClaimDone && !st.ClaimRelease:
		return originstream.EvictNever // a claimant is publishing from it
	case !known:
		// Settled or removed, or not applied here yet: evictable once the
		// origin moved on, or once this manifest has caught up with the
		// floors the origin sent (with no floors, it may be a new live
		// instance this manifest has not seen announced).
		if in.Replaced || (in.Floors && fsm.AppliedDone() >= in.FloorsApplied) {
			return originstream.EvictFirst
		}
		return originstream.EvictNever
	case st.Untracked:
		return originstream.EvictFirst // can never be claimed
	case st.Claimed && st.ClaimDone && !st.ClaimRelease:
		return originstream.EvictFirst // published
	case !in.Floors:
		return originstream.EvictNever // the origin reported nothing yet
	case st.Status == raft.InstanceAdopted && p.c.originHealthy(st.Origin):
		// Its live origin publishes it itself; kept unless the store would
		// otherwise drop the batches of the current primary.
		return originstream.EvictUnderPressure
	}
	return originstream.EvictNever
}

// originHealthy reports whether this node hears from origin.
func (c *Coordinator) originHealthy(origin string) bool {
	if c.registry == nil {
		return false
	}
	n, ok := c.registry.Get(origin)
	return ok && n.IsHealthy()
}
