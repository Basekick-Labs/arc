package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Compaction commit, coordinator side (#1207 d2). See raft/compaction_commit.go.
//
// The flag follows the stream claims pattern (stream_claims.go) without its
// conditions: every build that applies the rules advertises the capability,
// so a cluster of current builds enables it on its own. The leader proposes
// it when every node in the FSM table and every Raft server advertised the
// capability, and once it is on (or may be being proposed) a join without
// it is refused before the node enters the Raft configuration.

const compactionCommitEnableEvery = 10 * time.Second

// compactionCommitGate serializes the join gate with the enable proposal.
// inFlight (under mu) is set while an enable proposal may still commit (one
// that timed out): until the flag is seen, a join without the capability
// waits for a barrier first.
type compactionCommitGate struct {
	mu       sync.Mutex
	inFlight bool
	blockers atomic.Pointer[[]string]
}

// compactionCommitJoinGate is streamClaimsJoinGate for the compaction commit
// flag. Every node may propose the flag, so a join always takes the lock: an
// enable proposal cannot land between the check and the AddNode. A joiner
// without the capability waits for a barrier only while an earlier proposal
// may still commit.
func (c *Coordinator) compactionCommitJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.compactionCommit
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityCompactionCommit) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.CompactionCommitEnabled()
	if !enabled && g.inFlight {
		// An enable proposal that timed out may still commit: a barrier
		// appended after it applies it here before the flag is trusted off.
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its compaction commit state; retry the join", nil
		}
		enabled = fsm != nil && fsm.CompactionCommitEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "compaction commit is enabled in this cluster: the node must run a release that supports it", nil
}

// compactionCommitEnableLoop proposes the flag from the leader once every
// node supports it.
func (c *Coordinator) compactionCommitEnableLoop(ctx context.Context) {
	t := time.NewTicker(compactionCommitEnableEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.maybeEnableCompactionCommit()
		}
	}
}

// maybeEnableCompactionCommit is one pass of the enable loop.
func (c *Coordinator) maybeEnableCompactionCommit() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || fsm.CompactionCommitEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.compactionCommit
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityCompactionCommit)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableCompactionCommitPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	// Until the outcome is known the proposal may commit later: joins
	// without the capability barrier first (cleared once the flag is seen,
	// which ends the gate's use of it anyway).
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableCompactionCommit, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing compaction commit failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Compaction commit enabled: every node supports it")
}

// CompactionCommitActive reports whether the flag is applied here.
func (c *Coordinator) CompactionCommitActive() bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.CompactionCommitEnabled()
}

// CompactionCommitStatus is the /cluster status view.
type CompactionCommitStatus struct {
	Enabled  bool     `json:"enabled"`
	Staged   int      `json:"staged"`
	Blocking []string `json:"blocking_nodes,omitempty"`
}

// CompactionCommitStatus reports the flag, the staged jobs, and on the
// leader the nodes keeping the flag off.
func (c *Coordinator) CompactionCommitStatus() CompactionCommitStatus {
	var st CompactionCommitStatus
	if c.raftNode == nil {
		return st
	}
	if fsm := c.raftNode.FSM(); fsm != nil {
		st.Enabled = fsm.CompactionCommitEnabled()
		st.Staged = fsm.StagedCompactionCount()
	}
	if b := c.compactionCommit.blockers.Load(); b != nil && !st.Enabled && c.raftNode.IsLeader() {
		st.Blocking = *b
	}
	return st
}
