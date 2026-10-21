package cluster

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Cluster policies, coordinator side (#1324). See raft/cluster_policies.go.
//
// The flag follows the compaction commit pattern (compaction_commit.go): every
// build that applies the policy commands advertises the capability, the
// leader proposes the flag when every node in the FSM table and every Raft
// server advertised it, and once it is on (or may be being proposed) a join
// without it is refused before the node enters the Raft configuration.

const clusterPoliciesEnableEvery = 10 * time.Second

// clusterPoliciesGate serializes the join gate with the enable proposal.
// inFlight (under mu) is set while an enable proposal may still commit (one
// that timed out): until the flag is seen, a join without the capability
// waits for a barrier first.
type clusterPoliciesGate struct {
	mu       sync.Mutex
	inFlight bool
	blockers atomic.Pointer[[]string]
}

// clusterPoliciesJoinGate is compactionCommitJoinGate for the cluster
// policies flag.
func (c *Coordinator) clusterPoliciesJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.clusterPolicies
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityClusterPolicies) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.ClusterPoliciesEnabled()
	if !enabled && g.inFlight {
		// An enable proposal that timed out may still commit: a barrier
		// appended after it applies it here before the flag is trusted off.
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its cluster policies state; retry the join", nil
		}
		enabled = fsm != nil && fsm.ClusterPoliciesEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "cluster policies are enabled in this cluster: the node must run a release that supports them", nil
}

// clusterPoliciesEnableLoop proposes the flag from the leader once every node
// supports it.
func (c *Coordinator) clusterPoliciesEnableLoop(ctx context.Context) {
	t := time.NewTicker(clusterPoliciesEnableEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.maybeEnableClusterPolicies()
		}
	}
}

// maybeEnableClusterPolicies is one pass of the enable loop.
func (c *Coordinator) maybeEnableClusterPolicies() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || fsm.ClusterPoliciesEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.clusterPolicies
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityClusterPolicies)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableClusterPoliciesPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	// Until the outcome is known the proposal may commit later: joins
	// without the capability barrier first.
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableClusterPolicies, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing cluster policies failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Cluster policies enabled: every node supports them")
}

// ClusterPoliciesActive reports whether the flag is applied here.
func (c *Coordinator) ClusterPoliciesActive() bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.ClusterPoliciesEnabled()
}

// ClusterPoliciesStatus is the /cluster status view.
type ClusterPoliciesStatus struct {
	Enabled  bool     `json:"enabled"`
	Blocking []string `json:"blocking_nodes,omitempty"`
}

// ClusterPoliciesStatus reports the flag and, on the leader, the nodes keeping
// it off.
func (c *Coordinator) ClusterPoliciesStatus() ClusterPoliciesStatus {
	var st ClusterPoliciesStatus
	if c.raftNode == nil {
		return st
	}
	st.Enabled = c.ClusterPoliciesActive()
	if b := c.clusterPolicies.blockers.Load(); b != nil && !st.Enabled && c.raftNode.IsLeader() {
		st.Blocking = *b
	}
	return st
}
