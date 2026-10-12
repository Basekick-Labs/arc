package cluster

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
)

// Stream claims, coordinator side (#1207 S4a). See raft/stream_claims.go.
//
// The flag must never be applied while any node that applies the Raft log
// runs a build that cannot apply claims: such a node skips claim commands
// and ignores coverage on registrations, so its manifest would accept files
// every other node refuses. Two guards, serialized by streamClaimsMu on the
// leader:
//   - the leader proposes the flag only when every node in the FSM table and
//     every server in the Raft configuration advertised the capability at its
//     last join;
//   - once the flag is on (or being proposed), a join without the capability
//     is refused before the node enters the Raft configuration, and a member
//     that comes back without it (rolled back) is removed from it.

// localCapabilities is what this build advertises at join. A node that
// ingests advertises stream claims only when coverage is in effect on it
// (SetWALCoverageActive): the flag implies every such node stamps coverage
// on its files. Other roles apply the FSM rules and advertise them always.
func (c *Coordinator) localCapabilities() []string {
	if !c.cfg.StreamTakeover {
		return nil
	}
	if c.localNode.Role.GetCapabilities().CanIngest && !c.walCoverageActive.Load() {
		return nil
	}
	return []string{raft.CapabilityStreamClaims}
}

// SetWALCoverageActive records whether WAL coverage actually took effect on
// this node's ingest buffer. Call before Start: the first join carries it.
func (c *Coordinator) SetWALCoverageActive(on bool) { c.walCoverageActive.Store(on) }

func hasCapability(caps []string, want string) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}

// streamClaimsJoinGate runs on the leader before a joiner enters the Raft
// configuration. While stream claims are off and no enable proposal can run
// it takes no lock: joins stay concurrent. Otherwise it holds streamClaimsMu
// until release is called, so an enable proposal cannot land between the
// check and the AddNode. A non-empty why is the refusal (nothing to release).
//
// It does not remove a member that comes back without the capability: a
// stripped join (the capabilities are outside the first MAC) must not be
// able to evict a healthy node. The refusal is logged at Error for the
// operator.
func (c *Coordinator) streamClaimsJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	enabled := fsm != nil && fsm.StreamClaimsEnabled()
	if !enabled && !c.streamClaimsProposer.Load() {
		return "", func() {}
	}
	c.streamClaimsMu.Lock()
	enabled = fsm != nil && fsm.StreamClaimsEnabled()
	if !enabled || hasCapability(req.GetCapabilities(), raft.CapabilityStreamClaims) {
		return "", c.streamClaimsMu.Unlock
	}
	c.streamClaimsMu.Unlock()
	return "stream claims are enabled in this cluster: the node must run a release that supports them (with cluster.wal_coverage on for writers)", nil
}

const streamClaimsEnableEvery = 10 * time.Second

// streamClaimsEnableLoop proposes the flag from the leader once every node
// supports it. NOT started in this release (#1207 S4a): stream claims are
// one-way, and nothing proposes a claim until the takeover stage ships. That
// stage starts it, together with refreshing node records after a rolling
// upgrade and a barrier in the join gate.
func (c *Coordinator) streamClaimsEnableLoop(ctx context.Context) {
	t := time.NewTicker(streamClaimsEnableEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.maybeEnableStreamClaims()
		}
	}
}

// maybeEnableStreamClaims is one pass of the enable loop.
func (c *Coordinator) maybeEnableStreamClaims() {
	if c.raftNode == nil || c.cfg.SharedStorageMode || !c.cfg.WALCoverage || !c.cfg.StreamTakeover {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return
	}
	if fsm.StreamClaimsEnabled() || !c.raftNode.IsLeader() {
		return
	}
	c.streamClaimsMu.Lock()
	defer c.streamClaimsMu.Unlock()
	nodes, blockers := streamClaimsCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs)
	c.setStreamClaimsBlockers(blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableStreamClaimsPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableStreamClaims, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing stream claims failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Stream claims enabled: every node supports them")
	c.publishStreamClaims(fsm)
}

// streamClaimsCandidates returns the node ids to list in the enable command,
// and the ids keeping it from being proposed: any FSM node without the
// capability, and any Raft server that is not a capable FSM node (a member
// whose AddNode never applied could run anything).
func streamClaimsCandidates(fsmNodes []*raft.NodeInfo, config func() ([]string, error)) ([]string, []string) {
	capable := map[string]bool{}
	var nodes, blockers []string
	for _, n := range fsmNodes {
		if hasCapability(n.Capabilities, raft.CapabilityStreamClaims) {
			capable[n.ID] = true
			nodes = append(nodes, n.ID)
		} else {
			blockers = append(blockers, n.ID)
		}
	}
	ids, err := config()
	if err != nil {
		return nil, []string{"raft configuration unavailable"}
	}
	for _, id := range ids {
		if !capable[id] && !contains(blockers, id) {
			blockers = append(blockers, id)
		}
	}
	sort.Strings(nodes)
	sort.Strings(blockers)
	return nodes, blockers
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func (c *Coordinator) setStreamClaimsBlockers(b []string) {
	c.streamClaimsBlockers.Store(&b)
}

// StreamClaimsStatus is the /cluster status view.
type StreamClaimsStatus struct {
	Enabled  bool     `json:"enabled"`
	Claims   int      `json:"claims"`
	Blocking []string `json:"blocking_nodes,omitempty"`
}

// StreamClaimsStatus reports the flag, the number of claims, and the nodes
// keeping the flag off (writers without cluster.wal_coverage, older builds,
// or records of nodes that no longer exist — remove those).
func (c *Coordinator) StreamClaimsStatus() StreamClaimsStatus {
	var st StreamClaimsStatus
	if c.raftNode == nil {
		return st
	}
	if fsm := c.raftNode.FSM(); fsm != nil {
		st.Enabled = fsm.StreamClaimsEnabled()
		st.Claims = fsm.StreamClaimCount()
	}
	if b := c.streamClaimsBlockers.Load(); b != nil && !st.Enabled && c.raftNode.IsLeader() {
		st.Blocking = *b
	}
	return st
}

func (c *Coordinator) publishStreamClaims(fsm *raft.ClusterFSM) {
	metrics.Get().SetStreamClaims(fsm.StreamClaimsEnabled(), int64(fsm.StreamClaimCount()))
}

// StreamClaimsEnabled reports whether this node's FSM has stream claims on.
func (c *Coordinator) StreamClaimsEnabled() bool {
	if c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.StreamClaimsEnabled()
}

// streamClaimsMetricsLoop keeps the stream claims gauges current on every
// node, readers included.
func (c *Coordinator) streamClaimsMetricsLoop(ctx context.Context) {
	if c.raftNode == nil {
		return
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		if fsm := c.raftNode.FSM(); fsm != nil {
			c.publishStreamClaims(fsm)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
