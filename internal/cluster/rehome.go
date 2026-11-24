package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Re-home on a shared object store, coordinator side (#1825 slice 4). See
// raft/rehome.go.
//
// The flag follows the compaction commit pattern (compaction_commit.go):
// every build that applies the rules advertises the capability, the leader
// proposes the flag once every node and Raft server advertised it and
// compaction commit is on, and once it is on (or may be being proposed) a
// join without it is refused before the node enters the Raft configuration.
//
// Separately, a node advertises CapabilityRehomeInScope when its own
// configuration leaves nothing a job cannot account for (SetRehomeInScope):
// StageRehome refuses while any node lacks it.

const rehomeEnableEvery = 10 * time.Second

// rehomeGate serializes the join gate with the enable proposal, as
// compactionCommitGate does.
type rehomeGate struct {
	mu       sync.Mutex
	inFlight bool
	blockers atomic.Pointer[[]string]
}

// SetRehomeInScope records whether this node's configuration is one a
// re-home job can run under: no tiered storage, no Iceberg export, no edge
// sync. Advertised at the next join; call before Start.
func (c *Coordinator) SetRehomeInScope(ok bool) {
	if c != nil {
		c.rehomeInScope.Store(ok)
	}
}

// rehomeJoinGate is compactionCommitJoinGate for the re-home flag.
func (c *Coordinator) rehomeJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.rehomeGate
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityRehome) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.RehomeEnabled()
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its re-home state; retry the join", nil
		}
		enabled = fsm != nil && fsm.RehomeEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "re-home is enabled in this cluster: the node must run a release that supports it", nil
}

// rehomeEnableLoop proposes the flag from the leader once every node
// supports it and compaction commit is on.
func (c *Coordinator) rehomeEnableLoop(ctx context.Context) {
	t := time.NewTicker(rehomeEnableEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.maybeEnableRehome()
		}
	}
}

func (c *Coordinator) maybeEnableRehome() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || fsm.RehomeEnabled() || !fsm.CompactionCommitEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.rehomeGate
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityRehome)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableRehomePayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableRehome, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing re-home failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Re-home enabled: every node supports it")
}

// RehomeSource is the cluster's re-home record as the read path, the
// compactor, retention and DELETE ask it, in the shape of rehome.Registry's
// methods of the same names. It reads this node's FSM, so what a read leaves
// out changes with the apply. It fails closed while this node may not serve
// reads (its state may be behind the leader's): it owns every file, and a
// strict read errors.
type RehomeSource struct {
	c *Coordinator
}

// RehomeSource returns the source, nil without Raft.
func (c *Coordinator) RehomeSource() *RehomeSource {
	if c == nil || c.raftNode == nil {
		return nil
	}
	return &RehomeSource{c: c}
}

func (s *RehomeSource) fsm() *raft.ClusterFSM {
	if s == nil || s.c == nil || s.c.raftNode == nil {
		return nil
	}
	return s.c.raftNode.FSM()
}

// synced reports whether this node's state may be trusted for re-home
// decisions.
func (s *RehomeSource) synced() bool {
	ok, _ := s.c.ReadReady()
	return ok
}

// Owns reports whether a job owns key (its source or an output). True for
// every key while this node is not synced.
func (s *RehomeSource) Owns(key string) bool {
	f := s.fsm()
	if f == nil {
		return false
	}
	if !s.synced() {
		return true
	}
	return f.RehomeOwns(key)
}

// Active reports whether any job is in flight. True while not synced.
func (s *RehomeSource) Active() bool {
	f := s.fsm()
	if f == nil {
		return false
	}
	return !s.synced() || len(f.RehomeJobs()) > 0
}

// Open reports whether a job under prefix may leave files out of reads.
func (s *RehomeSource) Open(prefix string) bool {
	f := s.fsm()
	return f != nil && f.RehomeOpen(prefix)
}

// ExcludedFor returns the keys reads under prefix must leave out.
func (s *RehomeSource) ExcludedFor(_ context.Context, prefix string) []string {
	f := s.fsm()
	if f == nil {
		return nil
	}
	keys, _ := f.RehomeExcludedFor(prefix)
	return keys
}

// ExcludedTiered is ExcludedFor in the registry's tiered shape. A cluster
// job's source is always hot (tiering is out of scope), so coldKeys is
// empty.
func (s *RehomeSource) ExcludedTiered(ctx context.Context, prefix string) (keys, coldKeys []string) {
	return s.ExcludedFor(ctx, prefix), nil
}

// ExcludedForStrict errors while this node is not synced.
func (s *RehomeSource) ExcludedForStrict(ctx context.Context, prefix string) ([]string, error) {
	if s.fsm() != nil && !s.synced() {
		return nil, errors.New("this node's cluster state is not synced with the leader")
	}
	return s.ExcludedFor(ctx, prefix), nil
}

// Generation moves whenever what a job leaves out of reads may change.
func (s *RehomeSource) Generation() uint64 {
	f := s.fsm()
	if f == nil {
		return 0
	}
	return f.RehomeGeneration()
}

// RehomeStatus is the /cluster status view.
type RehomeStatus struct {
	Enabled  bool              `json:"enabled"`
	Jobs     []RehomeJobStatus `json:"jobs,omitempty"`
	Blocking []string          `json:"blocking_nodes,omitempty"`
}

// RehomeJobStatus is one job in the status view.
type RehomeJobStatus struct {
	ID          string   `json:"id"`
	Source      string   `json:"source"`
	State       string   `json:"state"`
	Outputs     int      `json:"outputs"`
	Finisher    string   `json:"finisher,omitempty"`
	MissingAcks []string `json:"missing_acks,omitempty"`
	Dropped     []string `json:"dropped,omitempty"`
}

// RehomeStatus reports the flag, the jobs, and on the leader the nodes
// keeping the flag off.
func (c *Coordinator) RehomeStatus() RehomeStatus {
	var st RehomeStatus
	if c == nil || c.raftNode == nil {
		return st
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return st
	}
	st.Enabled = fsm.RehomeEnabled()
	for _, j := range fsm.RehomeJobs() {
		st.Jobs = append(st.Jobs, RehomeJobStatus{
			ID: j.ID, Source: j.Source.Path, State: j.State, Outputs: len(j.Outputs),
			Finisher: j.FinisherNode, MissingAcks: j.MissingAcks(), Dropped: j.Dropped,
		})
	}
	if !st.Enabled && c.raftNode.IsLeader() {
		if b := c.rehomeGate.blockers.Load(); b != nil {
			st.Blocking = append([]string(nil), (*b)...)
		}
	}
	return st
}
