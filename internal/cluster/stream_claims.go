package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// Claims name nodes, so a node without an explicit cluster.node_id (the
// default is hostname-PID, which changes at every restart on bare metal)
// never advertises them.
func (c *Coordinator) localCapabilities() []string {
	// Compaction commit (#1207 d2) needs nothing of the node: every build
	// that applies its rules advertises it.
	caps := []string{raft.CapabilityCompactionCommit, CapabilityForwardIdentity}
	if c.keepsLocalReplicas() {
		caps = append(caps, CapabilityLocalReplica)
	}
	if c.streamClaimsCapable() {
		caps = append(caps, raft.CapabilityStreamClaims)
	}
	return caps
}

// streamClaimsCapable reports whether this node advertises stream claims.
func (c *Coordinator) streamClaimsCapable() bool {
	if !c.cfg.StreamTakeover {
		return false
	}
	if c.cfg.NodeID == "" {
		c.nodeIDWarn.Do(func() {
			c.logger.Error().Msg("cluster.stream_takeover needs an explicit cluster.node_id: stream takeover stays off on this node, and the cluster cannot enable it while this node is a member")
		})
		return false
	}
	return !c.localNode.Role.GetCapabilities().CanIngest || c.walCoverageActive.Load()
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
	capable := hasCapability(req.GetCapabilities(), raft.CapabilityStreamClaims)
	enabled = fsm != nil && fsm.StreamClaimsEnabled()
	if !enabled && !capable {
		// An enable proposal that timed out may still commit: a barrier
		// appended after it applies it here before the flag is trusted off.
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			c.streamClaimsMu.Unlock()
			return "the cluster could not confirm its stream claims state; retry the join", nil
		}
		enabled = fsm != nil && fsm.StreamClaimsEnabled()
	}
	if !enabled || capable {
		return "", c.streamClaimsMu.Unlock
	}
	c.streamClaimsMu.Unlock()
	return "stream claims are enabled in this cluster: the node must run a release that supports them (with cluster.wal_coverage on for writers)", nil
}

const streamClaimsJoinBarrier = 5 * time.Second

const streamClaimsEnableEvery = 10 * time.Second

// streamClaimsEnableLoop proposes the flag from the leader once every node
// supports it (#1207). Started on nodes with cluster.stream_takeover. Node records carry the capabilities of their last
// join (a restarted leader re-registers itself with them).
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
	if c.raftNode == nil || c.cfg.SharedStorageMode || !c.cfg.StreamTakeover {
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
	return capabilityCandidates(fsmNodes, config, raft.CapabilityStreamClaims)
}

// capabilityCandidates is streamClaimsCandidates for any capability.
func capabilityCandidates(fsmNodes []*raft.NodeInfo, config func() ([]string, error), capability string) ([]string, []string) {
	capable := map[string]bool{}
	var nodes, blockers []string
	for _, n := range fsmNodes {
		if hasCapability(n.Capabilities, capability) {
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
	metrics.Get().SetDepartedWriters(int64(len(fsm.DepartedWriters())))
	var oldest time.Duration
	for _, k := range fsm.StreamClaims() {
		if !k.Done && !k.Released {
			oldest = max(oldest, time.Since(k.At))
		}
	}
	metrics.Get().SetOldestStreamClaimAge(int64(oldest / time.Second))
}

// departedExpiryLoop, on the leader, removes writers whose graceful
// departure is older than cluster.stream_takeover_departed_ttl, as an
// operator removal would: their instances become claimable and their claims
// are released (#1207). A writer that re-joined in between is not touched
// (the removal names the departure it expires).
func (c *Coordinator) departedExpiryLoop(ctx context.Context) {
	ttl := c.cfg.StreamTakeoverDepartedTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.expireDeparted(time.Now(), ttl)
	}
}

func (c *Coordinator) expireDeparted(now time.Time, ttl time.Duration) {
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() || !c.raftNode.IsLeader() {
		return
	}
	departed := fsm.DepartedWriters()
	ids := make([]string, 0, len(departed))
	for id := range departed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		at := departed[id]
		if now.Sub(at) < ttl {
			continue
		}
		if err := c.raftNode.RemoveNodeWithReason(id, raft.RemoveReasonDepartedExpired, at, 5*time.Second); err != nil {
			c.logger.Warn().Err(err).Str("node_id", id).Msg("Could not expire a departed writer; retrying")
			continue
		}
		if _, still := fsm.DepartedWriters()[id]; still {
			continue // not applied: it re-joined, or this node is behind
		}
		if _, back := fsm.GetNode(id); back {
			continue // it re-joined: the expiry was a no-op
		}
		c.logger.Warn().Str("node_id", id).Time("departed_at", at).
			Msg("A writer that left the cluster did not come back in time: its stream takeover state is released (instances claimable, claims released)")
	}
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

// CoverageCovered, CoveringFiles and TakeoverGeneration read the takeover
// state of this node's FSM for the registration outbox (TakeoverReader).
func (c *Coordinator) CoverageCovered(inst uint64, key string, p raft.CoveragePiece, hour time.Time) bool {
	if c.raftNode == nil || c.raftNode.FSM() == nil {
		return false
	}
	return c.raftNode.FSM().CoverageCovered(inst, key, p, hour)
}

func (c *Coordinator) UncoveredBatches(inst uint64, key string, lo, hi uint64, hour time.Time) raft.Intervals {
	if c.raftNode == nil || c.raftNode.FSM() == nil {
		return raft.Intervals{{lo, hi}}
	}
	return c.raftNode.FSM().UncoveredBatches(inst, key, lo, hi, hour)
}

func (c *Coordinator) CoveringFilesOf(database, measurement string, hour time.Time, inst uint64, key string, set raft.Intervals) []raft.FileEntry {
	if c.raftNode == nil || c.raftNode.FSM() == nil {
		return nil
	}
	return c.raftNode.FSM().CoveringFilesOf(database, measurement, hour, inst, key, set)
}

func (c *Coordinator) TakeoverGeneration() uint64 {
	if c.raftNode == nil || c.raftNode.FSM() == nil {
		return 0
	}
	return c.raftNode.FSM().TakeoverGeneration()
}

var _ TakeoverReader = (*Coordinator)(nil)

// writerEligible says whether a writer may be designated primary: not one
// whose current process another writer has claimed (it exits for a restart,
// and its claimed batches are being published elsewhere) (#1207).
func (c *Coordinator) writerEligible(nodeID string) bool {
	if c.raftNode == nil {
		return true
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() {
		return true
	}
	return !fsm.CurrentInstanceClaimed(nodeID)
}

// StreamTakeoverView is the operator view of stream takeover (#1207).
type StreamTakeoverView struct {
	Enabled   bool                     `json:"enabled"`
	Instances []StreamTakeoverInstance `json:"instances"`
	Claims    []StreamTakeoverClaim    `json:"claims"`
	Departed  []StreamTakeoverDeparted `json:"departed_writers"`
	// DeleteRecords are the DELETEs kept until every writer process they
	// name has settled the batches before them (#1207 d1).
	DeleteRecords []StreamTakeoverDeleteRecord `json:"delete_records"`
}

// StreamTakeoverDeleteRecord is one DELETE kept for stream takeover.
// Waiting lists the writer processes (hex) that may still publish batches
// acknowledged before it; Unacknowledged those that have not reported them.
type StreamTakeoverDeleteRecord struct {
	ID             uint64    `json:"id"`
	Origin         string    `json:"origin"`
	Database       string    `json:"database"`
	Measurement    string    `json:"measurement,omitempty"`
	Where          string    `json:"where,omitempty"`
	At             time.Time `json:"at"`
	Unacknowledged []string  `json:"unacknowledged,omitempty"`
}

// StreamTakeoverInstance is one writer process the cluster knows.
type StreamTakeoverInstance struct {
	Instance  string `json:"instance"`
	Origin    string `json:"origin"`
	Status    string `json:"status"`
	AdoptedBy string `json:"adopted_by,omitempty"`
	Untracked bool   `json:"untracked,omitempty"`
}

// StreamTakeoverClaim is one claim, with sizes rather than its sets.
type StreamTakeoverClaim struct {
	Instance            string    `json:"instance"`
	Origin              string    `json:"origin"`
	By                  string    `json:"by"`
	Epoch               uint64    `json:"epoch"`
	Done                bool      `json:"done"`
	Released            bool      `json:"released"`
	Abandoned           bool      `json:"abandoned,omitempty"`
	FinishedBy          []string  `json:"finished_by,omitempty"`
	Keys                int       `json:"keys"`
	Batches             uint64    `json:"batches"`
	UnreplayableBatches uint64    `json:"unreplayable_batches"`
	At                  time.Time `json:"at"`
}

// StreamTakeoverDeparted is a writer that left gracefully and has not
// re-joined.
type StreamTakeoverDeparted struct {
	NodeID    string    `json:"node_id"`
	Since     time.Time `json:"since"`
	ExpiresAt time.Time `json:"expires_at"`
}

// StreamTakeover returns the operator view from this node's manifest.
func (c *Coordinator) StreamTakeover() StreamTakeoverView {
	v := StreamTakeoverView{Instances: []StreamTakeoverInstance{}, Claims: []StreamTakeoverClaim{}, Departed: []StreamTakeoverDeparted{}}
	if c.raftNode == nil || c.raftNode.FSM() == nil {
		return v
	}
	fsm := c.raftNode.FSM()
	v.Enabled = fsm.StreamClaimsEnabled()
	hex := func(i uint64) string { return fmt.Sprintf("%016x", i) }
	for _, in := range fsm.StreamInstances() {
		si := StreamTakeoverInstance{Instance: hex(in.Instance), Origin: in.Origin, Status: in.Status, Untracked: in.Untracked}
		if in.AdoptedBy != 0 {
			si.AdoptedBy = hex(in.AdoptedBy)
		}
		v.Instances = append(v.Instances, si)
	}
	for _, cl := range fsm.StreamClaims() {
		sc := StreamTakeoverClaim{Instance: hex(cl.Instance), Origin: cl.Origin, By: cl.By, Epoch: cl.Epoch, Done: cl.Done,
			Released: cl.Released, Abandoned: cl.Abandoned, FinishedBy: cl.FinishedBy, Keys: len(cl.Set), At: cl.At}
		for _, s := range cl.Set {
			sc.Batches += s.Count()
		}
		for _, s := range cl.Unreplayable {
			sc.UnreplayableBatches += s.Count()
		}
		v.Claims = append(v.Claims, sc)
	}
	ttl := c.cfg.StreamTakeoverDepartedTTL
	for id, at := range fsm.DepartedWriters() {
		v.Departed = append(v.Departed, StreamTakeoverDeparted{NodeID: id, Since: at, ExpiresAt: at.Add(ttl)})
	}
	sort.Slice(v.Departed, func(i, j int) bool { return v.Departed[i].NodeID < v.Departed[j].NodeID })
	v.DeleteRecords = []StreamTakeoverDeleteRecord{}
	for _, r := range fsm.DeleteRecords() {
		dr := StreamTakeoverDeleteRecord{ID: r.ID, Origin: r.Origin, Database: r.Database, Measurement: r.Measurement, Where: r.Where, At: r.At}
		for _, sc := range r.Before {
			if !sc.Acked {
				dr.Unacknowledged = append(dr.Unacknowledged, hex(sc.Instance))
			}
		}
		v.DeleteRecords = append(v.DeleteRecords, dr)
	}
	return v
}

// Errors of ReleaseStreamClaim, for the operator API.
var (
	ErrStreamClaimNotFound = errors.New("no active stream claim with that instance and epoch")
	ErrStreamClaimFinished = errors.New("the stream claim is finished")
)

// ReleaseStreamClaim releases an unfinished claim, as an operator (#1207):
// another writer may then claim the instance (its claimant too). With
// abandon, the claim is finished instead: every batch it has not published
// is left to its origin (lost when the origin is gone), its claimant does
// not claim the instance again, and DELETEs of its measurements proceed.
// Leader only.
func (c *Coordinator) ReleaseStreamClaim(instance, epoch uint64, abandon bool) error {
	if c.raftNode == nil {
		return ErrClusterRaftNotConfigured
	}
	if !c.raftNode.IsLeader() {
		return fmt.Errorf("%w (leader: %s)", ErrNotLeaderForTopology, c.raftNode.LeaderID())
	}
	by, err := c.releasableClaim(instance, epoch)
	if err != nil {
		return err
	}
	cmd := &raft.Command{Type: raft.CommandReleaseStreamClaim}
	var err2 error
	if abandon {
		cmd.Type = raft.CommandClaimDone
		cmd.Payload, err2 = json.Marshal(raft.ClaimDonePayload{Instance: instance, Epoch: epoch, Abandon: true, Now: time.Now().UTC()})
	} else {
		cmd.Payload, err2 = json.Marshal(raft.ReleaseStreamClaimPayload{Instance: instance, Epoch: epoch, Now: time.Now().UTC()})
	}
	if err2 != nil {
		return err2
	}
	if err := c.raftNode.Apply(cmd, 5*time.Second); err != nil {
		// It may have finished or been released meanwhile.
		if _, again := c.releasableClaim(instance, epoch); again != nil {
			return again
		}
		return err
	}
	if abandon {
		c.logger.Error().Str("instance", fmt.Sprintf("%016x", instance)).Uint64("epoch", epoch).Str("by", by).
			Msg("Stream claim abandoned by an operator: its unpublished batches are left to their origin")
		return nil
	}
	c.logger.Warn().Str("instance", fmt.Sprintf("%016x", instance)).Uint64("epoch", epoch).Str("by", by).
		Msg("Stream claim released by an operator")
	return nil
}

// releasableClaim returns the claimant of an active, unfinished claim, or
// why it cannot be released.
func (c *Coordinator) releasableClaim(instance, epoch uint64) (string, error) {
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.StreamClaimsEnabled() {
		return "", ErrStreamClaimNotFound
	}
	for _, cl := range fsm.StreamClaims() {
		if cl.Instance != instance || cl.Epoch != epoch || cl.Released {
			continue
		}
		if cl.Done {
			return "", ErrStreamClaimFinished
		}
		return cl.By, nil
	}
	return "", ErrStreamClaimNotFound
}
