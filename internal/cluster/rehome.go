package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
// re-home job can run under: no edge sync (tiered storage since #1869, with
// its cold tier running; the Iceberg export since #1870, following the
// compactor lease). Advertised at the next join, and
// by the self-record refresh when it changes after Start.
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
		// A node that does not take part in jobs (leave their files out of
		// reads, ack) would read both sides of one in flight. Jobs are
		// staged on the leader's log and a join is handled on the leader,
		// so its FSM is current here; the FSM also refuses every step but a
		// revert once such a node is a member.
		// A member (a node that came back without leaving) is let through
		// (#1859). One still in the Raft configuration applies the log
		// whatever the gate says, and refusing either kind only keeps its
		// old capabilities in its record, so the cluster would count it in
		// scope. Its join rewrites the record; the FSM then refuses every
		// step but a revert.
		if !hasCapability(req.GetCapabilities(), raft.CapabilityRehomeInScope) && fsm != nil && fsm.RehomeJobsInFlight() &&
			!c.isMember(req.GetNodeID()) {
			g.mu.Unlock()
			return "a re-home job is in flight and this node does not take part in one (its release or configuration); retry when it has ended", nil
		}
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
	if fsm == nil || fsm.RehomeEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.rehomeGate
	if !fsm.CompactionCommitEnabled() {
		off := []string{"(compaction commit is off)"}
		g.blockers.Store(&off)
		return
	}
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
		// Behind the leader, this node cannot tell which files jobs own:
		// all of them, once re-home is on (its FSM refuses a change to an
		// owned file in any case).
		return f.RehomeEnabled()
	}
	// And a compaction being finished under read fences (#1852): its FSM
	// refuses a change to its files until its inputs are gone.
	return f.RehomeOwns(key) || f.CompactionFencedOwns(key)
}

// OwnsUnder reports whether a job owns a file under prefix ("" is every
// file). True while not synced.
func (s *RehomeSource) OwnsUnder(prefix string) bool {
	f := s.fsm()
	if f == nil {
		return false
	}
	if !s.synced() {
		// Behind the leader, this node cannot tell which files jobs own:
		// all of them, once re-home is on (its FSM refuses a change to an
		// owned file in any case).
		return f.RehomeEnabled()
	}
	return f.RehomeOwnsUnder(prefix) || f.CompactionFencedOwnsUnder(prefix)
}

// Active reports whether any job is in flight. True while not synced.
func (s *RehomeSource) Active() bool {
	f := s.fsm()
	if f == nil {
		return false
	}
	return (!s.synced() && f.RehomeEnabled()) || len(f.RehomeJobs()) > 0
}

// Open reports whether a job under prefix may leave files out of reads.
func (s *RehomeSource) Open(prefix string) bool {
	f := s.fsm()
	return f != nil && !s.c.rehomeLocalExclusions.Load() && f.RehomeOpen(prefix)
}

// ExcludedFor returns the keys reads under prefix must leave out.
func (s *RehomeSource) ExcludedFor(_ context.Context, prefix string) []string {
	f := s.fsm()
	if f == nil {
		return nil
	}
	if s.c.rehomeLocalExclusions.Load() {
		return nil
	}
	keys, _ := f.RehomeExcludedFor(prefix)
	return keys
}

// SetRehomeLocalRecords names the jobs this node records itself
// (rehome.Registry.HasCluster): the unlink queued when a job ends leaves those
// to the node's own settling, which removes the source only once the outputs
// are here (#1860).
func (c *Coordinator) SetRehomeLocalRecords(fn func(id string) bool) {
	if c != nil && fn != nil {
		c.rehomeLocalRecords.Store(&fn)
	}
}

// SetRehomeLocalSources names the files that are the source of a job this
// node records (rehome.Registry.ClusterSource): the delete workers never
// unlink one; this node's part of the job, or SweepLocal, does, once the
// outputs are in place (#1860). Set before Start: a delete replayed from the
// log at start must see it.
func (c *Coordinator) SetRehomeLocalSources(fn func(path string) bool) {
	if c != nil && fn != nil {
		c.rehomeLocalSources.Store(&fn)
	}
}

// SetRehomeLocalExclusions makes this node's own job records (rehome.Registry)
// the only source of what its reads leave out for re-home jobs, on local
// storage (#1860): every node places and removes its own copies, and its
// record follows its disk, whatever its Raft state (behind the leader after a
// restart, before it heard from it). The source still answers ownership.
func (c *Coordinator) SetRehomeLocalExclusions() {
	if c != nil {
		c.rehomeLocalExclusions.Store(true)
	}
}

// ExcludedTiered is ExcludedFor in the registry's tiered shape. A cluster
// job's source is always hot (a cold source is refused, and every node checks
// before it acks published that cold storage holds no copy, #1869), so
// coldKeys is empty.
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
	// Backups: backups or restores running on any node (#1857); no job is
	// staged while one is renewed within raft.BackupMarkLease.
	Backups []raft.BackupMark `json:"backups,omitempty"`
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
	st.Backups = fsm.BackupMarks()
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

// isMember reports whether id is in the node table or the Raft
// configuration.
func (c *Coordinator) isMember(id string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	if fsm := c.raftNode.FSM(); fsm != nil {
		if _, ok := fsm.GetNode(id); ok {
			return true
		}
	}
	ids, err := c.raftNode.ConfigurationServerIDs()
	if err != nil {
		return false
	}
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

// BeginBackupMark records in the cluster that a backup or restore runs here
// (#1857): no re-home job is staged while it does, on any node. The mark is
// renewed until release, which ends it. An error: the mark could not be
// recorded, and the caller should not start. Nothing is recorded while
// re-home is off (no job can be staged; and an older leader would refuse the
// command).
func (s *RehomeSource) BeginBackupMark(ctx context.Context) (release func(), err error) {
	if s == nil || s.c == nil || s.c.raftNode == nil {
		return func() {}, nil
	}
	c := s.c
	if fsm := c.raftNode.FSM(); fsm == nil || !fsm.RehomeEnabled() {
		return func() {}, nil
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(b[:])
	propose := func(ctx context.Context, renew, end bool) error {
		raw, err := json.Marshal(raft.BackupMarkPayload{ID: id, Node: c.LocalNodeID(), Now: time.Now().UTC(), Renew: renew, End: end})
		if err != nil {
			return err
		}
		return c.applyOrForward(ctx, &raft.Command{Type: raft.CommandBackupMark, Payload: raw})
	}
	if err := propose(ctx, false, false); err != nil {
		return nil, fmt.Errorf("record the backup in the cluster: %w", err)
	}
	// This node's record catches up past the mark, so the caller's check
	// for jobs (after this) sees any job staged before the mark: a job and a
	// backup each record themselves, then look for the other.
	if err := c.SyncManifest(ctx); err != nil {
		_ = propose(context.WithoutCancel(ctx), false, true)
		return nil, fmt.Errorf("sync with the cluster after recording the backup: %w", err)
	}
	renew := func() bool {
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := propose(rctx, true, false)
		if err == nil {
			return true
		}
		if strings.Contains(err.Error(), raft.ErrBackupMarkLapsed.Error()) {
			c.logger.Error().Str("mark", id).
				Msg("This node's backup mark lapsed while the backup ran (the node lost the cluster for longer than its lease): a re-home job may have moved files under it; check the backup, or take it again")
			return false
		}
		c.logger.Warn().Err(err).Str("mark", id).Msg("Renewing this node's backup mark failed; retrying")
		return true
	}
	// Once now: the sync may have taken a good part of the lease.
	alive := renew()
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		if !alive {
			return
		}
		t := time.NewTicker(raft.BackupMarkLease / 3)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if !renew() {
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-stopped // no renew lands after the end
			rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := propose(rctx, false, true); err != nil {
				c.logger.Warn().Err(err).Str("mark", id).Msg("Ending this node's backup mark failed; it lapses on its own")
			}
		})
	}, nil
}

// Replaced returns, for each job under prefix whose reads use its outputs
// (latched or removed), its source's key with its outputs' keys: the Iceberg
// export commits such a file's removal together with them (#1870).
func (s *RehomeSource) Replaced(prefix string) map[string][]string {
	f := s.fsm()
	if f == nil {
		return nil
	}
	var out map[string][]string
	for _, j := range f.RehomeJobs() {
		if j.Database+"/"+j.Measurement+"/" != prefix || (j.State != raft.RehomeLatched && j.State != raft.RehomeRemoved) {
			continue
		}
		if out == nil {
			out = map[string][]string{}
		}
		keys := make([]string, len(j.Outputs))
		for i, o := range j.Outputs {
			keys[i] = o.Key
		}
		out[j.Source.Path] = keys
	}
	return out
}

// CapabilityIcebergExport is advertised by a node that runs the Iceberg
// export when it holds the compactor lease (#1870).
const CapabilityIcebergExport = "iceberg_export_v1"

// SetIcebergOn records whether this node runs the Iceberg export when it
// holds the compactor lease. Advertised at the next join, and by the
// self-record refresh when it changes after Start.
func (c *Coordinator) SetIcebergOn(on bool) {
	if c != nil {
		c.icebergOn.Store(on)
	}
}

// IcebergExporterPresent reports whether a node exports to Iceberg now: the
// compactor lease is held, by a node that runs the export (#1870).
func (c *Coordinator) IcebergExporterPresent() bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false
	}
	holder := fsm.GetActiveCompactorID()
	if holder == "" {
		return false
	}
	n, ok := fsm.GetNode(holder)
	return ok && hasCapability(n.Capabilities, CapabilityIcebergExport)
}
