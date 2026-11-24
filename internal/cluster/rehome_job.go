package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Re-home on a shared object store, the job's cluster side (#1825 slice 4b):
// what every node does for a job's state (the participant), and the
// proposals the job's driver makes (internal/api/rehome_cluster.go).

// RehomeParticipantFunc does this node's part of a job's current state:
//
//	committed, latched  wait for the reads from before the state to end
//	published, removed,
//	reverted            drop the query caches, then wait for the reads
//
// acked says whether this node is waited for (an ack follows a nil return);
// when it is not, only the cache step matters. It runs off the apply
// goroutine and may block up to ctx.
type RehomeParticipantFunc func(ctx context.Context, job raft.RehomeJob, acked bool) error

const (
	rehomeParticipantTick = 5 * time.Second
	// rehomeParticipantWait bounds one attempt; a failed one is retried at
	// the next tick.
	rehomeParticipantWait = 15 * time.Minute
)

// rehomeAckable: states a node acts on.
func rehomeAckable(state string) bool {
	switch state {
	case raft.RehomeCommitted, raft.RehomePublished, raft.RehomeLatched, raft.RehomeRemoved, raft.RehomeReverted:
		return true
	}
	return false
}

// rehomeSteps is the participant loop's memory.
type rehomeSteps struct {
	mu sync.Mutex
	// running: (job, state index) attempts in flight; done: those that
	// finished (acked, or the cache step of a state not waited for here).
	running map[string]bool
	done    map[string]bool
}

func rehomeStepKey(j raft.RehomeJob) string {
	return fmt.Sprintf("%s@%d", j.ID, j.StateIndex)
}

// SetRehomeParticipant sets this node's part of a job's states. Until it is
// set the node acks nothing: a job waits for it.
func (c *Coordinator) SetRehomeParticipant(fn RehomeParticipantFunc) {
	if c == nil || fn == nil {
		return
	}
	c.rehomePart.Store(&fn)
	c.nudgeRehome()
}

func (c *Coordinator) nudgeRehome() {
	select {
	case c.rehomeNotify <- struct{}{}:
	default:
	}
}

// startRehomeParticipant registers the FSM callbacks and starts the loop.
func (c *Coordinator) startRehomeParticipant(ctx context.Context) {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return
	}
	// On the apply goroutine: a non-blocking nudge only.
	fsm.SetRehomeCallbacks(func(raft.RehomeJob) { c.nudgeRehome() }, func(raft.RehomeJob) { c.nudgeRehome() })
	go c.rehomeParticipantLoop(ctx)
}

func (c *Coordinator) rehomeParticipantLoop(ctx context.Context) {
	t := time.NewTicker(rehomeParticipantTick)
	defer t.Stop()
	for {
		c.rehomeParticipantPass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-c.rehomeNotify:
		}
	}
}

// rehomeParticipantPass starts this node's part of every job state it has
// not done. Only while this node holds the read lease: a node behind the
// leader could ack a state whose exclusions its reads do not apply yet.
func (c *Coordinator) rehomeParticipantPass(ctx context.Context) {
	p := c.rehomePart.Load()
	if p == nil || c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.RehomeEnabled() {
		return
	}
	if ok, _ := c.ReadReady(); !ok {
		return
	}
	fn := *p
	self := c.LocalNodeID()
	jobs := fsm.RehomeJobs()
	live := map[string]bool{}
	st := &c.rehomeSteps
	st.mu.Lock()
	if st.running == nil {
		st.running, st.done = map[string]bool{}, map[string]bool{}
	}
	st.mu.Unlock()
	for _, j := range jobs {
		key := rehomeStepKey(j)
		live[key] = true
		if !rehomeAckable(j.State) {
			continue
		}
		wanted := contains(j.AckWanted, self)
		if wanted && j.Acks[self] {
			continue
		}
		st.mu.Lock()
		skip := st.running[key] || (st.done[key] && !wanted)
		if !skip {
			st.running[key] = true
		}
		st.mu.Unlock()
		if skip {
			continue
		}
		go c.rehomeParticipate(ctx, fn, j, wanted, key)
	}
	st.mu.Lock()
	for k := range st.done {
		if !live[k] {
			delete(st.done, k)
		}
	}
	st.mu.Unlock()
}

func (c *Coordinator) rehomeParticipate(ctx context.Context, fn RehomeParticipantFunc, j raft.RehomeJob, wanted bool, key string) {
	st := &c.rehomeSteps
	ok := false
	defer func() {
		st.mu.Lock()
		delete(st.running, key)
		if ok {
			st.done[key] = true
		}
		st.mu.Unlock()
	}()
	wctx, cancel := context.WithTimeout(ctx, rehomeParticipantWait)
	defer cancel()
	log := c.logger.With().Str("job", j.ID).Str("state", j.State).Logger()
	if err := fn(wctx, j, wanted); err != nil {
		log.Warn().Err(err).Msg("Re-home: this node's part of the job's state did not finish; retrying")
		return
	}
	if !wanted {
		ok = true
		return
	}
	// The lease again: a node that lost it meanwhile may have been behind
	// when its reads ended. The next pass tries again.
	if ready, _ := c.ReadReady(); !ready {
		return
	}
	raw, err := json.Marshal(raft.AckRehomePayload{Job: j.ID, Node: c.LocalNodeID(), State: j.State})
	if err != nil {
		return
	}
	if err := c.applyOrForward(wctx, &raft.Command{Type: raft.CommandAckRehome, Payload: raw}); err != nil {
		log.Warn().Err(err).Msg("Re-home: acking the job's state failed; retrying")
		return
	}
	ok = true
	log.Debug().Msg("Re-home: acked the job's state")
	c.nudgeRehome()
}

// The driver's proposals. Each names this node; the FSM decides.

// RehomeStage proposes a job.
func (c *Coordinator) RehomeStage(ctx context.Context, p raft.StageRehomePayload) error {
	p.Origin = c.LocalNodeID()
	return c.rehomePropose(ctx, raft.CommandStageRehome, p)
}

// RehomeAdvance moves a job this node holds to state to.
func (c *Coordinator) RehomeAdvance(ctx context.Context, id, to string) error {
	return c.rehomePropose(ctx, raft.CommandAdvanceRehome, raft.AdvanceRehomePayload{Job: id, By: c.LocalNodeID(), To: to, Now: time.Now().UTC()})
}

// RehomeClaim takes or renews a job's lease for this node.
func (c *Coordinator) RehomeClaim(ctx context.Context, id string) error {
	return c.rehomePropose(ctx, raft.CommandClaimRehomeFinisher, raft.ClaimRehomeFinisherPayload{Job: id, By: c.LocalNodeID(), Now: time.Now().UTC()})
}

// RehomeEnd ends a job this node holds (see raft.EndRehomePayload).
func (c *Coordinator) RehomeEnd(ctx context.Context, id string) error {
	return c.rehomePropose(ctx, raft.CommandEndRehome, raft.EndRehomePayload{Job: id, By: c.LocalNodeID(), Now: time.Now().UTC()})
}

func (c *Coordinator) rehomePropose(ctx context.Context, t raft.CommandType, p any) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return c.applyOrForward(ctx, &raft.Command{Type: t, Payload: raw})
}

// RehomeJob returns a job as this node's FSM has it.
func (c *Coordinator) RehomeJob(id string) (raft.RehomeJob, bool) {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return raft.RehomeJob{}, false
	}
	return c.raftNode.FSM().RehomeJob(id)
}

// RehomeJobs returns the jobs as this node's FSM has them.
func (c *Coordinator) RehomeJobs() []raft.RehomeJob {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return nil
	}
	return c.raftNode.FSM().RehomeJobs()
}

// RehomeRunnable reports why a job cannot be run from this node now, or "":
// re-home off, a node or stream takeover out of scope, this node not synced.
func (c *Coordinator) RehomeRunnable() string {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return "this node is not part of a running cluster"
	}
	fsm := c.raftNode.FSM()
	if !fsm.RehomeEnabled() {
		return "re-home is not enabled in this cluster yet (every node must run a release that supports it, with compaction commit on); GET /api/v1/cluster shows what keeps it off"
	}
	if why := fsm.RehomeOutOfScope(); why != "" {
		return why
	}
	if ok, why := c.ReadReady(); !ok {
		return "this node is not synced with the cluster leader: " + why
	}
	return ""
}

// RehomeConfigBlockers returns the Raft servers that are not in the node
// table: a job's acks come from the node table only, so a step waits while
// one exists (it could be reading without acking).
func (c *Coordinator) RehomeConfigBlockers() ([]string, error) {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return nil, errors.New("raft not initialized")
	}
	ids, err := c.raftNode.ConfigurationServerIDs()
	if err != nil {
		return nil, err
	}
	fsm := c.raftNode.FSM()
	var out []string
	for _, id := range ids {
		if _, ok := fsm.GetNode(id); !ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// ManifestFile is a file's entry in this node's manifest.
func (c *Coordinator) ManifestFile(path string) (*raft.FileEntry, bool) {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return nil, false
	}
	return c.raftNode.FSM().GetFile(path)
}

// RehomeOwnsPath reports whether a re-home job owns path, in this node's
// record (the registration outbox asks).
func (c *Coordinator) RehomeOwnsPath(path string) bool {
	if c == nil || c.raftNode == nil || c.raftNode.FSM() == nil {
		return false
	}
	return c.raftNode.FSM().RehomeOwns(path)
}
