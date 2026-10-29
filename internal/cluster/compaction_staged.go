package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
)

// The staged compaction commit, cluster side (#1207 d2a2a). See
// raft/compaction_commit.go for the rules and compaction/watcher_staged.go
// for the compactor's steps. Here:
//
//   - the CompletionWatcher's StagedCommitter (Stage, Commit, Abort, ...);
//   - on every local-storage node with replication: pull the staged output of
//     every staged job, make it durable, report holding it (CompactionHeld);
//   - when a job commits, move the staged copy into place instead of pulling
//     the output; when one ends without a commit, remove the staged copy;
//   - a sweep of leftover staged directories, and on the leader the abort of
//     staged jobs whose compactor is gone.

// CapabilityLocalReplica is advertised by a node that keeps a local copy of
// every file (local storage with replication on): it can hold a staged
// output, so a compactor waits for one before it commits.
const CapabilityLocalReplica = "local_replica_v1"

const (
	stagedRescanEvery     = 30 * time.Second
	stagedFetchTimeout    = 30 * time.Minute
	stagedDeleteWait      = 5 * time.Second
	stagedOriginGoneAfter = 10 * time.Minute
	stagedLeaderEvery     = time.Minute
)

// stagedReplica is the coordinator's staged-commit state.
type stagedReplica struct {
	mu       sync.Mutex
	fetching map[string]bool // jobs with a fetch running
	wake     chan struct{}
	// localJob reports a completion manifest on this node for the job (its
	// staged directory is in use): the watcher's HasStagedJob.
	localJob func(job string) bool
	// holds reports a path this node's registration outbox still owns.
	holds func(path string) bool
	// holdAt is the outbox's RegistrationHoldAt: the finisher's finer view,
	// for a caller that synced after since (#1358). Set with holds, from the
	// same outbox.
	holdAt func(path string, since time.Time) RegistrationHold
	// goneSince: leader memory of when a staged job's origin was first seen
	// unhealthy (the leader loop's goroutine only).
	goneSince map[string]time.Time
	// replicaReady: this node pulls staged outputs (its puller and replica
	// loop run), so it advertises local replicas.
	replicaReady atomic.Bool
	// finisherWake runs the object finisher loop now; finishing: the jobs a
	// finisher goroutine works on (#1207 d2a2b).
	finisherWake chan struct{}
	finishing    map[string]bool
	// waitUntil: per compaction job, until when the delete worker waits for
	// its staged copy to move into place (one wait per job, not per input).
	waitUntil sync.Map
	// settlingWarned: per job, when the finisher last warned that an input's
	// registration has not settled (#1358).
	settlingWarned sync.Map
}

// SetStagedCompactionHooks wires the compactor's and the outbox's views:
// localJob is the CompletionWatcher's HasStagedJob, outbox this node's
// registration outbox (nil when there is none). Both of the outbox's views
// come from one value so they cannot be wired apart. Call before Start.
func (c *Coordinator) SetStagedCompactionHooks(localJob func(job string) bool, outbox StagedRegistrationHolder) {
	c.staged.mu.Lock()
	defer c.staged.mu.Unlock()
	c.staged.localJob, c.staged.holds, c.staged.holdAt = localJob, nil, nil
	if outbox != nil {
		c.staged.holds, c.staged.holdAt = outbox.HoldsRegistration, outbox.RegistrationHoldAt
	}
}

// StagedRegistrationHolder is the registration outbox as the staged-commit
// paths see it (*RegistrationOutbox).
type StagedRegistrationHolder interface {
	HoldsRegistration(path string) bool
	RegistrationHoldAt(path string, since time.Time) RegistrationHold
}

func (c *Coordinator) stagedLocalJob(job string) bool {
	c.staged.mu.Lock()
	fn := c.staged.localJob
	c.staged.mu.Unlock()
	return fn != nil && fn(job)
}

// keepsLocalReplicas reports whether this node keeps a local copy of every
// file, and so can hold staged outputs.
func (c *Coordinator) keepsLocalReplicas() bool {
	return c.storage != nil && c.storage.Type() == "local" && c.cfg.ReplicationEnabled && c.staged.replicaReady.Load()
}

// stagedMinHolders is how many other nodes must hold a staged output before
// it commits: 1 when another member keeps local replicas, else 0.
func (c *Coordinator) stagedMinHolders() int {
	fsm := c.raftNode.FSM()
	for _, n := range fsm.GetAllNodes() {
		if n.ID != c.localNode.ID && hasCapability(n.Capabilities, CapabilityLocalReplica) {
			return 1
		}
	}
	return 0
}

// StagedCompactionMode reports whether a compaction launched now is staged:
// the cluster's compaction commit is on and storage is local, or an object
// store that copies objects in place (#1207 d2a2b).
func (c *Coordinator) StagedCompactionMode() bool {
	if !c.CompactionCommitActive() || c.storage == nil {
		return false
	}
	_, object := c.storage.(storage.ObjectCopier)
	return c.storage.Type() == "local" || object
}

// stagedObject reports whether staged jobs here are object-store jobs.
func (c *Coordinator) stagedObject() bool {
	return IsSharedObjectStore(c.storage)
}

// CompactionInputLookup describes path as the cluster manifest lists it, for
// a staged job: false unless it is a hot registered file no staged job owns.
func (c *Coordinator) CompactionInputLookup(path string) (int64, string, bool) {
	fsm := c.raftNode.FSM()
	e, ok := fsm.GetFile(path)
	if !ok || (e.Tier != "" && e.Tier != "hot") || fsm.StagedCompactionOwns(path) {
		return 0, "", false
	}
	return e.SizeBytes, e.SHA256, true
}

// stagedCommitter is the CompletionWatcher's StagedCommitter.
type stagedCommitter struct{ c *Coordinator }

// StagedCommitter returns the CompletionWatcher's view of the cluster.
func (c *Coordinator) StagedCommitter() compaction.StagedCommitter { return stagedCommitter{c} }

func (s stagedCommitter) outcome(err error) (compaction.StagedOutcome, error) {
	if err == nil {
		return compaction.StagedOK, nil
	}
	switch raft.CompactionConflictReason(err) {
	case raft.CompactionCommitted:
		return compaction.StagedCommitted, err
	case raft.CompactionPaused, raft.CompactionFull, raft.CompactionHolders:
		return compaction.StagedRetry, err
	case "":
		// Not a conflict: the FSM refused the command outright (a job id or
		// payload it will never accept), or it never got an answer (no
		// leader, a timeout, a lost connection). Only the second is worth
		// another try.
		if s.c.fsmRejected(err) {
			return compaction.StagedRefused, err
		}
		return compaction.StagedRetry, err
	}
	return compaction.StagedRefused, err
}

// fsmRejected reports an error that is the FSM's own answer to a command,
// as opposed to the command not reaching it (no leader, a timeout, a lost
// connection, a Raft failure).
func (c *Coordinator) fsmRejected(err error) bool {
	if err == nil || isTransientLeaderError(err) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	if strings.Contains(err.Error(), "failed to apply command") || strings.Contains(err.Error(), "raft not initialized") {
		return false
	}
	var fr *ForwardRejectedError
	if errors.As(err, &fr) {
		return fr.Code == protocol.ForwardCodeApplyFailed
	}
	// Applied here: an error that is not Raft's is the FSM's.
	return c.raftNode != nil && c.raftNode.IsLeader()
}

func (s stagedCommitter) Stage(ctx context.Context, job compaction.StagedJob) (compaction.StagedOutcome, error) {
	// The output's Tier is the storage tier a new file lands in ("hot"), as
	// every other registration has it; job.Output.Tier is the compaction
	// tier ("hourly", "daily").
	out := job.Output
	minHolders := 0
	if !job.Object {
		minHolders = s.c.stagedMinHolders()
	}
	ins := make([]raft.CompactionInput, 0, len(job.Inputs))
	for _, in := range job.Inputs {
		ins = append(ins, raft.CompactionInput{Path: in.Path, SizeBytes: in.SizeBytes, SHA256: in.SHA256})
	}
	raw, err := json.Marshal(raft.StageCompactionPayload{
		Job: job.JobID, Origin: s.c.localNode.ID, MinHolders: minHolders, Object: job.Object, Now: time.Now().UTC(), Inputs: ins,
		Output: raft.FileEntry{Path: out.Path, SHA256: out.SHA256, SizeBytes: out.SizeBytes, Database: out.Database,
			Measurement: out.Measurement, PartitionTime: out.PartitionTime, Tier: "hot", CreatedAt: out.CreatedAt},
	})
	if err != nil {
		return compaction.StagedRefused, err
	}
	return s.outcome(s.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandStageCompaction, Payload: raw}))
}

func (s stagedCommitter) Commit(ctx context.Context, job compaction.StagedJob) (compaction.StagedOutcome, error) {
	raw, err := json.Marshal(raft.CommitCompactionPayload{Job: job.JobID, Origin: s.c.localNode.ID,
		Output: job.Output.Path, SHA256: job.Output.SHA256, Now: time.Now().UTC()})
	if err != nil {
		return compaction.StagedRefused, err
	}
	// A commit retried after it applied (its reply lost) is the FSM's to
	// recognise: it answers success for its own output.
	out, err := s.outcome(s.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandCommitCompaction, Payload: raw}))
	if job.Object && (out == compaction.StagedOK || out == compaction.StagedCommitted) {
		s.c.wakeFinisher()
	}
	return out, err
}

func (s stagedCommitter) Abort(ctx context.Context, jobID, reason string) error {
	raw, err := json.Marshal(raft.AbortCompactionPayload{Job: jobID, By: s.c.localNode.ID, Reason: reason})
	if err != nil {
		return err
	}
	sj, staged := s.c.raftNode.FSM().StagedCompactionByJob(jobID)
	if err := s.c.applyOrForward(ctx, &raft.Command{Type: raft.CommandAbortCompaction, Payload: raw}); err != nil {
		return err
	}
	if staged && sj.Object && !sj.Finalizing {
		// The staged output of an object job is in the shared bucket:
		// nobody else removes it (the sweep would, hours later).
		_ = s.c.deleteStagedObject(ctx, raft.StagedCompactionKey(sj.Job, sj.Output.Path))
	}
	return nil
}

func (s stagedCommitter) Status(job compaction.StagedJob) compaction.StagedStatus {
	fsm := s.c.raftNode.FSM()
	var st compaction.StagedStatus
	if sj, ok := fsm.StagedCompactionByJob(job.JobID); ok && sj.Object == job.Object && sj.Output.SHA256 == job.Output.SHA256 {
		if sj.Finalizing {
			st.Finalizing = true
		} else {
			st.Staged, st.Holders, st.MinHolders = true, len(sj.Holders), sj.MinHolders
		}
	}
	if e, ok := fsm.GetFile(job.Output.Path); ok && e.CompactionJob == job.JobID && e.SHA256 == job.Output.SHA256 {
		st.Committed = true
	}
	return st
}

func (s stagedCommitter) SyncManifest(ctx context.Context) error { return s.c.SyncManifest(ctx) }

func (s stagedCommitter) Registered(path string) bool {
	_, ok := s.c.raftNode.FSM().GetFile(path)
	return ok
}

func (s stagedCommitter) HoldsRegistration(path string) bool {
	s.c.staged.mu.Lock()
	fn := s.c.staged.holds
	s.c.staged.mu.Unlock()
	return fn != nil && fn(path)
}

func (s stagedCommitter) Adopt(ctx context.Context, job compaction.StagedJob) error {
	e, ok := s.c.raftNode.FSM().GetFile(job.Output.Path)
	if !ok || e.CompactionJob != job.JobID {
		return errors.New("the output is not registered by this job")
	}
	if s.c.puller != nil {
		if s.c.puller.AdoptNow(e) {
			return nil
		}
		return errors.New("the staged copy could not be moved into place now")
	}
	if ok, err := s.c.adoptStaged(ctx, e); !ok {
		return fmt.Errorf("the staged copy could not be moved into place: %v", err)
	}
	return nil
}

func (s stagedCommitter) ForcePull(job compaction.StagedJob) {
	if e, ok := s.c.raftNode.FSM().GetFile(job.Output.Path); ok && s.c.puller != nil {
		s.c.puller.PullOwn(e)
	}
}

func (s stagedCommitter) OwnStaged() []string {
	var out []string
	for _, sj := range s.c.raftNode.FSM().StagedCompactions() {
		if sj.Origin == s.c.localNode.ID && !sj.Finalizing {
			out = append(out, sj.Job)
		}
	}
	return out
}

// adoptStaged moves this node's staged copy of a committed output into place
// (the puller's AdoptStaged): true when the output is at its path.
func (c *Coordinator) adoptStaged(ctx context.Context, e *raft.FileEntry) (bool, error) {
	return c.adoptStagedCopy(ctx, e, true)
}

// adoptStagedForPull is the puller's AdoptStaged: only an actual move counts.
// The puller already decided the file at the path will not do (a forced
// refresh of a same-size copy), so its presence is not an adoption.
func (c *Coordinator) adoptStagedForPull(ctx context.Context, e *raft.FileEntry) (bool, error) {
	return c.adoptStagedCopy(ctx, e, false)
}

func (c *Coordinator) adoptStagedCopy(ctx context.Context, e *raft.FileEntry, presentCounts bool) (bool, error) {
	mover, ok := c.storage.(storage.ObjectMover)
	if !ok || e.CompactionJob == "" {
		return false, nil
	}
	key := raft.StagedCompactionKey(e.CompactionJob, e.Path)
	dir := compaction.StagedJobDir(e.CompactionJob)
	if !raft.HotTier(e.Tier) {
		// Moved to the cold tier since (tiering marks a file cold only once
		// its cold copy is written): a hot copy here would read twice.
		if !c.stagedLocalJob(e.CompactionJob) {
			c.logger.Warn().Str("job", e.CompactionJob).Str("path", e.Path).Str("tier", e.Tier).
				Msg("Committed compaction output is no longer hot; removing its staged copy instead of moving it into place")
			_ = mover.RemoveAll(ctx, dir)
		}
		return false, nil
	}
	if size, err := c.storage.StatFile(ctx, e.Path); err == nil && size == e.SizeBytes && presentCounts {
		if !c.stagedLocalJob(e.CompactionJob) {
			_ = mover.RemoveAll(ctx, dir)
		}
		return true, nil
	}
	size, err := c.storage.StatFile(ctx, key)
	if err != nil || size != e.SizeBytes {
		return false, err
	}
	if err := mover.MoveObject(ctx, key, e.Path); err != nil {
		return false, err
	}
	// Moved after the output left the manifest (a DELETE or retention
	// removed it): not data any more.
	cur, ok := c.raftNode.FSM().GetFile(e.Path)
	if !ok || cur.CompactionJob != e.CompactionJob || cur.SHA256 != e.SHA256 {
		_ = c.storage.Delete(ctx, e.Path)
		return false, errors.New("the output left the manifest while it moved into place")
	}
	if !c.stagedLocalJob(e.CompactionJob) {
		_ = mover.RemoveAll(ctx, dir)
	}
	// This node's compactor wrote these bytes: what the path holds (#1182).
	c.recordLocalContent(cur, "", cur.SHA256)
	c.logger.Debug().Str("job", e.CompactionJob).Str("path", e.Path).Msg("Committed compaction output moved into place from its staged copy")
	return true, nil
}

// onCompactionStaged / Gone / Committed are the FSM callbacks. They must not
// block: each only wakes the staged worker or hands work to a goroutine.
func (c *Coordinator) onCompactionStaged(raft.StagedCompaction) { c.wakeStaged() }

func (c *Coordinator) onCompactionGone(s raft.StagedCompaction) {
	go c.endStagedCopy(s)
}

func (c *Coordinator) onCompactionCommitted(job string, out raft.FileEntry) {
	out.CompactionJob = job
	go func() {
		if c.puller != nil {
			c.puller.AdoptNow(&out)
			return
		}
		_, _ = c.adoptStaged(c.ctx, &out)
	}()
}

// endStagedCopy handles a staged job that left the FSM without a commit
// event (aborted, removed, or dropped by a snapshot): a copy of an output the
// manifest now registers by that job is moved into place (a snapshot taken
// after the commit), any other is removed, unless this node's compactor still
// uses the directory.
func (c *Coordinator) endStagedCopy(s raft.StagedCompaction) {
	if s.Object || c.storage == nil {
		return
	}
	if e, ok := c.raftNode.FSM().GetFile(s.Output.Path); ok && e.CompactionJob == s.Job {
		if c.puller != nil && c.puller.AdoptNow(e) {
			return
		}
		if ok, _ := c.adoptStaged(c.ctx, e); ok {
			return
		}
		// Committed, and the copy here is missing or partial (a fetch the
		// commit cut short; the staged key is no longer served): pull the
		// output itself from a node that has it.
		if c.puller != nil {
			c.puller.PullOwn(e)
		}
	}
	if c.stagedLocalJob(s.Job) {
		return
	}
	if mover, ok := c.storage.(storage.ObjectMover); ok {
		_ = mover.RemoveAll(c.ctx, compaction.StagedJobDir(s.Job))
	}
}

func (c *Coordinator) wakeStaged() {
	select {
	case c.staged.wake <- struct{}{}:
	default:
	}
}

// stagedReplicaLoop pulls the staged outputs this node does not hold, and
// sweeps leftover staged directories. Started on local-storage nodes with
// replication.
func (c *Coordinator) stagedReplicaLoop(ctx context.Context) {
	t := time.NewTicker(stagedRescanEvery)
	defer t.Stop()
	c.sweepStagedDirs(ctx)
	for {
		c.pullStagedOutputs(ctx)
		select {
		case <-ctx.Done():
			return
		case <-c.staged.wake:
		case <-t.C:
			c.sweepStagedDirs(ctx)
		}
	}
}

// pullStagedOutputs starts a fetch for every staged job this node should
// hold and does not: one fetch per job at a time.
func (c *Coordinator) pullStagedOutputs(ctx context.Context) {
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.CompactionCommitEnabled() {
		return
	}
	for _, s := range fsm.StagedCompactions() {
		if s.Object || s.Finalizing || s.Origin == c.localNode.ID || contains(s.Holders, c.localNode.ID) {
			continue
		}
		c.staged.mu.Lock()
		busy := c.staged.fetching[s.Job]
		if !busy {
			c.staged.fetching[s.Job] = true
		}
		c.staged.mu.Unlock()
		if busy {
			continue
		}
		go func(s raft.StagedCompaction) {
			defer func() {
				c.staged.mu.Lock()
				delete(c.staged.fetching, s.Job)
				c.staged.mu.Unlock()
			}()
			if err := c.holdStagedOutput(ctx, s); err != nil {
				c.logger.Debug().Err(err).Str("job", s.Job).Msg("Pulling a staged compaction output failed; retrying")
			}
		}(s)
	}
}

// holdStagedOutput makes this node hold s's staged output durably, then
// reports it.
func (c *Coordinator) holdStagedOutput(ctx context.Context, s raft.StagedCompaction) error {
	mover, ok := c.storage.(storage.ObjectMover)
	if !ok {
		return errors.New("storage cannot hold a staged output")
	}
	key := raft.StagedCompactionKey(s.Job, s.Output.Path)
	if !c.stagedCopyMatches(ctx, key, s.Output) {
		if err := c.fetchStagedOutput(ctx, s, key); err != nil {
			return err
		}
	}
	if err := mover.Sync(ctx, key); err != nil {
		return err
	}
	// Still staged? A job that ended meanwhile is not held.
	if cur, ok := c.raftNode.FSM().StagedCompactionByJob(s.Job); !ok || cur.Finalizing {
		c.endStagedCopy(s)
		return nil
	}
	raw, err := json.Marshal(raft.CompactionHeldPayload{Job: s.Job, Node: c.localNode.ID, SHA256: s.Output.SHA256})
	if err != nil {
		return err
	}
	return c.applyOrForward(ctx, &raft.Command{Type: raft.CommandCompactionHeld, Payload: raw})
}

// stagedCopyMatches reports a local copy at key with the output's size and
// checksum.
func (c *Coordinator) stagedCopyMatches(ctx context.Context, key string, out raft.FileEntry) bool {
	size, err := c.storage.StatFile(ctx, key)
	if err != nil || size != out.SizeBytes {
		return false
	}
	h := sha256.New()
	if err := c.storage.ReadTo(ctx, key, h); err != nil {
		return false
	}
	return fmt.Sprintf("%x", h.Sum(nil)) == out.SHA256
}

// fetchStagedOutput fetches s's staged output from its compactor, or from a
// node that holds it, into key. The fetch verifies the checksum as it reads.
func (c *Coordinator) fetchStagedOutput(ctx context.Context, s raft.StagedCompaction, key string) error {
	if c.fetcher == nil {
		return errors.New("no fetch client")
	}
	entry := raft.FileEntry{Path: key, SHA256: s.Output.SHA256, SizeBytes: s.Output.SizeBytes}
	var lastErr error
	for _, id := range append([]string{s.Origin}, s.Holders...) {
		n, ok := c.registry.Get(id)
		if !ok || n.Address == "" || id == c.localNode.ID {
			continue
		}
		fctx, cancel := context.WithTimeout(ctx, stagedFetchTimeout)
		pr, pw := io.Pipe()
		done := make(chan error, 1)
		go func() { done <- c.storage.WriteReader(fctx, key, pr, s.Output.SizeBytes) }()
		_, ferr := c.fetcher.Fetch(fctx, n.Address, &entry, pw, 0, nil)
		pw.CloseWithError(ferr)
		werr := <-done
		cancel()
		if ferr == nil && werr == nil {
			return nil
		}
		if ferr == nil {
			ferr = werr
		}
		lastErr = ferr
		_ = c.storage.Delete(ctx, key)
	}
	if lastErr == nil {
		lastErr = errors.New("no node to fetch it from")
	}
	return lastErr
}

// sweepStagedDirs removes staged directories nothing uses any more, and
// moves into place a copy whose job committed (a commit event this node
// missed). A directory is kept while this node's compactor has a manifest
// for its job, or the job is staged.
func (c *Coordinator) sweepStagedDirs(ctx context.Context) {
	lister, ok := c.storage.(interface {
		ListDirectories(ctx context.Context, prefix string) ([]string, error)
	})
	if !ok {
		return
	}
	dirs, err := lister.ListDirectories(ctx, compaction.StagedRoot+"/")
	if err != nil || len(dirs) == 0 {
		return
	}
	if c.SyncManifest(ctx) != nil {
		return
	}
	fsm := c.raftNode.FSM()
	for _, d := range dirs {
		job := strings.Trim(strings.TrimPrefix(strings.TrimSuffix(d, "/"), compaction.StagedRoot), "/")
		if job == "" || strings.Contains(job, "/") || c.stagedLocalJob(job) {
			continue
		}
		if s, ok := fsm.StagedCompactionByJob(job); ok && !s.Finalizing {
			continue
		}
		if e, ok := fsm.FileByCompactionJob(job); ok {
			if c.puller != nil && c.puller.AdoptNow(e) {
				continue
			}
			if ok, _ := c.adoptStaged(ctx, e); ok {
				continue
			}
		}
		if mover, ok := c.storage.(storage.ObjectMover); ok {
			_ = mover.RemoveAll(ctx, compaction.StagedJobDir(job))
		}
	}
}

// waitStagedAdoption delays the unlink of a compaction input while this node
// holds a staged copy of the job's output not yet moved into place, so local
// reads do not lose the rows in between (bounded).
// It reports whether it waited at all (#1271: the caller then reads the
// manifest again).
func (c *Coordinator) waitStagedAdoption(reason string) (waited bool) {
	job, ok := strings.CutPrefix(reason, "compaction:")
	if !ok || job == "" || c.storage == nil {
		return false
	}
	v, _ := c.staged.waitUntil.LoadOrStore(job, time.Now().Add(stagedDeleteWait))
	deadline := v.(time.Time)
	for time.Now().Before(deadline) {
		ok, err := c.storage.Exists(c.ctx, compaction.StagedJobDir(job))
		if err != nil || !ok {
			return waited
		}
		waited = true
		select {
		case <-c.ctx.Done():
			return waited
		case <-time.After(100 * time.Millisecond):
		}
	}
	return waited
}

// stagedLeaderLoop, on the leader, aborts staged jobs whose compactor has
// been unhealthy for stagedOriginGoneAfter: nothing will commit them, and
// they keep their inputs out of compaction.
func (c *Coordinator) stagedLeaderLoop(ctx context.Context) {
	t := time.NewTicker(stagedLeaderEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		c.staged.waitUntil.Range(func(k, v any) bool {
			if now.Sub(v.(time.Time)) > time.Minute {
				c.staged.waitUntil.Delete(k)
			}
			return true
		})
		if !c.raftNode.IsLeader() {
			continue
		}
		staged := c.raftNode.FSM().StagedCompactions()
		live := make(map[string]bool, len(staged))
		for _, s := range staged {
			live[s.Job] = true
		}
		for job := range c.staged.goneSince {
			if !live[job] {
				delete(c.staged.goneSince, job)
			}
		}
		for _, s := range staged {
			if s.Finalizing || c.originHealthy(s.Origin) {
				delete(c.staged.goneSince, s.Job)
				continue
			}
			first, seen := c.staged.goneSince[s.Job]
			if !seen {
				c.staged.goneSince[s.Job] = now
				continue
			}
			if now.Sub(first) < stagedOriginGoneAfter {
				continue
			}
			raw, _ := json.Marshal(raft.AbortCompactionPayload{Job: s.Job, By: c.localNode.ID, Reason: "its compactor is gone"})
			if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandAbortCompaction, Payload: raw}, 5*time.Second); err == nil {
				delete(c.staged.goneSince, s.Job)
				if s.Object {
					// Its staged output is in the shared bucket: nobody else
					// removes it (the bucket sweep would, later).
					_ = c.storage.Delete(ctx, raft.StagedCompactionKey(s.Job, s.Output.Path))
				}
				c.logger.Warn().Str("job", s.Job).Str("origin", s.Origin).Msg("Aborted a staged compaction: its compactor is gone")
			}
		}
	}
}

// CompactionOutputHeld reports a registered compaction output this node is
// moving into place from its staged copy (the reconciler must not take its
// absence for an orphan entry).
func (c *Coordinator) CompactionOutputHeld(path string) bool {
	fsm := c.raftNode.FSM()
	e, ok := fsm.GetFile(path)
	if !ok || e.CompactionJob == "" || c.storage == nil {
		return false
	}
	// An object job's output is absent from its path until its finisher
	// copies it there (#1207 d2a2b).
	if sj, staged := fsm.StagedCompactionByJob(e.CompactionJob); staged && sj.Object && sj.Finalizing {
		return true
	}
	ok, err := c.storage.Exists(c.ctx, raft.StagedCompactionKey(e.CompactionJob, e.Path))
	return err == nil && ok
}
