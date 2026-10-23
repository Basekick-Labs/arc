package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// The finisher of a committed object-store compaction (#1207 d2a2b). The
// commit registered the output and unregistered the inputs; the output waits
// in the bucket under its staged key and the input objects are still where
// they were, so reads see the inputs' rows. The finisher, the one node that
// holds the job's finisher lease:
//
//  1. copies the staged object to the output's path (the store copies it in
//     place; the object appears only once complete);
//  2. deletes the input objects;
//  3. finalizes the job, deletes the staged object, and ends the job.
//
// If the output left the manifest meanwhile, the reason decides: a remover
// that accounts for its rows (a DELETE, retention, a database deletion)
// means the inputs go too and the job ends; any other (the reconciler) means
// the inputs are registered again (revert), provided their objects are all
// there. Otherwise the job is marked wedged for an operator. Every step is
// safe to repeat; the job's compactor runs it at once, the leader takes over
// a job that has been finalizing for objectFinisherTakeover.

const (
	objectFinisherEvery    = 15 * time.Second
	objectFinisherTakeover = 5 * time.Minute
	objectStagedSweepEvery = 10 * time.Minute
	// objectStagedSweepAge: a staged object no job owns is removed once its
	// last modification is this old (store clocks may differ from ours; a
	// job never stages an output older than 30 minutes).
	objectStagedSweepAge = 6 * time.Hour
)

// objectFinisherDelay delays the finisher's first step of a job (a test-only
// knob for rigs: ARC_TEST_FINISHER_DELAY).
var objectFinisherDelay time.Duration

// objectFinisherLoop finishes the committed object jobs this node should
// finish, and, on the leader, sweeps staged objects no job owns.
func (c *Coordinator) objectFinisherLoop(ctx context.Context) {
	t := time.NewTicker(objectFinisherEvery)
	defer t.Stop()
	lastSweep := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.staged.finisherWake:
		case <-t.C:
		}
		fsm := c.raftNode.FSM()
		if fsm == nil || !fsm.CompactionCommitEnabled() {
			continue
		}
		leader := c.raftNode.IsLeader()
		now := time.Now()
		for _, s := range fsm.StagedCompactions() {
			if !finisherEligible(s, c.localNode.ID, leader, now) {
				continue
			}
			if objectFinisherDelay > 0 && now.Sub(s.FinalizingAt) < objectFinisherDelay {
				continue
			}
			// One goroutine per job: a long copy holds up no other job.
			c.staged.mu.Lock()
			busy := c.staged.finishing[s.Job]
			if !busy {
				c.staged.finishing[s.Job] = true
			}
			c.staged.mu.Unlock()
			if busy {
				continue
			}
			go func(job string) {
				defer func() {
					c.staged.mu.Lock()
					delete(c.staged.finishing, job)
					c.staged.mu.Unlock()
				}()
				if err := c.finishObjectJob(ctx, job); err != nil {
					c.logger.Debug().Err(err).Str("job", job).Msg("Finishing a committed compaction deferred; retrying")
				}
			}(s.Job)
		}
		if leader && time.Since(lastSweep) > objectStagedSweepEvery {
			lastSweep = time.Now()
			c.sweepStagedObjects(ctx)
		}
	}
}

// finisherEligible reports whether self should try to finish s now: its
// compactor, or the node holding its lease, at once; the leader once the job
// has been neither committed nor claimed for objectFinisherTakeover (its
// claim is granted only once the lease expired).
func finisherEligible(s raft.StagedCompaction, self string, leader bool, now time.Time) bool {
	if !s.Object || !s.Finalizing || s.Wedged {
		return false
	}
	if s.Origin == self || s.FinisherNode == self {
		return true
	}
	last := s.FinalizingAt
	if s.ClaimedAt.After(last) {
		last = s.ClaimedAt
	}
	return leader && now.Sub(last) > objectFinisherTakeover
}

// finisherRenewEvery is how often a finisher renews its lease during a long
// step (a variable for tests).
var finisherRenewEvery = time.Minute

// withLease runs fn while renewing job's finisher lease; a refused renewal
// cancels fn, whose error is then the refusal.
func (c *Coordinator) withLease(ctx context.Context, job string, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	done, stopped := make(chan struct{}), make(chan struct{})
	every := finisherRenewEvery
	go func() {
		defer close(stopped)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := c.claimFinisher(ctx, job, false); err != nil {
					cancel(fmt.Errorf("finisher lease not renewed: %w", err))
					return
				}
			}
		}
	}()
	err := fn(ctx)
	cause := context.Cause(ctx)
	close(done)
	cancel(nil)
	<-stopped // no renewal outlives the call
	if err != nil && cause != nil {
		return cause
	}
	return err
}

// afterCopy removes what a copy left at the output's path when the job no
// longer owns that path: it ended (another finisher, a revert) or its output
// left the manifest while the copy ran. A registered object is never touched.
func (c *Coordinator) afterCopy(ctx context.Context, s raft.StagedCompaction) {
	if err := c.SyncManifest(ctx); err != nil {
		return
	}
	cur, ok := c.raftNode.FSM().StagedCompactionByJob(s.Job)
	if ok && c.ownsOutput(cur) {
		return
	}
	if err := c.deleteUnregistered(ctx, s.Output.Path); err != nil {
		c.logger.Warn().Err(err).Str("job", s.Job).Str("path", s.Output.Path).
			Msg("Could not remove a compaction output copied after its job ended; the reconciler's orphan sweep removes it")
	}
}

// wakeFinisher runs the finisher loop now (a commit applied).
func (c *Coordinator) wakeFinisher() {
	select {
	case c.staged.finisherWake <- struct{}{}:
	default:
	}
}

var errFinisherLease = errors.New("another node holds the finisher lease")

// claimFinisher takes or renews job's finisher lease, then catches up with
// the manifest: a step decided after it sees every change before the claim.
func (c *Coordinator) claimFinisher(ctx context.Context, job string, wedged bool) error {
	if wedged {
		metrics.Get().IncCompactionWedged()
	}
	raw, err := json.Marshal(raft.ClaimFinisherPayload{Job: job, By: c.localNode.ID, Now: time.Now().UTC(), Wedged: wedged})
	if err != nil {
		return err
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandClaimFinisher, Payload: raw}); err != nil {
		if raft.CompactionConflictReason(err) == raft.CompactionClaimed {
			return errFinisherLease
		}
		return err
	}
	return c.SyncManifest(ctx)
}

// finishObjectJob takes one committed object job as far as it can go.
func (c *Coordinator) finishObjectJob(ctx context.Context, job string) error {
	copier, ok := c.storage.(storage.ObjectCopier)
	if !ok {
		return errors.New("the storage backend cannot copy objects")
	}
	if err := c.claimFinisher(ctx, job, false); err != nil {
		return err
	}
	s, ok := c.raftNode.FSM().StagedCompactionByJob(job)
	if !ok || !s.Finalizing || s.Wedged {
		return nil
	}
	staged := raft.StagedCompactionKey(s.Job, s.Output.Path)
	if s.Finalized {
		return c.endObjectJob(ctx, s, staged, false)
	}
	if !c.ownsOutput(s) {
		return c.outputLeft(ctx, s, staged)
	}
	var present bool
	if err := c.withLease(ctx, job, func(ctx context.Context) error {
		var err error
		present, err = c.objectAt(ctx, copier, s.Output.Path, s.Output.SizeBytes, s.Output.SHA256)
		return err
	}); err != nil {
		return err
	}
	if !present {
		size, sha, err := copier.ObjectSHA(ctx, staged)
		if err != nil {
			return err
		}
		if size != s.Output.SizeBytes || sha != s.Output.SHA256 {
			// Nothing to copy from: the inputs are the data.
			c.logger.Error().Str("job", s.Job).Str("staged", staged).Int64("size", size).
				Msg("Committed compaction has no staged output to copy; registering its inputs again")
			return c.revertObjectJob(ctx, s, staged)
		}
		if err := c.withLease(ctx, job, func(ctx context.Context) error {
			return copier.CopyObject(ctx, staged, s.Output.Path, s.Output.SizeBytes, s.Output.SHA256)
		}); err != nil {
			c.afterCopy(ctx, s)
			return err
		}
		// The output may have left the manifest during the copy, or the
		// lease another node.
		if err := c.claimFinisher(ctx, job, false); err != nil {
			c.afterCopy(ctx, s)
			return err
		}
		cur, ok := c.raftNode.FSM().StagedCompactionByJob(job)
		if !ok {
			c.afterCopy(ctx, s)
			return nil
		}
		s = cur
		if !c.ownsOutput(s) {
			return c.outputLeft(ctx, s, staged)
		}
	}
	if err := c.deleteObjectInputs(ctx, s); err != nil {
		return err
	}
	raw, _ := json.Marshal(raft.FinalizeCompactionPayload{Job: s.Job, By: c.localNode.ID, Output: s.Output.Path})
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandFinalizeCompaction, Payload: raw}); err != nil {
		return err
	}
	return c.endObjectJob(ctx, s, staged, false)
}

// ownsOutput reports whether the job's output is still registered by it.
func (c *Coordinator) ownsOutput(s raft.StagedCompaction) bool {
	e, ok := c.raftNode.FSM().GetFile(s.Output.Path)
	return ok && e.CompactionJob == s.Job && e.SHA256 == s.Output.SHA256
}

// objectAt reports whether path holds the output (size and checksum; an
// object without a recorded checksum is hashed).
func (c *Coordinator) objectAt(ctx context.Context, copier storage.ObjectCopier, path string, size int64, sha string) (bool, error) {
	got, recorded, err := copier.ObjectSHA(ctx, path)
	if err != nil || got != size {
		return false, err
	}
	if recorded != "" {
		return recorded == sha, nil
	}
	h := sha256.New()
	if err := c.storage.ReadTo(ctx, path, h); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == sha, nil
}

// errInputsSettling defers a finish: an input's registration here is not
// settled yet (#1358). The loop retries on its next tick.
var errInputsSettling = errors.New("an input's registration on this node has not settled")

// inputsSettlingWarnAfter: a job deferred this long is logged at Warn. A
// confirmation settles within outboxRecentWindow; a pending registration
// waits for its file's next registration attempt.
const inputsSettlingWarnAfter = 5 * time.Minute

// settlingWarnDue rate-limits the settling Warn to once per
// inputsSettlingWarnAfter per job (the loop retries every few seconds).
func (c *Coordinator) settlingWarnDue(job string) bool {
	now := time.Now()
	if last, ok := c.staged.settlingWarned.Load(job); ok && now.Sub(last.(time.Time)) < inputsSettlingWarnAfter {
		return false
	}
	c.staged.settlingWarned.Store(job, now)
	return true
}

// finisherSync is the sync deleteObjectInputs anchors on (a test seam).
var finisherSync = func(ctx context.Context, c *Coordinator) error { return c.SyncManifest(ctx) }

// deleteObjectInputs deletes the job's input objects, except a path the
// manifest lists again or this node's outbox still owns (a restore or a new
// write reused it). Every delete must succeed.
//
// It decides with a time taken BEFORE a sync with the leader (#1358). A
// confirmation this node's outbox recorded before then is of a registration
// the sync has applied here; if the manifest does not list the path, the
// commit (or a later entry) removed it, and the object goes. Before #1358 any
// confirmation of the last minute kept its input and the job ended anyway:
// an input registered less than a minute before the commit was never deleted,
// and every node read its rows twice for good. Taking the time after the
// sync would let a registration confirmed during the sync, not yet applied
// here, delete a live file.
//
// A registration still pending here, or confirmed after that time, defers
// the whole job (errInputsSettling) rather than ending it with the object
// left: it settles as registered (kept) or not (deleted) on a later attempt.
// A parked one waits for an operator: its object is kept and the job ends, as
// before. Every input is classified before any is deleted.
func (c *Coordinator) deleteObjectInputs(ctx context.Context, s raft.StagedCompaction) error {
	t0 := time.Now()
	if err := finisherSync(ctx, c); err != nil {
		return err
	}
	fsm := c.raftNode.FSM()
	c.staged.mu.Lock()
	holdAt := c.staged.holdAt
	c.staged.mu.Unlock()
	var del []string
	settling := ""
	for _, in := range s.Inputs {
		if _, reg := fsm.GetFile(in.Path); reg {
			continue
		}
		hold := HoldNone
		if holdAt != nil {
			hold = holdAt(in.Path, t0)
		}
		switch hold {
		case HoldParked:
			c.logger.Warn().Str("job", s.Job).Str("input", in.Path).
				Msg("Committed compaction keeps an input this node parked as refused: its object stays until an operator resolves the parked file")
		case HoldPending, HoldRecent:
			settling = in.Path
		default:
			del = append(del, in.Path)
		}
	}
	if settling != "" {
		if !s.FinalizingAt.IsZero() && time.Since(s.FinalizingAt) > inputsSettlingWarnAfter && c.settlingWarnDue(s.Job) {
			c.logger.Warn().Str("job", s.Job).Str("input", settling).Dur("since_commit", time.Since(s.FinalizingAt)).
				Msg("Committed compaction is still waiting for an input's registration on this node to settle; its input objects are not deleted until it does")
		}
		return fmt.Errorf("%w: %s", errInputsSettling, settling)
	}
	for _, p := range del {
		if err := c.storage.Delete(ctx, p); err != nil && !storage.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// outputLeft ends a job whose output left the manifest after its commit.
func (c *Coordinator) outputLeft(ctx context.Context, s raft.StagedCompaction, staged string) error {
	if raft.OutputGoneAccounted(s.OutputGoneReason) {
		// Its rows were dealt with by the remover; the inputs hold the same
		// rows and go too.
		if err := c.deleteUnregistered(ctx, s.Output.Path); err != nil {
			return err
		}
		if err := c.deleteObjectInputs(ctx, s); err != nil {
			return err
		}
		return c.endObjectJob(ctx, s, staged, true)
	}
	return c.revertObjectJob(ctx, s, staged)
}

// revertObjectJob registers the job's inputs again, if every input object is
// there at its size; otherwise it marks the job wedged.
func (c *Coordinator) revertObjectJob(ctx context.Context, s raft.StagedCompaction, staged string) error {
	for _, in := range s.Inputs {
		size, err := c.storage.StatFile(ctx, in.Path)
		if err != nil {
			return err
		}
		if size != in.SizeBytes {
			c.logger.Error().Str("job", s.Job).Str("input", in.Path).Str("output_gone_reason", s.OutputGoneReason).
				Msg("Committed compaction cannot be undone: an input object is missing")
			return c.claimFinisher(ctx, s.Job, true)
		}
	}
	// The output object goes first: a crash after the revert must not
	// leave it readable next to the inputs registered again.
	if err := c.deleteOutputObject(ctx, s); err != nil {
		return err
	}
	raw, _ := json.Marshal(raft.RevertCompactionPayload{Job: s.Job, By: c.localNode.ID, Now: time.Now().UTC()})
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandRevertCompaction, Payload: raw}); err != nil {
		if raft.CompactionConflictReason(err) == raft.CompactionOutputGone {
			// Its output path holds something else now: nothing this
			// finisher can decide.
			c.logger.Error().Str("job", s.Job).Str("output", s.Output.Path).Str("output_gone_reason", s.OutputGoneReason).
				Msg("Committed compaction cannot be undone: its output path was taken over")
			return c.claimFinisher(ctx, s.Job, true)
		}
		return err
	}
	return c.deleteStagedObject(ctx, staged)
}

// endObjectJob removes the staged object and ends the job.
func (c *Coordinator) endObjectJob(ctx context.Context, s raft.StagedCompaction, staged string, inputsDeleted bool) error {
	if err := c.deleteStagedObject(ctx, staged); err != nil {
		return err
	}
	raw, _ := json.Marshal(raft.CleanupCompactionPayload{Job: s.Job, By: c.localNode.ID, InputsDeleted: inputsDeleted})
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandCleanupCompaction, Payload: raw}); err != nil {
		return err
	}
	c.staged.settlingWarned.Delete(s.Job)
	metrics.Get().IncCompactionFinished()
	c.logger.Info().Str("job", s.Job).Str("output", s.Output.Path).Bool("output_removed", inputsDeleted).
		Msg("Committed compaction finished")
	return nil
}

func (c *Coordinator) deleteStagedObject(ctx context.Context, staged string) error {
	if err := c.storage.Delete(ctx, staged); err != nil && !storage.IsNotFound(err) {
		return err
	}
	return nil
}

// deleteOutputObject deletes the object at the job's output path unless
// something else is registered there (the job's own output may be).
func (c *Coordinator) deleteOutputObject(ctx context.Context, s raft.StagedCompaction) error {
	if e, reg := c.raftNode.FSM().GetFile(s.Output.Path); reg && !(e.CompactionJob == s.Job && e.SHA256 == s.Output.SHA256) {
		return nil
	}
	if c.holdsRegistration(s.Output.Path) {
		return nil
	}
	if err := c.storage.Delete(ctx, s.Output.Path); err != nil && !storage.IsNotFound(err) {
		return err
	}
	return nil
}

// holdsRegistration reports a path this node's outbox still owns.
func (c *Coordinator) holdsRegistration(path string) bool {
	c.staged.mu.Lock()
	holds := c.staged.holds
	c.staged.mu.Unlock()
	return holds != nil && holds(path)
}

// deleteUnregistered deletes the object at path unless the manifest lists it
// or this node's outbox owns it.
func (c *Coordinator) deleteUnregistered(ctx context.Context, path string) error {
	if _, reg := c.raftNode.FSM().GetFile(path); reg || c.holdsRegistration(path) {
		return nil
	}
	if err := c.storage.Delete(ctx, path); err != nil && !storage.IsNotFound(err) {
		return err
	}
	return nil
}

// sweepStagedObjects removes staged objects no job owns: a compactor that
// crashed before staging, or a delete that failed. A job is read from the
// manifest again right before its object goes.
func (c *Coordinator) sweepStagedObjects(ctx context.Context) {
	lister, ok := c.storage.(storage.ObjectLister)
	if !ok {
		return
	}
	objects, err := lister.ListObjects(ctx, raft.StagedCompactionRoot+"/")
	if err != nil {
		c.logger.Debug().Err(err).Msg("Listing staged compaction objects failed")
		return
	}
	for _, o := range objects {
		rest := strings.TrimPrefix(o.Path, raft.StagedCompactionRoot+"/")
		job, _, found := strings.Cut(rest, "/")
		if !found || job == "" || time.Since(o.LastModified) < objectStagedSweepAge {
			continue
		}
		if err := c.SyncManifest(ctx); err != nil {
			return
		}
		if _, staged := c.raftNode.FSM().StagedCompactionByJob(job); staged {
			continue
		}
		if err := c.storage.Delete(ctx, o.Path); err != nil && !storage.IsNotFound(err) {
			continue
		}
		c.logger.Info().Str("path", o.Path).Msg("Removed a staged compaction output no job owns")
	}
}

// FinalizingObjectInputs returns the input objects of the committed object
// job whose output is path, that are not registered again: a DELETE that
// rewrote or deleted that output removes them at once (their rows are the
// output's), rather than leaving them readable until the finisher runs
// (#1207 d2a2b).
func (c *Coordinator) FinalizingObjectInputs(path string) []string {
	if c == nil || c.raftNode == nil {
		return nil
	}
	fsm := c.raftNode.FSM()
	for _, s := range fsm.StagedCompactions() {
		if !s.Object || !s.Finalizing || s.Output.Path != path {
			continue
		}
		c.staged.mu.Lock()
		holds := c.staged.holds
		c.staged.mu.Unlock()
		var out []string
		for _, in := range s.Inputs {
			if _, reg := fsm.GetFile(in.Path); reg || (holds != nil && holds(in.Path)) {
				continue
			}
			out = append(out, in.Path)
		}
		return out
	}
	return nil
}

// ConsumedByCompaction reports a path that a compaction not yet ended took
// as an input, with this content (the SHA when both sides have one, else the
// size), and that the manifest no longer lists: its rows are in that job's
// output. The registration outbox asks it before registering a path again
// (#1358).
func (c *Coordinator) ConsumedByCompaction(path string, size int64, sha256 string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	if fsm == nil {
		return false
	}
	// The map lookup first: the outbox asks for every file it registers.
	if !fsm.StagedCompactionOwns(path) {
		return false
	}
	if _, reg := fsm.GetFile(path); reg {
		return false
	}
	for _, s := range fsm.StagedCompactions() {
		for _, in := range s.Inputs {
			if in.Path != path {
				continue
			}
			if in.SHA256 != "" && sha256 != "" {
				if in.SHA256 == sha256 {
					return true
				}
				continue
			}
			if in.SizeBytes == size {
				return true
			}
		}
	}
	return false
}

// CompactionOwns reports a file a staged compaction owns (an input or the
// output of a job not yet ended).
func (c *Coordinator) CompactionOwns(path string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	return c.raftNode.FSM().StagedCompactionOwns(path)
}

// FinalizingCompactions counts the committed object-store jobs not yet ended
// (wedged ones excluded: they wait for an operator).
func (c *Coordinator) FinalizingCompactions() int {
	if c == nil || c.raftNode == nil {
		return 0
	}
	n := 0
	for _, s := range c.raftNode.FSM().StagedCompactions() {
		if s.Object && s.Finalizing && !s.Wedged {
			n++
		}
	}
	return n
}

// ErrCompactionJobNotFound and ErrCompactionJobNotWedged: an operator abort
// of a compaction job that is not there, or not waiting for an operator.
var (
	ErrCompactionJobNotFound  = errors.New("no staged compaction job with that id")
	ErrCompactionJobNotWedged = errors.New("the compaction job is not wedged; it ends by itself")
)

// AbortWedgedCompaction ends a wedged compaction job for an operator who has
// checked its data (#1207 d2a2b): its entry goes, nothing in the manifest or
// the bucket changes. Its staged object is removed.
func (c *Coordinator) AbortWedgedCompaction(ctx context.Context, job string) error {
	if c == nil || c.raftNode == nil {
		return ErrClusterRaftNotConfigured
	}
	if err := c.SyncManifest(ctx); err != nil {
		return err
	}
	s, ok := c.raftNode.FSM().StagedCompactionByJob(job)
	if !ok {
		return ErrCompactionJobNotFound
	}
	if !s.Wedged {
		return ErrCompactionJobNotWedged
	}
	raw, _ := json.Marshal(raft.AbortCompactionPayload{Job: job, By: c.localNode.ID, Reason: "operator: wedged job aborted"})
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandAbortCompaction, Payload: raw}); err != nil {
		return err
	}
	if s.Object {
		_ = c.deleteStagedObject(ctx, raft.StagedCompactionKey(s.Job, s.Output.Path))
	}
	c.logger.Warn().Str("job", job).Msg("Wedged compaction job aborted by an operator")
	return nil
}

// StagedOutputKey returns where the output at path of a committed
// object-store job waits until its finisher copies it into place.
func (c *Coordinator) StagedOutputKey(path string) (string, bool) {
	if c == nil || c.raftNode == nil {
		return "", false
	}
	fsm := c.raftNode.FSM()
	e, ok := fsm.GetFile(path)
	if !ok || e.CompactionJob == "" {
		return "", false
	}
	s, ok := fsm.StagedCompactionByJob(e.CompactionJob)
	if !ok || !s.Object || !s.Finalizing || s.Output.Path != path {
		return "", false
	}
	return raft.StagedCompactionKey(s.Job, s.Output.Path), true
}
