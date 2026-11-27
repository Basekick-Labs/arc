package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// The finisher of a fenced object job (#1852, raft/compaction_fences.go).
// Each step waits until every node's read fence covers the job's current
// phase, then acts and advances:
//
//	committed  copy the staged object to the output's path, check it;
//	           published. No staged object to copy: delete the output
//	           object, reverting.
//	published  switched: reads use the output from here.
//	switched   delete the input objects; removed.
//	removed    Finalize, delete the staged object, Cleanup.
//	reverting  RevertCompaction: the inputs are registered again.
//
// The FSM refuses any other manifest change to the job's output and inputs
// until removed, so none of the endings of a legacy job (outputLeft) apply.

// fencedDroppedWait: after a node leaves the node table (no longer counted
// by the fence rule), how long a step waits before it goes on: a node
// removed while it could still read has lost its lease and stopped its reads
// by then.
var fencedDroppedWait = 30 * time.Second

// fencedPoll is how often a waiting finisher looks at the fences.
var fencedPoll = 250 * time.Millisecond

// fencedWaiverGrace: after a fence was waived, how long a step waits before
// it acts: a waived node that was in fact reading drops its read lease (and
// its reads) when it applies the waiver, within about a lease period.
var fencedWaiverGrace = readLeaseTimeout + 2*readLeaseTick

func (c *Coordinator) finishFencedJob(ctx context.Context, copier storage.ObjectCopier, job, staged string) error {
	self := c.localNode.ID
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s, ok := c.raftNode.FSM().StagedCompactionByJob(job)
		if !ok || !s.Finalizing || s.Wedged {
			return nil
		}
		if s.FinisherNode != self {
			return errFinisherLease
		}
		if err := c.waitFences(ctx, s); err != nil {
			return err
		}
		var err error
		switch s.Phase {
		case raft.PhaseCommitted:
			err = c.fencedCopy(ctx, copier, s, staged)
		case raft.PhasePublished:
			err = c.advanceCompaction(ctx, s, raft.PhaseSwitched)
		case raft.PhaseSwitched:
			if err = c.deleteFencedInputs(ctx, s); err == nil {
				err = c.advanceCompaction(ctx, s, raft.PhaseRemoved)
			}
		case raft.PhaseRemoved:
			if !c.ownsOutput(s) {
				// From removed on, a DELETE, retention or database delete
				// may take the output: its remover deals with its rows and
				// its object; the inputs are gone. Every fence is in.
				return c.endObjectJob(ctx, s, staged, true)
			}
			raw, _ := json.Marshal(raft.FinalizeCompactionPayload{Job: s.Job, By: self, Output: s.Output.Path})
			if err = c.applyOrForward(ctx, &raft.Command{Type: raft.CommandFinalizeCompaction, Payload: raw}); err == nil {
				return c.endObjectJob(ctx, s, staged, false)
			}
		case raft.PhaseReverting:
			if err = c.revertObjectJob(ctx, s, staged); err == nil {
				return nil
			}
		default:
			return fmt.Errorf("compaction job %s in phase %q", s.Job, s.Phase)
		}
		if err != nil {
			if raft.CompactionConflictReason(err) == raft.CompactionFencesBehind {
				continue // a node joined, or a fence moved: wait again
			}
			return err
		}
	}
}

// fencedCopy copies the staged output to its path and publishes; with no
// staged object to copy from, it deletes the output object and reverts.
func (c *Coordinator) fencedCopy(ctx context.Context, copier storage.ObjectCopier, s raft.StagedCompaction, staged string) error {
	var present bool
	if err := c.withLease(ctx, s.Job, func(ctx context.Context) error {
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
			c.logger.Error().Str("job", s.Job).Str("staged", staged).Int64("size", size).
				Msg("Committed compaction has no staged output to copy; registering its inputs again")
			// Left out of reads by name: deleting what may be there is safe.
			if err := c.deleteOutputObject(ctx, s); err != nil {
				return err
			}
			return c.advanceCompaction(ctx, s, raft.PhaseReverting)
		}
		if err := c.withLease(ctx, s.Job, func(ctx context.Context) error {
			return copier.CopyObject(ctx, staged, s.Output.Path, s.Output.SizeBytes, s.Output.SHA256)
		}); err != nil {
			return err
		}
		ok, err := c.objectAt(ctx, copier, s.Output.Path, s.Output.SizeBytes, s.Output.SHA256)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("compaction output %s is not in storage as copied", s.Output.Path)
		}
	}
	return c.advanceCompaction(ctx, s, raft.PhasePublished)
}

// deleteFencedInputs deletes every input object the manifest does not list.
// The FSM refused any registration of them since the commit, and the outbox
// defers a pending one until the job ends (then it settles: the input was
// consumed, or its object is gone). Paths are unique per flush (#1282): a
// different file at an input's path would need a reuse that a restore,
// which pauses compaction, is the only source of.
func (c *Coordinator) deleteFencedInputs(ctx context.Context, s raft.StagedCompaction) error {
	fsm := c.raftNode.FSM()
	return c.withLease(ctx, s.Job, func(ctx context.Context) error {
		for _, in := range s.Inputs {
			if fsm.HasFile(in.Path) {
				continue
			}
			if err := c.storage.Delete(ctx, in.Path); err != nil && !storage.IsNotFound(err) {
				return err
			}
		}
		return nil
	})
}

func (c *Coordinator) advanceCompaction(ctx context.Context, s raft.StagedCompaction, to string) error {
	raw, err := json.Marshal(raft.AdvanceCompactionPayload{Job: s.Job, By: c.localNode.ID, From: s.Phase, To: to})
	if err != nil {
		return err
	}
	if err := c.applyOrForward(ctx, &raft.Command{Type: raft.CommandAdvanceCompaction, Payload: raw}); err != nil {
		if raft.CompactionConflictReason(err) == raft.CompactionNotFinisher {
			return errFinisherLease
		}
		return err
	}
	return c.SyncManifest(ctx)
}

// waitFences returns once every node's read fence covers s's phase, no Raft
// server is outside the node table, and fencedDroppedWait has passed since a
// node left the table during the wait. The lease is renewed while it waits.
func (c *Coordinator) waitFences(ctx context.Context, s raft.StagedCompaction) error {
	fsm := c.raftNode.FSM()
	if s.Phase == raft.PhaseCommitted && !fsm.FencesAt(s.PhaseIndex) {
		c.logger.Debug().Str("job", s.Job).Msg("Committed compaction waits for every node's read fence before its copy")
	}
	start := map[string]bool{}
	for _, n := range fsm.GetAllNodes() {
		start[n.ID] = true
	}
	var left, waived time.Time
	began := time.Now()
	warned := false
	return c.withLease(ctx, s.Job, func(ctx context.Context) error {
		for {
			cur, ok := fsm.StagedCompactionByJob(s.Job)
			if !ok || cur.PhaseIndex != s.PhaseIndex {
				return nil // moved on: the caller looks again
			}
			now := map[string]bool{}
			for _, n := range fsm.GetAllNodes() {
				now[n.ID] = true
			}
			for id := range start {
				if !now[id] && left.IsZero() {
					left = time.Now()
				}
			}
			var blockers []string
			var err error
			ready := fsm.FencesAt(s.PhaseIndex) && (left.IsZero() || time.Since(left) >= fencedDroppedWait)
			if ready {
				// The Raft configuration only once the fences are in: it
				// goes through Raft's main loop.
				blockers, err = c.RehomeConfigBlockers()
				ready = err == nil && len(blockers) == 0
			}
			if ready && fsm.WaivedSince(s.PhaseIndex) {
				if waived.IsZero() {
					waived = time.Now()
				}
				ready = time.Since(waived) >= fencedWaiverGrace
			}
			if ready {
				return nil
			}
			if !warned && time.Since(began) > time.Minute {
				warned = true
				c.logger.Warn().Str("job", s.Job).Str("phase", s.Phase).Strs("fences_behind", fsm.FencesBehind(s.PhaseIndex)).
					Strs("raft_only", blockers).Msg("Committed compaction is waiting for nodes' read fences (a node the leader cannot reach is skipped after 30 s)")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(fencedPoll):
			}
		}
	})
}
