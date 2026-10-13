package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// The CompletionWatcher's staged commit (#1207 d2a2a). A manifest in state
// "staged" is driven one step per poll, never blocking the others:
//
//	not in the cluster -> verify the staged copy -> Stage
//	staged             -> wait until MinHolders other nodes hold it (bounded
//	                      by a deadline that grows with the output's size)
//	                   -> verify the copy again -> Commit
//	committed          -> the output at its final path (adopted from the
//	                      staged copy; pulled from a holder if that copy is
//	                      gone) -> local copies of the inputs the manifest no
//	                      longer lists removed -> observers -> manifest removed
//
// A refusal, a pause, the cluster dropping the job or a missing staged copy
// ends it: the staged copy and the manifest are removed, the inputs (never
// touched) stay, and the partition is compacted again by a later cycle.

const (
	// stagedHolderWait is the base wait for a holder; stagedHolderRate adds
	// time for the output's size, as a slow pull should not lose the job.
	stagedHolderWait = 10 * time.Minute
	stagedHolderRate = 10 << 20 // bytes per second
	// stagedForcePullAfter: a committed job whose output this node cannot
	// adopt (its staged copy is gone) pulls it from a holder.
	stagedForcePullAfter = 2 * time.Minute
	// stagedOrphanEvery paces the sweep for this node's staged jobs it no
	// longer has a manifest for.
	stagedOrphanEvery = time.Minute
)

// stagedJobState is the watcher's memory of one staged job; the cluster and
// the manifest are the durable record.
type stagedJobState struct {
	stagedAt    time.Time
	staged      bool
	verified    bool
	observed    bool
	committedAt time.Time
	forcedAt    time.Time
}

// stagedRegistry is the watcher state other goroutines read: the jobs in
// their commit branch (a pause waits for them) and the inputs of every local
// staged manifest (the manager keeps them out of new jobs).
type stagedRegistry struct {
	mu         sync.Mutex
	committing map[string]bool
	inputs     map[string]bool
}

func (w *CompletionWatcher) stagedState(job string) *stagedJobState {
	if w.stagedJobs == nil {
		w.stagedJobs = map[string]*stagedJobState{}
	}
	st := w.stagedJobs[job]
	if st == nil {
		st = &stagedJobState{}
		w.stagedJobs[job] = st
	}
	return st
}

// LocallyStagedInput reports an input of a staged manifest on this node.
func (w *CompletionWatcher) LocallyStagedInput(path string) bool {
	w.reg.mu.Lock()
	defer w.reg.mu.Unlock()
	return w.reg.inputs[path]
}

// HasStagedJob reports whether this node holds a completion manifest for job
// (any state): its staged directory is in use.
func (w *CompletionWatcher) HasStagedJob(job string) bool {
	if job == "" || validateJobID(job) != nil {
		return false
	}
	_, err := os.Stat(filepath.Join(w.cfg.Dir, job+".json"))
	return err == nil
}

// refreshStagedInputs records the inputs of the staged manifests listed now.
func (w *CompletionWatcher) refreshStagedInputs(paths []string) {
	inputs := map[string]bool{}
	for _, p := range paths {
		m, err := readCompletionManifest(p)
		if err != nil || m.State != CompletionStateStaged {
			continue
		}
		for _, in := range m.Inputs {
			inputs[in.Path] = true
		}
	}
	w.reg.mu.Lock()
	w.reg.inputs = inputs
	w.reg.mu.Unlock()
}

func (w *CompletionWatcher) setCommitting(job string, on bool) {
	w.reg.mu.Lock()
	defer w.reg.mu.Unlock()
	if w.reg.committing == nil {
		w.reg.committing = map[string]bool{}
	}
	if on {
		w.reg.committing[job] = true
	} else {
		delete(w.reg.committing, job)
	}
}

func (w *CompletionWatcher) committingCount() int {
	w.reg.mu.Lock()
	defer w.reg.mu.Unlock()
	return len(w.reg.committing)
}

func stagedJobOf(m *CompletionManifest) (StagedJob, bool) {
	if m.StagedOutput == nil || m.StagedKey == "" || len(m.Inputs) == 0 ||
		m.StagedKey != StagedKey(m.JobID, m.StagedOutput.Path) {
		return StagedJob{}, false
	}
	return StagedJob{JobID: m.JobID, StagedKey: m.StagedKey, Output: *m.StagedOutput, Inputs: m.Inputs}, true
}

// applyStaged drives one staged manifest one step.
func (w *CompletionWatcher) applyStaged(ctx context.Context, path string, m *CompletionManifest) {
	sc := w.cfg.Staged
	if sc == nil || w.cfg.Storage == nil {
		w.logger.Warn().Str("job_id", m.JobID).Msg("A staged compaction manifest with no staged commit wired; leaving it")
		return
	}
	job, ok := stagedJobOf(m)
	if !ok {
		w.logger.Error().Str("job_id", m.JobID).Msg("Malformed staged compaction manifest; dropping the job (its inputs were never touched)")
		w.endStaged(ctx, path, m.JobID, nil, "malformed manifest")
		return
	}
	st := w.stagedState(job.JobID)
	if err := sc.SyncManifest(ctx); err != nil {
		w.manifestsNotLeader.Add(1)
		return
	}
	status := sc.Status(job)
	if status.Committed {
		w.commitStaged(ctx, path, job, st)
		return
	}
	if w.pausedByGate() {
		// A restore may replace the manifest: no job stays staged across it.
		if status.Staged {
			if err := sc.Abort(ctx, job.JobID, "compaction paused"); err != nil {
				return
			}
		}
		w.endStaged(ctx, path, job.JobID, job.Inputs, "a compaction pause started")
		return
	}
	now := time.Now()
	if !status.Staged {
		if st.staged {
			w.endStaged(ctx, path, job.JobID, job.Inputs, "the cluster dropped the staged job")
			return
		}
		if !w.verifyStaged(ctx, job, st) {
			w.endStaged(ctx, path, job.JobID, job.Inputs, "the staged output is missing or damaged")
			return
		}
		out, err := sc.Stage(ctx, job)
		switch out {
		case StagedOK:
			st.staged, st.stagedAt = true, now
		case StagedCommitted:
			w.commitStaged(ctx, path, job, st)
		case StagedRefused:
			w.endStaged(ctx, path, job.JobID, job.Inputs, "the cluster refused to stage it: "+errText(err))
		default:
			w.logger.Debug().Err(err).Str("job_id", job.JobID).Msg("Staging the compaction deferred; retrying")
		}
		return
	}
	st.staged = true
	if st.stagedAt.IsZero() {
		st.stagedAt = now
	}
	if status.Holders < status.MinHolders {
		deadline := stagedHolderWait + time.Duration(job.Output.SizeBytes/stagedHolderRate)*time.Second
		if now.Sub(st.stagedAt) > deadline {
			if err := sc.Abort(ctx, job.JobID, "no node pulled the staged output in time"); err != nil {
				return
			}
			w.endStaged(ctx, path, job.JobID, job.Inputs, "no node pulled the staged output in time")
		}
		return
	}
	if !w.verifyStaged(ctx, job, st) {
		if err := sc.Abort(ctx, job.JobID, "staged output missing"); err != nil {
			return
		}
		w.endStaged(ctx, path, job.JobID, job.Inputs, "the staged output is missing or damaged")
		return
	}
	if !st.observed && w.cfg.BeforeCommit != nil {
		w.cfg.BeforeCommit(job.Output.Path)
		st.observed = true
	}
	out, err := sc.Commit(ctx, job)
	switch out {
	case StagedOK, StagedCommitted:
		w.commitStaged(ctx, path, job, st)
	case StagedRefused:
		if aerr := sc.Abort(ctx, job.JobID, "commit refused"); aerr != nil {
			return
		}
		w.endStaged(ctx, path, job.JobID, job.Inputs, "the cluster refused the commit: "+errText(err))
	default:
		w.logger.Debug().Err(err).Str("job_id", job.JobID).Msg("Committing the compaction deferred; retrying")
	}
}

// verifyStaged checks the staged copy: its size every time, its checksum
// once.
func (w *CompletionWatcher) verifyStaged(ctx context.Context, job StagedJob, st *stagedJobState) bool {
	size, err := w.cfg.Storage.StatFile(ctx, job.StagedKey)
	if err != nil || size != job.Output.SizeBytes {
		return false
	}
	if st.verified {
		return true
	}
	h := sha256.New()
	if err := w.cfg.Storage.ReadTo(ctx, job.StagedKey, h); err != nil {
		return false
	}
	st.verified = hex.EncodeToString(h.Sum(nil)) == job.Output.SHA256
	return st.verified
}

// commitStaged finishes a committed job.
func (w *CompletionWatcher) commitStaged(ctx context.Context, path string, job StagedJob, st *stagedJobState) {
	sc := w.cfg.Staged
	w.setCommitting(job.JobID, true)
	if st.committedAt.IsZero() {
		st.committedAt = time.Now()
	}
	if size, err := w.cfg.Storage.StatFile(ctx, job.Output.Path); err != nil || size != job.Output.SizeBytes {
		if aerr := sc.Adopt(ctx, job); aerr != nil {
			// Pulled from a node that holds it, again each period: a pull can be
			// dropped (queue full) or run out of attempts.
			if time.Since(st.committedAt) > stagedForcePullAfter && time.Since(st.forcedAt) > stagedForcePullAfter {
				w.logger.Warn().Err(aerr).Str("job_id", job.JobID).Str("output", job.Output.Path).
					Msg("Committed compaction output not adoptable here; pulling it from a node that holds it")
				sc.ForcePull(job)
				st.forcedAt = time.Now()
			}
			return
		}
	}
	// This node's copies of the inputs: the delete callbacks remove them,
	// but those are in memory and a crash loses them. A path the manifest
	// lists again, or this node's outbox owns, is not this job's to remove.
	if err := sc.SyncManifest(ctx); err != nil {
		return
	}
	var consumed []string
	for _, in := range job.Inputs {
		consumed = append(consumed, in.Path)
		if sc.Registered(in.Path) || sc.HoldsRegistration(in.Path) {
			continue
		}
		if err := w.cfg.Storage.Delete(ctx, in.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			w.logger.Warn().Err(err).Str("path", in.Path).Msg("Could not remove a compacted input; retrying")
			return
		}
	}
	if w.cfg.OnCommitted != nil {
		if err := w.cfg.OnCommitted(consumed); err != nil {
			w.logger.Warn().Err(err).Str("job_id", job.JobID).Msg("Compaction committed; its observers failed, retrying")
			return
		}
	}
	if mover, ok := w.cfg.Storage.(storage.ObjectMover); ok {
		if err := mover.RemoveAll(ctx, StagedJobDir(job.JobID)); err != nil {
			w.logger.Warn().Err(err).Str("job_id", job.JobID).Msg("Could not remove the staged directory; retrying")
			return
		}
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		w.logger.Warn().Err(err).Str("path", path).Msg("Could not remove the completion manifest; retrying")
		return
	}
	delete(w.stagedJobs, job.JobID)
	w.setCommitting(job.JobID, false)
	w.manifestsApplied.Add(1)
	w.logger.Info().Str("job_id", job.JobID).Str("output", job.Output.Path).Int("inputs", len(job.Inputs)).
		Msg("Compaction committed")
}

// endStaged drops a job that did not commit: its staged copy and manifest.
// An input the manifest no longer lists (and this node's outbox does not
// own) was removed by someone else (a DELETE, retention, a commit of this
// job before a crash); its local copy goes too, as its delete callback, lost
// with the crash, would have done.
func (w *CompletionWatcher) endStaged(ctx context.Context, path, job string, inputs []StagedInput, why string) {
	if len(inputs) > 0 {
		sc := w.cfg.Staged
		if err := sc.SyncManifest(ctx); err != nil {
			return
		}
		for _, in := range inputs {
			if sc.Registered(in.Path) || sc.HoldsRegistration(in.Path) {
				continue
			}
			if err := w.cfg.Storage.Delete(ctx, in.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return
			}
		}
	}
	if mover, ok := w.cfg.Storage.(storage.ObjectMover); ok && validateJobID(job) == nil {
		if err := mover.RemoveAll(ctx, StagedJobDir(job)); err != nil {
			w.logger.Warn().Err(err).Str("job_id", job).Msg("Could not remove the staged directory; retrying")
			return
		}
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return
	}
	delete(w.stagedJobs, job)
	w.setCommitting(job, false)
	w.logger.Warn().Str("job_id", job).Str("reason", why).
		Msg("Staged compaction dropped; its inputs are untouched and a later cycle compacts them")
}

// abortOrphanStaged aborts the uncommitted jobs this node staged and holds no
// manifest for (it crashed and lost its completion directory, or the
// manifest was removed): nothing will commit them, and they keep their inputs
// out of compaction.
func (w *CompletionWatcher) abortOrphanStaged(ctx context.Context) {
	sc := w.cfg.Staged
	if sc == nil || time.Since(w.lastOrphanSweep) < stagedOrphanEvery {
		return
	}
	w.lastOrphanSweep = time.Now()
	for _, job := range sc.OwnStaged() {
		if w.HasStagedJob(job) {
			continue
		}
		if err := sc.Abort(ctx, job, "the compactor holds no record of it"); err != nil {
			w.logger.Debug().Err(err).Str("job_id", job).Msg("Aborting an orphaned staged compaction failed; retrying")
			continue
		}
		w.logger.Warn().Str("job_id", job).Msg("Aborted a staged compaction this node no longer has a record of")
	}
}

func errText(err error) string {
	if err == nil {
		return "no reason given"
	}
	return err.Error()
}

// SetStagedObservers sets BeforeCommit and OnCommitted (the manager's
// observers, which exist once the watcher does). Call before Start.
func (w *CompletionWatcher) SetStagedObservers(before func(output string), committed func(inputs []string) error) {
	w.cfg.BeforeCommit, w.cfg.OnCommitted = before, committed
}
