package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
)

// A re-home job on a cluster with local storage (#1860). Every node holds its
// own copy of every file, so every node does each step on its own disk:
//
//	committed  after its reads from before ended: fetch each output into
//	           the job's staged directory (from the origin, or any node that
//	           has it), verify, fsync.
//	published  record the job on this node (rehome.Registry, committed:
//	           reads leave the outputs out, whatever this node's Raft state
//	           after a restart), move the outputs into place, drop caches.
//	latched    record it latched (reads leave the source out), wait for the
//	           reads from before, unlink this node's copy of the source.
//	removed    the source is gone here.
//	reverted   remove the outputs placed here and the staged copies, end the
//	           record.
//
// One lock per job orders the steps on this node: a fetch still running when
// the job reverts cannot place anything after the revert's cleanup.

// rehomeLocalCluster is what the local steps need from the cluster beyond
// RehomeCluster (cluster.Coordinator).
type rehomeLocalCluster interface {
	FetchRehomeStaged(ctx context.Context, j raft.RehomeJob, out raft.RehomeOutput) error
	SyncManifest(ctx context.Context) error
}

// rehomeLocalUnlinkGrace: after the reads from before the latch ended, a
// query that listed the directory before it saw the latch may still open the
// source by name (an excluded file is still listed); it gets this long.
var rehomeLocalUnlinkGrace = time.Second

type rehomeLocalLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func (l *rehomeLocalLocks) lock(id string) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[string]*sync.Mutex{}
	}
	m, ok := l.locks[id]
	if !ok {
		m = &sync.Mutex{}
		l.locks[id] = m
	}
	l.mu.Unlock()
	m.Lock()
	return m.Unlock
}

func (l *rehomeLocalLocks) forget(id string) {
	l.mu.Lock()
	delete(l.locks, id)
	l.mu.Unlock()
}

// localRecord is a cluster job as this node's registry records it.
func localRecord(j raft.RehomeJob, state rehome.State) *rehome.Job {
	rec := &rehome.Job{ID: j.ID, Database: j.Database, Measurement: j.Measurement, State: state, Cluster: true,
		Source: rehome.FileRef{Key: j.Source.Path, Size: j.Source.SizeBytes, SHA256: j.Source.SHA256}}
	for _, o := range j.Outputs {
		rec.Outputs = append(rec.Outputs, rehome.FileRef{Key: o.Key, Staged: o.StagedKey, Size: o.SizeBytes, SHA256: o.SHA256, Start: o.PartitionTime})
	}
	return rec
}

func outputRef(o raft.RehomeOutput) rehome.FileRef {
	return rehome.FileRef{Key: o.Key, Staged: o.StagedKey, Size: o.SizeBytes, SHA256: o.SHA256}
}

// ParticipateLocal is this node's part of a job's state on local storage.
func (h *RehomeHandler) ParticipateLocal(ctx context.Context, j raft.RehomeJob, acked bool) error {
	store, ok := h.mover()
	if !ok || h.swap == nil || h.swap.Registry == nil || h.swap.Cluster == nil {
		return errors.New("re-home on local storage needs local storage and the job registry")
	}
	lc, ok := h.swap.Cluster.(rehomeLocalCluster)
	if !ok {
		return errors.New("the cluster cannot fetch a job's outputs")
	}
	unlock := h.localLocks.lock(j.ID)
	defer unlock()
	// Moved on while this step waited for the lock: the next state's step
	// does what is needed.
	if cur, live := h.swap.Cluster.RehomeJob(j.ID); !live || cur.StateIndex != j.StateIndex {
		return nil
	}
	reg := h.swap.Registry
	switch j.State {
	case raft.RehomeCommitted:
		// A node the state does not wait for (it joined since) fetches too:
		// published waits for it, and by then peers may have moved their
		// copies into place.
		if acked {
			if err := h.waitClusterStepReads(ctx, j); err != nil {
				return err
			}
		}
		return h.holdStaged(ctx, store, lc, j)
	case raft.RehomePublished:
		// A node that did not take part in committed (it joined since)
		// fetches here.
		for _, o := range j.Outputs {
			if placed, err := h.outputInPlace(ctx, store, outputRef(o)); err != nil {
				return err
			} else if placed {
				continue
			}
			if err := h.holdStagedOne(ctx, store, lc, j, o); err != nil {
				return err
			}
		}
		if err := reg.Mirror(ctx, localRecord(j, rehome.StateCommitted)); err != nil {
			return fmt.Errorf("record the job on this node: %w", err)
		}
		if cur, live := h.swap.Cluster.RehomeJob(j.ID); !live || cur.State != raft.RehomePublished {
			return nil // reverting: the revert's step removes what is here
		}
		for _, o := range j.Outputs {
			placed, err := h.outputInPlace(ctx, store, outputRef(o))
			if err != nil {
				return err
			}
			if !placed {
				if err := store.MoveObject(ctx, o.StagedKey, o.Key); err != nil {
					return fmt.Errorf("move %s into place: %w", o.Key, err)
				}
			}
		}
		h.dropLocalCaches(j)
		if err := h.waitClusterStepReads(ctx, j); err != nil {
			return err
		}
		h.dropLocalCaches(j)
		return nil
	case raft.RehomeLatched:
		for _, o := range j.Outputs {
			placed, err := h.outputInPlace(ctx, store, outputRef(o))
			if err != nil {
				return err
			}
			if !placed {
				// Joined after published: the outputs are registered now and
				// its puller brings them. Recorded committed first, so each
				// is left out of reads as it lands, and the source stays
				// until all are here.
				if _, had := reg.Get(j.ID); !had {
					if err := reg.Mirror(ctx, localRecord(j, rehome.StateCommitted)); err != nil {
						return fmt.Errorf("record the job on this node: %w", err)
					}
				}
				return fmt.Errorf("%s is not on this node yet", o.Key)
			}
		}
		if err := reg.Mirror(ctx, localRecord(j, rehome.StateLatched)); err != nil {
			return fmt.Errorf("record the job latched on this node: %w", err)
		}
		// Waited for here whether or not the job waits for this node: the
		// unlink below must not pull a file from under a read.
		if err := h.waitClusterStepReads(ctx, j); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rehomeLocalUnlinkGrace):
		}
		return h.unlinkLocalSource(ctx, store, j)
	case raft.RehomeRemoved:
		if err := h.unlinkLocalSource(ctx, store, j); err != nil {
			return err
		}
		if err := store.RemoveAll(ctx, rehome.StagedDir(j.ID)); err != nil {
			return err
		}
		// From here this node's reads follow the cluster's record, which
		// leaves the source out until the job ends.
		return reg.End(ctx, j.ID)
	case raft.RehomeReverted:
		if err := h.removeLocalOutputs(ctx, store, j); err != nil {
			return err
		}
		if err := reg.End(ctx, j.ID); err != nil {
			return err
		}
		h.dropLocalCaches(j)
		return nil
	}
	return nil
}

// ForgetLocal drops this node's lock for an ended job.
func (h *RehomeHandler) ForgetLocal(id string) { h.localLocks.forget(id) }

// holdStaged makes every output of j durable in its staged key here.
func (h *RehomeHandler) holdStaged(ctx context.Context, store rehomeStore, lc rehomeLocalCluster, j raft.RehomeJob) error {
	for _, o := range j.Outputs {
		if err := h.holdStagedOne(ctx, store, lc, j, o); err != nil {
			return err
		}
	}
	return nil
}

func (h *RehomeHandler) holdStagedOne(ctx context.Context, store rehomeStore, lc rehomeLocalCluster, j raft.RehomeJob, o raft.RehomeOutput) error {
	staged := rehome.FileRef{Key: o.StagedKey, Size: o.SizeBytes, SHA256: o.SHA256}
	have, err := h.outputInPlace(ctx, store, staged)
	if err != nil {
		// Something else at the staged key (a fetch cut short): fetch again.
		if derr := store.Delete(ctx, o.StagedKey); derr != nil && !storage.IsNotFound(derr) {
			return derr
		}
		have = false
	}
	if !have {
		if err := lc.FetchRehomeStaged(ctx, j, o); err != nil {
			return fmt.Errorf("fetch %s: %w", o.Key, err)
		}
		if ok, err := h.outputInPlace(ctx, store, staged); err != nil || !ok {
			return fmt.Errorf("fetched %s does not match the job", o.Key)
		}
	}
	return store.Sync(ctx, o.StagedKey)
}

// unlinkLocalSource removes this node's copy of j's source, unless the
// manifest lists it (a job never removes a registered file).
func (h *RehomeHandler) unlinkLocalSource(ctx context.Context, store rehomeStore, j raft.RehomeJob) error {
	if _, listed := h.swap.Cluster.ManifestFile(j.Source.Path); listed {
		return fmt.Errorf("%s is in the manifest; not removed", j.Source.Path)
	}
	if err := storage.DeleteWithRetry(ctx, store, j.Source.Path); err != nil && !storage.IsNotFound(err) {
		return fmt.Errorf("remove %s: %w", j.Source.Path, err)
	}
	return nil
}

// removeLocalOutputs removes j's outputs placed on this node (any the
// manifest does not list) and its staged directory.
func (h *RehomeHandler) removeLocalOutputs(ctx context.Context, store rehomeStore, j raft.RehomeJob) error {
	for _, o := range j.Outputs {
		if _, listed := h.swap.Cluster.ManifestFile(o.Key); listed {
			continue
		}
		placed, err := h.outputInPlace(ctx, store, outputRef(o))
		if err != nil {
			// Another file at the key: not this job's to remove.
			h.logger.Warn().Err(err).Str("job", j.ID).Str("path", o.Key).Msg("Re-home: left a file that is not the job's output")
			continue
		}
		if !placed {
			continue
		}
		if err := storage.DeleteWithRetry(ctx, store, o.Key); err != nil && !storage.IsNotFound(err) {
			return fmt.Errorf("remove %s: %w", o.Key, err)
		}
	}
	if err := store.RemoveAll(ctx, rehome.StagedDir(j.ID)); err != nil {
		return fmt.Errorf("remove %s: %w", rehome.StagedDir(j.ID), err)
	}
	return nil
}

// SweepLocal settles what jobs that ended left on this node: a record whose
// job the cluster no longer has (this node missed the job's last steps: it
// was down, or removed and back), and staged directories no job owns. Run
// only once this node's record has caught up with the leader's. A job's
// staged files are written only after its stage applied on this node, so a
// directory of a job not in the record is not one being staged.
func (h *RehomeHandler) SweepLocal(ctx context.Context) error {
	store, ok := h.mover()
	if !ok || h.swap == nil || h.swap.Registry == nil || h.swap.Cluster == nil {
		return nil
	}
	lc, ok := h.swap.Cluster.(rehomeLocalCluster)
	if !ok {
		return nil
	}
	if err := lc.SyncManifest(ctx); err != nil {
		return fmt.Errorf("sync with the cluster first: %w", err)
	}
	reg := h.swap.Registry
	var errs []error
	for _, rec := range reg.Jobs() {
		if !rec.Cluster {
			continue
		}
		if _, live := h.swap.Cluster.RehomeJob(rec.ID); live {
			continue
		}
		errs = append(errs, h.settleEndedLocal(ctx, store, rec))
	}
	keys, err := store.List(ctx, rehome.StagedRoot+"/")
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	seen := map[string]bool{}
	for _, k := range keys {
		id, _, ok := strings.Cut(strings.TrimPrefix(k, rehome.StagedRoot+"/"), "/")
		if !ok || seen[id] {
			continue
		}
		seen[id] = true
		if _, live := h.swap.Cluster.RehomeJob(id); live {
			continue
		}
		if _, own := reg.Get(id); own {
			continue // this node's own job (or a record settled above that failed)
		}
		errs = append(errs, store.RemoveAll(ctx, rehome.StagedDir(id)))
	}
	return errors.Join(errs...)
}

// settleEndedLocal ends a record of a job the cluster no longer has. What
// the manifest lists now decides each file, not a guess at how the job ended
// (its source or an output may have been compacted or deleted since): an
// output the manifest lists with the job's checksum is put in place (from
// the staged copy, or the puller brings it); any other copy of an output is
// removed; the source's copy goes unless the manifest lists it.
func (h *RehomeHandler) settleEndedLocal(ctx context.Context, store rehomeStore, rec *rehome.Job) error {
	unlock := h.localLocks.lock(rec.ID)
	defer func() {
		unlock()
		h.localLocks.forget(rec.ID)
	}()
	j := raft.RehomeJob{ID: rec.ID, Database: rec.Database, Measurement: rec.Measurement,
		Source: raft.FileEntry{Path: rec.Source.Key}}
	for _, o := range rec.Outputs {
		j.Outputs = append(j.Outputs, raft.RehomeOutput{Key: o.Key, StagedKey: o.Staged, SizeBytes: o.Size, SHA256: o.SHA256, PartitionTime: o.Start})
	}
	for _, o := range j.Outputs {
		e, listed := h.swap.Cluster.ManifestFile(o.Key)
		if listed && e.SHA256 == o.SHA256 {
			placed, err := h.outputInPlace(ctx, store, outputRef(o))
			if err != nil {
				return err
			}
			if !placed {
				if ok, _ := h.outputInPlace(ctx, store, rehome.FileRef{Key: o.StagedKey, Size: o.SizeBytes, SHA256: o.SHA256}); ok {
					if err := store.MoveObject(ctx, o.StagedKey, o.Key); err != nil {
						return err
					}
				}
			}
			continue
		}
		if listed {
			continue // another file at the key now: not ours
		}
		placed, err := h.outputInPlace(ctx, store, outputRef(o))
		if err == nil && placed {
			if err := storage.DeleteWithRetry(ctx, store, o.Key); err != nil && !storage.IsNotFound(err) {
				return err
			}
		}
	}
	if _, listed := h.swap.Cluster.ManifestFile(rec.Source.Key); !listed {
		if err := storage.DeleteWithRetry(ctx, store, rec.Source.Key); err != nil && !storage.IsNotFound(err) {
			return err
		}
	}
	if err := store.RemoveAll(ctx, rehome.StagedDir(rec.ID)); err != nil {
		return err
	}
	if err := h.swap.Registry.End(ctx, rec.ID); err != nil {
		return err
	}
	h.logger.Info().Str("job", rec.ID).Str("path", rec.Source.Key).
		Msg("Re-home: settled a job this node missed the end of, by what the manifest lists now")
	h.dropLocalCaches(j)
	return nil
}
