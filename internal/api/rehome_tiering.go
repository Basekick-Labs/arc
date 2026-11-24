package api

import (
	"context"
	"errors"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
)

// With tiered storage on, a job's files must stay on the hot tier while it
// runs, and the files it puts in place need tier rows: reads take the tiers
// of a measurement from its rows, and a measurement whose rows are all cold
// is not read from hot at all (#1825).
//
//   - tiering leaves alone every file the registry owns
//     (tiering.Manager.SetOwnedFilter, wired in main);
//   - once a job owns its file it asks tiering about it, and is refused a
//     file that is being moved, or that cold storage holds or holds a copy
//     of, or any file while cold storage cannot be asked;
//   - before reads are sent to the new files, the job records them as hot;
//     after the old file is removed, it forgets that file's row.
//
// A file that is on the cold tier is a job of its own kind: rehome_cold.go.

// RehomeTiering is what a job needs of the tiering manager.
type RehomeTiering interface {
	CheckRehomeSource(ctx context.Context, path string) error
	CanRecord(path string) error
	RecordRehomedFiles(ctx context.Context, files []tiering.RehomedFile) error
	ForgetRehomedFiles(ctx context.Context, paths []string) error
	// For a file in cold storage (rehome_cold.go).
	ColdBackend() storage.Backend
	CheckRehomeColdSource(ctx context.Context, path string) (size int64, store string, err error)
	ColdSourceStat(ctx context.Context, path, store string) (int64, error)
	RemoveRehomedColdFile(ctx context.Context, path, store string) error
}

// SetTiering binds the tiering manager, or records that none runs (nil: off,
// no licence, or it failed to start). Call it before Wired.
func (h *RehomeHandler) SetTiering(t RehomeTiering) {
	h.mu.Lock()
	h.tiering = t
	h.mu.Unlock()
}

// SetExportNotStarted records that the Iceberg export is bound and will not
// run in this process (its cold tier is unavailable), with the reason. A job
// then gets that as the export's answer at once, instead of waiting on a
// scheduler that never serves it.
func (h *RehomeHandler) SetExportNotStarted(why string) {
	h.mu.Lock()
	h.exportDown = why
	h.mu.Unlock()
}

func (h *RehomeHandler) exportNotStarted() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.exportDown
}

// Wired tells the handler that everything a job may need has been bound or
// found absent: the Iceberg export (SetExport, SetExportNotStarted) and
// tiering (SetTiering). Jobs found at start are taken up from here, and new
// ones accepted.
func (h *RehomeHandler) Wired() {
	h.wiredOnce.Do(func() { close(h.wired) })
}

func (h *RehomeHandler) isWired() bool {
	select {
	case <-h.wired:
		return true
	default:
		return false
	}
}

// lateBound reports that this deployment has something a job needs that is
// bound after Recover runs.
func (h *RehomeHandler) lateBound() bool {
	return h.swap != nil && (h.swap.ExportExpected || h.swap.TieringExpected)
}

func (h *RehomeHandler) tieringManager() RehomeTiering {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tiering
}

// checkTieringSource is why tiering keeps a job off the file at key, as a
// refusal, or nil.
func (h *RehomeHandler) checkTieringSource(ctx context.Context, key string) error {
	err, _ := h.tieringSource(ctx, key)
	return err
}

// tieringSource is checkTieringSource, and whether the refusal is something
// tiering knows (the file is moving, or cold storage holds it or a copy)
// rather than something it could not find out.
func (h *RehomeHandler) tieringSource(ctx context.Context, key string) (refusal error, known bool) {
	if h.swap == nil || !h.swap.TieringExpected {
		return nil, false
	}
	t := h.tieringManager()
	if t == nil {
		if h.swap.ColdConfigured {
			return rehome.Refusef("tiered storage with a cold tier is configured and is not running on this node (no licence, or it failed to start), so cold storage cannot be asked whether it holds a copy of %s; moving the file's rows could leave them twice", key), false
		}
		return nil, false
	}
	err := t.CheckRehomeSource(ctx, key)
	switch {
	case err == nil:
		return nil, false
	case errors.Is(err, tiering.ErrFileMoving):
		return rehome.Refusef("%s is being moved to cold storage; send the request again once it has moved", key), true
	case errors.Is(err, tiering.ErrRehomeColdCopy):
		return rehome.Refusef("%s is on the hot tier and cold storage holds it too, or a copy of it: moving the hot file's rows would leave the cold one read beside them. POST /api/v1/tiering/reconcile finishes a migration that kept its hot copy; then send the request again", key), true
	}
	return rehome.Refusef("cold storage could not be asked whether it holds a copy of %s (%v); retry once it can", key, err), false
}

// canRecordTier reports whether tiering can record a file at key.
func (h *RehomeHandler) canRecordTier(key string) error {
	if t := h.tieringManager(); t != nil {
		return t.CanRecord(key)
	}
	return nil
}

// recordTierRows records the job's outputs as hot files. Safe to repeat.
func (h *RehomeHandler) recordTierRows(ctx context.Context, job *rehome.Job) error {
	t := h.tieringManager()
	if t == nil {
		return nil
	}
	files := make([]tiering.RehomedFile, len(job.Outputs))
	for i, o := range job.Outputs {
		files[i] = tiering.RehomedFile{Path: o.Key, SizeBytes: o.Size}
	}
	return t.RecordRehomedFiles(ctx, files)
}

// forgetTierRows removes the hot rows of files the job removed. A failure is
// logged: a tiering scan retires the row of a hot file that is gone, once
// the row is older than its margin (a few minutes).
func (h *RehomeHandler) forgetTierRows(ctx context.Context, id string, keys []string) {
	t := h.tieringManager()
	if t == nil {
		return
	}
	if err := t.ForgetRehomedFiles(ctx, keys); err != nil {
		h.logger.Warn().Err(err).Str("job", id).Msg("Re-home: could not remove the tier rows of files the job removed; a later tiering scan retires them")
	}
}

// Tiered storage on a cluster (#1869). Each node keeps its own tier rows, so
// each node's part of a job keeps them: the outputs' rows recorded before its
// reads may see the outputs (published, and again at latched and removed for
// a node that joined since), the source's row forgotten once the source is
// gone (removed), the outputs' rows forgotten on a revert. And each node checks,
// before it acks published, that its own tiering holds the source hot with no
// copy in cold storage: a migration that ran on another node before it saw the
// job makes the job revert rather than leave a cold copy read beside the
// outputs.

// clusterTieringReady fails while tiered storage is configured here and not
// yet bound (or found absent): the step is retried.
func (h *RehomeHandler) clusterTieringReady() error {
	if h.swap != nil && h.swap.TieringExpected && !h.isWired() {
		return errors.New("tiered storage on this node is not running yet")
	}
	return nil
}

// clusterTierCheck is this node's check of the source before it acks
// published.
func (h *RehomeHandler) clusterTierCheck(ctx context.Context, j raft.RehomeJob) error {
	if err := h.clusterTieringReady(); err != nil {
		return err
	}
	return h.checkTieringSource(ctx, j.Source.Path)
}

// clusterRecordTier records j's outputs as hot files on this node.
func (h *RehomeHandler) clusterRecordTier(ctx context.Context, j raft.RehomeJob) error {
	if err := h.clusterTieringReady(); err != nil {
		return err
	}
	return h.recordTierRows(ctx, localRecord(j, rehome.StateCommitted))
}

// clusterForgetSourceTier removes the source's row on this node.
func (h *RehomeHandler) clusterForgetSourceTier(ctx context.Context, j raft.RehomeJob) error {
	if err := h.clusterTieringReady(); err != nil {
		return err
	}
	h.forgetTierRows(ctx, j.ID, []string{j.Source.Path})
	return nil
}

// clusterForgetOutputsTier removes the outputs' rows on this node.
func (h *RehomeHandler) clusterForgetOutputsTier(ctx context.Context, j raft.RehomeJob) error {
	if err := h.clusterTieringReady(); err != nil {
		return err
	}
	keys := make([]string, len(j.Outputs))
	for i, o := range j.Outputs {
		keys[i] = o.Key
	}
	h.forgetTierRows(ctx, j.ID, keys)
	return nil
}

// clusterTierStep is this node's tier-row part of j's state (#1869): before
// it acks published, its check of the source and the outputs' rows; at
// latched and removed the outputs' rows again (a node that joined since), and
// at removed the source's row forgotten; at reverted the outputs' rows
// forgotten. A no-op without tiered storage.
func (h *RehomeHandler) clusterTierStep(ctx context.Context, j raft.RehomeJob, acked bool) error {
	if h.swap == nil || !h.swap.TieringExpected {
		return nil
	}
	switch j.State {
	case raft.RehomePublished:
		if acked {
			if err := h.clusterTierCheck(ctx, j); err != nil {
				return err
			}
		}
		return h.clusterRecordTier(ctx, j)
	case raft.RehomeLatched, raft.RehomeRemoved:
		// Again for a node that joined since published. A write that fails
		// here is logged, not retried into a stall: the job waits for every
		// node from latched on, and published wrote the rows on a node that
		// took part.
		if err := h.clusterRecordTier(ctx, j); err != nil {
			if rerr := h.clusterTieringReady(); rerr != nil {
				return rerr
			}
			h.logger.Warn().Err(err).Str("job", j.ID).Str("state", j.State).Msg("Re-home: could not record the outputs' tier rows; a later tiering scan records them")
		}
		if j.State == raft.RehomeRemoved {
			return h.clusterForgetSourceTier(ctx, j)
		}
		return nil
	case raft.RehomeReverted:
		return h.clusterForgetOutputsTier(ctx, j)
	}
	return nil
}
