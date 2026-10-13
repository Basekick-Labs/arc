package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
)

// The cluster coordinator is what production wires in.
var _ compactionCommitter = (*cluster.Coordinator)(nil)

// Row DELETE under compaction commit (#1207 d2): with the cluster flag on,
// the DELETE's manifest ops apply only to the content it read, and a file a
// compaction replaced while the DELETE ran is scanned again through the
// compaction's output.

// errFileChanged: the manifest refused a DELETE op because its file is no
// longer registered at the content the DELETE read.
var errFileChanged = errors.New("file changed under the delete")

// deleteChangedRounds bounds how many times a DELETE scans again after files
// changed under it.
const deleteChangedRounds = 3

// compactionCommitter is the coordinator's compaction commit side.
type compactionCommitter interface {
	CompactionCommitActive() bool
	WaitLocalFiles(ctx context.Context, database, measurement string, budget time.Duration) ([]string, error)
	// SyncManifest returns once this node applied everything the leader
	// had committed.
	SyncManifest(ctx context.Context) error
}

// errManifestNotSynced: this node could not catch up with the cluster
// manifest before a scan; a file registered a moment ago could be missed.
var errManifestNotSynced = errors.New("this node could not catch up with the cluster manifest")

// casActive reports whether DELETE ops carry the content they read.
func casActive(coord any) bool {
	c, ok := coord.(compactionCommitter)
	return ok && c.CompactionCommitActive()
}

// registrationHolder is the registration outbox's view of a path it still
// owns (pending, parked, or confirmed a moment ago).
type registrationHolder interface {
	HoldsRegistration(path string) bool
}

// registeredOrPending keeps the listed files the manifest lists (with their
// checksum) or this node's outbox still holds, and logs the rest. The local
// manifest is caught up with the leader first: a registration acknowledged
// by the leader (a rewrite of an earlier DELETE) may not have applied here.
func (h *DeleteHandler) registeredOrPending(ctx context.Context, files []fileInfo) ([]fileInfo, map[string]string, error) {
	if c, ok := h.coordinator.(compactionCommitter); ok {
		if err := c.SyncManifest(ctx); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", errManifestNotSynced, err)
		}
	}
	holder, _ := h.pending.(registrationHolder)
	shas := make(map[string]string)
	kept := files[:0]
	var skipped []string
	for _, f := range files {
		if e, ok := h.coordinator.GetFileEntry(f.relativePath); ok {
			shas[f.relativePath] = e.SHA256
			kept = append(kept, f)
			continue
		}
		if holder != nil && holder.HoldsRegistration(f.relativePath) {
			kept = append(kept, f)
			continue
		}
		skipped = append(skipped, f.relativePath)
	}
	if len(skipped) > 0 {
		h.logger.Info().Int("files", len(skipped)).Strs("sample", skipped[:min(len(skipped), 5)]).
			Msg("Delete skipped files in storage the cluster manifest does not list (compaction output not committed yet, or an input already replaced)")
	}
	return kept, shas, nil
}

// rescanAfterChange waits until the files the manifest lists for the
// measurement (the compaction's output among them) are on this node, then
// scans again. Files still missing are returned; unless allowUnreachable,
// they stop the DELETE.
func (h *DeleteHandler) rescanAfterChange(ctx context.Context, database, measurement, where string, allowUnreachable bool) ([]affectedFile, []string, error) {
	var missing []string
	if c, ok := h.coordinator.(compactionCommitter); ok {
		var err error
		if missing, err = c.WaitLocalFiles(ctx, database, measurement, deleteLocalWait); err != nil {
			return nil, nil, err
		}
		if len(missing) > 0 && !allowUnreachable {
			return nil, missing, errUnreachableFiles
		}
	}
	affected, err := h.findAffectedFiles(ctx, database, measurement, where, true)
	return affected, missing, err
}

// mergeUnreachable adds the files in more to list, once each.
func mergeUnreachable(list, more []string) []string {
	for _, m := range more {
		if !slices.Contains(list, m) {
			list = append(list, m)
		}
	}
	return list
}
