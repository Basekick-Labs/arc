package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// sweepOrphanManifest issues chunked Raft deletes for manifest entries
// whose underlying file is missing from storage. Mirrors the retention
// chunking pattern at internal/api/retention.go:827-866 — same chunk
// size, same manifest-failure abort semantics, same per-file marshal
// safety (a marshal failure skips the file rather than corrupting the
// batch).
//
// Re-check policy: before issuing each batch, every path is re-verified
// via storage.Exists. A `true` result means a concurrent register raced
// us — typical scenario is the writer completing a flush after we
// snapshotted the manifest. We skip these and increment SkippedRecheck
// on the run.
//
// Failure policy: on BatchFileOpsInManifest error we abort the run.
// Quoting retention: "On manifest failure we abort — a Raft quorum loss
// is not transient." The blast cap is checked at every batch boundary
// so a run with thousands of orphans terminates cleanly when the cap
// is hit.
//
// In dry-run mode we still execute the re-check (to produce accurate
// counts in the run report) but never call BatchFileOpsInManifest.
func (r *Reconciler) sweepOrphanManifest(
	ctx context.Context,
	run *Run,
	paths []string,
	dryRun bool,
) error {
	if len(paths) == 0 {
		return nil
	}
	if r.cfg.BackendKind == BackendStandalone || r.coord == nil {
		// No manifest to sweep.
		return nil
	}

	batchSize := r.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(paths); i += batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Re-gate at every chunk boundary so a lost lease/leadership
		// stops the run promptly without orphaning new manifest writes.
		if r.gate != nil && !r.gate.ShouldRunManifestSweep() {
			return fmt.Errorf("reconciliation: manifest sweep gate revoked mid-run")
		}
		// Blast cap.
		if run.ManifestDeletes+run.StorageDeletes >= r.cfg.MaxDeletesPerRun {
			run.CapHit = true
			r.emitAudit("reconcile.cap_hit", run, map[string]string{
				"run_id":         run.ID,
				"deletes_so_far": fmt.Sprintf("%d", run.ManifestDeletes+run.StorageDeletes),
			})
			r.logger.Warn().
				Str("run_id", run.ID).
				Int("manifest_deletes", run.ManifestDeletes).
				Int("storage_deletes", run.StorageDeletes).
				Int("cap", r.cfg.MaxDeletesPerRun).
				Msg("Reconciliation: per-run blast cap hit; stopping manifest sweep")
			return nil
		}

		end := i + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		chunk := paths[i:end]

		// Build the BatchFileOp slice with re-check + capped marshalling.
		// Mirrors retention's pattern at internal/api/retention.go:846-855:
		// a marshal failure skips the file rather than poisoning the batch.
		ops := make([]raft.BatchFileOp, 0, len(chunk))
		applied := make([]string, 0, len(chunk))
		remainingCap := r.cfg.MaxDeletesPerRun - (run.ManifestDeletes + run.StorageDeletes)
		capReachedInChunk := false
		for _, p := range chunk {
			if len(ops) >= remainingCap {
				// Stop building this batch at the cap. Mark CapHit so
				// the run report tells operators we did not finish.
				capReachedInChunk = true
				break
			}
			// Re-check storage. If the file came back, the manifest
			// entry is no longer orphaned and we leave it alone.
			exists, err := r.storage.Exists(ctx, p)
			if err != nil {
				// Treat exists-check failures as "skip and let the
				// next run retry" — better than risking a wrong delete.
				r.logger.Warn().Err(err).Str("path", p).Msg("Reconciliation: storage.Exists failed during manifest sweep; skipping")
				continue
			}
			if exists {
				run.SkippedRecheck++
				continue
			}
			payload, err := json.Marshal(raft.DeleteFilePayload{Path: p, Reason: "reconcile-orphan-manifest"})
			if err != nil {
				r.logger.Warn().Err(err).Str("path", p).Msg("Reconciliation: failed to marshal manifest delete op; skipping file")
				continue
			}
			ops = append(ops, raft.BatchFileOp{Type: raft.CommandDeleteFile, Payload: payload})
			applied = append(applied, p)
		}

		if len(ops) == 0 {
			continue
		}
		if dryRun {
			// Dry-run still counts the work we *would* have done.
			run.ManifestDeletes += len(ops)
			r.emitAudit("reconcile.manifest_batch_deleted", run, map[string]string{
				"run_id": run.ID,
				"count":  fmt.Sprintf("%d", len(ops)),
				"sample": joinSample(applied, r.cfg.SamplePathsCap),
				"mode":   "dry_run",
			})
			continue
		}
		if err := r.coord.BatchFileOpsInManifest(ops); err != nil {
			// Mirrors retention: aborting the run is the right call —
			// Raft quorum loss is not transient. Keep partial progress
			// counts so the run report reflects what we did manage.
			return fmt.Errorf("manifest sweep batch (offset %d, size %d): %w", i, len(ops), err)
		}
		run.ManifestDeletes += len(ops)
		r.emitAudit("reconcile.manifest_batch_deleted", run, map[string]string{
			"run_id": run.ID,
			"count":  fmt.Sprintf("%d", len(ops)),
			"sample": joinSample(applied, r.cfg.SamplePathsCap),
		})
		if capReachedInChunk {
			run.CapHit = true
			r.emitAudit("reconcile.cap_hit", run, map[string]string{
				"run_id":         run.ID,
				"deletes_so_far": fmt.Sprintf("%d", run.ManifestDeletes+run.StorageDeletes),
			})
			r.logger.Warn().
				Str("run_id", run.ID).
				Int("manifest_deletes", run.ManifestDeletes).
				Int("cap", r.cfg.MaxDeletesPerRun).
				Msg("Reconciliation: per-run blast cap hit; stopping manifest sweep")
			return nil
		}
	}
	return nil
}

// joinSample concatenates a bounded list of paths into a single string
// (newline-separated) for inclusion in the audit Detail map. The audit
// schema stores Detail as JSON-encoded values, so this keeps the wire
// format simple.
func joinSample(paths []string, limit int) string {
	if limit <= 0 || limit > len(paths) {
		limit = len(paths)
	}
	if limit == 0 {
		return ""
	}
	return strings.Join(paths[:limit], "\n")
}
