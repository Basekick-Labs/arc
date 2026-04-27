package reconciliation

import (
	"context"
	"fmt"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// sweepOrphanStorage deletes storage files that have no manifest entry
// and have aged past the grace window. Runs AFTER sweepOrphanManifest
// completes successfully — manifest-before-storage ordering across the
// whole reconcile cycle, not just per-batch. The rationale: if Raft
// quorum is unhealthy we want to know before touching any bytes.
//
// Re-check policy: every candidate is re-verified via
// coordinator.GetFileEntry. A `true` result means a concurrent register
// won the race (or the writer's flush completed between snapshot and
// now). We skip these and increment SkippedRecheck on the run.
//
// Failure policy: storage delete errors are warn-and-continue. The
// file may have been deleted by another path (retention, compaction,
// concurrent reconcile run on a different node — though the gate
// prevents that in shared mode). The next reconcile cycle will see it
// gone and not flag it again.
//
// Pre-manifest orphans: files whose path doesn't fit the
// `database/measurement/...` layout are gated behind
// DeletePreManifestOrphans (default true). When false, we skip them
// entirely so a misconfigured walk on shared infrastructure can't
// touch unrelated bytes.
func (r *Reconciler) sweepOrphanStorage(
	ctx context.Context,
	run *Run,
	candidates []orphanStorageCandidate,
	dryRun bool,
) error {
	if len(candidates) == 0 {
		return nil
	}
	if r.cfg.BackendKind == BackendStandalone {
		// Standalone Arc has no manifest — every storage file would be
		// classified as orphan-storage and (with DeletePreManifestOrphans
		// default true) deleted en masse. The wiring layer at
		// cmd/arc/main.go gates reconciliation on clusterCoordinator
		// being non-nil, so this branch is unreachable in production.
		// Defensive guard: refuse to act here too in case a future
		// caller misconfigures.
		r.logger.Warn().
			Int("candidates", len(candidates)).
			Msg("Reconciliation: refusing to run orphan-storage sweep in BackendStandalone mode")
		return nil
	}

	// Filter pre-manifest orphans if disabled. Detection: the path
	// doesn't have at least three segments (`<db>/<m>/<...>`). All Arc
	// data paths have at least 3.
	filtered := make([]orphanStorageCandidate, 0, len(candidates))
	for _, c := range candidates {
		if !r.cfg.DeletePreManifestOrphans && !looksLikeManagedPath(c.path) {
			continue
		}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 {
		return nil
	}

	batchSize := r.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	bd, hasBatchDelete := r.storage.(storage.BatchDeleter)

	for i := 0; i < len(filtered); i += batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.gate != nil && !r.gate.ShouldRunStorageScan() {
			return fmt.Errorf("reconciliation: storage scan gate revoked mid-run")
		}
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
				Msg("Reconciliation: per-run blast cap hit; stopping storage sweep")
			return nil
		}

		end := i + batchSize
		if end > len(filtered) {
			end = len(filtered)
		}
		chunk := filtered[i:end]

		// Re-check every candidate against the current manifest. If a
		// concurrent register raced, the entry will be there now and we
		// must not delete the file.
		toDelete := make([]string, 0, len(chunk))
		remainingCap := r.cfg.MaxDeletesPerRun - (run.ManifestDeletes + run.StorageDeletes)
		capReachedInChunk := false
		for _, c := range chunk {
			if len(toDelete) >= remainingCap {
				capReachedInChunk = true
				break
			}
			if r.coord != nil {
				if _, ok := r.coord.GetFileEntry(c.path); ok {
					run.SkippedRecheck++
					continue
				}
			}
			toDelete = append(toDelete, c.path)
		}

		if len(toDelete) == 0 {
			if capReachedInChunk {
				run.CapHit = true
			}
			continue
		}

		if dryRun {
			run.StorageDeletes += len(toDelete)
			r.emitAudit("reconcile.storage_batch_deleted", run, map[string]string{
				"run_id": run.ID,
				"count":  fmt.Sprintf("%d", len(toDelete)),
				"sample": joinSample(toDelete, r.cfg.SamplePathsCap),
				"mode":   "dry_run",
			})
			if capReachedInChunk {
				run.CapHit = true
				return nil
			}
			continue
		}

		applied := r.applyStorageDeletes(ctx, run, toDelete, bd, hasBatchDelete)
		run.StorageDeletes += applied
		r.emitAudit("reconcile.storage_batch_deleted", run, map[string]string{
			"run_id": run.ID,
			"count":  fmt.Sprintf("%d", applied),
			"sample": joinSample(toDelete, r.cfg.SamplePathsCap),
		})
		if capReachedInChunk {
			run.CapHit = true
			r.emitAudit("reconcile.cap_hit", run, map[string]string{
				"run_id":         run.ID,
				"deletes_so_far": fmt.Sprintf("%d", run.ManifestDeletes+run.StorageDeletes),
			})
			r.logger.Warn().
				Str("run_id", run.ID).
				Int("storage_deletes", run.StorageDeletes).
				Int("cap", r.cfg.MaxDeletesPerRun).
				Msg("Reconciliation: per-run blast cap hit; stopping storage sweep")
			return nil
		}
	}
	return nil
}

// applyStorageDeletes executes the actual deletes. Prefers BatchDeleter
// when the backend supports it (S3, Azure both do via DeleteObjects).
// Per-file Delete failures are recorded in run.Errors and summarized
// in a single Warn at the end of the batch — emitting one Warn per
// failure floods operator dashboards under transient backend issues.
// The next reconcile run will see the file again and retry.
//
// Returns the count of files we believe we deleted. Backends that
// silently succeed on a non-existent file (S3) inflate this count
// slightly relative to "files actually removed", but for run reporting
// the audit "we attempted to delete N" is the right semantics.
func (r *Reconciler) applyStorageDeletes(
	ctx context.Context,
	run *Run,
	paths []string,
	bd storage.BatchDeleter,
	hasBatchDelete bool,
) int {
	if hasBatchDelete && len(paths) > 1 {
		if err := bd.DeleteBatch(ctx, paths); err != nil {
			// Batch delete failures fall through to per-file delete so
			// a single bad path doesn't block the rest.
			r.logger.Warn().
				Err(err).
				Int("count", len(paths)).
				Msg("Reconciliation: BatchDeleter.DeleteBatch failed; falling back to per-file Delete")
		} else {
			return len(paths)
		}
	}
	applied := 0
	deleteErrCount := 0
	var deleteLastErr error
	for _, p := range paths {
		if err := r.storage.Delete(ctx, p); err != nil {
			deleteErrCount++
			deleteLastErr = err
			run.Errors = appendBounded(run.Errors, fmt.Sprintf("delete %q: %v", p, err), 32)
			continue
		}
		applied++
	}
	if deleteErrCount > 0 {
		r.logger.Warn().
			Err(deleteLastErr).
			Int("batch_failed_deletes", deleteErrCount).
			Int("batch_size", len(paths)).
			Msg("Reconciliation: storage.Delete failures — affected files will retry on next run")
	}
	return applied
}

// looksLikeManagedPath returns true if a path conforms to the Arc
// storage layout `<database>/<measurement>/<year>/<month>/<day>/<hour>/<file>`.
// Used to gate pre-manifest orphan deletion when an operator sets
// DeletePreManifestOrphans=false: only paths that look unambiguously
// like Arc data are touched, leaving stray files in mis-shared buckets
// alone.
//
// The check requires ≥7 segments (db/m/yyyy/mm/dd/hh/file.parquet),
// rejects "." or ".." segments to defend against traversal-shaped
// inputs, and rejects empty segments. Anything else is left alone.
func looksLikeManagedPath(p string) bool {
	parts := strings.Split(p, "/")
	if len(parts) < 7 {
		return false
	}
	for _, seg := range parts {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// appendBounded appends to a string slice but caps growth to keep the
// Run summary memory bounded under pathological failure conditions.
func appendBounded(s []string, msg string, cap int) []string {
	if cap > 0 && len(s) >= cap {
		return s
	}
	return append(s, msg)
}
