package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

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
			return fmt.Errorf("manifest sweep: %w", ErrGateRevoked)
		}
		// Blast cap.
		if run.ManifestDeletes+run.StorageDeletes >= r.cfg.MaxDeletesPerRun {
			run.CapHit = true
			r.emitAudit("reconcile.cap_hit", run, map[string]string{
				"run_id":         run.ID,
				"deletes_so_far": strconv.Itoa(run.ManifestDeletes + run.StorageDeletes),
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

		// Slice the chunk down to the cap before doing any I/O — no point
		// running parallel Exists checks on files we'd never act on.
		chunkLimit := len(chunk)
		if chunkLimit > remainingCap {
			chunkLimit = remainingCap
			capReachedInChunk = true
		}
		recheckPaths := chunk[:chunkLimit]

		// Re-check storage in parallel: each Exists is one HEAD on S3/Azure
		// (network RTT) and the chunk can be up to BatchSize=1000 paths.
		// Sequential would be unacceptably slow on remote backends.
		// Bounded concurrency (default 8) stays under cloud rate limits.
		// Per-batch counters for log-spam control: a transient backend
		// hiccup can fail Exists for hundreds of files in a row, and
		// printing one Warn per file drowns operator dashboards.
		// Collect into run.Errors (capped at 32) and emit a single
		// summary Warn at the end of the batch.
		recheckResults := r.parallelExists(ctx, recheckPaths)
		existsErrCount := 0
		var existsLastErr error
		for idx, res := range recheckResults {
			p := recheckPaths[idx]
			if res.err != nil {
				// Treat exists-check failures as "skip and let the
				// next run retry" — better than risking a wrong delete.
				existsErrCount++
				existsLastErr = res.err
				run.Errors = appendBounded(run.Errors, fmt.Sprintf("Exists %q: %v", p, res.err), 32)
				continue
			}
			if res.exists {
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

		// Per-batch summary log so a transient backend hiccup that
		// fails Exists across the chunk produces ONE Warn, not N.
		// Per-file detail still lives in run.Errors (bounded at 32).
		if existsErrCount > 0 {
			r.logger.Warn().
				Err(existsLastErr).
				Int("batch_failed_exists", existsErrCount).
				Int("batch_size", len(chunk)).
				Msg("Reconciliation: storage.Exists failures during manifest sweep — affected files skipped (next run retries)")
		}

		// If len(ops)==0 we have no Raft work for this chunk (every file
		// was recheck-skipped, errored, or marshal-failed). Apply the
		// cap+audit BEFORE the skip, otherwise a chunk that only shrunk
		// down via recheck-skip silently bypasses CapHit reporting and
		// the reconciler keeps doing recheck I/O on subsequent chunks.
		if capReachedInChunk {
			run.CapHit = true
			r.emitAudit("reconcile.cap_hit", run, map[string]string{
				"run_id":         run.ID,
				"deletes_so_far": strconv.Itoa(run.ManifestDeletes + run.StorageDeletes),
			})
			r.logger.Warn().
				Str("run_id", run.ID).
				Int("manifest_deletes", run.ManifestDeletes).
				Int("ops_in_chunk", len(ops)).
				Int("cap", r.cfg.MaxDeletesPerRun).
				Msg("Reconciliation: per-run blast cap hit; stopping manifest sweep")
		}
		if len(ops) == 0 {
			if capReachedInChunk {
				return nil
			}
			continue
		}
		if dryRun {
			// Dry-run still counts the work we *would* have done.
			run.ManifestDeletes += len(ops)
			r.emitAudit("reconcile.manifest_batch_deleted", run, map[string]string{
				"run_id": run.ID,
				"count":  strconv.Itoa(len(ops)),
				"sample": joinSample(applied, r.cfg.SamplePathsCap),
				"mode":   "dry_run",
			})
			if capReachedInChunk {
				return nil
			}
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
			"count":  strconv.Itoa(len(ops)),
			"sample": joinSample(applied, r.cfg.SamplePathsCap),
		})
		if capReachedInChunk {
			return nil
		}
	}
	return nil
}

// existsResult is one slot in the output of parallelExists. Index in
// the result slice matches the index in the input paths slice so the
// caller can build ops in the original order without sorting.
type existsResult struct {
	exists bool
	err    error
}

// parallelExists runs storage.Exists across `paths` with bounded
// concurrency. The result slice has the same length and order as
// `paths`. Sequential when RecheckConcurrency is 1 or paths is short
// enough that the goroutine overhead would outweigh the I/O savings.
//
// Each Exists call uses the run's ctx so a canceled run aborts in
// flight checks. The dispatch loop honors ctx cancellation between
// jobs; in-flight HEAD requests are cut by the storage backend's own
// ctx-aware client.
func (r *Reconciler) parallelExists(ctx context.Context, paths []string) []existsResult {
	results := make([]existsResult, len(paths))
	if len(paths) == 0 {
		return results
	}
	workers := r.cfg.RecheckConcurrency
	if workers <= 1 || len(paths) < 2 {
		// Sequential fast path: avoid the goroutine + channel overhead
		// for trivially small chunks or operator-forced sequential mode.
		for i, p := range paths {
			if ctx.Err() != nil {
				results[i] = existsResult{err: ctx.Err()}
				continue
			}
			exists, err := r.storage.Exists(ctx, p)
			results[i] = existsResult{exists: exists, err: err}
		}
		return results
	}
	if workers > len(paths) {
		workers = len(paths)
	}

	// jobs feeds indexes (not paths) so workers can write directly into
	// the results slice at the right slot. Channel is buffered to the
	// total job count so the dispatcher never blocks.
	jobs := make(chan int, len(paths))
	for i := range paths {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					results[idx] = existsResult{err: ctx.Err()}
					continue
				}
				exists, err := r.storage.Exists(ctx, paths[idx])
				results[idx] = existsResult{exists: exists, err: err}
			}
		}()
	}
	wg.Wait()
	return results
}
