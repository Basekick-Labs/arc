package api

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

// coldRetention is one execution's pass over the cold tier (#168). It is
// planned once per execution, then worked through one measurement at a time
// after that measurement's hot files.
//
// Nothing it cannot do aborts the execution: an unreadable, undeletable or
// unavailable cold file is counted and reported, and hot retention runs as it
// would without a cold tier. The counts are what keep that from being silent.
// A run that leaves expired cold data in place is recorded as
// completed_with_errors with a reason, never as a clean "completed" (the #746
// failure shape). Only a cancelled context stops the pass.
type coldRetention struct {
	backend storage.Backend                   // nil when the cold tier is not usable here
	files   map[string][]tiering.FileMetadata // candidates by measurement

	filesDeleted int
	rowsDeleted  int64

	// Why cold files were left in place.
	unavailable  int    // cold storage is not usable on this node
	quarantined  int    // tiering has marked the key unusable (#758)
	failed       int    // could not be checked, read or deleted
	discoveryErr string // the candidate query itself failed
	samples      []string

	// Rows whose cold object was already gone. Not an error: the cold sync
	// keeps such rows (it never retires one), so retention cleans them up.
	staleRetired  int
	staleReverted int // the file is in hot storage; the row now says hot

	removed int // objects deleted or rows changed: the query caches must go
}

const coldRetentionSampleCap = 32

// planColdRetention loads the policy's cold candidates. Never nil: without a
// cold tier the plan is empty and every step is a no-op.
func (h *RetentionHandler) planColdRetention(ctx context.Context, policy *RetentionPolicy, cutoff time.Time) *coldRetention {
	plan := &coldRetention{files: map[string][]tiering.FileMetadata{}}
	if h.coldTier == nil {
		return plan
	}
	files, err := h.coldTier.GetColdFilesForRetention(ctx, policy.Database, cutoff)
	if err != nil {
		h.logger.Error().Err(err).Str("database", policy.Database).
			Msg("Failed to load cold-tier retention candidates; the cold tier is not processed this run")
		plan.discoveryErr = err.Error()
		return plan
	}
	plan.backend = h.coldTier.ColdBackend()
	for _, f := range files {
		if policy.Measurement != nil && *policy.Measurement != "" && f.Measurement != *policy.Measurement {
			continue
		}
		if f.QuarantinedAt != nil {
			plan.quarantined++
			continue
		}
		if plan.backend == nil {
			plan.unavailable++
			continue
		}
		plan.files[f.Measurement] = append(plan.files[f.Measurement], f)
	}
	return plan
}

// addMeasurements returns the hot measurements plus the cold-only ones, which
// the hot listing cannot see once every file of a measurement has migrated.
func (c *coldRetention) addMeasurements(hot []string) []string {
	out := append([]string(nil), hot...)
	if c == nil {
		return out
	}
	seen := make(map[string]struct{}, len(out))
	for _, m := range out {
		seen[m] = struct{}{}
	}
	var coldOnly []string
	for m := range c.files {
		if _, ok := seen[m]; !ok {
			coldOnly = append(coldOnly, m)
		}
	}
	sort.Strings(coldOnly)
	return append(out, coldOnly...)
}

func (c *coldRetention) skip(path string, err error) {
	c.failed++
	if len(c.samples) < coldRetentionSampleCap {
		c.samples = append(c.samples, fmt.Sprintf("%s: %v", path, err))
	}
}

// deleteOldColdFiles applies the cutoff to one measurement's cold files and
// reports the rows and files deleted (or, in a dry run, that would be).
//
// The object is deleted before its row, so a failure in between leaves a row
// for a missing object, which the next run retires. A row is never removed
// while its object may still exist.
func (h *RetentionHandler) deleteOldColdFiles(ctx context.Context, c *coldRetention, measurement string, cutoff time.Time, dryRun bool) (int64, int, error) {
	if c == nil || c.backend == nil {
		return 0, 0, nil
	}
	var rows int64
	var deleted, retire []string
	for _, f := range c.files[measurement] {
		if err := ctx.Err(); err != nil {
			if !dryRun {
				h.finishColdMeasurement(ctx, c, deleted, retire)
				c.rowsDeleted += rows
			}
			return rows, len(deleted), err
		}

		// Checked first, by type: a row whose object is gone is not a read
		// error, and the footer read would only report it as HTTP text.
		size, err := c.backend.StatFile(ctx, f.Path)
		if err != nil {
			c.skip(f.Path, fmt.Errorf("check cold object: %w", err))
			continue
		}
		if size < 0 {
			if dryRun {
				continue
			}
			// The same file can still be in hot storage: a stale cold row
			// over a live hot copy. Point the row back at it, as orphan
			// reconciliation does, rather than unregister a file queries
			// still read.
			hotSize, err := h.storage.StatFile(ctx, f.Path)
			switch {
			case err != nil:
				c.skip(f.Path, fmt.Errorf("check hot copy of a missing cold object: %w", err))
			case hotSize >= 0:
				if err := h.coldTier.RevertColdRowToHot(ctx, f.Path); err != nil {
					c.skip(f.Path, fmt.Errorf("revert row to hot: %w", err))
				} else {
					c.staleReverted++
					c.removed++
				}
			default:
				retire = append(retire, f.Path)
			}
			continue
		}

		fullPath, err := readParquetPath(c.backend, f.Path)
		if err != nil {
			c.skip(f.Path, err)
			continue
		}
		maxTime, rowCount, err := h.getFileMaxTimeAndRowCount(ctx, fullPath)
		if err != nil {
			c.skip(f.Path, fmt.Errorf("read max(time): %w", err))
			continue
		}
		if !maxTime.Before(cutoff) {
			continue
		}

		h.logger.Info().Str("file", filepath.Base(f.Path)).Str("tier", string(tiering.TierCold)).
			Time("max_time", maxTime).Int64("rows", rowCount).Bool("dry_run", dryRun).
			Msg("File eligible for deletion")
		if dryRun {
			rows += rowCount
			deleted = append(deleted, f.Path)
			continue
		}
		del := c.backend.Delete
		if h.coldDeleteHook != nil {
			del = func(ctx context.Context, path string) error { return h.coldDeleteHook(ctx, c.backend, path) }
		}
		if err := del(ctx, f.Path); err != nil {
			// A delete can fail after the object went (a timed-out response).
			// Keep the row only while the object may still be there.
			if after, statErr := c.backend.StatFile(ctx, f.Path); statErr != nil || after >= 0 {
				c.skip(f.Path, fmt.Errorf("delete: %w", err))
				continue
			}
		}
		rows += rowCount
		deleted = append(deleted, f.Path)
	}
	if dryRun {
		c.rowsDeleted += rows
		c.filesDeleted += len(deleted)
		return rows, len(deleted), nil
	}
	h.finishColdMeasurement(ctx, c, deleted, retire)
	c.rowsDeleted += rows
	return rows, len(deleted), nil
}

// finishColdMeasurement retires the rows of deleted and already-missing
// objects. The query caches are dropped once per run, by notifyColdRemoved.
func (h *RetentionHandler) finishColdMeasurement(ctx context.Context, c *coldRetention, deleted, missing []string) {
	c.filesDeleted += len(deleted)
	paths := append(append([]string(nil), deleted...), missing...)
	if len(paths) > 0 {
		// Detached: these objects are gone whatever happens to the request,
		// and a row left behind costs a stat on the next run.
		removed, err := h.coldTier.RetireColdRows(context.WithoutCancel(ctx), paths)
		if err != nil {
			h.logger.Warn().Err(err).Int("removed", removed).Int("rows", len(paths)).
				Msg("Failed to retire cold tier rows; the next retention run retires them")
		}
		c.staleRetired += len(missing)
	}
	c.removed += len(deleted) + len(missing)
}

// notifyColdRemoved drops the query caches once, at the end of a run that
// removed cold objects or rows, however the run ended: a cached read can name
// a cold glob that now matches nothing. Once per run rather than per
// measurement, because it drops every measurement's cached transforms.
func (h *RetentionHandler) notifyColdRemoved(c *coldRetention) {
	if c.removed > 0 && h.coldTier != nil {
		h.coldTier.NotifyColdFilesRemoved(c.removed)
	}
}

func (c *coldRetention) skippedTotal() int {
	if c == nil {
		return 0
	}
	return c.unavailable + c.quarantined + c.failed
}

// skippedReason says why cold data was left in place, or "" when nothing was.
func (c *coldRetention) skippedReason() string {
	if c == nil {
		return ""
	}
	var parts []string
	if c.discoveryErr != "" {
		parts = append(parts, "cold tier not processed: "+c.discoveryErr)
	}
	if c.unavailable > 0 {
		parts = append(parts, fmt.Sprintf("%d cold file(s) not examined because cold storage is not enabled on this node", c.unavailable))
	}
	if c.quarantined > 0 {
		parts = append(parts, fmt.Sprintf("%d quarantined cold file(s) left in place", c.quarantined))
	}
	if c.failed > 0 {
		parts = append(parts, fmt.Sprintf("%d cold file(s) could not be checked, read or deleted; see the log", c.failed))
	}
	return strings.Join(parts, "; ")
}

func (c *coldRetention) logSkipped(logger zerolog.Logger, database string) {
	if c == nil {
		return
	}
	if c.staleRetired > 0 || c.staleReverted > 0 {
		logger.Info().Str("database", database).Int("retired", c.staleRetired).Int("reverted_to_hot", c.staleReverted).
			Msg("Retention cleaned up cold tier rows whose cold object was already gone")
	}
	if reason := c.skippedReason(); reason != "" {
		logger.Error().Str("database", database).Int("cold_files_skipped", c.skippedTotal()).
			Strs("sample", c.samples).Str("reason", reason).
			Msg("Retention left expired cold-tier data in place")
	}
}

func joinRetentionDetail(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + "; " + b
}
