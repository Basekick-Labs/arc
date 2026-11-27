package arcxrouter

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
)

// engineQueryEntries applies open-compaction exclusions to either the pruned
// list or the ordinary local listing. Exclusions must be snapshotted first:
// inputs are excluded only once the replacement output is present in storage.
func engineQueryEntries(
	ctx context.Context,
	local *storage.LocalBackend,
	shape, database, measurement, whereText string,
	preds []scanPred,
	pruner *pruning.PartitionPruner,
	exclude func(context.Context, string) []string,
) ([]string, error) {
	prefix := database + "/" + measurement + "/"
	var skip map[string]struct{}
	if exclude != nil {
		for _, key := range exclude(ctx, prefix) {
			if skip == nil {
				skip = make(map[string]struct{})
			}
			skip[key] = struct{}{}
		}
	}
	entries, pruned := engineParquetEntries(ctx, local, shape, database, measurement, whereText, preds, pruner)
	if !pruned {
		var err error
		entries, err = local.List(ctx, prefix)
		if err != nil {
			return nil, err
		}
	}
	filtered := entries[:0]
	for _, entry := range entries {
		if !strings.HasSuffix(entry, ".parquet") {
			continue
		}
		if _, excluded := skip[entry]; !excluded {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

// engineParquetEntries returns a fresh, range-limited file list when a routed
// query has time bounds that can be safely extracted. Other shapes keep using
// the backend's normal measurement listing.
func engineParquetEntries(
	ctx context.Context,
	local *storage.LocalBackend,
	shape, database, measurement, whereText string,
	preds []scanPred,
	pruner *pruning.PartitionPruner,
) ([]string, bool) {
	if local == nil || pruner == nil {
		return nil, false
	}
	where := engineTimeWhere(shape, whereText, preds)
	if where == "" {
		return nil, false
	}

	// The date-trunc matcher restricts its WHERE to AND-conjoined time comparisons.
	// Flat scan predicates are also safe because their atoms are AND-conjoined;
	// boolean trees are represented by WhereText and are deliberately ignored.
	timeRange := pruner.ExtractTimeRange("SELECT * FROM _ WHERE " + where)
	if timeRange == nil {
		return nil, false
	}
	// Only a range the query bounds on both sides is pruned. A start-only
	// predicate (`time > now() - INTERVAL '1 hour'`, the dashboard shape) gets
	// an ASSUMED end of now+24h, and pruning to it would drop rows stamped past
	// that end (forecasts, devices whose clocks run ahead). The DuckDB path
	// keeps them by adding the partitions that exist past the assumed end, and
	// the newest partition for an empty window (pruning/open_end.go, called
	// from PartitionPruner.optimize). This path does not do that yet, so it
	// takes the full listing instead: correct, and as slow as before #661.
	//
	// TODO(arcx, #661): when arcx work resumes, prune start-only ranges too by
	// reusing discoverOpenEnd/appendBeyond (with a future-stamped partition
	// test for both shapes). Also stop logging here per query:
	// GeneratePartitionPaths logs "Generated partition paths" at Info and bumps
	// QueriesOptimized, which the DuckDB path does only on a partitionCache
	// miss but this path does on every routed query.
	if timeRange.EndAssumed {
		return nil, false
	}
	// GeneratePartitionPaths treats End as exclusive. Include its containing
	// hour even when the SQL uses an inclusive upper bound; the engine's WHERE
	// clause then removes rows beyond an exclusive upper bound as usual.
	timeRange.End = timeRange.End.Add(time.Nanosecond)

	basePath := local.GetBasePath()
	patterns := pruner.GeneratePartitionPaths(ctx, basePath, database, measurement, timeRange)
	if len(patterns) == 0 {
		return nil, false
	}

	entries := make([]string, 0, len(patterns))
	checkedDirs := make(map[string]bool)
	for _, pattern := range patterns {
		if err := ctx.Err(); err != nil {
			return nil, false
		}
		matches, err := filepath.Glob(pattern)
		if err != nil {
			// Generated patterns should be valid, but a failed expansion must not
			// turn into a partial answer. Let the caller use the normal full list.
			return nil, false
		}
		for _, match := range matches {
			rel, err := filepath.Rel(basePath, match)
			if err != nil {
				return nil, false
			}
			rel = filepath.ToSlash(rel)
			// Match LocalBackend.List's listing contract: dot-prefixed entries and
			// keys rejected by storage validation are not queryable objects.
			if strings.HasPrefix(filepath.Base(match), ".") || storage.ValidateKey(rel) != nil {
				continue
			}
			info, err := os.Lstat(match)
			if err != nil {
				return nil, false
			}
			if info.IsDir() {
				continue
			}
			// Glob traverses directory symlinks; LocalBackend.List's WalkDir does
			// not. Fall back rather than introduce files from a linked directory.
			// Lstat preserves List's existing treatment of terminal file symlinks.
			for dir := filepath.Dir(match); dir != basePath; dir = filepath.Dir(dir) {
				// ValidateKey(rel) established containment; still decline if a
				// root is reached unexpectedly instead of looping at that root.
				if parent := filepath.Dir(dir); parent == dir {
					return nil, false
				}
				if checkedDirs[dir] {
					break
				}
				info, err := os.Lstat(dir)
				if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return nil, false
				}
				checkedDirs[dir] = true
			}
			entries = append(entries, rel)
		}
	}
	if len(entries) == 0 {
		// A missing pruned match is not proof that the measurement is empty.
		// Preserve the existing fallback behaviour for unusual layouts.
		return nil, false
	}

	sort.Strings(entries)
	return entries, true
}

// engineTimeWhere returns only time bounds that are necessary conditions for a
// routed query. Date-trunc WHERE text is already restricted by its matcher. A
// flat scan predicate list is also safe because its atoms are AND-conjoined; any
// boolean tree is represented by WhereText instead and is intentionally ignored.
func engineTimeWhere(shape, whereText string, preds []scanPred) string {
	if shape == ShapeDateTruncCent {
		return whereText
	}
	if shape != ShapeScan || len(preds) == 0 {
		return ""
	}

	var bounds []string
	for _, pred := range preds {
		if !strings.EqualFold(pred.col, timeColumn) || !pred.isStr || pred.isNull {
			continue
		}
		switch pred.op {
		case ">", ">=", "<", "<=":
			bounds = append(bounds, timeColumn+" "+pred.op+" '"+escapeStringLiteral(pred.str)+"'")
		}
	}
	return strings.Join(bounds, " AND ")
}
