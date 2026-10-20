package arcxrouter

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
)

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
