package pruning

import (
	"context"
	"path/filepath"
	"strings"
	"time"
)

// File-level time pruning. EXPERIMENTAL in 26.09.2 (opt-in via
// query.file_time_pruning); promotion to stable + default-on is tracked in
// #659, targeting 27.01.1.
//
// Partition pruning stops at hour-directory globs; the current (live) hour of
// a high-frequency ingest workload can accumulate thousands of small parquet
// files (one per buffer flush), and DuckDB lists + footer-reads every one of
// them on every query. This pass expands the ONE hour glob that contains the
// query's lower time bound — and only when that hour is the current UTC
// wall-clock hour — and drops files whose filename-embedded flush timestamp
// proves they cannot contain rows in range.
//
// Correctness model (the filename timestamp is the file's CREATION/flush
// time, not a data time — see issue #187 for the compaction analogue):
//   - Rows in a file always carry timestamps <= the file's flush time, except
//     rows a client stamped in the future. Therefore only Start-side pruning
//     is ever performed: a file flushed before (Start - margin) cannot hold
//     rows >= Start unless those rows were future-stamped beyond the margin.
//   - No End-side pruning: a file flushed after End can hold in-range rows.
//   - Backfill is safe by construction: old data arriving now lands in a new
//     file, whose flush time always passes the predicate; the WHERE clause
//     filters the rows.
//   - Future-stamped data whose data-hour is AHEAD of its flush hour lands in
//     a directory ahead of its flush time, so in the boundary directory for
//     hour H it appears as a file whose parsed time is BEFORE H — impossible
//     for normal ingest. Such anomalous files are always kept. The residual
//     contract: rows stamped ahead of the server clock by more than the
//     margin but still WITHIN the same hour are indistinguishable from
//     normal ingest and may be invisible until their hour closes (the hour
//     rollover self-heals; plain partition pruning has no such window).
//     Size the margin to the worst writer clock skew you expect.
//
// Restricting expansion to the current UTC hour has two purposes: it is the
// only hour that accumulates uncompacted per-flush files (older hours fold at
// :05), and hourly compaction never touches it (min age is clamped to >= 1h),
// so an expanded explicit list can never race a compaction delete.
//
// Expanded results are volatile: they must never be served from a cache,
// because a cached list hides every file flushed since it was built. The
// pruner skips its own partitionCache for them and marks the query's context
// so the SQL transform cache skips them too.

// maxExpandedFiles caps the explicit list. The benefit of expansion is
// proportional to what is PRUNED — when nearly everything survives, handing
// DuckDB a huge literal list costs SQL-size and parse time for no win, so
// past the cap the glob is kept unchanged.
const maxExpandedFiles = 1000

// VolatileResult carries the per-query "this transform must not be cached"
// flag through the context from the transform layer down to the pruner.
type VolatileResult struct {
	Volatile bool
}

type volatileCtxKey struct{}

// WithVolatileResult attaches a fresh volatility flag to the context and
// returns it. The transform layer checks the flag after conversion and skips
// its cache when set.
func WithVolatileResult(ctx context.Context) (context.Context, *VolatileResult) {
	vr := &VolatileResult{}
	return context.WithValue(ctx, volatileCtxKey{}, vr), vr
}

// markVolatile flips the context's volatility flag, if one is attached.
func markVolatile(ctx context.Context) {
	if vr, ok := ctx.Value(volatileCtxKey{}).(*VolatileResult); ok {
		vr.Volatile = true
	}
}

// SetFileTimePruning enables or disables file-level time pruning.
// margin widens the keep-window below the query's lower bound to absorb
// writer clock skew (rows stamped slightly ahead of the server clock).
// Startup-only: the fields are read unlocked by concurrent queries, so this
// must not be called while the handler is serving.
func (p *PartitionPruner) SetFileTimePruning(enabled bool, margin time.Duration) {
	if margin < 0 {
		// A negative margin would move the cutoff ABOVE the query's lower
		// bound and prune files that certainly contain in-range rows.
		p.logger.Warn().
			Dur("configured_margin", margin).
			Msg("file_time_pruning margin is negative; clamping to 0")
		margin = 0
	}
	p.fileTimePruning = enabled
	p.fileTimeMargin = margin
	if enabled {
		p.logger.Info().
			Dur("margin", margin).
			Msg("File-level time pruning enabled (current-UTC-hour boundary expansion)")
	}
}

func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

// parseFileTime extracts the flush timestamp from an ingest parquet filename:
// {measurement}_{YYYYMMDD}_{HHMMSS}_{nanos}.parquet. Parsing is deliberately
// strict — exactly 8-digit date and 6-digit time validated by time.Parse, all
// -digit final token, and a measurement token present — so no other filename
// shape (hourly/daily compacted outputs, hand-placed files) can mis-parse to
// a wrong time; anything else fails and the caller keeps the file (fail open).
func parseFileTime(name string) (time.Time, bool) {
	base := strings.TrimSuffix(filepath.Base(name), ".parquet")
	toks := strings.Split(base, "_")
	if len(toks) < 4 {
		return time.Time{}, false
	}
	d, hms, nanos := toks[len(toks)-3], toks[len(toks)-2], toks[len(toks)-1]
	if len(d) != 8 || len(hms) != 6 || !allDigits(d) || !allDigits(hms) || !allDigits(nanos) {
		return time.Time{}, false
	}
	ts, err := time.Parse("20060102150405", d+hms)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// applyFileTimePruning expands the boundary-hour glob into an explicit file
// list with provably-out-of-range files removed. Returns the (possibly
// modified) path list and whether it was modified; when modified, the result
// is volatile (never cache it) and the context is marked accordingly.
// Remote (s3://, azure://) paths are never touched.
func (p *PartitionPruner) applyFileTimePruning(ctx context.Context, paths []string, timeRange *TimeRange) ([]string, bool) {
	if !p.fileTimePruning || timeRange == nil || len(paths) == 0 {
		return paths, false
	}
	boundary := timeRange.Start.UTC().Truncate(time.Hour)
	if !boundary.Equal(p.nowFn().UTC().Truncate(time.Hour)) {
		return paths, false
	}

	// The boundary-hour glob ends in .../YYYY/MM/DD/HH/*.parquet, built by
	// GeneratePartitionPaths with these exact Format calls.
	suffix := string(filepath.Separator) + filepath.Join(
		boundary.Format("2006"), boundary.Format("01"), boundary.Format("02"),
		boundary.Format("15"), "*.parquet")

	idx := -1
	for i, pth := range paths {
		if strings.HasPrefix(pth, "s3://") || strings.HasPrefix(pth, "azure://") {
			continue
		}
		if strings.HasSuffix(pth, suffix) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return paths, false
	}

	// Fresh glob, never the TTL globCache: a cached expansion would hide
	// every file flushed since it was built — the exact staleness this
	// feature must not introduce.
	files, err := filepath.Glob(paths[idx])
	if err != nil || len(files) == 0 {
		return paths, false
	}

	cutoff := timeRange.Start.UTC().Add(-p.fileTimeMargin)
	kept := make([]string, 0, len(files))
	for _, f := range files {
		ft, ok := parseFileTime(f)
		// Keep when: name unparseable (fail open); anomalous placement
		// (parsed time before the directory's own hour — only
		// future-stamped data creates that, and it may hold in-range
		// rows); or flush time within the margin-padded range.
		if !ok || ft.Before(boundary) || !ft.Before(cutoff) {
			kept = append(kept, f)
		}
	}

	if len(kept) == len(files) || len(kept) > maxExpandedFiles {
		return paths, false
	}
	if len(kept) == 0 && len(paths) == 1 {
		// Fail safe: never empty the whole path set.
		return paths, false
	}

	out := make([]string, 0, len(paths)-1+len(kept))
	out = append(out, paths[:idx]...)
	out = append(out, kept...)
	out = append(out, paths[idx+1:]...)
	markVolatile(ctx)
	p.logger.Debug().
		Int("dir_files", len(files)).
		Int("kept", len(kept)).
		Time("cutoff", cutoff).
		Msg("File-level time pruning expanded the live-hour glob")
	return out, true
}
