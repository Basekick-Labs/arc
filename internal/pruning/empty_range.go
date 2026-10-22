package pruning

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Empty-range proof (#928).
//
// When a query's time range covers no partition directory, OptimizeTablePath
// falls back to the whole measurement glob and DuckDB scans every file to
// return zero rows. Since #914 every measurement with a complete field schema
// anchor can answer an empty range from the anchor alone. That is only safe
// when emptiness is PROVEN, and the proof has four parts, each guarding a
// way the plain pruning verdict is allowed to be wrong because the glob
// fallback covers for it:
//
//   - exactness: the extracted range must be the query's whole time
//     predicate. The scanner (time_bounds.go) reads bounds only from bare
//     top-level AND terms, but it ignores other terms that mention time
//     (date_trunc(…, time) = …, NOT (…)); those still constrain the rows,
//     and the proof must not run without knowing how. Only a query whose
//     every time-mentioning term is a bare bound qualifies;
//   - freshness: an absent-directory verdict from the listing cache can be
//     stale for GlobCacheTTL; the proof re-checks with a short freshness
//     window and fails open on any listing error;
//   - layout: the generated paths assume db/meas/YYYY/MM/DD/HH. A hub query
//     on a spoke namespace (FROM "spoke".db) has paths one level shallow and
//     would look empty forever; the measurement directory must have year
//     directories as its children;
//   - bound: at most emptyProofMaxSpan of range, so the fresh listings stay a
//     handful of calls.

// emptyProofMaxSpan bounds the range a proof will cover: 7 days is 7
// day-parent listings plus 8 day-level file checks on an object store.
const emptyProofMaxSpan = 7 * 24 * time.Hour

// emptyProofFreshness is how old a listing may be and still count as fresh
// for the proof: dashboard panels and range chunks issued together share
// one listing, and a flush that landed more than two seconds before the
// query is seen.
const emptyProofFreshness = 2 * time.Second

// yearDirPattern is what a standard measurement directory's children look
// like.
var yearDirPattern = regexp.MustCompile(`^\d{4}$`)

// IsExactTimeRange reports whether the range ExtractTimeRange returns for
// sqlStr is the query's real time predicate: a single-table query whose
// WHERE clause is a conjunction containing bare bounds on the column named
// time, with nothing after each literal. Conservative: any doubt is false.
func IsExactTimeRange(sqlStr string) bool {
	hasStart, hasEnd, ok := bareTimeBounds(sqlStr)
	return ok && hasStart && hasEnd
}

// IsStartOnlyTimeRange reports whether sqlStr has the shape IsExactTimeRange
// requires with a start bound and no end: the extracted start is the query's
// real predicate for its only table, so a window with no rows is truly
// empty (open_end.go).
func IsStartOnlyTimeRange(sqlStr string) bool {
	hasStart, hasEnd, ok := bareTimeBounds(sqlStr)
	return ok && hasStart && !hasEnd
}

// bareTimeBounds is the shape check behind IsExactTimeRange and
// IsStartOnlyTimeRange: ok when sqlStr is a single-table query whose WHERE
// clause is a conjunction of bare, parseable bounds on the column named time,
// and which kinds of bound it has.
func bareTimeBounds(sqlStr string) (hasStart, hasEnd, ok bool) {
	b, ok := scanTimeBounds(sqlStr)
	if !ok || !b.exact || !b.proofSafe {
		return false, false, false
	}
	return b.start != nil, b.end != nil, true
}

// layoutIsStandard reports whether db/meas exists under basePath with only
// year directories as children. Cached in globCache for GlobCacheTTL.
func (p *PartitionPruner) layoutIsStandard(ctx context.Context, basePath, database, measurement string) bool {
	cacheKey := "layout:" + basePath + "/" + database + "/" + measurement
	if cached, ok := p.globCache.get(cacheKey); ok {
		return len(cached) == 1 && cached[0] == "standard"
	}
	var children []string
	isRemote := strings.HasPrefix(basePath, "s3://") || strings.HasPrefix(basePath, "azure://")
	if isRemote {
		lister, ok := p.storage.(storage.DirectoryLister)
		if p.storage == nil || !ok {
			return false
		}
		prefix, ok := p.extractStoragePrefix(basePath+"/"+database+"/"+measurement+"/", basePath)
		if !ok {
			return false
		}
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		dirs, err := lister.ListDirectories(listCtx, prefix)
		cancel()
		if err != nil {
			return false // unknown, not cached: fail open on the next call too
		}
		for _, d := range dirs {
			d = strings.TrimSuffix(d, "/")
			if i := strings.LastIndex(d, "/"); i >= 0 {
				d = d[i+1:]
			}
			children = append(children, d)
		}
	} else {
		entries, err := os.ReadDir(filepath.Join(basePath, database, measurement))
		if err != nil {
			p.globCache.set(cacheKey, []string{"missing"})
			return false
		}
		for _, e := range entries {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			children = append(children, e.Name())
		}
	}
	standard := len(children) > 0
	for _, c := range children {
		if c == "" || strings.HasPrefix(c, ".") {
			continue
		}
		if !yearDirPattern.MatchString(c) {
			standard = false
			break
		}
	}
	if standard {
		p.globCache.set(cacheKey, []string{"standard"})
	} else {
		p.globCache.set(cacheKey, []string{"other"})
	}
	return standard
}

// OptimizeTablePathVerdict is OptimizeTablePath with a third result:
// provenEmpty is true when the proof described at the top of this file
// established that no file of the measurement can hold a row in the query's
// range. A proven-empty verdict is never cached and marks the context
// volatile, so the SQL transform cache does not pin an empty answer while
// data starts arriving. The returned path is still the fallback glob, for a
// caller that cannot use the verdict.
func (p *PartitionPruner) OptimizeTablePathVerdict(ctx context.Context, originalPath, sqlStr string) (interface{}, bool, bool) {
	return p.optimize(ctx, originalPath, sqlStr, true)
}

// proveEmpty runs the parts of the proof that follow an empty existence
// pass. paths are the generated partition globs; hits is how many of the
// first pass's answers came from the cache.
func (p *PartitionPruner) proveEmpty(ctx context.Context, basePath, database, measurement, sqlStr string, timeRange *TimeRange, paths []string, hits int, firstVerified bool) bool {
	if timeRange.StartAssumed || timeRange.EndAssumed || timeRange.End.Sub(timeRange.Start) > emptyProofMaxSpan {
		return false
	}
	if !IsExactTimeRange(sqlStr) {
		return false
	}
	if !p.layoutIsStandard(ctx, basePath, database, measurement) {
		return false
	}
	verified := firstVerified
	if hits > 0 {
		// The first pass leaned on the cache; look again, accepting only
		// listings younger than the freshness window.
		var fresh []string
		fresh, verified, _ = p.filterExistingPathsOpts(ctx, paths, basePath, emptyProofFreshness)
		if len(fresh) > 0 {
			return false
		}
	}
	if !verified {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	markVolatile(ctx)
	p.logger.Info().
		Str("database", database).
		Str("measurement", measurement).
		Time("start", timeRange.Start).
		Time("end", timeRange.End).
		Msg("Time range proven empty; answering from the field schema anchor")
	return true
}
