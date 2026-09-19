package pruning

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/sql"
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
//   - exactness: the extracted range must be the query's real range. The
//     extractor is a regex over the WHERE clause and is imprecise on JOINs
//     (one side's predicate is applied to both tables), OR, NOT, subqueries
//     and literal arithmetic. For normal pruning that is harmless: a wrong
//     narrow range still finds files, and an empty one falls back. An empty
//     answer built on a wrong range is a wrong answer, so only the simple
//     single-table shape with bare bounds qualifies;
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

var (
	// yearDirPattern is what a standard measurement directory's children
	// look like.
	yearDirPattern = regexp.MustCompile(`^\d{4}$`)
	// exactBoundPattern finds every time bound the extractor would use and
	// captures what follows it, so arithmetic or casts after the literal
	// can be rejected.
	// Only exclusive end bounds qualify: GeneratePartitionPaths treats End
	// as exclusive, so a row stamped exactly at an inclusive end (the next
	// hour directory) would sit outside every generated path. BETWEEN is
	// inclusive on both ends and is rejected for the same reason.
	exactBoundPattern = regexp.MustCompile(`(?i)(^|[\s(.])time\s*(>=|>|<)\s*('[^']*'|NOW\s*\(\s*\)\s*([-+])\s*INTERVAL\s*'?(\d+)\s*([a-z]+)'?)`)
	// maskPlaceholder is what sql.MaskStringLiterals leaves in place of a
	// literal or quoted identifier. It contains underscores, which are word
	// characters, so a keyword glued to a closing quote (`'a'OR`) would hide
	// from a \b boundary; the placeholders are spaced out before the
	// keyword scan.
	maskPlaceholder = regexp.MustCompile(`__(?:STR|IDENT)_\d+__`)
	// anyTimeComparison is every comparison the extractor's own patterns
	// would read as a time bound, including ones on columns that merely END
	// in "time" (last_time >= ...), which the extractor misreads.
	anyTimeComparison = regexp.MustCompile(`(?i)time\s*(>=|>|<=|<|=|<>|!=)`)
	// forbiddenClauseWord is any construct under which a matched bound is
	// not the query's whole time predicate. The left boundary admits a
	// digit (`v=1OR` is `v=1 OR` to the lexer) and excludes the underscore,
	// so neither a number nor a placeholder can hide a keyword; an
	// identifier such as v2or is then over-rejected, which is the safe side.
	forbiddenClauseWord = regexp.MustCompile(`(?i)(^|[^A-Za-z_])(or|not|in|exists|case|join|select)($|[^A-Za-z0-9_])`)
	// afterBoundPattern is what may follow a bare bound: closing parens,
	// then AND, a clause keyword, or the end.
	afterBoundPattern = regexp.MustCompile(`(?i)^\s*\)*\s*(and\b|group\b|order\b|limit\b|$)`)
	fromWordPattern   = regexp.MustCompile(`(?i)\bfrom\b`)
	shapeWordPattern  = regexp.MustCompile(`(?i)\b(join|with|union|except|intersect)\b`)
)

// IsExactTimeRange reports whether the range ExtractTimeRange returns for
// sqlStr is the query's real time predicate: a single-table query whose
// WHERE clause is a conjunction containing bare bounds on the column named
// time, with nothing after each literal. Conservative: any doubt is false.
func IsExactTimeRange(sqlStr string) bool {
	masked, masks := sql.MaskStringLiterals(sqlStr, sql.HasQuotes(sqlStr))
	lower := strings.ToLower(masked)
	if len(fromWordPattern.FindAllStringIndex(lower, -1)) != 1 || shapeWordPattern.MatchString(lower) {
		return false
	}
	if strings.Contains(lower, "(select") || strings.Contains(lower, "( select") {
		return false
	}
	m := whereClausePattern.FindStringSubmatch(masked)
	if len(m) < 2 {
		return false
	}
	maskedWhere := m[1]
	spaced := maskPlaceholder.ReplaceAllString(maskedWhere, " ~ ")
	if forbiddenClauseWord.MatchString(spaced) {
		return false
	}
	// A "timestamp" column bound would be read by the extractor as a time
	// bound; it is not the partition column.
	if regexp.MustCompile(`(?i)\btimestamp\s*(>=|>|<=|<|between)`).MatchString(maskedWhere) {
		return false
	}
	where := sql.UnmaskStringLiterals(maskedWhere, masks)
	bounds := exactBoundPattern.FindAllStringSubmatchIndex(where, -1)
	if len(bounds) == 0 {
		return false
	}
	// Both ends must be stated: an end-only predicate makes the extractor
	// assume a start, a start-only one makes it assume an end, and neither
	// assumption is a fact about the data. Each literal must also be one
	// the extractor parses, or it silently assumes that end as well.
	hasStart, hasEnd := false, false
	for _, b := range bounds {
		if !afterBoundPattern.MatchString(where[b[1]:]) {
			return false
		}
		if !extractorReadsBound(where, b) {
			return false
		}
		switch where[b[4]:b[5]] {
		case ">=", ">":
			hasStart = true
		default:
			hasEnd = true
		}
	}
	if !hasStart || !hasEnd {
		return false
	}
	// Every comparison the extractor could read as a time bound must be one
	// of the bare bounds found above: a column merely ending in "time", an
	// equality, or an expression means the extracted range is not the whole
	// predicate.
	if len(anyTimeComparison.FindAllStringIndex(where, -1)) != len(bounds) {
		return false
	}
	return true
}

// extractorReadsBound reports whether the literal of one exactBoundPattern
// match is one ExtractTimeRange parses: a quoted timestamp parseDateTime
// accepts, or an interval whose unit evaluateRelativeTime knows. The
// submatch indices are those of exactBoundPattern: 6/7 the literal, 8/9 the
// sign, 10/11 the amount and 12/13 the unit of an interval form.
func extractorReadsBound(where string, b []int) bool {
	literal := where[b[6]:b[7]]
	if strings.HasPrefix(literal, "'") {
		_, err := parseDateTime(strings.Trim(literal, "'"))
		return err == nil
	}
	if b[10] < 0 || b[12] < 0 {
		return false
	}
	_, err := evaluateRelativeTime(where[b[10]:b[11]], strings.ToLower(where[b[12]:b[13]]), where[b[8]:b[9]] == "+")
	return err == nil
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
