package pruning

// Per-tier partition pruning for tiered storage (#662).
//
// The single-tier OptimizeTablePath existence-filters against the pruner's one
// configured backend (p.storage). A tiered query spans TWO backends — the hot
// (usually local) primary and the cold archive — so its paths must be filtered
// against the tier's OWN backend, with listing keys relative to that backend's
// root. Filtering a cold s3://bucket/prefix/... path through the hot backend,
// or listing it with a bucket-relative key that the backend then re-prefixes,
// silently reports every cold partition as absent — which under a
// drop-empty-tiers policy would be data loss, not a performance bug.

import (
	"context"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// TierPruneOutcome classifies the result of PruneTierPaths for one tier.
type TierPruneOutcome int

const (
	// TierPruneFallback: the tier could not be pruned (pruning disabled, no
	// time range, unrecognized glob shape, the path cap tripped, or a listing
	// error left existence unverified with nothing surviving). The caller must
	// scan the tier's full glob.
	TierPruneFallback TierPruneOutcome = iota
	// TierPrunePruned: the returned paths are the tier's partitions for the
	// time range, existence-verified against the tier's backend.
	TierPrunePruned
	// TierPruneEmpty: every generated partition path was verified absent on
	// the tier's backend with no listing errors. The tier holds no data for
	// the time range and may be dropped from the query entirely.
	TierPruneEmpty
)

// PruneTierPaths prunes one storage tier's partition paths for a query time
// range, existence-filtering against the tier's own backend.
//
// tierGlob is the tier's full glob ({base}/{db}/{measurement}/**/*.parquet,
// where base may be local, s3://bucket/prefix, or azure://container).
// allowFileTime additionally applies file-level time pruning (#660) — hot tier
// only: the live hour that feature targets exists only on the hot tier, and
// the feature is documented for the local backend.
//
// Results are NOT cached at this layer (unlike OptimizeTablePath's
// partitionCache): the SQL transform cache already dedupes repeated queries,
// and the remote directory/file listings below are individually cached in
// globCache, so a re-prune costs no extra backend calls within the TTL.
func (p *PartitionPruner) PruneTierPaths(ctx context.Context, tierGlob, database, measurement string, timeRange *TimeRange, backend storage.Backend, allowFileTime bool) ([]string, TierPruneOutcome) {
	if !p.enabled || timeRange == nil {
		return nil, TierPruneFallback
	}
	suffix := database + "/" + measurement + "/**/*.parquet"
	if !strings.HasSuffix(tierGlob, suffix) {
		p.logger.Debug().Str("glob", tierGlob).Msg("Tier glob shape not recognized; not pruning")
		return nil, TierPruneFallback
	}
	// rootURL keeps its trailing separator so TrimPrefix yields clean
	// backend-relative keys; basePath drops it for path generation.
	rootURL := strings.TrimSuffix(tierGlob, suffix)
	basePath := strings.TrimSuffix(rootURL, "/")

	paths := p.GeneratePartitionPaths(ctx, basePath, database, measurement, timeRange)
	if len(paths) == 0 {
		// Range too wide for the path cap, or generation cancelled.
		return nil, TierPruneFallback
	}

	verified := true
	if strings.HasPrefix(tierGlob, "s3://") || strings.HasPrefix(tierGlob, "azure://") {
		paths, verified = p.filterTierRemotePaths(ctx, paths, backend, rootURL)
	} else {
		paths = p.filterExistingLocalPaths(paths)
	}

	if len(paths) == 0 {
		if verified {
			return nil, TierPruneEmpty
		}
		return nil, TierPruneFallback
	}

	if allowFileTime {
		paths, _ = p.applyFileTimePruning(ctx, paths, timeRange)
		if len(paths) == 0 {
			return nil, TierPruneFallback
		}
	}
	return paths, TierPrunePruned
}

// filterTierRemotePaths existence-filters remote partition paths against the
// given backend, using keys relative to rootURL (the tier glob's base,
// scheme+bucket+configured-prefix inclusive) so the backend's own key
// prefixing applies exactly once.
//
// Every listing failure fails OPEN — the path is kept and verified is
// returned false, so the caller can never mistake "could not check" for
// "verified absent". (The single-tier filter fails closed on day-level List
// errors; that is tolerable there because a dropped path still has the
// unpruned-glob fallback, which a dropped TIER would not.)
func (p *PartitionPruner) filterTierRemotePaths(ctx context.Context, paths []string, backend storage.Backend, rootURL string) ([]string, bool) {
	lister, canList := backend.(storage.DirectoryLister)
	if backend == nil || !canList {
		p.logger.Debug().Msg("Tier backend cannot list directories; keeping all paths unverified")
		return paths, false
	}

	verified := true
	// hourDirs groups hour-level paths by their backend-relative parent (day)
	// directory, listed once each.
	type hourEntry struct {
		path   string
		target string // hour segment, e.g. "14"
	}
	hourDirs := make(map[string][]hourEntry)
	var dayPaths []string // day-level compacted-file globs, checked individually

	existing := make([]string, 0, len(paths))
	for _, path := range paths {
		rel := strings.TrimPrefix(path, rootURL)
		if rel == path {
			// Path does not share the tier root — cannot derive a
			// backend-relative key; keep it unverified.
			p.logger.Debug().Str("path", path).Msg("Tier path outside tier root; keeping unverified")
			existing = append(existing, path)
			verified = false
			continue
		}
		dir := strings.TrimSuffix(rel, "/*.parquet")
		segs := strings.Split(dir, "/")
		switch len(segs) {
		case 6: // db/measurement/year/month/day/hour
			parent := strings.Join(segs[:5], "/")
			hourDirs[parent] = append(hourDirs[parent], hourEntry{path: path, target: segs[5]})
		case 5: // db/measurement/year/month/day (daily compacted files)
			dayPaths = append(dayPaths, path)
		default:
			existing = append(existing, path)
			verified = false
		}
	}

	// Hour-level: one ListDirectories per covered day.
	for parent, entries := range hourDirs {
		cacheKey := "tier:dirs:" + rootURL + parent
		var children []string
		if cached, ok := p.globCache.get(cacheKey); ok {
			children = cached
		} else {
			listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			subdirs, err := lister.ListDirectories(listCtx, parent+"/")
			cancel()
			if err != nil {
				p.logger.Debug().Err(err).Str("prefix", parent).Msg("Tier directory listing failed; keeping paths unverified")
				for _, e := range entries {
					existing = append(existing, e.path)
				}
				verified = false
				continue
			}
			children = make([]string, 0, len(subdirs))
			for _, subdir := range subdirs {
				subdir = strings.TrimSuffix(subdir, "/")
				if idx := strings.LastIndex(subdir, "/"); idx != -1 {
					subdir = subdir[idx+1:]
				}
				children = append(children, subdir)
			}
			p.globCache.set(cacheKey, children)
		}
		childSet := make(map[string]bool, len(children))
		for _, c := range children {
			childSet[c] = true
		}
		for _, e := range entries {
			if childSet[e.target] {
				existing = append(existing, e.path)
			}
		}
	}

	// Day-level: files must exist directly at the day directory (daily
	// compaction output), not only in hour subdirectories.
	for _, path := range dayPaths {
		rel := strings.TrimPrefix(path, rootURL)
		prefix := strings.TrimSuffix(rel, "*.parquet")
		cacheKey := "tier:dayfiles:" + rootURL + prefix
		var hasFiles bool
		if cached, ok := p.globCache.get(cacheKey); ok {
			hasFiles = len(cached) > 0
		} else {
			listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			files, err := backend.List(listCtx, prefix)
			cancel()
			if err != nil {
				p.logger.Debug().Err(err).Str("prefix", prefix).Msg("Tier day-level listing failed; keeping path unverified")
				existing = append(existing, path)
				verified = false
				continue
			}
			var directFiles []string
			for _, f := range files {
				remaining := strings.TrimPrefix(f, prefix)
				if remaining != "" && !strings.Contains(remaining, "/") && strings.HasSuffix(remaining, ".parquet") {
					directFiles = append(directFiles, remaining)
				}
			}
			p.globCache.set(cacheKey, directFiles)
			hasFiles = len(directFiles) > 0
		}
		if hasFiles {
			existing = append(existing, path)
		}
	}

	p.logger.Debug().
		Int("original_count", len(paths)).
		Int("existing_count", len(existing)).
		Bool("verified", verified).
		Msg("Filtered tier remote paths")
	return existing, verified
}
