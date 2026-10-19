package pruning

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// A start-only time predicate (`time > NOW() - INTERVAL '1 hour'`) asks for
// every row from its start on. The pruner gives it the end now+24h
// (EndAssumed) and generates the partitions up to there; two things went
// wrong with that alone:
//
//   - rows stamped later than now+24h (forecasts, devices whose clocks run
//     ahead) live in partitions that were never generated, so a pruned
//     query silently left them out;
//   - when no generated partition existed (a source that stopped sending),
//     the query fell back to the whole measurement's glob: every refresh of
//     a dashboard on that source read every file.
//
// The partitions that do exist are found by listing the measurement's
// directories newest first: every partition at or after the assumed end is
// added to the generated list (sparse: one far-future row costs one path),
// and the newest partition is kept for an empty window. The assumed end, the
// live-hour rules and the caches are unchanged.

// maxBeyondPartitions bounds the partitions added past the assumed end; with
// more, the query reads the unpruned glob (correct, slow) rather than drop
// rows.
const maxBeyondPartitions = 10000

var errTooManyBeyond = errors.New("too many partitions past the assumed end")

// partKey is one partition of a measurement: an hour directory, or a day
// directory's own files (daily compaction output) when hour is "".
type partKey struct {
	year, month, day, hour string
	start                  time.Time
}

func (k partKey) end() time.Time {
	if k.hour == "" {
		return k.start.Add(24 * time.Hour)
	}
	return k.start.Add(time.Hour)
}

// dirLister lists the names of the subdirectories of rel (a slash-separated
// path below the storage root, e.g. "db/cpu/2026"). A missing directory is an
// empty list, not an error.
type dirLister func(ctx context.Context, rel string) ([]string, error)

// numericNames keeps the names that are width-digit numbers within [lo, hi],
// newest (largest) first. Anything else (a non-standard layout) is ignored:
// it cannot be a partition the generator would have produced.
func numericNames(names []string, width, lo, hi int) []string {
	var out []string
	for _, n := range names {
		n = strings.TrimSuffix(n, "/")
		if i := strings.LastIndex(n, "/"); i >= 0 {
			n = n[i+1:]
		}
		if len(n) != width {
			continue
		}
		v, err := strconv.Atoi(n)
		if err != nil || v < lo || v > hi {
			continue
		}
		out = append(out, n)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(out)))
	return out
}

// discoverOpenEnd lists the measurement newest first and returns every
// partition ending after denseEnd (the assumed end), and the newest partition
// of the measurement. It stops at the first partition entirely before
// denseEnd, so the listing is a few directories in the common case.
func discoverOpenEnd(ctx context.Context, list dirLister, database, measurement string, denseEnd time.Time) (beyond []partKey, newest *partKey, err error) {
	base := database + "/" + measurement
	years, err := list(ctx, base)
	if err != nil {
		return nil, nil, err
	}
	add := func(k partKey) (stop bool, err error) {
		if newest == nil {
			kk := k
			newest = &kk
		}
		if !k.end().After(denseEnd) {
			return true, nil
		}
		if len(beyond) >= maxBeyondPartitions {
			return true, errTooManyBeyond
		}
		beyond = append(beyond, k)
		return false, nil
	}
	for _, y := range numericNames(years, 4, 1970, 9999) {
		months, err := list(ctx, base+"/"+y)
		if err != nil {
			return nil, nil, err
		}
		for _, m := range numericNames(months, 2, 1, 12) {
			days, err := list(ctx, base+"/"+y+"/"+m)
			if err != nil {
				return nil, nil, err
			}
			for _, d := range numericNames(days, 2, 1, 31) {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				dayStart, perr := time.ParseInLocation("2006/01/02", y+"/"+m+"/"+d, time.UTC)
				if perr != nil {
					continue
				}
				hours, err := list(ctx, base+"/"+y+"/"+m+"/"+d)
				if err != nil {
					return nil, nil, err
				}
				hs := numericNames(hours, 2, 0, 23)
				for _, h := range hs {
					hv, _ := strconv.Atoi(h)
					stop, err := add(partKey{y, m, d, h, dayStart.Add(time.Duration(hv) * time.Hour)})
					if err != nil {
						return nil, nil, err
					}
					if stop {
						return beyond, newest, nil
					}
				}
				// The day's own files (daily compaction output), whether or
				// not hour directories remain; the existence filter checks
				// it like any generated day path.
				stop, err := add(partKey{year: y, month: m, day: d, start: dayStart})
				if err != nil {
					return nil, nil, err
				}
				if stop && len(hs) == 0 {
					return beyond, newest, nil
				}
			}
		}
	}
	return beyond, newest, nil
}

// partGlob is a partition's glob in the format GeneratePartitionPaths uses.
func partGlob(basePath, database, measurement string, k partKey) string {
	if strings.HasPrefix(basePath, "s3://") || strings.HasPrefix(basePath, "azure://") {
		p := basePath + "/" + database + "/" + measurement + "/" + k.year + "/" + k.month + "/" + k.day
		if k.hour != "" {
			p += "/" + k.hour
		}
		return p + "/*.parquet"
	}
	parts := []string{basePath, database, measurement, k.year, k.month, k.day}
	if k.hour != "" {
		parts = append(parts, k.hour)
	}
	return filepath.Join(append(parts, "*.parquet")...)
}

// localDirLister lists directories below basePath.
func localDirLister(basePath string) dirLister {
	return func(_ context.Context, rel string) ([]string, error) {
		entries, err := os.ReadDir(filepath.Join(basePath, filepath.FromSlash(rel)))
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		var names []string
		for _, e := range entries {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
		return names, nil
	}
}

// remoteDirLister lists directories of the pruner's object store below
// basePath (s3:// or azure://), through the same cached parent listings as
// the existence filter.
func (p *PartitionPruner) remoteDirLister(basePath string) dirLister {
	return func(ctx context.Context, rel string) ([]string, error) {
		lister, ok := p.storage.(storage.DirectoryLister)
		if !ok {
			return nil, errors.New("storage cannot list directories")
		}
		parentDir := basePath + "/" + rel + "/"
		cacheKey := "remote:parent:" + parentDir
		if cached, ok := p.globCache.get(cacheKey); ok {
			return cached, nil
		}
		prefix, ok := p.extractStoragePrefix(parentDir, basePath)
		if !ok {
			return nil, errors.New("cannot resolve the storage prefix")
		}
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		subdirs, err := lister.ListDirectories(listCtx, prefix)
		cancel()
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(subdirs))
		for _, s := range subdirs {
			s = strings.TrimSuffix(s, "/")
			if i := strings.LastIndex(s, "/"); i >= 0 {
				s = s[i+1:]
			}
			names = append(names, s)
		}
		p.globCache.set(cacheKey, names)
		return names, nil
	}
}

// tierDirLister lists directories of a tier's backend, keys relative to the
// tier root, through the tier pruner's cached listings.
func (p *PartitionPruner) tierDirLister(backend storage.Backend, rootURL string) dirLister {
	return func(ctx context.Context, rel string) ([]string, error) {
		lister, ok := backend.(storage.DirectoryLister)
		if backend == nil || !ok {
			return nil, errors.New("tier backend cannot list directories")
		}
		cacheKey := "tier:dirs:" + rootURL + rel
		if cached, ok := p.globCache.get(cacheKey); ok {
			return cached, nil
		}
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		subdirs, err := lister.ListDirectories(listCtx, rel+"/")
		cancel()
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(subdirs))
		for _, s := range subdirs {
			s = strings.TrimSuffix(s, "/")
			if i := strings.LastIndex(s, "/"); i >= 0 {
				s = s[i+1:]
			}
			names = append(names, s)
		}
		p.globCache.set(cacheKey, names)
		return names, nil
	}
}

// appendBeyond adds the beyond partitions' globs to paths, skipping any
// already there.
func appendBeyond(paths []string, beyond []partKey, basePath, database, measurement string) []string {
	have := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		have[p] = struct{}{}
	}
	for _, k := range beyond {
		g := partGlob(basePath, database, measurement, k)
		if _, ok := have[g]; !ok {
			have[g] = struct{}{}
			paths = append(paths, g)
		}
	}
	return paths
}

// schemaAnchorKey marks a context whose read will carry the measurement's
// complete schema anchor in front of the partitions it reads: only then may
// an empty window be answered from one partition (the anchor supplies every
// column; one partition alone could lack a column older files have).
type schemaAnchorKey struct{}

// WithCompleteSchemaAnchor marks ctx as described for schemaAnchorKey.
func WithCompleteSchemaAnchor(ctx context.Context) context.Context {
	return context.WithValue(ctx, schemaAnchorKey{}, true)
}

func hasCompleteSchemaAnchor(ctx context.Context) bool {
	v, _ := ctx.Value(schemaAnchorKey{}).(bool)
	return v
}
