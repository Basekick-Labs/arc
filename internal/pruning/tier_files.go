package pruning

// Whether a tier holds any file at all for a measurement. A tier that reaches
// DuckDB unpruned (no time range, or the cold end-only fallback) goes as its
// full glob, and a glob that matches nothing makes DuckDB report "no files"
// for the whole read — cold data included. The query layer asks this before
// keeping such a tier.

import (
	"context"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// tierWalkBudget caps the listings one TierHasFiles walk may issue; past it
// the walk hands over to one recursive listing.
const tierWalkBudget = 64

// tierListTimeout bounds each listing the walk issues, as PruneTierPaths
// bounds its own, so a hung object store fails the check open instead of
// holding the query.
const tierListTimeout = 5 * time.Second

// partitionLeafDepth is the day level of {db}/{measurement}/Y/M/D[/H]: files
// live there (daily outputs) and in the hour directories below it.
const partitionLeafDepth = 3

// TierHasFiles reports whether the tier's backend holds at least one parquet
// file for the measurement, and whether that could be established.
// Partition directories alone do not count: compaction and migration leave
// empty year/month/day/hour directories behind — on a replicating reader
// nothing ever prunes them — and an empty glob is exactly what turns a read
// into "no files". The partition tree is walked newest partition first, one
// listing per level (a readdir locally, one delimited LIST on an object
// store), stopping at the first file: a live measurement resolves in four
// listings. When the walk finds nothing, or runs out of budget on a forest
// of empty directories, it falls back to one recursive listing of the
// measurement — cheap precisely because the measurement is then (nearly)
// empty, and immune to any listing that lags a just-flushed file. Only a
// listing error leaves the question open, and the caller keeps the tier.
func TierHasFiles(ctx context.Context, backend storage.Backend, database, measurement string) (has, verified bool) {
	prefix := database + "/" + measurement + "/"
	if lister, ok := backend.(storage.DirectoryLister); ok {
		budget := tierWalkBudget
		if has, verified := walkForParquet(ctx, backend, lister, prefix, 0, &budget); has {
			return true, true
		} else if !verified && budget > 0 {
			// A listing failed; the recursive listing below would too.
			return false, false
		}
	}
	return listHasParquet(ctx, backend, prefix)
}

// listHasParquet answers with one recursive listing under prefix.
func listHasParquet(ctx context.Context, backend storage.Backend, prefix string) (has, verified bool) {
	lctx, cancel := context.WithTimeout(ctx, tierListTimeout)
	defer cancel()
	objects, err := backend.List(lctx, prefix)
	if err != nil {
		return false, false
	}
	for _, p := range objects {
		if strings.HasSuffix(p, ".parquet") {
			return true, true
		}
	}
	return false, true
}

// walkForParquet descends the partition tree one listing per level, newest
// partition first, and stops at the first parquet file. Directory names
// come back bare from the local backend and as prefixes from object stores,
// sorted ascending by both; each reduces to its last segment. Budget
// exhaustion returns unverified with the budget at zero, which the caller
// tells apart from a listing error.
func walkForParquet(ctx context.Context, backend storage.Backend, lister storage.DirectoryLister, prefix string, depth int, budget *int) (has, verified bool) {
	if *budget <= 0 || ctx.Err() != nil {
		return false, false
	}
	*budget--
	if depth == partitionLeafDepth {
		return listHasParquet(ctx, backend, prefix)
	}
	lctx, cancel := context.WithTimeout(ctx, tierListTimeout)
	dirs, err := lister.ListDirectories(lctx, prefix)
	cancel()
	if err != nil {
		// Backends answer a missing prefix with an empty listing, not an
		// error; an error is the store failing, and the tier stays in.
		return false, false
	}
	verified = true
	for i := len(dirs) - 1; i >= 0; i-- {
		name := strings.TrimSuffix(dirs[i], "/")
		if j := strings.LastIndex(name, "/"); j >= 0 {
			name = name[j+1:]
		}
		if name == "" || name == "." {
			continue
		}
		subHas, subVerified := walkForParquet(ctx, backend, lister, prefix+name+"/", depth+1, budget)
		if subHas {
			return true, true
		}
		if !subVerified {
			verified = false
			if *budget <= 0 {
				return false, false
			}
		}
	}
	return false, verified
}
