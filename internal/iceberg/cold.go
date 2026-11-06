package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Files migrated to an S3 cold tier stay in the Iceberg table (#939 B2).
//
// Tiering keeps a file's storage key and changes only the backend holding it (copy to cold, flip
// the tier record, delete the hot copy). The source therefore lists both tiers and registers each
// key once, under the URI of the tier that holds it:
//
//   - on one tier only: that tier;
//   - on both (a migration between its copy and its hot delete, or a hot-only database, whose
//     cold copy sits next to the hot one it reads): hot, unless Arc's tier record says the key
//     is cold. That is exactly the file Arc's own reads use.
//
// Listing all of a measurement's cold history every pass would cost a LIST per thousand cold
// files per measurement per pass, so the cold listing is cached and listed again when it is
// invalidated (a tiering cycle, cold retention, a cold DELETE, a database drop), when it is older
// than the rescan interval, or — before any file is dropped — for a key that left the hot tier:
// absence from hot is never evidence of absence while a cold tier is configured, so such a key is
// looked for on cold (one HEAD) before the table loses it. A failed cold read fails the pass.

// coldTier is the source's cold tier.
type coldTier struct {
	backend  storage.Backend
	lister   storage.ObjectLister
	resolver *PathResolver
	// isCold reports whether Arc's tier record names key as migrated to cold. Asked only for a
	// key present on both tiers.
	isCold      func(ctx context.Context, key string) (bool, error)
	rescanEvery time.Duration

	mu       sync.Mutex
	listings map[string]*coldListing    // measurement prefix -> cached listing
	prevHot  map[string]map[string]bool // measurement prefix -> hot keys of the last listing
}

type coldListing struct {
	sizes    map[string]int64
	listedAt time.Time
}

// SetColdTier makes the source list an S3 cold tier next to the primary (#939 B2). isCold is the
// tier record lookup (tiering's tier_files), rescanEvery how long a cached cold listing is trusted
// without an invalidation (InvalidateCold).
func (s *StorageWalkSource) SetColdTier(backend storage.Backend, isCold func(ctx context.Context, key string) (bool, error), rescanEvery time.Duration) error {
	lister, ok := backend.(storage.ObjectLister)
	if !ok {
		return fmt.Errorf("cold tier backend %s cannot list objects with their sizes", backend.Type())
	}
	s.cold = &coldTier{
		backend: backend, lister: lister, resolver: NewPathResolver(backend),
		isCold: isCold, rescanEvery: rescanEvery,
		listings: map[string]*coldListing{}, prevHot: map[string]map[string]bool{},
	}
	return nil
}

// InvalidateCold drops cached cold listings so the next pass lists the cold tier again: database
// "" drops all of them. Wired to the tiering cycle, cold retention, a cold DELETE and a database
// drop — every Arc path that changes what the cold tier holds.
func (s *StorageWalkSource) InvalidateCold(database string) {
	c := s.cold
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if database == "" {
		c.listings = map[string]*coldListing{}
		return
	}
	for prefix := range c.listings {
		if strings.HasPrefix(prefix, database+"/") {
			delete(c.listings, prefix)
		}
	}
}

// coldSizes returns the cold tier's data files under prefix (key -> size): the cached listing,
// listed again when missing or stale, and completed with a HEAD on cold for every key that was hot
// in the previous listing and is not now.
func (s *StorageWalkSource) coldSizes(ctx context.Context, prefix string, hotNow map[string]bool) (map[string]int64, error) {
	c := s.cold
	c.mu.Lock()
	l := c.listings[prefix]
	prev := c.prevHot[prefix]
	c.mu.Unlock()
	if l == nil || time.Since(l.listedAt) >= c.rescanEvery {
		objs, err := c.lister.ListObjects(ctx, prefix)
		if err != nil {
			return nil, fmt.Errorf("list cold tier under %s: %w", prefix, err)
		}
		l = &coldListing{sizes: make(map[string]int64, len(objs)), listedAt: time.Now()}
		for _, o := range objs {
			if isDataFile(o.Path) {
				l.sizes[filepath.ToSlash(o.Path)] = o.Size
			}
		}
	}
	for key := range prev {
		if hotNow[key] {
			continue
		}
		if _, ok := l.sizes[key]; ok {
			continue
		}
		// Left the hot tier since the last listing and not in the cached cold listing: a
		// migration that moved it after that listing, or a removal. Only cold can tell.
		size, err := c.backend.StatFile(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("look for %s on the cold tier: %w", key, err)
		}
		if size >= 0 {
			l.sizes[key] = size
		}
	}
	c.mu.Lock()
	c.listings[prefix] = l
	c.prevHot[prefix] = hotNow
	c.mu.Unlock()
	out := make(map[string]int64, len(l.sizes))
	for k, v := range l.sizes {
		out[k] = v
	}
	return out, nil
}

// mergeCold adds the cold tier's files to a measurement's hot file set, each key once (see the
// package comment above for which tier a key on both is registered under). skip is the set the
// hot listing left out (pending compaction outputs, files Arc's reads leave out), applied to cold
// keys too.
func (s *StorageWalkSource) mergeCold(ctx context.Context, prefix string, files []FileRef, local []string, skip func(key string) bool) ([]FileRef, []string, error) {
	c := s.cold
	hotNow := make(map[string]bool, len(files))
	byKey := make(map[string]int, len(files))
	for i, f := range files {
		hotNow[f.Key] = true
		byKey[f.Key] = i
	}
	cold, err := s.coldSizes(ctx, prefix, hotNow)
	if err != nil {
		return nil, nil, err
	}
	dropLocal := map[string]bool{}
	for key, size := range cold {
		if skip(key) {
			continue
		}
		uri, err := c.resolver.Resolve(key)
		if err != nil {
			return nil, nil, fmt.Errorf("cold-tier file %q has no usable storage path: %w", key, err)
		}
		ref := FileRef{PhysicalPath: uri, SizeBytes: size, Key: key, Cold: true}
		i, onHot := byKey[key]
		if !onHot {
			files = append(files, ref)
			continue
		}
		migrated, err := c.isCold(ctx, key)
		if err != nil {
			return nil, nil, fmt.Errorf("tier of %s: %w", key, err)
		}
		if migrated {
			if files[i].LocalPath != "" {
				dropLocal[files[i].LocalPath] = true
			}
			files[i] = ref
		}
	}
	if len(dropLocal) > 0 {
		kept := local[:0]
		for _, lp := range local {
			if !dropLocal[lp] {
				kept = append(kept, lp)
			}
		}
		local = kept
	}
	return files, local, nil
}

// coldMeasurements lists the measurements present on the cold tier, so a measurement whose every
// file has migrated (gone from an object-store primary entirely) is still reconciled.
func (s *StorageWalkSource) coldMeasurements(ctx context.Context) ([]Measurement, error) {
	if s.cold == nil {
		return nil, nil
	}
	dl, ok := s.cold.backend.(dirLister)
	if !ok {
		return nil, fmt.Errorf("cold tier backend does not support directory listing")
	}
	return s.measurementsOn(ctx, dl)
}

// KeyOf maps a URI this source produced (hot or cold) back to its storage key.
func (s *StorageWalkSource) KeyOf(uri string) (string, bool) {
	roots := []string{DefaultWarehouse(s.backend)}
	if s.cold != nil {
		roots = append(roots, DefaultWarehouse(s.cold.backend))
	}
	for _, root := range roots {
		if key, ok := strings.CutPrefix(uri, strings.TrimSuffix(root, "/")+"/"); ok && key != "" {
			return key, true
		}
	}
	return "", false
}

// coldSchema reads a cold file's footer from the cold backend.
func (s *StorageWalkSource) coldSchema(ctx context.Context, f FileRef) (ArcSchema, error) {
	return SchemaFromBackend(ctx, s.cold.backend, f.Key, f.SizeBytes)
}
