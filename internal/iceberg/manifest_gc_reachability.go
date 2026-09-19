package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/basekick-labs/arc/internal/storage"
)

// collectReachableManifestMetadata builds a read-only protection set for a
// future metadata-only orphan sweep (#835). It deliberately does NOT delete
// anything. A missing or malformed reference invalidates the entire result;
// callers must never interpret a partial set as proof of unreachability.
//
// Every extant metadata JSON version (including directory-reader v<N> copies)
// is traversed, not merely the current catalog snapshot. The caller must also
// ensure its metadata listing is complete before acting on the result.
func (e *Exporter) collectReachableManifestMetadata(
	ctx context.Context, tbl *icetable.Table,
) (map[string]struct{}, error) {
	if e.backend == nil || tbl == nil {
		return nil, fmt.Errorf("manifest reachability requires a table and backend")
	}
	current := tbl.MetadataLocation()
	currentKey, ok := e.warehouseRelKey(current)
	if !ok {
		return nil, fmt.Errorf("current Iceberg metadata URI is not addressable: %q", current)
	}
	dir := path.Dir(currentKey)
	if dir == "." || dir == "/" || dir == "" {
		return nil, fmt.Errorf("unsafe Iceberg metadata directory %q", dir)
	}
	keys, err := e.backend.List(ctx, dir+"/")
	if err != nil {
		return nil, fmt.Errorf("list Iceberg metadata: %w", err)
	}
	meta := make(map[string]struct{})
	for _, key := range keys {
		if path.Dir(key) != dir || !strings.HasSuffix(path.Base(key), ".metadata.json") {
			continue
		}
		if _, duplicate := meta[key]; duplicate {
			return nil, fmt.Errorf("duplicate metadata object in listing: %q", key)
		}
		meta[key] = struct{}{}
	}
	if _, found := meta[currentKey]; !found {
		return nil, fmt.Errorf("current metadata %q missing from listing", currentKey)
	}

	refs := make(map[string]struct{})
	listCache := make(map[string]struct{})
	for key := range meta {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		raw, err := e.backend.Read(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read metadata %q: %w", key, err)
		}
		parsed, err := icetable.ParseMetadataBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("parse metadata %q: %w", key, err)
		}
		if parsed.TableUUID() != tbl.Metadata().TableUUID() {
			return nil, fmt.Errorf("metadata %q belongs to another table", key)
		}
		for _, snap := range parsed.Snapshots() {
			if snap.ManifestList == "" {
				return nil, fmt.Errorf("snapshot missing manifest list in %q", key)
			}
			listKey, ok := e.warehouseRelKey(snap.ManifestList)
			if !ok || path.Dir(listKey) != dir || !strings.HasSuffix(listKey, ".avro") {
				return nil, fmt.Errorf("unaddressable manifest list %q in %q", snap.ManifestList, key)
			}
			refs[listKey] = struct{}{}
			if _, alreadyRead := listCache[listKey]; alreadyRead {
				continue
			}
			body, err := e.backend.Read(ctx, listKey)
			if err != nil {
				return nil, fmt.Errorf("read manifest list %q: %w", listKey, err)
			}
			manifests, err := iceberg.ReadManifestList(bytes.NewReader(body))
			if err != nil {
				return nil, fmt.Errorf("decode manifest list %q: %w", listKey, err)
			}
			for _, manifest := range manifests {
				manifestKey, ok := e.warehouseRelKey(manifest.FilePath())
				if !ok || path.Dir(manifestKey) != dir || !strings.HasSuffix(manifestKey, ".avro") {
					return nil, fmt.Errorf("unaddressable manifest %q in %q", manifest.FilePath(), listKey)
				}
				exists, err := e.backend.Exists(ctx, manifestKey)
				if err != nil || !exists {
					return nil, fmt.Errorf("referenced manifest %q unavailable (exists=%v): %v", manifestKey, exists, err)
				}
				refs[manifestKey] = struct{}{}
			}
			listCache[listKey] = struct{}{}
		}
	}
	return refs, nil
}

// oldUnreachableManifestCandidates is classification ONLY. It cannot perform
// deletion, and its output is not a deletion authorisation: a future sweep
// must independently verify listing completeness and catalog identity.
func oldUnreachableManifestCandidates(
	objects []storage.ObjectInfo, metadataDir string, protected map[string]struct{},
	now time.Time,
) []string {
	cutoff := now.Add(-7 * 24 * time.Hour)
	seen := make(map[string]struct{})
	var candidates []string
	for _, obj := range objects {
		if path.Dir(obj.Path) != metadataDir || !strings.HasSuffix(path.Base(obj.Path), ".avro") ||
			obj.LastModified.IsZero() || obj.LastModified.After(cutoff) {
			continue
		}
		if _, ok := protected[obj.Path]; ok {
			continue
		}
		if _, ok := seen[obj.Path]; ok {
			continue
		}
		seen[obj.Path] = struct{}{}
		candidates = append(candidates, obj.Path)
	}
	sort.Strings(candidates)
	return candidates
}

// listManifestGCObjects refuses inconsistencies between the backend's key
// listing and its timestamped object listing. A partial view is not evidence
// that any file is unreferenced. Include nested objects in the consistency
// check even though this sweep only deletes immediate *.avro children.
func listManifestGCObjects(
	ctx context.Context, backend storage.Backend, lister storage.ObjectLister,
	dir string,
) (map[string]storage.ObjectInfo, error) {
	prefix := dir + "/"
	keys, err := backend.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list metadata keys: %w", err)
	}
	objects, err := lister.ListObjects(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list metadata objects: %w", err)
	}
	listed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) || key == prefix {
			return nil, fmt.Errorf("unexpected listed metadata key %q", key)
		}
		if _, ok := listed[key]; ok {
			return nil, fmt.Errorf("duplicate metadata key %q", key)
		}
		listed[key] = struct{}{}
	}
	if len(objects) != len(listed) {
		return nil, fmt.Errorf("metadata list mismatch: keys=%d objects=%d", len(listed), len(objects))
	}
	byKey := make(map[string]storage.ObjectInfo, len(objects))
	for _, obj := range objects {
		if _, ok := listed[obj.Path]; !ok {
			return nil, fmt.Errorf("metadata object %q absent from key listing", obj.Path)
		}
		if _, ok := byKey[obj.Path]; ok {
			return nil, fmt.Errorf("duplicate metadata object %q", obj.Path)
		}
		byKey[obj.Path] = obj
	}
	return byKey, nil
}

// sweepOrphanManifestMetadata reclaims only aged, unreachable table metadata
// Avro files. It NEVER deletes Parquet, JSON or directories. Every uncertainty
// aborts the sweep without a deletion: in particular absent/corrupt referenced
// files, listing changes, a newer catalog version, and changed object metadata.
// Single-writer reconciliation and a seven-day grace period protect concurrent
// readers; externally coordinated writers are outside this ownership contract.
func (e *Exporter) sweepOrphanManifestMetadata(
	ctx context.Context, tbl *icetable.Table, database, measurement string,
) {
	if e.retain <= 0 || e.backend == nil || tbl == nil {
		return
	}
	lister, ok := e.backend.(storage.ObjectLister)
	if !ok {
		return
	}
	current := tbl.MetadataLocation()
	key, ok := e.warehouseRelKey(current)
	if !ok {
		return
	}
	dir := path.Dir(key)
	if dir == "." || dir == "" || dir == "/" {
		return
	}
	warn := func(err error) {
		e.logger.Warn().Err(err).Str("metadata_dir", dir).
			Msg("Iceberg orphan manifest cleanup skipped (non-fatal)")
	}
	first, err := listManifestGCObjects(ctx, e.backend, lister, dir)
	if err != nil {
		warn(err)
		return
	}
	protected, err := e.collectReachableManifestMetadata(ctx, tbl)
	if err != nil {
		warn(err)
		return
	}
	objects := make([]storage.ObjectInfo, 0, len(first))
	for _, obj := range first {
		objects = append(objects, obj)
	}
	candidates := oldUnreachableManifestCandidates(objects, dir, protected, time.Now())
	if len(candidates) == 0 {
		return
	}

	// Recheck catalog identity and both listings immediately before deleting.
	// The second reachability pass also detects a new reference in a retained
	// version modified in place. All candidate checks finish before ANY delete.
	latest, err := e.catalog.LoadTable(ctx, e.tableIdent(database, measurement))
	if err != nil {
		warn(err)
		return
	}
	if latest.MetadataLocation() != current {
		warn(fmt.Errorf("catalog metadata changed during orphan scan"))
		return
	}
	second, err := listManifestGCObjects(ctx, e.backend, lister, dir)
	if err != nil {
		warn(err)
		return
	}
	if len(first) != len(second) {
		warn(fmt.Errorf("metadata object set changed during orphan scan"))
		return
	}
	for key, initial := range first {
		subsequent, ok := second[key]
		if !ok || initial.Size != subsequent.Size || !initial.LastModified.Equal(subsequent.LastModified) {
			warn(fmt.Errorf("metadata object %q changed during orphan scan", key))
			return
		}
	}
	protectedAgain, err := e.collectReachableManifestMetadata(ctx, latest)
	if err != nil {
		warn(err)
		return
	}
	for _, key := range candidates {
		if _, newlyProtected := protectedAgain[key]; newlyProtected {
			warn(fmt.Errorf("manifest %q became referenced during orphan scan", key))
			return
		}
		// No extra generic backend paths can pass this restriction.
		if path.Dir(key) != dir || !strings.HasSuffix(path.Base(key), ".avro") {
			warn(fmt.Errorf("unsafe orphan candidate %q", key))
			return
		}
	}
	// The exporter is the only metadata writer under its per-table directory;
	// readers are protected by the grace period and retained versions.
	for _, key := range candidates {
		if err := ctx.Err(); err != nil {
			warn(err)
			return
		}
		if err := e.backend.Delete(ctx, key); err != nil {
			warn(fmt.Errorf("delete orphan %q: %w", key, err))
			return
		}
		e.logger.Debug().Str("key", key).Msg("Reclaimed unreachable Iceberg metadata manifest")
	}
}
