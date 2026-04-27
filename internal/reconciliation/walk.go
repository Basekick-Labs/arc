package reconciliation

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// objectRecord is the per-file record produced by the storage walk. Only
// the fields the diff/grace check actually reads are carried — keeps peak
// memory proportional to file count, not object metadata weight.
type objectRecord struct {
	path         string
	lastModified time.Time
}

// walkResult is what walkStorage returns to Reconcile. It carries the
// objects plus a `partial` flag the caller surfaces in Run.WalkPartial
// so an operator can disambiguate "0 orphans found" from "we couldn't
// see most of the bucket". Per-prefix errors are also written into
// run.Errors so the run summary lists them.
type walkResult struct {
	records      []objectRecord
	partial      bool
	prefixErrors []string // human-readable; capped by caller via appendBounded
}

// walkStorage enumerates Parquet files in storage by per-prefix listing.
// It groups files by (database, measurement) prefix derived from the
// manifest snapshot to avoid the "single root List on a multi-TB bucket"
// problem documented in the plan.
//
// Pre-Phase-1 files (or any file in a database the manifest doesn't know
// about) are picked up via the DirectoryLister fallback when the backend
// supports it. If the backend doesn't implement DirectoryLister, those
// files are invisible to reconciliation — accepted limitation; the
// production local/S3/Azure backends all implement it.
//
// The returned slice is in arbitrary order. Callers should expect
// duplicates only if List itself returns duplicates (none of the
// production backends do).
//
// Logs the ObjectLister-missing Warn once per run (not per prefix) so
// a backend without ObjectLister doesn't flood operator dashboards on
// large schemas.
func (r *Reconciler) walkStorage(ctx context.Context, manifest []*ObjectKey) (walkResult, error) {
	discovery := r.derivePrefixes(ctx, manifest)
	prefixes := discovery.prefixes

	if _, hasObjectLister := r.storage.(storage.ObjectLister); !hasObjectLister {
		r.logger.Warn().
			Str("backend_type", r.storage.Type()).
			Int("prefix_count", len(prefixes)).
			Msg("Reconciliation: backend does not implement ObjectLister; orphan-storage detection is degraded (files protected by grace window)")
	}

	res := walkResult{
		records:      make([]objectRecord, 0, len(manifest)),
		partial:      discovery.partial,
		prefixErrors: append([]string{}, discovery.errors...),
	}
	seen := make(map[string]struct{}, len(manifest))

	for _, prefix := range prefixes {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		records, err := r.listPrefix(ctx, prefix)
		if err != nil {
			// A per-prefix list failure is logged and the prefix is
			// skipped; the run continues against the remaining prefixes
			// so a single transient backend hiccup doesn't kill the
			// whole cycle. Errors are reported on the Run summary so
			// operators can spot "0 orphans found" runs that were
			// actually blind to most of the bucket.
			res.partial = true
			res.prefixErrors = append(res.prefixErrors, fmt.Sprintf("prefix %q: %v", prefix, err))
			r.logger.Warn().
				Err(err).
				Str("prefix", prefix).
				Msg("Reconciliation: prefix list failed; skipping")
			continue
		}
		for _, rec := range records {
			if _, dup := seen[rec.path]; dup {
				continue
			}
			seen[rec.path] = struct{}{}
			res.records = append(res.records, rec)
		}
	}

	return res, nil
}

// prefixDiscovery is the result of derivePrefixes. partial=true and/or
// non-nil errors indicate the prefix set is not authoritative — the
// caller surfaces both into Run.WalkPartial / Run.Errors so a "0
// orphans found" run that was actually blind to part of the bucket
// is unambiguous on dashboards.
type prefixDiscovery struct {
	prefixes []string
	partial  bool
	errors   []string
}

// derivePrefixes returns the unique "<database>/<measurement>/" prefixes
// represented in the manifest plus an extra root walk for orphan
// pre-Phase-1 files. The result is sorted+deduplicated.
//
// We restrict to top-level db/measurement pairs (not deeper) because
// every file in a partition must live at db/measurement/yyyy/mm/dd/hh/...
// The reconciler does NOT walk hour-prefix-by-hour-prefix — a single
// db/m/ prefix already returns the whole partition tree from a paginated
// List. This keeps the per-prefix work bounded to one bucket call (with
// internal SDK pagination) per (db, measurement) pair.
//
// ctx is threaded through ListDirectories so MaxRunDuration applies to
// the discovery walk too — without this, a stuck top-level list could
// block past the run budget. Inner ListDirectories failures (e.g.
// permission denied on a single tenant prefix) flip the partial flag
// and append to errors so the caller can stamp Run.WalkPartial; this
// pairs with the per-prefix error surfacing in walkStorage.
func (r *Reconciler) derivePrefixes(ctx context.Context, manifest []*ObjectKey) prefixDiscovery {
	// Two sets sized to typical production schema cardinality:
	//   set:   "database/measurement/" — drives the per-prefix walk
	//   dbSet: "database/"             — used by the root-walk fallback
	//                                    to skip databases the manifest
	//                                    already covers, without burning
	//                                    inspection budget on them.
	// Keeping them separate fixes a real bug: a previous version checked
	// the database-only key against `set` and always missed, causing
	// known databases to consume the MaxRootWalkDatabases budget and
	// prematurely cut off discovery of truly-unknown databases.
	set := make(map[string]struct{}, 128)
	dbSet := make(map[string]struct{}, 128)
	for _, e := range manifest {
		if e.Database == "" || e.Measurement == "" {
			// Pre-Phase-1 file in the manifest with no metadata. The
			// root-walk fallback below covers these.
			continue
		}
		set[e.Database+"/"+e.Measurement+"/"] = struct{}{}
		dbSet[e.Database+"/"] = struct{}{}
	}

	out := prefixDiscovery{
		prefixes: make([]string, 0, len(set)+1),
	}
	for p := range set {
		out.prefixes = append(out.prefixes, p)
	}

	// Root walk catches pre-Phase-1 files and any file in a database the
	// manifest doesn't know about. The cost is one extra List of the
	// top-level directories plus one ListDirectories per unknown
	// database — bounded by cfg.MaxRootWalkDatabases so a sprawling
	// cluster (or a shared bucket with unrelated top-level dirs) can't
	// trigger thousands of metadata calls per tick. Set the cap to 0
	// in config to disable the root walk entirely.
	if r.cfg.MaxRootWalkDatabases <= 0 {
		return out
	}
	dl, ok := r.storage.(storage.DirectoryLister)
	if !ok {
		return out
	}
	dirs, err := dl.ListDirectories(ctx, "")
	if err != nil {
		// Top-level discovery itself failed — partial walk; orphan
		// detection for unknown databases is degraded for this tick.
		out.partial = true
		out.errors = append(out.errors, fmt.Sprintf("root list_directories: %v", err))
		return out
	}
	// Bound the iteration over the returned slice itself, not just the
	// inspected count. A shared bucket with many unrelated top-level
	// prefixes could otherwise pin megabytes of dirnames and burn CPU
	// on the dbSet lookup loop even when every entry is already known.
	// Headroom of 4× over the inspection cap covers normal cases where
	// most top-level entries are known databases the dbSet check skips.
	scanLimit := r.cfg.MaxRootWalkDatabases * 4
	if scanLimit < r.cfg.MaxRootWalkDatabases {
		// Defensive against integer overflow if MaxRootWalkDatabases is
		// set to a value > MaxInt/4; clamp to len(dirs) directly.
		scanLimit = len(dirs)
	}
	if scanLimit > len(dirs) {
		scanLimit = len(dirs)
	}
	if scanLimit < len(dirs) {
		out.partial = true
		r.logger.Warn().
			Int("returned", len(dirs)).
			Int("scan_limit", scanLimit).
			Int("cap", r.cfg.MaxRootWalkDatabases).
			Msg("Reconciliation: top-level directory count exceeds 4× root-walk cap; suffix entries skipped this tick")
	}
	inspected := 0
	for i := 0; i < scanLimit; i++ {
		// ctx-aware loop: a misbehaving backend that returns instantly
		// on canceled ctx would otherwise burn through scanLimit
		// iterations of dbSet checks after cancel. Bail at the top so
		// the caller gets a partial discovery, not a wasted budget.
		if ctx.Err() != nil {
			out.partial = true
			out.errors = append(out.errors, fmt.Sprintf("root walk canceled at %d/%d: %v", i, scanLimit, ctx.Err()))
			return out
		}
		d := dirs[i]
		if inspected >= r.cfg.MaxRootWalkDatabases {
			out.partial = true
			r.logger.Warn().
				Int("inspected", inspected).
				Int("scan_limit", scanLimit).
				Int("cap", r.cfg.MaxRootWalkDatabases).
				Msg("Reconciliation: root walk cap reached; remaining unknown databases skipped this tick")
			break
		}
		// Top-level directories are databases. We add each as a
		// separate prefix so the per-prefix List below picks up
		// any (database, measurement) pair not in the manifest.
		key := strings.TrimSuffix(d, "/") + "/"
		if _, exists := dbSet[key]; exists {
			// Already covered by the manifest-derived dbSet;
			// don't count toward the inspection cap.
			continue
		}
		inspected++
		// d is a database; we don't know its measurements yet.
		// Walk one level deeper to get measurements.
		innerDirs, innerErr := dl.ListDirectories(ctx, key)
		if innerErr != nil {
			// Inner listing failed (e.g. permissions on a tenant
			// prefix). Mark partial so operators can disambiguate
			// "0 orphans found" from "we couldn't see this database."
			out.partial = true
			out.errors = append(out.errors, fmt.Sprintf("list_directories %q: %v", key, innerErr))
			continue
		}
		for _, m := range innerDirs {
			subKey := strings.TrimSuffix(d, "/") + "/" + strings.TrimSuffix(m, "/") + "/"
			if _, exists := set[subKey]; !exists {
				out.prefixes = append(out.prefixes, subKey)
			}
		}
	}

	return out
}

// listPrefix lists objects under one prefix. Prefers ObjectLister so we
// get LastModified in a single round trip; falls back to List+StatFile
// otherwise (slower — production backends all implement ObjectLister).
func (r *Reconciler) listPrefix(ctx context.Context, prefix string) ([]objectRecord, error) {
	timeout := r.cfg.PerPrefixTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	prefixCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if ol, ok := r.storage.(storage.ObjectLister); ok {
		objs, err := ol.ListObjects(prefixCtx, prefix)
		if err != nil {
			return nil, fmt.Errorf("list objects %q: %w", prefix, err)
		}
		out := make([]objectRecord, 0, len(objs))
		for _, o := range objs {
			if !isParquetCandidate(o.Path) {
				continue
			}
			out = append(out, objectRecord{path: o.Path, lastModified: o.LastModified})
		}
		return out, nil
	}

	// Backend doesn't implement ObjectLister. Files come back without
	// mtime, so the grace window check in computeDiff will treat them
	// as YOUNG (protected) — orphan-storage cleanup degrades to a
	// no-op for these files, but no data loss is possible. The
	// ObjectLister-missing Warn fires once at the top of walkStorage,
	// not per prefix, to avoid log flooding on large schemas.
	paths, err := r.storage.List(prefixCtx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", prefix, err)
	}
	out := make([]objectRecord, 0, len(paths))
	for _, p := range paths {
		if !isParquetCandidate(p) {
			continue
		}
		out = append(out, objectRecord{path: p})
	}
	return out, nil
}

// isParquetCandidate filters the storage walk to only the file kinds the
// reconciler is allowed to act on. We deliberately ignore:
//
//   - Anything not ending in `.parquet`  — query-relevant data only
//   - Files in a `_compaction_state` path segment — owned by the
//     compaction watcher (the actual reserved directory is
//     `_compaction_state/...` per internal/compaction/manifest.go)
//   - `.tmp.*` files                    — in-flight writes
//   - hidden files (`.something`)       — never reconciler's concern
//
// The `_compaction_state` check is segment-aware (split on `/`)
// rather than substring-aware: a database or measurement name
// containing `_compaction_state` (e.g. `db/m_compaction_state_logs`)
// must NOT have all its valid data files filtered out.
func isParquetCandidate(p string) bool {
	if !strings.HasSuffix(p, ".parquet") {
		return false
	}
	base := path.Base(p)
	if strings.HasPrefix(base, ".tmp.") || strings.HasPrefix(base, ".") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "_compaction_state" {
			return false
		}
	}
	return true
}

// ObjectKey is the minimal manifest fact the walk needs: path + the
// (database, measurement) tuple used to derive prefixes. Reconciler
// converts FileEntry slices into ObjectKey slices internally so the
// walk stays storage-package-only with no Raft-package coupling beyond
// the Coordinator interface.
type ObjectKey struct {
	Path         string
	Database     string
	Measurement  string
	OriginNodeID string
}
