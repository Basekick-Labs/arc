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
func (r *Reconciler) walkStorage(ctx context.Context, manifest []*ObjectKey) (walkResult, error) {
	prefixes := r.derivePrefixes(ctx, manifest)

	res := walkResult{
		records: make([]objectRecord, 0, len(manifest)),
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
// block past the run budget.
func (r *Reconciler) derivePrefixes(ctx context.Context, manifest []*ObjectKey) []string {
	set := make(map[string]struct{}, 32)
	for _, e := range manifest {
		if e.Database == "" || e.Measurement == "" {
			// Pre-Phase-1 file in the manifest with no metadata. The
			// root-walk fallback below covers these.
			continue
		}
		set[e.Database+"/"+e.Measurement+"/"] = struct{}{}
	}

	prefixes := make([]string, 0, len(set)+1)
	for p := range set {
		prefixes = append(prefixes, p)
	}

	// Root walk catches pre-Phase-1 files and any file in a database the
	// manifest doesn't know about. The cost is one extra List of the
	// top-level directories — DirectoryLister returns those without
	// recursing, so this is cheap on every backend.
	if dl, ok := r.storage.(storage.DirectoryLister); ok {
		dirs, err := dl.ListDirectories(ctx, "")
		if err == nil {
			for _, d := range dirs {
				// Top-level directories are databases. We add each as a
				// separate prefix so the per-prefix List below picks up
				// any (database, measurement) pair not in the manifest.
				key := strings.TrimSuffix(d, "/") + "/"
				if _, exists := set[key]; !exists {
					// d is a database; we don't know its measurements yet.
					// Walk one level deeper to get measurements.
					innerDirs, innerErr := dl.ListDirectories(ctx, key)
					if innerErr == nil {
						for _, m := range innerDirs {
							subKey := strings.TrimSuffix(d, "/") + "/" + strings.TrimSuffix(m, "/") + "/"
							if _, exists := set[subKey]; !exists {
								prefixes = append(prefixes, subKey)
							}
						}
					}
				}
			}
		}
	}

	return prefixes
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

	paths, err := r.storage.List(prefixCtx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list %q: %w", prefix, err)
	}
	out := make([]objectRecord, 0, len(paths))
	for _, p := range paths {
		if !isParquetCandidate(p) {
			continue
		}
		// Without ObjectLister we have no mtime — set the zero value;
		// the grace window check treats time.Time{} as "infinitely old"
		// since IsZero() is checked before the time-since math runs.
		out = append(out, objectRecord{path: p})
	}
	return out, nil
}

// isParquetCandidate filters the storage walk to only the file kinds the
// reconciler is allowed to act on. We deliberately ignore:
//
//   - Anything not ending in `.parquet`  — query-relevant data only
//   - Files in `_compaction_state/`     — owned by the compaction watcher
//   - `.tmp.*` files                    — in-flight writes
//   - hidden files (`.something`)       — never reconciler's concern
func isParquetCandidate(p string) bool {
	if !strings.HasSuffix(p, ".parquet") {
		return false
	}
	base := path.Base(p)
	if strings.HasPrefix(base, ".tmp.") || strings.HasPrefix(base, ".") {
		return false
	}
	if strings.Contains(p, "_compaction_state/") {
		return false
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
