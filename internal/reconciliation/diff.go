package reconciliation

import (
	"sort"
	"time"
)

// diffResult is the outcome of comparing the manifest snapshot against
// the storage walk. Membership tests use the manifest path map.
type diffResult struct {
	// orphanManifest holds paths present in the manifest but missing
	// from storage. These are removed by the orphan-manifest sweep.
	orphanManifest []string

	// orphanStorage holds storage objects with no manifest entry that
	// have already passed the grace window check. These are eligible
	// for deletion by the orphan-storage sweep.
	orphanStorage []orphanStorageCandidate

	// skippedGraceCount counts orphan storage candidates that were
	// skipped because they're still inside the grace window. Surfaced
	// on the Run summary so operators can see "we noticed N young
	// files we couldn't touch yet".
	skippedGraceCount int
}

// orphanStorageCandidate carries the path AND the mtime that earned its
// orphan classification, so the per-file re-check in step 5 has the
// freshest mtime available without a second StatFile call.
type orphanStorageCandidate struct {
	path         string
	lastModified time.Time
}

// computeDiff is the streaming set-difference algorithm. It is pure: no
// I/O, no clock reads except via `now` parameter, no logging — just data
// transformation. Easy to unit test.
//
// The grace window is `cfg.GraceWindow + cfg.ClockSkewAllowance`. Files
// with a zero LastModified are treated as "still young" — i.e. PROTECTED
// from deletion. The fallback `List`+`StatFile` path produces zero
// mtimes when a backend doesn't implement `ObjectLister`; we'd rather
// leak orphan storage than risk deleting a file we can't age-check.
// Production backends (S3, Azure, Local) all implement ObjectLister and
// won't hit this branch; this is purely a safety net for custom
// backends and a hint to operators that they should implement
// ObjectLister to get full reconciliation coverage.
//
// In BackendLocal mode the per-node OriginNodeID filter applies to
// orphan-storage candidates: files whose manifest twin (if any) named
// a different node as origin are NOT candidates here. Since orphan
// storage means "no manifest entry", we have no OriginNodeID to filter
// against — for local mode we accept that the per-node walk is
// scoped by the backend's physical layout (each node only sees its
// own disks). The scheduler / wiring layer does NOT cross-mount
// remote disks, so the scope is correct.
func computeDiff(
	manifest []*ObjectKey,
	storage []objectRecord,
	now time.Time,
	graceTotal time.Duration,
) diffResult {
	manifestSet := make(map[string]*ObjectKey, len(manifest))
	for _, e := range manifest {
		manifestSet[e.Path] = e
	}

	// Track which manifest entries had a corresponding storage hit so
	// the leftovers become orphan-manifest at the end. unseen starts
	// as a copy of the manifest set keyed by path; storage matches
	// remove from it.
	unseen := make(map[string]struct{}, len(manifest))
	for p := range manifestSet {
		unseen[p] = struct{}{}
	}

	out := diffResult{}

	for _, rec := range storage {
		if _, inManifest := manifestSet[rec.path]; inManifest {
			delete(unseen, rec.path)
			continue
		}
		// Orphan-storage candidate.
		if isYoungerThan(rec.lastModified, now, graceTotal) {
			out.skippedGraceCount++
			continue
		}
		out.orphanStorage = append(out.orphanStorage, orphanStorageCandidate{
			path:         rec.path,
			lastModified: rec.lastModified,
		})
	}

	// Anything left in unseen is an orphan-manifest entry.
	out.orphanManifest = make([]string, 0, len(unseen))
	for p := range unseen {
		out.orphanManifest = append(out.orphanManifest, p)
	}

	// Sort both candidate lists so cap-bounded runs are deterministic
	// about which orphans get cleaned. Without this, Go's randomized
	// map iteration would let pathologically late-sorted paths repeatedly
	// miss the cap and never get processed across runs.
	sort.Strings(out.orphanManifest)
	sort.Slice(out.orphanStorage, func(i, j int) bool {
		return out.orphanStorage[i].path < out.orphanStorage[j].path
	})

	return out
}

// isYoungerThan reports whether a file at `mtime` should be considered
// younger than the grace window relative to `now`. Zero mtime is
// treated as YOUNG (protected) — see the rationale in computeDiff's
// docstring. The conservative choice prevents data loss on backends
// that don't expose modification times.
func isYoungerThan(mtime, now time.Time, grace time.Duration) bool {
	if mtime.IsZero() {
		return true
	}
	return now.Sub(mtime) < grace
}
