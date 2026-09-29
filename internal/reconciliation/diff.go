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
// The membership test uses EVERY manifest entry, whatever node originated
// it: the manifest is the cluster's source of truth for "does this path
// exist", so a tracked path is never an orphan-storage candidate. That
// matters on per-node storage with file replication (and on a local
// backend over a shared mount), where every node holds every file — a
// filter to this node's own-origin entries there reported the replicas of
// every other node as orphan storage (#957).
//
// The per-node origin scoping applies to the other direction only. With
// perNodeStorage set, an entry another node originated that is missing
// from this disk is not an orphan-manifest candidate: without replication
// it lives on that node's disk, with replication it has not been pulled
// yet, and either way the manifest is right. Entries with no origin
// (pre-Phase-1) stay candidates on every node. An empty localNodeID with
// perNodeStorage set fails safe — every originated entry is skipped and only
// origin-less entries are reported; NewReconciler rejects that configuration
// anyway. The manifest is a map keyed by path, so the slice holds no
// duplicates and the loop below cannot report a path twice.
func computeDiff(
	manifest []*ObjectKey,
	storage []objectRecord,
	now time.Time,
	graceTotal time.Duration,
	localNodeID string,
	perNodeStorage bool,
) diffResult {
	// Single map sized to manifest cardinality: value tracks whether
	// the path was seen in the storage walk. Replaces the previous
	// pair-of-maps shape that needed an O(N) copy of the manifest set.
	// On a 200k-entry manifest this saves ~16 MB of transient
	// allocation per run.
	manifestSeen := make(map[string]bool, len(manifest))
	for _, e := range manifest {
		manifestSeen[e.Path] = false
	}

	out := diffResult{}

	for _, rec := range storage {
		if _, inManifest := manifestSeen[rec.path]; inManifest {
			manifestSeen[rec.path] = true
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

	// Manifest entries with seen=false are orphans (referenced but
	// missing from storage). Iterate the keys, not the map: only the key
	// carries the origin the per-node scoping needs.
	out.orphanManifest = make([]string, 0)
	for _, e := range manifest {
		if manifestSeen[e.Path] {
			continue
		}
		if perNodeStorage && e.OriginNodeID != "" && e.OriginNodeID != localNodeID {
			continue
		}
		out.orphanManifest = append(out.orphanManifest, e.Path)
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
