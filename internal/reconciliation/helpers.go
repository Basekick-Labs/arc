package reconciliation

import "strings"

// Cap and sample helpers used across the package. All four functions
// share the same shape (cap a slice to a maximum length) so they live
// together rather than next to whichever sweep first needed them —
// gemini correctly flagged the cross-file helper placement as a
// maintenance hazard.

// appendBounded appends msg to s but caps the resulting slice at limit
// entries. Used to keep Run.Errors and similar diagnostic slices from
// growing unboundedly under sustained failure conditions. limit <= 0
// disables the cap.
func appendBounded(s []string, msg string, limit int) []string {
	if limit > 0 && len(s) >= limit {
		return s
	}
	return append(s, msg)
}

// joinSample concatenates a bounded list of paths into a single
// newline-separated string for inclusion in the audit Detail map. The
// audit schema stores Detail as a JSON-encoded map, so embedded
// newlines or control bytes in paths get escaped — there is no
// audit-log injection risk. limit <= 0 or limit > len(paths) emits the
// whole slice; len(paths)==0 returns the empty string.
func joinSample(paths []string, limit int) string {
	if limit <= 0 || limit > len(paths) {
		limit = len(paths)
	}
	if limit == 0 {
		return ""
	}
	return strings.Join(paths[:limit], "\n")
}

// sampleSlice returns up to limit entries from `in`, mapped through
// `key` so callers with non-string slices (e.g. orphanStorageCandidate)
// can pull out a string field. Returns nil when in is empty so JSON
// audit events omit the key entirely. limit <= 0 returns all entries.
func sampleSlice[T any](in []T, limit int, key func(T) string) []string {
	if len(in) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(in) {
		limit = len(in)
	}
	out := make([]string, limit)
	for i := 0; i < limit; i++ {
		out[i] = key(in[i])
	}
	return out
}

// sampleStrings is sampleSlice specialized to []string. Kept as a
// thin shim for call-site readability; the generic form would also work.
func sampleStrings(in []string, limit int) []string {
	return sampleSlice(in, limit, func(s string) string { return s })
}

// sampleCandidatePaths is sampleSlice specialized to extract the path
// field from each orphan-storage candidate.
func sampleCandidatePaths(in []orphanStorageCandidate, limit int) []string {
	return sampleSlice(in, limit, func(c orphanStorageCandidate) string { return c.path })
}

// boolStr renders a Go bool as "true"/"false" for the audit Detail map.
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
