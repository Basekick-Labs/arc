package raft

import (
	"errors"
	"fmt"
	"strings"
)

// MaxManifestPathLen bounds a manifest entry's path to a safe length.
// 4096 matches the typical POSIX PATH_MAX. A malicious sender that bypassed
// the other checks could otherwise propose a multi-megabyte path string and
// inflate the FSM's in-memory map.
const MaxManifestPathLen = 4096

// Sentinel errors returned by ValidateManifestPath. Exported so callers
// (live-commit handlers, startup quarantine, tests) can branch on the
// specific rejection reason via errors.Is.
var (
	// ErrPathEmpty: the path field is the empty string. This was the
	// only check pre-PR.
	ErrPathEmpty = errors.New("manifest path: empty")

	// ErrPathTooLong: path exceeds MaxManifestPathLen bytes.
	ErrPathTooLong = errors.New("manifest path: exceeds maximum length")

	// ErrPathTraversal: path contains a `..` segment. Legitimate Arc
	// storage paths are flat and partition-hierarchical; ".." never
	// appears.
	ErrPathTraversal = errors.New("manifest path: contains parent-directory traversal")

	// ErrPathAbsolute: path begins with `/` (POSIX absolute) or matches
	// a Windows drive prefix (C:\, etc). Legitimate Arc paths are
	// relative to the storage backend root, never absolute.
	ErrPathAbsolute = errors.New("manifest path: absolute paths not permitted")

	// ErrPathScheme: path contains a URL scheme separator (`://`).
	// Legitimate Arc paths are storage-backend-relative; the scheme is
	// added by the backend at read time, not stored in the manifest.
	// A scheme in the manifest is the canonical worm-primitive shape
	// (s3://attacker-bucket/...).
	ErrPathScheme = errors.New("manifest path: contains URL scheme")

	// ErrPathNullByte: path contains a NUL byte. NUL is forbidden in
	// most filesystem APIs and would smuggle through C-string callers
	// (everything before the NUL gets evaluated, everything after
	// silently ignored).
	ErrPathNullByte = errors.New("manifest path: contains NUL byte")
)

// ValidateManifestPath rejects paths that cannot legitimately come from
// Arc's writer (internal/cluster/file_registrar.go) or compactor
// (internal/cluster/compaction_bridge.go). Legitimate paths follow the
// shape {database}/{measurement}/{year}/{month}/{day}/{hour}/{filename}
// or a compaction-output variant — always RELATIVE to the storage
// backend root, never carrying a scheme, never containing parent
// traversal.
//
// We don't enforce the exact partition format because compaction
// output, retention rewrites, and future code paths can legitimately
// differ. The checks below catch every malicious shape enumerated in
// GHSA-f85q-mvg8-qf37 (Arc Enterprise audit X2, 2026-05-19) without
// over-constraining what writers may produce.
//
// Checks are ordered from cheapest to most expensive so the common
// "happy path" early-returns at the first emptiness check.
func ValidateManifestPath(path string) error {
	if path == "" {
		return ErrPathEmpty
	}
	if len(path) > MaxManifestPathLen {
		return fmt.Errorf("%w: %d bytes", ErrPathTooLong, len(path))
	}
	if strings.ContainsRune(path, 0) {
		return ErrPathNullByte
	}
	// Reject any colon in the path EXCEPT the Windows drive-letter
	// case at exactly index 1 (e.g. `C:\Windows\...`). This catches
	// not only `s3://...` (which the prior `://` substring check
	// covered) but also single-slash URIs like `file:/etc/passwd`
	// and scheme-only URIs like `mailto:foo` or `data:...`. Legit
	// Arc storage paths never contain `:` — the partition format is
	// `{db}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{filename}.parquet`,
	// none of which can legitimately carry a colon. The Windows-drive
	// exception falls through to the absolute-path check below, which
	// rejects with the more specific ErrPathAbsolute reason.
	if idx := strings.Index(path, ":"); idx != -1 {
		if idx != 1 || !isAbsolutePath(path) {
			return ErrPathScheme
		}
	}
	if isAbsolutePath(path) {
		return ErrPathAbsolute
	}
	if hasParentTraversalSegment(path) {
		return ErrPathTraversal
	}
	return nil
}

// isAbsolutePath returns true for POSIX-absolute paths (leading `/`)
// and Windows-absolute paths (drive letter, e.g. `C:\` or `C:/`).
// Legitimate Arc manifest paths are relative to the storage backend
// root and never lead with either shape.
func isAbsolutePath(path string) bool {
	if len(path) == 0 {
		return false
	}
	if path[0] == '/' || path[0] == '\\' {
		return true
	}
	// Windows drive letter (`C:`, `D:`, ...) followed by separator.
	// Catches `C:\Windows\System32\config\SAM` and `C:/foo/bar`.
	if len(path) >= 3 && path[1] == ':' &&
		((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
		(path[2] == '\\' || path[2] == '/') {
		return true
	}
	return false
}

// hasParentTraversalSegment returns true if any path segment is
// exactly `..`. Treats BOTH `/` and `\` as separators in the same pass
// so mixed-separator paths (e.g. `db/..\etc` or `db\..\etc`) are
// caught — splitting on one separator at a time misses the case where
// `..` sits between two different separators.
//
// We check segment equality, not substring containment, so legitimate
// filenames containing `..` as part of a longer token (e.g.
// `db/m/2026/05/20/15/measurement_20260520_150000..parquet`) aren't
// false-positives. The Arc writer doesn't produce that shape today,
// but the segment check is the correct semantics regardless.
func hasParentTraversalSegment(path string) bool {
	// Fast path: if the literal `..` substring isn't present at all,
	// no segment can equal `..`.
	if !strings.Contains(path, "..") {
		return false
	}
	segments := strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	for _, segment := range segments {
		if segment == ".." {
			return true
		}
	}
	return false
}
