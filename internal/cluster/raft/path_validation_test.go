package raft

import (
	"errors"
	"strings"
	"testing"
)

// TestValidateManifestPath_RejectionMatrix covers every malicious shape
// enumerated in GHSA-f85q-mvg8-qf37 (Arc Enterprise audit X2, 2026-05-19)
// plus close-shape variants. The test is table-driven and each row
// asserts BOTH that the path is rejected AND that the rejection reason
// (sentinel error) matches — so a future refactor that swallows the
// reason classification doesn't silently pass.
func TestValidateManifestPath_RejectionMatrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		path string
		want error
	}{
		// Empty (pre-PR's only check; kept for completeness).
		{"empty", "", ErrPathEmpty},

		// Length cap.
		{"too long", strings.Repeat("a", MaxManifestPathLen+1), ErrPathTooLong},
		{"length exactly cap", strings.Repeat("a", MaxManifestPathLen), nil},

		// NUL injection. NUL before `://` ordering matters: NUL is
		// checked first so the rejection reason is the more
		// security-relevant one.
		{"nul byte at end", "db/m/2026/05/20/15/file.parquet\x00", ErrPathNullByte},
		{"nul byte mid", "db\x00/etc/passwd", ErrPathNullByte},
		{"nul then scheme", "db\x00://etc/passwd", ErrPathNullByte},

		// URL schemes (the canonical worm-primitive shape).
		{"s3 scheme", "s3://attacker-bucket/poisoned.parquet", ErrPathScheme},
		{"http scheme", "http://evil.example.com/payload", ErrPathScheme},
		{"https scheme", "https://evil.example.com/payload", ErrPathScheme},
		{"file scheme triple slash", "file:///etc/passwd", ErrPathScheme},
		{"file scheme single slash", "file:/etc/passwd", ErrPathScheme},
		{"azure scheme", "azure://container/blob", ErrPathScheme},
		{"scheme inside", "db/m/x://y/file.parquet", ErrPathScheme},
		// Scheme-only URI shapes (no `//` after the colon) — still
		// rejected as scheme because no legit Arc path contains `:`.
		{"mailto scheme", "mailto:victim@example.com", ErrPathScheme},
		{"data scheme", "data:text/plain,exfil", ErrPathScheme},
		{"colon-only at zero", "x:y", ErrPathScheme},
		{"colon mid path", "db/m/foo:bar/file.parquet", ErrPathScheme},
		// Edge case: single-char prefix with colon at index 1 that is
		// NOT a Windows drive letter (digit, symbol). The Windows
		// drive-letter exception requires [A-Za-z] AND a path-separator
		// at index 2; a bare `0:foo` matches neither and is rejected
		// as scheme.
		{"digit colon", "0:foo", ErrPathScheme},

		// Absolute paths.
		{"posix absolute", "/etc/passwd", ErrPathAbsolute},
		{"posix absolute slash only", "/", ErrPathAbsolute},
		{"windows backslash absolute", "\\Windows\\System32", ErrPathAbsolute},
		{"windows drive letter upper", "C:\\Windows\\System32\\config\\SAM", ErrPathAbsolute},
		{"windows drive letter lower", "c:/foo/bar", ErrPathAbsolute},
		{"windows drive d", "D:\\data", ErrPathAbsolute},

		// Parent traversal.
		{"basic parent traversal", "../etc/passwd", ErrPathTraversal},
		{"deep parent traversal", "../../../../etc/shadow", ErrPathTraversal},
		{"traversal mid-path", "db/../../../etc/passwd", ErrPathTraversal},
		{"traversal windows separator", "db\\..\\..\\etc", ErrPathTraversal},
		{"traversal mixed separator", "db/..\\etc", ErrPathTraversal},

		// Negative cases that LOOK suspicious but aren't:
		// - filename containing ".." as substring inside a longer token
		//   (legitimate by our segment-equality rule)
		// - leading dot files like ".hidden" (not parent traversal)
		// - trailing dots like "foo." (not parent traversal)
		{"dotdot substring in filename", "db/m/2026/05/20/15/measurement_20260520..parquet", nil},
		{"dot filename", "db/m/2026/05/20/15/.hidden.parquet", nil},
		{"dotdot trailing on segment", "db/m/2026/05/20/15/foo..parquet", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateManifestPath(c.path)
			if c.want == nil {
				if err != nil {
					t.Errorf("expected accept, got reject: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected reject with %v, got nil", c.want)
			}
			if !errors.Is(err, c.want) {
				t.Errorf("expected errors.Is(%v), got %v", c.want, err)
			}
		})
	}
}

// TestValidateManifestPath_AcceptsLegitimateShapes covers the path
// formats Arc's production code (file_registrar.go + compaction_bridge.go)
// actually produces. If any of these starts failing, this PR has broken
// a hot ingestion path.
func TestValidateManifestPath_AcceptsLegitimateShapes(t *testing.T) {
	t.Parallel()
	legit := []string{
		// Writer hot path: ArrowBuffer.generateStoragePath format.
		"production/metrics/2026/05/20/15/metrics_20260520_153012_123456789.parquet",
		// Compaction output (hourly): the same shape but with `_compacted` suffix.
		"production/metrics/2026/05/20/15/metrics_20260520_150000_000000000_compacted.parquet",
		// Compaction output (daily): hour collapsed.
		"production/metrics/2026/05/20/metrics_20260520_compacted.parquet",
		// Database with hyphens, measurement with underscores.
		"my-app-staging/http_requests/2026/05/20/15/http_requests_20260520_153012_000000000.parquet",
		// Numeric-only database (legitimate; Arc doesn't restrict the shape).
		"7/m/2026/05/20/15/m_x.parquet",
	}
	for _, p := range legit {
		if err := ValidateManifestPath(p); err != nil {
			t.Errorf("legitimate path rejected: %q → %v", p, err)
		}
	}
}

// TestValidateManifestPath_SentinelErrorsViaErrorsIs pins that callers
// can branch on the rejection reason. A future refactor that returns
// raw fmt.Errorf strings (which can't be Is-matched) would break this
// test — by design, because the live-commit handler and the startup
// quarantine both log the rejection reason as a structured field.
func TestValidateManifestPath_SentinelErrorsViaErrorsIs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		path     string
		sentinel error
	}{
		{"", ErrPathEmpty},
		{strings.Repeat("a", MaxManifestPathLen+1), ErrPathTooLong},
		{"foo\x00bar", ErrPathNullByte},
		{"s3://x/y", ErrPathScheme},
		{"/etc/passwd", ErrPathAbsolute},
		{"../etc", ErrPathTraversal},
	}
	for _, c := range cases {
		err := ValidateManifestPath(c.path)
		if !errors.Is(err, c.sentinel) {
			t.Errorf("path %q: expected errors.Is(%v), got %v", c.path, c.sentinel, err)
		}
	}
}
