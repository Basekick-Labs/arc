package storage

import (
	"fmt"
	"strings"
)

// ValidateObjectPrefix checks a configured object-store prefix and returns it
// with a trailing separator.
//
// Shared by every object-store backend: S3 and MinIO read it from
// storage.s3_prefix, Azure Blob from storage.azure_prefix (#1102). It was
// called ValidateS3Prefix while S3 was the only backend with a prefix, which
// made the one generic rule in this package look S3-specific. Nothing about it
// ever was.
//
// It validates rather than rewrites. The previous SanitizeS3Prefix repaired its
// input, and the damage was on its SUCCESS path, not its failure path:
//
//	"/"      -> "/"      every key then starts with "/", which MinIO folds away
//	"a//b"   -> "a//b/"  every write 400s with XMinioInvalidObjectName
//	"."      -> "./"     every write 400s with XMinioInvalidResourceName
//	"a/..b"  -> ""       a legitimate prefix silently becomes the BUCKET ROOT
//
// The last is the worst of them: "" is not a safe fallback, it is a different
// and much larger location, so a typo relocated an entire deployment without a
// word. The ".." rejection that caused it was also a raw substring match, the
// same class this repo removed for keys in #741.
//
// An empty prefix is legitimate and means the bucket or container root was
// chosen deliberately.
//
// The character allowlist is narrower than Azure blob names permit, and that
// is deliberate rather than an S3 leftover: ValidateKey already holds every
// Arc key to a narrower set than any store accepts, and a prefix an operator
// configures is interpolated into DuckDB read_parquet() calls, which the
// allowlist is the defence in depth for. Widening it for Azure would widen the
// SQL surface for a shape no deployment needs.
func ValidateObjectPrefix(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", nil
	}
	// Reuse the key contract, which already rejects leading "/", "." and ".."
	// segments, empty interior segments, backslash and NUL. A trailing
	// separator is what a prefix is for, so strip it before checking and add
	// it back after.
	if err := ValidateListPrefix(prefix); err != nil {
		return "", fmt.Errorf("storage prefix %q is not usable: %w", prefix, err)
	}
	// Defence in depth against SQL injection: the prefix is interpolated into
	// DuckDB read_parquet() calls, so keep the character allowlist the old
	// implementation had.
	for _, c := range prefix {
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'):
		case c == '/' || c == '-' || c == '_' || c == '.':
		default:
			return "", fmt.Errorf("storage prefix %q contains an unsupported character %q", prefix, c)
		}
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix, nil
}
