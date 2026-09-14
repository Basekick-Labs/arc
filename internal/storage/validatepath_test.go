package storage

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Tests for #741. sanitizePath used to rewrite its input, folding every ".."
// to "_", which made the mapping from storage key to stored file many-to-one
// and produced #574 and #737. validatePath now rejects instead.

func newPathTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestValidateKey(t *testing.T) {
	accepted := []struct{ name, path string }{
		{"single segment", "cpu"},
		{"partition path", "default/cpu/2026/09/12/07/data.parquet"},
		// Ordinary filenames that merely contain dots. The old fold collapsed
		// the first onto "_foo", colliding it with an unrelated key.
		{"leading dots in a segment", "default/..foo/data.parquet"},
		{"interior dots in a segment", "default/a..b/data.parquet"},
		{"hidden file", "default/.hidden/data.parquet"},
		{"dots in a filename", "default/cpu/a..b.parquet"},
	}
	for _, tt := range accepted {
		t.Run("accept/"+tt.name, func(t *testing.T) {
			if err := ValidateKey(tt.path); err != nil {
				t.Errorf("ValidateKey(%q) = %v, want accepted", tt.path, err)
			}
		})
	}

	rejected := []struct{ name, path string }{
		{"absolute", "/etc/passwd"},
		{"parent segment", "../etc/passwd"},
		{"interior parent segment", "default/../../etc/passwd"},
		{"trailing parent segment", "default/.."},
		{"current segment", "default/./cpu"},
		{"lone current segment", "."},
		{"lone parent segment", ".."},
		{"empty interior segment", "default//cpu"},
		{"NUL byte", "default/cpu\x00.parquet"},
		{"NUL byte at the end", "default/cpu.parquet\x00"},
		// The validator scans "/" segments, so a backslash-built traversal
		// would pass whole on a platform whose OS also splits on it.
		{"backslash traversal", `..\..\..\etc\passwd`},
		{"backslash separator", `default\cpu.parquet`},
	}
	for _, tt := range rejected {
		t.Run("reject/"+tt.name, func(t *testing.T) {
			if err := ValidateKey(tt.path); err == nil {
				t.Errorf("ValidateKey(%q) was accepted", tt.path)
			}
		})
	}
}

// TestValidatePathMatchesFilepathJoin is the regression net for dropping
// filepath.Join. For every key the validator accepts, the result must be
// byte-identical to what the previous implementation produced.
func TestValidateKeyErrorsAreIdentifiable(t *testing.T) {
	// Callers driving cleanup loops must be able to tell a permanent rejection
	// from a transient I/O failure, or they retry the same key forever.
	for _, p := range []string{"/abs", "a/../b", "a//b", "a\x00b", `a\b`} {
		err := ValidateKey(p)
		if err == nil {
			t.Fatalf("ValidateKey(%q) was accepted", p)
		}
		if !errors.Is(err, ErrInvalidPath) {
			t.Errorf("ValidateKey(%q) = %v, which does not match ErrInvalidPath", p, err)
		}
	}
}

// TestValidatePathWithRootBasePath covers a root of "/", the one base path that
// already ends in a separator. t.TempDir() can never produce it.
func TestValidatePathWithRootBasePath(t *testing.T) {
	// Through NewLocalBackend, so it exercises how pathPrefix is derived. A
	// hand-built struct would pass even if the constructor appended a second
	// separator to a root that already ends in one.
	b, err := NewLocalBackend("/", zerolog.Nop())
	if err != nil {
		t.Skipf("cannot use / as a storage root here: %v", err)
	}
	got, err := b.validatePath("a/b")
	if err != nil {
		t.Fatalf("validatePath: %v", err)
	}
	if want := filepath.Join("/", "a/b"); got != want {
		t.Errorf("validatePath(\"a/b\") with root base = %q, want %q", got, want)
	}
}

func TestValidatePathMatchesFilepathJoin(t *testing.T) {
	b := newPathTestBackend(t)

	paths := []string{
		"cpu",
		"default/cpu/2026/09/12/07/data.parquet",
		"default/..foo/data.parquet",
		"default/a..b/data.parquet",
		"default/.hidden/data.parquet",
		".sync-staging/rocket-01/default/cpu/f.parquet",
		strings.Repeat("deep/", 40) + "leaf.parquet",
	}
	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			got, err := b.validatePath(p)
			if err != nil {
				t.Fatalf("validatePath(%q) = %v, want accepted", p, err)
			}
			// filepath.Join is the reference: it is what resolution should
			// produce. Note this is NOT equivalence with the previous
			// implementation, which folded the dotted names in this list onto
			// different locations. That divergence is the fix.
			if want := filepath.Join(b.basePath, p); got != want {
				t.Errorf("validatePath(%q) = %q, previous implementation gave %q", p, got, want)
			}
		})
	}
}

// TestValidatePathNeverEscapesRoot pays for the containment proof here instead
// of on every call. validatePath dropped its filepath.Rel check because a
// clean, relative, ".."-free key cannot escape by concatenation; this asserts
// that property over generated input rather than trusting the argument.
func TestValidatePathNeverEscapesRoot(t *testing.T) {
	b := newPathTestBackend(t)

	// Weighted so most generated keys are ACCEPTED, and so the accepted ones
	// are adversarial rather than trivially safe. A generator that mostly
	// produces rejects asserts almost nothing, because every reject is skipped.
	safe := []string{"a", "cpu", "..foo", "a..b", "...", "....", "foo..", "data.parquet", "_x", "x-y"}
	nasty := []string{"..", ".", "", "/", "x\x00y", "a/", "/a", "//", "a//b", `a\b`}
	rng := rand.New(rand.NewSource(1))

	accepted := 0
	distinct := make(map[string]struct{})

	for i := 0; i < 20000; i++ {
		n := 1 + rng.Intn(5)
		parts := make([]string, n)
		for j := range parts {
			// 85% safe segments, so the accepted population is large, with the
			// rest seeded from the traversal alphabet.
			if rng.Intn(100) < 85 {
				parts[j] = safe[rng.Intn(len(safe))]
			} else {
				parts[j] = nasty[rng.Intn(len(nasty))]
			}
		}
		key := strings.Join(parts, "/")

		got, err := b.validatePath(key)
		if err != nil {
			continue // rejected, so it never reaches the filesystem
		}
		accepted++
		distinct[key] = struct{}{}
		rel, relErr := filepath.Rel(b.basePath, got)
		if relErr != nil {
			t.Fatalf("accepted key %q resolved to %q, which is not relative to the root: %v", key, got, relErr)
		}
		// Compare on a separator boundary, not with a raw prefix: a directory
		// named "..foo" is inside the root and must not be read as an escape.
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatalf("accepted key %q escaped the root: resolved %q, relative %q", key, got, rel)
		}
		// The containment assertion is only worth as much as the equivalence
		// it rests on, so check both on every accepted key.
		if want := filepath.Join(b.basePath, key); got != want {
			t.Fatalf("accepted key %q resolved to %q, filepath.Join gives %q", key, got, want)
		}
	}

	// Guard against the generator drifting into rejecting almost everything,
	// which would leave this test asserting nothing while still passing.
	if accepted < 10000 || len(distinct) < 2000 {
		t.Fatalf("generator produced only %d accepted keys (%d distinct); the property is barely exercised", accepted, len(distinct))
	}
}

// TestStorageKeysAreInjective is #737's invariant at the layer that caused it.
func TestStorageKeysAreInjective(t *testing.T) {
	b := newPathTestBackend(t)

	// Pairs that the old fold collapsed onto one location.
	keys := []string{
		"default/a..b/x.parquet", "default/a_b/x.parquet",
		"default/..foo/x.parquet", "default/_foo/x.parquet",
		"rocket..01/x.parquet", "rocket_01/x.parquet",
	}

	seen := make(map[string]string, len(keys))
	for _, k := range keys {
		resolved, err := b.validatePath(k)
		if err != nil {
			t.Fatalf("validatePath(%q) = %v; all of these are legitimate keys", k, err)
		}
		if prev, clash := seen[resolved]; clash {
			t.Errorf("keys %q and %q both resolve to %q", prev, k, resolved)
		}
		seen[resolved] = k
	}
	if len(seen) != len(keys) {
		t.Errorf("%d keys resolved to %d locations", len(keys), len(seen))
	}
}

// TestValidateListPathMatchesFilepathJoin covers the two spellings that are
// legitimate for a prefix and not for a key.
func TestValidateListPathMatchesFilepathJoin(t *testing.T) {
	b := newPathTestBackend(t)
	for _, p := range []string{"", "databases/", "default/", "default/cpu", "default/cpu/"} {
		got, err := b.validateListPath(p)
		if err != nil {
			t.Fatalf("validateListPath(%q) = %v, want accepted", p, err)
		}
		if want := filepath.Join(b.basePath, p); got != want {
			t.Errorf("validateListPath(%q) = %q, want %q", p, got, want)
		}
	}
	for _, p := range []string{"/", "a//b", "a/../b", "a/./b", `a\b`, "a\x00b"} {
		if _, err := b.validateListPath(p); err == nil {
			t.Errorf("validateListPath(%q) was accepted", p)
		}
	}
}

// TestKeysAreInjective is the contract, stated as the property that matters.
//
// #741 asserted matching accept/reject sets, which cannot see this: every
// backend accepted both "coll" and "coll/", and on local they were ONE file.
// Two keys, one object, first payload silently lost. What differs between a
// correct and a broken implementation is the mapping, not the verdict, so the
// mapping is what this asserts.
func TestKeysAreInjective(t *testing.T) {
	b := newPathTestBackend(t)
	ctx := context.Background()

	// Every pair here named one location under some implementation.
	keys := []string{
		"coll", "coll/sub",
		"default/a..b/x.parquet", "default/a_b/x.parquet",
		"default/..foo/x.parquet", "default/_foo/x.parquet",
		"rocket..01/x.parquet", "rocket_01/x.parquet",
	}

	seen := make(map[string]string, len(keys))
	for _, k := range keys {
		resolved, err := b.validatePath(k)
		if err != nil {
			t.Fatalf("validatePath(%q) = %v; all of these are legitimate keys", k, err)
		}
		if prev, clash := seen[resolved]; clash {
			t.Errorf("keys %q and %q both resolve to %q", prev, k, resolved)
		}
		seen[resolved] = k
	}

	// And the spelling that actually collided: prove it through the backend,
	// not just the resolver, because the resolver is what was wrong.
	if err := b.Write(ctx, "coll2", []byte("no-slash")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Write(ctx, "coll2/", []byte("trailing-slash")); err == nil {
		got, _ := b.Read(ctx, "coll2")
		t.Fatalf("Write(%q) was accepted and Read(%q) now returns %q; two keys named one object", "coll2/", "coll2", got)
	}
}
