package edgesync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDestinationPolicy_AllowsOnlyConfiguredDirs(t *testing.T) {
	allowed := t.TempDir()
	other := t.TempDir()

	p, err := NewDestinationPolicy([]string{allowed}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	if _, err := p.Resolve(allowed); err != nil {
		t.Errorf("the allowed directory itself was refused: %v", err)
	}
	// Compare resolved against resolved: on macOS /var is a symlink to
	// /private/var, so the policy's output is legitimately spelled differently
	// from t.TempDir()'s return value.
	resolvedAllowed, err := filepath.EvalSymlinks(allowed)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	sub := filepath.Join(allowed, "bundles", "today")
	if got, err := p.Resolve(sub); err != nil {
		t.Errorf("a subdirectory was refused: %v", err)
	} else if !strings.HasPrefix(got, resolvedAllowed) {
		t.Errorf("resolved to %q, outside %q", got, resolvedAllowed)
	}

	if _, err := p.Resolve(other); err == nil {
		t.Error("a directory outside the allow-list was accepted")
	}
}

// strings.HasPrefix alone matches "/data/wh-other" as being under "/data/wh".
// This codebase has shipped that bug once already (#534).
func TestDestinationPolicy_SiblingNameIsNotInside(t *testing.T) {
	parent := t.TempDir()
	allowed := filepath.Join(parent, "wh")
	sibling := filepath.Join(parent, "wh-other")
	for _, d := range []string{allowed, sibling} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	p, err := NewDestinationPolicy([]string{allowed}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, err := p.Resolve(sibling); err == nil {
		t.Errorf("%q was accepted as being inside %q", sibling, allowed)
	}
}

// The filesystem root already ends in a separator, so the boundary check must
// not append a second one — the fix for the mid-segment bug shipping its own
// edge case. An unwise allow-list entry, but a plausible one on an appliance.
func TestDestinationPolicy_FilesystemRootAllowsEverything(t *testing.T) {
	p, err := NewDestinationPolicy([]string{string(os.PathSeparator)}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, err := p.Resolve(t.TempDir()); err != nil {
		t.Errorf("allowed_dirs=[\"/\"] refused a path: %v", err)
	}
}

// Traversal must be judged on where a path LANDS, not how it is spelled.
func TestDestinationPolicy_RejectsTraversalOutOfTheAllowList(t *testing.T) {
	parent := t.TempDir()
	allowed := filepath.Join(parent, "media")
	if err := os.MkdirAll(allowed, 0o700); err != nil {
		t.Fatal(err)
	}

	p, err := NewDestinationPolicy([]string{allowed}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, err := p.Resolve(filepath.Join(allowed, "..", "escaped")); err == nil {
		t.Error("a path traversing out of the allow-list was accepted")
	}
}

// A symlink inside an allowed directory can point anywhere; the check must
// follow it rather than trust the spelling.
func TestDestinationPolicy_FollowsSymlinksBeforeDeciding(t *testing.T) {
	allowed := t.TempDir()
	outside := t.TempDir()

	link := filepath.Join(allowed, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	p, err := NewDestinationPolicy([]string{allowed}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, err := p.Resolve(link); err == nil {
		t.Error("a symlink pointing outside the allow-list was accepted")
	}
}

// Exporting into the storage root makes the next discovery pass find the
// exported copies and queue them for sync — fan-out on every export.
func TestDestinationPolicy_RefusesTheStorageRoot(t *testing.T) {
	parent := t.TempDir()
	storage := filepath.Join(parent, "arc-data")
	if err := os.MkdirAll(storage, 0o700); err != nil {
		t.Fatal(err)
	}

	// Deliberately allow the whole parent, so only the storage-root rule can refuse.
	p, err := NewDestinationPolicy([]string{parent}, storage)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	for _, dest := range []string{storage, filepath.Join(storage, "bundles")} {
		_, err := p.Resolve(dest)
		if err == nil {
			t.Errorf("%q was accepted despite being in the storage root", dest)
			continue
		}
		if !strings.Contains(err.Error(), "storage root") {
			t.Errorf("error for %q does not explain why: %v", dest, err)
		}
	}

	// A sibling of the storage root is fine.
	if _, err := p.Resolve(filepath.Join(parent, "media")); err != nil {
		t.Errorf("a directory beside the storage root was refused: %v", err)
	}
}

// An unset allow-list means the operator has not decided where bundles may be
// written. Defaulting to "anywhere" would be the wrong reading.
func TestDestinationPolicy_RefusesEverythingWhenUnconfigured(t *testing.T) {
	p, err := NewDestinationPolicy(nil, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if p.Enabled() {
		t.Error("an empty allow-list reports as enabled")
	}
	if _, err := p.Resolve(t.TempDir()); err == nil {
		t.Error("a path was accepted with no allow-list configured")
	}
}

// An export creates its own directory, so a not-yet-existing leaf under an
// allowed root must be permitted.
func TestDestinationPolicy_AllowsANotYetCreatedSubdirectory(t *testing.T) {
	allowed := t.TempDir()
	p, err := NewDestinationPolicy([]string{allowed}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, err := p.Resolve(filepath.Join(allowed, "does", "not", "exist", "yet")); err != nil {
		t.Errorf("a not-yet-created subdirectory was refused: %v", err)
	}
}

// A destination dozens of missing levels below an existing directory is a typo
// or a hostile input, not a drive mount. Each level costs a stat, and refusing
// early gives a clearer error than resolving a multi-kilobyte path.
func TestDestinationPolicy_RefusesAnAbsurdlyDeepPath(t *testing.T) {
	allowed := t.TempDir()
	p, err := NewDestinationPolicy([]string{allowed}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}

	deep := allowed
	for i := 0; i < 40; i++ {
		deep = filepath.Join(deep, "nope")
	}
	if _, err := p.Resolve(deep); err == nil {
		t.Error("a path 40 missing levels deep was accepted")
	}

	// A normal not-yet-created destination must still work.
	if _, err := p.Resolve(filepath.Join(allowed, "bundles")); err != nil {
		t.Errorf("a one-level destination was refused: %v", err)
	}
}
