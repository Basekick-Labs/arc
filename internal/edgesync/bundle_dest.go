package edgesync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrDestinationRefused marks a bundle path an operator may not use.
var ErrDestinationRefused = errors.New("edgesync: bundle destination refused")

// DestinationPolicy decides which filesystem paths a bundle may use.
//
// Every other Arc write path goes through a storage backend, which confines it
// to the storage root. A bundle cannot: a USB mount is outside that root by
// definition, so an operator-supplied path reaches the filesystem directly.
//
// The endpoints are admin-only, so this is not a privilege boundary — an admin
// can already do worse. It guards against MISTAKES, one of which is not
// obvious: exporting into Arc's own storage root makes the next discovery pass
// find the exported copies and queue them for sync, which fans out on every
// export.
type DestinationPolicy struct {
	allowed     []string
	storageRoot string
}

// NewDestinationPolicy resolves the allow-list once, at startup.
//
// Resolving here rather than per-request means a symlink swapped later cannot
// change what a path means mid-flight, and an unresolvable entry is a startup
// error rather than a confusing runtime refusal.
func NewDestinationPolicy(allowedDirs []string, storageRoot string) (*DestinationPolicy, error) {
	p := &DestinationPolicy{}

	for _, d := range allowedDirs {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}
		resolved, err := resolveDir(d)
		if err != nil {
			return nil, fmt.Errorf("edgesync: bundle allowed directory %q: %w", d, err)
		}
		p.allowed = append(p.allowed, resolved)
	}

	if storageRoot != "" {
		// Best-effort: a storage root that does not resolve (a remote backend,
		// say) simply means there is no local root to protect.
		if resolved, err := resolveDir(storageRoot); err == nil {
			p.storageRoot = resolved
		}
	}
	return p, nil
}

// Enabled reports whether any destination is permitted.
func (p *DestinationPolicy) Enabled() bool { return len(p.allowed) > 0 }

// Resolve validates a requested path and returns its absolute, symlink-free form.
//
// The returned path is what callers must use — not the requested one — so a
// later component cannot re-resolve differently.
func (p *DestinationPolicy) Resolve(requested string) (string, error) {
	if len(p.allowed) == 0 {
		// Refuse rather than default to "anywhere". An unset allow-list means
		// the operator has not thought about where bundles may be written.
		return "", fmt.Errorf("%w: no allowed directories are configured", ErrDestinationRefused)
	}
	if requested == "" {
		return "", fmt.Errorf("%w: no path given", ErrDestinationRefused)
	}

	resolved, err := resolveDir(requested)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrDestinationRefused, err)
	}

	// The storage-root check comes first: it is the mistake with the least
	// obvious consequence, so it deserves the clearest error.
	if p.storageRoot != "" && isWithin(resolved, p.storageRoot) {
		return "", fmt.Errorf("%w: %s is inside Arc's storage root (%s); "+
			"the next discovery pass would find the exported files and queue them for sync",
			ErrDestinationRefused, resolved, p.storageRoot)
	}

	for _, dir := range p.allowed {
		if isWithin(resolved, dir) {
			return resolved, nil
		}
	}
	return "", fmt.Errorf("%w: %s is not under any configured allowed directory (%s)",
		ErrDestinationRefused, resolved, strings.Join(p.allowed, ", "))
}

// resolveDir makes a path absolute and symlink-free.
//
// EvalSymlinks is what stops a link inside an allowed directory from pointing
// somewhere that is not — the check must run on where a path actually LANDS,
// not on how it was spelled.
func resolveDir(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve %q: %w", path, err)
	}

	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("resolve %q: %w", path, err)
		}
		// The leaf may legitimately not exist yet — an export creates it. Walk
		// up to the nearest existing ancestor, resolve THAT, and re-attach the
		// remainder. Resolving only the existing part is the point: a symlink
		// can only exist on a path that exists.
		// Bounded. The walk terminates at the filesystem root either way, but
		// a destination whose parent chain is dozens of missing levels deep is
		// a typo or a hostile input, not a drive mount — and each level costs a
		// stat. Refusing early gives a clearer error than resolving a 2KB path.
		const maxMissingLevels = 32

		existing, remainder := abs, ""
		for depth := 0; ; depth++ {
			if depth > maxMissingLevels {
				return "", fmt.Errorf("resolve %q: more than %d missing directory levels; "+
					"a bundle destination should be at or just below an existing mount point",
					truncateForError(path), maxMissingLevels)
			}
			parent := filepath.Dir(existing)
			if parent == existing {
				return "", fmt.Errorf("resolve %q: no existing ancestor", truncateForError(path))
			}
			remainder = filepath.Join(filepath.Base(existing), remainder)
			existing = parent
			if _, statErr := os.Stat(existing); statErr == nil {
				break
			}
		}
		base, err := filepath.EvalSymlinks(existing)
		if err != nil {
			return "", fmt.Errorf("resolve %q: %w", path, err)
		}
		resolved = filepath.Join(base, remainder)
	}
	return filepath.Clean(resolved), nil
}

// isWithin reports whether p is dir or sits underneath it.
//
// Compared at a path-separator boundary, not by raw prefix: strings.HasPrefix
// alone matches "/data/wh-other" as being under "/data/wh", which is the #534
// bug class this codebase has already shipped once.
func isWithin(p, dir string) bool {
	if p == dir {
		return true
	}
	// The filesystem root already ends in a separator, so appending another
	// yields "//" and matches nothing — the fix for the mid-segment bug
	// shipping its own edge case. "/" is an unwise allow-list entry but a
	// plausible one on a locked-down appliance.
	if dir == string(os.PathSeparator) {
		return strings.HasPrefix(p, dir)
	}
	return strings.HasPrefix(p, dir+string(os.PathSeparator))
}
