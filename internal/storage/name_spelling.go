package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Name spelling on volumes that fold names (#1509).
//
// The local backend builds paths from database and measurement names as
// given. On a volume that folds names (macOS APFS by default, Docker Desktop
// bind mounts from a Mac, NTFS), `cpu` and `CPU` are one directory: a write,
// query or delete under either spelling acts on the same files, while RBAC
// matches names exactly. The guard here makes a name reach data only in the
// spelling it has on disk. Whether two names are the same is left to the
// filesystem (a stat), never decided by folding in Go, so Unicode folds the
// volume applies are covered as well as ASCII case.
//
// On a volume that does not fold names the guard is never armed; armed there
// anyway (the probe failed), it can never refuse, because a stat that finds
// an entry there means an entry with exactly that name exists.

// NameSpellingGuard is implemented by backends whose paths can alias names
// that differ only in spelling. Object stores and case-sensitive local
// volumes do not alias, so callers type-assert and skip when it is absent.
type NameSpellingGuard interface {
	// CheckNameSpelling refuses a name whose segments exist on disk under
	// another spelling. A segment that does not exist is accepted, along
	// with everything beneath it.
	CheckNameSpelling(segments ...string) error
	// AdmitNameSpelling is CheckNameSpelling for a write that may create
	// the database and measurement: it creates their directories, so the
	// filesystem, not a check that races another first write, decides which
	// spelling exists.
	AdmitNameSpelling(database, measurement string) error
}

// NameSpellingError refuses a name that exists on disk in another spelling.
type NameSpellingError struct {
	What      string // "database" or "measurement"
	Requested string
	OnDisk    string
}

func (e *NameSpellingError) Error() string {
	return fmt.Sprintf("%s %q exists in storage as %q: this storage volume does not distinguish names that differ only in case, so use the existing spelling", e.What, e.Requested, e.OnDisk)
}

// IsNameSpellingError reports whether err is a NameSpellingError.
func IsNameSpellingError(err error) bool {
	var e *NameSpellingError
	return errors.As(err, &e)
}

// listingTrustAge is how long after a directory's mtime a listing must have
// been taken to be trusted while that mtime is unchanged. A remove and a
// create landing within one mtime tick leave the mtime unchanged; volumes
// with coarse timestamps (exFAT keeps 10 ms to 2 s) make that window real.
// A listing taken within it is never trusted, however old it grows.
const listingTrustAge = 2 * time.Second

// nameSpelling is the guard's state on a LocalBackend.
type nameSpelling struct {
	armed bool
	folds nameFolds

	mu       sync.Mutex
	listings map[string]dirListing // by directory path
	admitMu  sync.Mutex
}

type dirListing struct {
	mod     time.Time
	takenAt time.Time
	names   map[string]struct{}
}

// maxSpellingListings bounds the listing cache: one entry per directory the
// guard looked in, which is the storage root and one per database.
const maxSpellingListings = 4096

// nameFolds is which folds the filesystem under the storage root applies.
type nameFolds struct{ Case, Normalization bool }

// probeNamesFold reports which folds the filesystem under dir applies to
// names: letter case, Unicode normalization (APFS folds it even when
// case-sensitive), both or neither. It mirrors edgesync.ProbeNameFolding.
// On error it answers both: the guard is a no-op on a volume that folds
// nothing.
func probeNamesFold(dir string) (nameFolds, error) {
	all := nameFolds{Case: true, Normalization: true}
	scratch, err := os.MkdirTemp(dir, ".arc-namefold-")
	if err != nil {
		return all, fmt.Errorf("probe name folding in %s: %w", dir, err)
	}
	defer os.RemoveAll(scratch)
	var f nameFolds
	for i, pair := range [][2]string{
		{"case-probe", "CASE-PROBE"},
		{"norm-\u00e9", "norm-e\u0301"}, // NFC, then NFD
	} {
		if err := os.WriteFile(filepath.Join(scratch, pair[0]), nil, 0o600); err != nil {
			return all, fmt.Errorf("probe name folding in %s: %w", dir, err)
		}
		_, err := os.Stat(filepath.Join(scratch, pair[1]))
		switch {
		case err == nil:
			if i == 0 {
				f.Case = true
			} else {
				f.Normalization = true
			}
		case !errors.Is(err, os.ErrNotExist):
			return all, fmt.Errorf("probe name folding in %s: %w", dir, err)
		}
	}
	return f, nil
}

// NamesFold reports whether the guard is armed: the storage root folds
// names (or the probe could not tell).
func (b *LocalBackend) NamesFold() bool { return b.spelling.armed }

// CaseFolds reports whether the storage root folds letter case.
func (b *LocalBackend) CaseFolds() bool { return b.spelling.folds.Case }

// CheckNameSpelling implements NameSpellingGuard.
func (b *LocalBackend) CheckNameSpelling(segments ...string) error {
	if !b.spelling.armed {
		return nil
	}
	_, err := b.checkSpelling(segments)
	return err
}

// AdmitNameSpelling implements NameSpellingGuard.
func (b *LocalBackend) AdmitNameSpelling(database, measurement string) error {
	if !b.spelling.armed {
		return nil
	}
	missing, err := b.checkSpelling([]string{database, measurement})
	if err != nil || !missing {
		return err
	}
	// A segment does not exist yet. Create both directories, one first
	// write at a time, and check again: if another spelling won (another
	// writer created it between the check and the mkdir), MkdirAll lands
	// in that directory and the second check refuses.
	b.spelling.admitMu.Lock()
	defer b.spelling.admitMu.Unlock()
	if err := os.MkdirAll(filepath.Join(b.basePath, database, measurement), 0o700); err != nil {
		return fmt.Errorf("create %s/%s: %w", database, measurement, err)
	}
	_, err = b.checkSpelling([]string{database, measurement})
	return err
}

// checkSpelling walks segments from the storage root. missing reports that a
// segment does not exist (nothing beneath it is checked).
func (b *LocalBackend) checkSpelling(segments []string) (missing bool, err error) {
	what := [...]string{"database", "measurement"}
	parent := b.basePath
	for i, seg := range segments {
		exists, onDisk, err := b.spelling.entry(parent, seg)
		if err != nil {
			return false, err
		}
		if !exists {
			return true, nil
		}
		if onDisk != seg {
			kind := "name"
			if i < len(what) {
				kind = what[i]
			}
			return false, &NameSpellingError{What: kind, Requested: seg, OnDisk: onDisk}
		}
		parent = filepath.Join(parent, seg)
	}
	return false, nil
}

// entry reports whether parent holds an entry the filesystem resolves seg
// to, and that entry's spelling on disk.
func (s *nameSpelling) entry(parent, seg string) (exists bool, onDisk string, err error) {
	// A stat can find an entry the listing taken just before it lacks:
	// another first write created it in between. The second pass lists again
	// after that stat, so an entry it still does not hold was renamed or
	// replaced, not merely late.
	for attempt := 0; attempt < 2; attempt++ {
		exists, onDisk, unlisted, err := s.entryOnce(parent, seg)
		if err != nil || !unlisted {
			return exists, onDisk, err
		}
	}
	// Resolved, but to no listed entry: refuse rather than accept a spelling
	// the listing does not hold.
	return true, "(another spelling)", nil
}

// entryOnce is one listing pass of entry. unlisted reports that the
// filesystem resolved seg but the listing holds no entry it resolves to.
func (s *nameSpelling) entryOnce(parent, seg string) (exists bool, onDisk string, unlisted bool, err error) {
	// Stat, not Lstat: a symlinked storage root's own mtime never changes.
	st, err := os.Stat(parent)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, "", false, nil
		}
		return false, "", false, err
	}
	mod := st.ModTime()
	s.mu.Lock()
	l, ok := s.listings[parent]
	s.mu.Unlock()
	if ok && l.mod.Equal(mod) && l.takenAt.Sub(mod) >= listingTrustAge {
		if _, ok := l.names[seg]; ok {
			return true, seg, false, nil
		}
	}
	takenAt := time.Now()
	entries, err := os.ReadDir(parent)
	if err != nil {
		return false, "", false, err
	}
	names := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		names[e.Name()] = struct{}{}
	}
	s.mu.Lock()
	if len(s.listings) >= maxSpellingListings {
		clear(s.listings)
	}
	s.listings[parent] = dirListing{mod: mod, takenAt: takenAt, names: names}
	s.mu.Unlock()
	if _, ok := names[seg]; ok {
		return true, seg, false, nil
	}
	// Not listed under this spelling: does the filesystem resolve it to
	// another entry?
	target, err := os.Lstat(filepath.Join(parent, seg))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, "", false, nil
		}
		return false, "", false, err
	}
	// Likely candidates first (a Unicode case fold), then every entry, for
	// folds strings.EqualFold does not apply (normalization).
	for _, pass := range [2]bool{true, false} {
		for _, e := range entries {
			if pass != strings.EqualFold(e.Name(), seg) {
				continue
			}
			if info, err := os.Lstat(filepath.Join(parent, e.Name())); err == nil && os.SameFile(info, target) {
				return true, e.Name(), false, nil
			}
		}
	}
	return true, "", true, nil
}
