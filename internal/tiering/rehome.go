package tiering

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// A re-home job replaces one file with files in the directories its rows
// belong to (#1825, internal/api/rehome_swap.go). While it runs it owns the
// file and the files replacing it.
//
// A file on the hot tier: tiering leaves it and its replacements there
// (heldElsewhere), the job refuses a file tiering has a copy of or is moving
// (CheckRehomeSource), and it writes the tier rows of the new files itself
// (RecordRehomedFiles), since nothing else registers them before the next
// scan.
//
// A file on the cold tier: the job checks that it is cold and only cold
// (CheckRehomeColdSource), reads it from cold storage, writes the new files
// to the hot tier as above, and removes the old one from cold storage with
// its row (RemoveRehomedColdFile).

// SetOwnedFilter installs the files a re-home job owns. Call it before Start:
// the startup scan and the first cycle must already see a job found at start.
// The filter answers true for every path until the job journal has been read.
func (m *Manager) SetOwnedFilter(owned func(path string) bool) {
	if m != nil {
		m.owned = owned
	}
}

// ErrRehomeColdCopy reports a file a re-home job must not split: cold storage
// holds it, or a copy of it. Splitting the hot copy would leave the cold one
// to be read beside the files that replace it.
var ErrRehomeColdCopy = errors.New("the file is in cold storage, or a copy of it is")

// CheckRehomeSource refuses a hot file a re-home job must not take: one the
// migrator is moving, one whose tier row says cold or quarantined, one with
// an object at its key in cold storage, whatever its name, or any file when
// cold storage is configured and cannot be asked. Call it once the job owns
// the file: from then the migrator leaves it alone, so the answer holds.
func (m *Manager) CheckRehomeSource(ctx context.Context, path string) error {
	if m == nil || m.metadata == nil {
		return nil
	}
	row, err := m.metadata.GetFile(ctx, path)
	if err != nil {
		return fmt.Errorf("%w: %s: read its tier row: %v", ErrColdCheckFailed, path, err)
	}
	if row != nil && (row.Tier != TierHot || row.QuarantinedAt != nil) {
		return fmt.Errorf("%w: %s (its tier row says %s)", ErrRehomeColdCopy, path, row.Tier)
	}
	if m.ColdTierMissing() {
		return fmt.Errorf("%w: %s: cold storage is enabled and could not be reached at start", ErrColdCheckFailed, path)
	}
	if !m.coldTierUsable() {
		return nil // no cold tier: nothing can hold a copy
	}
	if !m.claims.claimDelete(path) {
		return fmt.Errorf("%w: %s", ErrFileMoving, path)
	}
	defer m.claims.releaseDelete(path)
	// Any name, not only a daily file's (hasColdCopy): a file migrated by an
	// earlier version, or restored to hot from a backup, has a twin too.
	size, err := m.coldBackend.StatFile(ctx, path)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrColdCheckFailed, path, err)
	}
	if size >= 0 {
		return fmt.Errorf("%w: %s", ErrRehomeColdCopy, path)
	}
	return nil
}

// CanRecord reports whether a data file at path can have a tier row: tiering
// reads a file's partition from its directory, and takes no year before 1970.
func (m *Manager) CanRecord(path string) error {
	if m == nil {
		return nil
	}
	_, err := m.parseFilePath(path)
	return err
}

// RehomedFile is a file a re-home job put in place on the hot tier.
type RehomedFile struct {
	Path      string
	SizeBytes int64
}

// RecordRehomedFiles records the files a re-home job put in place as hot.
// A path that already has a cold or quarantined row is left as it is. It is
// safe to repeat.
func (m *Manager) RecordRehomedFiles(ctx context.Context, files []RehomedFile) error {
	if m == nil || m.metadata == nil {
		return nil
	}
	type scope struct{ db, meas string }
	touched := map[scope]struct{}{}
	for _, f := range files {
		info, err := m.parseFilePath(f.Path)
		if err != nil {
			return fmt.Errorf("parse re-homed file path %q: %w", f.Path, err)
		}
		if _, err := m.metadata.recordHotFileIfNotCold(ctx, &FileMetadata{
			Path: f.Path, Database: info.Database, Measurement: info.Measurement,
			PartitionTime: info.PartitionTime, SizeBytes: f.SizeBytes, CreatedAt: time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("record re-homed file %q in tier metadata: %w", f.Path, err)
		}
		touched[scope{info.Database, info.Measurement}] = struct{}{}
	}
	// Reads decide from the rows which tiers a measurement is read from.
	for s := range touched {
		m.metadata.invalidateTierCache(s.db, s.meas)
	}
	return nil
}

// ForgetRehomedFiles removes the hot rows of files a re-home job has removed:
// the file it replaced, or, for a job that was undone, the files it had put
// in place. A row that says cold is not touched.
func (m *Manager) ForgetRehomedFiles(ctx context.Context, paths []string) error {
	if m == nil || m.metadata == nil {
		return nil
	}
	var first error
	for _, p := range paths {
		if _, err := m.metadata.DeleteFileInTier(ctx, p, TierHot, time.Time{}); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// CheckRehomeColdSource refuses a file in cold storage a re-home job must not
// take, and returns its size and the identity of the cold store it is in.
// The file must be cold and only cold: a usable cold tier, a tier row that
// says cold and is not quarantined, the object there, nothing at its key on
// the hot tier, and no DELETE rewrite of it beside it on either tier (one of
// the two holds rows that DELETE removed). Call it once the job owns the
// file, with no DELETE acting on the cold tier (the rewrite lock).
func (m *Manager) CheckRehomeColdSource(ctx context.Context, path string) (size int64, store string, err error) {
	if m == nil || m.metadata == nil {
		return 0, "", fmt.Errorf("%w: %s: tiered storage is not running", ErrColdCheckFailed, path)
	}
	if !m.coldTierUsable() {
		return 0, "", fmt.Errorf("%w: %s: there is no usable cold tier", ErrColdCheckFailed, path)
	}
	row, err := m.metadata.GetFile(ctx, path)
	if err != nil {
		return 0, "", fmt.Errorf("%w: %s: read its tier row: %v", ErrColdCheckFailed, path, err)
	}
	switch {
	case row == nil:
		return 0, "", fmt.Errorf("%w: %s has no tier row (run a tiering scan: POST /api/v1/tiering/scan)", ErrRehomeNotCold, path)
	case row.QuarantinedAt != nil:
		return 0, "", fmt.Errorf("%w: %s is quarantined", ErrRehomeNotCold, path)
	case row.Tier != TierCold:
		return 0, "", fmt.Errorf("%w: its tier row says %s and it is not on the hot tier", ErrRehomeNotCold, row.Tier)
	}
	if n, err := m.hotBackend.StatFile(ctx, path); err != nil {
		return 0, "", fmt.Errorf("%w: %s: look for a hot copy: %v", ErrColdCheckFailed, path, err)
	} else if n >= 0 {
		return 0, "", fmt.Errorf("%w: %s is on both tiers (a migration kept its hot copy); POST /api/v1/tiering/reconcile finishes it", ErrRehomeNotCold, path)
	}
	size, err = m.coldBackend.StatFile(ctx, path)
	if err != nil {
		return 0, "", fmt.Errorf("%w: %s: %v", ErrColdCheckFailed, path, err)
	}
	if size < 0 {
		return 0, "", fmt.Errorf("%w: %s has a cold tier row and is not in cold storage", ErrRehomeNotCold, path)
	}
	dir := path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		dir = path[:i]
	}
	family := storage.RewriteFamily(path)
	for _, b := range []storage.Backend{m.coldBackend, m.hotBackend} {
		siblings, err := b.List(ctx, dir+"/")
		if err != nil && !storage.IsNotFound(err) {
			return 0, "", fmt.Errorf("%w: %s: list %s: %v", ErrColdCheckFailed, path, dir, err)
		}
		for _, k := range siblings {
			if k != path && storage.RewriteFamily(k) == family {
				return 0, "", fmt.Errorf("%w: %s was rewritten by a DELETE and the two copies are both in storage (%s); run a DELETE on the measurement first, which removes the stale one", ErrRehomeNotCold, path, k)
			}
		}
	}
	return size, m.ColdBackendIdentity(), nil
}

// ErrRehomeNotCold reports a file a re-home job was asked to take from cold
// storage that is not simply a cold file.
var ErrRehomeNotCold = errors.New("the file cannot be moved from cold storage as it is")

// ErrColdStoreChanged reports a job's cold file that was recorded in another
// cold store than this node uses now.
var ErrColdStoreChanged = errors.New("the cold store is not the one the job recorded")

// RemoveRehomedColdFile removes a re-home job's old file from cold storage:
// the read caches are dropped, the object is deleted, cold storage is asked
// that it is gone, its tier row is retired, and the removal is announced.
// store is the cold store's identity as the job recorded it. Object before
// row: a row retired
// while the object survives is recorded again by the next cold sync, and the
// file's rows are read beside the files that replaced it. It is safe to
// repeat, and returns nil only when cold storage says the object is gone and
// the row is retired.
func (m *Manager) RemoveRehomedColdFile(ctx context.Context, path, store string) error {
	if m == nil || m.metadata == nil || !m.coldTierUsable() {
		return fmt.Errorf("%w: %s: there is no usable cold tier", ErrColdCheckFailed, path)
	}
	if now := m.ColdBackendIdentity(); now != store {
		return fmt.Errorf("%w: %s was in %q, this node's cold store is %q", ErrColdStoreChanged, path, store, now)
	}
	m.BeginColdRemoval()
	defer m.EndColdRemoval()
	// A read planned before this names the object through its cold glob;
	// one planned from here on is built without what the caches held.
	m.notifyReadCachesStale()
	if err := m.coldBackend.Delete(ctx, path); err != nil && !storage.IsNotFound(err) {
		return fmt.Errorf("remove %s from cold storage: %w", path, err)
	}
	if n, err := m.coldBackend.StatFile(ctx, path); err != nil {
		return fmt.Errorf("confirm %s is gone from cold storage: %w", path, err)
	} else if n >= 0 {
		return fmt.Errorf("%s is still in cold storage after its removal", path)
	}
	if _, err := m.RetireColdRows(ctx, []string{path}); err != nil {
		return fmt.Errorf("retire the cold tier row of %s: %w", path, err)
	}
	m.NotifyColdFilesRemoved([]string{path})
	return nil
}

// ColdSourceStat is the size of a re-home job's cold source, -1 when cold
// storage does not hold it. store is the identity the job recorded.
func (m *Manager) ColdSourceStat(ctx context.Context, path, store string) (int64, error) {
	if m == nil || !m.coldTierUsable() {
		return 0, fmt.Errorf("%w: %s: there is no usable cold tier", ErrColdCheckFailed, path)
	}
	if now := m.ColdBackendIdentity(); now != store {
		return 0, fmt.Errorf("%w: %s was in %q, this node's cold store is %q", ErrColdStoreChanged, path, store, now)
	}
	return m.coldBackend.StatFile(ctx, path)
}

// coldRetiredKept is how long a retired cold row is remembered: longer than
// a cold sync runs (tiered_storage.scan_timeout defaults to 2h). Past
// coldRetiredMax entries the oldest half is forgotten: a sync then records
// one of those again only if its listing predates a removal that long ago.
const (
	coldRetiredKept = 6 * time.Hour
	coldRetiredMax  = 200_000
)

// noteColdRetired remembers that the cold rows of paths are being retired.
func (m *Manager) noteColdRetired(paths []string) {
	if len(paths) == 0 {
		return
	}
	now := time.Now()
	m.coldRetiredMu.Lock()
	defer m.coldRetiredMu.Unlock()
	if m.coldRetired == nil {
		m.coldRetired = make(map[string]time.Time)
	}
	if len(m.coldRetired)+len(paths) > coldRetiredMax {
		cutoff := now.Add(-coldRetiredKept)
		for p, at := range m.coldRetired {
			if at.Before(cutoff) {
				delete(m.coldRetired, p)
			}
		}
		// Still too many: forget arbitrarily down to half.
		for p := range m.coldRetired {
			if len(m.coldRetired) <= coldRetiredMax/2 {
				break
			}
			delete(m.coldRetired, p)
		}
	}
	for _, p := range paths {
		m.coldRetired[p] = now
	}
}

// dropRetiredSince returns files without those whose cold row was retired at
// or after since. It reuses files' storage.
func (m *Manager) dropRetiredSince(files []FileMetadata, since time.Time) []FileMetadata {
	m.coldRetiredMu.Lock()
	defer m.coldRetiredMu.Unlock()
	if len(m.coldRetired) == 0 {
		return files
	}
	kept := files[:0]
	for _, f := range files {
		if at, ok := m.coldRetired[f.Path]; ok && !at.Before(since) {
			continue
		}
		kept = append(kept, f)
	}
	return kept
}
