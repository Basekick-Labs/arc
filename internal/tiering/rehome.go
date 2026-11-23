package tiering

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// A re-home job replaces one hot file with files in the directories its rows
// belong to (#1825, internal/api/rehome_swap.go). While it runs it owns the
// file and the files replacing it: tiering leaves them on the hot tier
// (heldElsewhere), the job refuses a file tiering has a copy of or is moving
// (CheckRehomeSource), and it writes the tier rows of the new files itself
// (RecordRehomedFiles), since nothing else registers them before the next
// scan.

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
