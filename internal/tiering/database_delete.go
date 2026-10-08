package tiering

import (
	"context"
	"fmt"
	"sort"
)

// BeginDatabaseDelete reserves the tier lifecycle while a database is being
// removed. A migration or scan can otherwise copy a hot file to cold, or
// register a file, after the database deletion has already swept that tier.
// The caller must release the reservation on every exit path.
func (m *Manager) BeginDatabaseDelete() (func(), error) {
	if m == nil {
		return func() {}, nil
	}
	if !m.scanRunning.CompareAndSwap(false, true) {
		return nil, ErrScanRunning
	}
	if !m.cycleRunning.CompareAndSwap(false, true) {
		m.scanRunning.Store(false)
		return nil, ErrMigrationCycleRunning
	}
	return func() {
		m.cycleRunning.Store(false)
		m.scanRunning.Store(false)
	}, nil
}

// PrepareDatabaseDelete removes all manifest entries for a database before
// the API deletes any of its hot objects. The storage listing is included for
// paths not yet registered in the manifest; the manifest snapshot adds files
// held only by peers in a per-node-storage cluster.
func (m *Manager) PrepareDatabaseDelete(ctx context.Context, database string, hotPaths []string) error {
	if m == nil {
		return nil
	}
	paths := make(map[string]struct{}, len(hotPaths))
	for _, path := range hotPaths {
		paths[path] = struct{}{}
	}
	if m.manifest != nil {
		for _, path := range m.manifest.ManifestEntriesByDatabase(database) {
			paths[path] = struct{}{}
		}
	}

	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	if err := m.notifyHotFilesRemoved(ordered); err != nil {
		return fmt.Errorf("mark database files removed: %w", err)
	}
	for start := 0; start < len(ordered); start += manifestChunk {
		end := min(start+manifestChunk, len(ordered))
		if err := m.deleteFromManifest(ctx, ordered[start:end], manifestReasonDatabaseDelete); err != nil {
			return fmt.Errorf("remove database files from cluster manifest: %w", err)
		}
	}
	return nil
}

// CleanupDatabaseDelete removes cold objects and tier rows left after the API
// has deleted the hot files. Hot rows are retired only when their object was
// deleted or is already absent. Cold rows are retired only after the cold
// object is gone, so a failed delete remains queryable and can be retried.
// The returned count is the number of cold objects removed.
func (m *Manager) CleanupDatabaseDelete(ctx context.Context, database string, hotListed, hotFailed []string) (int, []error) {
	if m == nil || m.metadata == nil {
		return 0, nil
	}
	rows, err := m.metadata.GetFilesByDatabase(ctx, database)
	if err != nil {
		return 0, []error{fmt.Errorf("read tier rows for database %q: %w", database, err)}
	}

	listed := make(map[string]struct{}, len(hotListed))
	for _, path := range hotListed {
		listed[path] = struct{}{}
	}
	failed := make(map[string]struct{}, len(hotFailed))
	for _, path := range hotFailed {
		failed[path] = struct{}{}
	}

	cold := m.ColdBackend()
	var coldPaths []string
	var errs []error
	coldListOK := false
	if cold != nil {
		coldPaths, err = cold.List(ctx, database+"/")
		if err != nil {
			errs = append(errs, fmt.Errorf("list cold objects for database %q: %w", database, err))
		} else {
			coldListOK = true
		}
	}

	coldListed := make(map[string]struct{}, len(coldPaths))
	coldDeleted := make(map[string]struct{}, len(coldPaths))
	for _, path := range coldPaths {
		coldListed[path] = struct{}{}
		if err := cold.Delete(ctx, path); err != nil {
			// A timed-out response can follow a successful object deletion. Keep
			// the metadata only when the object is still present or its state is
			// unknown.
			size, statErr := cold.StatFile(ctx, path)
			if statErr == nil && size < 0 {
				coldDeleted[path] = struct{}{}
				continue
			}
			if statErr != nil {
				errs = append(errs, fmt.Errorf("delete cold object %q: %w (verify removal: %v)", path, err, statErr))
			} else {
				errs = append(errs, fmt.Errorf("delete cold object %q: %w", path, err))
			}
			continue
		}
		coldDeleted[path] = struct{}{}
	}

	removedCold := len(coldDeleted)
	removedRows := 0
	for _, row := range rows {
		removeRow := false
		switch row.Tier {
		case TierHot:
			if _, failedDelete := failed[row.Path]; failedDelete {
				continue
			}
			if _, wasListed := listed[row.Path]; wasListed {
				removeRow = true
			} else {
				size, statErr := m.hotBackend.StatFile(ctx, row.Path)
				if statErr != nil {
					errs = append(errs, fmt.Errorf("check hot object %q before retiring tier row: %w", row.Path, statErr))
					continue
				}
				if size < 0 {
					removeRow = true
				} else {
					errs = append(errs, fmt.Errorf("hot object %q remains after database listing", row.Path))
					continue
				}
			}
		case TierCold:
			if cold == nil {
				errs = append(errs, fmt.Errorf("cold backend is unavailable; retaining tier row for %q", row.Path))
				continue
			}
			if !coldListOK {
				continue
			}
			if _, remains := coldListed[row.Path]; !remains {
				removeRow = true
			} else if _, deleted := coldDeleted[row.Path]; deleted {
				removeRow = true
			}
		default:
			errs = append(errs, fmt.Errorf("unknown tier %q for %q; retaining tier row", row.Tier, row.Path))
			continue
		}

		if removeRow {
			if err := m.metadata.DeleteFile(ctx, row.Path); err != nil {
				errs = append(errs, fmt.Errorf("delete tier row for %q: %w", row.Path, err))
				continue
			}
			removedRows++
		}
	}

	if removedRows > 0 {
		m.notifyMigrationComplete(removedRows, 0)
	}
	return removedCold, errs
}
