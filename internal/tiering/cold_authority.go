package tiering

// Whether this node's cold tier rows can stand in for a listing of the cold
// store (#1550).
//
// When the cold tier cannot be listed, the query path may drop the cold
// partitions of a read whose range has no cold rows, so a hot-only query
// keeps working through a cold-tier outage. That is only safe where the rows
// are a complete record of what the cold store holds for the range:
//
//   - Not on a cluster node. A node learns of other nodes' cold files through
//     replication events and the cold-tier metadata sync, and several of those
//     paths record nothing while the cold store is unreachable (#1552).
//   - Not after a metadata restore. restore_metadata replaces tier_files with
//     the backup's snapshot (internal/backup/restore.go), so every file
//     migrated since the backup is in the cold store with no row.
//   - Not after a data restore that could not record some of its cold rows.
//
// The last two are persisted as a marker file next to the SQLite database,
// so a restart before the cold store is reachable again does not forget
// them. Only a cold-tier metadata sync that recorded a row for every object
// it listed clears the marker.

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

// ColdMetadataUnverifiedSuffix names the marker file, next to the SQLite
// database that holds tier_files, recording that this node's cold tier rows
// may be missing files the cold store holds.
const ColdMetadataUnverifiedSuffix = ".cold-metadata-unverified"

// ColdMetadataMarkerPath is the marker path for the SQLite database at
// dbPath, or "" without one.
func ColdMetadataMarkerPath(dbPath string) string {
	if dbPath == "" {
		return ""
	}
	return dbPath + ColdMetadataUnverifiedSuffix
}

// MarkColdMetadataUnverified writes the marker for the database at dbPath.
// Used at boot when a staged metadata restore replaced that database, before
// the tiering manager opens it.
func MarkColdMetadataUnverified(dbPath, reason string) error {
	path := ColdMetadataMarkerPath(dbPath)
	if path == "" {
		return nil
	}
	return writeColdMarker(path, reason)
}

func writeColdMarker(path, reason string) error {
	if err := os.WriteFile(path, []byte(reason+"\n"), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// Reasons reported by ColdMetadataAuthority.
const (
	coldAuthorityReasonCluster = "cluster node: cold rows arrive from other nodes and can lag the cold store"
	ColdMetadataReasonRestored = "tier metadata was restored from a backup"
	coldAuthorityReasonRecord  = "a restore could not record some of its cold rows"
)

// coldAuthority is the persisted "rows may be missing" state of one manager.
type coldAuthority struct {
	mu         sync.Mutex
	markerPath string
	unverified bool
	reason     string
	// gen counts markings. A sync clears the marker only when no marking
	// happened while it ran: rows a concurrent restore failed to record were
	// not in the sync's listing snapshot.
	gen uint64
}

// load reads the marker, if any.
func (a *coldAuthority) load(markerPath string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.markerPath = markerPath
	if markerPath == "" {
		return nil
	}
	b, err := os.ReadFile(markerPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		// Unreadable is not absent: treat it as set.
		a.unverified = true
		a.reason = ColdMetadataReasonRestored
		return fmt.Errorf("read %s: %w", markerPath, err)
	}
	a.unverified = true
	a.reason = strings.TrimSpace(string(b))
	if a.reason == "" {
		a.reason = ColdMetadataReasonRestored
	}
	return nil
}

// generation is the marking count, read at the start of a cold sync.
func (a *coldAuthority) generation() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.gen
}

// state is the current marking.
func (a *coldAuthority) state() (unverified bool, reason string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.unverified, a.reason
}

// mark records that rows may be missing, in memory always and on disk when
// there is a marker path. A failed write is returned; the in-memory state is
// set regardless, so this process stays conservative.
func (a *coldAuthority) mark(reason string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.gen++
	a.unverified = true
	a.reason = reason
	if a.markerPath == "" {
		return nil
	}
	return writeColdMarker(a.markerPath, reason)
}

// clear removes the marking after a complete cold sync that started at
// generation startGen. It reports whether the state was cleared; a marking
// that happened during the sync, or a marker file that cannot be removed,
// keeps it.
func (a *coldAuthority) clear(startGen uint64) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.unverified {
		return false, nil
	}
	if a.gen != startGen {
		return false, nil
	}
	if a.markerPath != "" {
		if err := os.Remove(a.markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("remove %s: %w", a.markerPath, err)
		}
	}
	a.unverified = false
	a.reason = ""
	return true, nil
}

// ColdMetadataAuthority reports whether this node's cold tier rows are a
// complete record of the cold store, so a read whose cold listing failed may
// trust them to say a range holds no cold data, and if not, why (#1550).
// Nil-receiver safe.
func (m *Manager) ColdMetadataAuthority() (authoritative bool, reason string) {
	if m == nil {
		return false, "tiering is not running"
	}
	if m.manifest != nil {
		return false, coldAuthorityReasonCluster
	}
	if unverified, why := m.coldAuth.state(); unverified {
		return false, why + "; cleared by the next cold-tier metadata sync that records every cold object"
	}
	return true, ""
}

// ColdMetadataAuthoritative is ColdMetadataAuthority without the reason.
func (m *Manager) ColdMetadataAuthoritative() bool {
	ok, _ := m.ColdMetadataAuthority()
	return ok
}

// markColdMetadataUnverified records that cold rows may be missing and logs
// a marker that could not be persisted.
func (m *Manager) markColdMetadataUnverified(reason string) {
	if err := m.coldAuth.mark(reason); err != nil {
		m.logger.Error().Err(err).
			Msg("Could not persist that cold tier rows may be incomplete; this process still treats them as incomplete, a restart before the next complete cold sync will not")
	}
}

// coldSyncCompleted is called by a cold-tier metadata sync that listed the
// whole cold store and returned no error. incomplete counts the objects it
// listed but could not record.
func (m *Manager) coldSyncCompleted(startGen uint64, incomplete int) {
	if incomplete > 0 {
		if unverified, _ := m.coldAuth.state(); unverified {
			m.logger.Warn().Int("objects_not_recorded", incomplete).
				Msg("Cold tier metadata sync could not record every cold object; cold tier rows stay marked incomplete")
		}
		return
	}
	cleared, err := m.coldAuth.clear(startGen)
	if err != nil {
		m.logger.Error().Err(err).Msg("Could not remove the incomplete-cold-rows marker after a complete cold sync; cold tier rows stay marked incomplete")
		return
	}
	if cleared {
		m.logger.Info().Msg("Cold tier metadata sync recorded every cold object; cold tier rows are complete again")
	}
}
