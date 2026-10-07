package backup

// The run index (#1085 stage B2b-2).
//
// <backupID>/index.json on the DEFAULT target, written before any copy and
// finalised after every manifest. It is an INTENT record: one key that names
// every target the run touched.
//
// It closes the hole stage B2b-1 recorded on CreateBackup's manifest write. A
// run that died after a sidecar landed left objects nothing ENUMERATED: the
// listing keys on manifest.json and correctly did not show them, and
// DeleteBackup by ID still found and removed them — so an operator who WATCHED
// the run fail could clean up, and one who did not had objects at the
// destination no listing mentioned. With several targets that hole is wider,
// because the leftovers can be on a store the default listing never looks at.
//
// It is NOT load-bearing for restore. Every leg's manifest of a MULTI-LEG run
// carries RunTargets, so a manifest found alone is self-describing and a
// restore never depends on the default target being reachable to learn the
// run's shape; a manifest without the field is a single-destination run, where
// the one manifest found IS the whole run and there is nothing to learn. The
// index's job is ENUMERATION.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Run index statuses. Informational, and deliberately NOT read by the
// listing's incomplete-run predicate: the write that sets "complete" is a
// second failure point carrying nothing the predicate uses, so a stale
// "started" with every manifest present must not read as aborted. The
// predicate keys on manifest presence alone.
const (
	runIndexStarted  = "started"
	runIndexComplete = "complete"
)

// RunIndex is one run's intent record.
type RunIndex struct {
	BackupID  string    `json:"backup_id"`
	CreatedAt time.Time `json:"created_at"`
	// OwnerInstanceID is this instance's backup identity, so an index for
	// another instance's aborted run is filtered out of the listing the same
	// way its manifests would be (see identity.go).
	OwnerInstanceID string `json:"owner_instance_id,omitempty"`
	// Scope mirrors the manifest's: the databases a scoped run was limited to.
	Scope []string `json:"scope,omitempty"`
	// DefaultTarget is "" and Targets empty for the backup.local_path
	// destination that predates targets, which readers interpret as "the
	// single configured destination". Written on EVERY run, including that
	// one, so there is one shape to read.
	DefaultTarget string   `json:"default_target"`
	Targets       []string `json:"targets"`
	// DatabaseTargets is the routing that was in effect, database → target, so
	// an operator reading an aborted run's index knows which store holds which
	// database's leftovers.
	DatabaseTargets map[string]string `json:"database_targets,omitempty"`
	Status          string            `json:"status"`
}

// indexPath is the run index's key, under the backup's own prefix so
// DeleteBackup's prefix listing sweeps it with everything else.
func indexPath(backupID string) string { return backupID + "/index.json" }

// writeRunIndex records the run's intent before anything is copied.
//
// A failure here FAILS THE RUN. Nothing has been copied at that point, so the
// failure is clean and there is nothing to compensate — and a run with no
// index is exactly the run whose leftovers nothing can enumerate, which is the
// hole this closes.
func (m *Manager) writeRunIndex(ctx context.Context, dest backupTarget, index *RunIndex) error {
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal the backup index: %w", err)
	}
	// Bounded: the index is small and fixed-size, so a minute is generous and
	// a stalled destination must not hold the operation lock for the whole run
	// budget. See destinationProbeTimeout.
	writeCtx, cancel := withDestinationTimeout(ctx)
	err = dest.backend.Write(writeCtx, indexPath(index.BackupID), data)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to write the backup index to %s: %w", dest.describe(), err)
	}
	return nil
}

// finaliseRunIndex rewrites the index with status "complete".
//
// A failure is a WARNING, never a failed run, and that is the asymmetry with
// writeRunIndex: every manifest has landed by the time this runs, so the
// backup is complete and restorable, and reporting it as failed would send an
// operator looking for a gap that does not exist. What a stale "started"
// costs is nothing, because the listing's predicate keys on manifest presence
// and never on this field.
func (m *Manager) finaliseRunIndex(ctx context.Context, dest backupTarget, index *RunIndex) {
	index.Status = runIndexComplete
	if err := m.writeRunIndex(ctx, dest, index); err != nil {
		m.logger.Warn().Err(err).Str("backup_id", index.BackupID).
			Msg("Could not mark the backup index complete; every manifest landed, so the backup is complete and the listing reads it from the manifests, not from this field")
	}
}

// findRunIndex reads one run's index from the first configured target that has
// it, or nil when no reachable target does.
//
// The index is WRITTEN only to the default target, so a single read of the
// default is what a healthy destination needs. This looks wider because the
// default can MOVE: an operator who adds a second target and re-points
// backup.default_target leaves every earlier run's index on the old one, and a
// reader of only the current default would silently stop enumerating exactly
// the leftovers this file exists to find. A transport failure is skipped rather
// than returned: the caller is enumerating, and one dead store must not hide
// the runs the others can account for.
func (m *Manager) findRunIndex(ctx context.Context, backupID string) *RunIndex {
	for _, t := range m.configuredTargets() {
		index, err := m.readRunIndex(ctx, t, backupID)
		if err != nil || index == nil {
			continue
		}
		return index
	}
	return nil
}

// readRunIndex reads one run's index, or nil when the run has none: a backup
// taken before this stage, or one whose index write is the thing that failed.
// A transport failure is returned as an error so a caller can tell the two
// apart.
func (m *Manager) readRunIndex(ctx context.Context, dest backupTarget, backupID string) (*RunIndex, error) {
	readCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	data, err := dest.backend.Read(readCtx, indexPath(backupID))
	if err != nil {
		if storage.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read the index of backup %s from %s: %w", backupID, dest.describe(), err)
	}
	index := &RunIndex{}
	if err := json.Unmarshal(data, index); err != nil {
		return nil, fmt.Errorf("failed to decode the index of backup %s: %w", backupID, err)
	}
	return index, nil
}
