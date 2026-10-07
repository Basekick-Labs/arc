package backup

import (
	"context"
)

// countColdFilesExcluded records, on each leg, how many files its databases
// hold in the COLD tier (#1085 stage B3). A backup copies hot storage only, so
// a tiered deployment's backup is complete with respect to hot storage and
// this count is the size of the gap; stage C (#1086) closes it.
//
// Informational, so a failure WARNS and the run continues: the count describes
// data the backup was never going to carry, and failing the run would turn a
// healthy backup into no backup over a diagnostic. Where #1084's
// checkScopeKnown does fail the run, it decides whether the run has anything
// to back up at all; this annotates a run that is already complete. Only the
// backup layer logs — the metadata store returns the error and says nothing.
//
// One grouped query, so the count either happened for every database or for
// none: there is no per-database partial result to describe, and so no flag
// for one. It is still only THIS NODE's view of tier metadata — see the field
// comment on Manifest.ColdFilesExcluded for the cluster case where that view
// is lawfully low — so the log names the node.
//
// Attribution follows the same rule the DATA follows, run.legFor, so a
// database routed to the audit target has its gap reported on that target's
// manifest and nowhere else. A fully cold database has no data file to route,
// and routes by name through the same map instead: routeKey returns a bare
// name unchanged, and planRun creates a leg for every routed target from the
// CONFIG rather than from the files, so the leg exists either way.
//
// Called with the other pre-copy decisions and deliberately NOT inside the
// Iceberg guard beside it: this runs on unscoped runs and with Iceberg off.
func (m *Manager) countColdFilesExcluded(ctx context.Context, run *backupRun, sc *scope) {
	if m.coldCounter == nil {
		return
	}
	counts, err := m.coldCounter.CountColdFilesByDatabase(ctx)
	if err != nil {
		m.logger.Warn().Err(err).
			Str("backup_id", run.id).
			Msg("Could not count cold-tier files. The backup itself is unaffected and completes, but its manifest will not record how many cold-tier files it is not carrying")
		return
	}

	// The scope filter runs BEFORE legFor, not as a trim afterwards. On a
	// scoped run planRun only plans the targets the scope routes to, so a
	// database outside the scope has no leg of its own; routing its name
	// anyway would fall through to legFor's default leg and report one
	// database's gap against another's slice.
	var total int64
	var named int
	for db, n := range counts {
		if n <= 0 || !sc.has(db) {
			continue
		}
		leg := run.legFor(db)
		leg.manifest.ColdFilesExcluded += n
		if leg.manifest.ColdFilesExcludedDatabases == nil {
			leg.manifest.ColdFilesExcludedDatabases = map[string]int64{}
		}
		// Summed, not assigned: a name can be counted once here, but the field
		// is also merged across legs, and summing at both ends means a future
		// caller that counts a name twice over-reports rather than silently
		// dropping the first figure.
		leg.manifest.ColdFilesExcludedDatabases[db] += n
		total += n
		named++
	}
	if total == 0 {
		return
	}

	run.progress.ColdFilesExcluded = total
	m.setProgress(run.progress)
	ev := m.logger.Info().
		Str("backup_id", run.id).
		Int64("cold_files_excluded", total).
		Int("databases", named)
	// Only when there is one to name: an owner identity is minted for a
	// cluster and for a standalone node with a backup destination, but an
	// empty string in a field called "node" reads as a missing value.
	if m.instanceID != "" {
		ev = ev.Str("node", m.instanceID)
	}
	ev.Msg("Backup does not carry these cold-tier files: a backup copies hot storage only, and this node reports its own tier metadata")
}
