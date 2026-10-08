package backup

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// countColdFilesExcluded records, on each leg, how many tier rows name data
// this backup does not hold (#1085 stage B3, retargeted by #1086 stage C).
//
// With a cold tier this node can read, the backup carries the cold objects, so
// the only rows it cannot carry are the ones whose object is missing from the
// store — data that is genuinely gone. With no cold tier, every cold row
// qualifies, which is what this counted when stage B introduced it.
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
// gap, when non-nil, is the cold walk's reconciliation: with a cold source
// wired the marker counts only the rows whose object is MISSING, because every
// row that had an object is now in the backup (#1086 stage C). Without one it
// keeps its stage-B meaning — every cold row, since none of them is carried.
// One sentence covers both: "tier rows whose data this backup does not carry".
func (m *Manager) countColdFilesExcluded(ctx context.Context, run *backupRun, sc *scope, gap *coldWalk) {
	// The gap branch is checked FIRST, before coldCounter. With a cold source
	// wired the walk has already computed the marker and no counter is needed
	// for it; gating on coldCounter would mean a node given SetColdSource
	// without SetColdCounter recorded no gap at all. cmd/arc wires both
	// together, so that is latent rather than live — but the gap is now the
	// primary path and must not depend on the other interface.
	if gap != nil && m.coldBackendOrNil() != nil {
		// The walk already reconciled rows against the listing, so the gap it
		// computed IS the marker. Counting rows again here would report every
		// cold row as excluded on a backup that just carried them.
		run.def.manifest.ColdFilesExcluded = gap.rowsWithoutObject
		// The per-database breakdown stays populated, because an operator
		// reading a total still needs to know WHICH database lost data, and
		// tooling built on the stage-B field must not silently start seeing
		// nothing. Attributed per leg by the same routing rule the data
		// follows, so the gap lands with its database's slice.
		for db, n := range gap.gapByDatabase {
			leg := run.legFor(db)
			if leg.manifest.ColdFilesExcludedDatabases == nil {
				leg.manifest.ColdFilesExcludedDatabases = map[string]int64{}
			}
			leg.manifest.ColdFilesExcludedDatabases[db] += n
			if leg != run.def {
				leg.manifest.ColdFilesExcluded += n
			}
		}
		if gap.rowsWithoutObject > 0 {
			run.progress.ColdFilesExcluded = gap.rowsWithoutObject
			m.setProgress(run.progress)
			m.logger.Warn().
				Str("backup_id", run.id).
				Int64("cold_files_excluded", gap.rowsWithoutObject).
				Msg("Cold-tier rows have no object in the cold store, so this backup could not carry them: the data they name is gone, not merely untiered")
		}
		return
	}
	// The stage-B fallback: no cold source, so nothing was carried and the
	// marker is every cold row. Needs the counter, and the guard the reorder
	// above moved past has to live here instead.
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

// coldBackendOrNil is this node's cold store, or nil when it has none:
// without tiering, with cold disabled, or when the cold backend failed to
// build. One accessor so no caller has to remember the nil-on-nil-source step.
func (m *Manager) coldBackendOrNil() storage.Backend {
	if m.coldSource == nil {
		return nil
	}
	return m.coldSource.ColdBackend()
}

// coldWalk is the cold-tier half of a backup's enumeration (#1086 stage C): the
// objects to copy, plus the two reconciliation counts.
type coldWalk struct {
	// objects are the cold files this run copies, already scope-filtered and
	// already deduped against the hot set.
	objects []storage.ObjectInfo
	// unrecorded counts listed cold objects this node has no tier row for.
	// They are COPIED anyway — see walkColdTier.
	unrecorded int64
	// rowsWithoutObject counts tier rows whose object the cold listing did not
	// return: data that is genuinely gone, and what cold_files_excluded
	// reports once a cold source is wired.
	rowsWithoutObject int64
	// gapByDatabase breaks rowsWithoutObject down by database, so the marker
	// keeps the per-database detail stage B shipped.
	gapByDatabase map[string]int64
	// dedupSkipped counts paths the hot set also held, dropped from the hot
	// side in favour of cold.
	dedupSkipped int64
	// dedupPaths are those paths, so the caller can remove them from the hot
	// set it has already built.
	dedupPaths map[string]struct{}
	// rowsStaleButHot counts tier rows that say cold, have no cold object, and
	// whose file this run is carrying from HOT storage anyway. Not a gap: the
	// data is in the backup. It is a metadata disagreement worth surfacing,
	// and keeping it out of rowsWithoutObject is what stops the run reporting
	// a file it holds as permanently gone.
	rowsStaleButHot int64
	// carried is every path the cold set is copying, so the end-of-run
	// manifest recheck can tell that a path the cluster manifest no longer
	// lists was nonetheless backed up — from cold.
	carried map[string]struct{}
	// backend is the cold store the objects were listed from, carried here so
	// the copy phase does not have to re-derive it and depend on the implicit
	// invariant that a non-empty cold set implies a non-nil source. Nil when
	// there was no cold tier, in which case objects is empty.
	backend storage.Backend
}

// walkColdTier lists this node's cold tier for the run's scope and reconciles
// it against the tier rows.
//
// THE LISTING IS AUTHORITATIVE AND THE ROWS ARE A CROSS-CHECK — the inverse of
// what the hot path does, deliberately. crossCheckManifest treats the Raft
// manifest as authoritative and holds back a listed file the manifest lacks,
// because on a cluster this node's listing can be a stale or partial view of a
// store several nodes write. The cold listing is not that: it is this node
// reading the cold store itself, which is direct evidence the object exists.
// So:
//
//   - object listed, row present — copy it. The ordinary case.
//   - object listed, NO row — copy it anyway and count it. The object is
//     there; a missing row is a metadata gap, and refusing would mean a node
//     whose rows are incomplete backs up NO cold data at all, which is the
//     failure this stage exists to fix.
//   - row present, NO object — count it as the gap. The data is genuinely
//     missing, and this is what cold_files_excluded becomes.
//
// One place the inverse-authority rule is deliberately NOT applied yet: the
// known-database check that admits a scoped name (checkScopeKnown, scope.go)
// recognises a fully cold database by its TIER ROWS and never consults the
// cold listing. So a scoped backup of a fully cold database whose rows are
// missing is refused as unknown even though the cold store holds its objects.
// Usually rescued by a surviving _schema/<db>/ anchor, which is why it is left
// alone here rather than widened into the scope check.
//
// READ-ONLY on tier metadata. It does not record a row for an unrecorded
// object even though the tiering cold sync does exactly that, because a backup
// must not mutate the state it is reporting on.
//
// And note when an unrecorded object is NOT a transient lag: the tiering cold
// sync only runs on a cluster with shared storage or replication, so on a
// standalone node and on a local-storage cluster without replication there is
// nothing that will ever record the row. There the count is a permanent
// condition and the operator needs to know, rather than being told to wait for
// a sync that does not run.
func (m *Manager) walkColdTier(ctx context.Context, sc *scope, hot map[string]struct{}) (*coldWalk, error) {
	w := &coldWalk{dedupPaths: map[string]struct{}{}, carried: map[string]struct{}{}}
	if m.coldSource == nil {
		return w, nil
	}
	cold := m.coldSource.ColdBackend()
	if cold == nil {
		return w, nil
	}
	w.backend = cold
	lister, ok := cold.(storage.ObjectLister)
	if !ok {
		return nil, errors.New("backup failed: the cold tier backend does not support ListObjects, so the cold-tier objects this backup is meant to carry cannot be enumerated")
	}

	objects, err := m.listColdObjects(ctx, lister, sc)
	if err != nil {
		return nil, err
	}
	// Every non-quarantined cold row, not just the scope's. Two columns and no
	// sort, but still the whole tier: a scoped backup of one small database
	// pays for all of it. Left that way deliberately — scoping this in SQL
	// needs an IN list or a LIKE per name, which for a 256-name scope is worse
	// than one sequential read, and the rows are only path and size. The cost
	// to watch is that it holds the one shared SQLite connection while it
	// runs, so auth, audit and tier registration wait; see
	// ColdFilePathsAndSizes.
	rows, err := m.coldSource.ColdRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("backup failed: could not read the cold tier metadata to reconcile against the cold listing: %w", err)
	}

	seen := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		p := filepath.ToSlash(obj.Path)
		seen[p] = struct{}{}
		if _, hotHas := hot[p]; hotHas {
			// Mid-migration: a migration copies to cold BEFORE deleting the
			// manifest entry and the hot copy, so one path can be in both
			// listings. The COLD copy wins. The cold object is canonical and
			// nothing in Arc ever deletes one, so it cannot vanish mid-run —
			// the hot side can, and if it does, the end-of-run manifest
			// recheck deletes the hot copy from the backup and the file ends
			// up in neither set. Taking cold also agrees with the tier row,
			// which the migration already flipped before releasing the hot
			// copy.
			w.dedupPaths[p] = struct{}{}
			w.dedupSkipped++
		}
		if _, hasRow := rows[p]; !hasRow {
			w.unrecorded++
		}
		w.objects = append(w.objects, obj)
		w.carried[p] = struct{}{}
	}
	for p := range rows {
		if _, ok := seen[p]; ok {
			continue
		}
		if !sc.ownsData(p) {
			continue
		}
		// A row with no cold object whose file is sitting in HOT storage is
		// NOT missing data: this run is carrying it from the hot set. Counting
		// it would report "the data they name is gone" about a file that is in
		// the backup, and that state is both reachable and permanent on a
		// non-primary node — the cold sync never downgrades a cold row, and
		// the reconciliation that would revert one is role gated. Counted
		// apart so the condition is still visible.
		if _, hotHas := hot[p]; hotHas {
			w.rowsStaleButHot++
			continue
		}
		w.rowsWithoutObject++
		db, _ := parseDBMeasurement(p)
		if db != "" {
			if w.gapByDatabase == nil {
				w.gapByDatabase = map[string]int64{}
			}
			w.gapByDatabase[db]++
		}
	}
	return w, nil
}

// listColdObjects lists the cold store for the run's scope, filtered to the
// files a backup carries.
//
// Not listScoped, which takes a lister but also unconditionally lists _schema/
// and _compaction_state/ and whose error text names hot concepts: against a
// cold store those are two spurious round trips per walk and a misleading
// error if the store refuses the prefixes. Arc's own reserved roots never hold
// cold objects anyway — the tiering sync skips them for the same reason.
func (m *Manager) listColdObjects(ctx context.Context, lister storage.ObjectLister, sc *scope) ([]storage.ObjectInfo, error) {
	var raw []storage.ObjectInfo
	if sc.empty() {
		objs, err := lister.ListObjects(ctx, "")
		if err != nil {
			return nil, fmt.Errorf("backup failed: could not list the cold tier: %w", err)
		}
		raw = objs
	} else {
		for _, name := range sc.names {
			objs, err := lister.ListObjects(ctx, name+"/")
			if err != nil {
				return nil, fmt.Errorf("backup failed: could not list the cold tier for database %s: %w", name, err)
			}
			raw = append(raw, objs...)
		}
	}

	out := make([]storage.ObjectInfo, 0, len(raw))
	for _, obj := range raw {
		p := filepath.ToSlash(obj.Path)
		if !strings.HasSuffix(p, ".parquet") {
			continue
		}
		first, _, _ := strings.Cut(p, "/")
		if storage.IsReservedRootDir(first) {
			continue
		}
		if !sc.ownsData(p) {
			continue
		}
		out = append(out, obj)
	}
	return out, nil
}
