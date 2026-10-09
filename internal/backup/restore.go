package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Restore modes (#1083).
//
// RestoreModeMerge is the behaviour every restore had before modes existed:
// the backup's files are written over whatever is there and nothing is
// removed. It therefore resurrects every file that retention, compaction or
// the delete API removed since the backup was taken — on every node of a
// cluster, once the restored files replicate — which for an audit database is
// the opposite of what an auditor wants.
//
// RestoreModeReplace removes the current files of every database the backup
// holds before writing the backup's. It is available on a cluster node only,
// where the removal goes through the cluster manifest (manifest-before-
// storage, as the Cluster Operations Checklist requires) and every node drops
// its copy; a standalone node has no manifest to drive it and refuses.
const (
	RestoreModeMerge   = "merge"
	RestoreModeReplace = "replace"
)

// NormalizeRestoreMode maps the request spelling onto a mode: empty means
// merge, the two names are accepted as is, anything else is an error naming
// the choices.
func NormalizeRestoreMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", RestoreModeMerge:
		return RestoreModeMerge, nil
	case RestoreModeReplace:
		return RestoreModeReplace, nil
	}
	return "", fmt.Errorf("invalid restore mode %q: use %q (additive, the default) or %q (remove the current files of each restored database first; cluster nodes only)", mode, RestoreModeMerge, RestoreModeReplace)
}

// ResolveRestoreMode is the one place the effective mode of a restore is
// decided (#1084), given the mode string exactly as the request spelled it,
// the scope of the backup being restored (its manifest's Scope) and whether
// the Raft file manifest is wired on this node. A request that named no mode
// resolves to replace when the backup is scoped AND the node is clustered:
// the point of restoring one database on a cluster is to put that database
// back, not to resurrect every file retention and compaction removed from it
// since. Everything else resolves as NormalizeRestoreMode does, so an explicit
// "merge" stays merge, which is why this takes the RAW string: the normaliser
// collapses "" and "merge" and cannot tell them apart. It never yields replace
// on a standalone node, where the manager refuses an explicit replace.
//
// The API handler calls it to echo the effective mode and the manager calls it
// again after reading the manifest, with the same inputs, so direct callers
// get the same answer and the two cannot drift.
func ResolveRestoreMode(rawMode string, scope []string, clustered bool) (string, error) {
	mode, err := NormalizeRestoreMode(rawMode)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(rawMode) == "" && len(scope) > 0 && clustered {
		return RestoreModeReplace, nil
	}
	return mode, nil
}

// CheckScopedReplace refuses a replace-mode restore of a scoped backup that
// holds no data files for one of its databases (#1084). Replace removes the
// current files of each restored database first, so for such a database it
// would only remove and restore nothing: a database dropped and recreated, or
// fully cold (its tier rows made it known at backup time, and a backup does
// not carry cold tier objects until #1086), would lose its hot tier on every
// node, and replace is the default a scoped backup resolves to on a cluster.
// The inventory is keyed by storage-root segment (parseDBMeasurement), so a
// spoke scope matches its own entry. Applied by the API handler (400) and by
// the manager (failed run) with the same text, so the two cannot disagree.
// Nil for an unscoped backup or any other mode.
func CheckScopedReplace(mode string, manifest *Manifest) error {
	if mode != RestoreModeReplace || len(manifest.Scope) == 0 {
		return nil
	}
	held := make(map[string]bool, len(manifest.Databases))
	for _, db := range manifest.Databases {
		held[db.Name] = true
	}
	var missing []string
	for _, name := range manifest.Scope {
		if !held[name] {
			missing = append(missing, fmt.Sprintf("%q", name))
		}
	}
	if len(missing) == 0 {
		return nil
	}
	noun := "database"
	if len(missing) > 1 {
		noun = "databases"
	}
	return fmt.Errorf("mode replace is refused: the backup holds no data files for %s %s, so replace would only remove its current files and restore nothing; use mode merge, or take the backup again once the database has hot files (cold tier objects are not carried by a backup until #1086)", noun, strings.Join(missing, ", "))
}

// restoreReplaceReason stamps the manifest deletes a replace-mode restore
// issues. Tiering reads it as an ordinary removal (the hot row is retired).
const restoreReplaceReason = "restore:replace"

// RestoreOptions controls what gets restored and from where.
type RestoreOptions struct {
	BackupID        string
	RestoreData     bool // restore parquet files
	RestoreMetadata bool // restore SQLite database
	RestoreConfig   bool // restore arc.toml (requires restart)
	// Mode is RestoreModeMerge or RestoreModeReplace, or empty for the
	// default: merge, except that a scoped backup restored on a cluster node
	// defaults to replace (see ResolveRestoreMode, which the manager applies
	// once it has read the manifest). An explicit value is used as given.
	Mode string
}

// RestoreResult is returned when a restore completes.
type RestoreResult struct {
	Manifest *Manifest
	Duration time.Duration
}

// errRestoreRead marks a failure reading a backup object from backup storage.
//
// It is the only per-file failure a restore tolerates mid-run: the object is
// counted and sampled and the restore moves on, so an operator recovering from
// a damaged backup still gets every file that can be read. The restore does
// not end "completed" because of it (see RestoreBackup): unlike backup, where
// a source read can fail because compaction or retention removed the file
// between listing and copy, nothing removes objects under a backup while it is
// being restored, so every such failure is damage or a degraded store.
//
// Temp-file and data-storage failures are not wrapped and abort the restore:
// they mean the environment underneath it is broken, and continuing past them
// is how a restore silently drops files.
var errRestoreRead = errors.New("restore source read failed")

func isRestoreReadError(err error) bool {
	return errors.Is(err, errRestoreRead)
}

// classifyReadTo preserves restore's source-read and destination error wording.
func classifyReadTo(srcPath string, readErr, writeErr error) error {
	return classifyReadToFailure(srcPath, readErr, writeErr, errRestoreRead, "backup")
}

// RestoreBackup restores data from a backup. It runs synchronously; the API
// layer launches it in a goroutine and exposes progress via GetProgress().
//
// A restore that could not restore every data file ends with Status "failed"
// and an error naming the counts, even though every readable file was written
// (#762). "completed" is what automation checks; reporting it over a gap turns
// the gap into a surprise at query time, which is the failure this exists to
// prevent. The files that were restored stay in place.
func (m *Manager) RestoreBackup(ctx context.Context, opts RestoreOptions) (*RestoreResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	startTime := time.Now()

	progress := &Progress{
		Operation: "restore",
		BackupID:  opts.BackupID,
		Status:    "running",
		StartedAt: startTime,
	}
	m.setProgress(progress)
	defer func() {
		now := time.Now()
		progress.CompletedAt = &now
		m.setProgress(progress)
	}()

	mode, err := NormalizeRestoreMode(opts.Mode)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}
	progress.Mode = mode
	m.setProgress(progress)

	// Cluster refusals (#1083). The API layer refuses these first, from the
	// coordinator's presence; this is the belt for callers that reach the
	// manager directly, keyed on the manifest hook being wired.
	if m.cluster != nil && (opts.RestoreMetadata || opts.RestoreConfig) {
		err := errors.New("restore refused on a cluster node: the SQLite database holds Raft-replicated tokens, per-node tier rows and the audit log, and arc.toml holds this node identity (cluster.node_id, role, seeds, raft_bootstrap, shared secret); restoring either from a backup would give this node another node state. Restore data only")
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}
	if m.cluster == nil && mode == RestoreModeReplace {
		err := errors.New("restore mode replace is only available on a cluster node, where the current files are removed through the cluster manifest; a standalone restore is additive (mode merge)")
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// ── 1. Read and validate the manifests ──────────────────────────────
	// Every target the run committed to, assembled into one MERGED run view
	// (#1085 stage B2b-2). Every gate below reads that and not one leg's
	// manifest: a scoped backup routed to one target and read from the
	// default's manifest has Scope=[audit] and Databases=[], which
	// CheckScopedReplace refuses as "holds no data files", and HasMetadata,
	// HasConfig and HasIcebergCatalog live on the default leg alone.
	read, err := m.readRunManifests(ctx, opts.BackupID)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, fmt.Errorf("failed to read backup manifest: %w", err)
	}
	manifest := read.merged
	// Three refusals, BEFORE anything is written, each naming the target and
	// the databases this node routes to it (#1085 stage B2b-2).
	//
	// The alternative — restore the targets that answer — was rejected: a
	// partial replace-mode restore deletes the live files of a database whose
	// backup bytes are on the unreachable target, and "the restore said it
	// succeeded and one database is empty" is the failure mode backups exist
	// to avoid. A merge-mode restore of a subset is no better: it reports
	// success over a set it did not restore.
	//
	// The databases named come from LOCAL configuration, like the resolution
	// itself: it is what this node would lose, and it needs no further read
	// from a store that may be the unreachable one.
	if err := m.refuseIncompleteRun(opts.BackupID, read); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// The effective mode is decided from the manifest (#1084): a scoped
	// backup on a cluster node restores in replace mode unless the request
	// named one. Same function and same inputs as the API handler's echo.
	// The spelling was checked above, so this cannot fail; the refusals above
	// stand, and the resolution never yields replace on a standalone node.
	mode, err = ResolveRestoreMode(opts.Mode, manifest.Scope, m.cluster != nil)
	if err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}
	progress.Mode = mode
	progress.Scope = manifest.Scope
	m.setProgress(progress)
	// Belt for the one replace that would only delete (#1084): refused here,
	// before the compaction pause is taken, with the text the API gives.
	if err := CheckScopedReplace(mode, manifest); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// A backup another instance wrote is restorable, and that is a decision,
	// not an oversight: restoring onto fresh hardware is what backups are
	// for, and a refusal here would be self-locking — the replacement
	// instance has a new identity by construction, so it could never restore
	// the backups it exists to restore. What it gets instead is an echo loud
	// enough to stop an operator who did not mean it, and the restore does
	// NOT adopt the owner id (see identity.go): this instance keeps the
	// identity it has, so the next backup it takes is its own.
	if !m.ownsManifest(manifest) {
		m.logger.Warn().
			Str("backup_id", opts.BackupID).
			Str("owner_instance_id", manifest.OwnerInstanceID).
			Str("this_instance_id", m.instanceID).
			Msg("Restoring a backup written by a different Arc instance: allowed, and this instance keeps its own identity, so later backups will not be listed with it")
	}

	start := m.logger.Info().Str("backup_id", opts.BackupID).Str("mode", mode)
	if len(manifest.Scope) > 0 {
		start = start.Strs("scope", manifest.Scope)
	}
	start.Msg("Starting restore")

	// A backup that was incomplete when it was taken restores exactly what it
	// holds. Surface that up front so a gap that predates the restore is not
	// mistaken for one the restore caused, and so an operator who never read
	// the manifest learns about it now.
	progress.BackupSkippedFiles = manifest.SkippedFiles
	progress.BackupUnaddressableFiles = manifest.UnaddressableFiles
	progress.BackupUnregisteredSkipped = manifest.UnregisteredSkipped
	progress.BackupManifestOnlyFiles = manifest.ManifestOnlyFiles
	progress.BackupColdFilesExcluded = manifest.ColdFilesExcluded
	m.setProgress(progress)
	// The cold-tier gap joins this informational WARN and NOTHING else
	// (#1085 stage B3). It is read from the MERGED run view, so a routed
	// backup reports every leg's gap summed and not whichever leg answered
	// first. Reported on its own line: unlike the counts above it is not a
	// defect in the backup, so an operator reading "incomplete" should not be
	// told the two are the same kind of thing.
	if manifest.ColdFilesExcluded > 0 {
		m.logger.Warn().
			Str("backup_id", opts.BackupID).
			Int64("backup_cold_files_excluded", manifest.ColdFilesExcluded).
			Interface("backup_cold_files_excluded_databases", manifest.ColdFilesExcludedDatabases).
			Msg("The backup being restored records cold-tier rows whose data it does not hold, so the restored databases will be missing those files. Either the backup was taken on a node with no readable cold tier, in which case the cold objects are untouched and still readable where they are, or their objects were already gone from the cold store when it ran")
	}
	if manifest.SkippedFiles > 0 || manifest.UnaddressableFiles > 0 || manifest.ManifestOnlyFiles > 0 || manifest.UnregisteredSkipped > 0 {
		m.logger.Warn().
			Str("backup_id", opts.BackupID).
			Int64("backup_skipped_files", manifest.SkippedFiles).
			Int64("backup_unaddressable_files", manifest.UnaddressableFiles).
			Int64("backup_manifest_only_files", manifest.ManifestOnlyFiles).
			Int64("backup_unregistered_skipped", manifest.UnregisteredSkipped).
			Strs("unaddressable_sample", manifest.UnaddressableSample).
			Strs("skipped_sample", manifest.SkippedSample).
			Strs("manifest_only_sample", manifest.ManifestOnlySample).
			Strs("unregistered_sample", manifest.UnregisteredSample).
			Msg("Restoring a backup that was incomplete, or deliberately excluded unregistered files, when it was taken")
	}

	// ── 2. Restore data files ───────────────────────────────────────────
	// On a cluster node under the cluster-wide compaction pause (#1087), both
	// modes: compaction commits in two Raft phases on the compactor, and a
	// job whose phase 2 lands during the restore manifest-deletes inputs the
	// restore has just registered (replace) or unlinks a freshly written
	// input before its batched register (both modes). The pause returns once
	// every node has quiesced; it is released by the deferred resume AFTER
	// the final Lost check below, with a fresh context because the run's own
	// may be done by then. The pause expires on its own within its TTL if
	// this process dies, so a failed resume is a warning, not a failure.
	var pause CompactionPause
	if opts.RestoreData && m.cluster != nil {
		progress.CompactionPause = "waiting"
		m.setProgress(progress)
		p, err := m.cluster.PauseCompaction(ctx, "restore "+opts.BackupID)
		if err != nil {
			err = fmt.Errorf("restore refused: compaction could not be paused cluster-wide: %w", err)
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		pause = p
		progress.CompactionPause = "paused"
		m.setProgress(progress)
		m.logger.Info().Str("backup_id", opts.BackupID).Str("mode", mode).Msg("Compaction is paused cluster-wide for this restore; every node has acknowledged")
		// Registered after the progress-publishing defer at the top of this
		// function, so it runs BEFORE it (LIFO): the final published
		// snapshot carries the "released" state this writes.
		defer func() {
			rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := pause.Resume(rctx); err != nil {
				m.logger.Warn().Err(err).Str("backup_id", opts.BackupID).Msg("Could not release the cluster-wide compaction pause; it expires on its own within six minutes")
				return
			}
			if progress.CompactionPause != "lost" {
				progress.CompactionPause = "released"
			}
		}()
	}
	if opts.RestoreData {
		if err := m.restoreDataFiles(ctx, opts.BackupID, read, progress, mode, pause); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
	}

	// ── 2b. Restore an Iceberg warehouse that lived outside the storage root
	// Tied to either flag: the catalog rows (metadata) and the data files both
	// depend on these metadata files, so restoring one without them leaves the
	// tables unloadable (#637).
	//
	// This and the two steps after it read from read.anchor.target, NOT from
	// this node's current default: the gates below gate on the MERGED view, so
	// they fire whenever any leg declared the flag, while only the anchor leg
	// ever wrote the bytes. The two are the same target until an operator adds
	// a second target and re-points backup.default_target, after which reading
	// from the current default fails on SQLite and arc.toml and silently
	// restores no warehouse at all. See runRead.anchor.
	if opts.RestoreData || opts.RestoreMetadata {
		catalogRestored := opts.RestoreMetadata && manifest.HasMetadata
		if err := m.restoreIcebergWarehouse(ctx, read.anchor.target, opts.BackupID, manifest, progress, catalogRestored); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
	}

	// ── 3. Restore SQLite metadata ──────────────────────────────────────
	if opts.RestoreMetadata && manifest.HasMetadata {
		if err := m.restoreSQLite(ctx, read.anchor.target, opts.BackupID); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, fmt.Errorf("failed to restore SQLite database: %w", err)
		}
	}

	// ── 4. Restore config ───────────────────────────────────────────────
	if opts.RestoreConfig && manifest.HasConfig {
		if err := m.restoreConfig(ctx, read.anchor.target, opts.BackupID); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, fmt.Errorf("failed to restore config: %w", err)
		}
	}

	// ── 5. Decide the outcome ───────────────────────────────────────────
	// Decided after the metadata and config steps so those are staged even
	// when the data set has a gap: an operator recovering a node wants both.
	duration := time.Since(startTime)
	// The pause must still have been this restore's up to the last register
	// (#1087); the between-batch checks cover the run, this one its tail.
	if err := checkCompactionPauseLost(pause, progress); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		m.setProgress(progress)
		m.logger.Error().Err(err).Str("backup_id", opts.BackupID).Msg("Restore failed: the cluster-wide compaction pause was lost")
		return nil, err
	}
	skipped := atomic.LoadInt64(&progress.SkippedFiles)
	missing := progress.MissingFiles
	unaddressable := progress.UnaddressableFiles
	// A catalog staged for a node that runs Iceberg but had nowhere to put the
	// warehouse files is guaranteed unloadable after restart; "completed" would
	// turn that into a surprise. A node with Iceberg off keeps the Warn only:
	// its catalog rows are inert.
	if progress.IcebergWarehouseFilesSkipped > 0 && m.icebergEnabled && opts.RestoreMetadata && manifest.HasMetadata {
		source := ""
		if manifest.IcebergWarehouse != nil {
			source = manifest.IcebergWarehouse.ConfiguredPath
			if source == "" {
				source = manifest.IcebergWarehouse.Path
			}
		}
		err := fmt.Errorf("restore incomplete: %d Iceberg warehouse files were not restored because this node's iceberg.warehouse is under its storage root while the backup's was %s; set iceberg.warehouse to that path and run the restore again", progress.IcebergWarehouseFilesSkipped, source)
		progress.Status = "failed"
		progress.Error = err.Error()
		m.logger.Error().Str("backup_id", opts.BackupID).Int64("iceberg_warehouse_files_skipped", progress.IcebergWarehouseFilesSkipped).Msg("Restore incomplete")
		return nil, err
	}
	if skipped+missing+unaddressable+progress.SidecarMismatches > 0 {
		var parts []string
		if skipped > 0 {
			parts = append(parts, fmt.Sprintf("%d objects could not be read from backup storage (skipped_sample)", skipped))
		}
		if progress.SidecarMismatches > 0 {
			parts = append(parts, fmt.Sprintf("%d data files in the backup do not match its sidecar (size, SHA-256, or no row) and were not restored (sidecar_mismatch_sample); the backup is damaged", progress.SidecarMismatches))
		}
		if unaddressable > 0 {
			parts = append(parts, fmt.Sprintf("%d data files are in backup storage under names no listing returns (unaddressable_sample; rename them and re-run)", unaddressable))
		}
		if missing > 0 {
			parts = append(parts, fmt.Sprintf("%d data files the backup inventoried are absent from backup storage (missing_files)", missing))
		}
		err := fmt.Errorf("restore incomplete: %s; the files that could be restored are in place", strings.Join(parts, "; "))
		progress.Status = "failed"
		progress.Error = err.Error()
		m.logger.Error().
			Str("backup_id", opts.BackupID).
			Int64("files_restored", atomic.LoadInt64(&progress.ProcessedFiles)).
			Int64("skipped", skipped).
			Int64("unaddressable", unaddressable).
			Int64("missing", missing).
			Int64("sidecar_mismatches", progress.SidecarMismatches).
			Strs("skipped_sample", progress.SkippedSample).
			Strs("unaddressable_sample", progress.UnaddressableSample).
			Strs("sidecar_mismatch_sample", progress.SidecarMismatchSample).
			Dur("duration", duration).
			Msg("Restore incomplete")
		return nil, err
	}

	progress.Status = "completed"

	m.logger.Info().
		Str("backup_id", opts.BackupID).
		Str("mode", mode).
		Int64("files_restored", atomic.LoadInt64(&progress.ProcessedFiles)).
		Int64("files_registered", progress.FilesRegistered).
		Int64("replaced_files", progress.ReplacedFiles).
		Dur("duration", duration).
		Msg("Restore completed")

	return &RestoreResult{Manifest: manifest, Duration: duration}, nil
}

// checkCompactionPauseLost fails the restore when the cluster-wide compaction
// pause it holds has stopped being its own (#1087): a compaction job may have
// run on the restored data meanwhile, so the restored set cannot be trusted.
// nil pause (standalone, or a restore without data) passes.
func checkCompactionPauseLost(pause CompactionPause, progress *Progress) error {
	if pause == nil {
		return nil
	}
	lost, cause := pause.Lost()
	if !lost {
		return nil
	}
	progress.CompactionPause = "lost"
	return fmt.Errorf("restore failed: the cluster-wide compaction pause was lost (%v); a compaction job may have raced this restore, so take a fresh backup and restore again", cause)
}

// refuseIncompleteRun is the three pre-write refusals of a routed restore
// (#1085 stage B2b-2).
//
// A run with ONE target resolves nothing: readRunManifests found its manifest
// at the destination this node is configured with, so a backup whose Target
// names a target this node does not have still restores, exactly as stage
// B2b-1 promised. Only a run spanning SEVERAL targets has names to resolve,
// because the other legs' bytes are somewhere this node must be able to reach.
func (m *Manager) refuseIncompleteRun(backupID string, read *runRead) error {
	describe := func(names []string) string {
		out := make([]string, 0, len(names))
		for _, name := range names {
			if dbs := m.databasesRoutedTo(name); len(dbs) > 0 {
				out = append(out, fmt.Sprintf("%s (this node routes %s to it)", name, strings.Join(dbs, ", ")))
				continue
			}
			out = append(out, name)
		}
		return strings.Join(out, ", ")
	}
	switch {
	case len(read.unknown) > 0:
		return fmt.Errorf("restore refused: backup %s was written to %d targets and this node has no backup target named %s, so the data on it cannot be read. Configure that target, or restore on a node that has it. Restoring only the targets this node can reach would report success over a set it did not restore, and in replace mode would delete the live files of a database whose backup bytes are on the target it could not read",
			backupID, len(read.merged.RunTargets), describe(read.unknown))
	case len(read.unreachable) > 0:
		return fmt.Errorf("restore refused: backup %s spans %d targets and %s would not answer; retry once it is reachable, or check its configuration. A partial restore would report success over a set it did not restore",
			backupID, len(read.merged.RunTargets), describe(read.unreachable))
	case len(read.missing) > 0:
		return fmt.Errorf("restore refused: backup %s names %d targets and %s holds no manifest for it, so that part of the run never committed and the backup is known-partial. Delete the backup id and take the backup again rather than restore a run that is missing a target",
			backupID, len(read.merged.RunTargets), describe(read.missing))
	}
	return nil
}

// databasesRoutedTo are the databases THIS NODE routes to a target, sorted,
// for a refusal that has to say what is at stake.
func (m *Manager) databasesRoutedTo(target string) []string {
	var out []string
	for db, name := range m.routing {
		if name == target {
			out = append(out, db)
		}
	}
	sort.Strings(out)
	return out
}

// restoreLeg is one target's half of a restore: its destination, the manifest
// it wrote, its listing, and the compaction inputs its own recovery manifests
// say must not be restored.
type restoreLeg struct {
	runLeg
	files      []string
	skipInputs map[string]bool
}

// restoreDataFiles copies parquet files from the backup back into data storage.
//
// Backup objects that cannot be read are skipped, counted, and sampled; every
// other failure aborts (see errRestoreRead). Data files the listing cannot
// return, and files the manifest inventoried that are gone, are counted too.
// The caller turns any non-zero count into a failed restore.
//
// On a cluster node (#1083) every database data file written is registered
// in the cluster manifest from the backup's sidecar, in batches, and in
// replace mode the current manifest entries of each restored database are
// removed first. See restoreRegistration and replaceDatabases. Every data
// file written is also reported to this node's tier metadata when tiering is
// on, standalone or clustered. pause is the cluster-wide compaction pause the
// caller holds for the run (#1087), nil on a standalone node; it is checked
// before every manifest write.
//
// With several legs (#1085 stage B2b-2) every per-leg quantity is computed per
// leg and every RUN-WIDE decision is made over the union:
//
//   - consumedInputsInBackup and the missing-files comparison are per leg,
//     which decision 1 of the B2b-2 plan makes correct: a recovery manifest,
//     its output and its inputs always share one storage-root segment, so they
//     are always on one target;
//   - the sidecar, the listing and skipInputs are UNIONED before
//     replaceDatabases, which then runs ONCE. Per target it would be unsound:
//     owns() keys on the cluster entry's Database label for an unscoped backup
//     while writes is built from the backup listing, so target B's owns() can
//     be true for a spoke entry whose bytes are in target A's listing and not
//     B's — and B would BatchDelete live files A is about to restore. The two
//     sets are only mutually protective when files is the whole run.
//
// The per-leg copy loop sits INSIDE the single compaction pause the caller
// holds (#1087); a loop placed above it would take and release one pause per
// leg.
func (m *Manager) restoreDataFiles(ctx context.Context, backupID string, read *runRead, progress *Progress, mode string, pause CompactionPause) error {
	// The ID the CALLER asked for, never the manifest's own backup_id field: a
	// manifest is data read from backup storage, so a field in it must not
	// select the keys further bytes are read from. The two agree for every
	// manifest Arc wrote; a corrupt or hand-edited one would otherwise send
	// the restore at another backup's prefix.
	dataPrefix := backupID + "/data/"

	legs := make([]*restoreLeg, 0, len(read.legs))
	var allFiles []string
	allSkipInputs := map[string]bool{}
	var totalFiles int64
	var presentTotal int64
	var missing int64
	for _, leg := range read.legs {
		files, err := leg.target.backend.List(ctx, dataPrefix)
		if err != nil {
			return fmt.Errorf("failed to list the backup data files in %s: %w", leg.target.describe(), err)
		}
		// Reconcile compaction state before copying anything (#930). A backup
		// taken between a compaction job's output upload and its input deletion
		// holds both; restoring both would serve every row of that partition
		// twice, until a compaction cycle's recovery deleted the inputs, and
		// forever if compaction is disabled on the restored node or a cycle
		// raced the restore. So the backed-up manifests are read first, and the
		// inputs of every manifest whose output the backup holds are simply not
		// restored: the restored store then looks exactly like a job that
		// finished, and recovery on the next cycle finds the output, tolerates
		// the absent inputs, fires the receipt hooks and deletes the manifest. A
		// manifest whose output the backup does NOT hold keeps its inputs: that
		// is a job that never uploaded, and recovery deletes the manifest so
		// compaction retries. Every restore order is safe this way, because the
		// inputs never land. Every other object under data/, the field schema
		// anchors under _schema/ (#927) included, is restored to its original
		// key.
		skipInputs := m.consumedInputsInBackup(ctx, leg.target, dataPrefix, files)
		totalFiles += int64(len(files) - len(skipInputs))
		allFiles = append(allFiles, files...)
		for k, v := range skipInputs {
			if v {
				allSkipInputs[k] = true
			}
		}
		legs = append(legs, &restoreLeg{runLeg: leg, files: files, skipInputs: skipInputs})
	}
	progress.TotalFiles = totalFiles
	progress.TotalBytes = read.merged.TotalSizeBytes

	// Three ways a leg's listing can under-represent what its manifest
	// promised, and nothing in the copy loop can notice any of them: every
	// listed file restores fine.
	//
	// 1. The listing hides it. An object store returns dot-prefixed keys and
	//    the backup copied them, but the local backup store's listing hides
	//    dot-prefixed names (and any key an older Arc wrote that the contract
	//    now refuses). ListUnusable returns exactly what List dropped, so those
	//    are counted and named: renaming them in the backup recovers the data.
	// 2. The object is gone: a partial sync, a truncated copy, an operator's
	//    rm. Counted against that leg's manifest inventory as missing.
	// 3. The backup itself never wrote it (manifest.SkippedFiles); not missing.
	//
	// Only .parquet entries are counted: the listing also holds Iceberg
	// warehouse metadata copied under data/, which is not part of TotalFiles.
	var unaddressableSample []string
	for _, leg := range legs {
		var present int64
		for _, f := range leg.files {
			if strings.HasSuffix(f, ".parquet") {
				present++
			}
		}
		presentTotal += present
		var hiddenHere int64
		if ul, ok := leg.target.backend.(storage.UnusableLister); ok {
			hidden, err := ul.ListUnusable(ctx, dataPrefix)
			if err != nil {
				return fmt.Errorf("failed to inventory the unlistable backup objects in %s: %w", leg.target.describe(), err)
			}
			for _, o := range hidden {
				if !strings.HasSuffix(o.Path, ".parquet") {
					continue
				}
				hiddenHere++
				if len(unaddressableSample) < unaddressableSampleCap {
					unaddressableSample = append(unaddressableSample, o.Path)
				}
			}
		}
		progress.UnaddressableFiles += hiddenHere
		// Per leg against its OWN listing, because each manifest's TotalFiles
		// describes only its own slice.
		if expected := leg.manifest.TotalFiles - leg.manifest.SkippedFiles; expected > present+hiddenHere {
			gap := expected - present - hiddenHere
			missing += gap
			m.logger.Warn().
				Str("target", leg.target.name).
				Int64("inventoried", expected).
				Int64("present", present).
				Int64("unaddressable", hiddenHere).
				Int64("missing", gap).
				Msg("Backup storage holds fewer data files than the manifest inventoried; the restore will be incomplete")
		}
	}
	if progress.UnaddressableFiles > 0 {
		progress.UnaddressableSample = unaddressableSample
		m.logger.Warn().
			Int64("unaddressable", progress.UnaddressableFiles).
			Strs("sample", unaddressableSample).
			Msg("Backup storage holds data files no listing returns; they cannot be restored until renamed")
	}
	if missing > 0 {
		progress.MissingFiles = missing
	}
	m.setProgress(progress)

	// ── Cluster: the sidecar, the manifest, and the replace half (#1083) ─
	// All before any byte is written. A cluster restore registers each file
	// from the sidecar and cannot do without it; the manifest is synced and
	// read once, for the labels of paths it already lists and for the replace
	// half, which removes the current entries first so a failure there leaves
	// the store exactly as it was.
	//
	// Every leg's sidecar is unioned into one map, which is sound because the
	// rows are disjoint: a path was copied by exactly the leg its routing key
	// named. Every leg must HAVE one — an empty leg still commits a sidecar,
	// which is why planRun commits on every member — so a missing one is the
	// same refusal it always was.
	// The per-file TIER (#1086 stage C) comes from the sidecar, which until
	// now was read only on a cluster, where it is needed for registration.
	// A standalone node needs it too — it is the only record of which tier a
	// file came from — so it is read here as well, TOLERANTLY: a backup with
	// no sidecar is one taken before cluster-aware backups, and everything in
	// it is hot, which is also what an absent tier means. The cluster path
	// below keeps its refusal, because there a missing sidecar means the files
	// cannot be registered at all.
	// Read on EVERY standalone restore, not only when this node has a cold
	// tier. A node WITHOUT one still has to know which files the backup read
	// from cold, or the fallback to hot storage happens silently and
	// cold_files_restored_to_hot is always zero — which is exactly the number
	// an operator restoring onto a cold-less node needs to see.
	tiers := map[string]string{}
	backupRows := map[string]ManifestFile{}
	if m.cluster == nil {
		for _, leg := range legs {
			legEntries, ok, err := m.readSidecar(ctx, leg.target, backupID)
			if err != nil {
				// TOLERANT means tolerant of a sidecar that cannot be USED,
				// not only of one that is absent. A standalone restore never
				// read this file before stage C, so failing the whole restore
				// on an unreadable or undecodable sidecar would be a new way
				// for an old backup to stop restoring. Without it every file
				// is treated as hot, which is what a pre-stage-C backup is —
				// so the restore still puts the data back, just all in hot
				// storage, and says so.
				//
				// The CLUSTER path above keeps its hard failure, because there
				// the sidecar is what the files are registered from and a
				// restore that cannot register is not a restore.
				m.logger.Warn().Err(err).
					Str("backup_id", backupID).
					Str("target", leg.target.describe()).
					Msg("Could not read the backup file sidecar; restoring every file to hot storage, so any file this backup took from a cold tier lands in hot")
				continue
			}
			if !ok {
				continue
			}
			for path, row := range legEntries {
				backupRows[path] = row
				if row.Tier != "" {
					tiers[path] = row.Tier
				}
			}
		}
	}

	var reg *restoreRegistration
	var current []ManifestFile
	if m.cluster != nil {
		entries := map[string]ManifestFile{}
		for _, leg := range legs {
			legEntries, ok, err := m.readSidecar(ctx, leg.target, backupID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("restore refused: backup %s has no %s in %s, so the files it holds cannot be registered in the cluster manifest; it was taken before cluster-aware backups and can be restored on a standalone node, or taken again", backupID, sidecarName, leg.target.describe())
			}
			for path, row := range legEntries {
				entries[path] = row
				backupRows[path] = row
				if row.Tier != "" {
					tiers[path] = row.Tier
				}
			}
		}
		if err := m.cluster.Sync(ctx); err != nil {
			return fmt.Errorf("restore refused: could not sync the cluster manifest before reading it: %w", err)
		}
		current = m.cluster.ManifestFiles()
		existing := make(map[string]ManifestFile, len(current))
		for _, e := range current {
			existing[filepath.ToSlash(e.Path)] = e
		}
		reg = &restoreRegistration{
			entries:  entries,
			existing: existing,
			overhead: registerOpOverhead(len(m.cluster.LocalNodeID())),
			pause:    pause,
		}
	}

	// A hot file in the backup may since have migrated to this node's cold
	// tier. Resolve that before replaceDatabases can remove any live data.
	//
	// The local tier row decides where the file belongs, not the backup's
	// sidecar: the row is this node's current truth, and the restore's job is to
	// make the bytes at that location correct rather than to re-decide tiering.
	// So a cold row routes the write to COLD, which the cold-sidecar path
	// already implements end to end — no hot manifest entry (the entry tiering
	// removed at migration, and whose re-creation is what made #1139 read the
	// file twice), the cold row recorder instead of the hot report, and the
	// existing no-cold-backend fallback when this node cannot store cold.
	//
	// Routing rather than forcing the row hot is deliberate. A forced hot row is
	// written by the batched flush long after the bytes land, and until it does
	// the row still says cold with a recent migrated_at — exactly what
	// Migrator.ReconcileOrphanedFiles selects. With a cold backend present and a
	// cold object confirmed, that sweep would delete the hot copy this restore
	// had just written and leave the stale cold object behind. Keeping the write
	// on the cold route means tier row and object never disagree, so the sweep
	// has nothing to act on, and coldToHot below stays reachable only when this
	// node has no cold backend at all — the precondition its own comment relies
	// on.
	//
	// Size is the discriminator because a tier row stores no checksum of the
	// backup object. It is weak on its own, but migration is a byte-preserving
	// move of the SAME path, and the writers that produce new content for a
	// measurement (compaction, DELETE rewrites) emit new filenames — so a cold
	// object at this path whose size matches the sidecar is the same file. A
	// mismatch is therefore the interesting case, and it routes to cold rather
	// than being trusted either way.
	alreadyCold := map[string]bool{}
	skipUnverifiable := map[string]bool{}
	toColdRouted := map[string]bool{}
	if m.coldSource != nil {
		coldRows, err := m.coldSource.ColdRows(ctx)
		if err != nil {
			return fmt.Errorf("restore refused: could not read cold tier rows before restoring: %w", err)
		}
		cold := m.coldBackendOrNil()
		for _, leg := range legs {
			for _, srcPath := range leg.files {
				// Two HEADs per candidate against an object store, so this loop
				// is cancellable and publishes, like the write loop below.
				if err := ctx.Err(); err != nil {
					return err
				}
				destPath := filepath.ToSlash(strings.TrimPrefix(srcPath, dataPrefix))
				if destPath == "" || destPath == filepath.ToSlash(srcPath) || leg.skipInputs[destPath] ||
					tiers[destPath] == tierCold {
					continue
				}
				if _, isCold := coldRows[destPath]; !isCold {
					continue
				}
				row, hasBackupRow := backupRows[destPath]
				if cold == nil {
					// Nowhere to put a cold copy, so the file goes to hot and its
					// row has to be forced hot or it is invisible. Marking the tier
					// cold is what reaches that existing branch; it is NOT a route
					// to cold, so it is not counted as one.
					tiers[destPath] = tierCold
					continue
				}
				if !hasBackupRow {
					// No sidecar row, so nothing can check the bytes we would
					// write, and what we would overwrite is the only copy. Leave it
					// alone: the row already says cold and the object is already
					// there, so the node keeps the state it had. Overwriting the
					// canonical copy with unverifiable bytes is the one outcome
					// worse than not restoring the file, so this is reported rather
					// than attempted.
					skipUnverifiable[destPath] = true
					atomic.AddInt64(&progress.ColdRowsSkippedUnverifiable, 1)
					m.logger.Warn().Str("path", destPath).
						Msg("Backup has no sidecar row for a file this node holds in cold storage; left the cold copy untouched rather than overwriting it with unverifiable bytes")
					m.setProgress(progress)
					continue
				}
				tiers[destPath] = tierCold
				toColdRouted[destPath] = true
				atomic.AddInt64(&progress.HotBackupFilesRoutedToCold, 1)
				m.setProgress(progress)
				// Exists AND StatFile, which is not the redundant pair it looks
				// like: LocalBackend.StatFile falls back to the ".part" staging
				// file when the final object is absent and returns ITS size
				// (internal/storage/local.go), so a stat alone reports a
				// half-transferred object as a complete one — and a matching size
				// would then skip the restore and leave the staging file as the
				// only copy. Exists looks only at the final path. The interface
				// comment on StatFile says it returns -1 when the file does not
				// exist, which is true of the object stores and not of local disk.
				exists, err := cold.Exists(ctx, destPath)
				if err != nil {
					return fmt.Errorf("restore refused: could not check whether the cold copy of %s exists before restoring: %w", destPath, err)
				}
				if !exists {
					continue
				}
				size, err := cold.StatFile(ctx, destPath)
				if err != nil {
					return fmt.Errorf("restore refused: could not read the size of cold copy %s before restoring: %w", destPath, err)
				}
				if size == row.SizeBytes {
					alreadyCold[destPath] = true
				}
			}
		}
	}

	if m.cluster != nil && mode == RestoreModeReplace {
		if err := m.replaceDatabases(ctx, read.merged, allFiles, dataPrefix, allSkipInputs, current, progress, pause); err != nil {
			return err
		}
	}

	// Skips are published on every exit, including a fatal abort part-way
	// through, so the status shows which objects were unreadable even when
	// something else ended the restore. The sample is assigned once, here, and
	// never appended to again: published snapshots copy the slice header, so
	// readers only ever see a finished slice.
	var skipped int64
	var sample []string
	defer func() {
		if skipped == 0 {
			return
		}
		atomic.AddInt64(&progress.SkippedFiles, skipped)
		progress.SkippedSample = sample
		m.setProgress(progress)
	}()

	// The tier rows this restore owes, batched (#1141). Flushed on EVERY exit
	// path, which is deliberately unlike the manifest registration beside it:
	// reg's pending rows are abandoned on an early return because a failed
	// restore is re-run and the cluster re-derives its manifest, but a tier
	// row has no such second chance. The bytes are already in the cold store,
	// and on a standalone node — or a cluster without shared storage or
	// replication — nothing ever writes the row, because the cold-metadata
	// sync does not run there. A file whose bytes landed and whose row was
	// dropped is unreadable indefinitely.
	batch := newColdRowBatch()
	defer func() {
		m.flushColdRows(ctx, batch, progress)
		// Published after the counters move: a flush error is a warn-and-count
		// outcome, and a defer cannot reach the return value, so this snapshot
		// is the only way an operator sees what the last chunk did.
		m.setProgress(progress)
	}()

	for _, leg := range legs {
		for _, srcPath := range leg.files {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Strip the backup prefix to get the original storage path
			destPath := filepath.ToSlash(strings.TrimPrefix(srcPath, dataPrefix))
			if destPath == "" || destPath == filepath.ToSlash(srcPath) {
				continue
			}
			if leg.skipInputs[destPath] {
				atomic.AddInt64(&progress.ConsumedInputsSkipped, 1)
				continue
			}
			if skipUnverifiable[destPath] {
				continue
			}
			if alreadyCold[destPath] {
				atomic.AddInt64(&progress.ColdFilesSkippedAlreadyCold, 1)
				m.setProgress(progress)
				continue
			}

			// On a cluster a data file is written only when the sidecar describes
			// it, and only if its bytes match that description (checked on the
			// temp-file hop, before the write): a damaged backup object must not
			// replace a good live copy, and a same-size corruption registered
			// with the sidecar's SHA would fail every peer's checksum forever.
			// A data file that would overflow the pending registration batch
			// flushes it BEFORE being written, so a refused batch aborts with
			// exactly that batch written-but-unregistered and nothing written
			// past it (see registerRestoredFile).
			registrable := isRegistrableDataFile(destPath)
			var pendingRow pendingRegistration
			var expect *ManifestFile
			// Standalone has no registration pass, so expect stays nil and
			// nothing checks the bytes. That was tolerable while an unverified
			// write landed in HOT storage beside an intact cold object; a routed
			// write overwrites the canonical copy, so verify it against the same
			// sidecar row the routing decision already trusted.
			if expect == nil && toColdRouted[destPath] {
				if row, ok := backupRows[destPath]; ok {
					verified := row
					expect = &verified
				}
			}
			if reg != nil && registrable {
				pendingRow = reg.lookup(destPath)
				if !pendingRow.ok {
					progress.SidecarMismatches++
					progress.SidecarMismatchSample = appendSample(progress.SidecarMismatchSample, []string{destPath})
					m.setProgress(progress)
					m.logger.Error().Str("path", destPath).Msg("Backup data file has no row in the sidecar; not restored (it could not be registered in the cluster manifest)")
					continue
				}
				expect = &pendingRow.row
				if !reg.batch.fits(pendingRow.size) {
					if err := m.flushRegistrations(ctx, reg, progress); err != nil {
						return err
					}
				}
			}

			// Where this file goes (#1086 stage C). A sidecar row saying
			// "cold" means the backup read it from a cold tier, so it belongs
			// in this node's cold tier — if this node has one. If it does not,
			// it goes to hot and is counted: the data is here and queryable,
			// it is simply all hot now, which is what restoring onto a node
			// without a cold tier means.
			//
			// Note the one case where writing to cold would be WRONG even
			// though a cold backend exists: cold configured but disabled. The
			// query path gates its cold glob on the same enabled flag, so a
			// file written there would be unreadable. ColdBackend() ANDs the
			// flag, so that case takes the hot branch here.
			dest, destName := m.dataStorage, "data storage"
			toCold, coldToHot := false, false
			if tiers[destPath] == tierCold {
				if cold := m.coldBackendOrNil(); cold != nil {
					dest, destName, toCold = cold, "cold tier storage", true
				} else {
					atomic.AddInt64(&progress.ColdFilesRestoredToHot, 1)
					// Only worth moving a row if there is a tier store to
					// move it in. With no tiering wired at all there are no
					// tier rows, so there is nothing to correct — and
					// m.coldSource is nil, so this must not be set.
					coldToHot = m.coldSource != nil
				}
			}

			// Stream via temp file to avoid loading entire Parquet file into memory
			bytesWritten, err := m.streamRestoreFile(ctx, leg.target, srcPath, destPath, expect, dest, destName)
			if err != nil {
				if errors.Is(err, errSidecarMismatch) {
					progress.SidecarMismatches++
					progress.SidecarMismatchSample = appendSample(progress.SidecarMismatchSample, []string{destPath})
					m.setProgress(progress)
					m.logger.Error().Str("path", destPath).Err(err).Msg("Backup data file does not match the sidecar; not restored, the live copy is untouched")
					continue
				}
				// Only a backup-storage read failure is skippable. A temp-file or
				// data-storage failure means the environment underneath the
				// restore is broken, and continuing would drop files silently.
				if !isRestoreReadError(err) {
					return fmt.Errorf("failed to restore %s: %w", srcPath, err)
				}
				skipped++
				if len(sample) < unaddressableSampleCap {
					sample = append(sample, srcPath)
				}
				m.logger.Warn().Str("path", srcPath).Err(err).Msg("Failed to read backup file, skipping")
				continue
			}

			atomic.AddInt64(&progress.ProcessedFiles, 1)
			atomic.AddInt64(&progress.ProcessedBytes, bytesWritten)
			if isCompactionState(destPath) && strings.HasSuffix(destPath, ".json") {
				atomic.AddInt64(&progress.CompactionStateRestored, 1)
			}
			if registrable && toCold {
				// A cold file is registered NOWHERE in the cluster manifest
				// and reported through NEITHER of the hot paths (#1086
				// stage C).
				//
				// Not the manifest: tiering removes a migrated file's entry as
				// phase 2 of every migration, and the manifest adapter drops
				// cold entries outright, so registering one would create an
				// entry the next tiering cycle wants gone — and would make
				// peers try to replicate a file that is not in hot storage.
				//
				// Not RecordRestoredFile either: that enqueues a "pulled"
				// event whose handler stats the HOT backend, finds nothing,
				// and returns without writing, and whose upsert is guarded to
				// hot rows anyway. A cold restore reported through it is
				// dropped silently, twice over.
				//
				// So: its own recorder, and a batched one (#1141) — the tier
				// row is what makes the file queryable at all, and each row
				// used to be its own transaction, and so its own fsync, on
				// the one shared SQLite connection.
				//
				// Queued AFTER the bytes landed, never before: a flush must
				// not be able to describe a file that is not there (Cluster
				// Operations Checklist item 2, applied to tier rows).
				m.queueColdRow(ctx, batch, destPath, bytesWritten, progress)
			} else if registrable {
				// Storage first, then the manifest (Cluster Operations Checklist):
				// the file is on disk before anything can route a reader to it.
				// Tier rows the same way, after the bytes landed.
				if coldToHot {
					// The backup read this file from a cold tier and this node
					// has none, so the bytes went to HOT storage — and the
					// row, which says cold, has to be moved or the data is
					// invisible rather than merely mis-tiered: the query path
					// omits the hot glob when nothing claims hot AND the cold
					// glob when there is no cold backend, which together
					// resolve to a read of nothing.
					//
					// The ordinary report below cannot do it: its upsert is
					// guarded to rows that already say hot. Hence a forced
					// recorder, used only here.
					// coldSource is non-nil here: coldToHot is set only when it
					// is, in the branch above.
					//
					// Batched (#1141), so the forced write now happens AFTER
					// the asynchronous RecordRestoredFile below rather than
					// before it, possibly much later. Both orders converge on
					// a hot row: the asynchronous report's upsert is guarded
					// to rows that already say hot, so it cannot downgrade
					// anything, and the forced write sets hot
					// unconditionally. Stated here because it is now an
					// argument rather than sequential code — anyone changing
					// either path has to keep it true.
					//
					// WHAT THE LAG DOES NOT COST, corrected from what this
					// comment claimed when #1142 shipped yesterday: until the
					// row is written it still says cold with a recent
					// migrated_at, which is what ReconcileOrphanedFiles looks
					// for — so the sweep could in principle find the hot copy
					// this restore just wrote and delete it once it confirmed
					// the cold object.
					//
					// It cannot, and the reason is this branch's own
					// precondition. coldToHot is reached only when
					// coldBackendOrNil() is nil, which means this node has no
					// cold backend OR has cold disabled — and cmd/arc/main.go
					// builds a cold backend only inside "if cold.Enabled", so
					// in both cases m.coldBackend is nil. The sweep then has
					// nothing to verify a cold copy against and keeps the hot
					// file; since #1143 it is skipped outright on such a node.
					// The window the #1142 note warned about, and its advice
					// to run this restore with tiering stopped, were wrong.
					m.queueHotRow(ctx, batch, destPath, bytesWritten, progress)
					// Still true after #1139, and load-bearing: a hot-sidecar file
					// whose local row says cold is ROUTED to cold in the pre-pass
					// rather than forced hot here, so this branch keeps its single
					// precondition. Give coldToHot a second assignment and every
					// paragraph above has to be re-argued — the sweep interaction
					// first, because with a cold backend present it deletes the hot
					// copy a restore has just written.
					//

				}
				if m.tierRecorder != nil {
					m.tierRecorder.RecordRestoredFile(destPath, bytesWritten)
				}
				if reg != nil {
					reg.queue(pendingRow)
				}
			}
			// Republish so /status polling sees live counters — published Progress
			// values are immutable snapshots, not the struct being mutated here.
			m.setProgress(progress)

			if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
				m.logger.Info().
					Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
					Int64("total", progress.TotalFiles).
					Msg("Restore progress")
			}
		}
	}

	if reg != nil {
		if err := m.flushRegistrations(ctx, reg, progress); err != nil {
			return err
		}
	}

	if n := atomic.LoadInt64(&progress.CompactionStateRestored); n > 0 || len(allSkipInputs) > 0 {
		m.logger.Info().
			Int64("compaction_manifests_restored", n).
			Int64("consumed_inputs_skipped", atomic.LoadInt64(&progress.ConsumedInputsSkipped)).
			Msg("Compaction recovery state restored. Inputs already replaced by a backed-up compacted output were not restored; the next compaction cycle completes each restored manifest. If metadata was restored too, restart before that cycle so the staged metadata is applied first; a hub's receipt marks made earlier would be overwritten by the swap. A manifest older than seven days logs a stale warning when processed; that is expected after a restore")
	}

	return nil
}

// coldRowBatchSize caps one batch call at the SQLite Review Checklist's
// number for batched work, which is also the bound the manifest registration
// beside it uses for Raft ops. The store writes exactly what it is given in
// ONE transaction, so this is the write-lock hold time an operator is agreeing
// to: a thousand upserts on the shared connection, rather than one fsync per
// file for the length of the restore (#1141).
const coldRowBatchSize = 1000

// coldRowFlushTimeout bounds ONE flush, each of which runs on a context
// detached from the restore's own (see flushColdRowsTo). Generous because a
// flush is up to a full chunk against a loaded SQLite file, and bounded so a
// cancelled restore cannot be held up by more than one of them. It does not
// need to cover a closed handle: BeginTx then fails at once with
// "sql: database is closed" rather than waiting.
const coldRowFlushTimeout = 30 * time.Second

// coldRowBatch is the tier-row side of one restore run (#1141): the rows owed
// for files already written to the cold tier, and for cold-carried files this
// node had to put in hot storage instead.
//
// Two maps rather than one with a tier field, because they are written by two
// different statements with two different guards, and a path can only ever be
// in one of them. Maps rather than slices because every entry is a path with
// one size, and a repeated path would be one row either way.
//
// One accounting consequence of the map: legs are not deduplicated, so the
// same destPath can be restored twice in one run, and the per-file recorder
// this replaced counted it twice. A map counts it once per chunk — so the
// count now depends on where the chunk boundary falls for a run with
// cross-leg duplicates. The ROW is right either way (last write wins, which
// is the last object written); only the counter is affected, and counting a
// path once is the more defensible of the two.
type coldRowBatch struct {
	cold map[string]int64
	hot  map[string]int64
}

// The maps are allocated on first use: a restore with no tiering, or one with
// no cold-carried files, never writes either.
func newColdRowBatch() *coldRowBatch {
	return &coldRowBatch{}
}

// queueColdRow adds the row a file just written to the cold tier needs, and
// flushes when the batch is full.
//
// Not quite reg.queue beside it: that one does NOT flush, because its caller
// pre-checks fits() and flushes BEFORE writing a file that would overflow,
// which is what gives the registration its "nothing written past a refused
// batch" property. This adds first and flushes after, because a tier row may
// only be queued once its bytes are in the store.
func (m *Manager) queueColdRow(ctx context.Context, batch *coldRowBatch, path string, sizeBytes int64, progress *Progress) {
	if batch == nil || m.coldSource == nil {
		return
	}
	if batch.cold == nil {
		batch.cold = make(map[string]int64, coldRowBatchSize)
	}
	batch.cold[path] = sizeBytes
	if len(batch.cold) >= coldRowBatchSize {
		m.flushColdRowsTo(ctx, batch, progress, true)
	}
}

// queueHotRow adds the forced-hot row a cold-carried file needs when this node
// has no cold tier to put it back in.
func (m *Manager) queueHotRow(ctx context.Context, batch *coldRowBatch, path string, sizeBytes int64, progress *Progress) {
	if batch == nil || m.coldSource == nil {
		return
	}
	if batch.hot == nil {
		batch.hot = make(map[string]int64, coldRowBatchSize)
	}
	batch.hot[path] = sizeBytes
	if len(batch.hot) >= coldRowBatchSize {
		m.flushColdRowsTo(ctx, batch, progress, false)
	}
}

// flushColdRows writes both pending sets. Never returns an error: the bytes
// are already in storage, the rest of the restore is sound, and the outcome an
// operator needs is on the progress and in the log.
func (m *Manager) flushColdRows(ctx context.Context, batch *coldRowBatch, progress *Progress) {
	m.flushColdRowsTo(ctx, batch, progress, true)
	m.flushColdRowsTo(ctx, batch, progress, false)
}

// flushColdRowsTo writes one of the two pending sets and accounts for it per
// path, which is the whole point of keeping a recorder here rather than
// reporting through the tier event queue: "41 of 50 written, 9 quarantined" is
// what an operator can act on, and a batch that collapsed it into one error
// would be strictly worse than the unbatched version it replaced.
func (m *Manager) flushColdRowsTo(ctx context.Context, batch *coldRowBatch, progress *Progress, toCold bool) {
	if batch == nil || m.coldSource == nil {
		return
	}

	sizes := batch.cold
	if !toCold {
		sizes = batch.hot
	}
	if len(sizes) == 0 {
		return
	}
	// Taken, not reused: the set is handed to the recorder, so a later queue
	// must not be able to add to the map a flush is reporting on. It also means
	// a flush that fails cannot be retried — the rows are gone from the batch —
	// which is why the context below is detached rather than the caller's.
	if toCold {
		batch.cold = nil
	} else {
		batch.hot = nil
	}

	// DETACHED from the caller's context, at EVERY flush point and not only the
	// final one. The restore runs under a fixed operation_timeout deadline
	// (internal/api/backup_routes.go), and a transaction cannot begin or
	// continue on a dead context — so a deadline landing anywhere inside a
	// flush would lose that whole chunk of rows, for files whose bytes are
	// already in the store, with nothing left to retry from. The window is not
	// small: it is the duration of a full chunk on the single shared
	// connection, which is the very thing this batching exists to shorten.
	//
	// Bounded by its own timeout so a cancelled restore still exits after at
	// most one flush: the copy loop returns at its next ctx.Done() check, and
	// the deferred flush is then the only one left.
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), coldRowFlushTimeout)
	defer cancel()

	submitted := int64(len(sizes))
	var quarantined, failed []string
	var err error
	if toCold {
		quarantined, failed, err = m.coldSource.RecordRestoredColdFiles(flushCtx, sizes)
	} else {
		quarantined, failed, err = m.coldSource.RecordRestoredHotFiles(flushCtx, sizes)
	}

	if err != nil {
		// Nothing in the chunk was written — the recorder runs one
		// transaction per call — so the whole chunk is unrecorded. The bytes
		// are in storage; only the rows are missing, so those files are not
		// yet queryable. Warn rather than fail: the rest of the restore is
		// sound, and on a cluster whose gate runs it the tiering cold sync
		// records the rows. On a standalone node nothing will.
		atomic.AddInt64(&progress.ColdRowsNotRecorded, submitted)
		event := m.logger.Warn().Int64("files", submitted).Err(err)
		if toCold {
			event.Msg("Restored cold-tier files but could not record their tier rows; the bytes are in the cold store and those files are not queryable until rows exist")
		} else {
			event.Msg("Restored cold-tier files into hot storage but could not move their tier rows to hot; the bytes are here and those measurements may read as empty until the rows are corrected")
		}
		return
	}

	// A quarantined row: the key is permanently unusable and tiering has
	// established it can never act on it, so the restore must not act on it
	// either. Counted rather than logged per file, as the tiering cold sync
	// treats the identical condition.
	atomic.AddInt64(&progress.ColdRestoreQuarantineSkipped, int64(len(quarantined)))
	// A path the recorder could not even attempt — it logged why, with a path.
	// Same unqueryable outcome as a failed write, so the same counter.
	atomic.AddInt64(&progress.ColdRowsNotRecorded, int64(len(failed)))

	// Only the cold set counts files restored INTO cold. The hot set's own
	// counter, ColdFilesRestoredToHot, is incremented where the fallback is
	// decided, before the bytes are written — it counts the routing decision,
	// not the row.
	if toCold {
		atomic.AddInt64(&progress.ColdFilesRestoredToCold, submitted-int64(len(quarantined))-int64(len(failed)))
	}
}

// restoreRegistration is the cluster-manifest side of one restore run
// (#1083): the sidecar's entries, the manifest as it stood when the restore
// started, and the files written but not yet registered.
type restoreRegistration struct {
	entries  map[string]ManifestFile
	existing map[string]ManifestFile
	overhead int // registerOpOverhead for this node's ID
	pending  []ManifestFile
	batch    opBatcher
	// pause is the cluster-wide compaction pause held for the run (#1087),
	// checked before every register batch.
	pause CompactionPause
}

// pendingRegistration is a data file's sidecar row, looked up before the file
// is written so the bytes can be verified against it and the batch flushed
// first if the row would overflow it.
type pendingRegistration struct {
	row  ManifestFile
	size int // registerOpBytes(row)
	ok   bool
}

// lookup finds the sidecar row for destPath. When the manifest already lists
// the path, the entry's database, measurement, partition time and created_at
// win over the sidecar's: the manifest is how the cluster labels the file
// (an edge-sync spoke file is labelled with the spoke's own database there,
// while the path's first segment is the spoke), and created_at is when the
// cluster first saw it. The SHA-256 and size stay the sidecar's: they
// describe the bytes being restored.
func (r *restoreRegistration) lookup(destPath string) pendingRegistration {
	p := filepath.ToSlash(destPath)
	row, ok := r.entries[p]
	if !ok {
		return pendingRegistration{}
	}
	if e, listed := r.existing[p]; listed {
		row.Database, row.Measurement, row.PartitionTime = e.Database, e.Measurement, e.PartitionTime
		if !e.CreatedAt.IsZero() {
			row.CreatedAt = e.CreatedAt
		}
	}
	return pendingRegistration{row: row, size: registerOpBytes(row, r.overhead), ok: true}
}

// queue adds a written, verified data file to the pending batch. The caller
// has already made room (restoreDataFiles flushes before the write when the
// row would not fit), so the row always fits here. Batches are capped by
// count AND bytes: in Pattern 1 the primary writer is routinely a Raft
// follower, so each batch is forwarded to the leader inside a 1 MiB frame
// (see manifestBatchBytes). Writes and registrations interleave per batch,
// so an abort strands at most one batch of written-but-unregistered files,
// and names them.
func (r *restoreRegistration) queue(pending pendingRegistration) {
	r.pending = append(r.pending, pending.row)
	r.batch.add(pending.size)
}

// flushRegistrations applies the pending batch as one Raft entry. A refusal
// is a quorum loss or a leader that will not apply, not a blip: the restore
// aborts here rather than carry on to the next batch, and names what it
// leaves behind. The files in the batch are on this node and not in the
// manifest, so peers never pull them and nothing re-registers them; an
// enabled reconciliation sweep (opt-in, and report-only until the operator
// turns its dry run off) removes them after its grace window, 24 h plus clock
// skew, and nothing else does. The recovery is to run the restore again.
//
// Registering a path that is already in the manifest is not a no-op: the FSM
// fires its registration callback on every register (one enqueue and a stat
// per peer), and a changed SHA fires the content-change callback too, so
// since #907 every peer re-pulls that file from this node.
func (m *Manager) flushRegistrations(ctx context.Context, reg *restoreRegistration, progress *Progress) error {
	if len(reg.pending) == 0 {
		return nil
	}
	// A batch registered after the pause was lost could be the one a
	// resumed compaction job races (#1087); the files of this batch are then
	// written but unregistered, which registration_failed reports below.
	if err := checkCompactionPauseLost(reg.pause, progress); err != nil {
		paths := make([]string, len(reg.pending))
		for i, f := range reg.pending {
			paths[i] = f.Path
		}
		progress.RegistrationFailed += int64(len(reg.pending))
		progress.RegistrationFailedSample = appendSample(progress.RegistrationFailedSample, paths)
		m.setProgress(progress)
		return err
	}
	batch := reg.pending
	if err := m.cluster.BatchRegister(ctx, batch); err != nil {
		paths := make([]string, len(batch))
		for i, f := range batch {
			paths[i] = f.Path
		}
		progress.RegistrationFailed += int64(len(batch))
		progress.RegistrationFailedSample = appendSample(progress.RegistrationFailedSample, paths)
		m.setProgress(progress)
		m.logger.Error().
			Err(err).
			Int("files", len(batch)).
			Strs("sample", progress.RegistrationFailedSample).
			Msg("Cluster manifest refused a batch of restored files; aborting the restore. The files are on this node but not in the manifest: peers will not replicate them, nothing re-registers them, and an enabled reconciliation sweep removes them after its grace window; nothing else does. Run the restore again")
		return fmt.Errorf("restore aborted: the cluster manifest refused to register %d restored files (registration_failed_sample); they are on this node but not in the manifest, so run the restore again once the manifest accepts writes: %w", len(batch), err)
	}
	progress.FilesRegistered += int64(len(batch))
	reg.pending = nil
	reg.batch.reset()
	m.setProgress(progress)
	return nil
}

// appendSample adds paths to a sample bounded like every other sample here.
func appendSample(sample, paths []string) []string {
	for _, p := range paths {
		if len(sample) >= unaddressableSampleCap {
			break
		}
		sample = append(sample, p)
	}
	return sample
}

// replaceDatabases is the delete half of a replace-mode restore. For every
// database the backup manifest names, the current cluster-manifest entries
// are removed, in batches, EXCEPT the paths this run is about to write. The
// two sets are kept disjoint on purpose: a manifest delete reaches the
// local-delete workers, which unlink after a short grace, and a path both
// deleted and rewritten would race its own unlink between the write and the
// batched register. A path in both sets is simply overwritten and
// re-registered; where its SHA changed, peers re-pull it (#907).
//
// Manifest first, then storage. On a local backend the coordinator's delete
// callback unlinks each path on every node, so nothing more is done here. On
// a shared backend (Pattern 2) that callback takes no action, by design, and
// the issuer removes the objects, exactly as retention does: the objects are
// deleted here after their manifest entries, so a failure between the two
// leaves an unregistered object the reconciliation sweep removes, never a
// manifest entry with no object.
//
// Refused when the backup is known incomplete: deleting live data to replace
// it with a set that is missing files is the one outcome this cannot undo. A
// skip the backup reconciled (the file had left the manifest by the end of
// the run) is not a gap, and neither are unregistered-skipped files, unless
// they were a large share of what the node listed: then the backup was taken
// against a stale or partial manifest view and describes much less than the
// node held, the way checkSkipRatio treats a run that skipped too much.
//
// A compaction job finishing on a restored database while this runs would
// manifest-delete inputs whose output the restore has just replaced, with no
// check that the output is still there (the watcher commits in two phases);
// the caller holds the cluster-wide compaction pause for the run (#1087), and
// pause is checked before every delete chunk.
//
// A scoped backup (#1084) selects the current entries by the PATH first
// segment being in the manifest's Scope, not by the entry's Database label:
// the label is the canonical database of an edge-sync spoke file while the
// scope is its storage-root segment, so a scope of ["spoke1"] replaces the
// spoke's files. A scope name absent from Databases (no data file for it in
// the backup) was refused before this runs (CheckScopedReplace), because for
// it replace would only delete. An unscoped backup selects by Database as
// before.
func (m *Manager) replaceDatabases(ctx context.Context, manifest *Manifest, files []string, dataPrefix string, skipInputs map[string]bool, current []ManifestFile, progress *Progress, pause CompactionPause) error {
	sc := scopeFromManifest(manifest.Scope)
	scopedDatabases := len(manifest.Databases)
	if !sc.empty() {
		scopedDatabases = len(sc.names)
	}
	// Manifest.ColdFilesExcluded (#1085 stage B3) is deliberately NOT in the
	// refusal below, and adding "the other incompleteness count" to it is the
	// mistake this comment exists to prevent. The counts here describe files
	// the backup SHOULD have carried and does not, so replacing a database
	// with the backup loses the difference. A cold-tier file is a different
	// population: replace cannot delete it, so refusing on it would prevent
	// nothing. Three independent mechanisms say so, not one:
	//
	//   - the delete set is built only from the cluster manifest entries this
	//     function is handed, and tiering removes a migrated file from that
	//     manifest as phase 2 of the migration (tiering/migrator.go,
	//     releaseHotCopies, manifest first), so a cold object is never in it;
	//   - the shared-backend object delete runs against m.dataStorage, which
	//     is the HOT backend, and cannot address the cold store at all;
	//   - the hot-side tier report cannot disturb a cold row either:
	//     recordHotFileIfNotCold binds its ON CONFLICT update to tier = 'hot'.
	//     Since #1086 a restore DOES write cold rows, for the files it puts
	//     back in the cold tier — through its own recorder, deliberately, not
	//     through that one. So a replace-mode restore of a backup carrying
	//     cold files flips those paths' rows hot-to-cold. That is the restore
	//     being authoritative about what it just wrote, and it is still not a
	//     delete of anything.
	//
	// What that buys is narrow and worth stating exactly: the refusal would
	// prevent nothing. It is NOT a claim that restoring a partly-cold backup
	// leaves tiering consistent. A file that was hot when the backup was taken
	// and migrated afterwards is restored to hot storage and registered, while
	// its tier row still says cold and the report that should flip it is
	// silently refused by that same ON CONFLICT clause. The query path then
	// reads that file twice wherever the measurement still has a hot row, and
	// where it does not, the measurement resolves to the cold glob alone and
	// the restored copy is invisible. That is a separate, pre-existing
	// defect about files the backup DOES carry; it happens with this count at
	// zero, it happens in merge mode too, and gating on this count would not
	// address it.
	//
	// Nor is this the gate that protects cold data from a restore. The one
	// path that can orphan a cold object is opts.RestoreMetadata, which
	// replaces tier_files wholesale with the backup-time snapshot; it is
	// refused on a cluster node and allowed standalone, and it is unrelated to
	// this count.
	//
	// Were ColdFilesExcluded added here, every partly-cold database would
	// refuse a replace-mode restore — the normal state of any deployment with
	// tiering on — making tiering and replace-mode restore mutually exclusive.
	unreconciled := manifest.SkippedFiles - manifest.SkippedReconciled
	if unreconciled > 0 || manifest.UnaddressableFiles > 0 || manifest.ManifestOnlyFiles > 0 ||
		progress.MissingFiles > 0 || progress.UnaddressableFiles > 0 {
		return fmt.Errorf("restore refused: mode replace would remove the current files of %d databases and the backup is incomplete (backup skipped %d of which %d reconciled, backup unaddressable %d, backup manifest-only %d, missing from backup storage %d, unaddressable in backup storage %d); use mode merge, or take a complete backup first",
			scopedDatabases, manifest.SkippedFiles, manifest.SkippedReconciled, manifest.UnaddressableFiles, manifest.ManifestOnlyFiles, progress.MissingFiles, progress.UnaddressableFiles)
	}
	if listed := manifest.TotalFiles - manifest.AuxiliaryFiles + manifest.UnregisteredSkipped; manifest.UnregisteredSkipped > 0 &&
		float64(manifest.UnregisteredSkipped) > maxSkipRatio*float64(listed) {
		return fmt.Errorf("restore refused: mode replace would remove the current files of %d databases, and %d of the %d data files the backup node listed were not in its cluster manifest (>%.0f%%), so the backup was taken against a stale or partial manifest view and holds much less than that node did; use mode merge, or take the backup again on a caught-up primary",
			scopedDatabases, manifest.UnregisteredSkipped, listed, maxSkipRatio*100)
	}
	m.logger.Info().
		Str("backup_id", manifest.BackupID).
		Int("databases", scopedDatabases).
		Msg("Replace mode restore starting under the cluster-wide compaction pause: no compaction job can commit on the restored databases until it completes")
	databases := make(map[string]bool, len(manifest.Databases))
	for _, db := range manifest.Databases {
		databases[db.Name] = true
	}
	if sc.empty() && len(databases) == 0 {
		return nil
	}
	owns := func(e ManifestFile, p string) bool {
		if sc.empty() {
			return databases[e.Database]
		}
		return sc.ownsData(p)
	}
	writes := make(map[string]bool, len(files))
	for _, f := range files {
		dest := filepath.ToSlash(strings.TrimPrefix(f, dataPrefix))
		if dest != "" && dest != f && !skipInputs[dest] && isRegistrableDataFile(dest) {
			writes[dest] = true
		}
	}

	var paths []string
	for _, e := range current {
		p := filepath.ToSlash(e.Path)
		if owns(e, p) && !writes[p] {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return nil
	}
	m.logger.Warn().
		Int("files", len(paths)).
		Int("databases", scopedDatabases).
		Msg("Replace mode: removing the current cluster-manifest entries of the restored databases before writing the backup")

	sharedBackend := m.dataStorage.Type() != "local"
	for _, c := range manifestChunks(len(paths), func(i int) int { return deleteOpBytes(paths[i], restoreReplaceReason) }) {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := paths[c[0]:c[1]]
		if err := checkCompactionPauseLost(pause, progress); err != nil {
			return fmt.Errorf("restore aborted with %d current files of the restored databases already removed from the manifest and none written: %w", progress.ReplacedFiles, err)
		}
		if err := m.cluster.BatchDelete(ctx, chunk, restoreReplaceReason); err != nil {
			return fmt.Errorf("restore aborted before any file was written: the cluster manifest refused to remove %d current files of the restored databases (%d already removed): %w", len(chunk), progress.ReplacedFiles, err)
		}
		progress.ReplacedFiles += int64(len(chunk))
		m.setProgress(progress)
		if sharedBackend {
			m.deleteReplacedObjects(ctx, chunk)
		}
	}
	return nil
}

// deleteReplacedObjects removes manifest-deleted objects from a shared
// backend. Best effort per object, as retention: the manifest entry is
// already gone, so a failure leaves an unregistered object, which is the safe
// side; an enabled reconciliation sweep removes it after its grace window,
// and nothing else does.
func (m *Manager) deleteReplacedObjects(ctx context.Context, paths []string) {
	if bd, ok := m.dataStorage.(storage.BatchDeleter); ok {
		if err := bd.DeleteBatch(ctx, paths); err == nil {
			return
		} else {
			m.logger.Warn().Err(err).Int("files", len(paths)).Msg("Batch delete of replaced objects failed; deleting one by one")
		}
	}
	for _, p := range paths {
		if err := m.dataStorage.Delete(ctx, p); err != nil {
			m.logger.Warn().Err(err).Str("path", p).Msg("Failed to delete a replaced object from shared storage; its manifest entry is gone, so an enabled reconciliation sweep removes it after its grace window and nothing else does")
		}
	}
}

// errSidecarMismatch marks a backup object whose bytes do not match the
// sidecar row that describes it (size or SHA-256). Detected on the temp-file
// hop, before anything is written to data storage, so the live copy is
// untouched. The restore counts and names the file and ends failed.
var errSidecarMismatch = errors.New("backup object does not match the sidecar")

// restoredManifest is the subset of compaction.Manifest the restore needs,
// decoded here so this package does not import compaction.
type restoredManifest struct {
	OutputPath string   `json:"output_path"`
	OutputSize int64    `json:"output_size"`
	InputFiles []string `json:"input_files"`
}

// consumedInputsInBackup reads every recovery manifest in the backup and
// returns the destination keys of the inputs that must not be restored: those
// of each manifest whose compacted output the backup holds INTACT, meaning
// present and of the size the manifest recorded. The size check is what
// keeps a damaged backup from becoming data loss: recovery deletes an output
// whose size is wrong (a short copy) together with its manifest, so if the
// inputs had been left out the partition's rows would be gone; restoring the
// inputs instead lets recovery take its short-output branch and keep them. A
// manifest that cannot be read or decoded, or an output whose size cannot be
// established, contributes nothing (its inputs are restored, and recovery
// decides later), logged once.
func (m *Manager) consumedInputsInBackup(ctx context.Context, src backupTarget, dataPrefix string, files []string) map[string]bool {
	present := make(map[string]bool, len(files))
	var manifests []string
	for _, f := range files {
		dest := filepath.ToSlash(strings.TrimPrefix(f, dataPrefix))
		present[dest] = true
		if isCompactionState(dest) && strings.HasSuffix(dest, ".json") {
			manifests = append(manifests, f)
		}
	}
	lister, canSize := src.backend.(storage.ObjectLister)
	skip := make(map[string]bool)
	for _, key := range manifests {
		data, err := src.backend.Read(ctx, key)
		if err != nil {
			m.logger.Warn().Err(err).Str("manifest", key).Msg("Cannot read a backed-up compaction manifest; its inputs are restored and left to recovery")
			continue
		}
		var mf restoredManifest
		if err := json.Unmarshal(data, &mf); err != nil || mf.OutputPath == "" {
			m.logger.Warn().Err(err).Str("manifest", key).Msg("Cannot decode a backed-up compaction manifest; its inputs are restored and left to recovery")
			continue
		}
		output := filepath.ToSlash(mf.OutputPath)
		if !present[output] {
			continue // the job never uploaded: restore its inputs, recovery retries
		}
		if !canSize || !m.backupObjectHasSize(ctx, lister, dataPrefix+output, mf.OutputSize) {
			m.logger.Warn().Str("manifest", key).Str("output", output).Int64("expected_size", mf.OutputSize).
				Msg("Backed-up compacted output is not intact or cannot be sized; its inputs are restored and recovery will discard the output")
			continue
		}
		for _, in := range mf.InputFiles {
			in = filepath.ToSlash(in)
			if present[in] {
				skip[in] = true
			}
		}
	}
	return skip
}

// backupObjectHasSize reports whether the backup object at key has exactly
// size bytes. Any listing failure is "no".
func (m *Manager) backupObjectHasSize(ctx context.Context, lister storage.ObjectLister, key string, size int64) bool {
	objects, err := lister.ListObjects(ctx, key)
	if err != nil {
		return false
	}
	for _, o := range objects {
		if filepath.ToSlash(o.Path) == filepath.ToSlash(key) {
			return o.Size == size
		}
	}
	return false
}

// streamRestoreFile streams a file from backup storage to data storage via a temp file,
// avoiding loading the entire file into memory (important for large Parquet files).
//
// Only a backup-storage read failure is wrapped with errRestoreRead (making it
// skippable by the caller); temp file, seek, and data-storage write failures are
// returned unwrapped and are fatal to the restore. A ReadTo failure caused by
// the temp file itself (see trackingWriter) is fatal, not a read.
//
// expect, when non-nil, is the sidecar row for the file (#1083): the bytes are
// hashed on the hop into the temp file and both size and SHA-256 must match
// before anything is written to data storage; otherwise errSidecarMismatch.
// nil (a standalone restore) hashes nothing and writes as before.
// dest is the store the file is written TO and destName names it in error
// text: the hot data backend, or this node's cold tier for a file the backup
// read from cold (#1086 stage C). The transport does not care which — both are
// a storage.Backend and the temp-file hop is the same.
func (m *Manager) streamRestoreFile(ctx context.Context, src backupTarget, srcPath, destPath string, expect *ManifestFile, dest storage.Backend, destName string) (int64, error) {
	tmpFile, err := createTempFile("arc-restore-*.parquet")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// Stream from backup storage to temp file, hashing on the way when the
	// bytes have to be verified.
	tw := &trackingWriter{w: tmpFile}
	var dst io.Writer = tw
	var hasher hash.Hash
	if expect != nil {
		hasher = sha256.New()
		dst = io.MultiWriter(tw, hasher)
	}
	if err := src.backend.ReadTo(ctx, srcPath, dst); err != nil {
		return 0, classifyReadTo(srcPath, err, tw.err)
	}

	// Get size and rewind for upload
	info, err := tmpFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat temp file: %w", err)
	}
	size := info.Size()

	if expect != nil {
		if size != expect.SizeBytes {
			return 0, fmt.Errorf("%w: %s is %d bytes, the sidecar recorded %d", errSidecarMismatch, destPath, size, expect.SizeBytes)
		}
		if sum := hex.EncodeToString(hasher.Sum(nil)); expect.SHA256 != "" && sum != expect.SHA256 {
			return 0, fmt.Errorf("%w: %s hashes to %s, the sidecar recorded %s", errSidecarMismatch, destPath, sum, expect.SHA256)
		}
	}

	if _, err := tmpFile.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Stream from temp file to the destination store
	if err := dest.WriteReader(ctx, destPath, tmpFile, size); err != nil {
		m.cleanupPartialWrite(ctx, dest, destPath)
		return 0, fmt.Errorf("failed to write to %s: %w", destName, err)
	}

	return size, nil
}

// restoreSQLite restores the SQLite database from the backup.
// It creates a .before-restore backup of the current database first.
func (m *Manager) restoreSQLite(ctx context.Context, src backupTarget, backupID string) error {
	if err := m.restoreSQLiteFile(ctx, src, backupID, "arc.db", m.sqliteDBPath); err != nil {
		return err
	}

	// Restore the Iceberg SQL catalog when it was backed up as a separate
	// database. Absent for a backup taken before the catalog was split out, or
	// one where the catalog lived in the shared database (already restored
	// above) — neither is an error.
	//
	// Presence is tested with Exists rather than by classifying the read error:
	// every backend implements Exists with an explicit (bool, error), whereas
	// not-found error text differs per backend ("file not found" locally,
	// wrapped SDK errors for S3/Azure). Matching on text would silently invert
	// — reporting "no catalog in this backup" for a real read failure — if the
	// backup destination ever stops being local.
	if m.icebergCatalogDBPath != "" {
		srcPath := fmt.Sprintf("%s/metadata/%s", backupID, icebergCatalogDBName)
		exists, err := src.backend.Exists(ctx, srcPath)
		if err != nil {
			return fmt.Errorf("failed to check for Iceberg catalog in backup: %w", err)
		}
		if !exists {
			m.logger.Info().Str("backup_id", backupID).
				Msg("Backup contains no separate Iceberg catalog; skipping")
			return nil
		}
		if err := m.restoreSQLiteFile(ctx, src, backupID, icebergCatalogDBName, m.icebergCatalogDBPath); err != nil {
			return fmt.Errorf("failed to restore Iceberg catalog: %w", err)
		}
		m.logger.Info().Str("backup_id", backupID).Msg("Iceberg catalog database restored")
	}

	return nil
}

// restoreSQLiteFile STAGES one SQLite database from metadata/<srcName> in the
// backup as <destPath>.pending-restore. The live database is never touched:
// applying a restore over a running server's database — even by atomic
// rename — leaves existing connections on the old inode while new pool
// connections open the restored file, splitting state across both (#635).
// ApplyPendingRestores applies the staged file at the next boot, before any
// subsystem opens the database, and takes the .before-restore safety copy at
// that point (when it can be made complete and cheaply). Restaging overwrites
// a previous staging: the last restore before restart wins. An operator can
// cancel by deleting the .pending-restore file before restarting.
func (m *Manager) restoreSQLiteFile(ctx context.Context, src backupTarget, backupID, srcName, destPath string) error {
	srcPath := fmt.Sprintf("%s/metadata/%s", backupID, srcName)

	// Stream from backup storage into the staging file (#639 item 8): the
	// shared database can be multi-GB on audit-heavy deployments, and
	// buffering it in memory violates the streaming rule everywhere else in
	// this package. CreateTemp creates 0600 before any byte lands, and the
	// staging file is renamed into the pending path only on full success.
	staging, err := os.CreateTemp(filepath.Dir(destPath), ".restore-staging-*")
	if err != nil {
		return fmt.Errorf("failed to create restore staging file: %w", err)
	}
	stagingPath := staging.Name()
	if err := src.backend.ReadTo(ctx, srcPath, staging); err != nil {
		staging.Close()
		os.Remove(stagingPath)
		return fmt.Errorf("failed to stream SQLite backup into staging: %w", err)
	}
	if err := staging.Close(); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("failed to close staged restore: %w", err)
	}
	if err := os.Chmod(stagingPath, 0600); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("failed to restrict staged restore: %w", err)
	}
	pendingPath := StagePath(destPath)
	if err := os.Rename(stagingPath, pendingPath); err != nil {
		os.Remove(stagingPath)
		return fmt.Errorf("failed to stage restored SQLite database: %w", err)
	}

	m.logger.Warn().
		Str("backup_id", backupID).
		Str("database", srcName).
		Str("staged_at", pendingPath).
		Msg("SQLite restore staged; it is applied at the next server start")
	return nil
}

// restoreConfig restores the arc.toml config file from the backup.
// It creates a .before-restore backup of the current config first.
func (m *Manager) restoreConfig(ctx context.Context, src backupTarget, backupID string) error {
	srcPath := fmt.Sprintf("%s/config/arc.toml", backupID)
	data, err := src.backend.Read(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("failed to read config backup: %w", err)
	}

	// Safety: backup the current config before overwriting
	if _, statErr := os.Stat(m.configPath); statErr == nil {
		preRestorePath := m.configPath + ".before-restore"
		currentData, err := os.ReadFile(m.configPath)
		if err == nil {
			if err := os.WriteFile(preRestorePath, currentData, 0600); err != nil {
				m.logger.Warn().Err(err).Msg("Failed to create pre-restore backup of config")
			} else {
				m.logger.Info().Str("path", preRestorePath).Msg("Created pre-restore backup of config")
			}
		}
	}

	if err := os.WriteFile(m.configPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file restored (restart required)")
	return nil
}
