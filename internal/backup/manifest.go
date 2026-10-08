package backup

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Manifest describes the contents of a backup.
type Manifest struct {
	Version    string    `json:"version"`
	BackupID   string    `json:"backup_id"`
	CreatedAt  time.Time `json:"created_at"`
	BackupType string    `json:"backup_type"` // "full" (future: "incremental")
	// Scope names the databases the backup was limited to (#1084), sorted:
	// storage-root segments, so an edge-sync spoke appears as the spoke. The
	// backup holds only their data files, schema anchors and compaction
	// recovery state, never the SQLite metadata or the Iceberg catalog, and a
	// restore of it touches only those databases (on a cluster node, in
	// replace mode unless the request names a mode; replace is refused when
	// the backup holds no data file for one of them, see CheckScopedReplace).
	// Absent for a whole-instance backup, which is also how a manifest
	// written before this field reads. BackupType stays "full" for every
	// backup: clients already read that field, and the scope is here.
	Scope          []string       `json:"scope,omitempty"`
	Databases      []DatabaseInfo `json:"databases"`
	TotalFiles     int64          `json:"total_files"`
	TotalSizeBytes int64          `json:"total_size_bytes"`
	// SkippedFiles counts data files that were listed and inventoried above but
	// could not be read from source storage at copy time, or whose backup
	// destination key would exceed the storage key limit (a source key longer
	// than storage.MaxUsableKeyLen minus the <backupID>/data/ prefix AND minus
	// the destination target's own object-key prefix, which counts since
	// #1085 stage B2b-1; #761). The per-backup figure is reported as
	// max_source_key_bytes in the log line that names each skip.
	// The backup log names each skipped file and SkippedSample names up to 32
	// of them (#977). When non-zero the
	// backup is incomplete: TotalFiles/TotalSizeBytes describe what was
	// inventoried, not what was actually stored. Counts the same population as
	// TotalFiles, so a restore can compare the two; Iceberg metadata skips are
	// in SkippedMetadataFiles. (Backups written before that split folded both
	// into this field.)
	SkippedFiles int64 `json:"skipped_files,omitempty"`
	// SkippedMetadataFiles counts auxiliary files under the storage root
	// (Iceberg warehouse metadata, compaction recovery state) that were listed
	// but could not be read at copy time, or whose backup destination key
	// would exceed the storage key limit. They are copied under data/ but are
	// not part of TotalFiles. A compaction manifest whose job finished between
	// the listing and the copy is the expected case. Skips from an
	// outside-root warehouse are in IcebergWarehouse.SkippedFiles.
	SkippedMetadataFiles int64 `json:"skipped_metadata_files,omitempty"`
	// SkippedSample names up to 32 of the files the copy loop skipped, in copy
	// order and for either cause (#977): data files (counted in SkippedFiles)
	// and in-root Iceberg metadata (counted in SkippedMetadataFiles) share the
	// one list; a metadata key carries a /metadata/ segment. Compaction
	// manifests that vanished because their job finished and outside-root
	// warehouse skips are not listed; the log names those. Manifests written
	// before this field read it as nil.
	SkippedSample []string `json:"skipped_sample,omitempty"`
	// SkippedOverlongKeys is how many of the skips in SkippedFiles and
	// SkippedMetadataFiles were for a backup destination key over the storage
	// limit (#761): the permanent cause, fixed by renaming the source file,
	// as opposed to a file that vanished between the listing and the copy.
	SkippedOverlongKeys int64 `json:"skipped_overlong_keys,omitempty"`
	// AuxiliaryFiles counts Parquet objects under a reserved root directory
	// (the field schema anchors under _schema/, #914) that were copied with
	// the data files. They are inside TotalFiles and TotalSizeBytes, because
	// the restore counts every .parquet object it finds against TotalFiles,
	// but they belong to no database and are absent from Databases (#927).
	// Manifests written before this field read it as zero.
	AuxiliaryFiles int64 `json:"auxiliary_files,omitempty"`
	// CompactionStateFiles counts the objects under _compaction_state/ that
	// were listed for this backup (#930): the crash-recovery manifests a
	// restore uses to avoid restoring both a compacted output and the inputs
	// it replaced, plus parked (.quarantined) manifests. Copied under data/,
	// not part of TotalFiles. Manifests written before this field read zero.
	CompactionStateFiles int64 `json:"compaction_state_files,omitempty"`
	// UnaddressableFiles counts data files that exist in source storage but
	// that no listing returns, because their key fails the storage key rules.
	// They were never inventoried and could not be copied, so when this is
	// non-zero the backup is incomplete in a way SkippedFiles does not describe:
	// those were listed and then unreadable, these were never listable at all
	// (#756). UnaddressableSample names up to 32 of them so an operator can find
	// and rename them.
	UnaddressableFiles  int64    `json:"unaddressable_files,omitempty"`
	UnaddressableSample []string `json:"unaddressable_sample,omitempty"`
	// ClusterManifestChecked records that the backup ran on a cluster node
	// and cross-checked the storage listing against the Raft file manifest
	// (#1083). The two counts below are only ever non-zero when it is true;
	// a standalone backup has no manifest to disagree with.
	ClusterManifestChecked bool `json:"cluster_manifest_checked,omitempty"`
	// UnregisteredSkipped counts database data files that were in this
	// node's storage listing but absent from the cluster manifest at both the
	// start and the end of the run, and so were deliberately NOT copied: the
	// manifest is what says a file is data, and a file the manifest does not
	// list is a compaction or retention input awaiting unlink, a pre-cluster
	// file, or a dropped registration, which the reconciliation sweep treats
	// the same way. They are outside TotalFiles and Databases.
	// UnregisteredSample names up to 32 of them.
	UnregisteredSkipped int64    `json:"unregistered_skipped,omitempty"`
	UnregisteredSample  []string `json:"unregistered_sample,omitempty"`
	// ManifestOnlyFiles counts manifest entries this node did not hold when
	// the run ended: files the cluster has that this node has not pulled yet
	// (a new primary still catching up). The backup is incomplete by that
	// many files, and it is the one gap a re-run on a caught-up node closes.
	// ManifestOnlySample names up to 32 of them.
	ManifestOnlyFiles  int64    `json:"manifest_only_files,omitempty"`
	ManifestOnlySample []string `json:"manifest_only_sample,omitempty"`
	// LeftManifestDuringRun counts data files that were copied and then
	// removed from the backup again because the cluster manifest no longer
	// listed them when it was re-read at the end of the data copy: the
	// cluster saying the file is no longer data (a compaction input whose
	// phase-2 delete landed during the run, a retention delete, a tiering
	// migration). Outside TotalFiles and Databases; LeftManifestSample names
	// up to 32 of them. Not a gap: the backup holds what the cluster held.
	LeftManifestDuringRun int64    `json:"left_manifest_during_run,omitempty"`
	LeftManifestSample    []string `json:"left_manifest_sample,omitempty"`
	// SkippedReconciled is how many of SkippedFiles had left the cluster
	// manifest by the end of the run: the file could not be read at copy
	// time because compaction, retention or tiering had removed it, so the
	// skip is not missing data. A replace-mode restore does not count them;
	// every other skip (still registered, or an overlong key) still makes the
	// backup incomplete.
	SkippedReconciled int64 `json:"skipped_reconciled,omitempty"`
	HasMetadata       bool  `json:"has_metadata"`
	// HasIcebergCatalog records that the Iceberg SQL catalog was stored as a
	// separate database (metadata/iceberg-catalog.db) because the operator
	// configured iceberg.catalog_db_path away from the shared database. When
	// false the catalog either lives in the shared database or does not exist.
	HasIcebergCatalog bool `json:"has_iceberg_catalog,omitempty"`
	HasConfig         bool `json:"has_config"`
	// IcebergWarehouse describes Iceberg table metadata copied from a
	// warehouse that lives OUTSIDE the data storage root, stored under
	// <backup_id>/iceberg/. Nil when Iceberg export is off or the warehouse is
	// under the root, where its metadata travels with the data listing (#637).
	IcebergWarehouse *IcebergWarehouseInfo `json:"iceberg_warehouse,omitempty"`
	// IcebergNamespaceFilesExcluded counts the files under the Iceberg
	// namespace directories of the scoped databases (<prefix>_<db>.db/, in
	// the storage root or in an outside-root warehouse) that a scoped backup
	// (#1084) deliberately did not copy: the Iceberg catalog that makes them
	// tables is instance-wide and travels with include_metadata, which a
	// scoped backup cannot carry. IcebergNamespacesExcluded names those
	// directories (relative to the storage root, or to the warehouse). Zero
	// and absent for an unscoped backup, when Iceberg export is off, or when
	// the warehouse is an object store, which this stage does not count (the
	// namespace directories are still excluded: they are never listed).
	IcebergNamespaceFilesExcluded int64    `json:"iceberg_namespace_files_excluded,omitempty"`
	IcebergNamespacesExcluded     []string `json:"iceberg_namespaces_excluded,omitempty"`
	// ColdFilesExcluded counts tier rows whose data this backup does NOT
	// carry. That one sentence covers both configurations:
	//
	//   - with a cold tier this node can read (#1086 stage C), the backup
	//     carries the cold objects, so the only rows it cannot carry are the
	//     ones whose object is MISSING from the cold store — data that is
	//     genuinely gone;
	//   - with no cold tier, every cold row qualifies, which is exactly what
	//     this field meant when stage B introduced it.
	//
	// It is INFORMATIONAL and
	// deliberately joins no refusal — see the comment above the incompleteness
	// refusal in restore.go, which explains why adding it there would prevent
	// nothing. ColdFilesExcludedDatabases is the per-database breakdown,
	// because one total over several databases does not tell an operator which
	// database has data the backup is missing.
	//
	// Per leg, like the counters on TargetSlice: a database's cold gap is
	// attributed to the target its DATA routes to, so the audit target's
	// manifest reports the audit database's gap and nothing else. Summed into
	// the run-level view by mergeRunManifests.
	//
	// THIS NODE's tier metadata, which is the only view a backup has. On a
	// cluster where one node migrates and the others learn about it through
	// the cold-tier metadata sync, a node whose sync has not run or has failed
	// holds fewer rows than the cluster has cold files and reports the lower
	// number without an error.
	//
	// Zero and absent in FOUR cases, which the JSON cannot distinguish: when
	// tiering is off; when nothing has been migrated; when the count could not
	// be taken (the WARN that names the failure is the only record of this
	// one); and — the one that misleads — when this node has no tiering
	// LICENCE. The tiering manager is licence-gated at startup, so an
	// unlicensed node wires no counter even though its cold objects and tier
	// rows are still there. An absent field is therefore NOT evidence that
	// nothing was migrated. A licence that lapses while the process runs keeps
	// reporting; the restart after a lapse does not.
	ColdFilesExcluded          int64            `json:"cold_files_excluded,omitempty"`
	ColdFilesExcludedDatabases map[string]int64 `json:"cold_files_excluded_databases,omitempty"`
	// ColdFiles counts the files in this leg that were read from the COLD tier
	// (#1086 stage C), with ColdSizeBytes their total. They are also in
	// TotalFiles, TotalSizeBytes and the per-database inventory, because they
	// are data the backup carries like any other; this is the figure that says
	// how much of it came from cold. Zero on a node with no cold tier, and on
	// every backup taken before stage C.
	//
	// The sidecar carries the tier PER FILE, which is what a restore reads;
	// these are the aggregate an operator sees.
	ColdFiles     int64 `json:"cold_files,omitempty"`
	ColdSizeBytes int64 `json:"cold_size_bytes,omitempty"`
	// ColdObjectsUnrecorded counts cold objects the listing returned that this
	// node has no tier row for. They ARE in the backup — the listing is the
	// authority on what exists, and refusing them would mean a node with
	// incomplete metadata backs up no cold data at all. The count is here
	// because it is also a report about the node: cold metadata that does not
	// match the store.
	//
	// Not necessarily transient. The tiering cold sync that would record the
	// missing rows only runs on a cluster with shared storage or replication,
	// so on a standalone node, and on a local-storage cluster without
	// replication, nothing will ever record them.
	ColdObjectsUnrecorded int64 `json:"cold_objects_unrecorded,omitempty"`
	// ColdDedupSkipped counts paths that were in BOTH the hot and the cold
	// listing — the window a migration opens by copying to cold before
	// releasing the hot copy — where the cold copy was taken and the hot one
	// dropped. One path, one copy, one sidecar row.
	ColdDedupSkipped int64 `json:"cold_dedup_skipped,omitempty"`
	// ColdRowsStaleButHot counts tier rows that say cold, have no object in the
	// cold store, and whose file the backup carried from HOT storage anyway.
	// NOT a gap — the data is in the backup — but a metadata disagreement an
	// operator should see, and reachable persistently on a node whose
	// reconciliation is role gated.
	ColdRowsStaleButHot int64 `json:"cold_rows_stale_but_hot,omitempty"`
	// Target names the configured backup target this backup was written to
	// (#1085 stage B2b-1), absent when it went to backup.local_path as
	// backups did before targets existed.
	//
	// IT IS A LABEL, NEVER A LOOKUP. The backend a restore reads from comes
	// from LOCAL configuration and nothing else, by the same rule already
	// established for the one other destination-naming field in this manifest
	// (see restoreIcebergWarehouse, which uses this node's configured
	// warehouse and not the manifest's path): the manifest is data read from
	// backup storage, so a field in it must never select the credentials or
	// the location used to read further bytes. A restore from a manifest whose
	// Target names a target this node does not have still works, because the
	// bytes were already found at the destination this node is configured
	// with; the field tells an operator where the backup came from.
	Target string `json:"target,omitempty"`
	// RunTargets names every target the run committed a manifest to, sorted
	// (#1085 stage B2b-2), and IsDefaultTarget says this manifest is the
	// default leg's.
	//
	// On EVERY leg's manifest, which is what makes a manifest found alone
	// self-describing: a restore learns the run's shape from whichever leg it
	// can read and never depends on the default target being reachable for it.
	// Absent for a run with one destination, where there is nothing to name,
	// and for every backup taken before this stage.
	//
	// They inherit Target's rule above: a LABEL, never a lookup. A restore
	// resolves these names against LOCAL configuration and refuses what it
	// cannot resolve when the run spans several targets, because the other
	// legs' bytes are somewhere this node must reach; a single-target run
	// resolves nothing, because its bytes were already found at the
	// destination this node is configured with.
	RunTargets      []string `json:"run_targets,omitempty"`
	IsDefaultTarget bool     `json:"is_default_target,omitempty"`
	// OwnerInstanceID identifies the Arc instance that wrote this backup: the
	// cluster name when clustered, a persistent per-instance UUID standalone
	// (see identity.go). Absent for every backup written before this field,
	// and for an instance that has no identity — and an ABSENT owner reads as
	// the reading instance's own, because the alternative would hide every
	// pre-upgrade backup from the listing.
	//
	// A restore does NOT adopt it. Identity is local configuration, by the
	// same rule as Target above; a restore onto fresh hardware keeps the
	// identity that hardware has, and the echo of this value is what tells
	// the operator the backup belongs to another instance.
	OwnerInstanceID string `json:"owner_instance_id,omitempty"`
}

// IcebergWarehouseInfo records the outside-root Iceberg warehouse a backup
// carries. ConfiguredPath is the source node's iceberg.warehouse as an
// absolute path, which is the spelling the catalog's metadata locations use;
// Path is that directory with symlinks resolved, which is what was walked. A
// restore resolves the tables only when ConfiguredPath, evaluated on the
// target node, lands in the directory the files are written to.
type IcebergWarehouseInfo struct {
	Path           string `json:"path"`
	ConfiguredPath string `json:"configured_path,omitempty"`
	FileCount      int64  `json:"file_count"`
	SizeBytes      int64  `json:"size_bytes"`
	// SkippedFiles counts warehouse files that were walked but could not be
	// read at copy time. They are not included in SkippedMetadataFiles.
	SkippedFiles int64 `json:"skipped_files,omitempty"`
}

// DatabaseInfo describes a single database within a backup.
type DatabaseInfo struct {
	Name         string            `json:"name"`
	Measurements []MeasurementInfo `json:"measurements"`
	FileCount    int               `json:"file_count"`
	SizeBytes    int64             `json:"size_bytes"`
}

// MeasurementInfo describes a single measurement within a database backup.
type MeasurementInfo struct {
	Name      string `json:"name"`
	FileCount int    `json:"file_count"`
	SizeBytes int64  `json:"size_bytes"`
}

// BackupSummary is a compact representation of a backup for listing.
type BackupSummary struct {
	BackupID   string    `json:"backup_id"`
	CreatedAt  time.Time `json:"created_at"`
	BackupType string    `json:"backup_type"`
	// Scope mirrors Manifest.Scope (#1084): the databases a scoped backup
	// was limited to; absent for a whole-instance backup.
	Scope         []string `json:"scope,omitempty"`
	TotalFiles    int64    `json:"total_files"`
	TotalBytes    int64    `json:"total_size_bytes"`
	DatabaseCount int      `json:"database_count"`
	// The manifest's incompleteness counts, so the listing says what the
	// manifest says (#977): TotalFiles is what was inventoried, and these are
	// what was not stored. Omitted when zero, so a complete backup's entry is
	// unchanged. The names are in the manifest (GET /api/v1/backup/:id).
	SkippedFiles         int64 `json:"skipped_files,omitempty"`
	SkippedMetadataFiles int64 `json:"skipped_metadata_files,omitempty"`
	UnaddressableFiles   int64 `json:"unaddressable_files,omitempty"`
	// Cluster cross-check counts (#1083), see Manifest. ManifestOnlyFiles is
	// a real gap; UnregisteredSkipped is a deliberate exclusion.
	UnregisteredSkipped   int64 `json:"unregistered_skipped,omitempty"`
	ManifestOnlyFiles     int64 `json:"manifest_only_files,omitempty"`
	LeftManifestDuringRun int64 `json:"left_manifest_during_run,omitempty"`
	// Target and OwnerInstanceID mirror the manifest's (#1085 stage B2b-1):
	// which configured destination holds the backup, and which instance wrote
	// it. ForeignOwner says the second belongs to a different instance than
	// the one answering, which only ListAllBackups can return — the default
	// listing leaves those out. It is derived per request rather than stored,
	// so it is absent from the manifest.
	Target          string `json:"target,omitempty"`
	OwnerInstanceID string `json:"owner_instance_id,omitempty"`
	ForeignOwner    bool   `json:"foreign_owner,omitempty"`
	// Targets names every destination holding a slice of this backup (#1085
	// stage B2b-2), as the RUN recorded it rather than as this listing managed
	// to read it. Target above is kept and is set only when there is exactly
	// one, so a client that reads the single-target field is never handed one
	// of several.
	Targets []string `json:"targets,omitempty"`
	// ColdFilesExcluded is the run-level cold-tier gap: files the backup did
	// not carry because they are no longer in hot storage. Summed over the
	// legs, so it is a lower bound whenever PartialView is set, like the other
	// counts here. The per-database breakdown is on the manifest, and so at
	// the TOP LEVEL of the detail response — not per target (TargetSlice
	// carries each leg's total only) and not in this listing row.
	ColdFilesExcluded int64 `json:"cold_files_excluded,omitempty"`
	// ColdFiles is how many of TotalFiles came from the cold tier (#1086
	// stage C), so a listing row shows at a glance whether a backup of a
	// tiered deployment actually carried the tiered data.
	ColdFiles int64 `json:"cold_files,omitempty"`
	// PartialView says the counts above are summed over FEWER legs than
	// Targets names, because a target would not answer or has not committed.
	// TotalFiles, TotalBytes and DatabaseCount are then lower bounds, and the
	// matching entry in the listing's incomplete_runs says which targets are
	// unaccounted for. Derived per request, like ForeignOwner, so it is absent
	// from the manifest.
	PartialView bool `json:"partial_view,omitempty"`
}

// Progress tracks the state of a running backup or restore operation.
type Progress struct {
	Operation string `json:"operation"` // "backup" or "restore"
	BackupID  string `json:"backup_id"`
	Status    string `json:"status"` // "running", "completed", "failed"
	// Scope (#1084): the databases a scoped backup is limited to, and for a
	// restore the scope of the backup being restored; absent for a
	// whole-instance backup.
	Scope          []string `json:"scope,omitempty"`
	TotalFiles     int64    `json:"total_files"`
	ProcessedFiles int64    `json:"processed_files"`
	SkippedFiles   int64    `json:"skipped_files"`
	// UnaddressableFiles: for a backup, mirrors Manifest.UnaddressableFiles for
	// live progress. For a restore, data files that are present in backup
	// storage but that no listing returns (a dot-prefixed name an object store
	// handed back at backup time, a key an older Arc wrote), so the restore
	// could not copy them; UnaddressableSample names up to 32 of them so the
	// operator can rename them in the backup and re-run.
	UnaddressableFiles  int64    `json:"unaddressable_files,omitempty"`
	UnaddressableSample []string `json:"unaddressable_sample,omitempty"`
	// SkippedSample names up to unaddressableSampleCap files. For a restore,
	// the backup objects it could not read. For a backup, the files its copy
	// loops skipped (the manifest's skipped_sample, #977), published once after
	// the copy phases, so a run the skip ratio then fails — which writes no
	// manifest — still names them here until the next operation starts.
	SkippedSample []string `json:"skipped_sample,omitempty"`
	// Restore only (#930). ConsumedInputsSkipped counts input files the
	// backup held alongside the compacted output that replaced them, per a
	// backed-up recovery manifest; they were deliberately not restored, so
	// the restored store serves each row once. CompactionStateRestored counts
	// recovery manifests (.json) put back; the next compaction cycle deletes
	// each after firing its receipt hooks.
	ConsumedInputsSkipped   int64 `json:"consumed_inputs_skipped,omitempty"`
	CompactionStateRestored int64 `json:"compaction_state_restored,omitempty"`
	// MissingFiles counts data files the backup's manifest inventoried that
	// are neither listed nor unaddressable in backup storage: they are gone.
	// Restore only.
	MissingFiles int64 `json:"missing_files,omitempty"`
	// BackupSkippedFiles and BackupUnaddressableFiles mirror the restored
	// backup's own manifest: files it already lacked when it was taken, so the
	// operator can tell a gap that predates the restore from one it caused.
	// Restore only.
	BackupSkippedFiles int64 `json:"backup_skipped_files,omitempty"`
	// IcebergWarehouseFilesSkipped counts backup objects under iceberg/ that a
	// restore left out because this node has no Iceberg warehouse outside its
	// storage root to put them in (the log names the fix).
	IcebergWarehouseFilesSkipped int64 `json:"iceberg_warehouse_files_skipped,omitempty"`
	BackupUnaddressableFiles     int64 `json:"backup_unaddressable_files,omitempty"`
	// ColdFilesExcluded: for a backup, the run-level cold-tier gap, published
	// once with the other pre-copy decisions — which is BEFORE the copy
	// phase, so a run that fails later keeps the figure here beside
	// status="failed". That is deliberate: it was true of the data when it was
	// taken, and no manifest lands for a failed run, so this is the only place
	// the gap is visible for one. For a restore,
	// BackupColdFilesExcluded mirrors the restored backup's own figure, so the
	// operator can tell data the backup never held from data the restore lost.
	// The per-database breakdown is NOT here: Progress is published by
	// setProgress, which takes a shallow copy, so a snapshot must share no
	// mutable map with the run that is still writing.
	ColdFilesExcluded       int64 `json:"cold_files_excluded,omitempty"`
	BackupColdFilesExcluded int64 `json:"backup_cold_files_excluded,omitempty"`
	// Restore only (#1086 stage C), where a backup carries cold-tier files.
	// ColdFilesRestoredToCold counts files put back in this node's cold tier
	// with their tier row recorded, so the query path can route to them.
	// ColdFilesRestoredToHot counts files the backup read from a cold tier
	// that landed in HOT storage instead, because this node has no cold tier
	// (or has one configured but disabled, where a cold write would be
	// unreadable): the data is here and queryable, it is simply all hot now.
	// ColdRestoreQuarantineSkipped counts files whose tier row is quarantined
	// — the bytes were written, the row was left alone, and the file is not
	// queryable until an operator clears the quarantine.
	// ColdRowsNotRecorded counts files whose bytes reached the cold store but
	// whose row could not be written, which is the same unqueryable outcome
	// from a different cause — and on a node where the cold-metadata sync
	// never runs (standalone, or a cluster without shared storage or
	// replication) nothing will write that row later either.
	ColdFilesRestoredToCold      int64 `json:"cold_files_restored_to_cold,omitempty"`
	ColdFilesRestoredToHot       int64 `json:"cold_files_restored_to_hot,omitempty"`
	ColdRestoreQuarantineSkipped int64 `json:"cold_restore_quarantine_skipped,omitempty"`
	ColdRowsNotRecorded          int64 `json:"cold_rows_not_recorded,omitempty"`
	// Cluster cross-check, backup only (#1083): mirrors of the manifest's
	// UnregisteredSkipped/ManifestOnlyFiles and their samples, published once
	// after the data copy.
	UnregisteredSkipped   int64    `json:"unregistered_skipped,omitempty"`
	UnregisteredSample    []string `json:"unregistered_sample,omitempty"`
	ManifestOnlyFiles     int64    `json:"manifest_only_files,omitempty"`
	ManifestOnlySample    []string `json:"manifest_only_sample,omitempty"`
	LeftManifestDuringRun int64    `json:"left_manifest_during_run,omitempty"`
	LeftManifestSample    []string `json:"left_manifest_sample,omitempty"`
	// Restore only (#1083). Mode is the restore mode that ran ("merge" or
	// "replace"). On a cluster node FilesRegistered counts data files the
	// restore wrote and registered in the manifest; RegistrationFailed counts
	// files written but NOT registered when a manifest batch was refused and
	// the restore aborted (RegistrationFailedSample names up to 32): they are
	// in this node's storage, peers will not replicate them, nothing
	// re-registers them, and an enabled reconciliation sweep removes them
	// after its grace window (the sweep is opt-in and report-only by
	// default), so the recovery is to run the restore again.
	// SidecarMismatches counts backup objects that were NOT written because
	// their bytes do not match the sidecar (size or SHA-256), or the sidecar
	// has no row for them: a damaged backup. The live copy, if any, is left
	// as it was; SidecarMismatchSample names up to 32. ReplacedFiles counts
	// the current manifest entries a replace-mode restore removed before
	// writing. CompactionPause is the cluster-wide compaction pause the
	// restore holds (#1087): "waiting" while every node quiesces, "paused"
	// while the restore runs under it, "released" once resumed, "lost" when
	// it stopped being this restore's pause mid-run (the restore then ends
	// failed). Empty on a standalone node and for a metadata-only restore.
	// BackupUnregisteredSkipped and BackupManifestOnlyFiles mirror
	// the restored backup's own cross-check counts.
	// Targets is one entry per leg of a routed backup (#1085 stage B2b-2), so
	// an operator watching a run can see which destination is being written
	// to and how far each has got. Absent for a run with one destination,
	// where the counters above describe it in full.
	//
	// REPLACED WHOLESALE on every update, never mutated in place: setProgress
	// publishes a SHALLOW copy, so every published snapshot shares this slice
	// header, and an element written after publication races every reader. The
	// same reason SkippedSample is handed over once and never appended to.
	Targets                   []TargetProgress `json:"targets,omitempty"`
	Mode                      string           `json:"mode,omitempty"`
	FilesRegistered           int64            `json:"files_registered,omitempty"`
	RegistrationFailed        int64            `json:"registration_failed,omitempty"`
	RegistrationFailedSample  []string         `json:"registration_failed_sample,omitempty"`
	SidecarMismatches         int64            `json:"sidecar_mismatches,omitempty"`
	SidecarMismatchSample     []string         `json:"sidecar_mismatch_sample,omitempty"`
	ReplacedFiles             int64            `json:"replaced_files,omitempty"`
	CompactionPause           string           `json:"compaction_pause,omitempty"`
	BackupUnregisteredSkipped int64            `json:"backup_unregistered_skipped,omitempty"`
	BackupManifestOnlyFiles   int64            `json:"backup_manifest_only_files,omitempty"`
	TotalBytes                int64            `json:"total_bytes"`
	ProcessedBytes            int64            `json:"processed_bytes"`
	StartedAt                 time.Time        `json:"started_at"`
	CompletedAt               *time.Time       `json:"completed_at,omitempty"`
	Error                     string           `json:"error,omitempty"`
}

// TargetProgress is one leg's share of a running backup (#1085 stage B2b-2).
type TargetProgress struct {
	Name      string `json:"name"`
	IsDefault bool   `json:"is_default,omitempty"`
	Files     int64  `json:"files"`
	Bytes     int64  `json:"bytes"`
	Skipped   int64  `json:"skipped"`
	// Status is "pending" before the leg's copy begins, "copying" while it
	// runs, "copied" once its files are in, "committed" once its sidecar and
	// manifest are, and "failed" when the run failed on this leg.
	Status string `json:"status"`
}

// Leg progress statuses.
const (
	legPending   = "pending"
	legCopying   = "copying"
	legCopied    = "copied"
	legCommitted = "committed"
	legFailed    = "failed"
)

// mergeRunManifests assembles the run-level view of a backup from its
// per-target manifests (#1085 stage B2b-2, decision 8 of the plan).
//
// Every restore gate, the API echo and the listing summary read THIS and never
// one leg's manifest. The alternative — "read the manifest from whichever
// target answers" — breaks eight gates that read one manifest: a scoped
// backup routed to target X and read from the default's manifest has
// Scope=[audit] and Databases=[], which CheckScopedReplace refuses as "holds
// no data files"; HasMetadata, HasConfig and HasIcebergCatalog live only on
// the default leg, so a non-default read silently skips the SQLite database,
// the Iceberg catalog and arc.toml.
//
// With exactly one manifest the result is a COPY that is deep-equal to it, so
// the single-destination path is byte-identical to what it replaced.
//
// legs must be non-empty, default leg first where there is one.
func mergeRunManifests(legs []*Manifest) *Manifest {
	if len(legs) == 1 {
		clone := *legs[0]
		// The slice fields are aliased, as they always have been, and no
		// caller appends to them. The breakdown is the manifest's first MAP
		// field, where an in-place m[k] = v is the natural way to write and
		// would reach through into the leg, so it is copied rather than
		// documented as untouchable.
		if clone.ColdFilesExcludedDatabases != nil {
			cp := make(map[string]int64, len(clone.ColdFilesExcludedDatabases))
			for k, v := range clone.ColdFilesExcludedDatabases {
				cp[k] = v
			}
			clone.ColdFilesExcludedDatabases = cp
		}
		return &clone
	}
	// Run-level fields come from the default leg when it is present, because
	// its manifest is the one that carries the default-leg-only fields.
	anchor := legs[0]
	for _, leg := range legs {
		if leg.IsDefaultTarget {
			anchor = leg
			break
		}
	}
	merged := &Manifest{
		Version:                anchor.Version,
		BackupID:               anchor.BackupID,
		CreatedAt:              anchor.CreatedAt,
		BackupType:             anchor.BackupType,
		Scope:                  anchor.Scope,
		OwnerInstanceID:        anchor.OwnerInstanceID,
		ClusterManifestChecked: anchor.ClusterManifestChecked,
		RunTargets:             anchor.RunTargets,
		Target:                 anchor.Target,
	}
	dbs := map[string]*DatabaseInfo{}
	for _, leg := range legs {
		merged.TotalFiles += leg.TotalFiles
		merged.TotalSizeBytes += leg.TotalSizeBytes
		merged.SkippedFiles += leg.SkippedFiles
		merged.SkippedMetadataFiles += leg.SkippedMetadataFiles
		merged.SkippedOverlongKeys += leg.SkippedOverlongKeys
		merged.AuxiliaryFiles += leg.AuxiliaryFiles
		merged.CompactionStateFiles += leg.CompactionStateFiles
		merged.UnaddressableFiles += leg.UnaddressableFiles
		merged.UnregisteredSkipped += leg.UnregisteredSkipped
		merged.ManifestOnlyFiles += leg.ManifestOnlyFiles
		merged.LeftManifestDuringRun += leg.LeftManifestDuringRun
		merged.SkippedReconciled += leg.SkippedReconciled
		merged.IcebergNamespaceFilesExcluded += leg.IcebergNamespaceFilesExcluded
		merged.ColdFiles += leg.ColdFiles
		merged.ColdSizeBytes += leg.ColdSizeBytes
		merged.ColdObjectsUnrecorded += leg.ColdObjectsUnrecorded
		merged.ColdDedupSkipped += leg.ColdDedupSkipped
		merged.ColdRowsStaleButHot += leg.ColdRowsStaleButHot
		merged.ColdFilesExcluded += leg.ColdFilesExcluded
		if len(leg.ColdFilesExcludedDatabases) > 0 && merged.ColdFilesExcludedDatabases == nil {
			merged.ColdFilesExcludedDatabases = map[string]int64{}
		}
		for db, n := range leg.ColdFilesExcludedDatabases {
			// Summed rather than assigned for the same reason
			// mergeDatabaseInfo folds instead of replacing: a database's
			// anchors and compaction state ride with its data, and a run whose
			// routing changed between backups could hold one name on two legs.
			merged.ColdFilesExcludedDatabases[db] += n
		}
		merged.SkippedSample = appendSample(merged.SkippedSample, leg.SkippedSample)
		merged.UnaddressableSample = appendSample(merged.UnaddressableSample, leg.UnaddressableSample)
		merged.UnregisteredSample = appendSample(merged.UnregisteredSample, leg.UnregisteredSample)
		merged.ManifestOnlySample = appendSample(merged.ManifestOnlySample, leg.ManifestOnlySample)
		merged.LeftManifestSample = appendSample(merged.LeftManifestSample, leg.LeftManifestSample)
		merged.IcebergNamespacesExcluded = appendSample(merged.IcebergNamespacesExcluded, leg.IcebergNamespacesExcluded)
		// OR'd: each is written by the default leg alone, so an OR over the
		// legs is the default leg's value and survives a reordering.
		merged.HasMetadata = merged.HasMetadata || leg.HasMetadata
		merged.HasIcebergCatalog = merged.HasIcebergCatalog || leg.HasIcebergCatalog
		merged.HasConfig = merged.HasConfig || leg.HasConfig
		if merged.IcebergWarehouse == nil {
			merged.IcebergWarehouse = leg.IcebergWarehouse
		}
		for _, db := range leg.Databases {
			mergeDatabaseInfo(dbs, db)
		}
	}
	for _, name := range sortedDatabaseNames(dbs) {
		merged.Databases = append(merged.Databases, *dbs[name])
	}
	return merged
}

// mergeDatabaseInfo folds one leg's entry for a database into the union.
//
// Reachable even though routing sends a database to exactly one target: the
// two reserved roots route by the database inside them, so a database's
// anchors and compaction state ride with its data, and a run whose routing
// changed between backups could still hold one name on two legs.
func mergeDatabaseInfo(dbs map[string]*DatabaseInfo, db DatabaseInfo) {
	into, ok := dbs[db.Name]
	if !ok {
		clone := db
		clone.Measurements = append([]MeasurementInfo(nil), db.Measurements...)
		dbs[db.Name] = &clone
		return
	}
	into.FileCount += db.FileCount
	into.SizeBytes += db.SizeBytes
	for _, meas := range db.Measurements {
		found := false
		for i := range into.Measurements {
			if into.Measurements[i].Name != meas.Name {
				continue
			}
			into.Measurements[i].FileCount += meas.FileCount
			into.Measurements[i].SizeBytes += meas.SizeBytes
			found = true
			break
		}
		if !found {
			into.Measurements = append(into.Measurements, meas)
		}
	}
}

// sortedDatabaseNames is the deterministic order the merged inventory is
// written in.
func sortedDatabaseNames(dbs map[string]*DatabaseInfo) []string {
	out := make([]string, 0, len(dbs))
	for name := range dbs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// MarshalManifest serializes a manifest to JSON.
func MarshalManifest(m *Manifest) ([]byte, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	return data, nil
}

// UnmarshalManifest deserializes a manifest from JSON.
func UnmarshalManifest(data []byte) (*Manifest, error) {
	m := &Manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest: %w", err)
	}
	return m, nil
}

// SummaryFromManifest creates a compact summary from a full manifest.
func SummaryFromManifest(m *Manifest) BackupSummary {
	return BackupSummary{
		BackupID:              m.BackupID,
		CreatedAt:             m.CreatedAt,
		BackupType:            m.BackupType,
		Scope:                 m.Scope,
		TotalFiles:            m.TotalFiles,
		TotalBytes:            m.TotalSizeBytes,
		DatabaseCount:         len(m.Databases),
		SkippedFiles:          m.SkippedFiles,
		SkippedMetadataFiles:  m.SkippedMetadataFiles,
		UnaddressableFiles:    m.UnaddressableFiles,
		UnregisteredSkipped:   m.UnregisteredSkipped,
		ManifestOnlyFiles:     m.ManifestOnlyFiles,
		LeftManifestDuringRun: m.LeftManifestDuringRun,
		Target:                m.Target,
		OwnerInstanceID:       m.OwnerInstanceID,
		ColdFilesExcluded:     m.ColdFilesExcluded,
		ColdFiles:             m.ColdFiles,
	}
}
