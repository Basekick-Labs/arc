package backup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// errBackupRead marks a failure reading the SOURCE file from data storage.
//
// Source-read failures are skippable: the file may legitimately have been
// deleted by compaction or retention between listing and copy. The other
// explicitly skippable case is an overlong destination key, checked before
// streaming in copyDataFiles.
//
// Every other failure — temp file creation, seek, backup-storage write — is
// fatal. Those indicate a broken environment (no temp space, unwritable or
// unreachable backup storage), not a race, and continuing would produce a
// backup that silently omits files while reporting success.
//
// Classification is deliberately positive rather than by exclusion: a failure
// mode added here later is fatal by default until someone marks it skippable.
var errBackupRead = errors.New("backup source read failed")

// maxSkipRatio is the fraction of inventoried files that may be skipped before the
// backup is treated as failed rather than merely incomplete.
//
// Skipping exists to tolerate two narrow cases: a file removed by compaction
// or retention between the listing and the copy, and a legal source key whose
// backup destination would overrun the storage key limit (#761). The first
// touches a handful of files at the tail of a run; the second is permanent
// but, in any deployment Arc itself wrote, affects no file at all. A low
// ceiling absorbs both. Anything above it is a different event — throttling,
// credential expiry, a storage outage, or a storage root full of foreign keys
// too long to back up — where returning a fraction of the data as a
// "successful" backup hides the gap until a restore needs it.
//
// Deliberately a constant, not a config key: no operator has needed to tune it,
// and a knob nobody sets is a knob nobody tests.
const maxSkipRatio = 0.10

// icebergCatalogDBName is the filename the Iceberg SQL catalog is stored under
// inside a backup's metadata/ directory, when it is a separate database from the
// shared one (which is stored as arc.db).
const icebergCatalogDBName = "iceberg-catalog.db"

func isSourceReadError(err error) bool {
	return errors.Is(err, errBackupRead)
}

// BackupOptions controls what gets backed up and where.
type BackupOptions struct {
	IncludeMetadata bool // back up the SQLite database
	IncludeConfig   bool // back up arc.toml
	// Databases limits the backup to these storage-root segments (#1084):
	// their data files, their field schema anchors and their compaction
	// recovery state, and nothing else. Empty means the whole instance,
	// exactly as before. The names are validated, sorted and de-duplicated
	// here as well as by the API layer (see newScope); the SQLite metadata
	// is never copied for a scoped backup, whatever IncludeMetadata says at
	// the API, because the API refuses that combination.
	Databases []string
}

// BackupResult is returned when a backup completes.
type BackupResult struct {
	Manifest *Manifest
	Duration time.Duration
}

// backupRun is one CreateBackup's per-target state (#1085 stage B2b-2).
//
// A run has one LEG per target it routes to. Files are partitioned into legs
// UP FRONT, before any copy, and each copy function takes its leg's
// destination as a parameter. Routing inside streamBackupFile would make the
// destination a function of the path at every write and force a per-file
// lookup of the right manifest, sidecar and tally.
type backupRun struct {
	m        *Manager
	id       string
	progress *Progress
	// legs are every target this run commits to, in COMMIT order: the
	// non-default legs first and the default leg last, because the default
	// target's manifest is the one an operator looks for and it should be the
	// last thing to land. The copy phase walks the same order.
	legs   []*backupLeg
	byName map[string]*backupLeg
	def    *backupLeg
	index  *RunIndex
}

// backupLeg is one target's slice of a run.
//
// A leg owns its destination, its own Manifest, its own dbMap, its own
// sidecarBuilder, its own skipTally, its own key headroom and its own state
// and data skip counters. The RUN owns Progress, the global skip ratio, the
// cluster cross-check and the index.
//
// The skip counters are per leg because the state-vs-data split used to be a
// global subtraction (all state copies, snapshot the counter, all data copies),
// which assumed every state copy precedes every data copy. Per-leg
// interleaving — A-state, A-data, B-state, B-data — breaks that, so each leg
// counts its own and progress.SkippedFiles stays a run-wide total used only
// for the gauge and the ratio.
type backupLeg struct {
	// run is the run this leg belongs to, so every copy function takes ONE
	// argument and a caller cannot hand it a leg of another run — which is
	// exactly the mistake that would publish per-target progress for a leg
	// nothing is writing to.
	run      *backupRun
	target   backupTarget
	manifest *Manifest
	dbMap    map[string]*DatabaseInfo
	sidecar  *sidecarBuilder
	tally    *skipTally
	state    []storage.ObjectInfo
	data     []storage.ObjectInfo
	// files and bytes are what landed on this leg, for the per-target
	// progress; skipped is every skip on it, and the two below split that into
	// the populations the manifest reports apart.
	files        int64
	bytes        int64
	skipped      int64
	stateSkipped int64
	dataSkipped  int64
	// metaSkipped are the in-root Iceberg metadata skips, which are the
	// default leg's and belong to SkippedMetadataFiles rather than to
	// SkippedFiles.
	metaSkipped int64
	status      string
}

// planRun decides the run's target set and builds its legs, BEFORE the
// listing: the set follows from the scope and the configured routing alone, so
// every target can be probed and the index written before a single file is
// read.
//
// Every leg in the set commits a manifest AND a sidecar, even with zero files
// (decision 4 of the B2b-2 plan), and that is what makes "RunTargets
// membership means exactly has-a-manifest" true. Three things fall out of it:
// a routed database whose name is a typo produces a leg with total_files: 0,
// which is the operator-facing signal and is not confusable with a target the
// run failed to commit to; a cluster restore refuses outright when a backup
// has no sidecar, so an empty leg that skipped its sidecar would make an
// otherwise-good run unrestorable on a cluster; and aborted-run detection
// stays a clean predicate.
//
// The set is defined BY THE SCOPE, because "always commits" is only safe once
// it is:
//
//   - an UNSCOPED run touches the default target plus every target that is
//     ROUTED at least one database. Not every CONFIGURED target: a target with
//     no databases= of its own is inert, and making it a leg would make it a
//     hard dependency of every backup in the instance — an operator who adds
//     the [backup.targets.x] block in one commit and its routing in the next,
//     or whose credentials for it are wrong, would break whole-instance
//     backups that have no business touching it. Each leg is probed before
//     anything is copied, and one unreachable leg fails the run.
//   - a SCOPED run touches the default target plus each target that owns at
//     least one scoped database. Not every routed target: a run scoped to
//     ["prod"] must not write an empty Scope=[prod], Databases=[] manifest
//     onto audit_bucket, where it would list as a prod backup holding nothing.
//
// The typo signal survives the stricter rule in both cases. A misspelled name
// in databases= still means that target IS routed something, so it is still a
// leg and still commits a manifest with total_files 0 — which is the signal an
// operator reads. What drops out is only the target nothing is routed to at
// all, whose empty manifest would say nothing a reader could act on.
func (m *Manager) planRun(backupID string, startTime time.Time, progress *Progress, sc *scope) *backupRun {
	run := &backupRun{m: m, id: backupID, progress: progress, byName: map[string]*backupLeg{}}

	// A nil *scope is an unscoped run, like one with no names (scope.empty),
	// so the names are read through the guard rather than off the pointer.
	var scopeNames []string
	if !sc.empty() {
		scopeNames = sc.names
	}

	names := []string{}
	for _, name := range m.sortedTargetNames() {
		if m.targetIsRouted(name) && (sc.empty() || m.targetOwnsAnyOf(name, sc)) {
			names = append(names, name)
		}
	}
	// RunTargets, and therefore IsDefaultTarget, are written only when the run
	// spans SEVERAL targets. With one destination there is nothing to name and
	// nothing to disambiguate, and leaving them out keeps a single-destination
	// manifest byte-identical to the one stage B2b-1 wrote.
	var runTargets []string
	if len(names) > 0 {
		runTargets = append([]string{m.targetName}, names...)
		sort.Strings(runTargets)
	}

	newLeg := func(t backupTarget, isDefault bool) *backupLeg {
		leg := &backupLeg{
			run:    run,
			target: t,
			dbMap:  make(map[string]*DatabaseInfo),
			tally:  &skipTally{},
			status: legPending,
			manifest: &Manifest{
				Version:         "dev",
				BackupID:        backupID,
				CreatedAt:       startTime.UTC(),
				BackupType:      "full",
				Scope:           scopeNames,
				Target:          t.name,
				OwnerInstanceID: m.instanceID,
				RunTargets:      runTargets,
			},
		}
		if isDefault && len(runTargets) > 0 {
			leg.manifest.IsDefaultTarget = true
		}
		run.byName[t.name] = leg
		return leg
	}
	for _, name := range names {
		run.legs = append(run.legs, newLeg(m.targets[name], false))
	}
	run.def = newLeg(m.defaultDestination(), true)
	run.legs = append(run.legs, run.def)

	// The index names every target this run touches, which is NOT runTargets:
	// a run with one destination writes no RunTargets (there is nothing to
	// name in a manifest found alone) and still has a target to record here
	// when one is configured. Empty only for the backup.local_path
	// destination, where readers take it as "the single configured
	// destination".
	indexTargets := []string{}
	for _, leg := range run.legs {
		if leg.target.name != "" {
			indexTargets = append(indexTargets, leg.target.name)
		}
	}
	sort.Strings(indexTargets)
	run.index = &RunIndex{
		BackupID:        backupID,
		CreatedAt:       startTime.UTC(),
		OwnerInstanceID: m.instanceID,
		Scope:           scopeNames,
		DefaultTarget:   m.targetName,
		Targets:         indexTargets,
		// The routing THIS RUN acted on, not the whole configured map. The
		// index is read by an operator hunting a failed run's's leftovers, and a
		// scoped run index listing a database whose store the run never
		// touched points them at the wrong bucket.
		DatabaseTargets: m.routedDatabasesTo(indexTargets),
		Status:          runIndexStarted,
	}
	return run
}

// targetIsRouted reports whether any database at all is routed to a target.
//
// A non-default target with no databases of its own is INERT: nothing can
// route to it, so a leg for it could only ever commit an empty manifest, and
// probing it would make an unrelated store a precondition of every backup.
// NewManager logs a warning for one at startup rather than refusing to load,
// because adding the block and the routing in separate commits is ordinary.
func (m *Manager) targetIsRouted(name string) bool {
	for _, target := range m.routing {
		if target == name {
			return true
		}
	}
	return false
}

// targetOwnsAnyOf reports whether a target is routed at least one of the
// scope's databases.
func (m *Manager) targetOwnsAnyOf(name string, sc *scope) bool {
	for _, db := range sc.names {
		if m.routing[db] == name {
			return true
		}
	}
	return false
}

// legFor is the leg a source path's copy belongs to.
//
// A path whose routing names a target this run does not touch falls back to
// the default leg, and that fallback is UNREACHABLE rather than a case with
// behaviour of its own: an unscoped run plans every ROUTED target (a path can
// only route to a target some database is routed to), and a scoped one plans
// every target the scope routes to, while every file set
// reaching here is scope-filtered first (listScoped for the inventory,
// scope.ownsPath for the unaddressable set, scope.ownsData for the
// manifest-only pass). It stays as defense in depth, because the alternative
// to a fallback is a nil leg.
func (r *backupRun) legFor(path string) *backupLeg {
	if len(r.m.routing) == 0 {
		return r.def
	}
	if name, ok := r.m.routing[routeKey(path)]; ok {
		if leg, ok := r.byName[name]; ok {
			return leg
		}
	}
	return r.def
}

// assign partitions a file group into the legs.
//
// The in-root Iceberg metadata group is NOT routed through here; it goes to
// the default leg whole, by CLASSIFICATION rather than by routeKey. See the
// group's own comment in CreateBackup.
func (r *backupRun) assign(files []storage.ObjectInfo, state bool) {
	for _, obj := range files {
		leg := r.legFor(obj.Path)
		if state {
			leg.state = append(leg.state, obj)
			continue
		}
		leg.data = append(leg.data, obj)
	}
}

// publishTargets republishes the per-target progress.
//
// A FRESH slice every time, never an element written in place: setProgress
// publishes a shallow copy, so every snapshot shares the slice header and a
// mutation after publication races every reader of /status. Absent for a run
// with one destination, where the run-wide counters describe it in full.
func (r *backupRun) publishTargets() {
	if len(r.legs) < 2 {
		r.m.setProgress(r.progress)
		return
	}
	out := make([]TargetProgress, 0, len(r.legs))
	for _, leg := range r.legs {
		out = append(out, TargetProgress{
			Name:      leg.target.name,
			IsDefault: leg == r.def,
			Files:     atomic.LoadInt64(&leg.files),
			Bytes:     atomic.LoadInt64(&leg.bytes),
			Skipped:   atomic.LoadInt64(&leg.skipped),
			Status:    leg.status,
		})
	}
	r.progress.Targets = out
	r.m.setProgress(r.progress)
}

// fail marks every leg that has not COMMITTED as failed and republishes, so a
// failed run status says which destination it died on and claims nothing about
// the others.
//
// leg is the one the failure happened on, and may be nil for a run-wide
// failure (the skip ratio, the cluster re-check) that belongs to no single
// destination. Either way every leg still pending, copying or copied is marked
// failed: the run is over and those legs will never commit, so leaving them at
// "copied" would show an operator a half-green status for a run that produced
// nothing restorable there. A leg that already committed keeps that status,
// because its manifest really did land and the listing will really find it.
func (r *backupRun) fail(leg *backupLeg) {
	if leg != nil {
		leg.status = legFailed
	}
	for _, other := range r.legs {
		if other.status != legCommitted {
			other.status = legFailed
		}
	}
	r.publishTargets()
}

// inventoried sums every leg's data-file inventory.
func (r *backupRun) inventoried() int64 {
	var total int64
	for _, leg := range r.legs {
		total += leg.manifest.TotalFiles
	}
	return total
}

// overlongSkips sums every leg's overlong-key skips.
func (r *backupRun) overlongSkips() int64 {
	var total int64
	for _, leg := range r.legs {
		total += leg.tally.overlong
	}
	return total
}

// minMaxSourceKeyBytes is the shortest source key any of this run's targets
// can hold.
//
// The MINIMUM, because the ratio failure says "longer than N bytes, which no
// backup destination key can hold", and that sentence is only true of every
// target at the smallest of their limits. The per-file skip warning names its
// own leg's figure, which is the one that actually rejected that file.
func (r *backupRun) minMaxSourceKeyBytes() int {
	min := r.def.target.maxSourceKeyBytes()
	for _, leg := range r.legs {
		if v := leg.target.maxSourceKeyBytes(); v < min {
			min = v
		}
	}
	return min
}

// remoteTargets names every CONFIGURED target that is an object store, for the
// include_config warning. A method on the run only because that is where the
// warning is raised; it deliberately does not consult r.legs.
//
// The configured set and not the run legs, because what leaks is the FILE:
// arc.toml carries every target's credentials, so copying it into a backup
// exposes the keys to every configured store no matter which legs this
// particular run wrote. A scoped backup of an unrelated database has legs ==
// [default], so a per-leg warning said nothing at all on exactly the request
// that can override include_config without a refusal — the scoped one.
//
// Named for the default target too when it is remote, which it is in the
// ordinary all-remote deployment.
func (r *backupRun) remoteTargets() []string {
	m := r.m
	var out []string
	if m.targetRemote && m.targetName != "" {
		out = append(out, m.targetName)
	}
	for _, name := range m.sortedTargetNames() {
		if m.targets[name].remote {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// CreateBackup performs a backup: the whole instance, or, when opts.Databases
// names databases, only those (#1084). It runs synchronously; the API layer
// launches it in a goroutine and exposes progress via GetProgress().
//
// With several configured targets (#1085 stage B2b-2) the run has one LEG per
// target it routes to, and the phases are:
//
//  1. mint the backup ID and plan the legs from the scope and the routing;
//  2. probe EVERY touched target, each naming itself;
//  3. discover the source files, classify them, and take the first cluster
//     manifest snapshot (phase 1 and 1a below);
//  4. write the run index on the default target, status "started" — the last
//     step before anything is copied;
//  5. copy phase, per leg: state files before data files;
//  6. the routed cluster re-check, which deletes and copies through the leg
//     that holds each file (phase 2a);
//  7. default leg only: the SQLite metadata, the Iceberg catalog, the in-root
//     Iceberg metadata group and an outside-root warehouse (phases 3 to 3c);
//  8. the run-wide skip ratio, summed over every leg, then the per-leg
//     unaddressable records;
//  9. the config copy, on the default leg (phase 4);
//  10. commit phase, per leg: sidecar then manifest, the default leg last;
//  11. finalise the index.
//
// The re-check before the default-leg extras, and the config copy after the
// ratio, are the base order and not a choice this stage made: the re-check
// belongs with the data copy it corrects, and the ratio is a refusal that must
// fire before arc.toml is written anywhere.
//
// Manifests last for everything, so a failure anywhere in the copy phase
// leaves NO manifest on any target — the listing then shows nothing, exactly
// the behaviour before targets — and the index names the leftovers.
func (m *Manager) CreateBackup(ctx context.Context, opts BackupOptions) (*BackupResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	backupID := generateBackupID()
	startTime := time.Now()

	progress := &Progress{
		Operation: "backup",
		BackupID:  backupID,
		Status:    "running",
		StartedAt: startTime,
	}
	m.setProgress(progress)
	defer func() {
		now := time.Now()
		progress.CompletedAt = &now
		m.setProgress(progress)
	}()

	// The scope (#1084): empty for a whole-instance backup, which changes
	// nothing below; otherwise the storage-root segments the run is limited
	// to, validated once more here for callers that bypass the API.
	sc, err := newScope(opts.Databases)
	if err != nil {
		err = fmt.Errorf("invalid backup scope: %w", err)
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	run := m.planRun(backupID, startTime, progress, sc)

	// Does every destination this run touches answer at all? Asked before any
	// copying, because a run's first destination touch is a bulk file write
	// under the two-hour operation timeout while the single-operation lock is
	// held, and nothing below it sets a response timeout. Every leg, not only
	// the default: a run spans its legs, so an unreachable one has to fail the
	// run before the others have written anything an operator must clean up.
	// See Manager.probeDestination.
	for _, leg := range run.legs {
		if err := m.probeDestination(ctx, leg.target, backupID); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(leg)
			return nil, err
		}
	}

	if sc.empty() {
		m.logger.Info().Str("backup_id", backupID).Strs("targets", run.index.Targets).Msg("Starting backup")
	} else {
		// The API refuses this combination; this is the belt for direct
		// callers, so the promise that a scoped backup never carries the
		// SQLite metadata holds whoever calls.
		if opts.IncludeMetadata {
			err := errors.New("invalid backup scope: include_metadata is not available on a scoped backup: the SQLite database holds the tier rows of every database, the tokens, the continuous queries and the audit log, so it cannot ride along with one database; take an unscoped backup for it")
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		progress.Scope = sc.names
		m.setProgress(progress)
		m.logger.Info().Str("backup_id", backupID).Strs("databases", sc.names).
			Strs("targets", run.index.Targets).Msg("Starting backup scoped to databases")
	}
	run.publishTargets()

	// ── 1. Discover data files ──────────────────────────────────────────
	objectLister, ok := m.dataStorage.(storage.ObjectLister)
	if !ok {
		progress.Status = "failed"
		progress.Error = "storage backend does not support listing objects"
		return nil, fmt.Errorf("storage backend does not support ListObjects")
	}

	// Enumerate the hidden set FIRST, and the inventory second. The two are
	// separate passes, so a file renamed between them is seen by one or the
	// other depending on the order: this way a key fixed mid-backup is reported
	// as unaddressable AND copied, which is a spurious warning. The reverse
	// order loses it from both, which is silently the very bug this guards
	// against (#756).
	unaddressable := m.findUnaddressable(ctx, progress, sc)

	// Unscoped: one listing of the whole root, as always. Scoped: the
	// databases' own prefixes plus the anchors and compaction state the scope
	// owns (listScoped), which then flow through the same split and copy code
	// below, so the #930 ordering, the #977 tallies and the #1083 sidecar are
	// untouched. The listing and the hidden set above double as three of the
	// four known-database rules, so the belt (checkScopeKnown) costs at most
	// one tier-metadata query per name that has neither hot files, nor an
	// anchor, nor a hidden key.
	var objects []storage.ObjectInfo
	if sc.empty() {
		objects, err = objectLister.ListObjects(ctx, "")
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, fmt.Errorf("failed to list data files: %w", err)
		}
	} else {
		listing, err := m.listScoped(ctx, objectLister, sc)
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		if err := m.checkScopeKnown(ctx, sc, listing, unaddressable); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		objects = listing.objects
	}

	// Filter to .parquet data files (the manifest inventory), and separately collect Iceberg
	// warehouse metadata (metadata.json / .avro / version-hint.text under an Iceberg table's
	// metadata/ dir). The Iceberg metadata is NOT parquet, so the old .parquet-only filter
	// silently dropped it — a restore then lost the Iceberg tables (the referenced parquet
	// survived, but the table metadata pointing at it did not). It is copied via the same
	// mechanism but kept out of the db/measurement inventory below.
	var parquetFiles []storage.ObjectInfo
	var icebergMetaFiles []storage.ObjectInfo
	// Compaction's crash-recovery state (#930): the manifests under
	// _compaction_state/ that let recovery finish a job interrupted between
	// its output upload and its input deletion, plus parked (.quarantined)
	// manifests kept as an operator record. Checked first so neither the
	// ".parquet" rule nor the Iceberg "/metadata/" rule sees them.
	var stateFiles []storage.ObjectInfo
	for _, obj := range objects {
		switch {
		case isCompactionState(obj.Path):
			stateFiles = append(stateFiles, obj)
		case strings.HasSuffix(obj.Path, ".parquet"):
			parquetFiles = append(parquetFiles, obj)
		case isIcebergMetadata(obj.Path):
			icebergMetaFiles = append(icebergMetaFiles, obj)
		}
	}
	// A scoped backup carries no Iceberg metadata (#1084): the namespace
	// directories are not listed at all, and the catalog that would make a
	// stray metadata file useful is never copied with it. The classifier
	// above still runs so a non-Parquet file under a /metadata/ segment
	// inside a scoped database lands in this bucket rather than in the data
	// inventory; the bucket is then dropped.
	if !sc.empty() && len(icebergMetaFiles) > 0 {
		m.logger.Debug().Int("files", len(icebergMetaFiles)).
			Msg("Non-Parquet files under a /metadata/ segment inside a scoped database are not copied: a scoped backup carries no Iceberg metadata")
		icebergMetaFiles = nil
	}

	// MEDIUM: decide the fatal case before doing any work. Copying every
	// Iceberg metadata file and only then failing would leave a half-written
	// backupID/data/... tree in backup storage with no manifest, which
	// ListBackups keys on and therefore can neither show nor clean up.
	if len(unaddressable) > 0 && len(parquetFiles) == 0 {
		err := fmt.Errorf("backup failed: all %d data file(s) in source storage have a key that cannot be addressed, so the backup would contain no data; rename them to conform to the storage key rules (see the log for paths)", len(unaddressable))
		m.logger.Error().Int("unaddressable", len(unaddressable)).
			Strs("sample", sampleUnaddressable(unaddressable)).Msg(err.Error())
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// ── 1a. Cluster manifest cross-check (#1083) ────────────────────────
	// Snapshot the manifest AFTER the listing, never before: registration is
	// asynchronous (internal/cluster/file_registrar.go), so a snapshot taken
	// first would call a file flushed between the two "unregistered" and
	// leave it out. A data file the listing has and the manifest lacks is
	// not copied: on a cluster the manifest says what is data, and a file it
	// does not list is a compaction or retention input awaiting unlink, a
	// pre-cluster file, or a dropped registration, which the reconciliation
	// sweep treats the same way. Restoring such a file next to the output
	// that replaced it would serve every row of that partition twice, on
	// every node, forever. The decision is provisional: the end of the data
	// copy re-reads the manifest once (recheckClusterManifest) so a file
	// whose registration had merely not reached this node yet is copied
	// after all, and a copied file the cluster has since stopped listing is
	// taken out again. Reserved-root Parquet (the _schema anchors) is never
	// in the manifest and is copied as before. Nil cluster: no manifest, no
	// check, behaviour unchanged.
	dataFiles := parquetFiles
	var xc *manifestCrossCheck
	if m.cluster != nil {
		dataFiles, xc, err = m.crossCheckManifest(ctx, parquetFiles, sc)
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
	}

	// Partition up front, never inside a write: each leg's files are decided
	// here and every copy function below takes its leg's destination.
	run.assign(stateFiles, true)
	run.assign(dataFiles, false)
	// Every leg sidecar ALIASES the one byPath map: it is the first cluster
	// manifest snapshot, the same immutable reference set for every leg, and
	// copying it per leg would duplicate an entry per registered file in the
	// instance for no gain. What is per leg is everything the leg itself
	// accumulates — files, dropped, the counters — so two legs never write to
	// each other's rows.
	//
	// The one writer into the shared map is recheckClusterManifest addLate,
	// which runs AFTER every leg copy, on one goroutine, and adds a row for a
	// file that registered late. That is safe today because nothing else is
	// running. If the leg copies are ever parallelised, this map has to become
	// per leg or the write has to be guarded.
	for _, leg := range run.legs {
		if xc != nil {
			leg.sidecar = &sidecarBuilder{byPath: xc.byPath}
		} else {
			leg.sidecar = &sidecarBuilder{}
		}
		leg.manifest.ClusterManifestChecked = xc != nil
		leg.manifest.CompactionStateFiles = int64(len(leg.state))
		for _, obj := range leg.data {
			addToInventory(leg.manifest, leg.dbMap, obj)
		}
	}

	// Scoped (#1084): count the Iceberg namespace metadata left out, now,
	// with the other pre-copy decisions. It lists the data store, and a
	// listing failure after the copy would leave <id>/data/ with no manifest,
	// which ListBackups can neither show nor clean up. On the DEFAULT leg,
	// with the rest of the Iceberg bookkeeping.
	if !sc.empty() && m.icebergEnabled {
		if err := m.countExcludedIcebergNamespaces(ctx, objectLister, sc, run.def.manifest); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
	}

	// The cold-tier gap (#1085 stage B3), with the other pre-copy decisions
	// and for the same reason: a tier-metadata read that failed after the data
	// copy would be a query failure over a <id>/data/ nobody can list. OUTSIDE
	// the Iceberg guard above on purpose — an unscoped backup of a tiered
	// deployment with Iceberg off is the common case, and it has the same gap.
	// Never fails the run; see countColdFilesExcluded.
	m.countColdFilesExcluded(ctx, run, sc)

	// Progress total includes Iceberg metadata files (copied in step 2b) and
	// compaction state (step 1b) so ProcessedFiles never exceeds TotalFiles.
	// The manifest inventory (TotalFiles) counts only data files.
	progress.TotalFiles = run.inventoried() + int64(len(icebergMetaFiles)) + int64(len(stateFiles))
	progress.TotalBytes = run.inventoriedBytes()
	run.publishTargets()

	// An unreadable outside-root Iceberg warehouse fails the backup now, not
	// after every data file was copied. A scoped backup does not copy the
	// warehouse at all, so there it is only noted.
	if err := m.preflightIcebergWarehouse(); err != nil {
		if sc.empty() {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		m.logger.Debug().Err(err).Msg("Iceberg warehouse is not readable; a scoped backup does not copy it, continuing")
	}

	// The run index, written after every pre-copy decision and BEFORE the
	// first copy (#1085 stage B2b-2). It is the intent record: one key, on the
	// default target, naming every target this run will touch, so a run that
	// dies anywhere in the copy or commit phases is ENUMERABLE — which the
	// sidecar-then-manifest ordering alone never made it. A failure here fails
	// the run and is clean: nothing has been copied, so there is nothing to
	// compensate.
	//
	// Here rather than straight after the probes, because the index exists to
	// enumerate LEFTOVERS and a run that fails in discovery has none: the
	// listing, the unaddressable scan, the cluster cross-check and the
	// Iceberg namespace count all run before the first write, and four tests
	// pin that such a failure leaves the destination untouched. An index for a
	// run that wrote nothing would be an "aborted run" in every listing with
	// nothing under it to clean up.
	if err := m.writeRunIndex(ctx, run.def.target, run.index); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		run.fail(run.def)
		return nil, err
	}

	// ── 2. Copy phase, per leg ──────────────────────────────────────────
	// Compaction recovery state BEFORE the data files, and that order is load
	// bearing (#930). A job writes its manifest, uploads its output, deletes
	// its inputs, then deletes the manifest. Every group here comes from the
	// one listing above, so what matters is what a job did between the listing
	// and each copy. Manifest first: if the job then finishes before its
	// inputs are copied, those copies fail and the backup holds manifest +
	// output, which recovery completes; if the job finished before even the
	// manifest copy, its inputs are gone too and the backup holds the output
	// alone. Data first would let inputs be copied while the job still ran and
	// the manifest copy fail afterwards: output + inputs with nothing to
	// reconcile them, and every row of the partition served twice after a
	// restore.
	//
	// The #930 order is about the SOURCE timeline, so it is preserved WITHIN a
	// leg and the legs may run in any order — a claim that holds only because
	// routing keeps a recovery manifest, its output and its inputs on one
	// target: GenerateManifestPath, partitionPath and OutputPath all join the
	// database as their first segment, so they share one routing key.
	for _, leg := range run.legs {
		leg.status = legCopying
		run.publishTargets()
		if len(leg.state) > 0 {
			skipped, err := m.copyStateFiles(ctx, leg)
			if err != nil {
				progress.Status = "failed"
				progress.Error = err.Error()
				run.fail(leg)
				return nil, err
			}
			leg.stateSkipped += skipped
			m.logger.Info().Int("files", len(leg.state)).Str("target", leg.target.name).
				Msg("Backed up compaction recovery state")
		}
		skipped, err := m.copyDataFiles(ctx, leg, leg.data, leg.sidecar)
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(leg)
			return nil, err
		}
		leg.dataSkipped += skipped
		leg.status = legCopied
		run.publishTargets()
	}

	// ── 2a. Re-read the cluster manifest once, both directions (#1083) ──
	// Registration is asynchronous and compaction commits in two Raft phases
	// on two watcher ticks, so the first snapshot can disagree with the end
	// of the run three ways: a listed file registered since (copied now), a
	// registered file this node has pulled since (copied now), and a copied
	// file the manifest has stopped listing since, the inputs of a compaction
	// whose phase 2 landed during the run above all (removed from the backup
	// again). What is still unregistered, and what the node still does not
	// hold, is reported. ROUTED: see recheckClusterManifest.
	if xc != nil {
		if err := m.recheckClusterManifest(ctx, run, xc, sc); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(nil)
			return nil, err
		}
	}
	for _, leg := range run.legs {
		for _, name := range sortedDatabaseNames(leg.dbMap) {
			leg.manifest.Databases = append(leg.manifest.Databases, *leg.dbMap[name])
		}
	}

	// ── 3. Copy SQLite metadata, on the DEFAULT leg ─────────────────────
	// Snapshotted BEFORE the Iceberg warehouse metadata below, on purpose: a
	// reconcile commit writes a new metadata file and only then points the
	// catalog row at it, so a catalog snapshotted after the file copy could
	// reference a file the backup never held (#637). Snapshotting the rows
	// first means every referenced file already existed, immutable, when its
	// row was written, and is copied by the passes that follow.
	//
	// The default leg, because the SQLite database is instance-wide: it holds
	// every database's tier rows, the tokens, the continuous queries and the
	// audit log, so it belongs to no routed database.
	if opts.IncludeMetadata && m.sqliteDBPath != "" {
		if err := m.backupSQLite(ctx, run.def.target, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup SQLite database")
			// Non-fatal: continue with backup
		} else {
			run.def.manifest.HasMetadata = true
		}

		// The Iceberg SQL catalog, when the operator put it in its own file.
		// It holds every Iceberg table's schema and snapshot pointers: without
		// it a restore brings back the Parquet and the warehouse metadata but
		// the tables no longer resolve. Empty when it lives in the shared
		// database, which the copy above already covers.
		if m.icebergCatalogDBPath != "" {
			if err := m.backupSQLiteFile(ctx, run.def.target, backupID, m.icebergCatalogDBPath, icebergCatalogDBName); err != nil {
				m.logger.Warn().Err(err).
					Str("path", m.icebergCatalogDBPath).
					Msg("Failed to backup Iceberg catalog database")
			} else {
				run.def.manifest.HasIcebergCatalog = true
			}
		}
	}

	// ── 3b. Copy Iceberg warehouse metadata under the storage root ──────
	// Same copy mechanism + path preservation as data files, so restore round-trips them to
	// their original locations and the SQLite catalog's metadata pointers still resolve. The
	// referenced parquet data is already copied above; only the Iceberg metadata is added here.
	//
	// ALWAYS THE DEFAULT LEG, and by CLASSIFICATION rather than by routeKey
	// (#1085 stage B2b-2). Two reasons, and the first is a #534-shaped trap:
	//
	//   - the group's first path segment is not arc_<db>.db unless the
	//     warehouse is at its DEFAULT. Point iceberg.warehouse at a
	//     subdirectory of the storage root and in-root metadata lives at
	//     <sub>/arc_<db>.db/<table>/metadata/…, so routeKey would answer
	//     <sub> — a segment no database is named after. Classifying the group
	//     removes the dependency on the key's shape entirely.
	//   - Iceberg table metadata is only useful with the SQL catalog, the
	//     catalog is instance-wide, and it rides with include_metadata to the
	//     default target.
	//
	// isIcebergMetadata is a deliberate catch-all — any non-.parquet path
	// containing /metadata/ — so <routed-db>/metadata/notes.txt also lands in
	// this group and therefore on the default target. No data is lost (a
	// restore puts it back at its original key, from the default target); it
	// is simply not on the routed database's target.
	if len(icebergMetaFiles) > 0 {
		skipped, err := m.copyDataFiles(ctx, run.def, icebergMetaFiles, nil)
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(run.def)
			return nil, err
		}
		run.def.metaSkipped += skipped
		m.logger.Info().Int("files", len(icebergMetaFiles)).Msg("Backed up Iceberg warehouse metadata")
	}

	// ── 3c. Copy an Iceberg warehouse that lives OUTSIDE the storage root ─
	// The listing above cannot see it, so it is walked on the filesystem and
	// stored under <backupID>/iceberg/; restore writes it back into the
	// node's configured warehouse (#637). Not for a scoped backup (#1084),
	// which instead counted what it leaves out before the copy began. On the
	// default leg, for the same reason as the catalog it belongs to.
	var warehouseFiles int
	if m.icebergWarehouse != "" && sc.empty() {
		whFiles, err := m.listIcebergWarehouseFiles()
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			return nil, err
		}
		info := &IcebergWarehouseInfo{Path: m.icebergWarehouse, ConfiguredPath: m.icebergWarehouseConfigured, FileCount: int64(len(whFiles))}
		for _, f := range whFiles {
			info.SizeBytes += f.size
		}
		progress.TotalFiles += int64(len(whFiles))
		run.publishTargets()
		skipped, err := m.copyIcebergWarehouse(ctx, run.def, whFiles)
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(run.def)
			return nil, err
		}
		info.SkippedFiles = skipped
		run.def.manifest.IcebergWarehouse = info
		warehouseFiles = len(whFiles)
		m.logger.Info().Int("files", len(whFiles)).Str("warehouse", m.icebergWarehouse).
			Msg("Backed up Iceberg warehouse metadata from outside the storage root")
	}

	// Every copy phase is done. Publish what was skipped where an operator can
	// reach it even when the run fails just below: the names on the status
	// endpoint, which stays until the next operation, and the total on the
	// gauge (#977). The sample is handed over once and never appended to
	// afterwards: published Progress values are snapshots that share this
	// slice header. The gauge is set here, after the copy phases and before
	// the ratio check, on purpose: a run the ratio fails still reports its
	// skips, and a run that failed earlier leaves the previous value rather
	// than clearing an alert with a zero that describes nothing.
	progress.SkippedSample = run.skippedSample()
	run.publishTargets()
	metrics.Get().SetBackupSkippedFiles(atomic.LoadInt64(&progress.SkippedFiles))

	// Evaluate the skip ratio once, over every file group of every leg. The
	// denominator is what the run set out to copy: unregistered files were
	// excluded on purpose and are neither skips nor inventory.
	if err := m.checkSkipRatio(run, int(run.inventoried())+len(icebergMetaFiles)+warehouseFiles+len(stateFiles)); err != nil {
		progress.Status = "failed"
		progress.Error = err.Error()
		return nil, err
	}

	// Record files that could never be copied because no listing returns them,
	// each in the manifest of the leg its database routes to. Per leg rather
	// than whole to every leg so each manifest counts only ITS OWN gaps: the
	// slice an operator reads for one target, and the figure the listing sums
	// across legs, are then both honest. Writing the run total onto every leg
	// would multiply it by the leg count; writing it only onto the default
	// would make a routed database's gaps vanish from the manifest that is
	// supposed to describe them. The fatal case was decided before any copying
	// began.
	m.recordUnaddressable(run, unaddressable)

	// ── 4. Copy config, on the DEFAULT leg ──────────────────────────────
	if opts.IncludeConfig && m.configPath != "" {
		// The default for a remote target is false, decided at the API layer
		// from Manager.TargetIsRemote. Reaching here with a remote target
		// means the request asked for it explicitly, which is allowed and
		// warned about once: arc.toml holds EVERY target's credentials, so the
		// copy puts the keys to each of those stores inside the backups one of
		// them holds. Warn, not Debug — a compaction-subprocess lesson: Debug
		// reaches no operator at default levels, and this is a defect-shaped
		// outcome the operator chose.
		// Every configured remote target, not just the ones this run writes
		// to: the leak is the file, and the file holds all of their
		// credentials. A scoped request is the one that can turn
		// include_config on without a refusal, and its legs are usually just
		// the default.
		if remote := run.remoteTargets(); len(remote) > 0 {
			m.logger.Warn().
				Strs("remote_targets", remote).
				Msg("Copying arc.toml into a backup while remote backup targets are configured: the config file carries the credentials of every one of them, so the backup store now holds the keys that unlock them")
		}
		if err := m.backupConfig(ctx, run.def.target, backupID); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to backup config file")
		} else {
			run.def.manifest.HasConfig = true
		}
	}

	// ── 5. Commit phase, per leg ────────────────────────────────────────
	// Record files that were inventoried but proved unreadable, so no manifest
	// claims contents its leg does not actually hold. Data-file skips are
	// recorded apart from the auxiliary ones (Iceberg metadata and compaction
	// state), per leg, because a leg's SkippedFiles must describe the same
	// population as its own TotalFiles for a restore to compare the two.
	//
	// The outside-root warehouse's own skips stay out of both: they are
	// counted in IcebergWarehouse.SkippedFiles, which the per-leg counters
	// achieve by construction — the global subtraction this replaced had to
	// subtract them back out.
	for _, leg := range run.legs {
		leg.manifest.SkippedFiles = leg.dataSkipped
		leg.manifest.SkippedMetadataFiles = leg.stateSkipped + leg.metaSkipped
		leg.manifest.SkippedSample = leg.tally.sample
		leg.manifest.SkippedOverlongKeys = leg.tally.overlong
	}

	// The file sidecar goes in before the manifest, on every leg: ListBackups
	// keys on manifest.json, so a run that dies between the two leaves nothing
	// a listing shows, rather than a listed backup a cluster restore refuses.
	// The non-default legs commit first and the default leg last, because the
	// default target's manifest is the one an operator looks for.
	//
	// A run that dies between two legs' commits is what the run index covers
	// (#1085 stage B2b-2): the ID then appears in the listing's backups AND in
	// its incomplete runs, naming the targets that committed and the ones that
	// did not. That is the hole stage B2b-1 recorded here and could not close
	// without building the index.
	for _, leg := range run.legs {
		if err := m.writeSidecar(ctx, leg.target, backupID, leg.sidecar); err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(leg)
			return nil, err
		}
		manifestData, err := MarshalManifest(leg.manifest)
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(leg)
			return nil, err
		}
		manifestPath := backupID + "/manifest.json"
		// No cleanupPartialBackupWrite here, deliberately, and of the two
		// uncompensated writes this is the one that matters most once a
		// destination can be remote: ListBackups keys on manifest.json, so a
		// committed-but-unreported manifest would make a failed run look like a
		// complete backup. Left out only to keep #1101 to the WriteReader sites it
		// was scoped to; tracked as #1110 with the config copy in backupConfig.
		manifestCtx, cancelManifest := withDestinationTimeout(ctx)
		err = leg.target.backend.Write(manifestCtx, manifestPath, manifestData)
		cancelManifest()
		if err != nil {
			progress.Status = "failed"
			progress.Error = err.Error()
			run.fail(leg)
			return nil, fmt.Errorf("failed to write the manifest to %s: %w", leg.target.describe(), err)
		}
		leg.status = legCommitted
		run.publishTargets()
	}

	// Every manifest has landed, so the backup is complete and restorable
	// whatever this does; see finaliseRunIndex for why its failure is a
	// warning and writeRunIndex's is fatal.
	m.finaliseRunIndex(ctx, run.def.target, run.index)

	progress.Status = "completed"
	duration := time.Since(startTime)

	// The merged run view: what the caller is handed and what the completion
	// log reports. With one leg it is a copy of that leg's manifest, so the
	// single-destination answer is unchanged.
	merged := run.mergedManifest()

	done := m.logger.Info().
		Str("backup_id", backupID).
		Int64("files", merged.TotalFiles).
		Int64("bytes", merged.TotalSizeBytes).
		Int64("skipped", merged.SkippedFiles).
		Dur("duration", duration)
	if len(run.legs) > 1 {
		done = done.Strs("targets", run.index.Targets)
	}
	if !sc.empty() {
		done = done.Strs("databases", sc.names)
	}
	done.Msg("Backup completed")

	return &BackupResult{Manifest: merged, Duration: duration}, nil
}

// inventoriedBytes sums every leg's inventoried size.
func (r *backupRun) inventoriedBytes() int64 {
	var total int64
	for _, leg := range r.legs {
		total += leg.manifest.TotalSizeBytes
	}
	return total
}

// skippedSample is the run-wide skip sample for the status endpoint: every
// leg's, bounded like any other sample.
func (r *backupRun) skippedSample() []string {
	if len(r.legs) == 1 {
		return r.legs[0].tally.sample
	}
	var out []string
	for _, leg := range r.legs {
		out = appendSample(out, leg.tally.sample)
	}
	return out
}

// mergedManifest is the run-level view of every leg.
func (r *backupRun) mergedManifest() *Manifest {
	legs := make([]*Manifest, 0, len(r.legs))
	// Default leg first, so the merge's anchor is found without scanning.
	legs = append(legs, r.def.manifest)
	for _, leg := range r.legs {
		if leg != r.def {
			legs = append(legs, leg.manifest)
		}
	}
	return mergeRunManifests(legs)
}

// copyDataFiles copies parquet files from data storage into ONE LEG's
// destination, and returns how many it skipped.
//
// Source-read failures and overlong backup destination keys are skipped and
// counted in progress.SkippedFiles, and recorded by cause and name in the
// leg's tally (#977). Every other failure aborts immediately.
//
// The skip count is RETURNED rather than attributed here, because the caller
// knows which population this group belongs to: a leg's data files feed its
// SkippedFiles and the in-root Iceberg metadata group feeds its
// SkippedMetadataFiles, and those are compared against different totals by a
// restore.
//
// The skip-ratio check is NOT applied here, because CreateBackup calls this
// more than once per run and the ratio is only meaningful over the whole
// backup: a handful of stale entries in a small metadata set is a large
// fraction of that set but a negligible fraction of the backup. The caller
// evaluates the ratio once via checkSkipRatio.
//
// sidecar, when non-nil, receives one row per database data file copied, with
// the SHA-256 of the bytes as they streamed through (#1083). The Iceberg
// metadata pass passes nil: those files are never registered.
func (m *Manager) copyDataFiles(ctx context.Context, leg *backupLeg, files []storage.ObjectInfo, sidecar *sidecarBuilder) (int64, error) {
	run := leg.run
	progress := run.progress
	var skipped int64

	for _, obj := range files {
		select {
		case <-ctx.Done():
			return skipped, ctx.Err()
		default:
		}

		destPath := run.id + "/data/" + obj.Path
		// A legal source key can exceed the storage limit once the backup
		// prefix is added. This predictable case is skippable; actual
		// backup-storage write failures must still abort the run.
		//
		// The threshold is PER DESTINATION, not a constant: a target with an
		// object-key prefix stores prefix+key, so max_source_key_bytes shrinks
		// by the prefix. Reporting the constant promised bytes the destination
		// could not hold, and the overrun it hid fails the write rather than
		// skipping the file. With several targets the figures differ, so the
		// message names the target whose limit was hit. See
		// backupTarget.maxSourceKeyBytes.
		if leg.target.keyTooLong(destPath) {
			skipped++
			atomic.AddInt64(&leg.skipped, 1)
			leg.tally.record(obj.Path, true)
			m.logger.Warn().
				Str("path", obj.Path).
				Str("target", leg.target.name).
				Int("destination_key_bytes", leg.target.destinationKeyBytes(destPath)).
				Int("maximum_key_bytes", storage.MaxUsableKeyLen).
				Int("max_source_key_bytes", leg.target.maxSourceKeyBytes()).
				Msg("Backup destination key too long; skipping (source keys longer than max_source_key_bytes cannot be backed up under this backup prefix)")
			continue
		}

		written, sha, err := m.streamBackupFileSHA(ctx, leg.target, obj.Path, destPath)
		if err != nil {
			// Only a source-read failure is skippable — the file may have been
			// deleted by compaction/retention between listing and copy. Anything
			// else (temp file, seek, backup write) means the environment is broken
			// and continuing would silently drop files from the backup.
			if !isSourceReadError(err) {
				return skipped, fmt.Errorf("failed to back up %s: %w", obj.Path, err)
			}
			skipped++
			atomic.AddInt64(&leg.skipped, 1)
			leg.tally.record(obj.Path, false)
			m.logger.Warn().Str("path", obj.Path).Err(err).Msg("Failed to read data file, skipping")
			continue
		}
		if sidecar != nil && isRegistrableDataFile(obj.Path) {
			if sidecar.add(obj.Path, sha, written, time.Now()) {
				m.logger.Warn().
					Str("path", obj.Path).
					Str("sha256", sha).
					Str("manifest_sha256", sidecar.byPath[filepath.ToSlash(obj.Path)].SHA256).
					Msg("Data file bytes differ from the checksum the cluster manifest registered for the path; the backup records the bytes it holds. Peers verifying a pull of this file against the manifest will reject it until the two agree")
			}
		}

		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, written)
		atomic.AddInt64(&leg.files, 1)
		atomic.AddInt64(&leg.bytes, written)
		// Republish so /status polling sees live counters — published Progress
		// values are immutable snapshots, not the struct being mutated here,
		// and the per-target slice is rebuilt rather than written into.
		run.publishTargets()

		if atomic.LoadInt64(&progress.ProcessedFiles)%100 == 0 {
			m.logger.Info().
				Int64("processed", atomic.LoadInt64(&progress.ProcessedFiles)).
				Int64("total", progress.TotalFiles).
				Msg("Backup progress")
		}
	}

	// Accumulate rather than overwrite: CreateBackup calls this once per file
	// group, and a later group must not erase an earlier group's skips.
	atomic.AddInt64(&progress.SkippedFiles, skipped)
	run.publishTargets()

	if skipped == 0 {
		return 0, nil
	}

	copied := atomic.LoadInt64(&progress.ProcessedFiles)

	m.logger.Warn().
		Int64("skipped", skipped).
		Int64("copied", copied).
		Str("target", leg.target.name).
		Msg("Files skipped during backup — backup will be incomplete")

	return skipped, nil
}

// findUnaddressable inventories data files that exist in source storage but
// that no listing returns, so nothing driven by a listing could copy them.
//
// Filtered with the same rule the inventory uses, because the storage layer
// reports everything a listing hid and only what a backup would have carried is
// the operator's loss: OS debris such as .DS_Store is hidden for good reason and
// naming it here would cry wolf on every macOS deployment.
//
// A backend that cannot enumerate them contributes nothing, which is correct
// rather than optimistic: it is the same position every caller was in before.
//
// A scoped backup (#1084) enumerates the hidden set under the prefixes it
// lists and nothing else (each <name>/, then _schema/ and _compaction_state/
// whole, since those two roots hold every database's anchors and state), so a
// one-database backup does not page the whole bucket or walk the whole tree
// for its diagnostic; the result is then filtered to what the scope owns, as
// the two reserved roots need. The gauge is set from the kept count, since it
// describes this run.
func (m *Manager) findUnaddressable(ctx context.Context, progress *Progress, sc *scope) []storage.UnusableObject {
	lister, ok := m.dataStorage.(storage.UnusableLister)
	if !ok {
		// Not clean, unchecked. Said out loud so a zero in the manifest is not
		// read as a guarantee.
		m.logger.Debug().Msg("Storage backend cannot enumerate hidden files; the backup cannot confirm it is complete")
		return nil
	}
	prefixes := []string{""}
	if !sc.empty() {
		prefixes = make([]string, 0, len(sc.names)+2)
		for _, name := range sc.names {
			prefixes = append(prefixes, name+"/")
		}
		prefixes = append(prefixes, "_schema/", compactionStateDir+"/")
	}
	var hidden []storage.UnusableObject
	for _, prefix := range prefixes {
		part, err := lister.ListUnusable(ctx, prefix)
		if err != nil {
			// Not fatal: failing the backup because the diagnostic failed would be
			// worse than the gap it reports. Loud, because the count is now part of
			// whether the backup can call itself complete.
			m.logger.Warn().Err(err).Str("prefix", prefix).Msg("Could not check for unaddressable files; the backup cannot confirm it is complete")
			return nil
		}
		hidden = append(hidden, part...)
	}
	var out []storage.UnusableObject
	for _, o := range hidden {
		if isBackupPayload(o.Path) && sc.ownsPath(o.Path) {
			out = append(out, o)
		}
	}
	// Set unconditionally, including 0: the gauge describes the store as of the
	// backup that just ran, so a deployment that fixed its keys must see it
	// fall back to zero rather than stay latched on the first bad file.
	metrics.Get().SetStorageUnaddressableFiles(int64(len(out)))
	if len(out) > 0 {
		atomic.AddInt64(&progress.UnaddressableFiles, int64(len(out)))
	}
	return out
}

// isBackupPayload reports whether a path is something CreateBackup would have
// copied had it been addressable, and therefore something whose absence is the
// operator's loss.
//
// It mirrors the inventory split above, and deliberately covers more than
// ".parquet". Iceberg metadata is the reason that split exists at all: losing a
// metadata.json or .avro loses a whole table even when every Parquet file it
// references survives, so an unaddressable one is worse than an unaddressable
// data file, not lesser. And a ".parquet.part" key on an object store is an
// ordinary committed object there (S3 and Azure do not stage), which the
// storage layer reports precisely because nothing else can name it.
func isBackupPayload(p string) bool {
	if isIcebergMetadata(p) {
		return true
	}
	return strings.HasSuffix(strings.TrimSuffix(p, storage.PartSuffix), ".parquet")
}

// unaddressableSampleCap bounds how many paths land in the manifest. The
// manifest is one JSON blob written to storage, and the over-length key shape
// makes each path up to a kilobyte, so an unbounded list could dwarf the
// manifest it is reported in. It bounds UnaddressableSample and SkippedSample
// alike (#977).
const unaddressableSampleCap = 32

// skipTally is one backup run's record of WHY files were skipped and WHICH.
// It lives outside Progress, which is published as value snapshots and so can
// only receive the list once, after the copy phases. Only copyDataFiles feeds
// it: that is the one loop with two skip causes — a source read that failed
// (the file vanished between the listing and the copy; nothing to do) and a
// destination key over the storage limit (permanent; the operator renames the
// file, #761). Compaction-state and outside-root warehouse skips are read-time
// skips with their own accounting and are not sampled.
//
// The unreadable count is derived, not stored: progress.SkippedFiles minus
// overlong. That holds only once every copy loop has returned nil, because a
// loop that aborts early has recorded here but not flushed its count to
// progress; checkSkipRatio is the one consumer and runs only then.
//
// No mutex: CreateBackup holds Manager.mu for the whole run and the copy loops
// are sequential.
type skipTally struct {
	overlong int64    // skips for a destination key over storage.MaxUsableKeyLen
	sample   []string // up to unaddressableSampleCap skipped source keys, either cause
	// paths is every skipped key, unbounded: on a cluster the end-of-run
	// manifest re-read reconciles each against the manifest (#1083), and a
	// run with enough skips for this to matter fails the ratio check anyway.
	paths []string
}

// record notes one skipped file. A nil tally is a no-op, so a caller without
// per-run accounting cannot panic.
func (t *skipTally) record(path string, overlong bool) {
	if t == nil {
		return
	}
	if overlong {
		t.overlong++
	}
	if len(t.sample) < unaddressableSampleCap {
		t.sample = append(t.sample, path)
	}
	t.paths = append(t.paths, path)
}

// recordUnaddressable puts the finding in the manifest and decides whether the
// run may still call itself a complete backup.
//
// Deliberately NOT folded into checkSkipRatio. That guard exists for a
// transient race (a file compaction removed between listing and copy), its
// denominator is the addressable inventory, and it short-circuits when
// SkippedFiles is zero, which is exactly the case here. Worse, its ratio would
// hard-fail a nine-file deployment with one legacy file forever, and its
// message sends the operator to diagnose storage rather than rename a file.
//
// The overlong backup DESTINATION key (#761) is the deliberate exception: that
// file is listable and readable, only the backup cannot hold it under its
// 37-byte prefix, so it is skipped and counted like a vanished file, the
// ratio applies, the ratio's message gives each cause's count, and the run's
// tally names the file in the manifest's SkippedSample (#977). Arc's own layout
// never reaches the threshold, so the hard-fail shape above needs a root
// where most keys are foreign and overlong.
func (m *Manager) recordUnaddressable(run *backupRun, unaddressable []storage.UnusableObject) {
	if len(unaddressable) == 0 {
		return
	}

	// Routed by the same rule as the files they would have been
	// (#1085 stage B2b-2), so each leg's manifest describes its own gaps. The
	// run-wide warning is still logged once.
	var sample []string
	for _, o := range unaddressable {
		leg := run.legFor(o.Path)
		leg.manifest.UnaddressableFiles++
		if len(leg.manifest.UnaddressableSample) < unaddressableSampleCap {
			leg.manifest.UnaddressableSample = append(leg.manifest.UnaddressableSample, o.Path)
		}
		if len(sample) < unaddressableSampleCap {
			sample = append(sample, o.Path)
		}
	}

	m.logger.Warn().
		Int("unaddressable", len(unaddressable)).
		Int64("addressable", run.inventoried()).
		Strs("sample", sample).
		Msg("Backup is incomplete: these files exist in storage but their key cannot be addressed, so they were not copied; rename them to conform to the storage key rules")
}

// sampleUnaddressable bounds a path list for logging, same cap as the manifest.
func sampleUnaddressable(objs []storage.UnusableObject) []string {
	out := make([]string, 0, unaddressableSampleCap)
	for i, o := range objs {
		if i == unaddressableSampleCap {
			break
		}
		out = append(out, o.Path)
	}
	return out
}

// checkSkipRatio fails the backup when too large a fraction of its files was skipped.
//
// Skipping tolerates two specific things: a file removed by compaction or
// retention between the listing and the copy, and a source key whose backup
// destination would exceed the storage key limit (#761). The race touches a
// handful of files at the tail of a run; the overrun is permanent, and the
// message gives each cause's count (from the run's tally, #977) so the
// operator renames keys rather than diagnosing storage. A large fraction of
// the backup skipping for either reason is a different event — throttling,
// credential expiry, a storage outage, a root
// full of foreign keys — and silently returning a fraction of the data as a
// successful backup is how an operator discovers the gap at restore time
// instead of at backup time.
//
// Evaluated once over every file group, not per group: a stale entry or two in a
// small Iceberg metadata set is a large fraction of that set but a negligible
// fraction of the backup, and must not abort a run whose data files all copied.
func (m *Manager) checkSkipRatio(run *backupRun, totalFiles int) error {
	skipped := atomic.LoadInt64(&run.progress.SkippedFiles)
	if skipped == 0 || totalFiles == 0 {
		return nil
	}
	if float64(skipped) > maxSkipRatio*float64(totalFiles) {
		return fmt.Errorf("backup failed: %d of %d files skipped (>%.0f%%): %s",
			skipped, totalFiles, maxSkipRatio*100, describeSkips(skipped, run.overlongSkips(), run.minMaxSourceKeyBytes()))
	}
	return nil
}

// describeSkips names each skip cause with its count, leaving out a cause with
// none, so a run skipped for one reason does not read "and 0 …". The unreadable
// count is everything the run did not attribute to an overlong key: data-file
// read failures, vanished compaction manifests and outside-root warehouse read
// failures alike. The byte threshold is the one the per-file warning reports as
// max_source_key_bytes and the backup docs quote, not the destination limit.
// It is passed in rather than computed here because it depends on the
// destinations' own key prefixes, and with several targets it is the SMALLEST
// of them: "which no backup destination key can hold" is only true of every
// target at the smallest limit. The per-file warning named the target whose
// limit actually rejected that file.
func describeSkips(skipped, overlong int64, maxSourceKeyBytes int) string {
	var parts []string
	if unreadable := skipped - overlong; unreadable > 0 {
		parts = append(parts, fmt.Sprintf("%d could not be read at copy time (check source storage)", unreadable))
	}
	if overlong > 0 {
		parts = append(parts, fmt.Sprintf("%d have source keys longer than %d bytes, which no backup destination key can hold (rename them)",
			overlong, maxSourceKeyBytes))
	}
	return strings.Join(parts, "; ")
}

// streamBackupFile streams a file from data storage to backup storage via a temp file,
// avoiding loading the entire file into memory (important for large Parquet files).
// It returns the number of bytes actually copied.
func (m *Manager) streamBackupFile(ctx context.Context, dest backupTarget, srcPath, destPath string) (int64, error) {
	written, _, err := m.streamBackupFileSHA(ctx, dest, srcPath, destPath)
	return written, err
}

// streamBackupFileSHA is streamBackupFile returning, as well, the hex SHA-256
// of the bytes that went through (#1083). The hash is taken on the hop from
// data storage to the temp file, which already runs through a buffered loop
// for trackingWriter (see readto.go), so hashing adds CPU on a disk-bound
// path and no extra pass over the bytes. The sidecar carries it so a cluster
// restore can register the file without hashing it again, and peers can
// verify a pull of the restored file against the manifest.
func (m *Manager) streamBackupFileSHA(ctx context.Context, dest backupTarget, srcPath, destPath string) (int64, string, error) {
	tmpFile, err := createTempFile("arc-backup-*.parquet")
	if err != nil {
		return 0, "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)
	defer tmpFile.Close()

	// Stream from data storage to temp file, hashing on the way. The hasher
	// never fails, so trackingWriter still attributes a write error to the
	// temp file alone.
	hasher := sha256.New()
	tw := &trackingWriter{w: tmpFile}
	if err := m.dataStorage.ReadTo(ctx, srcPath, io.MultiWriter(tw, hasher)); err != nil {
		return 0, "", classifyReadToFailure(srcPath, err, tw.err, errBackupRead, "data storage")
	}

	// Size the upload from the temp file rather than the listing: the listing is a
	// point-in-time snapshot that compaction or retention may have invalidated, and
	// a declared size that disagrees with the reader can truncate the upload on
	// backends that send it as Content-Length. Matches streamRestoreFile.
	info, err := tmpFile.Stat()
	if err != nil {
		return 0, "", fmt.Errorf("failed to stat temp file: %w", err)
	}
	size := info.Size()

	// Rewind for upload
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return 0, "", fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Stream from temp file to this leg's backup destination
	if err := dest.backend.WriteReader(ctx, destPath, tmpFile, size); err != nil {
		m.cleanupPartialBackupWrite(ctx, dest, destPath)
		return 0, "", fmt.Errorf("failed to write to %s: %w", dest.describe(), err)
	}

	return size, hex.EncodeToString(hasher.Sum(nil)), nil
}

// addToInventory counts one listed data file in the manifest.
//
// Parquet under a reserved root (the field schema anchors under _schema/,
// #914) is Arc's own state, not a database: it is copied and counted with the
// data files, because the restore compares TotalFiles against every .parquet
// object present and an anchor left out of the count would hide one missing
// data file, but it is kept out of the database inventory (#927).
func addToInventory(manifest *Manifest, dbMap map[string]*DatabaseInfo, obj storage.ObjectInfo) {
	manifest.TotalFiles++
	manifest.TotalSizeBytes += obj.Size
	if isReservedRootParquet(obj.Path) {
		manifest.AuxiliaryFiles++
		return
	}

	db, meas := parseDBMeasurement(obj.Path)
	di, exists := dbMap[db]
	if !exists {
		di = &DatabaseInfo{Name: db}
		dbMap[db] = di
	}
	di.FileCount++
	di.SizeBytes += obj.Size
	for i := range di.Measurements {
		if di.Measurements[i].Name == meas {
			di.Measurements[i].FileCount++
			di.Measurements[i].SizeBytes += obj.Size
			return
		}
	}
	di.Measurements = append(di.Measurements, MeasurementInfo{Name: meas, FileCount: 1, SizeBytes: obj.Size})
}

// removeFromInventory takes one copied data file back out of the manifest
// counts (the end-of-run re-check found the cluster no longer lists it).
func removeFromInventory(manifest *Manifest, dbMap map[string]*DatabaseInfo, path string, size int64) {
	manifest.TotalFiles--
	manifest.TotalSizeBytes -= size
	db, meas := parseDBMeasurement(path)
	di, ok := dbMap[db]
	if !ok {
		return
	}
	di.FileCount--
	di.SizeBytes -= size
	for i := range di.Measurements {
		if di.Measurements[i].Name != meas {
			continue
		}
		di.Measurements[i].FileCount--
		di.Measurements[i].SizeBytes -= size
		if di.Measurements[i].FileCount <= 0 {
			di.Measurements = append(di.Measurements[:i], di.Measurements[i+1:]...)
		}
		break
	}
	if di.FileCount <= 0 {
		delete(dbMap, db)
	}
}

// manifestCrossCheck is the first comparison of the listing against the
// cluster manifest (#1083), kept so the end of the run can re-check it.
type manifestCrossCheck struct {
	byPath map[string]ManifestFile // the manifest as of the first snapshot
	// unregistered are the listed database data files the first snapshot did
	// not have: provisionally excluded, re-read at the end of the data copy.
	unregistered []storage.ObjectInfo
	// manifestOnly are the manifest's data-file paths the listing did not
	// have, sorted: provisionally a gap, re-checked at the end of the data
	// copy.
	manifestOnly []string
}

// crossCheckManifest syncs the manifest, snapshots it, and splits the listed
// Parquet files into the ones this run copies now (reserved-root Parquet,
// plus every database data file the manifest lists) and the ones it holds
// back, noting the manifest entries the listing lacks. One ManifestFiles
// call, no per-path lookups.
//
// An EMPTY manifest while the listing holds data files fails the backup: it
// means this node has no populated Raft manifest to check against (a fresh
// FSM, or a coordinator without one), and reading it as "nothing is data"
// would produce an empty backup that reports success.
//
// Scoped (#1084): the manifest describes every database, the listing only
// the scope, so a manifest entry counts as manifest-only solely when the
// scope owns it. Ownership here is the PATH first segment alone: the entries
// in the manifest are all database data files (isRegistrableDataFile), and
// the entry's Database label is the canonical database of an edge-sync spoke
// file, not its storage-root segment.
func (m *Manager) crossCheckManifest(ctx context.Context, parquetFiles []storage.ObjectInfo, sc *scope) ([]storage.ObjectInfo, *manifestCrossCheck, error) {
	if err := m.cluster.Sync(ctx); err != nil {
		return nil, nil, fmt.Errorf("backup failed: could not sync the cluster manifest before snapshotting it, so a stale view might call registered files unregistered: %w", err)
	}
	entries := m.cluster.ManifestFiles()
	xc := &manifestCrossCheck{byPath: make(map[string]ManifestFile, len(entries))}
	for _, e := range entries {
		xc.byPath[filepath.ToSlash(e.Path)] = e
	}
	listed := make(map[string]struct{}, len(parquetFiles))
	keep := make([]storage.ObjectInfo, 0, len(parquetFiles))
	registrable, kept := 0, 0
	for _, obj := range parquetFiles {
		p := filepath.ToSlash(obj.Path)
		listed[p] = struct{}{}
		if !isRegistrableDataFile(p) {
			keep = append(keep, obj)
			continue
		}
		registrable++
		if _, ok := xc.byPath[p]; ok {
			keep = append(keep, obj)
			kept++
		} else {
			xc.unregistered = append(xc.unregistered, obj)
		}
	}
	if len(entries) == 0 && registrable > 0 {
		return nil, nil, fmt.Errorf("backup refused: the cluster manifest is empty while this node lists %d data files; an empty manifest means this node has no populated Raft file manifest to check against (cluster.raft_data_dir unset, or a manifest not yet populated), not that nothing is data. Take the backup on a node with a populated manifest", registrable)
	}
	for p := range xc.byPath {
		if _, ok := listed[p]; !ok && isRegistrableDataFile(p) && sc.ownsData(p) {
			xc.manifestOnly = append(xc.manifestOnly, p)
		}
	}
	sort.Strings(xc.manifestOnly)
	// manifest_entries is the whole cluster's count; the other counts are
	// the scope's when there is one.
	ev := m.logger.Info().
		Int("manifest_entries", len(entries)).
		Int("listed_data_files", registrable).
		Int("listed_and_registered", kept).
		Int("unregistered_provisional", len(xc.unregistered)).
		Int("manifest_only_provisional", len(xc.manifestOnly))
	if !sc.empty() {
		ev = ev.Strs("scope", sc.names)
	}
	ev.Msg("Cluster manifest cross-check: provisional counts, re-checked at the end of the data copy")
	return keep, xc, nil
}

// recheckClusterManifest syncs and re-reads the manifest once after the data
// copy and settles every provisional decision against it:
//
//   - a copied data file the manifest no longer lists is removed from the
//     backup (storage, inventory, sidecar) and counted as
//     left_manifest_during_run: the cluster says it is no longer data, the
//     inputs of a compaction whose phase 2 landed during the run above all;
//   - a held-back file the manifest now lists is copied (its registration had
//     not reached this node at the first snapshot); one still absent is
//     counted as unregistered_skipped;
//   - a manifest entry this node has pulled since the listing is copied; one
//     still absent is counted as a manifest-only gap; one that has left the
//     manifest meanwhile was retention or compaction doing its job;
//   - a skipped file (unreadable at copy time) that has left the manifest is
//     counted as reconciled: not missing data.
//
// Scoped (#1084): the manifest-only pass copies a late arrival only when the
// scope owns its path; the provisional list was built that way, and the
// check is repeated here so a scoped backup can never copy another
// database's file however the two snapshots differ.
//
// ROUTED (#1085 stage B2b-2), and that is a step of its own in the run order
// rather than part of the copy phase, because this function both WRITES and
// DELETES after every leg has copied:
//
//   - a delete of a copied file the manifest has stopped listing goes to the
//     leg that holds it, which is the leg routeKey sent it to. Unrouted, it
//     would go to the default destination — and LocalBackend.Delete returns
//     nil for a missing key while S3's DeleteObject is idempotent, so it would
//     REPORT SUCCESS while the file stayed on the routed target. That is
//     precisely the double-serve #1083 and #930 exist to prevent, arrived at
//     through a delete that looks fine;
//   - a late arrival is copied into the leg its own path routes to, with that
//     leg's manifest, dbMap, sidecar and tally.
//
// Its error behaviour is unchanged: a delete that fails still fails the run.
func (m *Manager) recheckClusterManifest(ctx context.Context, run *backupRun, xc *manifestCrossCheck, sc *scope) error {
	progress := run.progress
	if err := m.cluster.Sync(ctx); err != nil {
		return fmt.Errorf("backup failed: could not sync the cluster manifest for the end-of-run check: %w", err)
	}
	entries := m.cluster.ManifestFiles()
	now := make(map[string]ManifestFile, len(entries))
	for _, e := range entries {
		now[filepath.ToSlash(e.Path)] = e
	}

	// (c) copied, and no longer listed. A storage delete that fails leaves
	// a file in the backup the cluster says is not data; that is the
	// double-serve this exists to prevent, so it fails the run.
	var left int64
	// Run-wide sample for the warning, next to the per-leg samples that go in
	// the manifests: the log line names paths because that is what sends an
	// operator to the right file, and a count alone sends them nowhere. Built
	// here rather than from the merged manifests so the warning is independent
	// of the per-leg sample caps.
	var leftSample []string
	for _, leg := range run.legs {
		var legLeft int64
		for _, row := range leg.sidecar.rows() {
			if _, ok := now[row.Path]; ok {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			dest := run.id + "/data/" + row.Path
			if err := leg.target.backend.Delete(ctx, dest); err != nil {
				return fmt.Errorf("backup failed: %s left the cluster manifest during the run and its copy could not be removed from %s: %w", row.Path, leg.target.describe(), err)
			}
			removeFromInventory(leg.manifest, leg.dbMap, row.Path, row.SizeBytes)
			leg.sidecar.drop(row.Path)
			atomic.AddInt64(&progress.ProcessedFiles, -1)
			atomic.AddInt64(&progress.ProcessedBytes, -row.SizeBytes)
			atomic.AddInt64(&leg.files, -1)
			atomic.AddInt64(&leg.bytes, -row.SizeBytes)
			progress.TotalFiles--
			legLeft++
			left++
			if len(leg.manifest.LeftManifestSample) < unaddressableSampleCap {
				leg.manifest.LeftManifestSample = append(leg.manifest.LeftManifestSample, row.Path)
			}
			if len(leftSample) < unaddressableSampleCap {
				leftSample = append(leftSample, row.Path)
			}
		}
		leg.manifest.LeftManifestDuringRun = legLeft
	}
	if left > 0 {
		progress.TotalBytes = run.inventoriedBytes()
		run.publishTargets()
		m.logger.Warn().
			Int64("left_manifest_during_run", left).
			Strs("sample", leftSample).
			Msg("Data files left the cluster manifest while the backup ran and were removed from it again: compaction, retention or tiering replaced or removed them. The backup holds what the cluster holds")
	}

	// (b) listed but unregistered at the first snapshot.
	late := map[*backupLeg][]storage.ObjectInfo{}
	var lateCount int
	var still []storage.ObjectInfo
	// Writes into the byPath map that every leg's sidecar aliases, so the row
	// becomes visible to all of them. Correct — it is the manifest snapshot,
	// not a per-leg set, and only the leg's files list decides what each sidecar
	// emits — and safe only because this runs after every copy, single
	// threaded. See the aliasing note where the sidecars are built.
	addLate := func(obj storage.ObjectInfo, e ManifestFile) {
		leg := run.legFor(obj.Path)
		late[leg] = append(late[leg], obj)
		leg.sidecar.byPath[filepath.ToSlash(obj.Path)] = e
		lateCount++
	}
	for _, obj := range xc.unregistered {
		p := filepath.ToSlash(obj.Path)
		if e, ok := now[p]; ok {
			addLate(obj, e)
		} else {
			still = append(still, obj)
		}
	}
	// (a) in the manifest but not in the listing: pulled since, or still absent.
	var absent int64
	// Run-wide sample for the warning below; see leftSample above.
	var absentSample []string
	for _, p := range xc.manifestOnly {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !sc.ownsData(p) {
			// Defense in depth: the provisional list was already filtered by
			// crossCheckManifest, so this never fires; it stays so a scoped
			// backup cannot copy another database's file however the two
			// snapshots are later reconciled.
			continue
		}
		e, ok := now[p]
		if !ok {
			continue // left the manifest since: retention or compaction, not a gap
		}
		size, err := m.dataStorage.StatFile(ctx, p)
		if err != nil {
			// Cannot confirm the file is here; counting it keeps the backup
			// honest (INCOMPLETE) rather than optimistic.
			m.logger.Warn().Err(err).Str("path", p).Msg("Could not check whether a manifest entry is present locally; counting it as absent")
			size = -1
		}
		if size >= 0 {
			// Pulled since the listing: copy it. It is registered data this
			// node holds, and leaving it out would report it as a gap.
			addLate(storage.ObjectInfo{Path: p, Size: size}, e)
			continue
		}
		absent++
		leg := run.legFor(p)
		leg.manifest.ManifestOnlyFiles++
		if len(leg.manifest.ManifestOnlySample) < unaddressableSampleCap {
			leg.manifest.ManifestOnlySample = append(leg.manifest.ManifestOnlySample, p)
		}
		if len(absentSample) < unaddressableSampleCap {
			absentSample = append(absentSample, p)
		}
	}
	if lateCount > 0 {
		progress.TotalFiles += int64(lateCount)
		m.logger.Info().Int("files", lateCount).Msg("Data files registered in the cluster manifest, or pulled, since the first snapshot; copying them")
		for _, leg := range run.legs {
			files := late[leg]
			if len(files) == 0 {
				continue
			}
			for _, obj := range files {
				addToInventory(leg.manifest, leg.dbMap, obj)
			}
			progress.TotalBytes = run.inventoriedBytes()
			run.publishTargets()
			skipped, err := m.copyDataFiles(ctx, leg, files, leg.sidecar)
			if err != nil {
				return err
			}
			leg.dataSkipped += skipped
		}
	}
	if len(still) > 0 {
		var sample []string
		for _, obj := range still {
			leg := run.legFor(obj.Path)
			leg.manifest.UnregisteredSkipped++
			p := filepath.ToSlash(obj.Path)
			if len(leg.manifest.UnregisteredSample) < unaddressableSampleCap {
				leg.manifest.UnregisteredSample = append(leg.manifest.UnregisteredSample, p)
			}
			if len(sample) < unaddressableSampleCap {
				sample = append(sample, p)
			}
		}
		m.logger.Warn().
			Int("unregistered_skipped", len(still)).
			Strs("sample", sample).
			Msg("Data files in this node storage are not in the cluster manifest and were not backed up: a compaction or retention input awaiting unlink, a pre-cluster file, or a dropped registration. The manifest, not the listing, says what is data on a cluster")
	}
	if absent > 0 {
		m.logger.Warn().
			Int64("manifest_only_files", absent).
			Strs("sample", absentSample).
			Msg("Backup is incomplete: the cluster manifest lists files this node does not hold (not pulled yet); re-run the backup once this node has caught up")
	}

	// (d) skips that are not missing data: the file had left the manifest by
	// now, so compaction, retention or tiering removed it between the listing
	// and its copy. Per leg, because each leg's SkippedReconciled has to
	// describe the same population as its own SkippedFiles.
	for _, leg := range run.legs {
		for _, p := range leg.tally.paths {
			p = filepath.ToSlash(p)
			if !isRegistrableDataFile(p) {
				continue
			}
			if _, ok := now[p]; !ok {
				leg.manifest.SkippedReconciled++
			}
		}
	}

	// The run-wide mirrors on the progress record: the status endpoint reports
	// the run, and the per-leg figures are in the per-leg manifests.
	merged := run.crossCheckTotals()
	progress.UnregisteredSkipped = merged.UnregisteredSkipped
	progress.UnregisteredSample = merged.UnregisteredSample
	progress.ManifestOnlyFiles = merged.ManifestOnlyFiles
	progress.ManifestOnlySample = merged.ManifestOnlySample
	progress.LeftManifestDuringRun = merged.LeftManifestDuringRun
	progress.LeftManifestSample = merged.LeftManifestSample
	run.publishTargets()
	return nil
}

// crossCheckTotals sums the cross-check counters and unions their samples over
// every leg, for the run-wide progress mirrors.
func (r *backupRun) crossCheckTotals() *Manifest {
	out := &Manifest{}
	for _, leg := range r.legs {
		out.UnregisteredSkipped += leg.manifest.UnregisteredSkipped
		out.ManifestOnlyFiles += leg.manifest.ManifestOnlyFiles
		out.LeftManifestDuringRun += leg.manifest.LeftManifestDuringRun
		out.UnregisteredSample = appendSample(out.UnregisteredSample, leg.manifest.UnregisteredSample)
		out.ManifestOnlySample = appendSample(out.ManifestOnlySample, leg.manifest.ManifestOnlySample)
		out.LeftManifestSample = appendSample(out.LeftManifestSample, leg.manifest.LeftManifestSample)
	}
	return out
}

// cleanupPartialWrite removes the staging file a failed WriteReader leaves
// behind on a backend whose destination key may already hold an object this
// run did not write. The restore path is the only such caller: its destination
// is a live data key.
//
// LocalBackend.WriteReader deliberately preserves "<path>.part" on failure so the
// file-replication puller can resume from the last committed byte. Neither backup
// nor restore has a resume path — a retried backup starts over under a fresh
// backup ID, a retried restore rewrites the key from scratch — so that
// staging file is unreferenced garbage: never read, never listed as a backup
// (it has no manifest.json), and holding disk equal to the bytes transferred
// before the failure.
//
// A non-staging backend gets NOTHING here, and that is the whole difference
// from cleanupPartialBackupWrite below. S3 and Azure leave the previous object
// at the key untouched when a write fails (see that function for the
// evidence), so deleting the key would turn a failed OVERWRITE of a
// registered data file into deletion of that file, with a manifest entry
// nothing re-registers (#1101). Leaving the old bytes in place is the correct
// outcome of a failed restore write.
//
// Best-effort by design: the write already failed, so the cleanup very likely
// fails too (unwritable volume, storage unreachable). A cleanup failure must not
// mask the real error, so it is logged at debug and discarded.
func (m *Manager) cleanupPartialWrite(ctx context.Context, backend storage.Backend, destPath string) {
	// Addressed through the staging API rather than by appending the suffix to
	// the key. That suffix is reserved now, because the staging file of key
	// "x" used to BE the committed object "x.part" (#744).
	si, ok := backend.(storage.StagingInspector)
	if !ok {
		return
	}
	if err := si.DeleteStaged(ctx, destPath); err != nil {
		m.logger.Debug().
			Str("path", destPath).
			Err(err).
			Msg("Could not remove partial staging file after a failed write")
	}
}

// cleanupPartialBackupWrite compensates a failed write to BACKUP storage
// (#1101). It takes the LEG's destination since #1085 stage B2b-2, because a
// run now has several — but the invariant it rests on is still a property of
// the KEY and not of the backend: every key it is given is "<backupID>/..."
// for a backup ID minted by this run, so nothing but this run can have put an
// object there on any target, and removing it is never destructive.
//
// WHAT THAT INVARIANT ACTUALLY RESTS ON, corrected (#1085 stage B2b-1). An
// earlier draft of this comment claimed the premise moved from the code to the
// configuration once a destination could be remote. It did not. Every one of
// the four call sites — the data copy, the SQLite copy, the sidecar and the
// warehouse copy — passes a key of the form "<backupID>/…" for an ID this run
// minted, and no other writer in Arc produces that shape, so the key
// NAMESPACE is what makes the Delete safe — and that is a property of the
// code, unchanged by where the destination points or how many there are.
//
// Two honest qualifications rather than an absolute. The shape is not
// reserved: a hyphen is legal in a key segment, so a database named with the
// exact 31-character backup-ID spelling could in principle collide, which
// makes the namespace claim true in practice rather than by construction. And
// it only matters at all if two stores overlap, which is the thing
// config.checkBackupDestinationOverlap refuses.
//
// So the overlap refusal is still right, and it protects DIFFERENT things than
// this function: a backup destination inside the storage root is re-copied by
// every subsequent backup (the data listing returns every .parquet under the
// root), which on a cluster inflates the unregistered-skip count until the
// skip ratio refuses every replace-mode restore; and with reconciliation
// enabled and dry-run off, the sweep DELETES the backups, because a backup
// data key has nine segments and its managed-path heuristic triggers at seven.
// Those are the hazards. This Delete is cheap insurance on top.
//
// What does follow for THIS function: with a remote target the Delete below is
// the branch that always runs, where before it was unreachable because only
// LocalBackend implements storage.StagingInspector (s3.go and azure.go say in
// comments that they do not).
//
// What each backend actually leaves behind, established against the vendored
// SDKs rather than assumed:
//
//   - LocalBackend stages: "<key>.part" exists and the key does not. Removed
//     through DeleteStaged, exactly as before.
//   - S3 commits nothing on a failed write. A failed PutObject writes no
//     object, and manager.Uploader defaults LeavePartsOnError to false, so a
//     failed multipart calls AbortMultipartUpload itself
//     (feature/s3/manager@v1.20.12 upload.go:312, :855).
//   - Azure commits nothing either. azblob's UploadStream stages blocks and
//     then issues ONE CommitBlockList, or for a payload inside a single block
//     one Upload (blockblob@v1.6.4 chunkwriting.go:146, :171). Uncommitted
//     blocks are not readable and the service garbage-collects them.
//
// So the Delete below covers exactly one case: the commit SUCCEEDED and the
// success never came back — a lost response, or a retry that reported the
// error after the object had landed. That is the only way a committed object
// sits at the key after WriteReader returned an error. It is cheap insurance
// against that, and it is honest about what it does not reach: a Delete cannot
// abort an abandoned S3 multipart upload (the SDK's own abort runs on the
// failing context, so a CANCELLED one leaves parts behind — buckets taking
// Arc backups want an AbortIncompleteMultipartUpload lifecycle rule), and it
// cannot touch uncommitted Azure blocks.
//
// Best-effort, like the staging branch: logged at Debug and never fatal. The
// write already failed and that error is the one the caller must surface.
func (m *Manager) cleanupPartialBackupWrite(ctx context.Context, dest backupTarget, destPath string) {
	if si, ok := dest.backend.(storage.StagingInspector); ok {
		if err := si.DeleteStaged(ctx, destPath); err != nil {
			m.logger.Debug().
				Str("path", destPath).
				Err(err).
				Msg("Could not remove partial staging file after a failed backup write")
		}
		return
	}
	if err := dest.backend.Delete(ctx, destPath); err != nil {
		m.logger.Debug().
			Str("path", destPath).
			Err(err).
			Msg("Could not remove the backup destination key after a failed write")
	}
}

// backupSQLite copies the shared SQLite database into the backup. Always the
// default leg: the database is instance-wide state, belonging to no routed
// database.
func (m *Manager) backupSQLite(ctx context.Context, dest backupTarget, backupID string) error {
	return m.backupSQLiteFile(ctx, dest, backupID, m.sqliteDBPath, "arc.db")
}

// snapshotSQLite writes a consistent snapshot of the live database at dbPath to
// a fresh temporary file and returns its path. The caller must remove it.
//
// VACUUM INTO reads the database (including un-checkpointed WAL frames) under
// one read transaction, so concurrently committing writers cannot interleave
// pages into the snapshot — the failure mode of checkpoint-then-copy, where a
// checkpoint between the copy's start and end rewrites the main file mid-read
// once the WAL passes the auto-checkpoint threshold.
func snapshotSQLite(ctx context.Context, dbPath string) (string, error) {
	dir, err := os.MkdirTemp(filepath.Dir(dbPath), ".arc-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	snapshotPath := filepath.Join(dir, "snapshot.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to open SQLite for snapshot: %w", err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", snapshotPath); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("SQLite snapshot failed: %w", err)
	}
	if err := os.Chmod(snapshotPath, 0600); err != nil {
		os.RemoveAll(dir)
		return "", fmt.Errorf("failed to restrict SQLite snapshot: %w", err)
	}
	return snapshotPath, nil
}

// backupSQLiteFile copies one SQLite database file into the backup under
// metadata/<destName>, as a consistent snapshot of the live database streamed
// via the storage backend rather than a file read of the live path.
func (m *Manager) backupSQLiteFile(ctx context.Context, dest backupTarget, backupID, dbPath, destName string) error {
	snapshotPath, err := snapshotSQLite(ctx, dbPath)
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Dir(snapshotPath))

	// Get file size for WriteReader
	info, err := os.Stat(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to stat SQLite snapshot: %w", err)
	}
	size := info.Size()

	// Stream via temp file to avoid loading entire DB in memory
	f, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite snapshot: %w", err)
	}
	defer f.Close()

	destPath := backupID + "/metadata/" + destName
	if err := dest.backend.WriteReader(ctx, destPath, f, size); err != nil {
		m.cleanupPartialBackupWrite(ctx, dest, destPath)
		return fmt.Errorf("failed to write the SQLite backup to %s: %w", dest.describe(), err)
	}

	m.logger.Info().
		Str("backup_id", backupID).
		Str("database", destName).
		Int64("bytes", size).
		Msg("SQLite database backed up")
	return nil
}

// backupConfig copies the arc.toml config file into the backup.
//
// SECURITY: arc.toml typically contains plaintext credentials (S3 secret key,
// Azure account key, cluster shared secret). The backup storage must be treated
// as secret material with the same access controls as the live config.
// Operators who cannot secure backup storage should set backup.include_config
// to false in arc.toml.
func (m *Manager) backupConfig(ctx context.Context, dest backupTarget, backupID string) error {
	data, err := os.ReadFile(m.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	destPath := backupID + "/config/arc.toml"
	// No cleanupPartialBackupWrite here either; see the manifest write and
	// #1110.
	// Bounded: arc.toml is small and fixed-size, so a minute is generous and a
	// stalled destination must not hold the operation lock for the whole run
	// budget. See destinationProbeTimeout.
	writeCtx, cancel := withDestinationTimeout(ctx)
	err = dest.backend.Write(writeCtx, destPath, data)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to write the config backup to %s: %w", dest.describe(), err)
	}

	m.logger.Info().Str("backup_id", backupID).Msg("Config file backed up")
	return nil
}

// isIcebergMetadata reports whether a storage path is an Iceberg warehouse metadata file that
// must be backed up alongside data. Iceberg tables written by Arc's exporter live at
// {nsPrefix}_{db}.db/{measurement}/metadata/*, containing table metadata (*.metadata.json,
// incl. our v<N>.metadata.json reader copies), manifest lists + manifests (*.avro), and
// version-hint.text (current-version pointer for directory-based readers).
//
// Deliberately a catch-all — ANY non-parquet file under a "/metadata/" segment — rather than an
// allowlist of today's extensions. Iceberg keeps adding metadata file types (e.g. Puffin
// .puffin statistics/index files); an allowlist silently drops them from the backup and loses
// them on restore. Over-copying a stray file is cheap; losing table metadata is not.
//
// Safe against a user measurement literally named "metadata": its files are .parquet, and the
// caller's switch tests the .parquet branch FIRST, so data files never reach this predicate.
// The referenced parquet DATA files are backed up normally.
func isIcebergMetadata(p string) bool {
	p = filepath.ToSlash(p)
	if !strings.Contains(p, "/metadata/") {
		return false
	}
	return !strings.HasSuffix(p, ".parquet")
}

// parseDBMeasurement extracts the database and measurement from a storage path.
// Path format: {database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.parquet
// isReservedRootParquet reports whether a data-file key sits under an
// underscore-prefixed root directory, which Arc reserves for its own state
// (see storage.IsReservedRootDir). Dot-prefixed roots are deliberately not
// included: edge sync's unverified receive area lives there and its
// treatment by backup is unchanged by #927.
func isReservedRootParquet(path string) bool {
	first, _, _ := strings.Cut(filepath.ToSlash(path), "/")
	return strings.HasPrefix(first, "_")
}

// compactionStateDir is compaction.ManifestBasePath, spelled here so this
// package does not import compaction (which links DuckDB).
const compactionStateDir = "_compaction_state"

// copyStateFiles copies compaction's recovery state like copyDataFiles, with
// one difference in what a read failure means. A data file that vanished
// between the listing and its copy is an expected skip. A manifest that
// vanished is expected too (its job finished), but a manifest that could
// NOT be read and still exists (a throttled or failed GET) must fail the
// backup: continuing would copy the job's output and inputs with nothing to
// reconcile them, which is exactly the double-serve #930 exists to close,
// and a manifest is a few hundred bytes, so retrying the backup is cheap.
//
// The destination key-length check added by #1100 is checked before the write
// like copyDataFiles, and then does the OPPOSITE: it fails the run instead of
// skipping. An over-long destination key is a manifest that exists and cannot
// be copied, which is the very case the rule above already covers.
//
// The three copy paths differ on whether absence is detectable BY A RESTORE,
// not arbitrarily. A skipped data file is counted in the manifest, named in
// the skip sample, carried into the INCOMPLETE marker, and still held by the
// source, so a restore can see what it is missing. A skipped recovery
// manifest is invisible to a restore: it brings back the compacted output AND
// the inputs it replaced, nothing reconciles them, and the partition serves
// every row twice. So data skips; compaction state and the Iceberg warehouse
// fail.
//
// A third option was available, and understanding why it loses is the point:
// skip, count, and set the incomplete marker. It is not that nothing RECORDS
// the skip — progress.SkippedFiles and the manifest would both carry it. It is
// that the restore path does not READ that counter. It reads
// UnregisteredSkipped, ManifestOnlyFiles, LeftManifestDuringRun, and
// SkippedFiles against TotalFiles; a compaction-state skip is deliberately
// outside the TotalFiles population (see skipTally), so the one number that
// would carry the warning is the one a restore never consults. Teaching it to
// is a larger change than this, and would still leave the window between a
// backup that reported success and the restore that acts on it.
//
// What the check buys is the message, not the outcome. Before it, the overrun
// surfaced from inside WriteReader as a generic backup-storage write failure,
// which is not a source-read error and so failed the run anyway but said
// nothing about the key. Unlike the unreadable-but-exists branch below, this
// failure is deterministic: a retry changes nothing, so the error asks for a
// rename instead. Arc writes its own state keys short and fixed-shape, so
// reaching this needs a foreign file dropped under _compaction_state/.
func (m *Manager) copyStateFiles(ctx context.Context, leg *backupLeg) (int64, error) {
	run := leg.run
	progress := run.progress
	var skipped int64
	for _, obj := range leg.state {
		select {
		case <-ctx.Done():
			return skipped, ctx.Err()
		default:
		}
		destPath := run.id + "/data/" + obj.Path
		// Per-destination threshold, for the reason recorded in copyDataFiles,
		// and the message names the target whose limit was hit.
		if leg.target.keyTooLong(destPath) {
			return skipped, fmt.Errorf("backup failed: the backup destination key for compaction recovery state %s on %s is too long to store (destination_key_bytes=%d, maximum_key_bytes=%d, max_source_key_bytes=%d). A restore of a backup missing it would bring back the compacted output AND the inputs it replaced with nothing to reconcile them, so that partition would serve every row twice. Rename the file under %s/ so that its SOURCE KEY is at most max_source_key_bytes bytes",
				obj.Path, leg.target.describe(), leg.target.destinationKeyBytes(destPath), storage.MaxUsableKeyLen, leg.target.maxSourceKeyBytes(), compactionStateDir)
		}
		written, err := m.streamBackupFile(ctx, leg.target, obj.Path, destPath)
		if err != nil {
			if !isSourceReadError(err) {
				return skipped, fmt.Errorf("failed to back up %s: %w", obj.Path, err)
			}
			exists, existsErr := m.dataStorage.Exists(ctx, obj.Path)
			if existsErr != nil || exists {
				return skipped, fmt.Errorf("backup failed: compaction recovery state %s could not be read but still exists (read: %v; exists: %v); a backup without it could restore a compacted output next to the inputs it replaced, so retry the backup", obj.Path, err, existsErr)
			}
			skipped++
			atomic.AddInt64(&leg.skipped, 1)
			m.logger.Info().Str("path", obj.Path).Msg("Compaction manifest gone before its copy: its job finished; skipping")
			continue
		}
		atomic.AddInt64(&progress.ProcessedFiles, 1)
		atomic.AddInt64(&progress.ProcessedBytes, written)
		atomic.AddInt64(&leg.files, 1)
		atomic.AddInt64(&leg.bytes, written)
		run.publishTargets()
	}
	atomic.AddInt64(&progress.SkippedFiles, skipped)
	run.publishTargets()
	return skipped, nil
}

// isCompactionState reports whether a key is compaction's crash-recovery
// state: a manifest or a parked manifest under _compaction_state/.
func isCompactionState(path string) bool {
	return strings.HasPrefix(filepath.ToSlash(path), compactionStateDir+"/")
}

func parseDBMeasurement(path string) (database, measurement string) {
	path = filepath.ToSlash(path)
	parts := strings.SplitN(path, "/", 3)
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	if len(parts) == 1 {
		return parts[0], "unknown"
	}
	return "unknown", "unknown"
}
