package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// ManifestStatus represents the state of a compaction manifest
type ManifestStatus string

const (
	// ManifestStatusPending indicates the compaction is in progress
	ManifestStatusPending ManifestStatus = "pending"
)

// ManifestBasePath is the base directory for storing compaction manifests
const ManifestBasePath = "_compaction_state"

// ManifestQuarantineSuffix is appended to a manifest recovery can never act
// on: one whose contents name a storage key no backend can address (#747),
// or one whose body does not decode at all (#915). ListManifests selects on
// a ".json" suffix, so a parked manifest leaves the recovery work set and
// stops excluding its inputs from compaction, while the record of which
// output and inputs were involved (or, for an undecodable body, at least the
// path naming the tier, database and job) survives for an operator to act on.
const ManifestQuarantineSuffix = ".quarantined"

// ManifestMaxAge is the maximum age for manifests before they're considered stale.
// Manifests older than this are deleted during recovery - they likely indicate
// a deeper problem that requires investigation.
const ManifestMaxAge = 7 * 24 * time.Hour // 7 days

// ErrManifestUnparseable marks a manifest whose bytes were read but do not
// decode. It is distinct from a read failure on purpose: a read failure is
// transient and the manifest must be retained, while an undecodable body
// (typically a zero-length file left by a crash before the rename was
// durable) will never decode on any later pass.
var ErrManifestUnparseable = errors.New("compaction manifest cannot be parsed")

// Manifest tracks the state of a compaction operation for crash recovery.
// If a pod crashes after uploading the compacted file but before deleting
// source files, the manifest allows recovery to complete the deletion.
type Manifest struct {
	// Output file information
	OutputPath string `json:"output_path"` // Full storage path of compacted file
	OutputSize int64  `json:"output_size"` // Expected size of output file (for validation)

	// Input files that were compacted
	InputFiles []string `json:"input_files"`

	// Metadata
	Database      string         `json:"database"`
	Measurement   string         `json:"measurement"`
	PartitionPath string         `json:"partition_path"`
	Tier          string         `json:"tier"`
	Status        ManifestStatus `json:"status"`
	CreatedAt     time.Time      `json:"created_at"`
	JobID         string         `json:"job_id"`
}

// ManifestManager handles reading, writing, and recovering from compaction manifests
type ManifestManager struct {
	backend storage.Backend
	logger  zerolog.Logger
	mu      sync.Mutex

	// Cache of manifest paths to input files for quick lookup during candidate filtering
	// Key: manifest path, Value: set of input file paths
	manifestCache     map[string]map[string]struct{}
	manifestCacheMu   sync.RWMutex
	manifestCacheTime time.Time
	cacheTTL          time.Duration

	// unparseableLogged records manifest paths already reported as
	// unparseable, so a lingering bad file logs once per site rather than
	// once per measurement per pass. PendingOutputsUnder keys by bare path,
	// GetFilesInManifests by "cache:"+path; their messages differ.
	unparseableLogged sync.Map
}

// NewManifestManager creates a new manifest manager
func NewManifestManager(backend storage.Backend, logger zerolog.Logger) *ManifestManager {
	return &ManifestManager{
		backend:       backend,
		logger:        logger.With().Str("component", "manifest-manager").Logger(),
		manifestCache: make(map[string]map[string]struct{}),
		cacheTTL:      30 * time.Second,
	}
}

// GenerateManifestPath generates a unique manifest path for a compaction job.
//
// Path format: _compaction_state/{tier}/{database}/{jobID}.json
//
// The partition path is NOT repeated in the filename. jobID already embeds the
// sanitized database and the folded partition path (manager.go), and the
// database is a path segment here besides, so the old
// "{folded_partition}_{jobID}.json" carried the partition twice and the
// database three times. That pushed ordinary names past the 255-byte segment
// limit: a 30-character database with a 60-character measurement produced a
// 270-byte filename, which the storage key contract refuses, so WriteManifest
// failed and compaction for that partition failed on every cycle (#744).
//
// Nothing parses this name for recovery decisions. ListManifests filters on
// the ".json" suffix and recovery drives every decision off the unmarshalled
// body, so the format is free to change and old manifests stay discoverable.
// The one exception is manifestPathDatabase, which reads the database segment
// back out of the path for a manifest whose body cannot be decoded.
//
// The hash fallback covers the remaining tail: at the maximum permitted
// database (64) and measurement (128) lengths even the jobID alone exceeds the
// segment limit. It is deterministic, so a retry of the same job addresses the
// same manifest.
func (m *ManifestManager) GenerateManifestPath(tier, database, partitionPath, jobID string) string {
	// An empty jobID would give ".json", which LocalBackend's hidden-file
	// filter drops from List, so the manifest would be written and then never
	// discovered or deleted. The old format always had a partition prefix in
	// front; this one does not, so the guard is explicit.
	if jobID == "" {
		jobID = "unidentified-job"
	}
	name := jobID + ".json"
	// MaxUsableKeySegmentLen, not MaxKeySegmentLen: ValidateKey subtracts
	// PartSuffix from the bound, so comparing against the raw limit left names
	// of 251 to 255 bytes passing this check and then being refused by every
	// write, which is the failure #744 set out to remove.
	if len(name) > storage.MaxUsableKeySegmentLen {
		sum := sha256.Sum256([]byte(jobID))
		name = hex.EncodeToString(sum[:16]) + ".json"
	}
	return filepath.Join(ManifestBasePath, tier, database, name)
}

// WriteManifest writes a manifest to storage
func (m *ManifestManager) WriteManifest(ctx context.Context, manifest *Manifest) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	manifestPath := m.GenerateManifestPath(manifest.Tier, manifest.Database, manifest.PartitionPath, manifest.JobID)

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal manifest: %w", err)
	}

	if err := m.backend.Write(ctx, manifestPath, data); err != nil {
		return "", fmt.Errorf("failed to write manifest to %s: %w", manifestPath, err)
	}

	m.logger.Debug().
		Str("path", manifestPath).
		Str("output", manifest.OutputPath).
		Int("input_count", len(manifest.InputFiles)).
		Msg("Wrote compaction manifest")

	// Invalidate cache
	m.invalidateCache()

	return manifestPath, nil
}

// DeleteManifest removes a manifest from storage
func (m *ManifestManager) DeleteManifest(ctx context.Context, manifestPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.backend.Delete(ctx, manifestPath); err != nil {
		return fmt.Errorf("failed to delete manifest %s: %w", manifestPath, err)
	}

	m.logger.Debug().Str("path", manifestPath).Msg("Deleted compaction manifest")

	// Invalidate cache
	m.invalidateCache()

	return nil
}

// ReadManifest reads a manifest from storage
func (m *ManifestManager) ReadManifest(ctx context.Context, manifestPath string) (*Manifest, error) {
	data, err := m.backend.Read(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest %s: %w", manifestPath, err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest %s: %w: %w", manifestPath, ErrManifestUnparseable, err)
	}

	return &manifest, nil
}

// ListManifests lists all manifest files in storage.
//
// Errors are returned rather than reported as an empty list. "No manifests
// exist" and "storage is unreachable" mean opposite things to the caller:
// filterCandidateFiles treats an empty manifest set as "nothing is being
// compacted, proceed" but has an explicit guard that skips the partition when
// the lookup fails. Collapsing the error into an empty slice made that guard
// unreachable, so a transient List failure (S3 throttling, expired
// credentials, a network blip) let Arc re-compact files another job already
// had in flight.
//
// A missing manifest directory is not an error at this layer: LocalBackend.List
// skips directories that do not exist, and the object-store backends return an
// empty result for a prefix with no objects. So any error arriving here is a
// real failure.
func (m *ManifestManager) ListManifests(ctx context.Context) ([]string, error) {
	objects, err := m.backend.List(ctx, ManifestBasePath+"/")
	if err != nil {
		return nil, fmt.Errorf("failed to list manifests: %w", err)
	}

	var manifests []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".json") {
			manifests = append(manifests, obj)
		}
	}

	return manifests, nil
}

// recoveryScope limits mutations, including recovery, to the selected
// database and measurement. A nil Databases slice means unrestricted; a
// non-nil empty slice selects nothing. Databases holds the names the cycle
// iterates, so for a spoke namespace it is the expanded pseudo-database
// ("spoke/child"), which is also what the job writes into the manifest.
//
// Recovery is deliberately NOT scoped by tier. Each scheduler runs a single
// tier, so a tier-scoped recovery would leave a daily orphan waiting for the
// daily tick (up to a day, or forever once that tier is disabled) while its
// output and inputs coexist in the partition. #915 asks manual targeting to
// isolate a database/measurement, not a tier, and orphans are few, so every
// cycle recovers across all tiers as it did before the scope existed.
type recoveryScope struct {
	Databases   []string
	Measurement string
}

func (scope recoveryScope) matches(manifest *Manifest) bool {
	return (scope.Databases == nil || slices.Contains(scope.Databases, manifest.Database)) &&
		(scope.Measurement == "" || scope.Measurement == manifest.Measurement)
}

// matchesPath is the database-only check for a manifest whose body cannot be
// decoded: the measurement lives in the body, the database in the path.
func (scope recoveryScope) matchesPath(manifestPath string) bool {
	return scope.Databases == nil || slices.Contains(scope.Databases, manifestPathDatabase(manifestPath))
}

// manifestPathDatabase returns the database segment(s) of a manifest path,
// "_compaction_state/{tier}/{database}/{name}.json", where database may itself
// contain a slash for a spoke pseudo-database. Empty when the path does not
// have that shape.
func manifestPathDatabase(manifestPath string) string {
	rest, ok := strings.CutPrefix(filepath.ToSlash(manifestPath), ManifestBasePath+"/")
	if !ok {
		return ""
	}
	_, rest, ok = strings.Cut(rest, "/") // drop the tier
	if !ok {
		return ""
	}
	i := strings.LastIndex(rest, "/")
	if i < 0 {
		return ""
	}
	return rest[:i]
}

func (m *ManifestManager) recoveryManifestPaths(ctx context.Context, scope recoveryScope) ([]string, error) {
	if scope.Databases != nil && len(scope.Databases) == 0 {
		return nil, nil
	}
	// Every manifest is listed and matched on its metadata. Orphans are
	// bounded by crashed jobs, so the read cost of the unselected ones is
	// small, and a single listing is what every other reader of this prefix
	// does.
	return m.ListManifests(ctx)
}

// RecoverOrphanedManifests finds and processes orphaned manifests from interrupted compactions.
// Returns the number of manifests recovered and any error encountered.
// onKeptOutput, when non-nil, receives the storage key of every compacted
// output recovery decides to KEEP (issue #610): recovery completes
// compactions outside CompactPartition, so without it the edge sync ledger
// would never learn about the output. Passed as an argument rather than
// stored on the struct — the caller (Manager) copies it under its own mutex,
// which keeps the scheduler-goroutine read free of the wiring race a stored
// field would have (main.go wires observers after the schedulers start).
// onConsumedInputs, when non-nil, receives manifest.InputFiles once their
// deletion has fully succeeded on the KEPT-output branch — never on the
// output-missing branch, whose inputs were not consumed (#619 review F5).
// Fired BEFORE the manifest is deleted; a returned ERROR keeps the manifest
// so the next recovery pass re-fires the marks (consumers are idempotent) —
// deleting it despite a failed mark would silently lose them (B1).
func (m *ManifestManager) RecoverOrphanedManifests(ctx context.Context, onKeptOutput func(string), onConsumedInputs func([]string) error) (int, error) {
	return m.recoverOrphanedManifests(ctx, recoveryScope{}, onKeptOutput, onConsumedInputs)
}

func (m *ManifestManager) recoverOrphanedManifests(ctx context.Context, scope recoveryScope, onKeptOutput func(string), onConsumedInputs func([]string) error) (recovered int, recoveryErr error) {
	defer func() {
		// A cancellation may arrive after some manifests were completed.
		if recovered > 0 {
			metrics.Get().IncCompactionManifestsRecovered(int64(recovered))
		}
	}()
	paths, err := m.recoveryManifestPaths(ctx, scope)
	if err != nil {
		return 0, err
	}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return recovered, errors.Join(recoveryErr, err)
		}
		manifest, err := m.ReadManifest(ctx, path)
		if errors.Is(err, ErrManifestUnparseable) {
			// The body will never decode on a later pass either, so retaining
			// it would hold this file in the work set forever, and because
			// GetFilesInManifests must fail closed on anything it cannot read,
			// every candidate on the node would be skipped until an operator
			// found it. Park it under the quarantine suffix instead: that
			// removes it from the ".json" work set like a delete would, but
			// keeps the path, which is the only pointer to the partition an
			// operator should inspect. Only the database can be honored for
			// scope here; the measurement lives in the undecodable body.
			if !scope.matchesPath(path) {
				continue
			}
			if parkErr := m.parkUnparseableManifest(ctx, path, err); parkErr != nil {
				if ctx.Err() != nil {
					return recovered, errors.Join(recoveryErr, parkErr, ctx.Err())
				}
				m.logger.Error().Err(parkErr).Str("manifest", path).Msg("Failed to park unparseable manifest; retrying next cycle")
				recoveryErr = errors.Join(recoveryErr, fmt.Errorf("park %s: %w", path, parkErr))
			}
			// Not counted in recovered: nothing was recovered, only removed
			// from the work set. (An invalid-output-key park below still
			// counts, as it did before this branch existed.)
			continue
		}
		if err == nil && !scope.matches(manifest) {
			continue
		}
		if err == nil {
			err = m.recoverLoadedManifest(ctx, path, manifest, onKeptOutput, onConsumedInputs)
		}
		if err != nil {
			if ctx.Err() != nil {
				return recovered, errors.Join(recoveryErr, err, ctx.Err())
			}
			m.logger.Error().Err(err).Str("manifest", path).Msg("Failed to recover manifest; retaining recovery state")
			recoveryErr = errors.Join(recoveryErr, fmt.Errorf("recover %s: %w", path, err))
			continue
		}
		recovered++
	}
	return recovered, recoveryErr
}

func (m *ManifestManager) recoverLoadedManifest(ctx context.Context, manifestPath string, manifest *Manifest, onKeptOutput func(string), onConsumedInputs func([]string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Check for stale manifests - older than ManifestMaxAge likely indicate a deeper problem
	manifestAge := time.Since(manifest.CreatedAt)
	isStale := manifestAge > ManifestMaxAge
	if isStale {
		m.logger.Warn().
			Str("manifest", manifestPath).
			Dur("age", manifestAge).
			Time("created_at", manifest.CreatedAt).
			Msg("Processing stale manifest (older than 7 days) - investigate root cause")
	}

	m.logger.Info().
		Str("manifest", manifestPath).
		Str("output", manifest.OutputPath).
		Int("inputs", len(manifest.InputFiles)).
		Msg("Processing orphaned manifest")

	// Check if output file exists
	exists, err := m.backend.Exists(ctx, manifest.OutputPath)
	if errors.Is(err, storage.ErrInvalidPath) {
		// The output key names nothing any backend can address, so Exists,
		// Delete and Read all fail the same way every cycle and this manifest
		// would be retried forever (#747). Park it rather than process it.
		//
		// Parked rather than deleted, and the difference is narrower than it
		// looks: the consumed-inputs marks do NOT survive either way, because
		// this returns before the deletion loop and a parked manifest is
		// invisible to every later pass. What parking buys is that the record
		// of which output and which inputs were involved still exists, and an
		// operator is the only party who can act on it.
		//
		// That matters here because the inputs may already be gone. An older
		// binary that folded the key is the only thing that could have written
		// this manifest, and that same binary's upload and source deletion both
		// SUCCEEDED. Job.Run leaves the manifest behind on purpose when the
		// parent finalizes it (job.go), so a live manifest whose inputs are
		// already deleted is a designed state, not a corruption.
		//
		// Nothing sweeps a parked manifest, which is deliberate rather than an
		// oversight: it is a small JSON file, each affected manifest parks
		// exactly once (a later pass cannot see it), so the count is bounded by
		// how many bad manifests an earlier version wrote and cannot grow from
		// a loop.
		return m.quarantineManifest(ctx, manifestPath, manifest.OutputPath, err)
	}
	if err != nil {
		return fmt.Errorf("failed to check output file existence: %w", err)
	}

	if !exists {
		// Output file doesn't exist - compaction was interrupted before upload completed
		// Delete manifest and let compaction retry
		m.logger.Info().
			Str("manifest", manifestPath).
			Str("output", manifest.OutputPath).
			Msg("Output file missing, deleting manifest for retry")
		return m.DeleteManifest(ctx, manifestPath)
	}

	// Output file exists - verify size if we have ObjectLister
	if objectLister, ok := m.backend.(storage.ObjectLister); ok {
		objects, err := objectLister.ListObjects(ctx, manifest.OutputPath)
		if err == nil && len(objects) > 0 {
			actualSize := objects[0].Size
			if actualSize != manifest.OutputSize {
				// Size mismatch - partial upload, delete output and manifest for retry
				m.logger.Warn().
					Str("manifest", manifestPath).
					Int64("expected_size", manifest.OutputSize).
					Int64("actual_size", actualSize).
					Msg("Output file size mismatch, deleting for retry")

				if err := m.backend.Delete(ctx, manifest.OutputPath); err != nil {
					m.logger.Warn().Err(err).Str("output", manifest.OutputPath).Msg("Failed to delete partial output")
				}
				return m.DeleteManifest(ctx, manifestPath)
			}
		}
	}

	// Output file exists and is valid — the output is being KEPT, so tell
	// the edge sync observer now, before input deletion: a partial deletion
	// failure retries this manifest next cycle, and the consumer is
	// idempotent, so firing early is safe while firing late risks a window
	// where discovery syncs the output.
	if onKeptOutput != nil {
		onKeptOutput(manifest.OutputPath)
	}

	// Output file exists and is valid - complete the deletion of input files
	m.logger.Info().
		Str("manifest", manifestPath).
		Int("inputs", len(manifest.InputFiles)).
		Msg("Output file valid, completing input file deletion")

	var deleteErrors int
	for _, inputFile := range manifest.InputFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.backend.Delete(ctx, inputFile); err != nil {
			if errors.Is(err, storage.ErrInvalidPath) {
				// Permanent: this input cannot be deleted by this key, now or
				// ever, so counting it as a delete error would keep the whole
				// manifest for a retry that can only fail again (#747). Drop it
				// from the work set and let the rest of the recovery finish.
				//
				// The file is NOT reclaimable by anything downstream: the
				// reconciler's storage sweep addresses files by the same kind of
				// key and fails identically. It needs operator action, and
				// urgently, because this is the one site where the undeleted
				// file's rows are CERTAINLY also in the output: InputFiles holds
				// the keys DuckDB actually read. Whether the query path serves
				// both copies depends on the backend, and the log line says so
				// rather than assuming local.
				metrics.Get().IncStorageInvalidPathQuarantined()
				m.logger.Error().Err(err).
					Str("file", inputFile).
					Str("manifest", manifestPath).
					Msg("Compaction input cannot be deleted: its key is permanently unusable. Skipping it so recovery can finish. Its rows are already in the compacted output, so wherever the query path can still reach this file it is now serving them twice: on Azure a backslash key IS the separator-spelled blob, and on local disk the file is a normal filename inside the partition glob. Remove it by hand")
				continue
			}
			// Check if file already deleted
			exists, checkErr := m.backend.Exists(ctx, inputFile)
			if checkErr == nil && !exists {
				// File already deleted, continue
				continue
			}
			m.logger.Warn().Err(err).Str("file", inputFile).Msg("Failed to delete input file during recovery")
			deleteErrors++
		}
	}

	if deleteErrors > 0 {
		m.logger.Warn().
			Int("errors", deleteErrors).
			Int("total", len(manifest.InputFiles)).
			Msg("Some input files could not be deleted during recovery, keeping manifest for retry")
		return fmt.Errorf("failed to delete %d of %d input files", deleteErrors, len(manifest.InputFiles))
	}

	// All input files deleted. Fire the consumed-inputs observer BEFORE
	// removing the manifest: if the marks (or this process) die here, the
	// surviving manifest re-runs this branch and re-fires them (#619). A
	// mark failure KEEPS the manifest for the same reason (B1).
	if onConsumedInputs != nil {
		if err := onConsumedInputs(manifest.InputFiles); err != nil {
			return fmt.Errorf("receipt marking incomplete; keeping manifest for re-fire: %w", err)
		}
	}

	// All input files deleted — safe to remove manifest
	return m.DeleteManifest(ctx, manifestPath)
}

// quarantinePathFor derives the parked name for a manifest, alongside it and
// out of the ".json" suffix ListManifests selects on.
//
// Appending can overflow: GenerateManifestPath allows a filename right up to
// the 255-byte segment limit (#744), and the suffix pushes those past it.
// Rather than give up on parking for exactly the manifests with the longest
// names, fall back to the same deterministic hash that function uses, so a
// record always survives and a repeated pass addresses the same parked object.
func quarantinePathFor(manifestPath string) (string, error) {
	parked := manifestPath + ManifestQuarantineSuffix
	if storage.ValidateKey(parked) == nil {
		return parked, nil
	}
	dir, name := filepath.Split(manifestPath)
	sum := sha256.Sum256([]byte(name))
	parked = filepath.Join(dir, hex.EncodeToString(sum[:16])+ManifestQuarantineSuffix)
	if err := storage.ValidateKey(parked); err != nil {
		return "", err
	}
	return parked, nil
}

// quarantineManifest parks a manifest whose contents name a permanently
// unusable storage key, so recovery stops retrying work that cannot succeed
// (#747) without discarding the record of what the manifest described. The
// copy-then-delete mechanics and their ordering live in parkManifestCopy.
//
// A failure here returns an error, which keeps the manifest for the next cycle.
// That is right for the transient case (the backend is down). The one way it
// could loop forever, the parked name being too long to be a valid key itself,
// is removed by quarantinePathFor falling back to a hashed name.
func (m *ManifestManager) quarantineManifest(ctx context.Context, manifestPath, badKey string, cause error) error {
	parkedPath, err := quarantinePathFor(manifestPath)
	if err != nil {
		// Unreachable in practice: quarantinePathFor falls back to a fixed-size
		// hashed name that cannot overflow. Kept because the only alternative
		// to handling it is a manifest retried forever, which is the bug being
		// fixed. Delete rather than loop, and let the log line be the record.
		if delErr := m.DeleteManifest(ctx, manifestPath); delErr != nil {
			return delErr
		}
		metrics.Get().IncStorageInvalidPathQuarantined()
		m.logger.Error().
			Err(cause).
			Str("manifest", manifestPath).
			Str("output", badKey).
			AnErr("park_error", err).
			Msg("Compaction manifest names an unusable output key and could not be parked under any name; deleted it to stop an endless retry. This line is the only surviving record of the paths involved")
		return nil
	}

	if err := m.parkManifestCopy(ctx, manifestPath, parkedPath); err != nil {
		return err
	}

	// Counted here, not on entry: every step above can fail transiently, and
	// each failure keeps the manifest for the next cycle. Counting earlier
	// would report a drop from the work set that did not happen, once per
	// cycle, for an entry still being retried.
	metrics.Get().IncStorageInvalidPathQuarantined()

	m.logger.Error().
		Err(cause).
		Str("manifest", manifestPath).
		Str("parked_to", parkedPath).
		Str("output", badKey).
		Msg("Compaction manifest names an output key no storage backend can address; parked instead of retried. Its inputs are no longer held back from compaction. The output may still exist and still be served by the query path even though storage cannot address it: on Azure a backslash key IS the separator-spelled blob, and on local disk it is a normal filename inside the partition glob. Check that partition for duplicate rows")

	return nil
}

// parkManifestCopy is the copy-then-delete that moves a manifest out of the
// ".json" work set while keeping its raw bytes under parkedPath. It is a copy
// rather than a rename because the Backend interface has no rename, and it
// copies the raw bytes rather than re-marshalling so a manifest written by a
// different version keeps fields this binary does not know about. Write
// first, delete second: a crash between the two leaves both, and the next
// pass re-parks idempotently, whereas the reverse order could lose the record.
func (m *ManifestManager) parkManifestCopy(ctx context.Context, manifestPath, parkedPath string) error {
	raw, err := m.backend.Read(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("quarantine manifest %s: read: %w", manifestPath, err)
	}
	if err := m.backend.Write(ctx, parkedPath, raw); err != nil {
		return fmt.Errorf("quarantine manifest %s: park to %s: %w", manifestPath, parkedPath, err)
	}
	if err := m.DeleteManifest(ctx, manifestPath); err != nil {
		return fmt.Errorf("quarantine manifest %s: remove original after parking: %w", manifestPath, err)
	}
	return nil
}

// parkUnparseableManifest parks a manifest whose body does not decode. The
// bytes are copied as-is (a zero-length file parks as a zero-length file) so
// whatever an operator can still learn from them survives, and the parked
// name keeps the tier, database and job ID that GenerateManifestPath encodes,
// which is the only remaining pointer to the partition to inspect.
//
// It is parked rather than deleted because the manifest's absence is not
// proof that nothing happened: LocalBackend never fsyncs, so after a power
// loss the manifest can be empty while the output rename and some input
// unlinks that followed it are already journaled. A decodable manifest would
// repair either state (delete a short output, finish the input deletes); an
// undecodable one cannot, and the log line plus the parked path are what an
// operator has to go on.
func (m *ManifestManager) parkUnparseableManifest(ctx context.Context, manifestPath string, cause error) error {
	parkedPath, err := quarantinePathFor(manifestPath)
	if err != nil {
		// Unreachable in practice (quarantinePathFor hashes overlong names).
		// The alternative to handling it is a file that blocks every cycle.
		if delErr := m.DeleteManifest(ctx, manifestPath); delErr != nil {
			return delErr
		}
		metrics.Get().IncCompactionManifestParkedUnparseable()
		m.logger.Error().
			Err(cause).
			Str("manifest", manifestPath).
			AnErr("park_error", err).
			Msg("Compaction manifest cannot be parsed and could not be parked under any name; deleted it so compaction can proceed. This line is the only surviving record of the path")
		return nil
	}
	if err := m.parkManifestCopy(ctx, manifestPath, parkedPath); err != nil {
		return err
	}
	// Counted here, after the copy and the delete both landed (#926): a
	// failed park keeps the manifest for the next cycle and must not report
	// a drop from the work set that did not happen.
	metrics.Get().IncCompactionManifestParkedUnparseable()
	m.logger.Error().
		Err(cause).
		Str("manifest", manifestPath).
		Str("parked_to", parkedPath).
		Msg("Compaction manifest cannot be parsed; parked so it no longer blocks compaction. Its inputs are no longer held back. If the crash it records happened after the output upload, that partition may hold a short output or both the output and its inputs: check it for a zero-length _compacted file and for duplicate rows")
	return nil
}

// GetFilesInManifests returns a set of all input files currently tracked by manifests.
// This is used to exclude files from compaction candidate scans.
func (m *ManifestManager) GetFilesInManifests(ctx context.Context) (map[string]struct{}, error) {
	m.manifestCacheMu.RLock()
	if time.Since(m.manifestCacheTime) < m.cacheTTL && len(m.manifestCache) > 0 {
		// Return cached result
		result := make(map[string]struct{})
		for _, files := range m.manifestCache {
			for f := range files {
				result[f] = struct{}{}
			}
		}
		m.manifestCacheMu.RUnlock()
		return result, nil
	}
	m.manifestCacheMu.RUnlock()

	// Rebuild cache
	m.manifestCacheMu.Lock()
	defer m.manifestCacheMu.Unlock()

	// Double-check after acquiring write lock
	if time.Since(m.manifestCacheTime) < m.cacheTTL && len(m.manifestCache) > 0 {
		result := make(map[string]struct{})
		for _, files := range m.manifestCache {
			for f := range files {
				result[f] = struct{}{}
			}
		}
		return result, nil
	}

	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return nil, err
	}

	newCache := make(map[string]map[string]struct{})
	result := make(map[string]struct{})

	for _, manifestPath := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		manifest, err := m.ReadManifest(ctx, manifestPath)
		if errors.Is(err, ErrManifestUnparseable) {
			// Names no input this binary can act on, and recovery parks it on
			// the next cycle whose database scope includes it (a scheduled
			// cycle is unscoped). Failing closed on it would skip every
			// candidate on the node until then. Logged once per path; the key
			// is prefixed so PendingOutputsUnder's own once-per-path message
			// is not suppressed by this one.
			if _, seen := m.unparseableLogged.LoadOrStore("cache:"+manifestPath, struct{}{}); !seen {
				m.logger.Warn().Err(err).Str("manifest", manifestPath).
					Msg("Compaction manifest cannot be parsed; candidate filtering ignores it until recovery parks it")
			}
			continue
		}
		if err != nil {
			// A transient read failure hides inputs of an in-flight job.
			// Keep the previous cache untouched and fail closed this cycle.
			return nil, fmt.Errorf("read manifest for cache %s: %w", manifestPath, err)
		}

		files := make(map[string]struct{})
		for _, f := range manifest.InputFiles {
			files[f] = struct{}{}
			result[f] = struct{}{}
		}
		// Also protect outputs on both a cache miss and a cache hit.
		files[manifest.OutputPath] = struct{}{}
		result[manifest.OutputPath] = struct{}{}
		newCache[manifestPath] = files
	}

	m.manifestCache = newCache
	m.manifestCacheTime = time.Now()

	return result, nil
}

// invalidateCache clears the manifest cache
func (m *ManifestManager) invalidateCache() {
	m.manifestCacheMu.Lock()
	defer m.manifestCacheMu.Unlock()
	m.manifestCache = make(map[string]map[string]struct{})
	m.manifestCacheTime = time.Time{}
}

// IsFileInManifest checks if a file is tracked by any manifest
func (m *ManifestManager) IsFileInManifest(ctx context.Context, filePath string) (bool, error) {
	files, err := m.GetFilesInManifests(ctx)
	if err != nil {
		return false, err
	}
	_, exists := files[filePath]
	return exists, nil
}

// PendingOutputsUnder returns the storage keys of compaction outputs that are
// not yet committed and lie under prefix (a "{database}/{measurement}/" storage
// prefix): every manifest under _compaction_state/ whose OutputPath starts with
// the prefix and at least one of whose InputFiles still exists.
//
// The Iceberg exporter (#638) uses it to keep a compacted file out of the
// exported table until the compaction has replaced its sources: the job
// uploads the output before it deletes the inputs, and a pass listing the
// partition in between would otherwise register both, doubling the rows for
// external readers. An output whose inputs are all gone is committed for the
// exporter's purposes even if the manifest lingers (deletion failed, or the
// hub has not marked its receipts yet), so it is not excluded — otherwise a
// bookkeeping failure would turn into an export outage.
//
// Fresh listing and reads every call, never manifestCache: manifests are
// written and deleted by the compaction subprocess's own ManifestManager, so
// the parent's cache can be 30 s stale, and staleness here is exactly the
// window this exists to close. A manifest listed but gone by the time it is
// read was deleted by a compaction that just committed; that is the normal
// case and is treated as absent. A storage read failure is returned, so the
// caller fails closed on a transient outage. A manifest that reads but does
// not parse (a zero-length file left by a crash before the rename was
// durable) is skipped and reported once per path: recovery parks such a
// manifest under ManifestQuarantineSuffix, and failing every measurement on
// every pass until then (or forever, when compaction is disabled and
// recovery never runs) would be an outage over a file that names nothing.
//
// Keys are compared in slash form: OutputPath is written with filepath.Join,
// the listing the exporter compares against is ToSlash'ed.
func (m *ManifestManager) PendingOutputsUnder(ctx context.Context, prefix string) (map[string]struct{}, error) {
	manifests, err := m.ListManifests(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for _, path := range manifests {
		data, err := m.backend.Read(ctx, path)
		if err != nil {
			exists, existsErr := m.backend.Exists(ctx, path)
			if existsErr == nil && !exists {
				continue // deleted between the listing and the read: committed
			}
			return nil, fmt.Errorf("read compaction manifest %s: %w", path, err)
		}
		var manifest Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			if _, seen := m.unparseableLogged.LoadOrStore(path, struct{}{}); !seen {
				m.logger.Error().Err(err).Str("manifest", path).
					Msg("Compaction manifest cannot be parsed; the Iceberg export ignores it (compaction's next recovery cycle parks it under the .quarantined suffix; park or delete it by hand if compaction is disabled)")
			}
			continue
		}
		output := filepath.ToSlash(manifest.OutputPath)
		if !strings.HasPrefix(output, prefix) {
			continue
		}
		pending := false
		for _, input := range manifest.InputFiles {
			exists, err := m.backend.Exists(ctx, filepath.ToSlash(input))
			if err != nil {
				return nil, fmt.Errorf("check compaction input %s: %w", input, err)
			}
			if exists {
				pending = true
				break
			}
		}
		if pending {
			out[output] = struct{}{}
		}
	}
	return out, nil
}
