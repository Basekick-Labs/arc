package backup

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Manager orchestrates backup and restore operations.
type Manager struct {
	dataStorage storage.Backend // primary data storage
	// backupStorage is the default target's backend. It kept its name through
	// #1085 stage B2b-1, which made the destination configurable and possibly
	// remote: the field means "where a backup goes" exactly as it did, and
	// renaming it would have touched 43 test references that construct a
	// Manager directly without changing one thing about what it holds. Every
	// destination operation reads it through m.destination(), which is where
	// stage B2b-2 grows the per-database map.
	backupStorage storage.Backend
	// targetName is the configured target this destination came from, or "" when
	// the destination is backup.local_path as it was before targets existed.
	// Recorded in the manifest and named in every error that reports a
	// destination failure, so an operator reading "backup failed" learns which
	// destination failed.
	targetName string
	// targetKeyPrefix is the object-key prefix the destination backend adds to
	// every key, with its trailing separator, or "" for a local destination.
	// Key-length reservation needs it because the stored object name on an
	// object store is prefix+key and ValidateObjectPrefix bounds the prefix's
	// characters but never its length. See destinationKeyHeadroom.
	targetKeyPrefix string
	// targetRemote records that the destination is an object store. It decides
	// the include_config default (arc.toml carries the target's own
	// credentials) and nothing else.
	targetRemote bool
	// instanceID is this Arc instance's backup owner identity: the cluster
	// name when clustered, a persistent per-instance UUID when standalone, ""
	// when the caller supplied none. Written into every manifest this instance
	// produces and compared against every manifest it reads, so two instances
	// sharing one bucket and prefix do not merge listings. See
	// internal/backup/identity.go.
	instanceID   string
	sqliteDBPath string // path to shared SQLite database
	configPath   string // path to arc.toml
	// icebergCatalogDBPath is the Iceberg SQL catalog, backed up separately only
	// when iceberg.catalog_db_path points somewhere other than the shared DB.
	// The catalog holds every Iceberg table's schema and snapshot pointers, so a
	// backup without it restores data whose tables no longer resolve.
	icebergCatalogDBPath string
	// icebergWarehouse is the resolved local directory of an Iceberg warehouse
	// that lies OUTSIDE the data storage root, or "" when there is none or it
	// is under the root (where the data listing already covers it). See
	// configureIcebergWarehouse.
	icebergWarehouse string
	// icebergWarehouseConfigured is the configured spelling (absolute, not
	// symlink-resolved) of the same directory — the one the catalog's metadata
	// locations are built from. icebergEnabled records that Iceberg export is
	// on at all, so a restore can tell "no warehouse to write to" apart from
	// "Iceberg is off here and the catalog rows are inert".
	icebergWarehouseConfigured string
	icebergEnabled             bool
	icebergNSPrefix            string
	// icebergWarehouseKeyPrefix is the storage key prefix of an under-root
	// warehouse: "" when the warehouse IS the storage root (the default) and
	// "<sub>/" when it is a subdirectory of it (#534 layout). Only a scoped
	// backup reads it, to find the namespace directories it leaves out; the
	// two layouts are the same string only at the default, which is exactly
	// the #534 shape, so the prefix is stored rather than assumed.
	icebergWarehouseKeyPrefix string

	// cluster is the Raft file manifest on a cluster node, nil on a
	// standalone one; tierRecorder is this node's tier metadata, nil without
	// tiering. Independently wired (#1083): tiering runs standalone too, and
	// a cluster node may have tiering off. See cluster.go. tierLookup is the
	// known-database check's view of the same tier metadata (#1084), nil
	// without tiering; see scope.go.
	cluster      ClusterManifest
	tierRecorder TierRecorder
	tierLookup   TierLookup

	logger zerolog.Logger
	mu     sync.Mutex // serializes backup/restore operations
	active atomic.Pointer[Progress]
}

// ManagerConfig holds configuration for creating a backup manager.
type ManagerConfig struct {
	DataStorage storage.Backend
	// BackupPath is the local directory a backup is written to when Target is
	// nil. Required then, and IGNORED when Target is set — a deployment whose
	// backups go to an object store must not have ./data/backups created for
	// it, which building a LocalBackend would do (local.go's constructor
	// MkdirAlls its root).
	BackupPath string
	// Target is the configured destination (#1085 stage B2b-1), or nil for the
	// BackupPath destination that predates targets. Exactly one in this
	// release; stage B2b-2 is where it becomes a set.
	Target *Target
	// InstanceID is this instance's backup owner identity. Empty is allowed
	// and means "unidentified": manifests are written without an owner and
	// every manifest read is treated as this instance's own, which is how
	// every backup taken before #1085 stage B2b-1 reads. See identity.go.
	InstanceID   string
	SQLiteDBPath string
	// IcebergCatalogDBPath is the Iceberg SQL catalog path. Leave empty, or set
	// equal to SQLiteDBPath, when the catalog lives in the shared database —
	// it is then already covered by the shared-database backup.
	IcebergCatalogDBPath string
	// IcebergWarehousePath is the local directory of the Iceberg warehouse
	// (iceberg.warehouse with its file:// scheme stripped) when Iceberg export
	// is enabled; empty otherwise. The manager works out whether it needs its
	// own copy pass (#637).
	IcebergWarehousePath string
	// IcebergNamespacePrefix is iceberg.namespace_prefix (default "arc"); the
	// warehouse walk copies only <prefix>_*.db namespace directories.
	IcebergNamespacePrefix string
	ConfigPath             string
	Logger                 zerolog.Logger
}

// NewManager creates a new backup manager.
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	if cfg.DataStorage == nil {
		return nil, fmt.Errorf("data storage backend is required")
	}

	// One destination, built through the shared factory either way, which
	// keeps the typed-nil guarantee (#713) and the backend dispatch in one
	// place (internal/storage/factory.go).
	//
	// A configured target wins and BackupPath is not consulted at all. That is
	// the headline of #1085 stage B2b-1: a local path is no longer required,
	// and nothing in the backup or restore path uses it as scratch — the
	// SQLite snapshot temp dir sits beside the database, restore staging
	// beside the restore destination, the warehouse temp beside its
	// destination, and readto.go uses the system temp dir.
	spec := storage.BackendSpec{Type: "local", LocalPath: cfg.BackupPath}
	targetName, targetKeyPrefix, targetRemote := "", "", false
	if cfg.Target != nil {
		spec = cfg.Target.Spec
		targetName = cfg.Target.Name
		targetKeyPrefix = cfg.Target.KeyPrefix
		targetRemote = cfg.Target.Remote
	} else if cfg.BackupPath == "" {
		return nil, fmt.Errorf("backup path is required when no backup target is configured")
	}
	// Before the backend is built: this is a pure arithmetic check on the
	// configuration, it needs no I/O, and a prefix that cannot hold the
	// backup's own commit records must be reported as itself rather than
	// behind whatever the network did next. It joins the existing
	// "degrade, do not kill" path — cmd/arc/main.go logs a NewManager error at
	// Error and skips the backup API — which is the right severity because an
	// over-long prefix is as loud and as permanent as an unwritable local
	// backup directory, not a transient like an unreachable bucket.
	if targetKeyPrefix != "" {
		if err := checkTargetKeyPrefix(targetName, targetKeyPrefix); err != nil {
			return nil, err
		}
	}

	backupBackend, err := storage.NewBackend(spec, cfg.Logger)
	if err != nil {
		if targetName != "" {
			return nil, fmt.Errorf("failed to create backup storage for target %s: %w", targetName, err)
		}
		return nil, fmt.Errorf("failed to create backup storage: %w", err)
	}

	// Only treat the Iceberg catalog as a separate database when it really is a
	// different file. Both paths default to the same value, and a relative vs
	// absolute spelling of one file must not produce a redundant second copy.
	icebergCatalog := cfg.IcebergCatalogDBPath
	if icebergCatalog != "" && sameFilePath(icebergCatalog, cfg.SQLiteDBPath) {
		icebergCatalog = ""
	}

	m := &Manager{
		dataStorage:          cfg.DataStorage,
		backupStorage:        backupBackend,
		targetName:           targetName,
		targetKeyPrefix:      targetKeyPrefix,
		targetRemote:         targetRemote,
		instanceID:           cfg.InstanceID,
		sqliteDBPath:         cfg.SQLiteDBPath,
		icebergCatalogDBPath: icebergCatalog,
		configPath:           cfg.ConfigPath,
		logger:               cfg.Logger.With().Str("component", "backup-manager").Logger(),
	}
	m.configureIcebergWarehouse(cfg)
	return m, nil
}

// sameFilePath reports whether two configured paths refer to the same file,
// comparing cleaned absolute paths with symlinks resolved when they exist.
func sameFilePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	resolve := func(p string) string {
		abs, err := filepath.Abs(p)
		if err != nil {
			return filepath.Clean(p)
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			return resolved
		}
		return abs
	}
	return resolve(a) == resolve(b)
}

// Target is one configured backup destination (#1085 stage B2b-1).
//
// The backup package takes a prepared Target rather than reading config
// itself, because it cannot import internal/config (config imports
// internal/storage, and the overlap refusal that protects
// cleanupPartialBackupWrite runs inside config.Load). cmd/arc/main.go
// translates one into the other.
type Target struct {
	// Name is the target as the operator spells it. Recorded in the manifest
	// and named in destination errors.
	Name string
	// Spec is handed straight to storage.NewBackend.
	Spec storage.BackendSpec
	// KeyPrefix is the object-key prefix the backend adds to every key, with
	// its trailing separator, or "" for a local target. Supplied rather than
	// re-derived so the manager reserves headroom for the same prefix the
	// backend will actually apply.
	KeyPrefix string
	// Remote records that this is an object store. It decides the
	// include_config default and nothing else.
	Remote bool
}

// TargetName is the configured destination's name, or "" when the destination
// is backup.local_path as it was before targets existed.
func (m *Manager) TargetName() string { return m.targetName }

// TargetIsRemote reports whether backups go to an object store. The API layer
// reads it for the include_config default: arc.toml carries the target's own
// credentials, so copying it into that target is a credential leak into the
// store the credentials unlock.
func (m *Manager) TargetIsRemote() bool { return m.targetRemote }

// destination returns the backend a destination operation must use.
//
// Every read and write of backup storage goes through this rather than
// touching m.backupStorage, and that is the whole point of its existing with
// one target: stage B2b-2 routes per database, and the set of call sites it
// has to change is exactly the set that already calls this. With one target it
// is the identity function and the single-destination path is byte-identical
// to what it replaced.
func (m *Manager) destination() storage.Backend { return m.backupStorage }

// destinationProbeTimeout bounds the destination operations whose payload is
// small and fixed: the reachability probe, the manifest and config writes, and
// every metadata read the manager makes (listings, manifest reads, existence
// checks).
//
// It is NOT applied to a bulk data copy. A multi-gigabyte Parquet file over a
// slow link legitimately takes longer than any constant that would also be a
// useful stall detector, and the right instrument for that is a stall
// detector, not a deadline. Bulk copies stay bounded by the run's own
// operation_timeout, and a stalled one is caught earlier by the probe below —
// see Manager.probeDestination.
const destinationProbeTimeout = 60 * time.Second

// withDestinationTimeout bounds one small destination operation.
//
// Derived from the caller's context, so a SHORTER deadline already on it (the
// API handler's 30 s) still wins; this only caps the case where the caller's
// context is the two-hour operation timeout.
func withDestinationTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, destinationProbeTimeout)
}

// probeDestination checks the destination answers at all, before a run starts
// copying into it.
//
// This is what makes "an unreachable destination fails naming the target
// rather than hanging" true on the WRITE path. A backup's first destination
// touch is a bulk file copy under the run's operation_timeout (two hours by
// default) while the single-operation lock is held, and the S3 client sets no
// response timeout on purpose (internal/storage/s3.go), so a black-holed
// endpoint — as opposed to one that refuses the connection, which fails in
// seconds — would hold that lock for the full two hours and then report an
// error that did not say which destination failed. One HEAD against a key the
// run owns, under destinationProbeTimeout, turns that into a one-minute
// failure naming the target.
//
// The key is "<backupID>/manifest.json" for the run's own freshly minted ID,
// so it is absent by construction and the probe neither reads nor writes
// anything of consequence. A backend that answers "no" has answered, which is
// all the probe asks.
func (m *Manager) probeDestination(ctx context.Context, backupID string) error {
	probeCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	if _, err := m.destination().Exists(probeCtx, backupID+"/manifest.json"); err != nil {
		return fmt.Errorf("%s did not answer: %w", m.describeDestination(), err)
	}
	return nil
}

// describeDestination names the destination for an error message.
//
// An unreachable or misconfigured destination is reported with its target
// NAME, not just the underlying SDK error. Once a destination can be remote,
// "failed to list backup storage: dial tcp ... connection refused" leaves the
// operator to guess which of their configured stores is down, and with
// installErrSanitizer masking quoted spans in logged errors the bucket name
// inside the wrapped error is not reliably readable either.
func (m *Manager) describeDestination() string {
	if m.targetName == "" {
		return "the backup destination"
	}
	return "backup target " + m.targetName
}

// backupDataKeyHeadroom is the 31-byte generated backup ID plus "/data/": the
// bytes a data file's backup destination adds to its source key. A source key
// may fit storage.MaxUsableKeyLen while its destination does not; the copy
// checks the real destination length and reports this reservation so the
// operator-facing threshold is explicit. Pinned to generateBackupID's output
// by a test.
//
// It is NOT the whole reservation once the destination can be remote, which is
// why nothing outside destinationKeyHeadroom uses it directly: on an object
// store the stored object name is the target prefix plus this key, and
// storage.ValidateObjectPrefix bounds a prefix's characters and segments but
// never its length. Reporting this constant on a prefixed target promised 982
// usable source bytes and the true figure was 982 minus the prefix, in the
// direction that produces a failed write rather than a reported skip.
const backupDataKeyHeadroom = len("backup-20060102-150405-12345678/data/")

// MaxTargetKeyPrefixLen is storage.MaxBackupTargetPrefixLen.
//
// The arithmetic moved next to the limit it subtracts from, because the
// refusal it drives belongs at CONFIG LOAD and config cannot import this
// package. Keeping the name here is for callers that already have a Manager;
// the one check that matters runs in config.validateBackupTargets.
const MaxTargetKeyPrefixLen = storage.MaxBackupTargetPrefixLen

// checkTargetKeyPrefix is the belt for a Manager built directly, which every
// test in this package does and which therefore never passes through
// config.Load. The refusal an operator actually sees is the load-time one.
func checkTargetKeyPrefix(name, prefix string) error {
	if err := storage.CheckBackupTargetPrefix("the object key prefix of backup target "+name, prefix); err != nil {
		return fmt.Errorf("%w. Shorten the prefix", err)
	}
	return nil
}

// destinationKeyHeadroom is how many bytes this destination adds to a source
// key: the target's object-key prefix plus the per-run backup prefix.
func (m *Manager) destinationKeyHeadroom() int {
	return len(m.targetKeyPrefix) + backupDataKeyHeadroom
}

// maxSourceKeyBytes is the longest source key this destination can hold, the
// figure both overlong-key messages report as max_source_key_bytes.
func (m *Manager) maxSourceKeyBytes() int {
	return storage.MaxUsableKeyLen - m.destinationKeyHeadroom()
}

// destinationKeyTooLong reports whether a backup destination key, once the
// target prefix is applied, exceeds what the store can hold.
//
// Bounded by storage.MaxUsableKeyLen on every backend. That figure subtracts
// LocalBackend's ".part" staging suffix from a 1024-byte limit, which is also
// S3's object-key limit and under Azure's, so on an object store it is
// conservative by five bytes and never wrong in the permissive direction.
func (m *Manager) destinationKeyTooLong(destPath string) bool {
	return len(m.targetKeyPrefix)+len(destPath) > storage.MaxUsableKeyLen
}

// generateBackupID creates a unique backup identifier.
func generateBackupID() string {
	now := time.Now().UTC()
	short := uuid.New().String()[:8]
	return fmt.Sprintf("backup-%s-%s", now.Format("20060102-150405"), short)
}

// backupIDShape matches exactly what generateBackupID produces.
//
// The one definition of the shape, for the API's request validation and for
// the listing's directory filter alike. Those two had better agree: the
// listing uses it to decide which top-level names of a possibly-shared
// destination are Arc backups at all, and a shape the listing accepts but the
// API refuses would be a backup an operator can see and never restore.
var backupIDShape = regexp.MustCompile(`^backup-\d{8}-\d{6}-[a-f0-9]{8}$`)

// IsValidBackupID reports whether id is spelled the way generateBackupID
// spells one.
func IsValidBackupID(id string) bool { return backupIDShape.MatchString(id) }

// GetProgress returns the current active operation progress, or nil if idle.
// The returned value is an immutable snapshot — the operation goroutine never
// writes to a published Progress (see setProgress) — so callers may read and
// marshal it freely, but must not mutate it. It can lag the live operation by
// up to one file's worth of work.
func (m *Manager) GetProgress() *Progress {
	return m.active.Load()
}

// setProgress publishes an immutable snapshot of p. The operation goroutine
// keeps mutating its own private Progress and republishes after each update;
// readers (GetProgress, the /status handler, the API admission check) only
// ever see copies, so their unsynchronized field reads cannot race with the
// writer. Publishing the live pointer instead is a data race: every field
// write after the initial publish would race the readers.
func (m *Manager) setProgress(p *Progress) {
	snapshot := *p
	m.active.Store(&snapshot)
}

// ListBackups returns the backups in the destination that belong to this
// instance.
//
// "Belong to" means the manifest names this instance as owner, or names no
// owner at all (every backup taken before #1085 stage B2b-1, and every backup
// of an unidentified instance). A backup another instance wrote to the same
// bucket and prefix is left out, because merged listings are the hazard the
// owner field exists for. ListAllBackups returns those too, which is what
// makes disaster recovery onto fresh hardware possible.
func (m *Manager) ListBackups(ctx context.Context) ([]BackupSummary, error) {
	summaries, _, err := m.listBackups(ctx, false)
	return summaries, err
}

// ListBackupsFilteringForeign is ListBackups plus how many backups it left
// out because another instance wrote them.
//
// The count exists because the filter was otherwise INVISIBLE: on fresh
// hardware every backup at the destination reads as foreign, so recovery — the
// case backups exist for — was answered with an empty array and no sign that
// anything had been withheld, and the flag that makes it reachable was
// documented only in arc.toml. An empty listing over a populated destination
// now says so and says what to do next.
func (m *Manager) ListBackupsFilteringForeign(ctx context.Context) ([]BackupSummary, int, error) {
	return m.listBackups(ctx, false)
}

// ListAllBackups returns every backup in the destination, this instance's and
// any other instance's, each summary carrying its owner id and ForeignOwner.
//
// The opt-in exists so the owner filter cannot lock an operator out of their
// own data: after a restore onto fresh hardware the new instance has a new
// identity, so every backup it restored from reads as foreign, and an
// operator who could not list them could not find the id of the next one to
// restore. Foreign-owner restore is allowed and echoed, never refused, and
// this is the listing that makes that reachable.
func (m *Manager) ListAllBackups(ctx context.Context) ([]BackupSummary, error) {
	summaries, _, err := m.listBackups(ctx, true)
	return summaries, err
}

func (m *Manager) listBackups(ctx context.Context, includeForeign bool) ([]BackupSummary, int, error) {
	ids, err := m.listBackupIDs(ctx)
	if err != nil {
		return nil, 0, err
	}

	var filteredForeign int
	var summaries []BackupSummary
	for _, id := range ids {
		manifestPath := id + "/manifest.json"
		// A manifest is the commit record: the sidecar is written before it
		// precisely so a run that died between the two leaves nothing a
		// listing shows (see CreateBackup). A directory without one is such a
		// run, or a backup still in flight, and is skipped in silence rather
		// than reported as a failure — polling the listing during a backup
		// must not log a warning per poll.
		data, err := m.readManifest(ctx, manifestPath)
		switch {
		case errors.Is(err, ErrBackupNotFound):
			continue
		case err != nil:
			// Per-object, and that is why it warns and carries on rather than
			// failing the listing. The object is there and could not be
			// fetched: corruption, or a permission on that one key. A
			// transport-wide failure never reaches here — listBackupIDs ran
			// first and aborts naming the target, which is what makes this
			// branch safe to swallow and is the asymmetry two readers have now
			// had to derive.
			m.logger.Warn().Str("path", manifestPath).Err(err).
				Str("target", m.targetName).Msg("Failed to read manifest, skipping")
			continue
		}
		manifest, err := UnmarshalManifest(data)
		if err != nil {
			m.logger.Warn().Str("path", manifestPath).Err(err).
				Str("target", m.targetName).Msg("Failed to parse manifest, skipping")
			continue
		}

		own := m.ownsManifest(manifest)
		if !own && !includeForeign {
			filteredForeign++
			continue
		}
		summary := SummaryFromManifest(manifest)
		summary.ForeignOwner = !own
		summaries = append(summaries, summary)
	}

	if filteredForeign > 0 {
		m.logger.Info().
			Int("filtered_foreign", filteredForeign).
			Str("this_instance_id", m.instanceID).
			Msg("Backups at this destination belong to another Arc instance and were left out of the listing; ask for them with include_foreign=true")
	}
	return summaries, filteredForeign, nil
}

// readManifest reads one backup manifest, in ONE round trip.
//
// It returns ErrBackupNotFound when the object is absent, which is what lets
// the caller tell an uncommitted backup from a destination that will not
// answer without asking twice. It used to ask twice — Exists then Read — and
// that doubled the request count of a loop that runs once per backup, which on
// a remote destination holding a few hundred backups is what exhausts an API
// handler's 30-second budget against a perfectly healthy store.
//
// Bounded by destinationProbeTimeout: a manifest is small and fixed, and this
// is reachable from the restore path, whose context is the two-hour operation
// timeout rather than the handler's 30 s.
func (m *Manager) readManifest(ctx context.Context, manifestPath string) ([]byte, error) {
	readCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	data, err := m.destination().Read(readCtx, manifestPath)
	if err == nil {
		return data, nil
	}
	if storage.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s", ErrBackupNotFound, manifestPath)
	}
	return nil, fmt.Errorf("failed to read %s from %s: %w", manifestPath, m.describeDestination(), err)
}

// listBackupIDs returns the names in the destination that are shaped like a
// backup ID.
//
// It lists DIRECTORIES at the destination root rather than walking every
// object, and that is not only a cost question. The previous shape listed the
// whole destination and kept every key ending in "/manifest.json", which on a
// local directory the backup owned was cheap and unambiguous. On a remote
// target it is a full recursive walk of the configured prefix — a prefix that
// may legitimately sit in the same bucket as the cold tier — and the suffix
// match is a false-positive magnet for any foreign manifest.json under it.
//
// Note the two backends mean different things by List's prefix argument:
// S3Backend passes it to ListObjectsV2 as a key prefix, while LocalBackend
// resolves it to a DIRECTORY and walks that, so List(ctx, "backup-") returns
// every backup on S3 and nothing at all on local. ListDirectories is the one
// call whose meaning is the same on all three backends.
//
// DirectoryLister is optional, so a backend that does not implement it falls
// back to the old full listing, filtered by the same ID shape. The real
// backends all implement it; test fakes that embed storage.Backend do not.
func (m *Manager) listBackupIDs(ctx context.Context) ([]string, error) {
	listCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	if dl, ok := m.destination().(storage.DirectoryLister); ok {
		dirs, err := dl.ListDirectories(listCtx, "")
		if err != nil {
			return nil, fmt.Errorf("failed to list %s: %w", m.describeDestination(), err)
		}
		ids := make([]string, 0, len(dirs))
		for _, d := range dirs {
			if IsValidBackupID(d) {
				ids = append(ids, d)
			}
		}
		return ids, nil
	}

	files, err := m.destination().List(listCtx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list %s: %w", m.describeDestination(), err)
	}
	seen := make(map[string]bool)
	var ids []string
	for _, f := range files {
		id, _, found := strings.Cut(f, "/")
		if !found || seen[id] || !IsValidBackupID(id) {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, nil
}

// ErrBackupNotFound reports that no backup with the requested id exists in the
// destination. Separated from a destination FAILURE because the two want
// different answers: an unknown id is the caller's mistake and permanent, and
// a destination that cannot be read is neither. With a local directory the
// second was almost impossible and conflating them cost nothing; with a remote
// target it is an ordinary transient, and answering "backup not found" to it
// tells an operator their backup is gone when it is not.
var ErrBackupNotFound = errors.New("backup not found")

// GetBackup reads and returns the manifest for a specific backup.
//
// Never filtered by owner: a backup another instance wrote is readable and
// restorable, with its owner echoed. Refusing it would be self-locking —
// restoring onto fresh hardware is what backups are for.
func (m *Manager) GetBackup(ctx context.Context, backupID string) (*Manifest, error) {
	data, err := m.readManifest(ctx, fmt.Sprintf("%s/manifest.json", backupID))
	if errors.Is(err, ErrBackupNotFound) {
		// Re-stated against the backup ID rather than the key, because that is
		// what the caller asked for and what the API echoes.
		return nil, fmt.Errorf("%w: %s", ErrBackupNotFound, backupID)
	}
	if err != nil {
		return nil, err
	}
	return UnmarshalManifest(data)
}

// ErrOperationInProgress is returned by DeleteBackup when a backup or restore
// operation holds the manager. The API layer maps it to 409 Conflict.
var ErrOperationInProgress = errors.New("a backup or restore operation is in progress")

// DeleteBackup removes all files for a backup from the default backup storage.
//
// It refuses to run concurrently with a backup or restore (#626): deleting the
// backup a restore is reading tears files out from under it mid-operation, and
// deleting the backup being written leaves a half-written/half-deleted
// directory. TryLock rather than Lock — an operation can hold m.mu for hours,
// and queuing a synchronous HTTP-driven delete behind it would pin the request
// goroutine long past its context deadline. Callers get ErrOperationInProgress
// and retry when the operation finishes.
func (m *Manager) DeleteBackup(ctx context.Context, backupID string) error {
	if !m.mu.TryLock() {
		return ErrOperationInProgress
	}
	defer m.mu.Unlock()

	// Whose backup is this? Read before anything is removed, and only to log
	// it (#1085 stage B2b-1). A restore warns when the manifest belongs to
	// another instance; a delete used to read no manifest at all, so the
	// DESTRUCTIVE operation was the one with no echo — and the opt-in listing
	// that shows foreign backups is exactly what hands an operator the id they
	// then pass to this call. Best-effort: a manifest that cannot be read must
	// not stop an operator from deleting a backup, which is often why they are
	// deleting it.
	if data, err := m.readManifest(ctx, backupID+"/manifest.json"); err == nil {
		if manifest, err := UnmarshalManifest(data); err == nil && !m.ownsManifest(manifest) {
			m.logger.Warn().
				Str("backup_id", backupID).
				Str("owner_instance_id", manifest.OwnerInstanceID).
				Str("this_instance_id", m.instanceID).
				Msg("Deleting a backup written by a different Arc instance")
		}
	}

	// List all files under this backup ID. Bounded: a delete is reachable from
	// the API handler's 30 s context, but the listing itself is one request per
	// page and a stalled destination would otherwise hold the single-operation
	// slot for the whole handler budget.
	prefix := backupID + "/"
	listCtx, cancel := withDestinationTimeout(ctx)
	files, err := m.destination().List(listCtx, prefix)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to list the files of backup %s in %s: %w", backupID, m.describeDestination(), err)
	}
	if len(files) == 0 {
		return fmt.Errorf("%w: %s", ErrBackupNotFound, backupID)
	}

	// Use batch delete if available
	if bd, ok := m.destination().(storage.BatchDeleter); ok {
		if err := bd.DeleteBatch(ctx, files); err != nil {
			return fmt.Errorf("failed to delete backup %s from %s: %w", backupID, m.describeDestination(), err)
		}
	} else {
		for _, f := range files {
			if err := m.destination().Delete(ctx, f); err != nil {
				m.logger.Warn().Str("path", f).Err(err).Msg("Failed to delete backup file")
			}
		}
	}

	m.logger.Info().Str("backup_id", backupID).Int("files_deleted", len(files)).Msg("Backup deleted")
	return nil
}
