package backup

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
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
	// backupStorage is the DEFAULT target's backend. It kept its name through
	// #1085 stage B2b-1, which made the destination configurable and possibly
	// remote: the field means "where a backup goes" exactly as it did, and
	// renaming it would have touched 43 test references that construct a
	// Manager directly without changing one thing about what it holds.
	//
	// It and the three fields below are the ONE description of the default
	// destination, read through m.defaultDestination() on every use and never
	// cached (#1085 stage B2b-2). Twelve tests swap this field to stand in for
	// a failing destination, so a cached copy would route past their fakes.
	backupStorage storage.Backend
	// targetName is the configured DEFAULT target this destination came from,
	// or "" when the destination is backup.local_path as it was before targets
	// existed. Recorded in the manifest and named in every error that reports
	// a destination failure, so an operator reading "backup failed" learns
	// which destination failed.
	targetName string
	// targetKeyPrefix is the object-key prefix the destination backend adds to
	// every key, with its trailing separator, or "" for a local destination.
	// Key-length reservation needs it because the stored object name on an
	// object store is prefix+key and ValidateObjectPrefix bounds the prefix's
	// characters but never its length. See destinationKeyHeadroom.
	targetKeyPrefix string
	// targetRemote records that the DEFAULT destination is an object store. It
	// decides the include_config default (arc.toml carries the target's own
	// credentials) and nothing else; TargetIsRemote asks it of every target.
	targetRemote bool
	// targets holds the NON-default configured targets, keyed by name, and is
	// NIL for both single-destination shapes (#1085 stage B2b-2): no target
	// configured, and exactly one target, where the default must be it and
	// routing is a no-op. The default destination is never in here — it is the
	// flat fields above — so there is exactly one place a destination is
	// described and the backend-swapping tests keep working. See
	// Manager.defaultDestination.
	targets map[string]backupTarget
	// routing is database name → target name for every ROUTED database, nil
	// when nothing routes. A database the map does not name goes to the
	// default destination. The key is the storage-root segment a path routes
	// by (routeKey), which is the same rule a scoped backup uses.
	routing map[string]string
	// operationTimeout is backup.operation_timeout, the bound the API handler
	// puts on one run. The LISTING reads it: a run whose index landed within
	// that window and whose manifests have not is "possibly in flight" rather
	// than aborted, which is the only thing that stops a cluster READER —
	// which has no m.active for the primary's live run and no primary-writer
	// gate on the listing — from reporting a healthy backup as aborted.
	operationTimeout time.Duration
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
	// BackupPath is the local directory a backup is written to when no target
	// is configured. Required then, and IGNORED once Targets is non-empty — a
	// deployment whose backups go to an object store must not have
	// ./data/backups created for it, which building a LocalBackend would do
	// (local.go's constructor MkdirAlls its root).
	BackupPath string
	// Targets are the configured destinations (#1085 stage B2b-1, a set since
	// B2b-2), or empty for the BackupPath destination that predates targets.
	// DefaultTarget must name one of them and is where everything unrouted
	// goes; Routing is database name → target name for the rest.
	//
	// Order is irrelevant: NewManager keys them by name and refuses a
	// duplicate, a DefaultTarget that names none of them, and a Routing entry
	// naming a target that is not here.
	Targets       []Target
	DefaultTarget string
	Routing       map[string]string
	// OperationTimeout is backup.operation_timeout. Zero means
	// DefaultOperationTimeout. The listing reads it to tell a run that may
	// still be in flight from one that aborted; see Manager.operationTimeout.
	OperationTimeout time.Duration
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

	// Every destination is built through the shared factory, which keeps the
	// typed-nil guarantee (#713) and the backend dispatch in one place
	// (internal/storage/factory.go).
	//
	// A configured target wins and BackupPath is not consulted at all. That is
	// the headline of #1085 stage B2b-1: a local path is no longer required,
	// and nothing in the backup or restore path uses it as scratch — the
	// SQLite snapshot temp dir sits beside the database, restore staging
	// beside the restore destination, the warehouse temp beside its
	// destination, and readto.go uses the system temp dir.
	spec, def, others, err := buildTargets(cfg)
	if err != nil {
		return nil, err
	}
	// Before any backend is built: this is a pure arithmetic check on the
	// configuration, it needs no I/O, and a prefix that cannot hold the
	// backup's own commit records must be reported as itself rather than
	// behind whatever the network did next. It joins the existing
	// "degrade, do not kill" path — cmd/arc/main.go logs a NewManager error at
	// Error and skips the backup API — which is the right severity because an
	// over-long prefix is as loud and as permanent as an unwritable local
	// backup directory, not a transient like an unreachable bucket.
	//
	// Every target, not only the default one: a routed target whose prefix
	// cannot hold a backup's keys fails the same way, one leg in.
	for _, t := range append([]Target{def}, others...) {
		if t.KeyPrefix == "" {
			continue
		}
		if err := checkTargetKeyPrefix(t.Name, t.KeyPrefix); err != nil {
			return nil, err
		}
	}

	backupBackend, err := storage.NewBackend(spec, cfg.Logger)
	if err != nil {
		if def.Name != "" {
			return nil, fmt.Errorf("failed to create backup storage for target %s: %w", def.Name, err)
		}
		return nil, fmt.Errorf("failed to create backup storage: %w", err)
	}
	// The non-default targets. Built here rather than lazily so an
	// unconstructable one is reported at startup, where cmd/arc/main.go logs
	// it and skips the backup API, rather than at the first run that routes to
	// it — which may be weeks later and is the run an operator was relying on.
	var targets map[string]backupTarget
	for _, t := range others {
		backend, err := storage.NewBackend(t.Spec, cfg.Logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create backup storage for target %s: %w", t.Name, err)
		}
		if targets == nil {
			targets = make(map[string]backupTarget, len(others))
		}
		targets[t.Name] = backupTarget{backend: backend, name: t.Name, keyPrefix: t.KeyPrefix, remote: t.Remote}
	}
	// Routing is dropped with the map it would address: with one destination
	// every path resolves to it anyway, and keeping a non-empty routing map
	// beside a nil targets map would mean every lookup had to special-case the
	// default name.
	routing := cfg.Routing
	if targets == nil {
		routing = nil
	}
	operationTimeout := cfg.OperationTimeout
	if operationTimeout <= 0 {
		operationTimeout = DefaultOperationTimeout
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
		targetName:           def.Name,
		targetKeyPrefix:      def.KeyPrefix,
		targetRemote:         def.Remote,
		targets:              targets,
		routing:              routing,
		operationTimeout:     operationTimeout,
		instanceID:           cfg.InstanceID,
		sqliteDBPath:         cfg.SQLiteDBPath,
		icebergCatalogDBPath: icebergCatalog,
		configPath:           cfg.ConfigPath,
		logger:               cfg.Logger.With().Str("component", "backup-manager").Logger(),
	}
	m.configureIcebergWarehouse(cfg)
	m.warnAboutInertTargets()
	return m, nil
}

// warnAboutInertTargets reports a configured target that nothing routes to.
//
// A WARNING and not a load refusal. Adding the [backup.targets.x] block in one
// commit and its databases= in the next is ordinary, and refusing to start an
// Arc instance over a destination that is merely inert would be the worse
// outcome by a wide margin. What the operator needs to know is that no backup
// will ever write to it, because planRun only makes a leg for a target some
// database is routed to — the alternative, a leg for every configured target,
// would make an unrelated store a hard precondition of every backup in the
// instance.
func (m *Manager) warnAboutInertTargets() {
	var inert []string
	for _, name := range m.sortedTargetNames() {
		if !m.targetIsRouted(name) {
			inert = append(inert, name)
		}
	}
	if len(inert) == 0 {
		return
	}
	m.logger.Warn().
		Strs("inert_targets", inert).
		Msg("These backup targets have no databases routed to them, so no backup will write to them; add databases to the target or remove it")
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

// DefaultOperationTimeout is the bound one backup or restore run had before
// backup.operation_timeout existed, and the fallback for a Manager built
// without one. The API handler shares it, so the window the listing calls
// "possibly in flight" and the window a run actually gets cannot drift.
const DefaultOperationTimeout = 2 * time.Hour

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
//
// Every target a run touches is probed, each naming itself, before the index
// is written and before anything is copied (#1085 stage B2b-2): a run spans
// its legs, so one unreachable leg has to fail the run before the other legs
// have written anything an operator then has to clean up.
func (m *Manager) probeDestination(ctx context.Context, dest backupTarget, backupID string) error {
	probeCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	if _, err := dest.backend.Exists(probeCtx, backupID+"/manifest.json"); err != nil {
		return fmt.Errorf("%s did not answer: %w", dest.describe(), err)
	}
	return nil
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

// The key-length arithmetic lives on backupTarget (see target.go), because it
// is PER DESTINATION: two targets of one run with different object-key
// prefixes have different usable source-key lengths, and the message that
// reports a skip has to name whose limit was hit.

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

// BackupListing is one listing over every configured destination (#1085 stage
// B2b-2).
//
// A struct rather than a fifth return value: the listing now reports three
// things beyond the backups themselves, and a signature with five results is
// how a caller silently drops the one it did not know about.
type BackupListing struct {
	// Backups is every backup found, one entry per backup ID even when
	// several targets hold a slice of it; the entry's Targets names them all.
	Backups []BackupSummary
	// FilteredForeign is how many backups were left out because another
	// instance wrote them. See ListBackupsFilteringForeign.
	FilteredForeign int
	// UnreachableTargets names the configured targets that would not answer.
	// They contribute nothing to Backups, and the listing still succeeds: one
	// dead store must not hide the backups on the others. Empty when every
	// target answered; a listing where EVERY target failed is an error
	// instead.
	UnreachableTargets []string
	// IncompleteRuns are runs whose index or manifests say they did not commit
	// to every target they touched. A separate field, never mixed into
	// Backups: a client that renders that array must not grow phantom
	// entries.
	IncompleteRuns []IncompleteRun
}

// IncompleteRun is a backup run that did not commit a manifest on every target
// it named (#1085 stage B2b-2).
//
// This is what the run index exists for. Before it, a run that died after a
// sidecar landed left objects nothing enumerated: the listing keys on
// manifest.json and correctly did not show them, DeleteBackup by ID still
// removed them, so an operator who WATCHED the run fail could clean up and one
// who did not had objects at the destination no listing mentioned.
type IncompleteRun struct {
	BackupID  string    `json:"backup_id"`
	CreatedAt time.Time `json:"created_at"`
	// Targets is every target the run named, Committed the ones that have a
	// manifest, and Missing the ones that do not.
	//
	// Missing can be EMPTY, in two shapes, so a client must not treat it as
	// the reason the entry exists: when every target that did not commit was
	// merely unreachable (see Unknown), and for the backup.local_path
	// destination that predates targets, whose run index names no targets at
	// all because that destination has no name — there the entry itself is
	// the report and Targets is empty too.
	Targets   []string `json:"targets,omitempty"`
	Committed []string `json:"committed_targets,omitempty"`
	Missing   []string `json:"missing_targets"`
	// Unknown are this run's targets that would not answer, so whether they
	// committed could not be established. They are NOT in Missing: "missing"
	// means the listing looked and found no manifest, and counting a target
	// that was briefly down would report a complete backup as a run that did
	// not finish. An entry with Missing empty and Unknown non-empty is exactly
	// that case, and resolves itself when the store answers again.
	Unknown []string `json:"unknown_targets,omitempty"`
	// State is "aborted", "possibly_in_flight" or "undetermined".
	//
	// The third is for an entry with nothing known to be missing: every target
	// that has not committed is one that would not answer, so the run may well
	// be COMPLETE and neither of the other two words would be true of it. A
	// listing taken while one store is briefly down must not report a
	// finished backup as a run that did not finish.
	//
	// The second is not politeness. ListBackups has no primary-writer gate, so
	// on a shared destination a READER node lists a run the primary is
	// executing and has no m.active for it — reporting that as aborted is a
	// visible false alarm where the behaviour before this was a silent skip.
	// A run whose index landed within backup.operation_timeout of now may
	// still be running somewhere, and says so.
	State string `json:"state"`
}

// Incomplete-run states.
const (
	IncompleteRunAborted         = "aborted"
	IncompleteRunPossiblyRunning = "possibly_in_flight"
	IncompleteRunUndetermined    = "undetermined"
)

// ListBackups returns the backups at every configured destination that belong
// to this instance.
//
// "Belong to" means the manifest names this instance as owner, or names no
// owner at all (every backup taken before #1085 stage B2b-1, and every backup
// of an unidentified instance). A backup another instance wrote to the same
// bucket and prefix is left out, because merged listings are the hazard the
// owner field exists for. ListAllBackups returns those too, which is what
// makes disaster recovery onto fresh hardware possible.
//
// The three convenience forms below drop the unreachable targets and the
// incomplete runs; ListBackupsDetailed is the one the API answers from.
func (m *Manager) ListBackups(ctx context.Context) ([]BackupSummary, error) {
	listing, err := m.listBackups(ctx, false)
	return listing.Backups, err
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
	listing, err := m.listBackups(ctx, false)
	return listing.Backups, listing.FilteredForeign, err
}

// ListAllBackups returns every backup at every configured destination, this
// instance's and any other instance's, each summary carrying its owner id and
// ForeignOwner.
//
// The opt-in exists so the owner filter cannot lock an operator out of their
// own data: after a restore onto fresh hardware the new instance has a new
// identity, so every backup it restored from reads as foreign, and an
// operator who could not list them could not find the id of the next one to
// restore. Foreign-owner restore is allowed and echoed, never refused, and
// this is the listing that makes that reachable.
func (m *Manager) ListAllBackups(ctx context.Context) ([]BackupSummary, error) {
	listing, err := m.listBackups(ctx, true)
	return listing.Backups, err
}

// ListBackupsDetailed is the full listing: the backups, the foreign count, the
// targets that would not answer and the runs that did not finish.
func (m *Manager) ListBackupsDetailed(ctx context.Context, includeForeign bool) (BackupListing, error) {
	return m.listBackups(ctx, includeForeign)
}

// targetListing is one target's contribution to a listing.
type targetListing struct {
	target backupTarget
	// manifests is the manifest of every backup ID this target holds a
	// committed slice of, keyed by ID.
	manifests map[string]*Manifest
	// uncommitted are the IDs that have a directory here and no manifest: a
	// run in flight, or one that died before its commit record.
	uncommitted []string
	// err is a transport-wide failure. The target then contributes nothing and
	// is named in UnreachableTargets.
	err error
}

// listBackups fans out over every configured target, concurrently, and unions
// the results by backup ID.
//
// Concurrent for a reason that is not arithmetic. withDestinationTimeout
// derives from the caller's context, so N targets cannot each cost a fresh
// 60 s — but ONE dead target consumes the whole 30 s handler budget and
// cancels the rest, so a serial listing could never report the unreachable
// target it is supposed to mark.
func (m *Manager) listBackups(ctx context.Context, includeForeign bool) (BackupListing, error) {
	targets := m.configuredTargets()
	results := make([]targetListing, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t backupTarget) {
			defer wg.Done()
			results[i] = m.listOneTarget(ctx, t)
		}(i, t)
	}
	wg.Wait()

	var listing BackupListing
	var failed int
	var firstErr error
	for _, r := range results {
		if r.err != nil {
			failed++
			if firstErr == nil {
				firstErr = r.err
			}
			listing.UnreachableTargets = append(listing.UnreachableTargets, r.target.name)
		}
	}
	// Every target failed: that is a failure of the listing, not a partial
	// answer, and it keeps the single-destination contract — an unreachable
	// lone destination is an error naming it, as it was before the fan-out.
	if failed == len(results) && failed > 0 {
		return BackupListing{}, firstErr
	}

	// Union by ID, default target first so its manifest anchors the run-level
	// fields and a single-destination listing is ordered exactly as before.
	type run struct {
		manifests []*Manifest
		found     []string
		order     int
	}
	runs := map[string]*run{}
	var ids []string
	noManifest := map[string]bool{}
	for _, r := range results {
		if r.err != nil {
			continue
		}
		for _, id := range sortedManifestIDs(r.manifests) {
			entry, ok := runs[id]
			if !ok {
				entry = &run{order: len(ids)}
				runs[id] = entry
				ids = append(ids, id)
			}
			entry.manifests = append(entry.manifests, r.manifests[id])
			entry.found = append(entry.found, r.target.name)
		}
		for _, id := range r.uncommitted {
			noManifest[id] = true
		}
	}
	// An ID can be in both: one target committed its manifest and another did
	// not. It is then a partially-committed run, handled with the manifests
	// below — not a run with nothing anywhere, which is what the index pass
	// after it reports. Filtered here rather than inside the loop above
	// because the loop walks targets in a fixed order and whether the
	// committed leg comes first or second must not decide the answer.
	for id := range runs {
		delete(noManifest, id)
	}

	live := ""
	if p := m.active.Load(); p != nil && p.Status == "running" {
		live = p.BackupID
	}

	for _, id := range ids {
		entry := runs[id]
		merged := mergeRunManifests(entry.manifests)
		own := m.ownsManifest(merged)
		if !own && !includeForeign {
			listing.FilteredForeign++
			continue
		}
		// A run this process is executing right now is reported through
		// /status, not here: its legs commit at the end, so a leg that has
		// landed while the others have not is progress, not a gap.
		if id == live {
			continue
		}
		summary := SummaryFromManifest(merged)
		summary.ForeignOwner = !own
		// Sorted, like RunTargets and the incomplete-run fields: every list of
		// target names a client reads has one order, so a diff of two listings
		// is a change in the backups and never in map iteration.
		//
		// From the run itself (RunTargets), NOT from the legs this listing
		// could read. Those differ precisely when a target is unreachable, and
		// then entry.found is a subset: reporting it would hand a client the
		// single-target field for a backup that spans two, with a file count
		// that is the readable legs summed and therefore too low. The entry
		// says so instead, via PartialView.
		expected := expectedTargets(merged, entry.found)
		summary.Targets = sortedCopy(expected)
		// Target stays set only when exactly one destination holds the
		// backup, so a client that reads the single-target field is not handed
		// one of several.
		if len(expected) != 1 {
			summary.Target = ""
		}
		missing := missingTargets(expected, entry.found, listing.UnreachableTargets)
		unknown := intersect(expected, listing.UnreachableTargets)
		summary.PartialView = len(missing) > 0 || len(unknown) > 0
		listing.Backups = append(listing.Backups, summary)
		if summary.PartialView {
			listing.IncompleteRuns = append(listing.IncompleteRuns, IncompleteRun{
				BackupID:  id,
				CreatedAt: merged.CreatedAt,
				Targets:   expected,
				Committed: sortedCopy(entry.found),
				Missing:   missing,
				Unknown:   unknown,
				State:     m.runState(merged.CreatedAt, missing),
			})
		}
	}

	// Runs with no manifest anywhere. Their shape comes from the run index,
	// which is the one key that names every target a run touched; without one
	// there is nothing to report and the directory is skipped in silence,
	// exactly as it was before the index existed (an in-flight run, or a
	// pre-index one that died).
	//
	// Looked for on EVERY configured target, not just the current default,
	// even though the write only ever goes to the default. The two differ
	// after an operator re-points backup.default_target, and then the run that
	// a directory listing has already enumerated would be dropped here in
	// silence — re-opening the hole the index exists to close. This loop only
	// runs for IDs that have no manifest anywhere, which is rare, so the extra
	// requests cost nothing on a healthy destination.
	//
	// KNOWN LIMIT: a run that died before any manifest is invisible here when
	// no reachable target holds its index — the target that was the default
	// when it ran is unreachable now, or its objects have been moved. The
	// directory listing that produced the ID and the index key are on the same
	// store, so there is nothing left to read; UnreachableTargets says which
	// store would not answer, and the ID becomes enumerable again as soon as
	// it does.
	for _, id := range sortedKeysOf(noManifest) {
		if id == live {
			continue
		}
		index := m.findRunIndex(ctx, id)
		if index == nil {
			continue
		}
		if !m.ownsInstanceID(index.OwnerInstanceID) && !includeForeign {
			listing.FilteredForeign++
			continue
		}
		listing.IncompleteRuns = append(listing.IncompleteRuns, IncompleteRun{
			BackupID:  id,
			CreatedAt: index.CreatedAt,
			Targets:   index.Targets,
			Missing:   index.Targets,
			// incompleteState, never runState: a run with no manifest ANYWHERE
			// is missing in its entirety, whether or not its index names
			// targets, and nothing about it is unreachable. The
			// backup.local_path destination that predates targets has no
			// target name at all, so its index names none and Missing is
			// empty — which runState would read as "nothing is known to be
			// missing" and report undetermined, the one state that is false
			// here.
			State: m.incompleteState(index.CreatedAt),
		})
	}

	if listing.FilteredForeign > 0 {
		m.logger.Info().
			Int("filtered_foreign", listing.FilteredForeign).
			Str("this_instance_id", m.instanceID).
			Msg("Backups at this destination belong to another Arc instance and were left out of the listing; ask for them with include_foreign=true")
	}
	if len(listing.UnreachableTargets) > 0 {
		m.logger.Warn().
			Strs("unreachable_targets", listing.UnreachableTargets).
			Err(firstErr).
			Msg("Some backup targets would not answer; the listing holds what the others returned and names the ones it could not reach")
	}
	return listing, nil
}

// sortedCopy is a sorted copy, so a caller's slice is never reordered under
// it and every target list a client reads has one order.
func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// runState is IncompleteRun.State: "undetermined" when nothing is known to be
// missing, and otherwise whether the run may still be running.
//
// The split matters because an entry is also raised for a run whose only
// non-committed targets are unreachable ones, and such a run may be perfectly
// complete — calling it aborted, or possibly in flight, would be a claim about
// a manifest nobody has looked at.
func (m *Manager) runState(createdAt time.Time, missing []string) string {
	if len(missing) == 0 {
		return IncompleteRunUndetermined
	}
	return m.incompleteState(createdAt)
}

// incompleteState decides whether a run with missing manifests may still be
// running. See IncompleteRun.State.
func (m *Manager) incompleteState(createdAt time.Time) string {
	// Zero is a Manager built by hand rather than through NewManager, which
	// every test in this package does; without this the window would be
	// closed and every incomplete run would read as aborted.
	window := m.operationTimeout
	if window <= 0 {
		window = DefaultOperationTimeout
	}
	if !createdAt.IsZero() && time.Since(createdAt) < window {
		return IncompleteRunPossiblyRunning
	}
	return IncompleteRunAborted
}

// expectedTargets is every target a run committed to, as the run itself
// records it.
//
// RunTargets is on every leg's manifest of a MULTI-LEG run (#1085 stage
// B2b-2), so a manifest found alone is self-describing and nothing here
// depends on the default target being reachable. A manifest WITHOUT it is a
// single-destination run — planRun writes the field only when there is more
// than one leg, which keeps such a manifest byte-identical to stage B2b-1 — or
// a backup taken before this stage; either way the targets it was found at are
// the whole run.
func expectedTargets(merged *Manifest, found []string) []string {
	if len(merged.RunTargets) > 0 {
		return merged.RunTargets
	}
	return found
}

// missingTargets are the expected names this listing LOOKED AT and found no
// manifest on.
//
// A target that would not answer is left out, because "missing" has to mean
// "looked and found nothing". Counting an unreachable one would report a
// perfectly complete backup as a run that did not finish, every time a store
// was briefly down — and the honest report of that state is
// BackupListing.UnreachableTargets, with IncompleteRun.Unknown naming the ones
// belonging to this run.
func missingTargets(expected, found, unreachable []string) []string {
	skip := make(map[string]bool, len(found)+len(unreachable))
	for _, name := range found {
		skip[name] = true
	}
	for _, name := range unreachable {
		skip[name] = true
	}
	var missing []string
	for _, name := range expected {
		if !skip[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

// intersect is the sorted overlap of two name lists.
func intersect(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, name := range b {
		in[name] = true
	}
	var out []string
	for _, name := range a {
		if in[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// listOneTarget reads one target's backup IDs and their manifests.
func (m *Manager) listOneTarget(ctx context.Context, t backupTarget) targetListing {
	out := targetListing{target: t, manifests: map[string]*Manifest{}}
	ids, err := m.listBackupIDs(ctx, t)
	if err != nil {
		out.err = err
		return out
	}
	for _, id := range ids {
		manifestPath := id + "/manifest.json"
		// A manifest is the commit record: the sidecar is written before it
		// precisely so a run that died between the two leaves nothing a
		// listing shows (see CreateBackup). A directory without one is such a
		// run, or a backup still in flight, and is NOT reported as a failure
		// — polling the listing during a backup must not log a warning per
		// poll. Since #1085 stage B2b-2 the ID is remembered so the run index
		// can say whether it aborted.
		data, err := m.readManifest(ctx, t, manifestPath)
		switch {
		case errors.Is(err, ErrBackupNotFound):
			out.uncommitted = append(out.uncommitted, id)
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
				Str("target", t.name).Msg("Failed to read manifest, skipping")
			continue
		}
		manifest, err := UnmarshalManifest(data)
		if err != nil {
			m.logger.Warn().Str("path", manifestPath).Err(err).
				Str("target", t.name).Msg("Failed to parse manifest, skipping")
			continue
		}
		out.manifests[id] = manifest
	}
	return out
}

// sortedManifestIDs is the deterministic iteration order of one target's
// manifests.
func sortedManifestIDs(manifests map[string]*Manifest) []string {
	out := make([]string, 0, len(manifests))
	for id := range manifests {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// sortedKeysOf is the deterministic iteration order of a string set.
func sortedKeysOf(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// readManifest reads one backup manifest from one target, in ONE round trip.
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
func (m *Manager) readManifest(ctx context.Context, t backupTarget, manifestPath string) ([]byte, error) {
	readCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	data, err := t.backend.Read(readCtx, manifestPath)
	if err == nil {
		return data, nil
	}
	if storage.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s", ErrBackupNotFound, manifestPath)
	}
	return nil, fmt.Errorf("failed to read %s from %s: %w", manifestPath, t.describe(), err)
}

// listBackupIDs returns the names at one target that are shaped like a backup
// ID.
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
func (m *Manager) listBackupIDs(ctx context.Context, t backupTarget) ([]string, error) {
	listCtx, cancel := withDestinationTimeout(ctx)
	defer cancel()
	if dl, ok := t.backend.(storage.DirectoryLister); ok {
		dirs, err := dl.ListDirectories(listCtx, "")
		if err != nil {
			return nil, fmt.Errorf("failed to list %s: %w", t.describe(), err)
		}
		ids := make([]string, 0, len(dirs))
		for _, d := range dirs {
			if IsValidBackupID(d) {
				ids = append(ids, d)
			}
		}
		return ids, nil
	}

	files, err := t.backend.List(listCtx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list %s: %w", t.describe(), err)
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

// GetBackup reads a backup's MERGED run view: the per-target manifests of
// every target the run committed to, assembled into one (#1085 stage B2b-2).
//
// Never filtered by owner: a backup another instance wrote is readable and
// restorable, with its owner echoed. Refusing it would be self-locking —
// restoring onto fresh hardware is what backups are for.
//
// It does NOT refuse a run with an unreachable or unconfigured leg, because
// its callers are the API echo and the pre-restore mode resolution, which want
// the best view available. The RESTORE refuses; see readRunManifests and
// RestoreBackup.
func (m *Manager) GetBackup(ctx context.Context, backupID string) (*Manifest, error) {
	read, err := m.readRunManifests(ctx, backupID)
	if err != nil {
		return nil, err
	}
	return read.merged, nil
}

// BackupDetail is the answer to GET /api/v1/backup/:id (#1085 stage B2b-2):
// the merged run view at the top level, exactly where a one-manifest reader
// already looks for it, plus the per-target slices beside it.
//
// The embedded manifest is what keeps arcli and every other existing client
// working: encoding/json promotes an embedded struct's fields, so the response
// is the manifest it was, with "targets" added.
type BackupDetail struct {
	*Manifest
	// Targets is one entry per leg, default first. Absent for a backup with a
	// single destination, where the top-level fields already describe it in
	// full and an array of one would be noise.
	Targets []TargetSlice `json:"targets,omitempty"`
	// UnknownTargets, UnreachableTargets and MissingTargets are the three
	// states a leg can be in that a RESTORE refuses (see RestoreBackup). They
	// are reported here rather than hidden so an operator can see why a
	// restore will be refused before they attempt one.
	UnknownTargets     []string `json:"unknown_targets,omitempty"`
	UnreachableTargets []string `json:"unreachable_targets,omitempty"`
	MissingTargets     []string `json:"missing_targets,omitempty"`
}

// TargetSlice is one target's slice of a backup, as its own manifest describes
// it.
type TargetSlice struct {
	Name      string `json:"name"`
	IsDefault bool   `json:"is_default,omitempty"`
	// Databases are the storage-root segments this leg holds data files for.
	Databases      []string `json:"databases,omitempty"`
	TotalFiles     int64    `json:"total_files"`
	TotalSizeBytes int64    `json:"total_size_bytes"`
	SkippedFiles   int64    `json:"skipped_files,omitempty"`
	AuxiliaryFiles int64    `json:"auxiliary_files,omitempty"`
	// CompactionStateFiles, UnregisteredSkipped and ManifestOnlyFiles are per
	// leg for the reason recorded on Manifest.RunTargets: the counts that gate
	// a replace-mode restore have to describe the slice they belong to, or
	// every per-leg replace would be refused by another leg's gaps.
	CompactionStateFiles int64 `json:"compaction_state_files,omitempty"`
	UnregisteredSkipped  int64 `json:"unregistered_skipped,omitempty"`
	ManifestOnlyFiles    int64 `json:"manifest_only_files,omitempty"`
	UnaddressableFiles   int64 `json:"unaddressable_files,omitempty"`
}

// GetBackupDetail is GetBackup plus the per-target slices, for the API.
func (m *Manager) GetBackupDetail(ctx context.Context, backupID string) (*BackupDetail, error) {
	read, err := m.readRunManifests(ctx, backupID)
	if err != nil {
		return nil, err
	}
	detail := &BackupDetail{
		Manifest:           read.merged,
		UnknownTargets:     read.unknown,
		UnreachableTargets: read.unreachable,
		MissingTargets:     read.missing,
	}
	if len(read.legs) > 1 {
		for _, leg := range read.legs {
			detail.Targets = append(detail.Targets, leg.slice())
		}
	}
	return detail, nil
}

// slice renders one leg as the API's per-target entry.
func (l runLeg) slice() TargetSlice {
	s := TargetSlice{
		Name:                 l.target.name,
		IsDefault:            l.manifest.IsDefaultTarget,
		TotalFiles:           l.manifest.TotalFiles,
		TotalSizeBytes:       l.manifest.TotalSizeBytes,
		SkippedFiles:         l.manifest.SkippedFiles,
		AuxiliaryFiles:       l.manifest.AuxiliaryFiles,
		CompactionStateFiles: l.manifest.CompactionStateFiles,
		UnregisteredSkipped:  l.manifest.UnregisteredSkipped,
		ManifestOnlyFiles:    l.manifest.ManifestOnlyFiles,
		UnaddressableFiles:   l.manifest.UnaddressableFiles,
	}
	for _, db := range l.manifest.Databases {
		s.Databases = append(s.Databases, db.Name)
	}
	return s
}

// runLeg is one target's slice of a run: the destination and the manifest it
// holds.
type runLeg struct {
	target   backupTarget
	manifest *Manifest
}

// runRead is everything a restore or an API read needs to know about a run.
type runRead struct {
	// merged is the run-level view every gate reads (decision 9 of the B2b-2
	// plan): the union of the legs. Never one leg's manifest, because eight
	// restore gates read it and a non-default leg's manifest has no
	// HasMetadata, no HasConfig and no Databases for the other legs.
	merged *Manifest
	// legs are the manifests found, in configuredTargets order — the order
	// this node happens to be configured in now, which says nothing about
	// which leg wrote the default-leg-only fields. Read anchor for that.
	legs []runLeg
	// anchor is the LEG whose manifest asserted the default-leg-only fields —
	// HasMetadata, HasConfig, IcebergWarehouse — and therefore the leg those
	// bytes must be read back FROM.
	//
	// It is the leg, not just its manifest, because the three default-leg
	// restore steps have to reach a backend and this node's current default is
	// not necessarily the one that wrote them: a target that was the sole
	// target (and so the default) stays the anchor after an operator adds a
	// second target and re-points backup.default_target at it. Reading those
	// steps from the current default then fails on SQLite and arc.toml, and
	// SILENTLY no-ops the outside-root Iceberg warehouse, because a zero-object
	// listing is a legitimate "this run had no warehouse".
	//
	// Chosen by IsDefaultTarget where it is set, and otherwise by being the
	// only leg. It must NOT be keyed off IsDefaultTarget alone: planRun writes
	// that field only for multi-leg runs, so the single-target manifest this
	// exists to fix has it false.
	anchor runLeg
	// unknown are RunTargets names this node has no configured target for;
	// unreachable the configured ones that would not answer; missing the
	// configured, reachable ones with no manifest for the ID. A restore
	// refuses on any of the three, BEFORE writing anything.
	unknown     []string
	unreachable []string
	missing     []string
}

// readRunManifests reads a run's manifest from every configured target,
// concurrently, and resolves the run's shape.
//
// The names in RunTargets are resolved against LOCAL configuration and nothing
// else, which is Manifest.Target's rule inherited: a manifest is data read from
// backup storage, so a field in it must never select the credentials or the
// location used to read further bytes.
//
// A run with ONE target is self-contained and resolves no names at all: the
// manifest was found at the destination this node is configured with, so a
// backup whose Target names a target this node does not have still reads and
// still restores, exactly as #1085 stage B2b-1 promised. Only a run that spans
// SEVERAL targets has to resolve them, because the other legs' bytes are
// somewhere this node must be able to reach.
func (m *Manager) readRunManifests(ctx context.Context, backupID string) (*runRead, error) {
	targets := m.configuredTargets()
	manifests := make([]*Manifest, len(targets))
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t backupTarget) {
			defer wg.Done()
			data, err := m.readManifest(ctx, t, backupID+"/manifest.json")
			if err != nil {
				errs[i] = err
				return
			}
			manifest, err := UnmarshalManifest(data)
			if err != nil {
				errs[i] = err
				return
			}
			manifests[i] = manifest
		}(i, t)
	}
	wg.Wait()

	var found []runLeg
	var transport []string
	var firstTransportErr error
	for i, t := range targets {
		switch {
		case manifests[i] != nil:
			found = append(found, runLeg{target: t, manifest: manifests[i]})
		case errors.Is(errs[i], ErrBackupNotFound):
		case errs[i] != nil:
			transport = append(transport, t.name)
			if firstTransportErr == nil {
				firstTransportErr = errs[i]
			}
		}
	}
	if len(found) == 0 {
		if firstTransportErr != nil {
			return nil, firstTransportErr
		}
		// Re-stated against the backup ID rather than the key, because that is
		// what the caller asked for and what the API echoes.
		return nil, fmt.Errorf("%w: %s", ErrBackupNotFound, backupID)
	}

	read := &runRead{legs: found}
	// The anchor is the default target's leg when it has a manifest, because
	// that manifest is the one carrying the default-leg-only fields; any leg's
	// RunTargets describes the run, so a run whose default leg is unreachable
	// still resolves. Falls back to the first leg found, which is the right
	// answer for the single-leg run whose manifest has no IsDefaultTarget.
	read.anchor = found[0]
	for _, leg := range found {
		if leg.manifest.IsDefaultTarget {
			read.anchor = leg
			break
		}
	}
	anchor := read.anchor.manifest
	legManifests := make([]*Manifest, 0, len(found))
	for _, leg := range found {
		legManifests = append(legManifests, leg.manifest)
	}
	read.merged = mergeRunManifests(legManifests)

	if len(anchor.RunTargets) <= 1 {
		// One destination: nothing to resolve. A transport failure on ANOTHER
		// configured target is not this run's problem.
		return read, nil
	}
	have := make(map[string]bool, len(found))
	for _, leg := range found {
		have[leg.target.name] = true
	}
	configured := make(map[string]bool, len(targets))
	for _, t := range targets {
		configured[t.name] = true
	}
	failed := make(map[string]bool, len(transport))
	for _, name := range transport {
		failed[name] = true
	}
	for _, name := range anchor.RunTargets {
		switch {
		case have[name]:
		case !configured[name]:
			read.unknown = append(read.unknown, name)
		case failed[name]:
			read.unreachable = append(read.unreachable, name)
		default:
			read.missing = append(read.missing, name)
		}
	}
	return read, nil
}

// ErrOperationInProgress is returned by DeleteBackup when a backup or restore
// operation holds the manager. The API layer maps it to 409 Conflict.
var ErrOperationInProgress = errors.New("a backup or restore operation is in progress")

// DeleteBackup removes all files for a backup from EVERY configured target
// (#1085 stage B2b-2).
//
// It refuses to run concurrently with a backup or restore (#626): deleting the
// backup a restore is reading tears files out from under it mid-operation, and
// deleting the backup being written leaves a half-written/half-deleted
// directory. TryLock rather than Lock — an operation can hold m.mu for hours,
// and queuing a synchronous HTTP-driven delete behind it would pin the request
// goroutine long past its context deadline. Callers get ErrOperationInProgress
// and retry when the operation finishes.
//
// One lock for every target, not one per target: a run spans its legs, so
// per-target locking would not make "back up to A while deleting from B" safe.
//
// It deletes by PREFIX LISTING rather than from the manifest, which is what
// makes it reach an aborted run's leftovers — the run index names the targets,
// and this is what cleans them up. A target that cannot be reached is reported
// AFTER everything reachable has been deleted, naming it, so a re-run finishes
// the job; the alternative, aborting on the first failure, leaves objects on
// targets that were perfectly able to answer.
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
	//
	// Every configured target is tried, not just the default: a backup held
	// only on a routed target would otherwise get no echo at all before an
	// irreversible delete, which is the opposite of the point.
	for _, t := range m.configuredTargets() {
		data, err := m.readManifest(ctx, t, backupID+"/manifest.json")
		if err != nil {
			continue
		}
		manifest, err := UnmarshalManifest(data)
		if err != nil {
			continue
		}
		if !m.ownsManifest(manifest) {
			m.logger.Warn().
				Str("backup_id", backupID).
				Str("target", t.name).
				Str("owner_instance_id", manifest.OwnerInstanceID).
				Str("this_instance_id", m.instanceID).
				Msg("Deleting a backup written by a different Arc instance")
		}
		break
	}

	prefix := backupID + "/"
	var deleted int
	var held int
	var unreachable []string
	var firstErr error
	for _, t := range m.configuredTargets() {
		// List the files under this backup ID. Bounded: a delete is reachable
		// from the API handler's 30 s context, but the listing itself is one
		// request per page and a stalled destination would otherwise hold the
		// single-operation slot for the whole handler budget.
		listCtx, cancel := withDestinationTimeout(ctx)
		files, err := t.backend.List(listCtx, prefix)
		cancel()
		if err != nil {
			unreachable = append(unreachable, t.name)
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to list the files of backup %s in %s: %w", backupID, t.describe(), err)
			}
			continue
		}
		if len(files) == 0 {
			continue
		}
		held++
		// Use batch delete if available
		if bd, ok := t.backend.(storage.BatchDeleter); ok {
			if err := bd.DeleteBatch(ctx, files); err != nil {
				unreachable = append(unreachable, t.name)
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to delete backup %s from %s: %w", backupID, t.describe(), err)
				}
				continue
			}
			deleted += len(files)
			continue
		}
		for _, f := range files {
			if err := t.backend.Delete(ctx, f); err != nil {
				m.logger.Warn().Str("path", f).Str("target", t.name).Err(err).Msg("Failed to delete backup file")
				continue
			}
			deleted++
		}
	}

	if len(unreachable) > 0 {
		// Having deleted what it could: the error names the targets that still
		// hold objects so a re-run finishes the job, and "not found" is never
		// the answer when a target might still hold the backup.
		return fmt.Errorf("backup %s was deleted where it could be reached, but these backup targets could not be: %s; run the delete again: %w",
			backupID, strings.Join(unreachable, ", "), firstErr)
	}
	if held == 0 {
		return fmt.Errorf("%w: %s", ErrBackupNotFound, backupID)
	}

	m.logger.Info().Str("backup_id", backupID).Int("files_deleted", deleted).Int("targets", held).Msg("Backup deleted")
	return nil
}
