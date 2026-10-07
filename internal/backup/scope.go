package backup

// Scoping a backup to one or more databases (#1084, stage A of the
// per-database backup series).
//
// A scope is a set of storage-root segments. `databases: ["prod"]` takes
// everything under prod/, prod's field schema anchors (_schema/prod/...) and
// prod's compaction recovery state (_compaction_state/<tier>/prod/...), and
// nothing else: not other databases, not the Iceberg namespace directories,
// not the SQLite metadata (which holds every database's tier rows, the
// tokens, the continuous queries and the audit log, and so cannot ride with
// one database). Edge-sync spoke data lives under <spoke>/<db>/..., so its
// storage-root segment is the spoke: ["prod"] does not include spoke1/prod,
// and ["spoke1"] takes the whole spoke. The two reserved roots encode a spoke
// differently (_schema/<spoke>/<db>/... versus _compaction_state/<tier>/
// <spoke>/<db>/...), which is why ownsSchemaAnchor is an exact-match rule on
// the second segment and ownsCompactionState a boundary-prefix rule on the
// database part; both give the same answer for the same scope.
//
// An empty scope is today's whole-instance backup, byte for byte: every method
// here answers "yes, own it" for an empty scope, so the unscoped call sites
// read the same as before.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// maxScopeDatabases bounds how many names one backup may be scoped to. The
// handler probes each name synchronously inside its request timeout.
const maxScopeDatabases = 256

// scope is the set of storage-root segments one backup is limited to. A nil
// *scope and a scope with no names both mean unscoped.
type scope struct {
	names []string // sorted, de-duplicated
	set   map[string]struct{}
}

// newScope validates, sorts and de-duplicates the names. Every name must be
// usable as one storage key segment (it becomes a ListObjects prefix and a
// PrefixProber prefix) and must not be a reserved root: _schema and
// _compaction_state hold Arc's own state, not a database, and the hot-prefix
// probe would otherwise call them known. The API layer applies the same rules
// before the manager sees the names; this is the belt for direct callers.
func newScope(names []string) (*scope, error) {
	sc := &scope{set: make(map[string]struct{}, len(names))}
	for _, name := range names {
		if name == "" {
			return nil, errors.New("database name is empty")
		}
		if err := storage.ValidateKeySegment(name); err != nil {
			return nil, fmt.Errorf("database name %q is not usable as a storage path segment: %w", name, err)
		}
		if storage.IsReservedRootDir(name) {
			return nil, fmt.Errorf("database name %q is a reserved storage root, not a database", name)
		}
		if _, dup := sc.set[name]; dup {
			continue
		}
		sc.set[name] = struct{}{}
		sc.names = append(sc.names, name)
	}
	if len(sc.names) > maxScopeDatabases {
		return nil, fmt.Errorf("a backup can be scoped to at most %d databases, got %d", maxScopeDatabases, len(sc.names))
	}
	sort.Strings(sc.names)
	return sc, nil
}

// scopeFromManifest rebuilds the scope a backup was taken with, for a restore.
// The names come from the manifest and are only ever compared against path
// segments here, never joined into a key.
func scopeFromManifest(names []string) *scope {
	if len(names) == 0 {
		return nil
	}
	sc := &scope{names: append([]string(nil), names...), set: make(map[string]struct{}, len(names))}
	for _, n := range names {
		sc.set[n] = struct{}{}
	}
	sort.Strings(sc.names)
	return sc
}

func (sc *scope) empty() bool {
	return sc == nil || len(sc.names) == 0
}

// has reports whether first (a storage-root segment) is in the scope. Always
// true for an empty scope.
func (sc *scope) has(first string) bool {
	if sc.empty() {
		return true
	}
	_, ok := sc.set[first]
	return ok
}

// ownsData reports whether a database data file (or any key whose first
// segment is a database) belongs to the scope: its storage-root segment is in
// the set. The rule the cluster cross-check and a replace-mode restore use;
// note it is the PATH segment, not the cluster manifest's Database label,
// which for an edge-sync spoke file is the canonical database rather than the
// spoke.
func (sc *scope) ownsData(path string) bool {
	if sc.empty() {
		return true
	}
	first, _, _ := strings.Cut(filepath.ToSlash(path), "/")
	return sc.has(first)
}

// ownsSchemaAnchor reports whether a key under _schema/ belongs to the scope:
// the second segment is the scope name exactly (_schema/<db>/<meas>.parquet,
// or _schema/<spoke>/<db>/<meas>.parquet; #914, #927).
func (sc *scope) ownsSchemaAnchor(path string) bool {
	if sc.empty() {
		return true
	}
	parts := strings.SplitN(filepath.ToSlash(path), "/", 3)
	if len(parts) < 3 || parts[0] != "_schema" {
		return false
	}
	return sc.has(parts[1])
}

// ownsCompactionState reports whether a key under _compaction_state/ belongs
// to the scope: _compaction_state/<tier>/<database>/<job>.json[.quarantined],
// where the database part is everything between the tier and the last slash
// (compaction.manifestPathDatabase) and carries a slash for a spoke
// pseudo-database. Owned when that part equals a name or starts with
// "<name>/".
func (sc *scope) ownsCompactionState(path string) bool {
	if sc.empty() {
		return true
	}
	rest, ok := strings.CutPrefix(filepath.ToSlash(path), compactionStateDir+"/")
	if !ok {
		return false
	}
	_, rest, ok = strings.Cut(rest, "/") // drop the tier
	if !ok {
		return false
	}
	i := strings.LastIndex(rest, "/")
	if i < 0 {
		return false
	}
	database := rest[:i]
	for _, name := range sc.names {
		if database == name || strings.HasPrefix(database, name+"/") {
			return true
		}
	}
	return false
}

// ownsPath is the union rule for a hidden key: a data key the scope owns, or
// an anchor or compaction manifest of one of its databases. Used to filter
// the unaddressable inventory, which lists the whole root.
func (sc *scope) ownsPath(path string) bool {
	if sc.empty() {
		return true
	}
	p := filepath.ToSlash(path)
	first, _, _ := strings.Cut(p, "/")
	switch {
	case first == "_schema":
		return sc.ownsSchemaAnchor(p)
	case first == compactionStateDir:
		return sc.ownsCompactionState(p)
	}
	return sc.has(first)
}

// icebergNamespaceDir is the Iceberg exporter's namespace directory for a
// database: <nsPrefix>_<db>.db.
func icebergNamespaceDir(nsPrefix, database string) string {
	return nsPrefix + "_" + database + ".db"
}

// ownsIcebergNamespaceDir reports whether a depth-1 warehouse directory is the
// Iceberg namespace of one of the scoped databases.
func (sc *scope) ownsIcebergNamespaceDir(dir, nsPrefix string) bool {
	if sc.empty() {
		return true
	}
	for _, name := range sc.names {
		if dir == icebergNamespaceDir(nsPrefix, name) {
			return true
		}
	}
	return false
}

// TierLookup answers whether this node's tier metadata holds any row for a
// database. It is how a scoped backup recognises a fully cold database, whose
// hot prefix is empty and whose anchors may be gone: one indexed query
// (tiering.MetadataStore.GetTiersForDatabase). Nil when tiering is off, where
// a fully cold database cannot exist. Independent of TierRecorder in the
// wiring, though cmd/arc attaches both from the same tiering manager.
type TierLookup interface {
	DatabaseHasTierRows(ctx context.Context, database string) (bool, error)
}

// SetTierLookup wires this node's tier metadata for the known-database check.
// Nil is ignored, as SetTierRecorder; a caller holding a typed nil pointer
// must check for nil itself (#713).
func (m *Manager) SetTierLookup(l TierLookup) {
	if l == nil {
		return
	}
	m.tierLookup = l
}

// ColdCounter reports how many cold-tier files this node's tier metadata
// holds, grouped by database. It is how a backup records the files it is NOT
// carrying: cold-tier objects are not copied yet (#1085 stage C), so a backup
// of a tiered deployment is complete only with respect to hot storage, and the
// count is the size of that gap.
//
// One grouped query (tiering.MetadataStore.CountFilesInTierByDatabase), not
// one per database, because the set that matters is "every database with cold
// rows" and that is not the backup inventory — a fully cold database has no
// hot files and so appears in neither. Nil when tiering is off, where no file
// can be cold. Independent of TierRecorder and TierLookup in the wiring,
// though cmd/arc attaches all three from the same tiering manager.
type ColdCounter interface {
	CountColdFilesByDatabase(ctx context.Context) (map[string]int64, error)
}

// SetColdCounter wires this node's tier metadata for the cold-file marker.
// Nil is ignored, as SetTierLookup; a caller holding a typed nil pointer must
// check for nil itself (#713).
func (m *Manager) SetColdCounter(c ColdCounter) {
	if c == nil {
		return
	}
	m.coldCounter = c
}

// ClusterManifestWired reports whether the Raft file manifest is attached.
// The API layer resolves a scoped restore mode from this, not from the
// presence of a cluster coordinator: the two differ on a cluster node without
// cluster.raft_data_dir (coordinator wired, manifest not), and the manager
// runs the restore from this value, so the handler must echo from it too.
func (m *Manager) ClusterManifestWired() bool {
	return m.cluster != nil
}

// UnknownDatabasesError names the scoped databases nothing on this node
// knows: no listable object under <name>/, no schema anchor under
// _schema/<name>/ and no tier row. The API layer maps it to 400.
type UnknownDatabasesError struct {
	Names []string
}

func (e *UnknownDatabasesError) Error() string {
	parts := make([]string, 0, len(e.Names))
	for _, n := range e.Names {
		parts = append(parts, fmt.Sprintf("unknown database %q: no data files, no schema anchors and no tier rows", n))
	}
	return strings.Join(parts, "; ")
}

// CheckDatabasesKnown reports, synchronously and with bounded work per name,
// whether every name is a database this node knows. Per name, cheapest
// first: one indexed tier-metadata query (when tiering is wired), then a
// bounded probe of <name>/, then of _schema/<name>/. Neither probe is a
// listing: a ListObjects of a large database would walk every file, and the
// API calls this for up to 256 names inside one request timeout. Backends
// without storage.PrefixProber (test fakes) fall back to ListObjects. Only
// when all three say no, and the backend can enumerate hidden keys, is the
// hidden set under <name>/ consulted: a database whose every file has a key
// no listing returns exists all the same, and the run must then fail with
// the rename advice rather than call the database unknown. That enumeration
// is bounded by the hidden keys under a prefix that holds no listable object,
// never by the size of a real database. Any probe or lookup error fails the
// check with its text (fail closed). Returns *UnknownDatabasesError when at
// least one name is unknown everywhere.
func (m *Manager) CheckDatabasesKnown(ctx context.Context, names []string) error {
	sc, err := newScope(names)
	if err != nil {
		return err
	}
	var unknown []string
	for _, name := range sc.names {
		rule, err := m.databaseKnownBy(ctx, name)
		if err != nil {
			return fmt.Errorf("could not check whether database %q is known: %w", name, err)
		}
		if rule == "" {
			unknown = append(unknown, name)
			continue
		}
		m.logger.Debug().Str("database", name).Str("rule", rule).Msg("Scoped backup database is known")
	}
	if len(unknown) > 0 {
		return &UnknownDatabasesError{Names: unknown}
	}
	return nil
}

// databaseKnownBy returns which rule recognised the database ("tier_rows",
// "data_files", "schema_anchors", "unaddressable_files"), or "" when none did.
// The hidden-key rule here looks under <name>/ only, while the belt inside
// CreateBackup (checkScopeKnown) also owns hidden anchors and compaction
// state of the name: this check is the stricter of the two, and a database
// evidenced by nothing but a hidden anchor being refused at the API is the
// safe direction.
func (m *Manager) databaseKnownBy(ctx context.Context, name string) (string, error) {
	if m.tierLookup != nil {
		has, err := m.tierLookup.DatabaseHasTierRows(ctx, name)
		if err != nil {
			return "", fmt.Errorf("tier metadata lookup: %w", err)
		}
		if has {
			return "tier_rows", nil
		}
	}
	has, err := m.prefixHasObjects(ctx, name+"/")
	if err != nil {
		return "", err
	}
	if has {
		return "data_files", nil
	}
	has, err = m.prefixHasObjects(ctx, "_schema/"+name+"/")
	if err != nil {
		return "", err
	}
	if has {
		return "schema_anchors", nil
	}
	// Nothing listable: the prefix is empty or holds only keys a listing
	// hides, so enumerating the hidden set is bounded by those keys.
	if ul, ok := m.dataStorage.(storage.UnusableLister); ok {
		hidden, err := ul.ListUnusable(ctx, name+"/")
		if err != nil {
			return "", fmt.Errorf("hidden key enumeration: %w", err)
		}
		for _, o := range hidden {
			if isBackupPayload(o.Path) {
				return "unaddressable_files", nil
			}
		}
	}
	return "", nil
}

// prefixHasObjects is the bounded probe, with the ListObjects fallback for a
// backend that does not implement storage.PrefixProber.
func (m *Manager) prefixHasObjects(ctx context.Context, prefix string) (bool, error) {
	if pp, ok := m.dataStorage.(storage.PrefixProber); ok {
		return pp.HasObjectsUnderPrefix(ctx, prefix)
	}
	lister, ok := m.dataStorage.(storage.ObjectLister)
	if !ok {
		return false, errors.New("storage backend does not support ListObjects")
	}
	objs, err := lister.ListObjects(ctx, prefix)
	if err != nil {
		return false, err
	}
	return len(objs) > 0, nil
}

// scopedListing is what a scoped backup lists: the objects under each scoped
// database root, the anchors under _schema/ the scope owns and the compaction
// state under _compaction_state/ the scope owns, in that order, plus which
// names were seen under their hot prefix and which had an anchor, for the
// known-database belt.
type scopedListing struct {
	objects    []storage.ObjectInfo
	hotSeen    map[string]bool
	anchorSeen map[string]bool
}

// listScoped replaces the whole-root listing for a scoped backup. Everything
// else under the root (other databases, Iceberg namespace directories, other
// reserved roots, dot-prefixed roots) is never listed, so a scoped run costs
// what its databases cost. On S3 a prefix is a key prefix, so "a/" does not
// match "ab/"; on local it selects the directory to walk.
func (m *Manager) listScoped(ctx context.Context, lister storage.ObjectLister, sc *scope) (*scopedListing, error) {
	out := &scopedListing{
		hotSeen:    make(map[string]bool, len(sc.names)),
		anchorSeen: make(map[string]bool, len(sc.names)),
	}
	for _, name := range sc.names {
		objs, err := lister.ListObjects(ctx, name+"/")
		if err != nil {
			return nil, fmt.Errorf("failed to list data files of database %q: %w", name, err)
		}
		if len(objs) > 0 {
			out.hotSeen[name] = true
		}
		out.objects = append(out.objects, objs...)
	}
	anchors, err := lister.ListObjects(ctx, "_schema/")
	if err != nil {
		return nil, fmt.Errorf("failed to list schema anchors: %w", err)
	}
	for _, obj := range anchors {
		if !sc.ownsSchemaAnchor(obj.Path) {
			continue
		}
		parts := strings.SplitN(filepath.ToSlash(obj.Path), "/", 3)
		out.anchorSeen[parts[1]] = true
		out.objects = append(out.objects, obj)
	}
	state, err := lister.ListObjects(ctx, compactionStateDir+"/")
	if err != nil {
		return nil, fmt.Errorf("failed to list compaction recovery state: %w", err)
	}
	for _, obj := range state {
		if sc.ownsCompactionState(obj.Path) {
			out.objects = append(out.objects, obj)
		}
	}
	return out, nil
}

// checkScopeKnown is the belt inside CreateBackup: with the scoped listing
// and the scope-filtered hidden set in hand, the hot, anchor and
// unaddressable rules cost nothing more, so only a name all three missed asks
// the tier metadata. A name unknown by every rule fails the run with the same
// error the API check gives. A name known only by hidden keys goes on to the
// all-unaddressable refusal, which names the real problem.
func (m *Manager) checkScopeKnown(ctx context.Context, sc *scope, listing *scopedListing, unaddressable []storage.UnusableObject) error {
	hiddenOwned := func(name string) bool {
		one := scopeFromManifest([]string{name})
		for _, o := range unaddressable {
			if one.ownsPath(o.Path) {
				return true
			}
		}
		return false
	}
	var unknown []string
	for _, name := range sc.names {
		switch {
		case listing.hotSeen[name]:
			m.logger.Debug().Str("database", name).Str("rule", "data_files").Msg("Scoped backup database is known")
		case listing.anchorSeen[name]:
			m.logger.Debug().Str("database", name).Str("rule", "schema_anchors").Msg("Scoped backup database is known")
		case hiddenOwned(name):
			m.logger.Debug().Str("database", name).Str("rule", "unaddressable_files").Msg("Scoped backup database is known only by keys no listing returns")
		case m.tierLookup != nil:
			has, err := m.tierLookup.DatabaseHasTierRows(ctx, name)
			if err != nil {
				return fmt.Errorf("could not check whether database %q is known: tier metadata lookup: %w", name, err)
			}
			if !has {
				unknown = append(unknown, name)
				continue
			}
			m.logger.Debug().Str("database", name).Str("rule", "tier_rows").Msg("Scoped backup database is known (fully cold: no hot files)")
		default:
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		return &UnknownDatabasesError{Names: unknown}
	}
	return nil
}
