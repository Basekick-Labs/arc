package backup

// Per-database backup routing (#1085 stage B2b-2).
//
// Stage B2b-1 made the destination configurable and possibly remote, with
// exactly one target. This is where it becomes a set: each target may name the
// databases whose files go to it, and everything else goes to the default
// target. A run then has one LEG per target it touches, each with its own
// manifest, sidecar and tallies (see backupRun in backup.go).
//
// The routing rule is deliberately the SAME rule a scoped backup already uses
// to decide what a database owns (scope.go), and that matters beyond
// consistency: an edge-sync spoke file lives at <spoke>/<db>/…, so its
// storage-root segment is the spoke, and ["prod"] does not mean spoke1/prod.
// If routing and scoping disagreed, a scoped backup of a routed database would
// enumerate one set of files and write them to another target.

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
)

// backupTarget is one destination a backup leg writes to: the backend, the
// name an operator spells, the object-key prefix the backend adds to every key
// and whether it is an object store.
//
// A VALUE type, copied at every use. The alternative — a pointer shared with
// the Manager's map — invites a caller to mutate a target mid-run, and the
// fields are four words.
type backupTarget struct {
	backend   storage.Backend
	name      string
	keyPrefix string
	remote    bool
}

// describe names the destination for an error message.
//
// An unreachable or misconfigured destination is reported with its target
// NAME, not just the underlying SDK error. With more than one configured
// store, "failed to list backup storage: dial tcp ... connection refused"
// leaves the operator to guess which one is down, and with installErrSanitizer
// masking quoted spans in logged errors the bucket name inside the wrapped
// error is not reliably readable either.
func (t backupTarget) describe() string {
	if t.name == "" {
		return "the backup destination"
	}
	return "backup target " + t.name
}

// keyHeadroom is how many bytes this destination adds to a source key: the
// target's object-key prefix plus the per-run backup prefix.
func (t backupTarget) keyHeadroom() int {
	return len(t.keyPrefix) + backupDataKeyHeadroom
}

// maxSourceKeyBytes is the longest source key this destination can hold, the
// figure both overlong-key messages report as max_source_key_bytes. It is PER
// TARGET: a target with an object-key prefix stores prefix+key, so the usable
// length shrinks by the prefix, and two targets of one run can disagree.
func (t backupTarget) maxSourceKeyBytes() int {
	return storage.MaxUsableKeyLen - t.keyHeadroom()
}

// keyTooLong reports whether a backup destination key, once this target's
// prefix is applied, exceeds what the store can hold.
//
// Bounded by storage.MaxUsableKeyLen on every backend. That figure subtracts
// LocalBackend's ".part" staging suffix from a 1024-byte limit, which is also
// S3's object-key limit and under Azure's, so on an object store it is
// conservative by five bytes and never wrong in the permissive direction.
func (t backupTarget) keyTooLong(destPath string) bool {
	return len(t.keyPrefix)+len(destPath) > storage.MaxUsableKeyLen
}

// destinationKeyBytes is the stored object name's length for a destination
// key, which is what the overlong-key messages report.
func (t backupTarget) destinationKeyBytes(destPath string) int {
	return len(t.keyPrefix) + len(destPath)
}

// defaultDestination is the target everything unrouted goes to.
//
// It re-reads the flat fields on EVERY call and caches nothing, and that is a
// hard requirement rather than a convenience. Twelve tests in this package
// swap m.backupStorage (and three of them m.targetName) on a Manager built by
// NewManager, to stand in for a counting, failing, unreachable, write-refusing
// or stalling destination. A cached destination, or a map entry for the
// default target, would route past those fakes and leave every one of them
// GREEN against a broken implementation.
//
// That is also why m.targets holds only the NON-default targets: the flat
// fields are the one description of the default destination, whatever shape
// the configuration has.
func (m *Manager) defaultDestination() backupTarget {
	return backupTarget{
		backend:   m.backupStorage,
		name:      m.targetName,
		keyPrefix: m.targetKeyPrefix,
		remote:    m.targetRemote,
	}
}

// destination returns the target of the given name, or the default
// destination when the name is the default target's or is unknown.
//
// A TEST ACCESSOR with no production caller, kept because the by-name lookup
// is what the key-length and routing tests assert over and reaching into
// m.targets from a test would couple them to the field rather than to the
// rule. Production code resolves a path to a LEG (backupRun.legFor) or reads
// the run's anchor leg (runRead.anchor), because both need the manifest,
// sidecar and tally that come with it, not a bare backend.
//
// An unknown name is not reachable from a configuration NewManager accepted —
// it refuses a routing entry naming a target it was not given — so the
// fallback is the ordinary defensive one, not a case with behaviour of its
// own.
func (m *Manager) destination(name string) backupTarget {
	if name != "" && name != m.targetName {
		if t, ok := m.targets[name]; ok {
			return t
		}
	}
	return m.defaultDestination()
}

// routeKey is the storage-root segment a path routes by, with the two
// unwrapped reserved roots handled as scope.go handles them.
//
// Byte-identical to the scope rules, including their edge cases:
//
//   - _schema/<db>/<rest> needs at least THREE segments, because
//     scope.ownsSchemaAnchor requires len(parts) >= 3 and answers false for a
//     two-segment "_schema/x". Arc only ever writes _schema/<db>/<meas>.parquet
//     (internal/fieldschema/anchor.go), so a two-segment key is a foreign one;
//     it routes by "_schema" and therefore lands on the default.
//   - _compaction_state/<tier>/<dbpart>/<job>.json[.quarantined] routes by the
//     FIRST segment of <dbpart>, which is everything between the tier and the
//     last slash and carries a slash for an edge-sync spoke
//     (scope.ownsCompactionState). No tier segment, or no <dbpart>, routes by
//     "_compaction_state" and lands on the default, matching that rule's false.
//
// storage.IsReservedRootDir is NOT an enumeration — it is "starts with _ or
// ." (storage/util.go), deliberately, because edge sync's receive area is
// dot-prefixed. So the rule is: _schema and _compaction_state are unwrapped,
// and every OTHER underscore- or dot-prefixed root routes by its own segment,
// which the databases validation can never match (config refuses a reserved
// root as a database name), so it always lands on the default.
func routeKey(path string) string {
	p := filepath.ToSlash(path)
	first, rest, _ := strings.Cut(p, "/")
	switch first {
	case schemaAnchorDir:
		parts := strings.SplitN(p, "/", 3)
		if len(parts) < 3 {
			return first
		}
		return parts[1]
	case compactionStateDir:
		_, afterTier, ok := strings.Cut(rest, "/")
		if !ok {
			return first
		}
		i := strings.LastIndex(afterTier, "/")
		if i < 0 {
			return first
		}
		dbPart, _, _ := strings.Cut(afterTier[:i], "/")
		return dbPart
	}
	return first
}

// schemaAnchorDir is the reserved root the field schema anchors live under
// (#914), spelled here so this package does not import fieldschema.
const schemaAnchorDir = "_schema"

// A path is routed to a LEG rather than to a bare target, because the leg owns
// the manifest, sidecar and tally the copy also has to reach: see
// backupRun.legFor in backup.go. destination(name) above is the by-name lookup
// that resolution and the key-length tests use.

// configuredTargets is every destination this manager can write to: the
// default first, then the others by name.
//
// Sorted so a listing fan-out, a delete fan-out and every message that names
// several targets are deterministic.
func (m *Manager) configuredTargets() []backupTarget {
	out := make([]backupTarget, 0, len(m.targets)+1)
	out = append(out, m.defaultDestination())
	for _, name := range m.sortedTargetNames() {
		out = append(out, m.targets[name])
	}
	return out
}

// sortedTargetNames are the non-default target names, sorted.
func (m *Manager) sortedTargetNames() []string {
	if len(m.targets) == 0 {
		return nil
	}
	names := make([]string, 0, len(m.targets))
	for name := range m.targets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RoutedDatabases is database → target name for every routed database, read by
// cmd/arc at startup for the ready log. Nil when nothing routes.
func (m *Manager) RoutedDatabases() map[string]string {
	if len(m.routing) == 0 {
		return nil
	}
	out := make(map[string]string, len(m.routing))
	for db, target := range m.routing {
		out[db] = target
	}
	return out
}

// routedDatabasesTo is the routing restricted to a set of target names: what a
// particular RUN acted on, for its index. Nil when nothing in the set is
// routed anything, so a single-destination run index carries no routing at
// all. See the DatabaseTargets comment in planRun.
func (m *Manager) routedDatabasesTo(targets []string) map[string]string {
	if len(m.routing) == 0 || len(targets) == 0 {
		return nil
	}
	want := make(map[string]bool, len(targets))
	for _, name := range targets {
		want[name] = true
	}
	var out map[string]string
	for db, target := range m.routing {
		if !want[target] {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(m.routing))
		}
		out[db] = target
	}
	return out
}

// TargetName is the DEFAULT destination's name, or "" when the destination is
// backup.local_path as it was before targets existed.
func (m *Manager) TargetName() string { return m.targetName }

// TargetNames are every configured target's name, the default first. Empty
// for the backup.local_path destination.
func (m *Manager) TargetNames() []string {
	if m.targetName == "" && len(m.targets) == 0 {
		return nil
	}
	out := make([]string, 0, len(m.targets)+1)
	out = append(out, m.targetName)
	return append(out, m.sortedTargetNames()...)
}

// TargetIsRemote reports whether ANY configured destination is an object store
// (#1085 stage B2b-2). The API layer reads it for the include_config default:
// arc.toml carries every target's credentials, so a local default plus one
// remote routed target still means copying it leaks the keys to that store
// into a backup held in it.
func (m *Manager) TargetIsRemote() bool {
	if m.targetRemote {
		return true
	}
	for _, t := range m.targets {
		if t.remote {
			return true
		}
	}
	return false
}

// buildTargets turns the configured set into the shape the Manager holds: the
// flat fields for the default destination, and a map of the OTHERS.
//
// The map is left NIL for both single-destination shapes — no target
// configured, and exactly one target, where the default must be it and routing
// is a no-op — which is what keeps the single-destination path byte-identical
// to B2b-1's and keeps the backend-swapping tests honest (see
// defaultDestination).
func buildTargets(cfg *ManagerConfig) (spec storage.BackendSpec, def Target, others []Target, err error) {
	switch {
	case len(cfg.Targets) == 0:
		if cfg.DefaultTarget != "" {
			return spec, def, nil, fmt.Errorf("backup default target %s is named but no backup target is configured", cfg.DefaultTarget)
		}
		if len(cfg.Routing) > 0 {
			return spec, def, nil, fmt.Errorf("backup database routing is configured but no backup target is")
		}
		if cfg.BackupPath == "" {
			return spec, def, nil, fmt.Errorf("backup path is required when no backup target is configured")
		}
		return storage.BackendSpec{Type: "local", LocalPath: cfg.BackupPath}, Target{}, nil, nil
	case cfg.DefaultTarget == "":
		return spec, def, nil, fmt.Errorf("a backup default target is required when backup targets are configured")
	}
	byName := make(map[string]Target, len(cfg.Targets))
	for _, t := range cfg.Targets {
		if _, dup := byName[t.Name]; dup {
			return spec, def, nil, fmt.Errorf("backup target %s is configured twice", t.Name)
		}
		byName[t.Name] = t
	}
	def, ok := byName[cfg.DefaultTarget]
	if !ok {
		return spec, def, nil, fmt.Errorf("backup default target %s is not one of the configured targets", cfg.DefaultTarget)
	}
	// A routing entry naming a target that was not configured is a wiring
	// mistake in the caller, not an operator's: config.Load builds both halves
	// from the same block and refuses a database claimed twice. Caught here
	// rather than silently writing that database to the default, which is the
	// one outcome an operator would never detect.
	for db, name := range cfg.Routing {
		if _, ok := byName[name]; !ok {
			return spec, def, nil, fmt.Errorf("database %s is routed to backup target %s, which is not configured", db, name)
		}
	}
	for _, name := range sortedKeys(byName) {
		if name != cfg.DefaultTarget {
			others = append(others, byName[name])
		}
	}
	return def.Spec, def, others, nil
}

// sortedKeys is the deterministic iteration order every message over a target
// set needs.
func sortedKeys(m map[string]Target) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
