package backup

// Backup owner identity (#1085 stage B2b-1).
//
// A local backup directory belongs to exactly one Arc instance because it is a
// directory on that instance's disk. A remote target does not: two instances
// pointed at one bucket and prefix would merge listings, and an operator
// restoring "the latest backup" could restore the other instance's data over
// their own. The manifest therefore records who wrote it and the listing
// compares.
//
// Identity is the CLUSTER, not the node. A per-node identity would make every
// writer failover orphan its own cluster's backups, because the new primary
// would read every earlier manifest as foreign. So on a cluster it is
// cluster.cluster_name — whose default, "arc-cluster", means two unrelated
// clusters that both leave it at the default collide, which the docs must say.
// Standalone there is no cluster name, so a UUID is minted and persisted.
//
// WHERE the standalone UUID is persisted is a decision, not an accident: a
// file beside the shared SQLite database, NOT a row inside it. A restore must
// not adopt the identity of the backup it restored — disaster recovery onto
// fresh hardware is the point of backups, and an instance that adopted the
// identity in a restored manifest would then claim the source instance's
// backups as its own. The shared database IS backed up and restored
// (backupSQLite / the pending-restore apply at boot), so an identity stored in
// it would travel with a restore and have to be unwound by boot ordering
// around ApplyPendingRestores.
//
// BY WHAT MECHANISM the sidecar escapes that, stated precisely because the
// obvious answer is wrong. It is NOT "the file sits outside the backed-up
// tree": an operator is free to put auth.db inside the storage root, and then
// this file sits in it too. Three independent things keep it out of a backup,
// in the order they apply:
//
//  1. THE LEADING DOT, which is why InstanceIDFileName has one. No listing in
//     internal/storage returns a dot-prefixed name — the rule exists to hide
//     Arc's own in-flight ".arc-*.tmp" writes — so the file is not even
//     enumerated. This is the decisive one.
//  2. The inventory classifier (CreateBackup), which keeps only compaction
//     state, ".parquet" objects and Iceberg metadata, so a non-Parquet name
//     that did reach it would still be dropped.
//  3. A restore writes only keys the backup contains, so nothing it writes can
//     land on this path even if the file were somehow present.
//
// Because the property has three guards, no single-site mutation breaks it;
// TestTheInstanceIdentitySurvivesABackupAndRestoreFromInsideTheStorageRoot
// characterises the outcome rather than any one of them.
//
// The echo of the backup's owner id in the listing, the restore log and the
// delete log is what tells the operator those backups belong to another
// instance.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// InstanceIDFileName is the file, beside the shared SQLite database, that holds
// a standalone instance's backup owner identity.
const InstanceIDFileName = ".arc-backup-instance-id"

// LoadOrCreateInstanceID returns the persistent backup owner identity stored
// beside dbPath, minting and persisting one on first call.
//
// dbPath is the shared SQLite database path; the identity file is its
// SIBLING, never its content (see the package comment above). 0600 and 0700
// because the directory already holds auth tokens and audit logs, and because
// nothing here should be the loosest thing in that directory.
//
// A caller that gets an error must run on with an empty identity rather than
// refuse to start: an unidentified instance writes manifests without an owner
// and reads every manifest as its own, which is exactly today's behaviour.
func LoadOrCreateInstanceID(dbPath string) (string, error) {
	if strings.TrimSpace(dbPath) == "" {
		return "", fmt.Errorf("cannot resolve the backup instance identity without a database path")
	}
	dir := filepath.Dir(dbPath)
	path := filepath.Join(dir, InstanceIDFileName)

	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
		// An empty or whitespace-only file is a truncated write from a crash
		// between create and write. Mint again rather than return "": "" is
		// not an identity, it is the absence of one, and silently running
		// unidentified is the failure this file exists to prevent.
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to read the backup instance identity: %w", err)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("failed to create the directory for the backup instance identity: %w", err)
	}
	id := uuid.New().String()
	// WriteFile with 0600 does not narrow an existing file's mode, so chmod
	// after it for the case where a previous boot created it more loosely.
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("failed to persist the backup instance identity: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return "", fmt.Errorf("failed to restrict the backup instance identity file: %w", err)
	}
	return id, nil
}

// ownsManifest reports whether a manifest was written by this instance.
//
// A manifest with NO owner reads as this instance's own, and that rule is load
// bearing on upgrade: every backup that exists today was written before this
// field did, so treating an absent owner as foreign would hide every one of
// them from the listing the moment Arc is upgraded. Only a present-AND-
// different owner is foreign.
//
// An instance with no identity of its own also owns everything it can see:
// it cannot distinguish its own backups from anyone else's, and hiding
// backups on the strength of a comparison it cannot make would be the same
// upgrade failure by another route.
func (m *Manager) ownsManifest(manifest *Manifest) bool {
	if manifest == nil {
		return true
	}
	return m.ownsInstanceID(manifest.OwnerInstanceID)
}

// ownsInstanceID is ownsManifest's rule applied to a bare owner id, for the
// run index of an aborted run, which has no manifest to read it from.
func (m *Manager) ownsInstanceID(owner string) bool {
	if owner == "" || m.instanceID == "" {
		return true
	}
	return owner == m.instanceID
}
