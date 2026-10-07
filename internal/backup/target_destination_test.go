package backup

// Tests for the configurable backup destination (#1085 stage B2b-1): target
// construction, per-target key headroom, prefix-scoped listing and owner
// identity.
//
// Every expected byte count below is spelled as a LITERAL rather than
// recomputed from the constants the production code uses. A test that asks the
// implementation what the answer should be cannot disagree with it: the
// existing state-key test computes "max_source_key_bytes" as
// storage.MaxUsableKeyLen-backupDataKeyHeadroom, which is the figure the code
// prints, so it would have passed unchanged against a destination whose real
// limit was lower by the target prefix. The numbers here are
// 1024 (MaxKeyLen) - 5 (".part") - 37 ("backup-20060102-150405-12345678/data/")
// = 982 with no target prefix, and 978 with the four-byte prefix "arc/". A
// source key of 979 bytes therefore stores as 4 + 37 + 979 = 1020 bytes, one
// over the 1019-byte limit.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// localTarget is a Target whose backend is a directory, carrying a KeyPrefix
// an object-store target would carry.
//
// The prefix is SUPPLIED rather than derived, which is Target.KeyPrefix's
// documented contract (cmd/arc/main.go asks the config for it), so a local
// backend with a declared prefix exercises exactly the arithmetic a remote
// target exercises without a network or a credential. The alternative — a real
// S3 target — cannot run in a unit test: NewS3Backend probes the bucket with a
// ten-second timeout.
func localTarget(dir, name, keyPrefix string, remote bool) Target {
	return Target{
		Name:      name,
		Spec:      storage.BackendSpec{Type: "local", LocalPath: dir},
		KeyPrefix: keyPrefix,
		Remote:    remote,
	}
}

// oneTarget is the single-target configuration: the shape in which
// NewManager leaves Manager.targets NIL, so every fake these tests swap into
// m.backupStorage is still the destination every operation uses.
func oneTarget(dir, name, keyPrefix string, remote bool) []Target {
	return []Target{localTarget(dir, name, keyPrefix, remote)}
}

// TestNewManagerDoesNotNeedALocalPathWithATarget is the headline cell of this
// change: backup.local_path stops being required. Nothing in the backup or
// restore path uses it as scratch, so a deployment that backs up to an object
// store should not have to name a directory it will never write to.
func TestNewManagerDoesNotNeedALocalPathWithATarget(t *testing.T) {
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })

	targetDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{
		DataStorage:   dataStorage,
		BackupPath:    "", // deliberately unset
		Targets:       oneTarget(targetDir, "audit", "", false),
		DefaultTarget: "audit",
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewManager with a target and no BackupPath = %v, want success", err)
	}
	if m.TargetName() != "audit" {
		t.Errorf("TargetName() = %q, want \"audit\"", m.TargetName())
	}

	// And still required when there is no target, because then it IS the
	// destination.
	if _, err := NewManager(&ManagerConfig{DataStorage: dataStorage, Logger: zerolog.Nop()}); err == nil {
		t.Error("NewManager with neither a target nor a BackupPath succeeded, want an error")
	} else if !strings.Contains(err.Error(), "backup path is required") {
		t.Errorf("error = %q, want it to mention that a backup path is required", err.Error())
	}
}

// TestATargetLeavesTheLocalBackupDirectoryUncreated: LocalBackend's
// constructor MkdirAlls its root, so building one for an unused
// backup.local_path would create ./data/backups at every boot of a deployment
// that backs up to an object store.
func TestATargetLeavesTheLocalBackupDirectoryUncreated(t *testing.T) {
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })

	root := t.TempDir()
	unused := filepath.Join(root, "unused-local-backups")
	if _, err := NewManager(&ManagerConfig{
		DataStorage:   dataStorage,
		BackupPath:    unused,
		Targets:       oneTarget(filepath.Join(root, "target"), "audit", "", false),
		DefaultTarget: "audit",
		Logger:        zerolog.Nop(),
	}); err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if _, err := os.Stat(unused); !os.IsNotExist(err) {
		t.Errorf("os.Stat(%q) err = %v, want os.IsNotExist: a configured target must not build a backend for the unused local path", unused, err)
	}
}

// TestATargetKeyPrefixThatCannotHoldTheBackupsOwnKeysIsRefused is the edge case
// of the headroom arithmetic this change introduced, and it is here because the
// first version of that arithmetic produced a NEGATIVE
// max_source_key_bytes — measured: a 994-byte prefix made both overlong-key
// messages read "source keys longer than -12 bytes", and the manifest and file
// sidecar writes then failed from inside the backend with the store's own
// KeyTooLongError, naming neither the target nor the prefix.
//
// The refusal is a NewManager error rather than a load-time one, which puts it
// on the existing degrade path: cmd/arc/main.go logs it at Error and skips the
// backup API, the same severity an unwritable local backup directory gets,
// because an over-long prefix is permanent rather than transient.
func TestATargetKeyPrefixThatCannotHoldTheBackupsOwnKeysIsRefused(t *testing.T) {
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })

	// 968, as a literal. 1019 usable key bytes minus the 51 of
	// "backup-20060102-150405-12345678/manifest-files.json".
	if MaxTargetKeyPrefixLen != 968 {
		t.Fatalf("MaxTargetKeyPrefixLen = %d, want 968", MaxTargetKeyPrefixLen)
	}

	build := func(prefixLen int) (*Manager, error) {
		// A legal prefix of exactly prefixLen bytes: 200-byte segments, since
		// a single segment is capped well below this.
		var sb strings.Builder
		for sb.Len()+201 <= prefixLen {
			sb.WriteString(strings.Repeat("a", 200))
			sb.WriteString("/")
		}
		sb.WriteString(strings.Repeat("b", prefixLen-sb.Len()-1))
		sb.WriteString("/")
		prefix := sb.String()
		if len(prefix) != prefixLen {
			t.Fatalf("built a %d-byte prefix, want %d", len(prefix), prefixLen)
		}
		if _, err := storage.ValidateObjectPrefix(strings.TrimSuffix(prefix, "/")); err != nil {
			t.Fatalf("a %d-byte prefix must still pass ValidateObjectPrefix, which is the point: %v", prefixLen, err)
		}
		return NewManager(&ManagerConfig{
			DataStorage:   dataStorage,
			Targets:       oneTarget(t.TempDir(), "audit", prefix, true),
			DefaultTarget: "audit",
			Logger:        zerolog.Nop(),
		})
	}

	_, err = build(969)
	if err == nil {
		t.Fatal("NewManager accepted a 969-byte target prefix, want a refusal")
	}
	for _, want := range []string{
		"backup target audit", // which target
		"is 969 bytes",        // what it is
		"968-byte maximum",    // what the limit is
		"up to 51 bytes",      // why
		"1019-byte object name limit",
		"Shorten the prefix", // the remedy
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not contain %q; got: %s", want, err.Error())
		}
	}

	// And the largest ACCEPTED prefix must still leave a positive figure, or
	// the bound is off by one in the direction that prints nonsense.
	m, err := build(968)
	if err != nil {
		t.Fatalf("NewManager refused a 968-byte prefix, which is exactly the maximum: %v", err)
	}
	if got := m.defaultDestination().maxSourceKeyBytes(); got != 14 {
		t.Errorf("maxSourceKeyBytes() at the maximum prefix = %d, want 14 (1019 - 968 - 37)", got)
	}
	if m.defaultDestination().maxSourceKeyBytes() <= 0 {
		t.Error("maxSourceKeyBytes() is not positive at the largest accepted prefix; the messages would report a negative byte count")
	}
}

// TestIcebergContainmentWarningFollowsTheDestination covers
// localDestinationPath, which shipped with no coverage at all: a mutation
// reverting it to resolveExistingPath(cfg.BackupPath) left the whole package
// green.
//
// Two claims, one per arm. With a REMOTE target there is no containment
// question — an object store cannot contain a directory on this machine — and
// asking it of cfg.BackupPath would be worse than useless, because BackupPath
// keeps its "./data/backups" default and is not the destination, and
// resolveExistingPath("") resolves to the WORKING DIRECTORY, which contains
// almost everything. With a LOCAL target the warning must name the TARGET's
// directory, not the ignored BackupPath.
func TestIcebergContainmentWarningFollowsTheDestination(t *testing.T) {
	// A warehouse that CONTAINS the backup directory is the shape that warns.
	root := t.TempDir()
	warehouse := filepath.Join(root, "warehouse")
	insideWarehouse := filepath.Join(warehouse, "backups")
	if err := os.MkdirAll(insideWarehouse, 0o700); err != nil {
		t.Fatal(err)
	}

	newWith := func(t *testing.T, targets []Target, backupPath string) string {
		defaultTarget := ""
		if len(targets) > 0 {
			defaultTarget = targets[0].Name
		}
		t.Helper()
		dataStorage, err := storage.NewLocalBackend(filepath.Join(t.TempDir(), "data"), zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { dataStorage.Close() })
		var logOutput bytes.Buffer
		if _, err := NewManager(&ManagerConfig{
			DataStorage:          dataStorage,
			BackupPath:           backupPath,
			Targets:              targets,
			DefaultTarget:        defaultTarget,
			IcebergWarehousePath: warehouse,
			Logger:               zerolog.New(&logOutput),
		}); err != nil {
			t.Fatalf("NewManager: %v", err)
		}
		return logOutput.String()
	}

	t.Run("no target: the warning names backup.local_path", func(t *testing.T) {
		logs := newWith(t, nil, insideWarehouse)
		if !strings.Contains(logs, "contains the backup directory") {
			t.Fatalf("no containment warning for a backup directory inside the warehouse; got: %s", logs)
		}
		if !strings.Contains(logs, insideWarehouse) {
			t.Errorf("warning does not name %q; got: %s", insideWarehouse, logs)
		}
	})

	t.Run("local target: the warning names the TARGET path", func(t *testing.T) {
		// BackupPath points somewhere harmless and is ignored; the target is
		// the directory inside the warehouse.
		elsewhere := filepath.Join(root, "ignored-local-path")
		logs := newWith(t, oneTarget(insideWarehouse, "nearby", "", false), elsewhere)
		if !strings.Contains(logs, "contains the backup directory") {
			t.Fatalf("no containment warning for a local TARGET inside the warehouse; got: %s", logs)
		}
		if !strings.Contains(logs, insideWarehouse) {
			t.Errorf("warning does not name the target directory %q; got: %s", insideWarehouse, logs)
		}
		if strings.Contains(logs, elsewhere) {
			t.Errorf("warning names the ignored backup.local_path %q; the destination is the target; got: %s", elsewhere, logs)
		}
	})

	t.Run("remote target: no containment warning at all", func(t *testing.T) {
		// BOTH paths point inside the warehouse, so only the Remote guard can
		// keep this silent. That is not a contrived shape: a remote target
		// carries whatever backup.targets.<name>.local_path holds, because
		// validateBackupTargets never clears the keys the chosen type does not
		// read, so "type = s3" beside a leftover local_path is an ordinary
		// half-edited config. An object store cannot contain a directory on
		// this machine, so the answer is silence either way.
		logs := newWith(t, oneTarget(insideWarehouse, "audit", "", true), insideWarehouse)
		if strings.Contains(logs, "contains the backup directory") {
			t.Errorf("a remote target produced a backup-directory containment warning about a path nothing writes to; got: %s", logs)
		}
	})

	t.Run("remote target with no BackupPath: the working directory is not the destination", func(t *testing.T) {
		// resolveExistingPath("") answers the WORKING DIRECTORY. A warehouse
		// under it would then warn about a path that is not a destination at
		// all, which is the hazard the "" guard exists for.
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		under := filepath.Join(cwd, "arc-b2b-warehouse-probe")
		if err := os.MkdirAll(under, 0o700); err != nil {
			t.Skipf("cannot create a probe directory under the working directory: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(under) })

		dataStorage, err := storage.NewLocalBackend(filepath.Join(t.TempDir(), "data"), zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { dataStorage.Close() })
		var logOutput bytes.Buffer
		if _, err := NewManager(&ManagerConfig{
			DataStorage:          dataStorage,
			BackupPath:           "",
			Targets:              oneTarget(t.TempDir(), "audit", "", true),
			DefaultTarget:        "audit",
			IcebergWarehousePath: cwd,
			Logger:               zerolog.New(&logOutput),
		}); err != nil {
			t.Fatalf("NewManager: %v", err)
		}
		if strings.Contains(logOutput.String(), "contains the backup directory") {
			t.Errorf("an empty BackupPath resolved to the working directory and produced a containment warning; got: %s", logOutput.String())
		}
	})
}

// overlongKeyForHeadroom builds a legal source key that is exactly one byte
// too long for a destination with the given headroom.
func overlongKeyForHeadroom(t *testing.T, sourceLimit int) string {
	t.Helper()
	prefix := "db/cpu/" + strings.Repeat(strings.Repeat("a", 200)+"/", 4)
	tailLen := sourceLimit + 1 - len(prefix) - len(".parquet")
	if tailLen <= 0 || tailLen+len(".parquet") > storage.MaxUsableKeySegmentLen {
		t.Fatalf("test key segment length %d is unusable", tailLen)
	}
	key := prefix + strings.Repeat("z", tailLen) + ".parquet"
	if len(key) != sourceLimit+1 {
		t.Fatalf("built key is %d bytes, want %d", len(key), sourceLimit+1)
	}
	if err := storage.ValidateKey(key); err != nil {
		t.Fatalf("source key must be legal for the storage contract: %v", err)
	}
	return key
}

// headroomInventory presents one extra, readable, over-long data key.
type headroomInventory struct {
	storage.Backend
	longKey string
}

func (h headroomInventory) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := h.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(h.longKey, prefix) {
		objects = append(objects, storage.ObjectInfo{Path: h.longKey, Size: 4, LastModified: time.Now()})
	}
	return objects, nil
}

func (h headroomInventory) ReadTo(ctx context.Context, path string, dst io.Writer) error {
	if path == h.longKey {
		_, err := io.WriteString(dst, "PAR1")
		return err
	}
	return h.Backend.ReadTo(ctx, path, dst)
}

// TestDataKeyHeadroomShrinksByTheTargetPrefix is the per-target reservation,
// through the data-file message.
//
// The source key here is 978+1 bytes: legal for the storage contract, legal
// for a backup with no target prefix, and one byte too long for a target whose
// prefix is "arc/". Before this change the key was copied, the stored object
// name was 1024-5+4 bytes, and the write failed from inside WriteReader with a
// generic storage error — the promise of 982 usable bytes was false by exactly
// the length of the prefix, in the direction that fails the run rather than
// reporting a skip.
func TestDataKeyHeadroomShrinksByTheTargetPrefix(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	// Enough ordinary files that one skip stays under the skip ratio.
	for i := 0; i < 20; i++ {
		if err := dataStorage.Write(ctx, fmt.Sprintf("db/cpu/2026/10/07/00/good-%02d.parquet", i), []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	longKey := overlongKeyForHeadroom(t, 978)

	var logOutput bytes.Buffer
	m, err := NewManager(&ManagerConfig{
		DataStorage:   headroomInventory{Backend: dataStorage, longKey: longKey},
		Targets:       oneTarget(t.TempDir(), "audit", "arc/", true),
		DefaultTarget: "audit",
		Logger:        zerolog.New(&logOutput),
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := m.defaultDestination().maxSourceKeyBytes(); got != 978 {
		t.Errorf("maxSourceKeyBytes() with the prefix \"arc/\" = %d, want 978", got)
	}

	result, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if result.Manifest.SkippedOverlongKeys != 1 {
		t.Fatalf("manifest skipped_overlong_keys = %d, want 1: the key is one byte too long for a target with the prefix \"arc/\"", result.Manifest.SkippedOverlongKeys)
	}
	logs := logOutput.String()
	// The figure, as a literal. A bare "it warned" check would pass against
	// the pre-change message, which carried the same field names and the wrong
	// number.
	for _, want := range []string{
		`"max_source_key_bytes":978`,
		`"destination_key_bytes":1020`,
		`"maximum_key_bytes":1019`,
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("backup log does not contain %s; got: %s", want, logs)
		}
	}
	if strings.Contains(logs, `"max_source_key_bytes":982`) {
		t.Errorf("backup log reports 982 usable source bytes, which is the figure for a target with NO prefix; got: %s", logs)
	}
}

// longStateInventory presents one over-long compaction-recovery-state key.
type longStateInventory struct {
	storage.Backend
	longKey string
}

func (l longStateInventory) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := l.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(l.longKey, prefix) {
		objects = append(objects, storage.ObjectInfo{Path: l.longKey, Size: 4, LastModified: time.Now()})
	}
	return objects, nil
}

func (l longStateInventory) ReadTo(ctx context.Context, path string, dst io.Writer) error {
	if path == l.longKey {
		_, err := io.WriteString(dst, "PAR1")
		return err
	}
	return l.Backend.ReadTo(ctx, path, dst)
}

// TestStateKeyHeadroomShrinksByTheTargetPrefix is the same reservation through
// the OTHER message — the fatal one for compaction recovery state, whose
// omission would let a restore serve every row of a partition twice (#930).
// Two call sites report the figure, so two tests report it.
func TestStateKeyHeadroomShrinksByTheTargetPrefix(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	if err := dataStorage.Write(ctx, "db/cpu/2026/10/07/00/good.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}

	// 978+1 bytes, under compactionStateDir so copyStateFiles handles it.
	suffix := overlongKeyForHeadroom(t, 978-len(compactionStateDir+"/"))
	longKey := compactionStateDir + "/" + suffix

	m, err := NewManager(&ManagerConfig{
		DataStorage:   longStateInventory{Backend: dataStorage, longKey: longKey},
		Targets:       oneTarget(t.TempDir(), "audit", "arc/", true),
		DefaultTarget: "audit",
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("CreateBackup succeeded; an uncopyable compaction recovery manifest must fail the run (#930)")
	}
	msg := err.Error()
	for _, want := range []string{
		"max_source_key_bytes=978",
		"destination_key_bytes=1020",
		"maximum_key_bytes=1019",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error does not contain %q; got: %s", want, msg)
		}
	}
	if strings.Contains(msg, "max_source_key_bytes=982") {
		t.Errorf("error reports 982 usable source bytes, the figure for a target with NO prefix; got: %s", msg)
	}
}

// TestNoTargetKeepsTheUnprefixedHeadroom is the other side of the same
// arithmetic: with no target the figure is unchanged, which is what makes the
// untouched existing key-length suites a regression proof rather than a
// coincidence.
func TestNoTargetKeepsTheUnprefixedHeadroom(t *testing.T) {
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	m, err := NewManager(&ManagerConfig{
		DataStorage: dataStorage,
		BackupPath:  t.TempDir(),
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.defaultDestination().maxSourceKeyBytes(); got != 982 {
		t.Errorf("maxSourceKeyBytes() with no target = %d, want 982", got)
	}
	if got := m.defaultDestination().keyHeadroom(); got != 37 {
		t.Errorf("destinationKeyHeadroom() with no target = %d, want 37", got)
	}
}

// newBackupAt makes a Manager writing into dir with the given owner identity.
func newBackupAt(t *testing.T, dir, instanceID string) *Manager {
	t.Helper()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	if err := dataStorage.Write(context.Background(), "db/cpu/2026/10/07/00/a.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(&ManagerConfig{
		DataStorage: dataStorage,
		BackupPath:  dir,
		InstanceID:  instanceID,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// writeManifest drops a manifest at <id>/manifest.json in dir, as a backup
// written by another instance (or by an older Arc) would appear.
func writeManifest(t *testing.T, dir, id string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, id), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, id, "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestListBackupsOnlyConsidersBackupIDDirectories: the listing used to keep
// every key ending in "/manifest.json" anywhere under the destination. On a
// local directory the backup owned that was unambiguous; on a shared remote
// prefix it is a false-positive magnet — including for the cold tier, whose
// prefix comes from the same kind of config block.
func TestListBackupsOnlyConsidersBackupIDDirectories(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := newBackupAt(t, dir, "")

	result, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	realID := result.Manifest.BackupID

	// A foreign manifest.json under the same destination, at the depth the old
	// suffix match looked at. Valid JSON, so it would have parsed and been
	// listed as a backup with an empty id.
	writeManifest(t, dir, "someone-elses-tool", []byte(`{"version":"1","backup_id":"not-a-backup"}`))
	// And a nested one, which the recursive walk also reached.
	if err := os.MkdirAll(filepath.Join(dir, "iceberg", "arc_db.db", "cpu", "metadata"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "iceberg", "arc_db.db", "cpu", "metadata", "manifest.json"), []byte(`{"version":"1","backup_id":"nested"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// A directory whose name is nearly a backup id but is not one.
	writeManifest(t, dir, "backup-2026100-070000-abcdef01", []byte(`{"version":"1","backup_id":"malformed"}`))

	summaries, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(summaries) != 1 || summaries[0].BackupID != realID {
		ids := make([]string, 0, len(summaries))
		for _, s := range summaries {
			ids = append(ids, s.BackupID)
		}
		t.Fatalf("ListBackups returned %v, want exactly [%s]", ids, realID)
	}
}

// manifestFaultDestination makes a candidate backup's manifest unreadable or
// malformed while leaving listing and every other object operation available.
type manifestFaultDestination struct {
	*storage.LocalBackend
	path string
	data []byte
	err  error
}

func (b *manifestFaultDestination) Read(ctx context.Context, path string) ([]byte, error) {
	if path == b.path {
		return b.data, b.err
	}
	return b.LocalBackend.Read(ctx, path)
}

// A directory listing can succeed while every manifest fetch fails. Returning
// an empty successful result would tell an operator their backups are gone
// rather than unavailable. A malformed commit record is equally unreadable to
// the listing and must be surfaced too.
func TestListBackupsDoesNotReportEmptyWhenManifestIsUnreadable(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		err  error
	}{
		{name: "read failure", err: errors.New("simulated manifest read failure")},
		{name: "malformed manifest", data: []byte("not json")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := newBackupAt(t, t.TempDir(), "")
			result, err := m.CreateBackup(ctx, BackupOptions{})
			if err != nil {
				t.Fatalf("CreateBackup: %v", err)
			}

			m.backupStorage = &manifestFaultDestination{
				LocalBackend: m.backupStorage.(*storage.LocalBackend),
				path:         result.Manifest.BackupID + "/manifest.json",
				data:         tc.data,
				err:          tc.err,
			}
			_, err = m.ListBackups(ctx)
			if err == nil {
				t.Fatal("ListBackups succeeded after a candidate manifest could not be read; want an error rather than an incomplete answer")
			}
			if !strings.Contains(err.Error(), result.Manifest.BackupID+"/manifest.json") {
				t.Errorf("ListBackups error = %q, want it to name the unreadable manifest", err)
			}
		})
	}
}

// countingDestination records which listing call the manager makes.
type countingDestination struct {
	storage.Backend
	lists int
	dirs  int
}

func (c *countingDestination) List(ctx context.Context, prefix string) ([]string, error) {
	c.lists++
	return c.Backend.List(ctx, prefix)
}

func (c *countingDestination) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	c.dirs++
	return c.Backend.(storage.DirectoryLister).ListDirectories(ctx, prefix)
}

// noDirectoryLister hides ListDirectories, the way a test fake that embeds
// storage.Backend does, so the fallback path is reachable.
type noDirectoryLister struct {
	storage.Backend
	lists int
}

func (n *noDirectoryLister) List(ctx context.Context, prefix string) ([]string, error) {
	n.lists++
	return n.Backend.List(ctx, prefix)
}

// TestListBackupsDoesNotWalkTheWholeDestination: the listing used to call
// List(ctx, "") and keep every key ending in "/manifest.json". On a local
// directory the backup owned that was a cheap walk of a directory nothing else
// writes to. On a remote target it is a full recursive enumeration of the
// configured prefix, which may hold the cold tier, and it grows with the SIZE
// of the backups rather than their number.
//
// Note why the obvious fix is not available: List's prefix argument does not
// mean the same thing on every backend. S3Backend hands it to ListObjectsV2 as
// a key prefix, while LocalBackend resolves it to a DIRECTORY and walks that,
// so List(ctx, "backup-") would return every backup on S3 and nothing at all
// on a local directory. ListDirectories is the one call whose meaning is the
// same on all three.
func TestListBackupsDoesNotWalkTheWholeDestination(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := newBackupAt(t, dir, "")
	if _, err := m.CreateBackup(ctx, BackupOptions{}); err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	counting := &countingDestination{Backend: m.backupStorage}
	m.backupStorage = counting
	summaries, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("ListBackups returned %d entries, want 1", len(summaries))
	}
	if counting.dirs == 0 {
		t.Error("ListBackups made no ListDirectories call, want at least one")
	}
	if counting.lists != 0 {
		t.Errorf("ListBackups made %d List calls, want 0: a destination-wide recursive walk is what this replaced", counting.lists)
	}

	// A backend that cannot list directories still has to work, because a
	// test fake that embeds storage.Backend does not implement the optional
	// interface. The fallback walks, and filters by the same id shape.
	fallback := &noDirectoryLister{Backend: counting.Backend}
	m.backupStorage = fallback
	summaries, err = m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups via the fallback: %v", err)
	}
	if len(summaries) != 1 {
		t.Errorf("fallback ListBackups returned %d entries, want 1", len(summaries))
	}
	if fallback.lists == 0 {
		t.Error("the fallback made no List call, want at least one")
	}
}

// TestListBackupsSkipsADirectoryWithNoManifest: the sidecar is written before
// the manifest precisely so a run that died between the two shows nothing, and
// an in-flight backup has a directory and no manifest. Neither may be listed,
// and neither may log a warning — the status endpoint is polled during a
// backup, and a warning per poll is an alert an operator learns to ignore.
func TestListBackupsSkipsADirectoryWithNoManifest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var logOutput bytes.Buffer
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	m, err := NewManager(&ManagerConfig{
		DataStorage: dataStorage,
		BackupPath:  dir,
		Logger:      zerolog.New(&logOutput),
	})
	if err != nil {
		t.Fatal(err)
	}

	// A well-formed backup id with data under it and no manifest: exactly what
	// a run that died before its commit record leaves.
	if err := os.MkdirAll(filepath.Join(dir, "backup-20261007-120000-deadbeef", "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "backup-20261007-120000-deadbeef", "data", "a.parquet"), []byte("PAR1"), 0o600); err != nil {
		t.Fatal(err)
	}

	summaries, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(summaries) != 0 {
		t.Errorf("ListBackups returned %d entries, want 0: a directory without a manifest is not a backup", len(summaries))
	}
	if logs := logOutput.String(); strings.Contains(logs, "Failed to read manifest") {
		t.Errorf("ListBackups logged a read failure for an uncommitted backup; got: %s", logs)
	}
}

// TestBackupRecordsItsTargetAndOwner: the two new manifest fields, written by
// the run and echoed into the listing.
func TestBackupRecordsItsTargetAndOwner(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	if err := dataStorage.Write(ctx, "db/cpu/2026/10/07/00/a.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(&ManagerConfig{
		DataStorage:   dataStorage,
		Targets:       oneTarget(t.TempDir(), "audit", "", false),
		DefaultTarget: "audit",
		InstanceID:    "cluster-alpha",
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if result.Manifest.Target != "audit" {
		t.Errorf("manifest target = %q, want \"audit\"", result.Manifest.Target)
	}
	if result.Manifest.OwnerInstanceID != "cluster-alpha" {
		t.Errorf("manifest owner_instance_id = %q, want \"cluster-alpha\"", result.Manifest.OwnerInstanceID)
	}
	summaries, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("ListBackups returned %d entries, want 1", len(summaries))
	}
	if summaries[0].Target != "audit" || summaries[0].OwnerInstanceID != "cluster-alpha" {
		t.Errorf("summary target/owner = %q/%q, want \"audit\"/\"cluster-alpha\"", summaries[0].Target, summaries[0].OwnerInstanceID)
	}
	if summaries[0].ForeignOwner {
		t.Error("summary foreign_owner = true for this instance own backup, want false")
	}
}

// TestListBackupsFiltersAForeignOwner is the reason the owner field exists:
// two instances sharing one bucket and prefix must not merge listings.
//
// The companion assertions are the ones that keep the filter from being
// self-locking: ListAllBackups still returns it with its owner echoed, and
// GetBackup still reads it, because restoring onto fresh hardware is what
// backups are for.
func TestListBackupsFiltersAForeignOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const foreignID = "backup-20260101-010101-aaaaaaaa"
	writeManifest(t, dir, foreignID, []byte(`{"version":"1","backup_id":"`+foreignID+`","created_at":"2026-01-01T01:01:01Z","backup_type":"full","databases":[],"total_files":1,"total_size_bytes":4,"owner_instance_id":"the-other-instance"}`))

	m := newBackupAt(t, dir, "this-instance")

	own, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(own) != 0 {
		t.Errorf("ListBackups returned %d entries, want 0: the only backup belongs to another instance", len(own))
	}

	all, err := m.ListAllBackups(ctx)
	if err != nil {
		t.Fatalf("ListAllBackups: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("ListAllBackups returned %d entries, want 1", len(all))
	}
	if !all[0].ForeignOwner {
		t.Error("ListAllBackups entry foreign_owner = false, want true")
	}
	if all[0].OwnerInstanceID != "the-other-instance" {
		t.Errorf("entry owner_instance_id = %q, want \"the-other-instance\" echoed", all[0].OwnerInstanceID)
	}

	if _, err := m.GetBackup(ctx, foreignID); err != nil {
		t.Errorf("GetBackup on a foreign backup = %v, want success: a refusal here would be self-locking after a restore onto fresh hardware", err)
	}
}

// TestAManifestWithNoOwnerReadsAsOwn is the upgrade rule, and it is the one
// that bites if it is wrong: every backup that exists today was written before
// the owner field did, so treating an absent owner as foreign would hide every
// one of them from the listing at the moment Arc is upgraded.
//
// The manifest below is a LITERAL pre-B2b-1 document rather than one this code
// produced, because a manifest this code produced would carry whatever field
// set this code currently writes.
func TestAManifestWithNoOwnerReadsAsOwn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const oldID = "backup-20260501-120000-0123abcd"
	writeManifest(t, dir, oldID, []byte(`{
  "version": "dev",
  "backup_id": "`+oldID+`",
  "created_at": "2026-05-01T12:00:00Z",
  "backup_type": "full",
  "databases": [{"name": "db", "measurements": [{"name": "cpu", "file_count": 1, "size_bytes": 4}], "file_count": 1, "size_bytes": 4}],
  "total_files": 1,
  "total_size_bytes": 4
}`))

	m := newBackupAt(t, dir, "an-identity-this-instance-minted-after-the-upgrade")
	summaries, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(summaries) != 1 || summaries[0].BackupID != oldID {
		t.Fatalf("ListBackups returned %+v, want the pre-upgrade backup %s listed as this instance own", summaries, oldID)
	}
	if summaries[0].ForeignOwner {
		t.Error("a manifest with no owner is marked foreign_owner, want false: that would hide every backup taken before the field existed")
	}
}

// TestAnUnidentifiedInstanceOwnsEverythingItCanSee: the same upgrade failure
// by the other route. An instance that could not resolve an identity cannot
// tell its own backups from anyone else's, so hiding backups on the strength
// of a comparison it cannot make would be worse than making none.
func TestAnUnidentifiedInstanceOwnsEverythingItCanSee(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const foreignID = "backup-20260101-010101-bbbbbbbb"
	writeManifest(t, dir, foreignID, []byte(`{"version":"1","backup_id":"`+foreignID+`","created_at":"2026-01-01T01:01:01Z","backup_type":"full","databases":[],"total_files":0,"total_size_bytes":0,"owner_instance_id":"somebody"}`))

	m := newBackupAt(t, dir, "") // no identity
	summaries, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("ListBackups returned %d entries, want 1: an instance with no identity must not hide backups", len(summaries))
	}
	if summaries[0].ForeignOwner {
		t.Error("an unidentified instance marked a backup foreign_owner, want false")
	}
}

// TestANewBackupCarriesTheNewIdentityAfterAChangeOfOwner is the matrix row for
// "restore onto fresh hardware, then back up again", which is what makes
// "foreign-owner restore is allowed with an echo" a tested claim rather than
// an asserted one.
//
// The replacement instance keeps the identity it has: the backup it restored
// from still reads as foreign, and the backup it then takes carries the NEW
// identity. An instance that adopted the restored owner id would claim the
// source instance's backups as its own, and two live instances would then
// write into one listing under one name.
func TestANewBackupCarriesTheNewIdentityAfterAChangeOfOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const oldOwnersBackup = "backup-20260101-010101-cccccccc"
	writeManifest(t, dir, oldOwnersBackup, []byte(`{"version":"1","backup_id":"`+oldOwnersBackup+`","created_at":"2026-01-01T01:01:01Z","backup_type":"full","databases":[],"total_files":0,"total_size_bytes":0,"owner_instance_id":"the-decommissioned-instance"}`))

	fresh := newBackupAt(t, dir, "the-replacement-instance")
	result, err := fresh.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}
	if result.Manifest.OwnerInstanceID != "the-replacement-instance" {
		t.Errorf("the new backup owner = %q, want \"the-replacement-instance\": a restore must not make this instance a continuation of the old one",
			result.Manifest.OwnerInstanceID)
	}

	all, err := fresh.ListAllBackups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foreign := map[string]bool{}
	for _, s := range all {
		foreign[s.BackupID] = s.ForeignOwner
	}
	if len(all) != 2 {
		t.Fatalf("ListAllBackups returned %d entries, want 2", len(all))
	}
	if !foreign[oldOwnersBackup] {
		t.Error("the decommissioned instance backup is not marked foreign_owner, want true")
	}
	if foreign[result.Manifest.BackupID] {
		t.Error("the new backup is marked foreign_owner, want false")
	}
}

// TestLoadOrCreateInstanceIDPersistsBesideTheDatabase pins the decision that
// the standalone identity is a SIBLING of the shared SQLite database and not a
// row inside it.
//
// Inside it, the identity would be copied into every backup (the shared
// database is backed up) and restored with it, so a restore onto fresh
// hardware would make the new instance a continuation of the old one unless
// boot ordering around ApplyPendingRestores unwound it. A sibling file is
// outside every backup, so the rule holds by construction.
func TestLoadOrCreateInstanceIDPersistsBesideTheDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arc.db")

	first, err := LoadOrCreateInstanceID(dbPath)
	if err != nil {
		t.Fatalf("LoadOrCreateInstanceID: %v", err)
	}
	if first == "" {
		t.Fatal("LoadOrCreateInstanceID returned an empty identity")
	}
	second, err := LoadOrCreateInstanceID(dbPath)
	if err != nil {
		t.Fatalf("LoadOrCreateInstanceID (second call): %v", err)
	}
	if second != first {
		t.Errorf("identity changed between calls: %q then %q; it must persist", first, second)
	}

	path := filepath.Join(dir, InstanceIDFileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the identity must be stored beside the database at %q: %v", path, err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("identity file mode = %04o, want 0600: it sits in the directory that holds auth tokens", mode)
	}
	// Not inside the database file, which is what a restore would carry.
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Errorf("LoadOrCreateInstanceID created or touched %q (err=%v); it must never open the shared database", dbPath, err)
	}

	// A truncated file from a crash between create and write must not become
	// an empty identity, which is the absence of one rather than a value.
	if err := os.WriteFile(path, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	third, err := LoadOrCreateInstanceID(dbPath)
	if err != nil {
		t.Fatalf("LoadOrCreateInstanceID after a truncated write: %v", err)
	}
	if third == "" {
		t.Error("a truncated identity file produced an empty identity, want a freshly minted one")
	}
}

// TestGetBackupSeparatesNotFoundFromAnUnreadableDestination: an unknown id is
// the caller's permanent mistake and an unreachable destination is neither.
// They used to be one error; the distinction is what lets the API answer 404
// for the first and 503 for the second instead of telling an operator their
// backup is gone because of a transient.
func TestGetBackupSeparatesNotFoundFromAnUnreadableDestination(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m := newBackupAt(t, dir, "")

	_, err := m.GetBackup(ctx, "backup-20260101-010101-dddddddd")
	if err == nil {
		t.Fatal("GetBackup on an unknown id succeeded, want an error")
	}
	if !errorIsBackupNotFound(err) {
		t.Errorf("GetBackup on an unknown id = %v, want it to wrap ErrBackupNotFound", err)
	}
	if !strings.Contains(err.Error(), "backup not found") {
		t.Errorf("error = %q, want it to read \"backup not found\"", err.Error())
	}

	// The same call against a destination that cannot answer at all. Swapped
	// in place rather than copied: a Manager holds a mutex.
	m.backupStorage = unreachableDestination{Backend: m.backupStorage}
	m.targetName = "audit"
	_, err = m.GetBackup(ctx, "backup-20260101-010101-dddddddd")
	if err == nil {
		t.Fatal("GetBackup against an unreachable destination succeeded, want an error")
	}
	if errorIsBackupNotFound(err) {
		t.Errorf("GetBackup against an unreachable destination = %v, want NOT ErrBackupNotFound: that answer tells an operator their backup is gone", err)
	}
	if !strings.Contains(err.Error(), "backup target audit") {
		t.Errorf("error = %q, want it to name the target so an operator with more than one store knows which is down", err.Error())
	}
}

// TestListBackupsNamesTheUnreachableTarget: the per-operation report for an
// unreachable destination. The destination can now be remote, so "failed to
// list backup storage" leaves an operator to guess which of their configured
// stores is down.
func TestListBackupsNamesTheUnreachableTarget(t *testing.T) {
	ctx := context.Background()
	m := newBackupAt(t, t.TempDir(), "")
	m.backupStorage = unreachableDestination{Backend: m.backupStorage}
	m.targetName = "audit"

	_, err := m.ListBackups(ctx)
	if err == nil {
		t.Fatal("ListBackups against an unreachable destination succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "backup target audit") {
		t.Errorf("error = %q, want it to name the target", err.Error())
	}
}

// TestDeleteBackupNamesTheDestination: DeleteBackup is a destination operation
// like the other three, so its failures have to name the target too. Its
// not-found case also carries the sentinel now, so a caller can tell "no such
// backup" from "the destination would not answer".
func TestDeleteBackupNamesTheDestination(t *testing.T) {
	ctx := context.Background()
	m := newBackupAt(t, t.TempDir(), "")

	err := m.DeleteBackup(ctx, "backup-20260101-010101-eeeeeeee")
	if err == nil {
		t.Fatal("DeleteBackup on an unknown id succeeded, want an error")
	}
	if !errorIsBackupNotFound(err) {
		t.Errorf("DeleteBackup on an unknown id = %v, want it to wrap ErrBackupNotFound", err)
	}
	if !strings.Contains(err.Error(), "backup not found") {
		t.Errorf("error = %q, want it to read \"backup not found\"", err.Error())
	}

	m.backupStorage = unreachableListing{Backend: m.backupStorage}
	m.targetName = "audit"
	err = m.DeleteBackup(ctx, "backup-20260101-010101-eeeeeeee")
	if err == nil {
		t.Fatal("DeleteBackup against an unreachable destination succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "backup target audit") {
		t.Errorf("error = %q, want it to name the target", err.Error())
	}
	if errorIsBackupNotFound(err) {
		t.Errorf("error = %v, want NOT ErrBackupNotFound: an unreachable destination is not an absent backup", err)
	}
}

// unreachableListing fails List, which is how DeleteBackup finds a backup's
// files.
type unreachableListing struct {
	storage.Backend
}

func (unreachableListing) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, fmt.Errorf("dial tcp 127.0.0.1:1: connect: connection refused")
}

func errorIsBackupNotFound(err error) bool {
	for e := err; e != nil; {
		if e == ErrBackupNotFound {
			return true
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}

// unreachableDestination answers every read with a transport failure, the way
// a backend pointed at a closed port does.
//
// Read is overridden as well as the listings, and that matters: the manifest
// lookup is ONE Read now rather than Exists-then-Read, so a fake that failed
// only Exists would let the embedded local backend answer "not found" and the
// test would assert the pre-change behaviour against the post-change code.
// Nothing here must look like a not-found to storage.IsNotFound, or the
// transport failure would be reported as an absent backup — which is the exact
// lie these tests exist to prevent.
type unreachableDestination struct {
	storage.Backend
}

const unreachableErr = "dial tcp 127.0.0.1:1: connect: connection refused"

func (unreachableDestination) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	return nil, errors.New(unreachableErr)
}

func (unreachableDestination) Exists(ctx context.Context, path string) (bool, error) {
	return false, errors.New(unreachableErr)
}

func (unreachableDestination) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, errors.New(unreachableErr)
}

// TestATransportFailureIsNeverClassifiedAsAMissingObject guards the one-round-
// trip manifest read against the misclassification it could introduce:
// storage.IsNotFound decides between "absent" and "the store would not
// answer", and a predicate that said yes to a transport error would turn every
// outage into "Backup not found".
func TestATransportFailureIsNeverClassifiedAsAMissingObject(t *testing.T) {
	for _, msg := range []string{
		unreachableErr,
		"dial tcp 10.0.0.1:443: i/o timeout",
		"context deadline exceeded",
		"operation error S3: GetObject, exceeded maximum number of attempts, 3",
		"RequestError: send request failed",
	} {
		if storage.IsNotFound(errors.New(msg)) {
			t.Errorf("storage.IsNotFound(%q) = true, want false: a transport failure is not an absent object", msg)
		}
	}
	// And the real absent-object error from the local backend IS classified,
	// which is what makes the single read sound.
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	_, readErr := b.Read(context.Background(), "no/such/object.json")
	if readErr == nil {
		t.Fatal("reading a missing object succeeded, want an error")
	}
	if !storage.IsNotFound(readErr) {
		t.Errorf("storage.IsNotFound(%v) = false, want true", readErr)
	}
}

// TestManifestTargetAndOwnerRoundTripThroughJSON: both fields are omitempty,
// so a manifest from a deployment with no target and no identity must serialise
// exactly as it did before they existed — otherwise every client that compares
// manifests sees a change that is not one.
func TestManifestTargetAndOwnerRoundTripThroughJSON(t *testing.T) {
	data, err := MarshalManifest(&Manifest{Version: "dev", BackupID: "backup-20261007-000000-00000000", BackupType: "full"})
	if err != nil {
		t.Fatal(err)
	}
	var generic map[string]any
	if err := json.Unmarshal(data, &generic); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"target", "owner_instance_id"} {
		if _, present := generic[key]; present {
			t.Errorf("manifest JSON carries %q for a deployment that has neither, want it omitted", key)
		}
	}

	data, err = MarshalManifest(&Manifest{Version: "dev", BackupID: "backup-20261007-000000-00000000", BackupType: "full", Target: "audit", OwnerInstanceID: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	back, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if back.Target != "audit" || back.OwnerInstanceID != "alpha" {
		t.Errorf("round trip gave target/owner %q/%q, want \"audit\"/\"alpha\"", back.Target, back.OwnerInstanceID)
	}
}

// TestTheInstanceIdentitySurvivesABackupAndRestoreFromInsideTheStorageRoot
// pins the mechanism that keeps the identity out of a backup, for the
// configuration that breaks the obvious explanation.
//
// The shared database — and therefore the identity sidecar beside it — is put
// INSIDE the storage root here on purpose. "The file sits outside the
// backed-up tree" is then false, so if that were the mechanism the identity
// would travel with the backup and a restore would overwrite it. What actually
// holds is that the backup inventories data files and the restore writes only
// keys the backup contains, so the sidecar is neither copied nor overwritten.
func TestTheInstanceIdentitySurvivesABackupAndRestoreFromInsideTheStorageRoot(t *testing.T) {
	ctx := context.Background()
	storageRoot := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(storageRoot, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	if err := dataStorage.Write(ctx, "db/cpu/2026/10/07/00/a.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}

	// auth.db, and so the sidecar, inside the storage root.
	dbPath := filepath.Join(storageRoot, "arc.db")
	ownID, err := LoadOrCreateInstanceID(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	sidecar := filepath.Join(storageRoot, InstanceIDFileName)
	before, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatal(err)
	}

	backupDir := t.TempDir()
	m, err := NewManager(&ManagerConfig{
		DataStorage: dataStorage,
		BackupPath:  backupDir,
		InstanceID:  ownID,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := m.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup: %v", err)
	}

	// The sidecar is not in the backup at all.
	copied, err := m.defaultDestination().backend.List(ctx, result.Manifest.BackupID+"/")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range copied {
		if strings.Contains(key, InstanceIDFileName) {
			t.Errorf("the backup contains the instance identity at %q; it must never be inventoried", key)
		}
	}

	if _, err := m.RestoreBackup(ctx, RestoreOptions{
		BackupID:        result.Manifest.BackupID,
		RestoreData:     true,
		RestoreMetadata: false,
	}); err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	after, err := os.ReadFile(sidecar)
	if err != nil {
		t.Fatalf("the identity file is gone after a restore: %v", err)
	}
	if string(after) != string(before) {
		t.Errorf("the identity changed across a backup and restore: %q then %q", before, after)
	}
	again, err := LoadOrCreateInstanceID(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if again != ownID {
		t.Errorf("identity after the restore = %q, want the pre-restore %q: a restore must not adopt the backup identity", again, ownID)
	}
}

// TestTheForeignOwnerFilterReportsWhatItSuppressed: the filter has to announce
// itself. On fresh hardware every backup reads as foreign, so recovery — the
// case backups exist for — was answered with an empty listing and no sign that
// anything had been withheld.
func TestTheForeignOwnerFilterReportsWhatItSuppressed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for _, id := range []string{"backup-20260101-010101-11111111", "backup-20260102-010101-22222222"} {
		writeManifest(t, dir, id, []byte(`{"version":"1","backup_id":"`+id+`","created_at":"2026-01-01T01:01:01Z","backup_type":"full","databases":[],"total_files":0,"total_size_bytes":0,"owner_instance_id":"the-decommissioned-instance"}`))
	}
	m := newBackupAt(t, dir, "the-replacement-instance")

	summaries, filtered, err := m.ListBackupsFilteringForeign(ctx)
	if err != nil {
		t.Fatalf("ListBackupsFilteringForeign: %v", err)
	}
	if len(summaries) != 0 {
		t.Errorf("listing returned %d entries, want 0", len(summaries))
	}
	if filtered != 2 {
		t.Errorf("filtered count = %d, want 2: an empty listing over a populated destination must say what it withheld", filtered)
	}

	// And zero when nothing was withheld, so the field stays absent on the
	// ordinary response.
	own := newBackupAt(t, dir, "")
	if _, filtered, err := own.ListBackupsFilteringForeign(ctx); err != nil || filtered != 0 {
		t.Errorf("filtered count = %d (err %v) for an instance that owns everything, want 0", filtered, err)
	}
}

// TestDeleteBackupEchoesAForeignOwner: restore warns on a foreign manifest and
// delete did not, so the DESTRUCTIVE operation was the one with no echo — and
// the opt-in listing that reveals foreign backups is exactly what hands an
// operator the id they pass to this call.
func TestDeleteBackupEchoesAForeignOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const foreignID = "backup-20260101-010101-ffffffff"
	writeManifest(t, dir, foreignID, []byte(`{"version":"1","backup_id":"`+foreignID+`","created_at":"2026-01-01T01:01:01Z","backup_type":"full","databases":[],"total_files":0,"total_size_bytes":0,"owner_instance_id":"the-other-instance"}`))

	var logOutput bytes.Buffer
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	m, err := NewManager(&ManagerConfig{
		DataStorage: dataStorage,
		BackupPath:  dir,
		InstanceID:  "this-instance",
		Logger:      zerolog.New(&logOutput),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteBackup(ctx, foreignID); err != nil {
		t.Fatalf("DeleteBackup on a foreign backup = %v, want success: a refusal would be self-locking", err)
	}
	logs := logOutput.String()
	if !strings.Contains(logs, "written by a different Arc instance") {
		t.Errorf("deleting a foreign backup logged no owner echo; got: %s", logs)
	}
	if !strings.Contains(logs, "the-other-instance") {
		t.Errorf("the echo does not name the owner; got: %s", logs)
	}
	// It really was deleted: the echo is a warning, not a refusal.
	if _, err := m.GetBackup(ctx, foreignID); !errorIsBackupNotFound(err) {
		t.Errorf("GetBackup after the delete = %v, want ErrBackupNotFound", err)
	}
}

// writeRefusingDestination accepts every read and fails every write, the way a
// destination whose credentials lost write access does.
// writeRefusingDestination refuses every write, except the keys allow lets
// through.
//
// The allow list exists because #1085 stage B2b-2 made the RUN INDEX the first
// write a backup makes, so a blanket refusal makes every one of these tests
// report the index failure and stop testing the message it was written for.
// Letting index.json through keeps each test on its own call site: the test
// that claims to check the data-copy message checks the data-copy message, and
// the index write has a test of its own below.
type writeRefusingDestination struct {
	storage.Backend
	allow func(path string) bool
}

const writeRefusedErr = "AccessDenied: the credential may not write to this bucket"

// refuseAllWritesExceptTheIndex is the usual allow list.
func refuseAllWritesExceptTheIndex(path string) bool {
	return strings.HasSuffix(path, "/index.json")
}

func (w writeRefusingDestination) Write(ctx context.Context, path string, data []byte) error {
	if w.allow != nil && w.allow(path) {
		return w.Backend.Write(ctx, path, data)
	}
	return errors.New(writeRefusedErr)
}

func (w writeRefusingDestination) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if w.allow != nil && w.allow(path) {
		return w.Backend.WriteReader(ctx, path, r, size)
	}
	return errors.New(writeRefusedErr)
}

// TestEveryDestinationWriteFailureNamesTheDestination: the read paths named
// the target from the start and the WRITE paths did not, which is most of what
// a backup does. "failed to write to backup storage: AccessDenied" leaves an
// operator with more than one configured store to guess which one refused.
//
// Driven through CreateBackup so it exercises the real call chain rather than
// the message in isolation. The RUN INDEX is the first write a run makes since
// #1085 stage B2b-2, so the fake lets that one key through and the data copy
// is then the first write that fails — which is the error this test is about.
// The index write has its own test below.
func TestEveryDestinationWriteFailureNamesTheDestination(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	if err := dataStorage.Write(ctx, "db/cpu/2026/10/07/00/a.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	m, err := NewManager(&ManagerConfig{
		DataStorage:   dataStorage,
		Targets:       oneTarget(t.TempDir(), "audit", "", true),
		DefaultTarget: "audit",
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.backupStorage = writeRefusingDestination{Backend: m.backupStorage, allow: refuseAllWritesExceptTheIndex}

	_, err = m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("CreateBackup succeeded against a destination that refuses every write")
	}
	if !strings.Contains(err.Error(), "backup target audit") {
		t.Errorf("error = %q, want it to name the destination", err.Error())
	}
	if strings.Contains(err.Error(), "failed to write to backup storage") {
		t.Errorf("error = %q, still carries the anonymous wording", err.Error())
	}
}

// TestTheSidecarAndManifestWriteFailuresNameTheDestination covers the two
// fixed-name writes separately, because they are different call sites with
// their own messages and a backup that got past the data copy reports one of
// those instead.
func TestTheSidecarAndManifestWriteFailuresNameTheDestination(t *testing.T) {
	ctx := context.Background()
	// No data files, so the run reaches the sidecar and the manifest.
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	m, err := NewManager(&ManagerConfig{
		DataStorage:   dataStorage,
		Targets:       oneTarget(t.TempDir(), "audit", "", true),
		DefaultTarget: "audit",
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.backupStorage = writeRefusingDestination{Backend: m.backupStorage, allow: refuseAllWritesExceptTheIndex}

	_, err = m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("CreateBackup succeeded against a destination that refuses every write")
	}
	if !strings.Contains(err.Error(), "the file sidecar to backup target audit") {
		t.Errorf("error = %q, want the sidecar failure to name the destination", err.Error())
	}
}

// stallingDestination fails every existence check and RECORDS every write, so
// a test can tell whether the run reached a write at all.
type stallingDestination struct {
	storage.Backend
	writes []string
}

func (s *stallingDestination) Exists(ctx context.Context, path string) (bool, error) {
	return false, errors.New("dial tcp 10.0.0.1:443: i/o timeout")
}

func (s *stallingDestination) Write(ctx context.Context, path string, data []byte) error {
	s.writes = append(s.writes, path)
	return s.Backend.Write(ctx, path, data)
}

func (s *stallingDestination) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	s.writes = append(s.writes, path)
	return s.Backend.WriteReader(ctx, path, r, size)
}

// TestABackupProbesTheDestinationBeforeCopying is what makes "an unreachable
// destination fails naming the target rather than hanging" true on the WRITE
// path, which is where it was NOT true.
//
// A run's first destination touch is a bulk file copy, under the run's own
// operation_timeout — two hours by default — while the single-operation lock
// is held, and the S3 client sets no response timeout on purpose. A
// connection-refused endpoint fails in seconds; a BLACK-HOLED one would have
// held that lock for the full two hours and then reported an error that did
// not say which destination failed.
//
// The load-bearing assertion is the second one: the run must fail WITHOUT
// having attempted a write. That is what proves the probe ran first rather
// than the copy happening to fail.
func TestABackupProbesTheDestinationBeforeCopying(t *testing.T) {
	ctx := context.Background()
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	for i := 0; i < 5; i++ {
		if err := dataStorage.Write(ctx, fmt.Sprintf("db/cpu/2026/10/07/00/a%d.parquet", i), []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	m, err := NewManager(&ManagerConfig{
		DataStorage:   dataStorage,
		Targets:       oneTarget(t.TempDir(), "audit", "", true),
		DefaultTarget: "audit",
		Logger:        zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	stalling := &stallingDestination{Backend: m.backupStorage}
	m.backupStorage = stalling

	_, err = m.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatal("CreateBackup succeeded against a destination that will not answer")
	}
	if !strings.Contains(err.Error(), "backup target audit") {
		t.Errorf("error = %q, want it to name the destination", err.Error())
	}
	if !strings.Contains(err.Error(), "did not answer") {
		t.Errorf("error = %q, want it to say the destination did not answer", err.Error())
	}
	if len(stalling.writes) != 0 {
		t.Errorf("the run attempted %d write(s) (%v) against a destination that will not answer; the probe must fail it first, or a black-holed endpoint holds the operation lock for the whole run budget",
			len(stalling.writes), stalling.writes)
	}
	// And the progress record carries the same answer, so the API reports it.
	if p := m.GetProgress(); p == nil || p.Status != "failed" || !strings.Contains(p.Error, "backup target audit") {
		t.Errorf("progress = %+v, want a failed status naming the destination", p)
	}
}

// TestTheDestinationProbeIsBounded pins that the probe carries its own
// deadline, which is the whole point: it must not inherit the run's two-hour
// operation timeout.
func TestTheDestinationProbeIsBounded(t *testing.T) {
	if destinationProbeTimeout <= 0 || destinationProbeTimeout > 5*time.Minute {
		t.Fatalf("destinationProbeTimeout = %s, want a positive bound well under a run budget", destinationProbeTimeout)
	}
	ctx, cancel := withDestinationTimeout(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("withDestinationTimeout returned a context with no deadline")
	}
	if remaining := time.Until(deadline); remaining > destinationProbeTimeout {
		t.Errorf("deadline is %s away, want at most %s", remaining, destinationProbeTimeout)
	}

	// A SHORTER deadline already on the caller's context still wins, or the
	// API handler's 30 s budget would be widened to a minute by this helper.
	short, cancelShort := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelShort()
	bounded, cancelBounded := withDestinationTimeout(short)
	defer cancelBounded()
	d, ok := bounded.Deadline()
	if !ok {
		t.Fatal("no deadline on the derived context")
	}
	if time.Until(d) > time.Second {
		t.Errorf("the derived deadline is %s away, want the caller's shorter one to win", time.Until(d))
	}
}
