package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// longStateKeyInventory injects one overlong, otherwise legal
// _compaction_state/ key into the listing, the way longKeyInventory does for
// data files, so the test does not depend on the host OS path limit.
type longStateKeyInventory struct {
	storage.Backend
	longKey string
}

func (s longStateKeyInventory) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := s.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if prefix != "" && !strings.HasPrefix(s.longKey, prefix) {
		return objects, nil
	}
	return append([]storage.ObjectInfo{{Path: s.longKey, Size: 2}}, objects...), nil
}

func (s longStateKeyInventory) ReadTo(ctx context.Context, path string, dst io.Writer) error {
	if path == s.longKey {
		_, err := io.WriteString(dst, "{}")
		return err
	}
	return s.Backend.ReadTo(ctx, path, dst)
}

// writeWatchingBackend records every destination key a backup attempts to
// write, so a test can prove a write was never ATTEMPTED rather than only
// that it left nothing behind — a failed write leaves nothing behind either.
//
// The StagingInspector methods are delegated rather than dropped: the backup
// manager type-asserts that interface on its destination
// (cleanupPartialBackupWrite), so a wrapper that hid it would silently move
// the test onto the remote-destination branch.
type writeWatchingBackend struct {
	storage.Backend
	writes []string
}

func (b *writeWatchingBackend) Write(ctx context.Context, path string, data []byte) error {
	b.writes = append(b.writes, path)
	return b.Backend.Write(ctx, path, data)
}

func (b *writeWatchingBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	b.writes = append(b.writes, path)
	return b.Backend.WriteReader(ctx, path, r, size)
}

func (b *writeWatchingBackend) StagedSize(ctx context.Context, key string) (int64, error) {
	return b.Backend.(storage.StagingInspector).StagedSize(ctx, key)
}

func (b *writeWatchingBackend) ReadStaged(ctx context.Context, key string, w io.Writer) error {
	return b.Backend.(storage.StagingInspector).ReadStaged(ctx, key, w)
}

func (b *writeWatchingBackend) DeleteStaged(ctx context.Context, key string) error {
	return b.Backend.(storage.StagingInspector).DeleteStaged(ctx, key)
}

func (b *writeWatchingBackend) ListStaged(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return b.Backend.(storage.StagingInspector).ListStaged(ctx, prefix)
}

var _ storage.StagingInspector = (*writeWatchingBackend)(nil)

// A compaction recovery manifest whose BACKUP destination key is over the
// storage limit fails the run, before the write is attempted, with an error
// that names the key and the byte figures (#1100).
//
// Failing rather than skipping is the point. copyStateFiles already holds the
// rule that a manifest which exists and cannot be copied must fail the backup,
// because the alternative is #930: the restore brings back the compacted
// output AND the inputs it replaced, nothing reconciles them, and the
// partition serves every row twice. An over-long destination key is exactly
// "exists and cannot be copied".
//
// Both assertions matter and neither is redundant:
//
//   - The run already failed BEFORE the check existed, from inside
//     WriteReader, so asserting err != nil would pass against the bug. The
//     test matches the error TEXT instead.
//   - The run already wrote nothing to that key before the check existed,
//     because the write failed. So "the object is absent" would pass against
//     the bug too. The test asserts the write was never ATTEMPTED.
func TestBackupOverlongCompactionStateKeyFailsTheRun(t *testing.T) {
	ctx := context.Background()

	// A legal source key whose destination needs exactly one byte more than
	// the limit allows, built under _compaction_state/ so it is routed as
	// state rather than as a data file.
	sourceLimit := storage.MaxUsableKeyLen - backupDataKeyHeadroom
	prefix := compactionStateDir + "/hourly/db/" + strings.Repeat(strings.Repeat("a", 200)+"/", 4)
	tailLen := sourceLimit + 1 - len(prefix) - len(".json")
	if tailLen <= 0 || tailLen+len(".json") > storage.MaxUsableKeySegmentLen {
		t.Fatalf("invalid test key segment length: %d", tailLen)
	}
	longKey := prefix + strings.Repeat("z", tailLen) + ".json"
	if len(longKey) != sourceLimit+1 {
		t.Fatalf("source length = %d, want %d", len(longKey), sourceLimit+1)
	}
	if err := storage.ValidateKey(longKey); err != nil {
		t.Fatalf("premise failed: the source key must be legal: %v", err)
	}
	if !isCompactionState(longKey) {
		t.Fatal("premise failed: the key must be routed as compaction state")
	}

	var logOutput bytes.Buffer
	logger := zerolog.New(&logOutput)

	dataStorage, err := storage.NewLocalBackend(t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}

	// A real partition plus a real recovery manifest, so the run has
	// something to copy and the failure is not an artefact of an empty store.
	const partition = "db/cpu/2026/01/01/00"
	for i := 0; i < 20; i++ {
		if err := dataStorage.Write(ctx, fmt.Sprintf("%s/good-%02d.parquet", partition, i), []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	good := compaction.Manifest{
		OutputPath: partition + "/x_compacted.parquet", OutputSize: 4,
		InputFiles: []string{partition + "/good-00.parquet"},
		Database:   "db", Measurement: "cpu", PartitionPath: partition, Tier: "hourly",
		Status: compaction.ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "job1",
	}
	goodData, err := json.Marshal(good)
	if err != nil {
		t.Fatal(err)
	}
	const goodManifestKey = compactionStateDir + "/hourly/db/job1.json"
	if err := dataStorage.Write(ctx, goodManifestKey, goodData); err != nil {
		t.Fatal(err)
	}

	manager, err := NewManager(&ManagerConfig{
		DataStorage: longStateKeyInventory{Backend: dataStorage, longKey: longKey},
		BackupPath:  t.TempDir(),
		Logger:      logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	watcher := &writeWatchingBackend{Backend: manager.backupStorage}
	manager.backupStorage = watcher

	result, err := manager.CreateBackup(ctx, BackupOptions{})
	if err == nil {
		t.Fatalf("CreateBackup must fail: a recovery manifest that cannot be copied is the #930 double-serve. Got result %+v", result)
	}

	// The error, not merely its existence. Pre-#1100 the run failed too, with
	// "failed to write to backup storage: invalid path: ... over the
	// 1019-byte limit" — the generic storage wording, from inside WriteReader.
	msg := err.Error()
	for _, want := range []string{
		longKey, // which file
		"destination_key_bytes=" + strconv.Itoa(backupDataKeyHeadroom+len(longKey)),
		"maximum_key_bytes=" + strconv.Itoa(storage.MaxUsableKeyLen),
		"max_source_key_bytes=" + strconv.Itoa(storage.MaxUsableKeyLen-backupDataKeyHeadroom),
		"serve every row twice",                       // the stakes
		"Rename the file under " + compactionStateDir, // the remedy
		"SOURCE KEY", // which length the figure is
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error does not mention %q; got: %s", want, msg)
		}
	}
	// Deterministic, so asking for a retry would be wrong: the unreadable-but-
	// exists branch says retry because that failure is transient; this one is
	// not.
	if strings.Contains(strings.ToLower(msg), "retry") {
		t.Errorf("error asks for a retry, but an over-long key is deterministic and a retry changes nothing; got: %s", msg)
	}
	// There is no operator-settable backup key prefix: the destination key is
	// the generated backup ID plus "/data/", and a destination backend own
	// prefix is applied below ValidateKey and does not count toward the limit.
	// So a remedy that says to shorten it would send the operator looking for
	// a knob that does not exist.
	if strings.Contains(msg, "shorten the backup prefix") {
		t.Errorf("error prescribes shortening the backup prefix, which is not a setting; got: %s", msg)
	}

	// Checked BEFORE the write: no write to that key was ever attempted. The
	// backup ID is minted inside the run and CreateBackup returns no result on
	// failure, so the key is matched by suffix rather than rebuilt.
	for _, w := range watcher.writes {
		if strings.HasSuffix(w, longKey) {
			t.Errorf("a write to %q was attempted; the check must run before the write, so the failure names the key rather than the storage layer", w)
		}
	}

	// And the run stopped at the state pass rather than continuing: no
	// manifest.json was written, which is the signal a reader uses for an
	// incomplete run.
	for _, w := range watcher.writes {
		if strings.HasSuffix(w, "/manifest.json") {
			t.Errorf("manifest.json was written at %q; a failed run must leave none", w)
		}
	}
}
