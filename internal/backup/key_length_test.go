package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// longKeyInventory models an object store that can list and read a long,
// otherwise legal source key without depending on the host OS path limit.
type longKeyInventory struct {
	storage.Backend
	longKey string
}

func (s longKeyInventory) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objects, err := s.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return append([]storage.ObjectInfo{{Path: s.longKey, Size: 4}}, objects...), nil
}

func (s longKeyInventory) ReadTo(ctx context.Context, path string, dst io.Writer) error {
	if path == s.longKey {
		_, err := io.WriteString(dst, "PAR1")
		return err
	}
	return s.Backend.ReadTo(ctx, path, dst)
}

func TestBackupLongSourceKeyIsSkippedAndReported(t *testing.T) {
	ctx := context.Background()

	// The generated backup ID must reserve exactly the documented headroom.
	backupID := generateBackupID()
	if got := len(backupID + "/data/"); got != backupDataKeyHeadroom {
		t.Fatalf("backup prefix length = %d, want %d", got, backupDataKeyHeadroom)
	}

	// The source fits the shared storage contract by exactly one byte less
	// than its backup destination requires.
	sourceLimit := storage.MaxUsableKeyLen - backupDataKeyHeadroom
	prefix := "db/cpu/" + strings.Repeat(strings.Repeat("a", 200)+"/", 4)
	tailLen := sourceLimit + 1 - len(prefix) - len(".parquet")
	if tailLen <= 0 || tailLen+len(".parquet") > storage.MaxUsableKeySegmentLen {
		t.Fatalf("invalid test key segment length: %d", tailLen)
	}

	longKey := prefix + strings.Repeat("z", tailLen) + ".parquet"
	if len(longKey) != sourceLimit+1 {
		t.Fatalf("source length = %d, want %d", len(longKey), sourceLimit+1)
	}
	if err := storage.ValidateKey(longKey); err != nil {
		t.Fatalf("source key must be legal: %v", err)
	}
	if err := storage.ValidateKey(backupID + "/data/" + longKey); err == nil {
		t.Fatal("premise failed: backup destination must exceed the key limit")
	}

	var logOutput bytes.Buffer
	logger := zerolog.New(&logOutput)

	dataStorage, err := storage.NewLocalBackend(t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}

	// Twenty ordinary files ensure one skip remains below maxSkipRatio.
	// The long object is supplied by the wrapper, avoiding macOS PATH_MAX.
	var firstGoodPath string
	const goodContent = "PAR1-good"

	for i := 0; i < 20; i++ {
		path := fmt.Sprintf("db/cpu/2026/09/17/00/good-%02d.parquet", i)
		if i == 0 {
			firstGoodPath = path
		}
		if err := dataStorage.Write(ctx, path, []byte(goodContent)); err != nil {
			t.Fatalf("write good file: %v", err)
		}
	}

	manager, err := NewManager(&ManagerConfig{
		DataStorage: longKeyInventory{
			Backend: dataStorage,
			longKey: longKey,
		},
		BackupPath: t.TempDir(),
		Logger:     logger,
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := manager.CreateBackup(ctx, BackupOptions{})
	if err != nil {
		t.Fatalf("CreateBackup must skip only the overlong destination: %v", err)
	}

	manifest := result.Manifest
	if manifest.TotalFiles != 21 || manifest.SkippedFiles != 1 {
		t.Errorf("manifest counts = total %d, skipped %d; want 21, 1",
			manifest.TotalFiles, manifest.SkippedFiles)
	}

	progress := manager.GetProgress()
	if progress == nil {
		t.Fatal("missing final progress")
	}
	if progress.Status != "completed" ||
		progress.ProcessedFiles != 20 ||
		progress.SkippedFiles != 1 {
		t.Errorf("progress = %+v; want completed, 20 copied, 1 skipped", progress)
	}

	// The backup must still contain its manifest and an ordinary data file.
	manifestData, err := manager.backupStorage.Read(ctx, manifest.BackupID+"/manifest.json")
	if err != nil {
		t.Fatalf("completed backup has no manifest: %v", err)
	}

	var stored Manifest
	if err := json.Unmarshal(manifestData, &stored); err != nil {
		t.Fatalf("decode stored manifest: %v", err)
	}
	if stored.TotalFiles != 21 || stored.SkippedFiles != 1 {
		t.Errorf("stored manifest = total %d, skipped %d; want 21, 1",
			stored.TotalFiles, stored.SkippedFiles)
	}

	goodData, err := manager.backupStorage.Read(
		ctx, manifest.BackupID+"/data/"+firstGoodPath,
	)
	if err != nil {
		t.Fatalf("ordinary source file was not copied: %v", err)
	}
	if string(goodData) != goodContent {
		t.Errorf("copied content = %q, want %q", goodData, goodContent)
	}

	if !strings.Contains(logOutput.String(), "Backup destination key too long") {
		t.Error("skip reason was not logged")
	}
}
