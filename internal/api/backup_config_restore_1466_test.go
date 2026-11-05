package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
)

// TestRestoreConfigRefusedUpFrontWhenItCannotBeWritten (#1466): a backup that
// holds a config, restored with restore_config on a node that started without
// a config file, is answered 400 before anything is restored. Without the
// config step the same restore is admitted.
func TestRestoreConfigRefusedUpFrontWhenItCannotBeWritten(t *testing.T) {
	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	backupPath := t.TempDir()

	configPath := filepath.Join(t.TempDir(), "arc.toml")
	if err := os.WriteFile(configPath, []byte("# backed up\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := backup.NewManager(&backup.ManagerConfig{DataStorage: dataStorage, BackupPath: backupPath, ConfigPath: configPath, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	res, err := src.CreateBackup(context.Background(), backup.BackupOptions{IncludeConfig: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Manifest.HasConfig {
		t.Fatal("source backup holds no config")
	}

	// The restoring node: no config file loaded.
	m, err := backup.NewManager(&backup.ManagerConfig{DataStorage: dataStorage, BackupPath: backupPath, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	NewBackupHandler(m, nil, 2*time.Hour, zerolog.Nop()).RegisterRoutes(app)
	t.Cleanup(func() { app.Shutdown() })

	post := func(body string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/backup/restore", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 5_000)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	id := res.Manifest.BackupID
	code, body := post(`{"backup_id":"` + id + `","confirm":true,"restore_data":true,"restore_metadata":false,"restore_config":true}`)
	if code != fiber.StatusBadRequest || !strings.Contains(body, "without a config file") {
		t.Fatalf("restore_config without a config file: %d %s, want 400 naming the reason", code, body)
	}
	if p := m.GetProgress(); p != nil {
		t.Fatalf("a refused restore started an operation: %+v", p)
	}

	code, body = post(`{"backup_id":"` + id + `","confirm":true,"restore_data":true,"restore_metadata":false,"restore_config":false}`)
	if code != fiber.StatusAccepted {
		t.Fatalf("restore without restore_config: %d %s, want 202", code, body)
	}
	var resp map[string]any
	_ = json.Unmarshal([]byte(body), &resp)
	if _, present := resp["restart_required"]; present {
		t.Errorf("data-only restore: restart_required = %v, want absent", resp["restart_required"])
	}
}
