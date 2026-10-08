package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

const clusterDeleteBackupID = "backup-20261008-120000-deadbeef"

// Two real destinations exercise the delete sweep, including a non-default
// target. Seed files directly so these tests do not depend on backup creation.
func newClusterDeleteRig(t *testing.T) (*BackupHandler, *fiber.App, []string) {
	t.Helper()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = data.Close() })
	roots := []string{t.TempDir(), t.TempDir()}
	targets := make([]backup.Target, len(roots))
	for i, root := range roots {
		targets[i] = backup.Target{Name: fmt.Sprintf("target%d", i), Spec: storage.BackendSpec{Type: "local", LocalPath: root}}
	}
	manager, err := backup.NewManager(&backup.ManagerConfig{DataStorage: data, Targets: targets, DefaultTarget: "target0", Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewBackupHandler(manager, nil, time.Hour, zerolog.Nop())
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	handler.RegisterRoutes(app)
	t.Cleanup(func() { _ = app.Shutdown() })
	return handler, app, roots
}

func seedClusterDeleteBackup(t *testing.T, roots []string) {
	t.Helper()
	for _, root := range roots {
		dir := filepath.Join(root, clusterDeleteBackupID, "data")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "file.parquet"), []byte("backup payload"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func assertClusterDeleteFiles(t *testing.T, roots []string, deleted bool) {
	t.Helper()
	for _, root := range roots {
		data, err := os.ReadFile(filepath.Join(root, clusterDeleteBackupID, "data", "file.parquet"))
		if deleted {
			if !os.IsNotExist(err) {
				t.Errorf("target %s: deleted file still present or unreadable: %v", root, err)
			}
		} else if err != nil || string(data) != "backup payload" {
			t.Errorf("target %s: refused delete changed backup: data=%q err=%v", root, data, err)
		}
	}
}

func requestClusterDelete(t *testing.T, app *fiber.App, id string) (int, map[string]any) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/backup/"+id, nil), 5_000)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func assertClusterDeleteRejected(t *testing.T, status int, body map[string]any, role string) {
	t.Helper()
	want := fmt.Sprintf("delete rejected: node role %q is not primary writer; route to the primary writer", role)
	if status != fiber.StatusServiceUnavailable || body["error"] != want || body["role"] != role {
		t.Errorf("got status=%d body=%v; want 503 with error=%q and role=%q", status, body, want, role)
	}
}

func TestDeleteBackupPrimaryWriterGate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		coordinator *fakeBackupCoordinator
	}{
		{"standalone and OSS", nil},
		{"primary writer", &fakeBackupCoordinator{primary: true, role: "writer"}},
		{"standby writer", &fakeBackupCoordinator{role: "writer"}},
		{"reader", &fakeBackupCoordinator{role: "reader"}},
		{"compactor", &fakeBackupCoordinator{role: "compactor"}},
		{"standalone role in cluster", &fakeBackupCoordinator{role: "standalone"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, app, roots := newClusterDeleteRig(t)
			seedClusterDeleteBackup(t, roots)
			if tc.coordinator != nil {
				handler.SetCoordinator(tc.coordinator)
			}
			status, body := requestClusterDelete(t, app, clusterDeleteBackupID)
			admitted := tc.coordinator == nil || tc.coordinator.primary
			if admitted {
				if status != fiber.StatusOK || body["backup_id"] != clusterDeleteBackupID || body["message"] != "Backup deleted" {
					t.Errorf("admitted delete: status=%d body=%v", status, body)
				}
			} else {
				assertClusterDeleteRejected(t, status, body, tc.coordinator.role)
			}
			assertClusterDeleteFiles(t, roots, admitted)
			if handler.activeOperation.Load() != nil {
				t.Error("delete left the admission slot occupied")
			}
		})
	}
}

func TestDeleteBackupRechecksPrimaryWriter(t *testing.T) {
	handler, app, roots := newClusterDeleteRig(t)
	coordinator := &fakeBackupCoordinator{role: "writer"}
	handler.SetCoordinator(coordinator)
	// The role string and handler stay the same: eligibility changes between
	// requests as a writer is promoted and then demoted without restarting.
	for _, primary := range []bool{false, true, false} {
		coordinator.primary = primary
		seedClusterDeleteBackup(t, roots)
		status, body := requestClusterDelete(t, app, clusterDeleteBackupID)
		if primary {
			if status != fiber.StatusOK {
				t.Errorf("promoted writer: status=%d body=%v", status, body)
			}
		} else {
			assertClusterDeleteRejected(t, status, body, "writer")
		}
		assertClusterDeleteFiles(t, roots, primary)
		if handler.activeOperation.Load() != nil {
			t.Fatal("request left the admission slot occupied")
		}
	}
}

func TestDeleteBackupGatePrecedesValidationAndAdmission(t *testing.T) {
	for _, primary := range []bool{false, true} {
		t.Run(fmt.Sprintf("primary=%t", primary), func(t *testing.T) {
			handler, app, roots := newClusterDeleteRig(t)
			seedClusterDeleteBackup(t, roots)
			handler.SetCoordinator(&fakeBackupCoordinator{primary: primary, role: "writer"})
			operation := "restore"
			handler.activeOperation.Store(&operation)
			for _, id := range []string{"invalid", clusterDeleteBackupID} {
				status, body := requestClusterDelete(t, app, id)
				if !primary {
					assertClusterDeleteRejected(t, status, body, "writer")
				} else if id == "invalid" {
					if status != fiber.StatusBadRequest {
						t.Errorf("invalid id: status=%d body=%v", status, body)
					}
				} else if status != fiber.StatusConflict || body["operation"] != "restore" {
					t.Errorf("busy primary: status=%d body=%v", status, body)
				}
				if handler.activeOperation.Load() != &operation {
					t.Error("refused delete changed another operation's slot")
				}
				assertClusterDeleteFiles(t, roots, false)
			}
		})
	}
}
