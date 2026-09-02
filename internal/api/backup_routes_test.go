package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type controlledBackupStorage struct {
	storage.Backend

	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	listErr   error
	listCalls atomic.Int32
}

func (s *controlledBackupStorage) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	s.listCalls.Add(1)
	s.startOnce.Do(func() { close(s.started) })

	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
}

type backupRouteRig struct {
	app     *fiber.App
	storage *controlledBackupStorage
	handler *BackupHandler
}

type backupRouteResponse struct {
	status int
	err    error
}

func newBackupRouteRig(t *testing.T, listErr error) *backupRouteRig {
	t.Helper()

	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("create data storage: %v", err)
	}
	t.Cleanup(func() { dataStorage.Close() })

	controlled := &controlledBackupStorage{
		Backend: dataStorage,
		started: make(chan struct{}),
		release: make(chan struct{}),
		listErr: listErr,
	}
	manager, err := backup.NewManager(&backup.ManagerConfig{
		DataStorage: controlled,
		BackupPath:  t.TempDir(),
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("create backup manager: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	handler := NewBackupHandler(manager, nil, zerolog.Nop())
	handler.RegisterRoutes(app)
	t.Cleanup(func() { app.Shutdown() })

	return &backupRouteRig{app: app, storage: controlled, handler: handler}
}

func (r *backupRouteRig) post(path, body string) (int, error) {
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, 5_000)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func (r *backupRouteRig) mustPost(t *testing.T, path, body string) int {
	t.Helper()

	status, err := r.post(path, body)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return status
}

func waitForOperationRelease(t *testing.T, handler *BackupHandler) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if handler.activeOperation.Load() == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("backup operation did not release the admission slot")
}

func waitForListCalls(t *testing.T, storage *controlledBackupStorage, want int32) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if storage.listCalls.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("ListObjects calls=%d, want at least %d", storage.listCalls.Load(), want)
}

func TestBackupHandlerRejectsConcurrentBackups(t *testing.T) {
	rig := newBackupRouteRig(t, nil)

	const requests = 16
	start := make(chan struct{})
	responses := make(chan backupRouteResponse, requests)
	var workers sync.WaitGroup
	workers.Add(requests)
	for range requests {
		go func() {
			defer workers.Done()
			<-start
			status, err := rig.post("/api/v1/backup/", `{}`)
			responses <- backupRouteResponse{status: status, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(responses)

	accepted := 0
	conflicts := 0
	for response := range responses {
		if response.err != nil {
			t.Errorf("concurrent backup request: %v", response.err)
			continue
		}
		switch response.status {
		case fiber.StatusAccepted:
			accepted++
		case fiber.StatusConflict:
			conflicts++
		default:
			t.Errorf("unexpected response status: %d", response.status)
		}
	}
	if accepted != 1 || conflicts != requests-1 {
		t.Fatalf("accepted=%d conflicts=%d, want 1 and %d", accepted, conflicts, requests-1)
	}

	select {
	case <-rig.storage.started:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted backup did not reach storage discovery")
	}
	close(rig.storage.release)
	waitForOperationRelease(t, rig.handler)
	if calls := rig.storage.listCalls.Load(); calls != 1 {
		t.Fatalf("ListObjects calls=%d, want 1; a rejected backup was queued", calls)
	}
}

func TestBackupHandlerSharesAdmissionWithRestoreAndReleasesIt(t *testing.T) {
	rig := newBackupRouteRig(t, nil)

	if status := rig.mustPost(t, "/api/v1/backup/", `{}`); status != fiber.StatusAccepted {
		t.Fatalf("initial backup status=%d, want %d", status, fiber.StatusAccepted)
	}
	select {
	case <-rig.storage.started:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted backup did not reach storage discovery")
	}

	restoreBody := `{"backup_id":"backup-20260820-120000-deadbeef","confirm":true}`
	if status := rig.mustPost(t, "/api/v1/backup/restore", restoreBody); status != fiber.StatusConflict {
		t.Fatalf("competing restore status=%d, want %d", status, fiber.StatusConflict)
	}

	close(rig.storage.release)
	waitForOperationRelease(t, rig.handler)
	if status := rig.mustPost(t, "/api/v1/backup/", `{}`); status != fiber.StatusAccepted {
		t.Fatalf("backup after completion status=%d, want %d", status, fiber.StatusAccepted)
	}

	waitForListCalls(t, rig.storage, 2)
	if calls := rig.storage.listCalls.Load(); calls != 2 {
		t.Fatalf("ListObjects calls=%d, want 2 after later backup", calls)
	}
	waitForOperationRelease(t, rig.handler)
}

func TestBackupHandlerReleasesAdmissionAfterFailure(t *testing.T) {
	rig := newBackupRouteRig(t, errors.New("storage discovery failed"))
	close(rig.storage.release)

	if status := rig.mustPost(t, "/api/v1/backup/", `{}`); status != fiber.StatusAccepted {
		t.Fatalf("initial backup status=%d, want %d", status, fiber.StatusAccepted)
	}
	waitForOperationRelease(t, rig.handler)

	if status := rig.mustPost(t, "/api/v1/backup/", `{}`); status != fiber.StatusAccepted {
		t.Fatalf("backup after failure status=%d, want %d", status, fiber.StatusAccepted)
	}

	waitForListCalls(t, rig.storage, 2)
	if calls := rig.storage.listCalls.Load(); calls != 2 {
		t.Fatalf("ListObjects calls=%d, want 2 after retry", calls)
	}
	waitForOperationRelease(t, rig.handler)
}

func (r *backupRouteRig) delete(t *testing.T, path string) int {
	t.Helper()

	req := httptest.NewRequest(http.MethodDelete, path, nil)
	resp, err := r.app.Test(req, 5_000)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// #626: DELETE shares the backup/restore admission slot. Deleting while a
// backup runs is refused with 409; once the operation finishes, a real backup
// deletes cleanly and the slot is free for the next operation.
func TestDeleteBackupSharesAdmissionSlot(t *testing.T) {
	rig := newBackupRouteRig(t, nil)

	if status := rig.mustPost(t, "/api/v1/backup/", `{}`); status != fiber.StatusAccepted {
		t.Fatalf("initial backup status=%d, want %d", status, fiber.StatusAccepted)
	}
	select {
	case <-rig.storage.started:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted backup did not reach storage discovery")
	}

	if status := rig.delete(t, "/api/v1/backup/backup-20260820-120000-deadbeef"); status != fiber.StatusConflict {
		t.Fatalf("delete during running backup status=%d, want %d", status, fiber.StatusConflict)
	}

	close(rig.storage.release)
	waitForOperationRelease(t, rig.handler)

	// The backup that just completed is a real, deletable backup.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	backups, err := rig.handler.manager.ListBackups(ctx)
	if err != nil || len(backups) != 1 {
		t.Fatalf("ListBackups = %v backups, err %v; want exactly 1", len(backups), err)
	}
	if status := rig.delete(t, "/api/v1/backup/"+backups[0].BackupID); status != fiber.StatusOK {
		t.Fatalf("delete after completion status=%d, want %d", status, fiber.StatusOK)
	}

	// The delete released the slot: the next operation is admitted.
	waitForOperationRelease(t, rig.handler)
	if status := rig.mustPost(t, "/api/v1/backup/", `{}`); status != fiber.StatusAccepted {
		t.Fatalf("backup after delete status=%d, want %d", status, fiber.StatusAccepted)
	}
	waitForListCalls(t, rig.storage, 2)
	waitForOperationRelease(t, rig.handler)
}

// The restore response must tell operators when a restart is needed: restored
// databases replace the live files by rename and a restored config only takes
// effect on reload. A config-only restore is explicitly restart-required too,
// and a data-only restore is not.
func TestRestoreBackupReportsRestartRequired(t *testing.T) {
	rig := newBackupRouteRig(t, nil)
	close(rig.storage.release)

	restore := func(t *testing.T, body string) map[string]any {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/backup/restore", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := rig.app.Test(req, 5_000)
		if err != nil {
			t.Fatalf("POST restore: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != fiber.StatusAccepted {
			t.Fatalf("restore status = %d, want 202", resp.StatusCode)
		}
		var payload map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("decode restore response: %v", err)
		}
		waitForOperationRelease(t, rig.handler)
		return payload
	}

	meta := restore(t, `{"backup_id":"backup-20260901-000000-00000000","confirm":true,"restore_data":false,"restore_metadata":true}`)
	if meta["restart_required"] != true {
		t.Errorf("metadata restore: restart_required = %v, want true", meta["restart_required"])
	}
	cfg := restore(t, `{"backup_id":"backup-20260901-000000-00000000","confirm":true,"restore_data":false,"restore_metadata":false,"restore_config":true}`)
	if cfg["restart_required"] != true {
		t.Errorf("config-only restore: restart_required = %v, want true", cfg["restart_required"])
	}
	data := restore(t, `{"backup_id":"backup-20260901-000000-00000000","confirm":true,"restore_data":true,"restore_metadata":false,"restore_config":false}`)
	if _, present := data["restart_required"]; present {
		t.Errorf("data-only restore: restart_required = %v, want absent", data["restart_required"])
	}
}
