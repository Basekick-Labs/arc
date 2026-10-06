package api

// Tests for #1083: the backup API on a cluster node. A fake coordinator
// stands in for the cluster; the manager underneath has no manifest hook, so
// an admitted operation runs the standalone path and releases the slot.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

type fakeBackupCoordinator struct {
	primary bool
	role    string
}

func (f *fakeBackupCoordinator) IsPrimaryWriter() bool { return f.primary }
func (f *fakeBackupCoordinator) Role() string          { return f.role }

func (r *backupRouteRig) postJSON(t *testing.T, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, 5_000)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	payload := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("decode %s response %q: %v", path, raw, err)
		}
	}
	return resp.StatusCode, payload
}

// settle lets an admitted operation finish so the next request is not a 409.
func (r *backupRouteRig) settle(t *testing.T) {
	t.Helper()
	select {
	case <-r.storage.started:
	case <-time.After(5 * time.Second):
	}
	waitForOperationRelease(t, r.handler)
}

// Every node that is not the primary writer answers 503 on both endpoints
// with the delete API's wording; the primary and a standalone node (no
// coordinator) are admitted. The rejected request takes no slot and does no
// listing.
func TestBackupHandlerGatesOnPrimaryWriter(t *testing.T) {
	const restoreBody = `{"backup_id":"backup-20260901-000000-00000000","confirm":true,"restore_metadata":false}`
	cases := []struct {
		name        string
		coordinator *fakeBackupCoordinator
		want        int
	}{
		{"no coordinator", nil, fiber.StatusAccepted},
		{"primary writer", &fakeBackupCoordinator{primary: true, role: "writer"}, fiber.StatusAccepted},
		{"standby writer", &fakeBackupCoordinator{primary: false, role: "writer"}, fiber.StatusServiceUnavailable},
		{"reader", &fakeBackupCoordinator{primary: false, role: "reader"}, fiber.StatusServiceUnavailable},
		{"compactor", &fakeBackupCoordinator{primary: false, role: "compactor"}, fiber.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		for _, ep := range []struct{ name, path, body string }{
			{"backup", "/api/v1/backup/", `{}`},
			{"restore", "/api/v1/backup/restore", restoreBody},
		} {
			t.Run(tc.name+"/"+ep.name, func(t *testing.T) {
				rig := newBackupRouteRig(t, nil)
				close(rig.storage.release)
				if tc.coordinator != nil {
					rig.handler.SetCoordinator(tc.coordinator)
				}
				status, payload := rig.postJSON(t, ep.path, ep.body)
				if status != tc.want {
					t.Fatalf("status = %d, want %d (payload %v)", status, tc.want, payload)
				}
				if tc.want == fiber.StatusServiceUnavailable {
					msg, _ := payload["error"].(string)
					if !strings.Contains(msg, ep.name+" rejected") || !strings.Contains(msg, "route to the primary writer") || !strings.Contains(msg, tc.coordinator.role) {
						t.Errorf("error = %q, want the delete API shape naming the operation, the role and the primary writer", msg)
					}
					if rig.handler.activeOperation.Load() != nil {
						t.Error("a rejected request holds the admission slot")
					}
					if rig.storage.listCalls.Load() != 0 {
						t.Error("a rejected backup reached storage discovery")
					}
					return
				}
				if ep.name == "backup" {
					rig.settle(t)
					if rig.storage.listCalls.Load() != 1 {
						t.Errorf("admitted backup made %d listings, want 1", rig.storage.listCalls.Load())
					}
				} else {
					waitForOperationRelease(t, rig.handler)
				}
			})
		}
	}
}

// SetCoordinator ignores a nil interface so standalone wiring may call it
// unconditionally.
func TestBackupHandlerSetCoordinatorIgnoresNil(t *testing.T) {
	rig := newBackupRouteRig(t, nil)
	rig.handler.SetCoordinator(nil)
	if rig.handler.coordinator != nil {
		t.Fatal("nil coordinator stored")
	}
}

// On a cluster node restore_metadata defaults to false and an explicit true
// is refused, as is restore_config; a data-only restore is admitted and is
// not reported as staged or restart-required.
func TestRestoreBackupRefusesMetadataAndConfigOnACluster(t *testing.T) {
	rig := newBackupRouteRig(t, nil)
	close(rig.storage.release)
	rig.handler.SetCoordinator(&fakeBackupCoordinator{primary: true, role: "writer"})

	const id = `"backup_id":"backup-20260901-000000-00000000","confirm":true`
	status, payload := rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`,"restore_metadata":true}`)
	if status != fiber.StatusBadRequest || !strings.Contains(payload["error"].(string), "restore_metadata is not available on a cluster node") {
		t.Errorf("restore_metadata=true: status %d payload %v, want 400 naming the refusal", status, payload)
	}
	status, payload = rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`,"restore_config":true}`)
	if status != fiber.StatusBadRequest || !strings.Contains(payload["error"].(string), "restore_config is not available on a cluster node") {
		t.Errorf("restore_config=true: status %d payload %v, want 400 naming the refusal", status, payload)
	}
	if rig.handler.activeOperation.Load() != nil {
		t.Fatal("a refused restore holds the admission slot")
	}

	// The default body is admitted: restore_metadata defaults to false here.
	status, payload = rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`}`)
	if status != fiber.StatusAccepted {
		t.Fatalf("default restore: status %d payload %v, want 202", status, payload)
	}
	if _, staged := payload["staged"]; staged {
		t.Error("default cluster restore reports staged; metadata must default to off")
	}
	if _, restart := payload["restart_required"]; restart {
		t.Error("default cluster restore reports restart_required")
	}
	if payload["mode"] != "merge" {
		t.Errorf("mode = %v, want merge", payload["mode"])
	}
	waitForOperationRelease(t, rig.handler)
}

// mode is validated: unknown spellings are 400, replace is 400 without a
// coordinator and admitted with one, and the response echoes the mode.
func TestRestoreBackupModeValidation(t *testing.T) {
	const id = `"backup_id":"backup-20260901-000000-00000000","confirm":true,"restore_metadata":false`
	t.Run("standalone", func(t *testing.T) {
		rig := newBackupRouteRig(t, nil)
		close(rig.storage.release)
		status, payload := rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`,"mode":"overwrite"}`)
		if status != fiber.StatusBadRequest || !strings.Contains(payload["error"].(string), "invalid restore mode") {
			t.Errorf("mode=overwrite: status %d payload %v, want 400", status, payload)
		}
		status, payload = rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`,"mode":"replace"}`)
		if status != fiber.StatusBadRequest || !strings.Contains(payload["error"].(string), "only available on a cluster node") {
			t.Errorf("mode=replace standalone: status %d payload %v, want 400", status, payload)
		}
		status, payload = rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`,"mode":"merge"}`)
		if status != fiber.StatusAccepted || payload["mode"] != "merge" {
			t.Errorf("mode=merge: status %d payload %v, want 202 with mode merge", status, payload)
		}
		waitForOperationRelease(t, rig.handler)
	})
	t.Run("cluster", func(t *testing.T) {
		rig := newBackupRouteRig(t, nil)
		close(rig.storage.release)
		rig.handler.SetCoordinator(&fakeBackupCoordinator{primary: true, role: "writer"})
		status, payload := rig.postJSON(t, "/api/v1/backup/restore", `{`+id+`,"mode":"replace"}`)
		if status != fiber.StatusAccepted || payload["mode"] != "replace" {
			t.Errorf("mode=replace cluster: status %d payload %v, want 202 with mode replace", status, payload)
		}
		waitForOperationRelease(t, rig.handler)
	})
}
