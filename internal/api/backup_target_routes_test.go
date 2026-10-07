package api

// API-layer behaviour of a configurable backup destination (#1085 stage
// B2b-1): the include_config default for a remote target, and the
// not-found/unreachable split that the old single 404 and the old Debug
// fall-through hid.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// targetRig is a backup API over a manager whose destination is a declared
// target. The backend is a directory either way — a real object store needs a
// network and a credential — and Target.Remote is the flag the handler
// actually reads, so declaring it is what the test is about.
type targetRig struct {
	app     *fiber.App
	manager *backup.Manager
	destDir string // the local directory the target writes into, "" when remote
}

func newTargetRig(t *testing.T, name string, remote bool, configPath string) *targetRig {
	t.Helper()
	return newTargetRigFull(t, name, remote, configPath, zerolog.Nop(), "")
}

func newTargetRigLogging(t *testing.T, name string, remote bool, configPath string, logger zerolog.Logger) *targetRig {
	t.Helper()
	return newTargetRigFull(t, name, remote, configPath, logger, "")
}

// newTargetRigIdentified gives the manager a backup owner identity, which it
// needs before the owner filter does anything at all: an instance with no
// identity owns every manifest it can see (the upgrade rule), so a rig without
// one cannot filter.
func newTargetRigIdentified(t *testing.T, name string, remote bool, instanceID string) *targetRig {
	t.Helper()
	return newTargetRigFull(t, name, remote, "", zerolog.Nop(), instanceID)
}

func newTargetRigFull(t *testing.T, name string, remote bool, configPath string, logger zerolog.Logger, instanceID string) *targetRig {
	t.Helper()

	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("create data storage: %v", err)
	}
	t.Cleanup(func() { dataStorage.Close() })
	if err := dataStorage.Write(context.Background(), "db/cpu/2026/10/07/00/a.parquet", []byte("PAR1")); err != nil {
		t.Fatalf("seed data storage: %v", err)
	}

	destDir := t.TempDir()
	manager, err := backup.NewManager(&backup.ManagerConfig{
		DataStorage: dataStorage,
		Target: &backup.Target{
			Name:   name,
			Spec:   storage.BackendSpec{Type: "local", LocalPath: destDir},
			Remote: remote,
		},
		InstanceID: instanceID,
		ConfigPath: configPath,
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("create backup manager: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	handler := NewBackupHandler(manager, nil, 2*time.Hour, zerolog.Nop())
	handler.RegisterRoutes(app)
	t.Cleanup(func() { app.Shutdown() })
	return &targetRig{app: app, manager: manager, destDir: destDir}
}

// writeForeignManifest drops a manifest another instance would have written
// straight into the destination directory. Written through the filesystem
// rather than through an exported test hook, so the production Manager keeps
// no method that exists only for tests.
func (r *targetRig) writeForeignManifest(t *testing.T, id, owner string) {
	t.Helper()
	if r.destDir == "" {
		t.Fatal("this rig has no local destination directory")
	}
	dir := filepath.Join(r.destDir, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	body := `{"version":"1","backup_id":"` + id + `","created_at":"2026-01-01T01:01:01Z","backup_type":"full","databases":[],"total_files":0,"total_size_bytes":0,"owner_instance_id":"` + owner + `"}`
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (r *targetRig) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	// 60 s, not the usual 5 s: one of these rigs points at a dead endpoint on
	// purpose, and the AWS SDK's retry ladder against a refused connection
	// takes a few seconds — more under -race with the rest of the package
	// competing for CPU, which is how a 5 s client timeout here failed the
	// FULL-package race run while passing -count=20 on its own. The client has
	// to outlast the handler's own 30 s context, or the test measures its own
	// patience instead of the handler's bound.
	resp, err := r.app.Test(req, 60_000)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	payload := map[string]any{}
	_ = json.Unmarshal(raw, &payload)
	return resp.StatusCode, payload
}

func (r *targetRig) waitForBackup(t *testing.T) *backup.Manifest {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		p := r.manager.GetProgress()
		if p != nil && p.Status != "running" {
			if p.Status != "completed" {
				t.Fatalf("backup status = %q, error = %q", p.Status, p.Error)
			}
			m, err := r.manager.GetBackup(context.Background(), p.BackupID)
			if err != nil {
				t.Fatalf("GetBackup(%s): %v", p.BackupID, err)
			}
			return m
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("backup did not finish")
	return nil
}

// TestIncludeConfigDefaultsOffForARemoteTarget: arc.toml holds the target's
// own credentials (backup.targets.<name>.s3_secret_key and friends), so the
// default must not copy it into the store those credentials unlock. The old
// default was "true unless scoped", and on a remote target that silently put
// the keys to the backup store inside every backup held in it.
func TestIncludeConfigDefaultsOffForARemoteTarget(t *testing.T) {
	configPath := writeTempConfig(t)

	t.Run("remote target", func(t *testing.T) {
		rig := newTargetRig(t, "audit", true, configPath)
		if status, payload := rig.do(t, http.MethodPost, "/api/v1/backup", `{}`); status != fiber.StatusAccepted {
			t.Fatalf("POST /api/v1/backup = %d %v, want 202", status, payload)
		}
		if m := rig.waitForBackup(t); m.HasConfig {
			t.Error("manifest has_config = true for a remote target with no include_config in the request, want false")
		}
	})

	t.Run("local target", func(t *testing.T) {
		rig := newTargetRig(t, "nearby", false, configPath)
		if status, payload := rig.do(t, http.MethodPost, "/api/v1/backup", `{}`); status != fiber.StatusAccepted {
			t.Fatalf("POST /api/v1/backup = %d %v, want 202", status, payload)
		}
		if m := rig.waitForBackup(t); !m.HasConfig {
			t.Error("manifest has_config = false for a LOCAL target, want true: a local directory is not what the credentials unlock, so the old default stands")
		}
	})

	t.Run("remote target, asked for explicitly", func(t *testing.T) {
		// The default is safe, not a refusal — AND the override is warned
		// about, which the matrix row claimed and no test checked. Warn rather
		// than Debug on purpose: a Debug line reaches no operator at default
		// levels, and this is a credential-shaped outcome the operator chose.
		var logs bytes.Buffer
		rig := newTargetRigLogging(t, "audit", true, configPath, zerolog.New(&logs))
		if status, payload := rig.do(t, http.MethodPost, "/api/v1/backup", `{"include_config":true}`); status != fiber.StatusAccepted {
			t.Fatalf("POST /api/v1/backup = %d %v, want 202", status, payload)
		}
		if m := rig.waitForBackup(t); !m.HasConfig {
			t.Error("manifest has_config = false although the request asked for it; the default is safe, not a refusal")
		}
		out := logs.String()
		if !strings.Contains(out, `"level":"warn"`) || !strings.Contains(out, "Copying arc.toml into a remote backup target") {
			t.Errorf("no warning for an explicit include_config on a remote target; got: %s", out)
		}
		if !strings.Contains(out, `"target":"audit"`) {
			t.Errorf("the warning does not name the target; got: %s", out)
		}
	})
}

// writeTempConfig drops a stand-in arc.toml so backupConfig has something to
// copy. Its contents are irrelevant; only whether it was copied is.
func writeTempConfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/arc.toml"
	if err := os.WriteFile(path, []byte("[backup]\nenabled = true\n"), 0o600); err != nil {
		t.Fatalf("write arc.toml: %v", err)
	}
	return path
}

// newUnreachableTargetRig builds the API over a manager whose default target
// is a REAL S3 backend pointed at a closed port on the loopback interface.
//
// Not a fake backend: the matrix row this covers is "default target
// unreachable at boot", and the two things it claims are that NewManager
// SUCCEEDS (so the API comes up at all) and that each operation then fails
// naming the target. A fake would prove the second and assume the first, and
// the first is the one that depends on real backend behaviour — NewS3Backend
// probes the bucket and only WARNS when the probe fails, so construction has
// to survive a dead endpoint. Static credentials, so the SDK never reaches for
// the instance-metadata service, which on a cloud runner can be slow rather
// than refused.
func newUnreachableTargetRig(t *testing.T, name string) *targetRig {
	t.Helper()

	dataStorage, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("create data storage: %v", err)
	}
	t.Cleanup(func() { dataStorage.Close() })

	// A port nothing listens on: bound, read, released.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	endpoint := ln.Addr().String()
	ln.Close()

	manager, err := backup.NewManager(&backup.ManagerConfig{
		DataStorage: dataStorage,
		Target: &backup.Target{
			Name:   name,
			Remote: true,
			Spec: storage.BackendSpec{
				Type: "s3",
				S3: storage.S3Config{
					Bucket:    "acme-arc-backups",
					Region:    "us-east-1",
					Endpoint:  endpoint,
					AccessKey: "test-access-key",
					SecretKey: "test-secret-key",
					PathStyle: true,
				},
			},
		},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewManager with an unreachable target = %v; it must succeed so the backup API comes up and reports per operation", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	handler := NewBackupHandler(manager, nil, 2*time.Hour, zerolog.Nop())
	handler.RegisterRoutes(app)
	t.Cleanup(func() { app.Shutdown() })
	return &targetRig{app: app, manager: manager}
}

// TestUnreachableTargetIsReportedPerOperationNamingTheTarget is the shape the
// unreachable-target matrix row fixes: the API comes up, and each operation
// says which destination is unreachable rather than hanging or claiming the
// backup does not exist.
//
// 503 and not 404 for the listing and the manifest read, because once a
// destination can be remote this is a transient far more often than a defect,
// and "Backup not found" for a transient tells an operator their backup is
// gone.
func TestUnreachableTargetIsReportedPerOperationNamingTheTarget(t *testing.T) {
	const id = "backup-20261007-120000-abcdef01"

	// One rig for all three operations: construction is the expensive part
	// (NewS3Backend probes the bucket), the three requests are read-only, and
	// none of them starts an operation, so they cannot interfere. Each still
	// exercises a different handler.
	rig := newUnreachableTargetRig(t, "audit")

	t.Run("list", func(t *testing.T) {
		start := time.Now()
		status, payload := rig.do(t, http.MethodGet, "/api/v1/backup", "")
		// Bounded by the HANDLER's context, not hanging. 40 s is the handler's
		// own 30 s bound plus slack: the claim is that something stops the
		// call, not that the SDK is fast. A refused connection in fact fails
		// in a couple of seconds; a black-holed endpoint would ride the 30 s
		// context to the end, and that is still a bound.
		if elapsed := time.Since(start); elapsed > 40*time.Second {
			t.Errorf("GET /api/v1/backup took %s against a dead endpoint; it must fail, not hang", elapsed)
		}
		if status != fiber.StatusServiceUnavailable {
			t.Fatalf("GET /api/v1/backup = %d %v, want 503", status, payload)
		}
		assertNamesTarget(t, payload, "audit")
	})

	t.Run("get", func(t *testing.T) {
		status, payload := rig.do(t, http.MethodGet, "/api/v1/backup/"+id, "")
		if status != fiber.StatusServiceUnavailable {
			t.Fatalf("GET /api/v1/backup/:id = %d %v, want 503 (not 404: the manifest may well exist)", status, payload)
		}
		assertNamesTarget(t, payload, "audit")
	})

	t.Run("restore", func(t *testing.T) {
		status, payload := rig.do(t, http.MethodPost, "/api/v1/backup/restore",
			`{"backup_id":"`+id+`","confirm":true,"restore_metadata":false}`)
		if status != fiber.StatusServiceUnavailable {
			t.Fatalf("POST /api/v1/backup/restore = %d %v, want 503 rather than a 202 whose echoed mode is a guess", status, payload)
		}
		assertNamesTarget(t, payload, "audit")
		if mode, ok := payload["mode"]; ok {
			t.Errorf("the 503 echoes mode=%v; the point of the error is that the mode could not be resolved", mode)
		}
		if p := rig.manager.GetProgress(); p != nil {
			t.Errorf("a restore was started (progress %+v); the request must be refused before any work", p)
		}
	})
}

// TestAnAbsentManifestStillAdmitsTheRestore: an unknown id is a different
// answer from an unreadable destination and keeps the behaviour it has — the
// manager fails the run asynchronously with "backup not found". Only "the
// manifest may exist and I could not reach it" gets the 503.
func TestAnAbsentManifestStillAdmitsTheRestore(t *testing.T) {
	rig := newTargetRig(t, "audit", true, "")
	status, payload := rig.do(t, http.MethodPost, "/api/v1/backup/restore",
		`{"backup_id":"backup-20261007-120000-abcdef01","confirm":true,"restore_metadata":false}`)
	if status != fiber.StatusAccepted {
		t.Fatalf("POST /api/v1/backup/restore for an unknown id = %d %v, want 202 and an asynchronous failure", status, payload)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if p := rig.manager.GetProgress(); p != nil && p.Status == "failed" {
			if !strings.Contains(p.Error, "backup not found") {
				t.Errorf("progress error = %q, want it to read \"backup not found\"", p.Error)
			}
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("the restore of an unknown backup id did not fail asynchronously")
}

// TestGetBackupStillAnswers404ForAnUnknownID: the 503 must not swallow the
// honest 404, or a typo would read as an outage.
func TestGetBackupStillAnswers404ForAnUnknownID(t *testing.T) {
	rig := newTargetRig(t, "audit", true, "")
	status, payload := rig.do(t, http.MethodGet, "/api/v1/backup/backup-20261007-120000-abcdef01", "")
	if status != fiber.StatusNotFound {
		t.Fatalf("GET /api/v1/backup/:id for an unknown id = %d %v, want 404", status, payload)
	}
}

func assertNamesTarget(t *testing.T, payload map[string]any, name string) {
	t.Helper()
	if got, _ := payload["target"].(string); got != name {
		t.Errorf("response target = %q, want %q", got, name)
	}
	msg, _ := payload["error"].(string)
	if !strings.Contains(msg, `backup target "`+name+`"`) {
		t.Errorf("response error = %q, want it to name backup target %q", msg, name)
	}
	// The destination's own SDK wording must not reach the response: it is the
	// thing installErrSanitizer masks in logs, and it is not an operator-facing
	// explanation.
	if strings.Contains(msg, "dial tcp") {
		t.Errorf("response error = %q, want it to exclude the transport error", msg)
	}
}

// TestTheListingAnnouncesWhatTheOwnerFilterWithheld is the discoverability
// half of H4. Recovery onto fresh hardware is the case backups exist for, and
// there every backup at the destination reads as foreign — so an empty array
// with no explanation is the answer an operator gets at the worst moment, and
// the flag that fixes it was documented only in arc.toml.
func TestTheListingAnnouncesWhatTheOwnerFilterWithheld(t *testing.T) {
	rig := newTargetRigIdentified(t, "audit", false, "the-replacement-instance")

	// Two backups written by the instance this one replaced.
	for _, id := range []string{"backup-20260101-010101-aaaa1111", "backup-20260102-010101-bbbb2222"} {
		rig.writeForeignManifest(t, id, "the-decommissioned-instance")
	}

	status, payload := rig.do(t, http.MethodGet, "/api/v1/backup", "")
	if status != fiber.StatusOK {
		t.Fatalf("GET /api/v1/backup = %d %v, want 200", status, payload)
	}
	if n, _ := payload["count"].(float64); n != 0 {
		t.Errorf("count = %v, want 0", payload["count"])
	}
	n, ok := payload["filtered_foreign"].(float64)
	if !ok || n != 2 {
		t.Fatalf("filtered_foreign = %v, want 2: an empty listing must say what it withheld", payload["filtered_foreign"])
	}
	hint, _ := payload["hint"].(string)
	if !strings.Contains(hint, "include_foreign=true") {
		t.Errorf("hint = %q, want it to name the flag that makes recovery reachable", hint)
	}

	// And the opt-in returns them, so the hint is actionable.
	status, payload = rig.do(t, http.MethodGet, "/api/v1/backup?include_foreign=true", "")
	if status != fiber.StatusOK {
		t.Fatalf("GET with include_foreign = %d %v, want 200", status, payload)
	}
	if n, _ := payload["count"].(float64); n != 2 {
		t.Errorf("include_foreign count = %v, want 2", payload["count"])
	}
	if _, present := payload["filtered_foreign"]; present {
		t.Error("include_foreign=true still reports filtered_foreign, want it absent: nothing was withheld")
	}
}

// TestTheListingOmitsTheFilteredCountWhenNothingWasWithheld keeps the ordinary
// response unchanged, so every existing client sees exactly what it did.
func TestTheListingOmitsTheFilteredCountWhenNothingWasWithheld(t *testing.T) {
	rig := newTargetRig(t, "audit", false, "")
	status, payload := rig.do(t, http.MethodGet, "/api/v1/backup", "")
	if status != fiber.StatusOK {
		t.Fatalf("GET /api/v1/backup = %d %v, want 200", status, payload)
	}
	for _, key := range []string{"filtered_foreign", "hint"} {
		if _, present := payload[key]; present {
			t.Errorf("response carries %q with nothing withheld, want it absent", key)
		}
	}
}
