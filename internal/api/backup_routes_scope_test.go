package api

// Tests for #1084: POST /api/v1/backup with `databases`, and the restore mode
// a scoped backup resolves to.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// scopeRouteRig is a handler over a plain local backend (which implements
// storage.PrefixProber, so the known-database check probes rather than
// lists), with the SQLite metadata and arc.toml wired so the recorded
// manifest shows which defaults the handler applied. The handler holds the
// concrete manager, so the manifest stands in for a fake.
type scopeRouteRig struct {
	app     *fiber.App
	handler *BackupHandler
	manager *backup.Manager
}

func newScopeRouteRig(t *testing.T) *scopeRouteRig {
	t.Helper()
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("data storage: %v", err)
	}
	t.Cleanup(func() { data.Close() })
	ctx := context.Background()
	for _, k := range []string{"a/cpu/2026/01/01/00/a1.parquet", "b/cpu/2026/01/01/00/b1.parquet", "_schema/a/cpu.parquet"} {
		if err := data.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	sqlitePath := filepath.Join(t.TempDir(), "arc.db")
	db, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	configPath := filepath.Join(t.TempDir(), "arc.toml")
	if err := os.WriteFile(configPath, []byte("[server]\nport = 8000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := backup.NewManager(&backup.ManagerConfig{
		DataStorage:  data,
		BackupPath:   t.TempDir(),
		SQLiteDBPath: sqlitePath,
		ConfigPath:   configPath,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("backup manager: %v", err)
	}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	handler := NewBackupHandler(manager, nil, zerolog.Nop())
	handler.RegisterRoutes(app)
	t.Cleanup(func() { app.Shutdown() })
	return &scopeRouteRig{app: app, handler: handler, manager: manager}
}

func (r *scopeRouteRig) post(t *testing.T, path, body string) (int, map[string]any) {
	t.Helper()
	return r.postWithType(t, path, body, "application/json")
}

// postWithType posts body under the given Content-Type; an empty contentType
// sends no header at all (curl --data-binary @file without -H).
func (r *scopeRouteRig) postWithType(t *testing.T, path, body, contentType string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
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

// backupAndRead posts a backup request, waits for the run, and returns the
// manifest it wrote.
func (r *scopeRouteRig) backupAndRead(t *testing.T, body string) (map[string]any, *backup.Manifest) {
	t.Helper()
	status, payload := r.post(t, "/api/v1/backup/", body)
	if status != fiber.StatusAccepted {
		t.Fatalf("POST backup %s: status %d payload %v, want 202", body, status, payload)
	}
	waitForOperationRelease(t, r.handler)
	p := r.manager.GetProgress()
	if p == nil || p.Status != "completed" {
		t.Fatalf("backup %s did not complete: %+v", body, p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mf, err := r.manager.GetBackup(ctx, p.BackupID)
	if err != nil {
		t.Fatalf("GetBackup(%s): %v", p.BackupID, err)
	}
	return payload, mf
}

// Every malformed scope is a 400 that names the offending value, before any
// work and without taking the admission slot; the metadata refusal and the
// unknown-database check are 400s too.
func TestCreateBackupScopeValidation(t *testing.T) {
	rig := newScopeRouteRig(t)
	cases := []struct{ name, body, want string }{
		{"separator in a name", `{"databases":["bad/name"]}`, `"bad/name"`},
		{"backslash in a name", `{"databases":["bad\\name"]}`, `"bad\\name"`},
		{"dot-prefixed name", `{"databases":[".hidden"]}`, `".hidden"`},
		{"parent segment", `{"databases":[".."]}`, `".."`},
		{"empty name", `{"databases":[""]}`, "empty name"},
		{"duplicate", `{"databases":["a","a"]}`, `"a" more than once`},
		{"reserved root", `{"databases":["_schema"]}`, `"_schema", which is a reserved storage root`},
		{"unknown database", `{"databases":["zzz"]}`, `unknown database "zzz": no data files, no schema anchors and no tier rows`},
		{"metadata on a scoped backup", `{"databases":["a"],"include_metadata":true}`, "include_metadata is not available on a scoped backup"},
		{"malformed databases", `{"databases":"a"}`, "Invalid request body"},
		{"malformed json", `{"databases":[`, "Invalid request body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, payload := rig.post(t, "/api/v1/backup/", tc.body)
			msg, _ := payload["error"].(string)
			if status != fiber.StatusBadRequest || !strings.Contains(msg, tc.want) {
				t.Errorf("status %d error %q, want 400 containing %q", status, msg, tc.want)
			}
			if rig.handler.activeOperation.Load() != nil {
				t.Error("a refused backup holds the admission slot")
			}
			if tc.name == "unknown database" {
				if names, _ := payload["unknown_databases"].([]any); len(names) != 1 || names[0] != "zzz" {
					t.Errorf("unknown_databases = %v, want [zzz]", payload["unknown_databases"])
				}
			}
		})
	}
	t.Run("too many names", func(t *testing.T) {
		names := make([]string, 0, maxScopeDatabases+1)
		for i := 0; i <= maxScopeDatabases; i++ {
			names = append(names, fmt.Sprintf(`"d%03d"`, i))
		}
		status, payload := rig.post(t, "/api/v1/backup/", `{"databases":[`+strings.Join(names, ",")+`]}`)
		if msg, _ := payload["error"].(string); status != fiber.StatusBadRequest || !strings.Contains(msg, "at most 256") {
			t.Errorf("status %d error %q, want 400 naming the cap", status, msg)
		}
	})
	t.Run("long name is refused as a name, not probed", func(t *testing.T) {
		long := strings.Repeat("n", storage.MaxUsableKeySegmentLen+1)
		status, payload := rig.post(t, "/api/v1/backup/", `{"databases":["`+long+`"]}`)
		if msg, _ := payload["error"].(string); status != fiber.StatusBadRequest || !strings.Contains(msg, "not a valid database name") {
			t.Errorf("status %d error %q, want 400 as an invalid name", status, msg)
		}
	})
	// A non-empty body that is not JSON is refused outright. The form type is
	// what curl -d sends by default; Fiber would form-parse the JSON text into
	// an empty request and run a whole-instance backup with metadata. No
	// Content-Type at all (curl --data-binary @file without -H) is refused the
	// same way.
	for _, ct := range []string{"application/x-www-form-urlencoded", "text/plain", ""} {
		t.Run("non-JSON body "+ct, func(t *testing.T) {
			status, payload := rig.postWithType(t, "/api/v1/backup/", `{"databases":["a"]}`, ct)
			if msg, _ := payload["error"].(string); status != fiber.StatusBadRequest || !strings.Contains(msg, "send JSON (Content-Type: application/json)") {
				t.Errorf("status %d error %q, want 400 asking for JSON", status, msg)
			}
			if rig.handler.activeOperation.Load() != nil || rig.manager.GetProgress() != nil {
				t.Error("a non-JSON body started a backup")
			}
		})
	}
}

// A scoped backup defaults include_metadata and include_config to false,
// echoes the sorted scope in the 202 body, and records it in the manifest;
// include_config may be switched on; an unscoped backup keeps its defaults
// and its manifest has no scope.
func TestCreateBackupScopedDefaultsAndEcho(t *testing.T) {
	rig := newScopeRouteRig(t)

	payload, mf := rig.backupAndRead(t, `{"databases":["b","a"]}`)
	if got, _ := payload["databases"].([]any); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("202 databases = %v, want [a b] (sorted)", payload["databases"])
	}
	if strings.Join(mf.Scope, ",") != "a,b" || mf.HasMetadata || mf.HasConfig || len(mf.Databases) != 2 || mf.BackupType != "full" {
		t.Errorf("scoped manifest: scope=%v metadata=%v config=%v databases=%d type=%s", mf.Scope, mf.HasMetadata, mf.HasConfig, len(mf.Databases), mf.BackupType)
	}

	_, mf = rig.backupAndRead(t, `{"databases":["a"],"include_config":true}`)
	if !mf.HasConfig || mf.HasMetadata || strings.Join(mf.Scope, ",") != "a" {
		t.Errorf("scoped with include_config: config=%v metadata=%v scope=%v", mf.HasConfig, mf.HasMetadata, mf.Scope)
	}

	_, mf = rig.backupAndRead(t, `{"databases":["a"],"include_metadata":false}`)
	if mf.HasMetadata || mf.HasConfig {
		t.Errorf("scoped with explicit include_metadata false: metadata=%v config=%v", mf.HasMetadata, mf.HasConfig)
	}

	payload, mf = rig.backupAndRead(t, `{}`)
	if _, present := payload["databases"]; present {
		t.Errorf("unscoped 202 carries databases: %v", payload["databases"])
	}
	if mf.Scope != nil || !mf.HasMetadata || !mf.HasConfig || len(mf.Databases) != 2 {
		t.Errorf("unscoped manifest: scope=%v metadata=%v config=%v databases=%d", mf.Scope, mf.HasMetadata, mf.HasConfig, len(mf.Databases))
	}
	payload, mf = rig.backupAndRead(t, `{"databases":[]}`)
	if _, present := payload["databases"]; present || mf.Scope != nil || !mf.HasMetadata {
		t.Errorf("empty databases list must be unscoped: payload=%v scope=%v metadata=%v", payload, mf.Scope, mf.HasMetadata)
	}
}

// fakeRouteClusterManifest is the minimal backup.ClusterManifest for the
// restore echo test: an empty manifest that accepts every write and every
// pause, so a cluster restore runs to completion.
type fakeRouteClusterManifest struct{}

func (fakeRouteClusterManifest) Sync(context.Context) error           { return nil }
func (fakeRouteClusterManifest) ManifestFiles() []backup.ManifestFile { return nil }
func (fakeRouteClusterManifest) LocalNodeID() string                  { return "node-a" }
func (fakeRouteClusterManifest) BatchRegister(context.Context, []backup.ManifestFile) error {
	return nil
}
func (fakeRouteClusterManifest) BatchDelete(context.Context, []string, string) error { return nil }
func (fakeRouteClusterManifest) PauseCompaction(context.Context, string) (backup.CompactionPause, error) {
	return fakeRoutePause{}, nil
}

type fakeRoutePause struct{}

func (fakeRoutePause) Lost() (bool, error)          { return false, nil }
func (fakeRoutePause) Resume(context.Context) error { return nil }

// The 202 echoes the effective mode: a scoped backup with no mode named
// restores in replace mode only when the Raft manifest is wired on this
// node, which is what the manager runs from; a coordinator without the
// manifest, a standalone node, an explicit merge and an unscoped backup all
// resolve to merge, and an unknown backup id is still admitted (and fails
// asynchronously) as before.
func TestRestoreBackupEchoesTheResolvedMode(t *testing.T) {
	rig := newScopeRouteRig(t)
	ctx := context.Background()
	scopedRes, err := rig.manager.CreateBackup(ctx, backup.BackupOptions{Databases: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	// The API defaults of an unscoped backup: metadata and config included.
	unscopedRes, err := rig.manager.CreateBackup(ctx, backup.BackupOptions{IncludeMetadata: true, IncludeConfig: true})
	if err != nil {
		t.Fatal(err)
	}
	if !unscopedRes.Manifest.HasMetadata {
		t.Fatal("premise: the unscoped backup must carry the SQLite metadata")
	}
	scoped, unscoped := scopedRes.Manifest.BackupID, unscopedRes.Manifest.BackupID

	restore := func(t *testing.T, body string) (int, map[string]any) {
		t.Helper()
		status, payload := rig.post(t, "/api/v1/backup/restore", body)
		if status == fiber.StatusAccepted {
			waitForOperationRelease(t, rig.handler)
		}
		return status, payload
	}
	want := func(t *testing.T, body, mode string) {
		t.Helper()
		status, payload := restore(t, body)
		if status != fiber.StatusAccepted || payload["mode"] != mode {
			t.Errorf("%s: status %d mode %v, want 202 %s", body, status, payload["mode"], mode)
		}
	}
	id := func(backupID string) string {
		return `"backup_id":"` + backupID + `","confirm":true,"restore_metadata":false`
	}

	// Standalone: no coordinator, no manifest.
	want(t, `{`+id(scoped)+`}`, "merge")
	if p := rig.manager.GetProgress(); p.Mode != "merge" || p.Status != "completed" || len(p.Scope) != 1 || p.Scope[0] != "a" {
		t.Errorf("standalone scoped restore ran mode=%s status=%s scope=%v", p.Mode, p.Status, p.Scope)
	}
	// Standalone with restore_metadata at its default (true): a scoped
	// backup carries no metadata, so nothing is staged and no restart is
	// required; the unscoped backup of this rig does carry it.
	if status, payload := restore(t, `{"backup_id":"`+scoped+`","confirm":true}`); status != fiber.StatusAccepted || payload["restart_required"] != nil || payload["staged"] != nil {
		t.Errorf("scoped default restore: status %d payload %v, want 202 without restart_required or staged", status, payload)
	}
	if status, payload := restore(t, `{"backup_id":"`+unscoped+`","confirm":true}`); status != fiber.StatusAccepted || payload["restart_required"] != true || payload["staged"] != true {
		t.Errorf("unscoped default restore: status %d payload %v, want 202 with restart_required and staged", status, payload)
	}

	// A cluster node without cluster.raft_data_dir: coordinator wired, Raft
	// manifest not. The manager runs merge, so the echo must say merge, and
	// an explicit replace is refused here rather than failing asynchronously.
	rig.handler.SetCoordinator(&fakeBackupCoordinator{primary: true, role: "writer"})
	want(t, `{`+id(scoped)+`}`, "merge")
	if p := rig.manager.GetProgress(); p.Mode != "merge" {
		t.Errorf("coordinator-without-manifest scoped restore ran mode=%s", p.Mode)
	}
	if status, payload := restore(t, `{`+id(scoped)+`,"mode":"replace"}`); status != fiber.StatusBadRequest || !strings.Contains(payload["error"].(string), "only available on a cluster node") {
		t.Errorf("explicit replace without a Raft manifest: status %d payload %v, want 400", status, payload)
	}

	// A cluster node with the manifest: scoped and unnamed is replace, and
	// the manager runs replace.
	rig.manager.SetClusterManifest(fakeRouteClusterManifest{})
	want(t, `{`+id(scoped)+`}`, "replace")
	if p := rig.manager.GetProgress(); p.Mode != "replace" || p.Status != "completed" {
		t.Errorf("cluster scoped restore ran mode=%s status=%s, want replace completed", p.Mode, p.Status)
	}
	want(t, `{`+id(scoped)+`,"mode":"merge"}`, "merge")
	if p := rig.manager.GetProgress(); p.Mode != "merge" {
		t.Errorf("explicit merge ran mode=%s", p.Mode)
	}
	want(t, `{`+id(scoped)+`,"mode":"replace"}`, "replace")
	want(t, `{`+id(unscoped)+`}`, "merge")
	if p := rig.manager.GetProgress(); p.Mode != "merge" {
		t.Errorf("unscoped cluster restore ran mode=%s", p.Mode)
	}
	// Unknown id: admitted with merge echoed, fails asynchronously as before.
	want(t, `{`+id("backup-20260901-000000-00000000")+`}`, "merge")
	if p := rig.manager.GetProgress(); p.Status != "failed" || !strings.Contains(p.Error, "backup not found") {
		t.Errorf("unknown backup id: status=%s error=%q, want the asynchronous not-found failure", p.Status, p.Error)
	}
	// Bad spelling is still a 400 before anything is read.
	if status, payload := restore(t, `{`+id(scoped)+`,"mode":"overwrite"}`); status != fiber.StatusBadRequest || !strings.Contains(payload["error"].(string), "invalid restore mode") {
		t.Errorf("mode=overwrite: status %d payload %v, want 400", status, payload)
	}
}

type fakeRouteTierLookup struct{ rows map[string]bool }

func (f fakeRouteTierLookup) DatabaseHasTierRows(_ context.Context, database string) (bool, error) {
	return f.rows[database], nil
}

// A scoped backup that holds no data file for one of its databases (fully
// cold at backup time, or dropped and recreated with its tier rows left
// behind) cannot be restored in replace mode: on a cluster node that is the
// default, and it would only remove the current files. The handler refuses
// it synchronously, defaulted or explicit, with the manager's text; an
// explicit merge is admitted. A form-typed restore body never reaches the
// manifest: backup_id does not parse and it is a 400 as before.
func TestRestoreBackupRefusesReplaceOfAnEmptyScopedBackup(t *testing.T) {
	rig := newScopeRouteRig(t)
	rig.manager.SetTierLookup(fakeRouteTierLookup{rows: map[string]bool{"cold": true}})
	ctx := context.Background()
	res, err := rig.manager.CreateBackup(ctx, backup.BackupOptions{Databases: []string{"a", "cold"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Manifest.Databases) != 1 || len(res.Manifest.Scope) != 2 {
		t.Fatalf("premise: databases=%+v scope=%v, want one held database of two scoped", res.Manifest.Databases, res.Manifest.Scope)
	}
	id := res.Manifest.BackupID
	rig.manager.SetClusterManifest(fakeRouteClusterManifest{})
	rig.handler.SetCoordinator(&fakeBackupCoordinator{primary: true, role: "writer"})
	body := func(extra string) string { return `{"backup_id":"` + id + `","confirm":true` + extra + `}` }

	for _, tc := range []struct{ name, extra string }{{"mode absent (defaults to replace)", ""}, {"explicit replace", `,"mode":"replace"`}} {
		t.Run(tc.name, func(t *testing.T) {
			status, payload := rig.post(t, "/api/v1/backup/restore", body(tc.extra))
			msg, _ := payload["error"].(string)
			if status != fiber.StatusBadRequest || !strings.HasPrefix(msg, "mode replace is refused: ") || !strings.Contains(msg, `database "cold"`) || !strings.Contains(msg, "use mode merge") {
				t.Errorf("status %d error %q, want 400 with the refusal naming cold", status, msg)
			}
			if rig.handler.activeOperation.Load() != nil {
				t.Error("a refused restore holds the admission slot")
			}
		})
	}
	t.Run("explicit merge", func(t *testing.T) {
		status, payload := rig.post(t, "/api/v1/backup/restore", body(`,"mode":"merge"`))
		if status != fiber.StatusAccepted || payload["mode"] != "merge" {
			t.Fatalf("status %d payload %v, want 202 merge", status, payload)
		}
		waitForOperationRelease(t, rig.handler)
		if p := rig.manager.GetProgress(); p.Status != "completed" || p.Mode != "merge" || p.ReplacedFiles != 0 {
			t.Errorf("merge ran status=%s mode=%s replaced=%d", p.Status, p.Mode, p.ReplacedFiles)
		}
	})
	t.Run("form-typed body", func(t *testing.T) {
		status, payload := rig.postWithType(t, "/api/v1/backup/restore", body(""), "application/x-www-form-urlencoded")
		if status != fiber.StatusBadRequest {
			t.Errorf("status %d payload %v, want 400 (backup_id does not parse from a form body)", status, payload)
		}
	})
}
