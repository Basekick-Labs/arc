package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/gofiber/fiber/v2"
	hashicorpraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func setupTestClusterApp(t *testing.T, coord *cluster.Coordinator, am *auth.AuthManager) *fiber.App {
	t.Helper()
	app := fiber.New()
	if am != nil {
		app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
	}
	handler := NewClusterHandler(coord, am, nil, zerolog.Nop())
	handler.RegisterRoutes(app)
	return app
}

func mustCreateAdminAuth(t *testing.T) (*auth.AuthManager, string) {
	t.Helper()
	tmpDir := t.TempDir()
	authDBPath := filepath.Join(tmpDir, "auth.db")
	am, err := auth.NewAuthManager(authDBPath, time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("auth.NewAuthManager: %v", err)
	}
	t.Cleanup(func() { _ = am.Close() })

	tok, err := am.CreateToken(context.Background(), "admin-tok", "admin token", "admin", nil)
	if err != nil {
		t.Fatalf("am.CreateToken: %v", err)
	}
	return am, tok
}

func registerFileInFSM(t *testing.T, fsm *clusterraft.ClusterFSM, path, db, measurement string) {
	t.Helper()
	file := clusterraft.FileEntry{
		Path:        path,
		Database:    db,
		Measurement: measurement,
		SizeBytes:   1024,
		CreatedAt:   time.Now().UTC(),
	}
	payload, err := json.Marshal(clusterraft.RegisterFilePayload{File: file})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	cmd, err := json.Marshal(clusterraft.Command{
		Type:    clusterraft.CommandRegisterFile,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	log := &hashicorpraft.Log{Index: 1, Data: cmd}
	if res := fsm.Apply(log); res != nil {
		t.Fatalf("fsm.Apply: %v", res)
	}
}

func TestClusterHandler_DeleteFile_Disabled(t *testing.T) {
	app := setupTestClusterApp(t, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["enabled"] != false || body["mode"] != "standalone" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestClusterHandler_DeleteFile_AdminAuthRequired(t *testing.T) {
	am, adminToken := mustCreateAdminAuth(t)
	fsm := clusterraft.NewClusterFSM(zerolog.Nop())
	coord := cluster.NewTestCoordinator(fsm)
	app := setupTestClusterApp(t, coord, am)

	// Unauthenticated request -> rejected with 401
	reqNoAuth := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet", nil)
	respNoAuth, err := app.Test(reqNoAuth, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer respNoAuth.Body.Close()

	if respNoAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d, want %d", respNoAuth.StatusCode, http.StatusUnauthorized)
	}

	// Authenticated request with admin token -> proceeds past auth
	reqAuth := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet", nil)
	reqAuth.Header.Set("Authorization", "Bearer "+adminToken)
	respAuth, err := app.Test(reqAuth, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer respAuth.Body.Close()

	// Since file is not in manifest, it should pass auth and return 404
	if respAuth.StatusCode != http.StatusNotFound {
		t.Fatalf("status with admin token = %d, want %d", respAuth.StatusCode, http.StatusNotFound)
	}
}

func TestClusterHandler_DeleteFile_MissingPath(t *testing.T) {
	fsm := clusterraft.NewClusterFSM(zerolog.Nop())
	coord := cluster.NewTestCoordinator(fsm)
	app := setupTestClusterApp(t, coord, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["success"] != false || body["error"] != "path query parameter is required" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestClusterHandler_DeleteFile_NotFound(t *testing.T) {
	fsm := clusterraft.NewClusterFSM(zerolog.Nop())
	coord := cluster.NewTestCoordinator(fsm)
	app := setupTestClusterApp(t, coord, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/ghost.parquet", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["success"] != false || body["error"] != "file not found in cluster manifest" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestClusterHandler_DeleteFile_Success(t *testing.T) {
	fsm := clusterraft.NewClusterFSM(zerolog.Nop())
	filePath := "db/cpu/2026/04/11/14/file.parquet"
	registerFileInFSM(t, fsm, filePath, "db", "cpu")

	coord := cluster.NewTestCoordinator(fsm)
	app := setupTestClusterApp(t, coord, nil)

	// Verify file is in manifest before delete
	if _, exists := coord.GetFileEntry(filePath); !exists {
		t.Fatalf("file %q not found in FSM before delete", filePath)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+filePath+"&reason=test-delete", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d; body = %s", resp.StatusCode, http.StatusOK, string(bodyBytes))
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["success"] != true || body["path"] != filePath {
		t.Fatalf("unexpected response body: %v", body)
	}

	// Verify file is removed from manifest after delete
	if _, exists := coord.GetFileEntry(filePath); exists {
		t.Fatalf("file %q still found in FSM after delete", filePath)
	}
}
