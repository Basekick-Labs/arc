package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type stubFilesCoordinator struct {
	mu      sync.RWMutex
	files   map[string]*clusterraft.FileEntry
	failErr error
}

func newStubFilesCoordinator() *stubFilesCoordinator {
	return &stubFilesCoordinator{
		files: make(map[string]*clusterraft.FileEntry),
	}
}

func (s *stubFilesCoordinator) addFile(path, db, measurement string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[path] = &clusterraft.FileEntry{
		Path:        path,
		Database:    db,
		Measurement: measurement,
		SizeBytes:   1024,
		CreatedAt:   time.Now().UTC(),
	}
}

func (s *stubFilesCoordinator) setFailErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failErr = err
}

func (s *stubFilesCoordinator) GetFileEntry(path string) (*clusterraft.FileEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.files[path]
	return f, ok
}

func (s *stubFilesCoordinator) DeleteFileFromManifest(ctx context.Context, path, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failErr != nil {
		return s.failErr
	}
	delete(s.files, path)
	return nil
}

func setupTestClusterApp(t *testing.T, filesCoord clusterFilesCoordinator, am *auth.AuthManager, auditDetailCapture ...*map[string]string) *fiber.App {
	t.Helper()
	app := fiber.New(fiber.Config{
		ReadBufferSize: 8192,
	})
	if len(auditDetailCapture) > 0 && auditDetailCapture[0] != nil {
		app.Use(func(c *fiber.Ctx) error {
			err := c.Next()
			if d, ok := c.Locals(audit.DetailLocalsKey).(map[string]string); ok {
				*auditDetailCapture[0] = d
			}
			return err
		})
	}
	if am != nil {
		app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
	}
	handler := NewClusterHandler(nil, am, nil, zerolog.Nop())
	handler.filesCoordinator = filesCoord
	handler.RegisterRoutes(app)
	return app
}

func mustCreateTestAuth(t *testing.T) (*auth.AuthManager, string, string) {
	t.Helper()
	tmpDir := t.TempDir()
	authDBPath := filepath.Join(tmpDir, "auth.db")
	am, err := auth.NewAuthManager(authDBPath, time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("auth.NewAuthManager: %v", err)
	}
	t.Cleanup(func() { _ = am.Close() })

	adminTok, err := am.CreateToken(context.Background(), "admin-tok", "admin token", "admin", nil)
	if err != nil {
		t.Fatalf("am.CreateToken(admin): %v", err)
	}
	writerTok, err := am.CreateToken(context.Background(), "writer-tok", "writer token", "writer", nil)
	if err != nil {
		t.Fatalf("am.CreateToken(writer): %v", err)
	}
	return am, adminTok, writerTok
}

func TestClusterHandler_DeleteFile_Disabled(t *testing.T) {
	app := setupTestClusterApp(t, nil, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=true", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusConflict)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["success"] != false || !strings.Contains(fmt.Sprint(body["error"]), "clustering is not enabled") {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestClusterHandler_DeleteFile_AdminAuthRequired(t *testing.T) {
	am, adminToken, writerToken := mustCreateTestAuth(t)
	stub := newStubFilesCoordinator()
	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, am, &capturedDetail)

	// Unauthenticated request -> rejected with 401, audit detail captured
	capturedDetail = nil
	reqNoAuth := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=true", nil)
	respNoAuth, err := app.Test(reqNoAuth, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer respNoAuth.Body.Close()

	if respNoAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d, want %d", respNoAuth.StatusCode, http.StatusUnauthorized)
	}

	// Non-admin token -> rejected with 403 Forbidden, audit detail captured
	capturedDetail = nil
	reqWriter := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=true&reason=test-reason", nil)
	reqWriter.Header.Set("Authorization", "Bearer "+writerToken)
	respWriter, err := app.Test(reqWriter, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer respWriter.Body.Close()

	if respWriter.StatusCode != http.StatusForbidden {
		t.Fatalf("status with non-admin token = %d, want %d", respWriter.StatusCode, http.StatusForbidden)
	}
	if capturedDetail == nil || capturedDetail["path"] != "db/cpu/f1.parquet" || capturedDetail["reason"] != "test-reason" {
		t.Fatalf("expected audit detail on 403, got %+v", capturedDetail)
	}

	// Authenticated request with admin token -> proceeds past auth (file not found -> 404)
	capturedDetail = nil
	reqAuth := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=true", nil)
	reqAuth.Header.Set("Authorization", "Bearer "+adminToken)
	respAuth, err := app.Test(reqAuth, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer respAuth.Body.Close()

	if respAuth.StatusCode != http.StatusNotFound {
		t.Fatalf("status with admin token = %d, want %d", respAuth.StatusCode, http.StatusNotFound)
	}
	if capturedDetail == nil || capturedDetail["path"] != "db/cpu/f1.parquet" {
		t.Fatalf("expected audit detail on 404, got %+v", capturedDetail)
	}
}

func TestClusterHandler_DeleteFile_MissingPath(t *testing.T) {
	stub := newStubFilesCoordinator()
	app := setupTestClusterApp(t, stub, nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?confirm=true", nil)
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

func TestClusterHandler_DeleteFile_UnaddressableKey_Success(t *testing.T) {
	stub := newStubFilesCoordinator()
	invalidPaths := []string{
		"../etc/passwd",
		"/etc/passwd",
		"s3://bucket/db/cpu/file.parquet",
		"db/cpu/file\x00.parquet",
	}

	for _, p := range invalidPaths {
		stub.addFile(p, "db", "cpu")
		var capturedDetail map[string]string
		app := setupTestClusterApp(t, stub, nil, &capturedDetail)

		req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+url.QueryEscape(p)+"&confirm=true&reason=cleanup-corrupted", nil)
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatalf("app.Test(%q): %v", p, err)
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("path %q status = %d, want %d; body = %s", p, resp.StatusCode, http.StatusOK, string(bodyBytes))
		}

		var body map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			t.Fatalf("json decode: %v", err)
		}
		resp.Body.Close()

		if body["success"] != true || body["path"] != p {
			t.Fatalf("unexpected body: %+v", body)
		}

		// Verify removed from stub
		if _, exists := stub.GetFileEntry(p); exists {
			t.Fatalf("path %q still found in stub after deletion", p)
		}

		// Verify flagged as unaddressable in audit detail
		if capturedDetail == nil || capturedDetail["unaddressable_key"] != "true" {
			t.Fatalf("path %q expected unaddressable_key in audit detail, got %+v", p, capturedDetail)
		}
	}
}

func TestClusterHandler_DeleteFile_UnaddressableKey_NotFound(t *testing.T) {
	stub := newStubFilesCoordinator()
	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, nil, &capturedDetail)

	invalidPaths := []string{
		"../etc/passwd",
		"/etc/passwd",
		"s3://bucket/db/cpu/file.parquet",
		"db/cpu/file\x00.parquet",
	}

	for _, p := range invalidPaths {
		capturedDetail = nil
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+url.QueryEscape(p)+"&confirm=true", nil)
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatalf("app.Test(%q): %v", p, err)
		}

		if resp.StatusCode != http.StatusNotFound {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("path %q status = %d, want %d (must be 404, never 400); body = %s", p, resp.StatusCode, http.StatusNotFound, string(bodyBytes))
		}

		var body map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			t.Fatalf("json decode: %v", err)
		}
		resp.Body.Close()

		if body["success"] != false || body["error"] != "file not found in cluster manifest" {
			t.Fatalf("unexpected body: %+v", body)
		}

		if capturedDetail == nil {
			t.Fatalf("path %q expected audit detail to be captured, got nil", p)
		}
		if capturedDetail["path"] != p {
			t.Fatalf("path %q captured audit detail path = %q", p, capturedDetail["path"])
		}
		if capturedDetail["unaddressable_key"] != "true" {
			t.Fatalf("path %q expected unaddressable_key=true in audit detail, got %+v", p, capturedDetail)
		}
	}
}

func TestClusterHandler_DeleteFile_PathLength(t *testing.T) {
	stub := newStubFilesCoordinator()
	app := setupTestClusterApp(t, stub, nil)

	longPath := strings.Repeat("a", 4097)
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+longPath+"&confirm=true", nil)
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
	errMsg, _ := body["error"].(string)
	if !strings.Contains(errMsg, "path exceeds maximum length") {
		t.Fatalf("expected path length error, got %q", errMsg)
	}
}

func TestClusterHandler_DeleteFile_RequiresConfirmation(t *testing.T) {
	stub := newStubFilesCoordinator()
	stub.addFile("db/cpu/f1.parquet", "db", "cpu")
	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, nil, &capturedDetail)

	tests := []struct {
		name string
		url  string
	}{
		{
			name: "missing confirm",
			url:  "/api/v1/cluster/files?path=db/cpu/f1.parquet",
		},
		{
			name: "confirm is false",
			url:  "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=false",
		},
		{
			name: "confirm is other string",
			url:  "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=yes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capturedDetail = nil
			req := httptest.NewRequest(http.MethodDelete, tt.url, nil)
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
			errMsg, _ := body["error"].(string)
			if !strings.Contains(errMsg, "confirmation required") {
				t.Fatalf("expected confirmation required error, got %q", errMsg)
			}

			if capturedDetail == nil || capturedDetail["path"] != "db/cpu/f1.parquet" || capturedDetail["reason"] != "operator" {
				t.Fatalf("expected audit detail captured on 400 unconfirmed, got %+v", capturedDetail)
			}
		})
	}
}

func TestClusterHandler_DeleteFile_ReasonLength(t *testing.T) {
	stub := newStubFilesCoordinator()
	stub.addFile("db/cpu/f1.parquet", "db", "cpu")
	app := setupTestClusterApp(t, stub, nil)

	// Reason too long (> 256 chars) -> 400
	longReason := strings.Repeat("r", 257)
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/f1.parquet&confirm=true&reason="+longReason, nil)
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
	errMsg, _ := body["error"].(string)
	if !strings.Contains(errMsg, "reason exceeds maximum length") {
		t.Fatalf("expected reason length error, got %q", errMsg)
	}
}

func TestClusterHandler_DeleteFile_NotFound(t *testing.T) {
	stub := newStubFilesCoordinator()
	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, nil, &capturedDetail)

	filePath := "db/cpu/ghost.parquet"
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+filePath+"&confirm=true", nil)
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

	if capturedDetail == nil {
		t.Fatal("expected audit detail to be captured for 404, got nil")
	}
	if capturedDetail["path"] != filePath {
		t.Fatalf("captured audit detail path = %q, want %q", capturedDetail["path"], filePath)
	}
	if capturedDetail["reason"] != "operator" {
		t.Fatalf("captured audit detail reason = %q, want %q", capturedDetail["reason"], "operator")
	}
}

func TestClusterHandler_DeleteFile_RaftApplyError(t *testing.T) {
	stub := newStubFilesCoordinator()
	filePath := "db/cpu/2026/04/11/14/file.parquet"
	stub.addFile(filePath, "db", "cpu")
	stub.setFailErr(errors.Join(clusterraft.ErrManifestApply, errors.New("timeout waiting for leader reply")))

	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, nil, &capturedDetail)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+filePath+"&confirm=true", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("Retry-After header = %q, want %q", retryAfter, "1")
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["success"] != false || body["error"] != "cluster manifest update unavailable; retry later" {
		t.Fatalf("unexpected body: %v", body)
	}

	if capturedDetail == nil {
		t.Fatal("expected audit detail to be captured for 503, got nil")
	}
	if capturedDetail["path"] != filePath {
		t.Fatalf("captured audit detail path = %q, want %q", capturedDetail["path"], filePath)
	}
	if capturedDetail["reason"] != "operator" {
		t.Fatalf("captured audit detail reason = %q, want %q", capturedDetail["reason"], "operator")
	}
}

func TestClusterHandler_DeleteFile_GenericError(t *testing.T) {
	stub := newStubFilesCoordinator()
	filePath := "db/cpu/2026/04/11/14/file.parquet"
	stub.addFile(filePath, "db", "cpu")
	stub.setFailErr(errors.New("unexpected internal storage failure"))

	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, nil, &capturedDetail)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+filePath+"&confirm=true", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if body["success"] != false || body["error"] != "unexpected internal storage failure" {
		t.Fatalf("unexpected body: %v", body)
	}

	if capturedDetail == nil {
		t.Fatal("expected audit detail to be captured for 500, got nil")
	}
	if capturedDetail["path"] != filePath {
		t.Fatalf("captured audit detail path = %q, want %q", capturedDetail["path"], filePath)
	}
	if capturedDetail["reason"] != "operator" {
		t.Fatalf("captured audit detail reason = %q, want %q", capturedDetail["reason"], "operator")
	}
}

func TestClusterHandler_DeleteFile_Success(t *testing.T) {
	stub := newStubFilesCoordinator()
	filePath := "db/cpu/2026/04/11/14/file.parquet"
	stub.addFile(filePath, "db", "cpu")

	var capturedDetail map[string]string
	app := setupTestClusterApp(t, stub, nil, &capturedDetail)

	// Verify file is in stub before delete
	if _, exists := stub.GetFileEntry(filePath); !exists {
		t.Fatalf("file %q not found in stub before delete", filePath)
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path="+filePath+"&confirm=true&reason=test-delete", nil)
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
	if body["message"] != "file removed from cluster manifest and deleted cluster-wide" {
		t.Fatalf("unexpected message: %v", body["message"])
	}

	// Verify file is removed from stub after delete
	if _, exists := stub.GetFileEntry(filePath); exists {
		t.Fatalf("file %q still found in stub after delete", filePath)
	}

	// Verify audit detail is set in locals
	if capturedDetail == nil {
		t.Fatal("expected audit detail to be captured from locals, got nil")
	}
	if capturedDetail["path"] != filePath || capturedDetail["reason"] != "test-delete" {
		t.Fatalf("unexpected audit detail: %+v", capturedDetail)
	}
	if _, hasUnaddressable := capturedDetail["unaddressable_key"]; hasUnaddressable {
		t.Fatalf("valid path should not be marked unaddressable_key, got %+v", capturedDetail)
	}
}

func TestClusterHandler_DeleteFile_PersistedAudit_400And403(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	auditLogger, err := audit.NewLogger(&audit.LoggerConfig{
		DB:     db,
		Config: &config.AuditLogConfig{Enabled: true, RetentionDays: 90},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	auditLogger.Start()
	t.Cleanup(func() { auditLogger.Stop() })

	am, adminToken, writerToken := mustCreateTestAuth(t)
	stub := newStubFilesCoordinator()
	stub.addFile("db/cpu/f1.parquet", "db", "cpu")

	app := fiber.New(fiber.Config{ReadBufferSize: 8192})
	app.Use(audit.Middleware(auditLogger, false))
	app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))

	handler := NewClusterHandler(nil, am, nil, zerolog.Nop())
	handler.filesCoordinator = stub
	handler.RegisterRoutes(app)

	// 1. Send 403 request (non-admin token)
	req403 := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/secret.parquet&confirm=true", nil)
	req403.Header.Set("Authorization", "Bearer "+writerToken)
	resp403, err := app.Test(req403, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test 403: %v", err)
	}
	resp403.Body.Close()
	if resp403.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp403.StatusCode)
	}

	// 2. Send 400 request (missing confirmation with admin token)
	req400 := httptest.NewRequest(http.MethodDelete, "/api/v1/cluster/files?path=db/cpu/unconfirmed.parquet", nil)
	req400.Header.Set("Authorization", "Bearer "+adminToken)
	resp400, err := app.Test(req400, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test 400: %v", err)
	}
	resp400.Body.Close()
	if resp400.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp400.StatusCode)
	}

	// Wait for audit background logger to flush to sqlite
	time.Sleep(1500 * time.Millisecond)

	entries, err := auditLogger.Query(context.Background(), &audit.QueryFilter{Limit: 10})
	if err != nil {
		t.Fatalf("auditLogger.Query: %v", err)
	}

	var found403, found400 bool
	for _, entry := range entries {
		if entry.StatusCode == http.StatusForbidden {
			found403 = true
			if entry.EventType != "auth.failed" {
				t.Fatalf("403 event type = %q, want auth.failed", entry.EventType)
			}
			if !strings.Contains(entry.Detail, "db/cpu/secret.parquet") {
				t.Fatalf("expected 403 audit entry detail to contain target path, got %q", entry.Detail)
			}
			if !strings.Contains(entry.Detail, "operator") {
				t.Fatalf("expected 403 audit entry detail to contain reason operator, got %q", entry.Detail)
			}
		}
		if entry.StatusCode == http.StatusBadRequest {
			found400 = true
			if entry.EventType != "api.delete" {
				t.Fatalf("400 event type = %q, want api.delete", entry.EventType)
			}
			if !strings.Contains(entry.Detail, "db/cpu/unconfirmed.parquet") {
				t.Fatalf("expected 400 audit entry detail to contain target path, got %q", entry.Detail)
			}
			if !strings.Contains(entry.Detail, "operator") {
				t.Fatalf("expected 400 audit entry detail to contain reason operator, got %q", entry.Detail)
			}
		}
	}

	if !found403 {
		t.Fatal("did not find persisted audit log entry with status 403")
	}
	if !found400 {
		t.Fatal("did not find persisted audit log entry with status 400")
	}
}
