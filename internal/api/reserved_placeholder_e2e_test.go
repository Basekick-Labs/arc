package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A token granted only db1 is refused, with a 400, a statement that spells
// a function-body FROM placeholder; before the fix the transform restored
// the caller's copy after the permission check. The direct db2 query is the
// control: still a 403.
func TestReservedPlaceholderRefusedEndToEnd(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()
	metrics.Init(logger)
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	backend, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	defer buffer.Close()
	for db, sentinel := range map[string]string{
		"db1": "OWN_ROW",
		"db2": "OTHER_ROW",
	} {
		err := buffer.WriteTypedColumnarDirect(ctx, db, "cpu", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{
				"time":         []int64{time.Now().UTC().UnixMicro()},
				"secret_value": []string{sentinel},
			},
		}, 1)
		if err != nil {
			t.Fatalf("write %s fixture: %v", db, err)
		}
	}
	if err := buffer.FlushAllAndWait(ctx); err != nil {
		t.Fatal(err)
	}

	duck, err := database.New(&database.Config{
		MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()

	authManager, err := auth.NewAuthManager(filepath.Join(root, "auth.db"), 5*time.Minute, 100, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer authManager.Close()
	rbac := auth.NewRBACManager(&auth.RBACManagerConfig{DB: authManager.GetDB(), Logger: logger})
	org, err := rbac.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "reserved-ph-org"})
	if err != nil {
		t.Fatal(err)
	}
	team, err := rbac.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "reserved-ph-team"})
	if err != nil {
		t.Fatal(err)
	}
	role, err := rbac.CreateRole(ctx, team.ID, &auth.CreateRoleRequest{
		DatabasePattern: "db1", Permissions: []string{"read"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rbac.CreateMeasurementPermission(ctx, role.ID, &auth.CreateMeasurementPermissionRequest{
		MeasurementPattern: "cpu", Permissions: []string{"read"},
	}); err != nil {
		t.Fatal(err)
	}
	token, err := authManager.CreateToken(ctx, "reserved-ph-reader", "read only on db1.cpu", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	tokenInfo := authManager.VerifyToken(token)
	if tokenInfo == nil {
		t.Fatal("verify token")
	}
	if _, err := rbac.AddTokenToTeam(ctx, tokenInfo.ID, team.ID); err != nil {
		t.Fatal(err)
	}
	rbac.InvalidateAllCache()

	handler := NewQueryHandler(duck, backend, logger, 0, 0)
	handler.SetAuthAndRBAC(authManager, rbac)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	middleware := auth.DefaultMiddlewareConfig()
	middleware.AuthManager = authManager
	app.Use(auth.NewMiddleware(middleware))
	handler.RegisterRoutes(app)

	query := func(sql string) (int, []byte) {
		t.Helper()
		body, err := json.Marshal(QueryRequest{SQL: sql})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, responseBody
	}

	ownStatus, ownBody := query("SELECT secret_value FROM db1.cpu")
	if ownStatus != fiber.StatusOK || !bytes.Contains(ownBody, []byte("OWN_ROW")) {
		t.Fatalf("db1.cpu baseline failed: HTTP %d %s", ownStatus, ownBody)
	}

	siblingGlob := filepath.Join(dataDir, "db2", "cpu", "**", "*.parquet")
	sql2 := fmt.Sprintf("SELECT secret_value, EXTRACT(YEAR FROM DATE '2026-01-01') AS yr __FROM_MASK_0__ '%s'", siblingGlob)
	controlSQL := "SELECT secret_value FROM db2.cpu"
	refusedStatus, refusedBody := query(sql2)
	controlStatus, controlBody := query(controlSQL)

	if refusedStatus != fiber.StatusBadRequest || bytes.Contains(refusedBody, []byte("OTHER_ROW")) || !bytes.Contains(refusedBody, []byte("reserved sequence")) {
		t.Fatalf("placeholder text in caller SQL was not refused: HTTP %d %s", refusedStatus, refusedBody)
	}
	if controlStatus != fiber.StatusForbidden || !strings.Contains(string(controlBody), "access denied") {
		t.Fatalf("direct db2 query was not denied: HTTP %d %s", controlStatus, controlBody)
	}
}
