package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type tieringPolicyTestManager struct {
	policies  *tiering.PolicyStore
	coldCount int64
	coldErr   error
	coldCalls int
	coldDB    string
}

func (m *tieringPolicyTestManager) GetPolicies() *tiering.PolicyStore {
	return m.policies
}

func (m *tieringPolicyTestManager) CountColdFilesForDatabase(_ context.Context, database string) (int64, error) {
	m.coldCalls++
	m.coldDB = database
	return m.coldCount, m.coldErr
}

func (m *tieringPolicyTestManager) GetEffectivePolicy(ctx context.Context, database string) *tiering.EffectivePolicy {
	return m.policies.GetEffective(ctx, database)
}

func newTieringPoliciesTestApp(t *testing.T, coldCount int64, coldErr error) (*fiber.App, *tieringPolicyTestManager) {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "tiering.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	policies, err := tiering.NewPolicyStore(db, &config.TieredStorageConfig{DefaultHotMaxAgeDays: 7}, zerolog.Nop())
	if err != nil {
		t.Fatalf("create policy store: %v", err)
	}
	manager := &tieringPolicyTestManager{policies: policies, coldCount: coldCount, coldErr: coldErr}
	handler := &TieringPoliciesHandler{manager: manager, logger: zerolog.Nop()}
	app := fiber.New()
	app.Put("/api/v1/tiering/policies/:database", handler.SetPolicy)
	return app, manager
}

func TestSetHotOnlyPolicyReportsExistingColdFiles(t *testing.T) {
	app, manager := newTieringPoliciesTestApp(t, 3, nil)
	request := httptest.NewRequest(http.MethodPut, "/api/v1/tiering/policies/db1", strings.NewReader(`{"hot_only":true}`))
	request.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	response, err := app.Test(request, 1000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}

	var body struct {
		ColdFiles *struct {
			RecordedBeforeUpdate int64  `json:"recorded_before_update"`
			Scope                string `json:"scope"`
			Note                 string `json:"note"`
		} `json:"cold_files"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ColdFiles == nil {
		t.Fatal("response is missing the cold_files observation")
	}
	if body.ColdFiles.RecordedBeforeUpdate != 3 || body.ColdFiles.Scope != "node_local_metadata" {
		t.Fatalf("cold_files = %+v, want 3 recorded rows scoped to this node", *body.ColdFiles)
	}
	if !strings.Contains(body.ColdFiles.Note, "does not restore existing cold files") {
		t.Fatalf("note = %q, want to explain existing files are not restored", body.ColdFiles.Note)
	}
	if manager.coldCalls != 1 {
		t.Fatalf("cold file count called %d times, want 1", manager.coldCalls)
	}
	if manager.coldDB != "db1" {
		t.Fatalf("cold file count requested for %q, want db1", manager.coldDB)
	}
	policy, err := manager.policies.Get(context.Background(), "db1")
	if err != nil || policy == nil || !policy.HotOnly {
		t.Fatalf("saved policy = %v, err = %v; want hot_only", policy, err)
	}
}

func TestSetHotOnlyPolicyReportsZeroColdFiles(t *testing.T) {
	app, manager := newTieringPoliciesTestApp(t, 0, nil)
	request := httptest.NewRequest(http.MethodPut, "/api/v1/tiering/policies/db1", strings.NewReader(`{"hot_only":true}`))
	request.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	response, err := app.Test(request, 1000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}
	var body struct {
		ColdFiles *struct {
			RecordedBeforeUpdate int64  `json:"recorded_before_update"`
			Scope                string `json:"scope"`
			Note                 string `json:"note"`
		} `json:"cold_files"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ColdFiles == nil || body.ColdFiles.RecordedBeforeUpdate != 0 || body.ColdFiles.Scope != "node_local_metadata" {
		t.Fatalf("cold_files = %+v, want zero recorded rows scoped to this node", body.ColdFiles)
	}
	if !strings.Contains(body.ColdFiles.Note, "does not restore existing cold files") {
		t.Fatalf("note = %q, want to explain existing files are not restored", body.ColdFiles.Note)
	}
	if manager.coldCalls != 1 {
		t.Fatalf("cold file count called %d times, want 1", manager.coldCalls)
	}
}

func TestSetNonHotOnlyPolicyDoesNotCountColdFiles(t *testing.T) {
	app, manager := newTieringPoliciesTestApp(t, 3, nil)
	request := httptest.NewRequest(http.MethodPut, "/api/v1/tiering/policies/db1", strings.NewReader(`{"hot_only":false}`))
	request.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	response, err := app.Test(request, 1000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}
	var body map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := body["cold_files"]; ok {
		t.Fatal("non-hot-only policy response unexpectedly includes a cold file count")
	}
	if manager.coldCalls != 0 {
		t.Fatalf("cold file count called %d times, want 0", manager.coldCalls)
	}
}

func TestSetHotOnlyPolicyDoesNotUpdateWhenColdFileCountFails(t *testing.T) {
	app, manager := newTieringPoliciesTestApp(t, 0, errors.New("metadata unavailable"))
	request := httptest.NewRequest(http.MethodPut, "/api/v1/tiering/policies/db1", strings.NewReader(`{"hot_only":true}`))
	request.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	response, err := app.Test(request, 1000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.StatusCode, fiber.StatusInternalServerError)
	}
	policy, err := manager.policies.Get(context.Background(), "db1")
	if err != nil {
		t.Fatalf("get policy after failed count: %v", err)
	}
	if policy != nil {
		t.Fatalf("policy = %+v after failed count, want no update", policy)
	}
	if manager.coldCalls != 1 {
		t.Fatalf("cold file count called %d times, want 1", manager.coldCalls)
	}
}

// collectingSource is a cluster that is collecting the nodes' policies.
type collectingSource struct{}

func (collectingSource) Seeded() bool                                       { return false }
func (collectingSource) Collecting() bool                                   { return true }
func (collectingSource) Get(string) (*tiering.DatabasePolicy, bool)         { return nil, false }
func (collectingSource) List() []tiering.DatabasePolicy                     { return nil }
func (collectingSource) Set(context.Context, *tiering.DatabasePolicy) error { return nil }
func (collectingSource) Delete(context.Context, string) error               { return nil }

// #1324: while the cluster collects the nodes' policies a write answers 503
// (retry), and a delete of a missing policy is still 404.
func TestTieringPolicyWritesDuringACollect(t *testing.T) {
	app, manager := newTieringPoliciesTestApp(t, 0, nil)
	app.Delete("/api/v1/tiering/policies/:database", (&TieringPoliciesHandler{manager: manager, logger: zerolog.Nop()}).DeletePolicy)

	del := func() int {
		resp, err := app.Test(httptest.NewRequest(http.MethodDelete, "/api/v1/tiering/policies/nope", nil), 1000)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := del(); code != fiber.StatusNotFound {
		t.Fatalf("delete of a missing policy = %d, want 404", code)
	}

	manager.policies.SetClusterSource(collectingSource{})
	req := httptest.NewRequest(http.MethodPut, "/api/v1/tiering/policies/db1", strings.NewReader(`{"hot_max_age_days":3}`))
	req.Header.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	resp, err := app.Test(req, 1000)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Fatalf("set during a collect = %d, want 503", resp.StatusCode)
	}
	if code := del(); code != fiber.StatusServiceUnavailable {
		t.Fatalf("delete during a collect = %d, want 503", code)
	}
}
