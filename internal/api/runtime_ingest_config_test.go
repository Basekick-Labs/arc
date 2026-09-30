package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type runtimeConfigBufferStub struct {
	size int
	age  int
}

func (b *runtimeConfigBufferStub) RuntimeConfig() (int, int) { return b.size, b.age }

func (b *runtimeConfigBufferStub) PatchRuntimeConfig(size, age *int) error {
	if size != nil {
		if *size < ingest.MinRuntimeIngestBufferSize {
			return fiber.NewError(fiber.StatusBadRequest, "max_buffer_size is below the supported minimum")
		}
		b.size = *size
	}
	if age != nil {
		if *age <= 0 {
			return fiber.NewError(fiber.StatusBadRequest, "max_buffer_age_ms must be greater than zero")
		}
		b.age = *age
	}
	return nil
}

func TestRuntimeIngestConfigPatchPersistsAndDeleteRestoresStartupValues(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := ingest.NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	buffer := &runtimeConfigBufferStub{size: 50000, age: 5000}
	startup := ingest.RuntimeIngestConfig{MaxBufferSize: 50000, MaxBufferAgeMS: 5000}
	app := fiber.New()
	NewRuntimeIngestConfigHandler(buffer, store, startup, nil, zerolog.Nop()).RegisterRoutes(app)

	patch := httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest", strings.NewReader(`{"max_buffer_age_ms":9000,"persistent":true}`))
	patch.Header.Set("Content-Type", "application/json")
	response, err := app.Test(patch)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("PATCH status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}
	var patched runtimeIngestConfigResponse
	if err := json.NewDecoder(response.Body).Decode(&patched); err != nil {
		t.Fatal(err)
	}
	if patched.MaxBufferSize != 50000 || patched.MaxBufferAgeMS != 9000 || !patched.Persistent || patched.Source != "persistent_override" {
		t.Fatalf("PATCH response = %+v, want merged persistent settings", patched)
	}
	if got, found, err := store.Load(); err != nil || !found || got != (ingest.RuntimeIngestConfig{MaxBufferSize: 50000, MaxBufferAgeMS: 9000}) {
		t.Fatalf("persisted settings = (%+v, %v, %v)", got, found, err)
	}

	reset, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/config/runtime/ingest", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer reset.Body.Close()
	if reset.StatusCode != fiber.StatusOK {
		t.Fatalf("DELETE status = %d, want %d", reset.StatusCode, fiber.StatusOK)
	}
	var restored runtimeIngestConfigResponse
	if err := json.NewDecoder(reset.Body).Decode(&restored); err != nil {
		t.Fatal(err)
	}
	if restored.MaxBufferSize != startup.MaxBufferSize || restored.MaxBufferAgeMS != startup.MaxBufferAgeMS || restored.Persistent || restored.Source != "startup_config" {
		t.Fatalf("DELETE response = %+v, want startup settings", restored)
	}
	if gotSize, gotAge := buffer.RuntimeConfig(); gotSize != startup.MaxBufferSize || gotAge != startup.MaxBufferAgeMS {
		t.Fatalf("live settings after DELETE = (%d, %d), want (%d, %d)", gotSize, gotAge, startup.MaxBufferSize, startup.MaxBufferAgeMS)
	}
}

func TestRuntimeOnlyPatchKeepsExistingPersistentOverride(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := ingest.NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	saved := ingest.RuntimeIngestConfig{MaxBufferSize: 2000, MaxBufferAgeMS: 5000}
	if err := store.Save(saved); err != nil {
		t.Fatal(err)
	}
	buffer := &runtimeConfigBufferStub{size: saved.MaxBufferSize, age: saved.MaxBufferAgeMS}
	app := fiber.New()
	NewRuntimeIngestConfigHandler(buffer, store, saved, nil, zerolog.Nop()).RegisterRoutes(app)

	request := httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest", strings.NewReader(`{"max_buffer_size":3000}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("runtime-only PATCH status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}

	get, err := app.Test(httptest.NewRequest("GET", "/api/v1/config/runtime/ingest", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer get.Body.Close()
	var got runtimeIngestConfigResponse
	if err := json.NewDecoder(get.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.MaxBufferSize != 3000 || got.Persistent || got.Source != "runtime_override" {
		t.Fatalf("GET after runtime-only PATCH = %+v", got)
	}
	if gotSaved, found, err := store.Load(); err != nil || !found || gotSaved != saved {
		t.Fatalf("saved override after runtime-only PATCH = (%+v, %v, %v), want unchanged %+v", gotSaved, found, err, saved)
	}
}

func TestRuntimeIngestConfigPatchRejectsInvalidValueWithoutSaving(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := ingest.NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	buffer := &runtimeConfigBufferStub{size: 1000, age: 200}
	app := fiber.New()
	NewRuntimeIngestConfigHandler(buffer, store, ingest.RuntimeIngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 200}, nil, zerolog.Nop()).RegisterRoutes(app)

	request := httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest", strings.NewReader(`{"max_buffer_size":999}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("PATCH status = %d, want %d", response.StatusCode, fiber.StatusBadRequest)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("invalid PATCH persisted an override: found=%v err=%v", found, err)
	}
	if size, age := buffer.RuntimeConfig(); size != 1000 || age != 200 {
		t.Fatalf("invalid PATCH changed live settings to (%d, %d)", size, age)
	}
}

func TestRuntimeIngestConfigPatchRollsBackLiveValueWhenPersistenceFails(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := ingest.NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TRIGGER reject_runtime_ingest_config BEFORE INSERT ON arc_runtime_ingest_config BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END`); err != nil {
		t.Fatal(err)
	}
	buffer := &runtimeConfigBufferStub{size: 1000, age: 200}
	app := fiber.New()
	NewRuntimeIngestConfigHandler(buffer, store, ingest.RuntimeIngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 200}, nil, zerolog.Nop()).RegisterRoutes(app)

	request := httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest", strings.NewReader(`{"max_buffer_size":1300,"persistent":true}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("PATCH status = %d, want %d", response.StatusCode, fiber.StatusInternalServerError)
	}
	if size, age := buffer.RuntimeConfig(); size != 1000 || age != 200 {
		t.Fatalf("persistence failure left live settings at (%d, %d)", size, age)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("persistence failure left override found=%v err=%v", found, err)
	}
}

func TestRuntimeIngestConfigRoutesRequireAdmin(t *testing.T) {
	am, adminToken, writerToken := mustCreateTestAuth(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	store, err := ingest.NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	buffer := &runtimeConfigBufferStub{size: 1000, age: 200}
	app := fiber.New()
	app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
	NewRuntimeIngestConfigHandler(buffer, store, ingest.RuntimeIngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 200}, am, zerolog.Nop()).RegisterRoutes(app)

	requests := []struct {
		method string
		body   string
	}{
		{method: http.MethodGet},
		{method: http.MethodPatch, body: `{"max_buffer_size":1300}`},
		{method: http.MethodDelete},
	}
	for _, tc := range requests {
		for _, authCase := range []struct {
			name, token string
			want        int
		}{
			{name: "missing", want: fiber.StatusUnauthorized},
			{name: "non-admin", token: writerToken, want: fiber.StatusForbidden},
			{name: "admin", token: adminToken, want: fiber.StatusOK},
		} {
			req := httptest.NewRequest(tc.method, "/api/v1/config/runtime/ingest", strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			if authCase.token != "" {
				req.Header.Set("Authorization", "Bearer "+authCase.token)
			}
			resp, err := app.Test(req, -1)
			if err != nil {
				t.Fatalf("%s %s: %v", tc.method, authCase.name, err)
			}
			resp.Body.Close()
			if resp.StatusCode != authCase.want {
				t.Errorf("%s with %s token: status = %d, want %d", tc.method, authCase.name, resp.StatusCode, authCase.want)
			}
		}
	}
}
