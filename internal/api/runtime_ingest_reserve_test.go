package api

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type runtimeReserveBufferStub struct {
	cfg  ingest.RuntimeElasticReserveConfig
	used int64
}

func (b *runtimeReserveBufferStub) ElasticReserveConfig() ingest.RuntimeElasticReserveConfig {
	return b.cfg
}
func (b *runtimeReserveBufferStub) ElasticReserveUsedRecords() int64 { return b.used }
func (b *runtimeReserveBufferStub) ConfigureElasticReserve(cfg ingest.RuntimeElasticReserveConfig) error {
	if cfg.CapacityRecords < b.used {
		return fiber.NewError(fiber.StatusBadRequest, "capacity is below usage")
	}
	if !cfg.Enabled && b.used > 0 {
		return fiber.NewError(fiber.StatusBadRequest, "reserve is not empty")
	}
	b.cfg = cfg
	return nil
}
func (b *runtimeReserveBufferStub) ConfigureElasticReserveWithPersistence(cfg ingest.RuntimeElasticReserveConfig, persist func() error) error {
	if cfg.CapacityRecords < b.used {
		return fiber.NewError(fiber.StatusBadRequest, "capacity is below usage")
	}
	if !cfg.Enabled && b.used > 0 {
		return fiber.NewError(fiber.StatusBadRequest, "reserve is not empty")
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	b.cfg = cfg
	return nil
}

func TestRuntimeIngestReservePersistsSeparatelyFromBufferReset(t *testing.T) {
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
	wantBuffers := ingest.RuntimeIngestConfig{MaxBufferSize: 1234, MaxBufferAgeMS: 5678}
	if err := store.Save(wantBuffers); err != nil {
		t.Fatal(err)
	}
	buffer := &runtimeReserveBufferStub{}
	app := fiber.New()
	NewRuntimeIngestReserveHandler(buffer, store, nil, zerolog.Nop()).RegisterRoutes(app)

	request := httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest/elastic-reserve", strings.NewReader(`{"enabled":true,"capacity_records":5000,"persistent":true}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("PATCH status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}
	var patched runtimeIngestReserveResponse
	if err := json.NewDecoder(response.Body).Decode(&patched); err != nil {
		t.Fatal(err)
	}
	if !patched.Enabled || patched.CapacityRecords != 5000 || !patched.Persistent || patched.Source != "persistent_override" {
		t.Fatalf("PATCH response = %+v", patched)
	}
	if got, found, err := store.LoadElasticReserve(); err != nil || !found || got != (ingest.RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 5000}) {
		t.Fatalf("reserve override = (%+v, %v, %v)", got, found, err)
	}
	request = httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest/elastic-reserve", strings.NewReader(`{"capacity_records":6000}`))
	request.Header.Set("Content-Type", "application/json")
	response, err = app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("PATCH without persistent status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}
	if got, found, err := store.LoadElasticReserve(); err != nil || !found || got != (ingest.RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 6000}) {
		t.Fatalf("partial PATCH cleared persistence = (%+v, %v, %v)", got, found, err)
	}

	if err := store.Delete(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.LoadElasticReserve(); err != nil || !found {
		t.Fatalf("buffer reset affected reserve override: found=%v err=%v", found, err)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("buffer reset left threshold override: found=%v err=%v", found, err)
	}
	reset, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/config/runtime/ingest/elastic-reserve", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer reset.Body.Close()
	if reset.StatusCode != fiber.StatusOK {
		t.Fatalf("reserve DELETE status = %d, want %d", reset.StatusCode, fiber.StatusOK)
	}
	if _, found, err := store.LoadElasticReserve(); err != nil || found {
		t.Fatalf("reserve DELETE left override: found=%v err=%v", found, err)
	}
}

func TestRuntimeIngestReserveIsNotPersistentByDefault(t *testing.T) {
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
	buffer := &runtimeReserveBufferStub{}
	app := fiber.New()
	NewRuntimeIngestReserveHandler(buffer, store, nil, zerolog.Nop()).RegisterRoutes(app)

	request := httptest.NewRequest("PATCH", "/api/v1/config/runtime/ingest/elastic-reserve", strings.NewReader(`{"enabled":true,"capacity_records":250}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != fiber.StatusOK {
		t.Fatalf("PATCH status = %d, want %d", response.StatusCode, fiber.StatusOK)
	}
	if _, found, err := store.LoadElasticReserve(); err != nil || found {
		t.Fatalf("runtime-only reserve unexpectedly persisted: found=%v err=%v", found, err)
	}
}
