package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Embed only Backend so tests can exercise individual deletion separately
// from the optional BatchDeleter fast path.
type databaseTimeoutBackend struct {
	storage.Backend
	failMeasurements bool
	failMarker       bool
	waitForDeadline  bool
	failure          error
}

func (b *databaseTimeoutBackend) fail(ctx context.Context) error {
	if !b.waitForDeadline {
		return b.failure
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("storage context has no deadline")
	}
	<-ctx.Done()
	return ctx.Err()
}

func (b *databaseTimeoutBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	if b.failMeasurements {
		return nil, b.fail(ctx)
	}
	return b.Backend.(storage.DirectoryLister).ListDirectories(ctx, prefix)
}

func (b *databaseTimeoutBackend) Delete(ctx context.Context, key string) error {
	if b.failMarker && key == "testdb/.arc-database" {
		return b.fail(ctx)
	}
	return b.Backend.Delete(ctx, key)
}

type databaseTimeoutBatchBackend struct{ *databaseTimeoutBackend }

func (b *databaseTimeoutBatchBackend) DeleteBatch(ctx context.Context, keys []string) error {
	return b.Backend.(storage.BatchDeleter).DeleteBatch(ctx, keys)
}

func TestDatabasesHandlerGetReportsMeasurementListingFailure(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			ctx := context.Background()
			for _, key := range []string{"testdb/.arc-database", "testdb/cpu/data.parquet"} {
				if err := backend.Write(ctx, key, []byte("data")); err != nil {
					t.Fatal(err)
				}
			}
			failure := errors.New("measurement listing unavailable")
			wrapped := &databaseTimeoutBackend{Backend: backend, failMeasurements: true, waitForDeadline: deadline, failure: failure}
			handler := NewDatabasesHandler(wrapped, nil, nil, zerolog.Nop())
			handler.requestTimeout = 10 * time.Millisecond
			app := fiber.New()
			handler.RegisterRoutes(app)
			resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/databases/testdb", nil), testRequestTimeoutMS)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			var body map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != fiber.StatusInternalServerError {
				t.Fatalf("status = %d, want 500: %v", resp.StatusCode, body)
			}
			wantError := failure.Error()
			if deadline {
				wantError = context.DeadlineExceeded.Error()
			}
			if got, ok := body["error"].(string); !ok || !strings.Contains(got, "Failed to list measurements: "+wantError) {
				t.Fatalf("wrong failure response: %v", body)
			}
			if _, ok := body["measurement_count"]; ok {
				t.Fatalf("failed listing returned a measurement count: %v", body)
			}
		})
	}
}

func TestDatabasesHandlerDeleteReportsMarkerFailure(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		for _, mode := range []string{"empty", "individual", "batch"} {
			t.Run(fmt.Sprintf("%s/deadline=%t", mode, deadline), func(t *testing.T) {
				backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
				if err != nil {
					t.Fatal(err)
				}
				defer backend.Close()
				ctx := context.Background()
				marker := "testdb/.arc-database"
				if err := backend.Write(ctx, marker, []byte("{}")); err != nil {
					t.Fatal(err)
				}
				var files []string
				if mode != "empty" {
					files = []string{"testdb/cpu/a.parquet", "testdb/cpu/b.parquet"}
				}
				for _, key := range files {
					if err := backend.Write(ctx, key, []byte("data")); err != nil {
						t.Fatal(err)
					}
				}
				failure := errors.New("marker deletion unavailable")
				wrapped := &databaseTimeoutBackend{Backend: backend, failMarker: true, waitForDeadline: deadline, failure: failure}
				var apiBackend storage.Backend = wrapped
				if mode == "batch" {
					apiBackend = &databaseTimeoutBatchBackend{wrapped}
				}
				handler := NewDatabasesHandler(apiBackend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
				handler.requestTimeout = 10 * time.Millisecond
				app := fiber.New()
				handler.RegisterRoutes(app)
				resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/testdb?confirm=true", nil), testRequestTimeoutMS)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				var body struct {
					Error        string   `json:"error"`
					DeletedCount int      `json:"deleted_count"`
					Errors       []string `json:"errors"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != fiber.StatusInternalServerError {
					t.Fatalf("status = %d, want 500: %+v", resp.StatusCode, body)
				}
				if body.Error != "Partial deletion - some files could not be deleted" || body.DeletedCount != len(files) {
					t.Fatalf("partial deletion result = %+v, want %d deleted", body, len(files))
				}
				wantError := failure.Error()
				if deadline {
					wantError = context.DeadlineExceeded.Error()
				}
				if len(body.Errors) != 1 || body.Errors[0] != marker+": "+wantError {
					t.Fatalf("errors = %v, want marker failure %q", body.Errors, wantError)
				}
				if exists, err := backend.Exists(ctx, marker); err != nil || !exists {
					t.Fatalf("failed marker delete should leave marker: exists=%t err=%v", exists, err)
				}
				for _, key := range files {
					if exists, err := backend.Exists(ctx, key); err != nil || exists {
						t.Fatalf("data deletion should succeed: key=%s exists=%t err=%v", key, exists, err)
					}
				}
			})
		}
	}
}
