package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/tiering"
)

func TestTriggerMigrationRejectsInvalidTiersAtHTTPBoundary(t *testing.T) {
	h := NewTieringHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/api/v1/tiering/migrate", h.TriggerMigration)

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "invalid from tier",
			body: `{"from_tier":"bogus","to_tier":"cold"}`,
			want: "invalid from_tier",
		},
		{
			name: "invalid to tier",
			body: `{"from_tier":"hot","to_tier":"bogus"}`,
			want: "invalid to_tier",
		},
		{
			name: "unsupported direction",
			body: `{"from_tier":"cold","to_tier":"hot"}`,
			want: "unsupported migration: only hot to cold is supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/tiering/migrate", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("POST /api/v1/tiering/migrate: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != fiber.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusBadRequest)
			}

			var response map[string]string
			if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response["error"] != tc.want {
				t.Fatalf("error = %q, want %q", response["error"], tc.want)
			}
		})
	}
}

func TestTriggerMigrationDryRunPropagatesFiltersAndReportsPreview(t *testing.T) {
	var gotFrom, gotTo tiering.Tier
	var gotDatabase, gotMeasurement string
	calls := 0

	h := &TieringHandler{
		logger: zerolog.Nop(),
		previewMigration: func(_ context.Context, from, to tiering.Tier, database, measurement string) ([]tiering.MigrationCandidate, error) {
			calls++
			gotFrom, gotTo = from, to
			gotDatabase, gotMeasurement = database, measurement
			return []tiering.MigrationCandidate{
				{Path: "db1/cpu/a.parquet", SizeBytes: 100},
				{Path: "db1/cpu/b.parquet", SizeBytes: 250},
			}, nil
		},
	}
	app := fiber.New()
	app.Post("/api/v1/tiering/migrate", h.TriggerMigration)

	body := `{"from_tier":"hot","to_tier":"cold","database":"db1","measurement":"cpu","dry_run":true}`
	req := httptest.NewRequest("POST", "/api/v1/tiering/migrate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("POST /api/v1/tiering/migrate: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusOK)
	}
	if calls != 1 {
		t.Fatalf("previewMigration calls = %d, want 1", calls)
	}
	if gotFrom != tiering.TierHot || gotTo != tiering.TierCold {
		t.Fatalf("tiers = %v -> %v, want hot -> cold", gotFrom, gotTo)
	}
	if gotDatabase != "db1" || gotMeasurement != "cpu" {
		t.Fatalf("filters = %q/%q, want db1/cpu", gotDatabase, gotMeasurement)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["status"] != "dry_run" {
		t.Fatalf("status field = %v, want dry_run", response["status"])
	}
	if response["would_migrate"] != float64(2) {
		t.Fatalf("would_migrate = %v, want 2", response["would_migrate"])
	}
	if response["total_bytes"] != float64(350) {
		t.Fatalf("total_bytes = %v, want 350", response["total_bytes"])
	}
}
