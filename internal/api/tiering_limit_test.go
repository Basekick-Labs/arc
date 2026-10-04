package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
)

func newTieringLimitTestApp(files []tiering.FileMetadata) *fiber.App {
	h := &TieringHandler{
		getFiles: func(_ context.Context, _, _ string) ([]tiering.FileMetadata, error) {
			return files, nil
		},
	}
	app := fiber.New()
	app.Get("/api/v1/tiering/files", h.GetFiles)
	return app
}

func TestTieringGetFilesRejectsInvalidLimit(t *testing.T) {
	h := &TieringHandler{}
	app := fiber.New()
	app.Get("/api/v1/tiering/files", h.GetFiles)

	for _, tc := range []struct {
		name  string
		limit string
	}{
		{name: "negative", limit: "-1"},
		{name: "not an integer", limit: "abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/tiering/files?limit="+tc.limit, nil), 1000)
			if err != nil {
				t.Fatalf("app.Test: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusBadRequest)
			}
		})
	}
}

func TestTieringGetFilesLimitSemantics(t *testing.T) {
	files := []tiering.FileMetadata{{Path: "one"}, {Path: "two"}, {Path: "three"}}
	app := newTieringLimitTestApp(files)

	for _, tc := range []struct {
		name      string
		limit     string
		wantCount int
	}{
		{name: "zero returns empty", limit: "0", wantCount: 0},
		{name: "positive limit", limit: "2", wantCount: 2},
		{name: "larger than result", limit: "5000", wantCount: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/tiering/files?limit="+tc.limit, nil), 1000)
			if err != nil {
				t.Fatalf("app.Test: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusOK {
				t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusOK)
			}
			var body struct {
				Files []tiering.FileMetadata `json:"files"`
				Count int                    `json:"count"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body.Count != tc.wantCount || len(body.Files) != tc.wantCount {
				t.Fatalf("count=%d files=%d, want %d", body.Count, len(body.Files), tc.wantCount)
			}
		})
	}
}
