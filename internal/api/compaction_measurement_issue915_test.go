package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func TestCompactionMeasurementValidationIssue915(t *testing.T) {
	manager := &compaction.Manager{CycleTimeout: time.Minute}
	handler := NewCompactionHandler(
		manager, nil, nil, nil, zerolog.Nop(),
	)

	app := fiber.New()
	handler.RegisterRoutes(app)

	paths := []string{
		"/api/v1/compaction/trigger?measurement=cpu",
		"/api/v1/compaction/trigger?database=db&measurement=bad%2Fname",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf(
					"status = %d, want 400",
					resp.StatusCode,
				)
			}
		})
	}
}
