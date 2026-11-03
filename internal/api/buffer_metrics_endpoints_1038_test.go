package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// The endpoints summary lists the queued and in-flight record gauges beside
// records_buffered, so the three that sum to ingest's memory sit together (#1038).
func TestEndpointMetricsBufferIncludesQueuedAndInflight(t *testing.T) {
	metrics.Get().SetBufferRecordsQueued(1234)
	metrics.Get().SetBufferRecordsInflight(567)
	t.Cleanup(func() {
		metrics.Get().SetBufferRecordsQueued(0)
		metrics.Get().SetBufferRecordsInflight(0)
	})

	s := &Server{}
	app := fiber.New()
	app.Get("/api/v1/metrics/endpoints", s.endpointMetricsHandler)
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/metrics/endpoints", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Buffer map[string]any `json:"buffer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Buffer["records_queued"] != float64(1234) || body.Buffer["records_inflight"] != float64(567) {
		t.Fatalf("buffer = %v, want records_queued 1234 and records_inflight 567", body.Buffer)
	}
	if _, ok := body.Buffer["records_dropped_total"].(float64); !ok {
		t.Fatalf("buffer records_dropped_total = %v, want numeric counter", body.Buffer["records_dropped_total"])
	}
}
