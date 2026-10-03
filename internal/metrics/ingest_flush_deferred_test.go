package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestIngestFlushDeferredMetric(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.IncIngestFlushDeferred()

	if got := m.Snapshot()["ingest_flush_deferred_total"]; got != int64(1) {
		t.Fatalf("ingest_flush_deferred_total = %v, want 1", got)
	}
	if got := m.PrometheusFormat(); !strings.Contains(got, "arc_ingest_flush_deferred_total 1") {
		t.Fatalf("Prometheus output missing deferred-flush counter:\n%s", got)
	}
}
