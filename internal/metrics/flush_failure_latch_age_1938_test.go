package metrics

import (
	"strings"
	"testing"
	"time"
)

// #1938: how long the WAL recovery latch has been set, as a gauge in seconds.
func TestFlushFailureLatchAgeGauge1938(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.SetFlushFailureLatchAge(90 * time.Second)
	out := m.PrometheusFormat()
	for _, want := range []string{
		"# TYPE arc_ingest_flush_failure_latch_age_seconds gauge\n",
		"arc_ingest_flush_failure_latch_age_seconds 90\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
	if got := m.Snapshot()["ingest_flush_failure_latch_age_seconds"]; got != 90.0 {
		t.Errorf("snapshot = %v, want 90", got)
	}
	m.SetFlushFailureLatchAge(0)
	if !strings.Contains(m.PrometheusFormat(), "arc_ingest_flush_failure_latch_age_seconds 0\n") {
		t.Error("a clear latch is not 0")
	}
}
