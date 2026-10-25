package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestBufferRecordsDroppedMetric(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.IncBufferRecordsDropped(3)

	if got := m.Snapshot()["buffer_records_dropped_total"]; got != int64(3) {
		t.Fatalf("buffer_records_dropped_total = %v, want 3", got)
	}

	text := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_buffer_records_dropped_total Records dropped from the buffer after a flush failure when no complete WAL copy was available",
		"# TYPE arc_buffer_records_dropped_total counter",
		"arc_buffer_records_dropped_total 3",
		"# HELP arc_buffer_flush_failures_total Total buffer flush failures",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("Prometheus output missing %q", want)
		}
	}
}
