package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestEdgeSyncMetricsAreExported(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.EnableEdgeSyncSpokeScheduler()
	completedAt := time.Unix(1_800_000_000, 0)
	m.RecordEdgeSyncSpokeSuccess(completedAt)
	m.IncEdgeSyncSpokePassFailures()

	snapshot := m.Snapshot()
	if got := snapshot["edge_sync_spoke_last_success_timestamp_seconds"]; got != completedAt.Unix() {
		t.Errorf("snapshot last success = %v, want %d", got, completedAt.Unix())
	}
	if got := snapshot["edge_sync_spoke_pass_failures_total"]; got != int64(1) {
		t.Errorf("snapshot failures = %v, want 1", got)
	}

	text := m.PrometheusFormat()
	for _, want := range []string{
		"# TYPE arc_edgesync_spoke_last_success_timestamp_seconds gauge",
		"arc_edgesync_spoke_last_success_timestamp_seconds 1800000000",
		"# TYPE arc_edgesync_spoke_pass_failures_total counter",
		"arc_edgesync_spoke_pass_failures_total 1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("Prometheus output missing %q", want)
		}
	}
}
