package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestEdgeSyncSpokeMetricsDisabledAreAbsentIssue828(t *testing.T) {
	m := &Metrics{}

	for _, key := range []string{
		"arc_edgesync_spoke_scheduler_enabled",
		"arc_edgesync_spoke_last_success_timestamp_seconds",
		"arc_edgesync_spoke_pass_failures_total",
	} {
		if strings.Contains(m.PrometheusFormat(), key) {
			t.Fatalf("disabled scheduler exported %q", key)
		}
	}

	if _, ok := m.Snapshot()["edge_sync_spoke_scheduler_enabled"]; ok {
		t.Fatal("disabled scheduler appeared in JSON metrics")
	}

	m.RecordEdgeSyncSpokeSuccess(time.Unix(1700000000, 0))
	m.IncEdgeSyncSpokePassFailures()

	m.EnableEdgeSyncSpokeScheduler()

	if got := m.Snapshot()["edge_sync_spoke_last_success_timestamp_seconds"]; got != int64(0) {
		t.Fatalf("disabled outcome leaked after enable: %v", got)
	}
	if got := m.Snapshot()["edge_sync_spoke_pass_failures_total"]; got != int64(0) {
		t.Fatalf("disabled failure leaked after enable: %v", got)
	}
}

func TestEdgeSyncSpokeMetricsWiredToBothFormatsIssue828(t *testing.T) {
	m := &Metrics{}
	m.EnableEdgeSyncSpokeScheduler()

	at := time.Unix(1700000000, 0)
	m.RecordEdgeSyncSpokeSuccess(at)
	m.IncEdgeSyncSpokePassFailures()
	m.IncEdgeSyncSpokePassFailures()

	snapshot := m.Snapshot()

	if got := snapshot["edge_sync_spoke_scheduler_enabled"]; got != int64(1) {
		t.Fatalf("enabled = %v, want 1", got)
	}
	if got := snapshot["edge_sync_spoke_last_success_timestamp_seconds"]; got != at.Unix() {
		t.Fatalf("last success = %v, want %d", got, at.Unix())
	}
	if got := snapshot["edge_sync_spoke_pass_failures_total"]; got != int64(2) {
		t.Fatalf("failures = %v, want 2", got)
	}

	prom := m.PrometheusFormat()

	for _, want := range []string{
		"# TYPE arc_edgesync_spoke_scheduler_enabled gauge\n",
		"arc_edgesync_spoke_scheduler_enabled 1\n",
		"# TYPE arc_edgesync_spoke_last_success_timestamp_seconds gauge\n",
		"arc_edgesync_spoke_last_success_timestamp_seconds 1700000000\n",
		"# TYPE arc_edgesync_spoke_pass_failures_total counter\n",
		"arc_edgesync_spoke_pass_failures_total 2\n",
	} {
		if !strings.Contains(prom, want) {
			t.Fatalf("Prometheus output missing %q", want)
		}
	}
}
