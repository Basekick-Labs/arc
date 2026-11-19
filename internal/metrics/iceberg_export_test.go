package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestIcebergExportMetrics(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	for _, d := range []time.Duration{time.Second, 61 * time.Second, 2 * time.Hour, -time.Second} {
		m.ObserveIcebergExportLag(d)
	}
	m.AddIcebergExportBacklogFiles(3)
	m.AddIcebergExportBacklogFiles(0)
	out := m.PrometheusFormat()
	for _, want := range []string{
		"# TYPE arc_iceberg_export_lag_seconds histogram\n",
		`arc_iceberg_export_lag_seconds_bucket{le="1"} 2` + "\n",  // 1 s on its bound, and the negative one as zero
		`arc_iceberg_export_lag_seconds_bucket{le="60"} 2` + "\n", // 61 s is over 60
		`arc_iceberg_export_lag_seconds_bucket{le="90"} 3` + "\n",
		`arc_iceberg_export_lag_seconds_bucket{le="3600"} 3` + "\n",
		`arc_iceberg_export_lag_seconds_bucket{le="+Inf"} 4` + "\n",
		"arc_iceberg_export_lag_seconds_sum 7262\n",
		"arc_iceberg_export_lag_seconds_count 4\n",
		"# TYPE arc_iceberg_export_pending_seconds gauge\n",
		"arc_iceberg_export_pending_seconds 0\n",
		"# TYPE arc_iceberg_export_backlog_files_total counter\n",
		"arc_iceberg_export_backlog_files_total 3\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
	snap := m.Snapshot()
	if snap["iceberg_export_lag_count"] != int64(4) || snap["iceberg_export_backlog_files"] != int64(3) || snap["iceberg_export_lag_sum_seconds"] != float64(7262) {
		t.Errorf("snapshot: count=%v backlog=%v sum=%v", snap["iceberg_export_lag_count"], snap["iceberg_export_backlog_files"], snap["iceberg_export_lag_sum_seconds"])
	}

	// The pending gauge is read at scrape time: it grows while a file waits.
	m.SetIcebergOldestPendingExport(time.Now().Add(-90 * time.Second))
	var got float64
	line := ""
	for _, l := range strings.Split(m.PrometheusFormat(), "\n") {
		if strings.HasPrefix(l, "arc_iceberg_export_pending_seconds ") {
			line = l
		}
	}
	if _, err := fmt.Sscanf(line, "arc_iceberg_export_pending_seconds %g", &got); err != nil || got < 90 || got > 100 {
		t.Errorf("pending = %q, want about 90 s", line)
	}
	if got := m.Snapshot()["iceberg_export_pending_seconds"].(float64); got < 90 || got > 100 {
		t.Errorf("snapshot pending = %v, want about 90 s", got)
	}
	m.SetIcebergOldestPendingExport(time.Time{})
	if got := m.Snapshot()["iceberg_export_pending_seconds"]; got != float64(0) {
		t.Errorf("pending = %v after it was cleared, want 0", got)
	}
	// A report stamped ahead of the clock reads as zero, not negative.
	m.SetIcebergOldestPendingExport(time.Now().Add(time.Hour))
	if got := m.Snapshot()["iceberg_export_pending_seconds"]; got != float64(0) {
		t.Errorf("pending = %v for a report ahead of the clock, want 0", got)
	}
}

func TestIcebergExportLagLabelsMatchBounds(t *testing.T) {
	if len(icebergExportLagLabels) != len(icebergExportLagBounds)+1 {
		t.Fatalf("%d labels for %d bounds", len(icebergExportLagLabels), len(icebergExportLagBounds))
	}
	for i := 1; i < len(icebergExportLagBounds); i++ {
		if icebergExportLagBounds[i] <= icebergExportLagBounds[i-1] {
			t.Fatalf("bounds not increasing at %d", i)
		}
	}
}
