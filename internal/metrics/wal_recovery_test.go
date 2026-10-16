package metrics

import (
	"strings"
	"testing"
)

func TestWALRecoveryMetricsAreExposed(t *testing.T) {
	m := &Metrics{}
	m.SetWALDirectoryBytes(12345)
	m.IncWALQuarantinedFiles()

	snapshot := m.Snapshot()
	if got := snapshot["wal_directory_bytes"]; got != int64(12345) {
		t.Fatalf("wal_directory_bytes = %v, want 12345", got)
	}
	if got := snapshot["wal_quarantined_files"]; got != int64(1) {
		t.Fatalf("wal_quarantined_files = %v, want 1", got)
	}

	formatted := m.PrometheusFormat()
	for _, want := range []string{
		"# TYPE arc_wal_dir_bytes gauge",
		"arc_wal_dir_bytes 12345",
		"# TYPE arc_wal_quarantined_files_total counter",
		"arc_wal_quarantined_files_total 1",
	} {
		if !strings.Contains(formatted, want) {
			t.Errorf("Prometheus output missing %q", want)
		}
	}
}

// #676: lost entries, the disk-full subset and recovered failures.
func TestWALWriteFailureMetricsAreExposed(t *testing.T) {
	m := &Metrics{}
	m.IncWALFailedWrites()
	m.IncWALFailedWrites()
	m.IncWALDiskFullWrites()
	m.IncWALRecoveredWrites()

	snapshot := m.Snapshot()
	if snapshot["wal_disk_full_writes"] != int64(1) || snapshot["wal_recovered_writes"] != int64(1) {
		t.Fatalf("snapshot = %v / %v, want 1 / 1", snapshot["wal_disk_full_writes"], snapshot["wal_recovered_writes"])
	}
	formatted := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_wal_failed_writes_total WAL entries lost to a failed write",
		"arc_wal_failed_writes_total 2",
		"# TYPE arc_wal_disk_full_writes_total counter",
		"arc_wal_disk_full_writes_total 1",
		"# TYPE arc_wal_recovered_writes_total counter",
		"arc_wal_recovered_writes_total 1",
	} {
		if !strings.Contains(formatted, want) {
			t.Errorf("Prometheus output missing %q", want)
		}
	}
}
