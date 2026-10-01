package metrics

import (
	"strings"
	"testing"
)

// arc_backup_skipped_files (#977) is a gauge set by every backup that finishes
// its copy phases, so a clean run clears it; it must be present in both the
// JSON snapshot and the Prometheus output.
func TestBackupSkippedFilesGauge(t *testing.T) {
	m := Get()

	m.SetBackupSkippedFiles(3)
	if got, ok := m.Snapshot()["backup_skipped_files"].(int64); !ok || got != 3 {
		t.Fatalf("backup_skipped_files = %v (%t), want 3", got, ok)
	}
	prom := m.PrometheusFormat()
	for _, want := range []string{"# HELP arc_backup_skipped_files ", "# TYPE arc_backup_skipped_files gauge\n", "arc_backup_skipped_files 3\n"} {
		if !strings.Contains(prom, want) {
			t.Errorf("Prometheus output lacks %q", want)
		}
	}

	m.SetBackupSkippedFiles(0)
	if got, _ := m.Snapshot()["backup_skipped_files"].(int64); got != 0 {
		t.Errorf("after a clean backup = %d, want 0", got)
	}
}
