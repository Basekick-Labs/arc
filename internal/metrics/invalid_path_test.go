package metrics

import (
	"strings"
	"testing"
)

// TestStorageInvalidPathQuarantinedIsExposedOnBothSurfaces pins that the
// counter #747 introduced reaches the two places operators read.
//
// The counter matters more than most: the entries it counts are dropped from
// their work sets, so after the fix there is no retry storm, no growing error
// log and no stuck queue to notice. This number is the only aggregated signal
// that anything is wrong, which makes "it is exported" part of the fix rather
// than decoration.
func TestStorageInvalidPathQuarantinedIsExposedOnBothSurfaces(t *testing.T) {
	m := Get()
	before := m.Snapshot()["storage_invalid_path_quarantined_total"].(int64)

	m.IncStorageInvalidPathQuarantined()

	after, ok := m.Snapshot()["storage_invalid_path_quarantined_total"].(int64)
	if !ok {
		t.Fatal("storage_invalid_path_quarantined_total is missing from the JSON snapshot")
	}
	if after != before+1 {
		t.Errorf("counter = %d, want %d", after, before+1)
	}

	prom := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_storage_invalid_path_quarantined_total",
		"# TYPE arc_storage_invalid_path_quarantined_total counter",
		"arc_storage_invalid_path_quarantined_total ",
	} {
		if !strings.Contains(prom, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
}
