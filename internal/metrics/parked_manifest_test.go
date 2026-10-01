package metrics

import (
	"strings"
	"testing"
)

// The counter #926 introduced must reach both operator surfaces: a parked
// manifest lets compaction resume, so the number is the only aggregated
// sign that a partition needs inspecting.
func TestCompactionManifestParkedUnparseableIsExposedOnBothSurfaces(t *testing.T) {
	m := Get()
	before := m.Snapshot()["compaction_manifests_parked_unparseable_total"].(int64)

	m.IncCompactionManifestParkedUnparseable()

	after, ok := m.Snapshot()["compaction_manifests_parked_unparseable_total"].(int64)
	if !ok {
		t.Fatal("compaction_manifests_parked_unparseable_total is missing from the JSON snapshot")
	}
	if after != before+1 {
		t.Errorf("counter = %d, want %d", after, before+1)
	}
	prom := m.PrometheusFormat()
	for _, want := range []string{
		"# HELP arc_compaction_manifests_parked_unparseable_total",
		"# TYPE arc_compaction_manifests_parked_unparseable_total counter",
		"arc_compaction_manifests_parked_unparseable_total ",
	} {
		if !strings.Contains(prom, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
}
