package main

import (
	"os"
	"strings"
	"testing"
)

// The Iceberg export catches up after a compaction only if main wires both
// paths to the scheduler: legacy jobs through the manager, and staged
// cluster jobs through the completion watcher (#1722). Either line dropped
// would put that path back on the reconcile timer without any test failing.
func TestIcebergIsToldAboutFinishedCompactions(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"compactionManager.SetOnMeasurementCompacted(icebergScheduler.RequestReconcile)",
		"watcher.SetStagedSettled(compactionManager.MeasurementCompacted)",
	} {
		if !strings.Contains(string(src), want) {
			t.Errorf("main.go no longer wires %q", want)
		}
	}
}
