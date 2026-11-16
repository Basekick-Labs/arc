package main

import (
	"os"
	"strings"
	"testing"
)

// The exporter node's own signals reach the Iceberg scheduler only through
// these lines (#1728).
func TestIcebergIsToldAboutClusterRemovals(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"clusterCoordinator.SetIcebergPathCallbacks(icebergScheduler.RequestReconcilePaths, icebergScheduler.RequestReconcilePathsLater)",
		"reconciler.SetOnPathsDeleted(notifyIcebergPaths)",
	} {
		if !strings.Contains(string(src), want) {
			t.Errorf("main.go no longer has %q", want)
		}
	}
}
