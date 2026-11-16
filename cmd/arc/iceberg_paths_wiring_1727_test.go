package main

import (
	"os"
	"strings"
	"testing"
)

// The paths that remove, move or rewrite data files reach the Iceberg
// scheduler only through these lines (#1727); one dropped would put its path
// back on the reconcile timer without any other test failing.
func TestIcebergIsToldAboutChangedPaths(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"deleteHandler.SetOnPathsChanged(notifyIcebergPaths)",
		"databasesHandler.SetOnPathsChanged(notifyIcebergPaths)",
		"retentionHandler.SetOnPathsChanged(notifyIcebergPaths)",
		"tieringManager.SetOnPathsChanged(notifyIcebergPaths)",
		"notifyIcebergPaths([]string{path})",
		"icebergPathsChanged.Store(&requestPaths)",
	} {
		if !strings.Contains(string(src), want) {
			t.Errorf("main.go no longer has %q", want)
		}
	}
}
