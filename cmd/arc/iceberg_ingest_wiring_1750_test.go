package main

import (
	"os"
	"strings"
	"testing"
)

// An ingest flush reaches the Iceberg scheduler only if main installs the
// receiver, and the query caches keep theirs only if both are ADDED: the
// buffer's Set form replaces every receiver, so using it for either would
// silently unhook the other (#1750, #1757).
func TestIngestFlushIsReportedToQueryCachesAndIceberg(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, want := range []string{
		"arrowBuffer.AddOnFilePublished(queryHandler.FilePublished)",
		"icebergScheduler.RequestIngestReconcile(database, measurement)",
		"IngestInterval: time.Duration(cfg.Iceberg.IngestExportInterval) * time.Second",
		// Standalone only, and only with the trigger on.
		"if clusterCoordinator == nil && cfg.Iceberg.IngestExportInterval > 0 {",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("main.go no longer has %q", want)
		}
	}
	if strings.Contains(src, "arrowBuffer.SetOnFilePublished(") {
		t.Error("main.go uses SetOnFilePublished, which replaces every receiver; use AddOnFilePublished")
	}
	// The Iceberg receiver is installed when the export starts, not when the
	// scheduler is built: a scheduler that never starts would only collect requests.
	start := strings.Index(src, "icebergStart = func() {")
	add := strings.Index(src, "icebergScheduler.RequestIngestReconcile(database, measurement)")
	if start < 0 || add < start {
		t.Error("the Iceberg receiver is not installed inside icebergStart")
	}
}
