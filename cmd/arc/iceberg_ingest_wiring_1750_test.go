package main

import (
	"os"
	"strings"
	"testing"
	"time"
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
		// Standalone only, and only with the trigger on: one helper decides
		// for the scheduler and for the receiver.
		"IngestInterval: icebergIngestInterval(cfg.Iceberg.IngestExportInterval, clusterCoordinator != nil),",
		"if icebergIngestInterval(cfg.Iceberg.IngestExportInterval, clusterCoordinator != nil) > 0 {",
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
	end := strings.Index(src, `shutdownCoordinator.RegisterHook("iceberg"`) // the statement after the wrapper
	if start < 0 || end < start || add < start || add > end {
		t.Error("the Iceberg receiver is not installed inside icebergStart")
	}
}

func TestIcebergIngestIntervalIsOffOnAClusterNode(t *testing.T) {
	for _, tc := range []struct {
		seconds        int
		hasCoordinator bool
		want           time.Duration
	}{
		{60, false, time.Minute},
		{12, false, 12 * time.Second},
		{0, false, 0},
		{-5, false, 0},
		{60, true, 0},
	} {
		if got := icebergIngestInterval(tc.seconds, tc.hasCoordinator); got != tc.want {
			t.Errorf("icebergIngestInterval(%d, %v) = %v, want %v", tc.seconds, tc.hasCoordinator, got, tc.want)
		}
	}
}

// The append path (#1751): the export is told each file a flush REGISTERS, by
// its key. With the append path on, the published hook must not also make
// requests: it fires before the registration, and a request served in
// between finds no file and lists the whole measurement.
func TestIngestRegistrationIsReportedToTheIcebergAppendPath(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	for _, want := range []string{
		"IngestAppend: cfg.Iceberg.IngestAppendEnabled,",
		"IngestConcurrency: cfg.Iceberg.IngestExportConcurrency,",
		"icebergScheduler.RequestIngestAppend(database, measurement, storagePath, sizeBytes)",
		"arrowBuffer.SetOnFileRemoved(icebergScheduler.RequestReconcile)",
		"exporter.SetVersionRetention(time.Duration(cfg.Iceberg.MinVersionAge)*time.Second,",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("main.go no longer has %q", want)
		}
	}
	on := strings.Index(src, "if cfg.Iceberg.IngestAppendEnabled {")
	registered := strings.Index(src, "arrowBuffer.SetOnFileRegistered(")
	otherwise := strings.Index(src, "arrowBuffer.AddOnFilePublished(func(database, measurement string, _ time.Time) {")
	if on < 0 || registered < on || otherwise < registered {
		t.Fatal("the registered hook and the published hook are not the two arms of iceberg.ingest_append_enabled")
	}
	if between := src[registered:otherwise]; !strings.Contains(between, "} else {") {
		t.Error("the published hook is installed beside the registered one, not instead of it")
	}
	start := strings.Index(src, "icebergStart = func() {")
	end := strings.Index(src, `shutdownCoordinator.RegisterHook("iceberg"`)
	if start < 0 || registered < start || registered > end {
		t.Error("the append receiver is not installed inside icebergStart")
	}
}
