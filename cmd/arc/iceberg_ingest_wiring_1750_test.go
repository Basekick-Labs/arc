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
		// Standalone flushes and, on a cluster node, files reaching it
		// (#1756): the scheduler's interval is the sum of the two helpers,
		// exactly one of which is non-zero.
		"IngestInterval: icebergIngestInterval(cfg.Iceberg.IngestExportInterval, clusterCoordinator != nil) +",
		"icebergArrivalInterval(cfg.Iceberg.IngestExportInterval, clusterCoordinator != nil),",
		"clusterCoordinator.SetIcebergFileArrived(",
		"if icebergOwnFlushReports(cfg.Iceberg.IngestExportInterval, clusterCoordinator != nil, storageBackend.Type()) {",
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
		"icebergPassesAtOnce := icebergReconcileConcurrency(cfg.Iceberg.ReconcileConcurrency, clusterCoordinator != nil, icebergTopo.warnNoElection, storageBackend.Type() != \"local\")",
		"ReconcileConcurrency: icebergPassesAtOnce,",
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

// #1786: reconcile passes of several tables at once are a standalone node's;
// a node of a cluster, or one that may not be the only exporter, runs one.
func TestIcebergReconcileConcurrencyIsOneOnAClusterNode(t *testing.T) {
	for _, tc := range []struct {
		configured                              int
		hasCoordinator, noElection, objectStore bool
		want                                    int
	}{
		// Not set: by the storage.
		{0, false, false, true, icebergReconcileOnObjectStore},
		{0, false, false, false, 1},
		// Set: as set, on either storage.
		{4, false, false, false, 4},
		{16, false, false, true, 16},
		{1, false, false, true, 1},
		// A cluster node, or one with no election on a shared bucket: one.
		{4, true, false, true, 1},
		{0, true, false, true, 1},
		{4, false, true, true, 1},
		{0, false, true, true, 1},
		{4, true, true, false, 1},
		{-3, false, false, true, 1},
	} {
		if got := icebergReconcileConcurrency(tc.configured, tc.hasCoordinator, tc.noElection, tc.objectStore); got != tc.want {
			t.Errorf("icebergReconcileConcurrency(%d, coordinator=%v, noElection=%v, objectStore=%v) = %d, want %d",
				tc.configured, tc.hasCoordinator, tc.noElection, tc.objectStore, got, tc.want)
		}
	}
	if icebergReconcileOnObjectStore != 4 {
		t.Errorf("a standalone node on an object store runs %d passes at once by default, want 4", icebergReconcileOnObjectStore)
	}
}

// #1756: a cluster node asks for exports when files reach it; a standalone
// node does not (its flushes report).
func TestIcebergArrivalIntervalIsOnlyOnAClusterNode(t *testing.T) {
	for _, tc := range []struct {
		seconds        int
		hasCoordinator bool
		want           time.Duration
	}{
		{60, true, time.Minute},
		{0, true, 0},
		{60, false, 0},
	} {
		if got := icebergArrivalInterval(tc.seconds, tc.hasCoordinator); got != tc.want {
			t.Errorf("icebergArrivalInterval(%d, %v) = %v, want %v", tc.seconds, tc.hasCoordinator, got, tc.want)
		}
	}
}

// #1902: a cluster node's own flushes report on local storage only.
func TestIcebergOwnFlushReports(t *testing.T) {
	for _, tc := range []struct {
		seconds        int
		hasCoordinator bool
		storage        string
		want           bool
	}{
		{60, false, "local", true},
		{60, false, "s3", true},
		{0, false, "local", false},
		{60, true, "local", true},
		{60, true, "s3", false},
		{60, true, "azure", false},
		{0, true, "local", false},
	} {
		if got := icebergOwnFlushReports(tc.seconds, tc.hasCoordinator, tc.storage); got != tc.want {
			t.Errorf("icebergOwnFlushReports(%d, %v, %q) = %v, want %v", tc.seconds, tc.hasCoordinator, tc.storage, got, tc.want)
		}
	}
}
