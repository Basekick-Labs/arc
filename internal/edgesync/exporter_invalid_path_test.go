package edgesync

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestExporter_ExportSkipsAnUnusableKeyInsteadOfWedging is the air-gap half of
// #747, and it is the worst case found: nothing on this path caps attempts.
//
// The keep-on-Exists-error rule in selectEntries is written for TRANSIENT
// errors, so a permanent one kept the entry, the copy then failed, and a copy
// failure aborts the WHOLE export rather than one file. The next export
// re-selected the same row and aborted again, so one unusable file stopped all
// telemetry leaving the site, forever, with no self-heal and no attempt cap —
// on exactly the box where editing SQLite by hand is not an option.
//
// The row is inserted with Ledger.Track rather than produced by discovery,
// because that is its real provenance. The ledger is persisted SQLite state
// that survives upgrades, so the row dates from a binary that still accepted
// the key; today's discovery drops such a file earlier, at hashFile. Same
// provenance class as a compaction manifest carrying an unusable OutputPath.
func TestExporter_ExportSkipsAnUnusableKeyInsteadOfWedging(t *testing.T) {
	ctx := context.Background()
	rig := newAgentRig(t)

	rig.writeFile(t, "metrics/cpu/2026/08/07/14/keep.parquet", []byte("kept"))

	// A backslash is a legal character in a POSIX filename and an unusable
	// storage key, because Azure treats it as a separator.
	const unusableKey = `metrics/cpu/2026/08/07/15/bad\name.parquet`
	if err := storage.ValidateKey(unusableKey); err == nil {
		t.Fatal("the key under test is accepted by ValidateKey, so this test proves nothing")
	}

	dest := t.TempDir()
	policy, err := NewDestinationPolicy([]string{dest}, "")
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	writer, err := NewBundleWriter(BundleWriterConfig{
		Backend: rig.backend,
		SpokeID: "rocket-01",
		HubID:   DefaultHubID,
		Secret:  "0123456789abcdef0123456789abcdef",
		Logger:  zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	discoverer, err := NewDiscoverer(rig.ledger, rig.backend, DefaultHubID, zerolog.Nop())
	if err != nil {
		t.Fatalf("discoverer: %v", err)
	}
	exporter, err := NewExporter(ExporterConfig{
		Ledger:     rig.ledger,
		Writer:     writer,
		Policy:     policy,
		Discoverer: discoverer,
		HubID:      DefaultHubID,
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("exporter: %v", err)
	}

	if _, err := discoverer.Discover(ctx); err != nil {
		t.Fatalf("discover: %v", err)
	}
	// The row an older binary left behind.
	if err := rig.ledger.Track(ctx, &LedgerEntry{
		HubID:         DefaultHubID,
		Path:          unusableKey,
		SHA256:        "deadbeef",
		SizeBytes:     11,
		Database:      "metrics",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 8, 7, 15, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("track: %v", err)
	}

	beforeMetric := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64)

	res, err := exporter.Export(ctx, dest, 0)
	if err != nil {
		t.Fatalf("Export aborted on one unusable key instead of skipping it: %v", err)
	}
	if res.FileCount != 1 {
		t.Errorf("bundle files = %d, want 1 (the addressable file)", res.FileCount)
	}
	if res.Skipped != 1 {
		t.Errorf("export skipped = %d, want 1", res.Skipped)
	}
	if got := metrics.Get().Snapshot()["storage_invalid_path_quarantined_total"].(int64) - beforeMetric; got != 1 {
		t.Errorf("quarantine counter moved by %d, want 1", got)
	}

	// The wedge regression proper: the next export must not re-select it.
	if _, err := exporter.Export(ctx, dest, 0); err != ErrNothingToExport {
		t.Errorf("second export = %v, want ErrNothingToExport; the unusable key was re-selected and the bundle path is still wedged", err)
	}
}
