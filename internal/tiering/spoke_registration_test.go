package tiering

// Spoke-namespace tiering registration (#686 follow-up): spoke files live one
// level deeper ({spoke}/{db}/{meas}/...), register with the query-visible
// split (database=spoke, measurement=spoke-db), and are excluded from cold
// migration until receipt-aware migration exists.

import (
	"context"
	"testing"
	"time"
)

func TestParseFilePath_SpokeNamespace(t *testing.T) {
	m := &Manager{}

	hour, err := m.parseFilePath("rocket-01/telemetry/engine_temp/2024/03/15/14/f.parquet")
	if err != nil {
		t.Fatalf("spoke hour-level rejected: %v", err)
	}
	if hour.Database != "rocket-01" || hour.Measurement != "telemetry" {
		t.Fatalf("spoke hour split = (%q, %q), want the query-visible (rocket-01, telemetry)", hour.Database, hour.Measurement)
	}
	if want := time.Date(2024, 3, 15, 14, 0, 0, 0, time.UTC); !hour.PartitionTime.Equal(want) {
		t.Fatalf("spoke hour partition time = %v, want %v", hour.PartitionTime, want)
	}

	day, err := m.parseFilePath("rocket-01/telemetry/engine_temp/2024/03/15/f_daily.parquet")
	if err != nil {
		t.Fatalf("spoke day-level rejected: %v", err)
	}
	if day.Database != "rocket-01" || day.Measurement != "telemetry" {
		t.Fatalf("spoke day split = (%q, %q), want (rocket-01, telemetry)", day.Database, day.Measurement)
	}

	if _, err := m.parseFilePath("a/b/c/d/2024/03/15/14/f.parquet"); err == nil {
		t.Fatal("4-segment prefix (double namespacing) unexpectedly accepted")
	}
}

func TestSpokeFilesRegisterButDoNotMigrate(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()

	spokeDaily := "rocket-01/telemetry/engine_temp/2024/03/15/engine_temp_20240315_231459_1_b1_daily.parquet"
	plainDaily := "db1/cpu/2024/03/15/cpu_20240315_231459_1_b1_daily.parquet"
	for _, p := range []string{spokeDaily, plainDaily} {
		if err := hot.Write(ctx, p, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}

	res, err := m.ScanAndRegisterFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors != 0 || res.FilesRegistered != 2 {
		t.Fatalf("scan result = %+v, want both files registered with 0 errors (spoke paths previously errored)", res)
	}

	meta, err := m.metadata.GetFile(ctx, spokeDaily)
	if err != nil || meta.Database != "rocket-01" || meta.Measurement != "telemetry" {
		t.Fatalf("spoke row = (%+v, %v), want database rocket-01, measurement telemetry", meta, err)
	}

	candidates, err := m.migrator.FindCandidates(ctx, TierHot, TierCold)
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Path != plainDaily {
		t.Fatalf("candidates = %+v, want ONLY the plain daily file (spoke files gated off migration)", candidates)
	}
}
