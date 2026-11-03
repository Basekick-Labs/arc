package tiering

import (
	"context"
	"testing"
)

// #1350: a DELETE reads one measurement's cold rows, quarantined ones and
// other measurements' excluded, and nothing hot.
func TestColdFilesForMeasurement(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	recordAt(t, store, "db1/cpu/2025/01/01/a_daily.parquet", "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/q_daily.parquet", "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/00/h.parquet", "db1", TierHot)
	recordAt(t, store, "db2/cpu/2025/01/01/b_daily.parquet", "db2", TierCold)
	if err := store.QuarantineFile(ctx, "db1/cpu/2025/01/01/q_daily.parquet", "unusable key"); err != nil {
		t.Fatal(err)
	}
	m := &Manager{metadata: store}
	rows, err := m.ColdFilesForMeasurement(ctx, "db1", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Path != "db1/cpu/2025/01/01/a_daily.parquet" {
		t.Fatalf("rows %+v, want only db1's unquarantined cold row", rows)
	}
	if m.ColdConfigured() {
		t.Fatal("a manager with no config reports cold storage configured")
	}
	var nilM *Manager
	if nilM.ColdConfigured() {
		t.Fatal("a nil manager reports cold storage configured")
	}
}
