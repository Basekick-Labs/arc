package config

import (
	"strings"
	"testing"
)

// iceberg.export_lag_table_series (#1772): off by default, 0..1000.
func TestIcebergExportLagTableSeries(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_ICEBERG_ENABLED", "true")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Iceberg.ExportLagTableSeries != 0 {
		t.Errorf("iceberg.export_lag_table_series defaults to %d, want 0", cfg.Iceberg.ExportLagTableSeries)
	}
	for ok, want := range map[string]int{"0": 0, "2": 2, "1000": 1000} {
		t.Setenv("ARC_ICEBERG_EXPORT_LAG_TABLE_SERIES", ok)
		if cfg, err = Load(); err != nil || cfg.Iceberg.ExportLagTableSeries != want {
			t.Errorf("iceberg.export_lag_table_series = %s: %d, %v", ok, cfg.Iceberg.ExportLagTableSeries, err)
		}
	}
	for _, bad := range []string{"-1", "1001"} {
		t.Setenv("ARC_ICEBERG_EXPORT_LAG_TABLE_SERIES", bad)
		if _, err = Load(); err == nil || !strings.Contains(err.Error(), "iceberg.export_lag_table_series") {
			t.Errorf("iceberg.export_lag_table_series = %s: err %v, want it refused by name", bad, err)
		}
	}
}
