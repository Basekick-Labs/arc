package config

import (
	"strings"
	"testing"
)

// iceberg.load_export_min_interval (#1752): 0 (the default) is off; otherwise
// at least 5 and at most iceberg.ingest_export_interval, which must be on.
func TestLoad_IcebergLoadExportMinInterval(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		want    int
		wantErr string
	}{
		{"default off", nil, 0, ""},
		{"shortest", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "5"}, 5, ""},
		{"non-default", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "7", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "10"}, 7, ""},
		{"equal to the ingest interval", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "60"}, 60, ""},
		{"too short", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "4"}, 0, "iceberg.load_export_min_interval must be 0 or between 5"},
		{"negative", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "-1"}, 0, "iceberg.load_export_min_interval must be 0 or between 5"},
		{"above the ingest interval", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "61"}, 0, "iceberg.load_export_min_interval must be 0 or between 5"},
		{"ingest trigger off", map[string]string{"ARC_ICEBERG_LOAD_EXPORT_MIN_INTERVAL": "5", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "0"}, 0, "iceberg.load_export_min_interval must be 0 or between 5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_ICEBERG_ENABLED", "true")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load() error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Iceberg.LoadExportMinInterval != tc.want {
				t.Errorf("LoadExportMinInterval = %d, want %d", cfg.Iceberg.LoadExportMinInterval, tc.want)
			}
		})
	}
}
