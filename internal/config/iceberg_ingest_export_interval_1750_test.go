package config

import (
	"strings"
	"testing"
)

// iceberg.ingest_export_interval: its default, its range, and the bound that
// ties it to iceberg.retain_snapshots (the published versions kept must span
// at least icebergMinVersionSpanSeconds).
func TestLoad_IcebergIngestExportInterval(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		want    int
		wantErr string
	}{
		{"default", nil, 60, ""},
		{"off", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "0"}, 0, ""},
		{"shortest the default retention allows", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "11"}, 11, ""},
		{"too short for the default retention", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "10"}, 0, "to at least 11"},
		{"negative", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "-1"}, 0, "must be >= 0"},
		{"the default with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1"}, 60, ""},
		{"too short with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "59"}, 0, "to at least 60"},
		{"short, with enough snapshots kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "23", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "5"}, 5, ""},
		{"off, with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "0"}, 0, ""},
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
			if cfg.Iceberg.IngestExportInterval != tc.want {
				t.Errorf("IngestExportInterval = %d, want %d", cfg.Iceberg.IngestExportInterval, tc.want)
			}
		})
	}
	// With the export off the key is not checked: nothing reads it.
	t.Run("export disabled", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_ICEBERG_INGEST_EXPORT_INTERVAL", "1")
		if _, err := Load(); err != nil {
			t.Fatalf("Load() error = %v", err)
		}
	})
}
