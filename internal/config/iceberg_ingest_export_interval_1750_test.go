package config

import (
	"strings"
	"testing"
)

// iceberg.ingest_export_interval: its default, its range, and the bound that
// ties it to iceberg.retain_snapshots (interval x retain_snapshots must reach
// icebergMinVersionSpan). A value the operator wrote is refused; the default
// is raised with a warning.
func TestLoad_IcebergIngestExportInterval(t *testing.T) {
	for _, tc := range []struct {
		name     string
		env      map[string]string
		want     int
		wantErr  string
		wantWarn bool
	}{
		{"default", nil, 60, "", false},
		{"off", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "0"}, 0, "", false},
		{"shortest the default retention allows", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "12"}, 12, "", false},
		{"too short for the default retention", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "11"}, 0, "to at least 12", false},
		{"negative", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "-1"}, 0, "must be >= 0", false},
		// A low retain_snapshots with the key left alone: an upgrade must not
		// stop the node, so the default is raised and a warning says so.
		{"the default with one snapshot kept is raised", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1"}, 120, "", true},
		{"the default with two snapshots kept stands", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "2"}, 60, "", false},
		{"written too short with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "119"}, 0, "to at least 120", false},
		{"written, long enough with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "120"}, 120, "", false},
		{"short, with enough snapshots kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "24", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "5"}, 5, "", false},
		{"short, one snapshot short of enough", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "23", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "5"}, 0, "to at least 6", false},
		{"off, with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "0"}, 0, "", false},
		// With Glue on, a failed Glue update is retried after
		// glue.sync_interval: the versions kept must span that plus a minute.
		{"Glue on, default sync interval: the same bound", map[string]string{"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
			"ARC_ICEBERG_GLUE_ENABLED": "true", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "12"}, 12, "", false},
		{"Glue on with a long sync interval", map[string]string{"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
			"ARC_ICEBERG_GLUE_ENABLED": "true", "ARC_ICEBERG_GLUE_SYNC_INTERVAL": "300", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "35"}, 0, "to at least 36", false},
		{"Glue on with a long sync interval, the default stands", map[string]string{"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
			"ARC_ICEBERG_GLUE_ENABLED": "true", "ARC_ICEBERG_GLUE_SYNC_INTERVAL": "300"}, 60, "", false},
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
			warned := false
			for _, w := range cfg.Warnings {
				if w.Key == "iceberg.ingest_export_interval" {
					warned = true
					if !strings.Contains(w.Message, "raised from its default") {
						t.Errorf("warning does not say what was done: %s", w.Message)
					}
				}
			}
			if warned != tc.wantWarn {
				t.Errorf("warned = %v, want %v", warned, tc.wantWarn)
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
