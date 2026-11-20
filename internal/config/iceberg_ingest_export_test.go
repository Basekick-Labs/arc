package config

import (
	"strconv"
	"strings"
	"testing"
)

// The keys that govern ingest-triggered Iceberg exports (#1750, #1751):
// iceberg.ingest_export_interval (0, or at least 5 s), iceberg.min_version_age
// (at least icebergMinVersionSpan: a value the operator wrote is refused, the
// default is raised with a warning) and iceberg.ingest_append_enabled.
func TestLoad_IcebergIngestExport(t *testing.T) {
	glue := func(extra map[string]string) map[string]string {
		env := map[string]string{"ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b", "ARC_ICEBERG_GLUE_ENABLED": "true"}
		for k, v := range extra {
			env[k] = v
		}
		return env
	}
	for _, tc := range []struct {
		name         string
		env          map[string]string
		wantInterval int
		wantAge      int
		wantErr      string
		wantWarn     bool
	}{
		{"defaults", nil, 60, 300, "", false},
		{"interval off", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "0"}, 0, 300, "", false},
		{"shortest interval", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "5"}, 5, 300, "", false},
		{"interval too short", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "4"}, 0, 0, "must be 0 or at least 5", false},
		{"interval negative", map[string]string{"ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "-1"}, 0, 0, "must be 0 or at least 5", false},
		// The interval no longer depends on retain_snapshots: versions are
		// kept by age.
		{"short interval with one snapshot kept", map[string]string{"ARC_ICEBERG_RETAIN_SNAPSHOTS": "1", "ARC_ICEBERG_INGEST_EXPORT_INTERVAL": "5"}, 5, 300, "", false},
		{"shortest age", map[string]string{"ARC_ICEBERG_MIN_VERSION_AGE": "120"}, 60, 120, "", false},
		{"age too short", map[string]string{"ARC_ICEBERG_MIN_VERSION_AGE": "119"}, 0, 0, "iceberg.min_version_age must be at least 120", false},
		{"age zero", map[string]string{"ARC_ICEBERG_MIN_VERSION_AGE": "0"}, 0, 0, "iceberg.min_version_age must be at least 120", false},
		{"a long age", map[string]string{"ARC_ICEBERG_MIN_VERSION_AGE": "3600"}, 60, 3600, "", false},
		// With Glue on, a failed Glue update is retried after
		// glue.sync_interval: a version must outlive that plus a minute.
		{"Glue on, default sync interval", glue(nil), 60, 300, "", false},
		{"Glue on, the longest sync interval the default age covers", glue(map[string]string{"ARC_ICEBERG_GLUE_SYNC_INTERVAL": "240"}), 60, 300, "", false},
		{"Glue on with a long sync interval: the default age is raised", glue(map[string]string{"ARC_ICEBERG_GLUE_SYNC_INTERVAL": "600"}), 60, 660, "", true},
		{"Glue on with a long sync interval: a written age is refused", glue(map[string]string{"ARC_ICEBERG_GLUE_SYNC_INTERVAL": "600", "ARC_ICEBERG_MIN_VERSION_AGE": "659"}), 0, 0, "iceberg.min_version_age must be at least 660", false},
		{"Glue on with a long sync interval: a written age that covers it", glue(map[string]string{"ARC_ICEBERG_GLUE_SYNC_INTERVAL": "600", "ARC_ICEBERG_MIN_VERSION_AGE": "660"}), 60, 660, "", false},
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
			if cfg.Iceberg.IngestExportInterval != tc.wantInterval {
				t.Errorf("IngestExportInterval = %d, want %d", cfg.Iceberg.IngestExportInterval, tc.wantInterval)
			}
			if cfg.Iceberg.MinVersionAge != tc.wantAge {
				t.Errorf("MinVersionAge = %d, want %d", cfg.Iceberg.MinVersionAge, tc.wantAge)
			}
			warned := false
			for _, w := range cfg.Warnings {
				if w.Key == "iceberg.min_version_age" {
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
	t.Run("append switch", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_ICEBERG_ENABLED", "true")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if !cfg.Iceberg.IngestAppendEnabled {
			t.Error("iceberg.ingest_append_enabled defaults to false, want true")
		}
		t.Setenv("ARC_ICEBERG_INGEST_APPEND_ENABLED", "false")
		if cfg, err = Load(); err != nil {
			t.Fatal(err)
		}
		if cfg.Iceberg.IngestAppendEnabled {
			t.Error("iceberg.ingest_append_enabled = false was not read")
		}
	})
	t.Run("export concurrency", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_ICEBERG_ENABLED", "true")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Iceberg.IngestExportConcurrency != 8 {
			t.Errorf("iceberg.ingest_export_concurrency defaults to %d, want 8", cfg.Iceberg.IngestExportConcurrency)
		}
		for _, ok := range []string{"1", "16", "32"} {
			t.Setenv("ARC_ICEBERG_INGEST_EXPORT_CONCURRENCY", ok)
			if cfg, err = Load(); err != nil {
				t.Fatalf("iceberg.ingest_export_concurrency = %s refused: %v", ok, err)
			}
			if got := strconv.Itoa(cfg.Iceberg.IngestExportConcurrency); got != ok {
				t.Errorf("iceberg.ingest_export_concurrency = %s read as %s", ok, got)
			}
		}
		for _, bad := range []string{"0", "-1", "33"} {
			t.Setenv("ARC_ICEBERG_INGEST_EXPORT_CONCURRENCY", bad)
			if _, err = Load(); err == nil || !strings.Contains(err.Error(), "iceberg.ingest_export_concurrency") {
				t.Errorf("iceberg.ingest_export_concurrency = %s: err %v, want it refused by name", bad, err)
			}
		}
		// Off, it is not checked.
		t.Setenv("ARC_ICEBERG_ENABLED", "false")
		if _, err = Load(); err != nil {
			t.Errorf("an out-of-range concurrency failed the load with the export off: %v", err)
		}
	})
	// With the export off the keys are not checked: nothing reads them.
	t.Run("export disabled", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_ICEBERG_INGEST_EXPORT_INTERVAL", "1")
		t.Setenv("ARC_ICEBERG_MIN_VERSION_AGE", "1")
		if _, err := Load(); err != nil {
			t.Fatalf("Load() error = %v", err)
		}
	})
}
