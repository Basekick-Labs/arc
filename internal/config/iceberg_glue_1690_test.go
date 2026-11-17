package config

import (
	"strings"
	"testing"
)

// #1690 option B: iceberg.glue.* defaults and refusals.
func TestLoad_IcebergGlue(t *testing.T) {
	base := map[string]string{
		"ARC_ICEBERG_ENABLED": "true", "ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
		"ARC_STORAGE_S3_REGION": "us-east-2", "ARC_ICEBERG_GLUE_ENABLED": "true",
	}
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr string
		check   func(t *testing.T, c *Config)
	}{
		{"defaults off", map[string]string{"ARC_ICEBERG_GLUE_ENABLED": "false"}, "", func(t *testing.T, c *Config) {
			if c.Iceberg.Glue.Enabled || c.Iceberg.Glue.SyncInterval != 60 || c.Iceberg.Glue.Region != "" {
				t.Fatalf("defaults %+v", c.Iceberg.Glue)
			}
		}},
		{"enabled on s3", nil, "", func(t *testing.T, c *Config) {
			if !c.Iceberg.Glue.Enabled {
				t.Fatal("not enabled")
			}
		}},
		{"non-default values", map[string]string{"ARC_ICEBERG_GLUE_REGION": "eu-west-1", "ARC_ICEBERG_GLUE_CATALOG_ID": "083710314845", "ARC_ICEBERG_GLUE_SYNC_INTERVAL": "10"}, "", func(t *testing.T, c *Config) {
			g := c.Iceberg.Glue
			if g.Region != "eu-west-1" || g.CatalogID != "083710314845" || g.SyncInterval != 10 {
				t.Fatalf("%+v", g)
			}
		}},
		{"without export", map[string]string{"ARC_ICEBERG_ENABLED": "false"}, "iceberg.glue.enabled", nil},
		{"local storage", map[string]string{"ARC_STORAGE_BACKEND": "local"}, "storage.backend", nil},
		{"non-AWS endpoint", map[string]string{"ARC_STORAGE_S3_ENDPOINT": "localhost:8333"}, "storage.s3_endpoint", nil},
		{"AWS endpoint", map[string]string{"ARC_STORAGE_S3_ENDPOINT": "s3.us-east-2.amazonaws.com"}, "", nil},
		{"bad catalog id", map[string]string{"ARC_ICEBERG_GLUE_CATALOG_ID": "12345"}, "catalog_id", nil},
		{"interval too small", map[string]string{"ARC_ICEBERG_GLUE_SYNC_INTERVAL": "1"}, "sync_interval", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range base {
				t.Setenv(k, v)
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			c, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.check != nil {
				tc.check(t, c)
			}
		})
	}
}
