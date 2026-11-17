package config

import (
	"strings"
	"testing"
)

// #1690: iceberg.rest_catalog_enabled defaults to false and needs the export.
func TestLoad_IcebergRESTCatalog(t *testing.T) {
	for _, tc := range []struct {
		name, enabled, catalog string
		want, wantErr          bool
	}{
		{"default off", "true", "", false, false},
		{"on with export", "true", "true", true, false},
		{"on without export", "false", "true", false, true},
		{"off without export", "false", "false", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_ICEBERG_ENABLED", tc.enabled)
			t.Setenv("ARC_STORAGE_BACKEND", "local")
			if tc.catalog != "" {
				t.Setenv("ARC_ICEBERG_REST_CATALOG_ENABLED", tc.catalog)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "iceberg.rest_catalog_enabled") {
					t.Fatalf("err = %v, want a refusal naming iceberg.rest_catalog_enabled", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Iceberg.RESTCatalogEnabled != tc.want {
				t.Fatalf("RESTCatalogEnabled = %v, want %v", cfg.Iceberg.RESTCatalogEnabled, tc.want)
			}
		})
	}
}
