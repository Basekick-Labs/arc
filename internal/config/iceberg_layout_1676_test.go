package config

import (
	"strings"
	"testing"
)

// #1676: iceberg.layout is "warehouse" (default) or "colocated", in any case;
// anything else is refused at load, whether or not the export is on.
func TestLoad_IcebergLayout(t *testing.T) {
	for _, tc := range []struct {
		value, want string
		enabled     string
		wantErr     bool
	}{
		{"", IcebergLayoutWarehouse, "true", false},
		{"warehouse", IcebergLayoutWarehouse, "true", false},
		{"colocated", IcebergLayoutColocated, "true", false},
		{" Colocated ", IcebergLayoutColocated, "true", false},
		{"co-located", "", "true", true},
		{"colocated-ish", "", "false", true},
	} {
		t.Run(tc.value+"/"+tc.enabled, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_ICEBERG_ENABLED", tc.enabled)
			t.Setenv("ARC_STORAGE_BACKEND", "local")
			if tc.value != "" {
				t.Setenv("ARC_ICEBERG_LAYOUT", tc.value)
			}
			cfg, err := Load()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "iceberg.layout") {
					t.Fatalf("layout %q: err = %v, want a refusal naming iceberg.layout", tc.value, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("layout %q: %v", tc.value, err)
			}
			if cfg.Iceberg.Layout != tc.want {
				t.Fatalf("layout %q loaded as %q, want %q", tc.value, cfg.Iceberg.Layout, tc.want)
			}
		})
	}
}
