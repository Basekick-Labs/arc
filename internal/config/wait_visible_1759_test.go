package config

import (
	"strings"
	"testing"
)

// #1759: ingest.wait_visible_enabled defaults to false, is read, and warns on
// a cluster and with a buffer age above half the 20 s wait budget.
func TestLoad_WaitVisible1759(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		want     bool
		warnKeys []string // the wait-visible warnings, in order
	}{
		{name: "default off", want: false},
		{name: "off on a cluster with a long age: no warning", env: map[string]string{
			"ARC_CLUSTER_ENABLED": "true", "ARC_INGEST_MAX_BUFFER_AGE_MS": "30000",
		}},
		{name: "on", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true"}, want: true},
		{name: "on, age at the bound", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true", "ARC_INGEST_MAX_BUFFER_AGE_MS": "10000",
		}, want: true},
		{name: "on, age above the bound", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true", "ARC_INGEST_MAX_BUFFER_AGE_MS": "10001",
		}, want: true, warnKeys: []string{"ingest.max_buffer_age_ms"}},
		{name: "on a cluster", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true", "ARC_CLUSTER_ENABLED": "true",
		}, want: true, warnKeys: []string{"ingest.wait_visible_enabled"}},
		{name: "on object storage", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true", "ARC_STORAGE_BACKEND": "s3", "ARC_STORAGE_S3_BUCKET": "b",
		}, want: true, warnKeys: []string{"storage.backend"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Ingest.WaitVisibleEnabled != tc.want {
				t.Fatalf("WaitVisibleEnabled = %v, want %v", cfg.Ingest.WaitVisibleEnabled, tc.want)
			}
			var got []string
			for _, w := range cfg.Warnings {
				if strings.Contains(w.Message, "wait_visible") {
					got = append(got, w.Key)
				}
			}
			if strings.Join(got, ",") != strings.Join(tc.warnKeys, ",") {
				t.Fatalf("wait-visible warnings = %v, want %v (all: %+v)", got, tc.warnKeys, cfg.Warnings)
			}
		})
	}
}
