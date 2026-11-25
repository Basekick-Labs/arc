package config

import (
	"strings"
	"testing"
)

// #1759 (b): ingest.wait_visible_min_interval_ms defaults to 5000, is read,
// is bounded to [1000, 3600000] whether or not the feature is on, and warns
// above ingest.max_buffer_age_ms with the feature on.
func TestLoad_WaitVisibleMinInterval1759(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		want    int
		err     bool
		warnKey bool // the interval warning
	}{
		{name: "default", want: 5000},
		{name: "floor", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "1000"}, want: 1000},
		{name: "ceiling", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "3600000"}, want: 3600000},
		{name: "below the floor", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "999"}, err: true},
		{name: "zero", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "0"}, err: true},
		{name: "above the ceiling", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "3600001"}, err: true},
		{name: "off: bounded too", env: map[string]string{"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "-5"}, err: true},
		{name: "on, interval above the age", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true", "ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "6000",
		}, want: 6000, warnKey: true},
		{name: "on, interval at the age", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_ENABLED": "true", "ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "5000",
		}, want: 5000},
		{name: "off, interval above the age: no warning", env: map[string]string{
			"ARC_INGEST_WAIT_VISIBLE_MIN_INTERVAL_MS": "6000",
		}, want: 6000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.err {
				if err == nil || !strings.Contains(err.Error(), "ingest.wait_visible_min_interval_ms") {
					t.Fatalf("Load error = %v, want one naming ingest.wait_visible_min_interval_ms", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Ingest.WaitVisibleMinIntervalMS != tc.want {
				t.Fatalf("WaitVisibleMinIntervalMS = %d, want %d", cfg.Ingest.WaitVisibleMinIntervalMS, tc.want)
			}
			got := false
			for _, w := range cfg.Warnings {
				if w.Key == "ingest.wait_visible_min_interval_ms" {
					got = true
				}
			}
			if got != tc.warnKey {
				t.Fatalf("interval warning = %v, want %v (all: %+v)", got, tc.warnKey, cfg.Warnings)
			}
		})
	}
}

// The age warning no longer says a visible write always waits for the age:
// it names the early flush it falls back from.
func TestWaitVisibleAgeWarningNamesTheEarlyFlush1759(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_INGEST_WAIT_VISIBLE_ENABLED", "true")
	t.Setenv("ARC_INGEST_MAX_BUFFER_AGE_MS", "15000")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range cfg.Warnings {
		if w.Key == "ingest.max_buffer_age_ms" && strings.Contains(w.Message, "wait_visible") {
			if !strings.Contains(w.Message, "cannot flush its measurement early") {
				t.Fatalf("age warning does not name the early flush: %s", w.Message)
			}
			return
		}
	}
	t.Fatalf("no age warning: %+v", cfg.Warnings)
}
