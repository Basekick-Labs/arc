package config

import (
	"strings"
	"testing"
)

// #1760: query.fresh_flush_* default to false / 5000 / 64, are read, are
// bounded whether or not the feature is on, and warn only with it on.
func TestLoad_FreshFlush1760(t *testing.T) {
	on := map[string]string{"ARC_QUERY_FRESH_FLUSH_ENABLED": "true"}
	with := func(kv ...string) map[string]string {
		m := map[string]string{}
		for k, v := range on {
			m[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}
	cases := []struct {
		name              string
		env               map[string]string
		enabled           bool
		interval, waiters int
		errKey            string
		warn              []string // Key:substring of each warning about the feature
	}{
		{name: "default", interval: 5000, waiters: 64},
		{name: "on, defaults: no warning", env: on, enabled: true, interval: 5000, waiters: 64},
		{name: "non-default values", env: with("ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS", "1500", "ARC_QUERY_FRESH_FLUSH_MAX_WAITERS", "1"), enabled: true, interval: 1500, waiters: 1},
		{name: "interval floor", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "1000"}, interval: 1000, waiters: 64},
		{name: "interval ceiling", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "3600000"}, interval: 3600000, waiters: 64},
		{name: "interval 999, off: refused too", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "999"}, errKey: "query.fresh_flush_min_interval_ms"},
		{name: "interval 0", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "0"}, errKey: "query.fresh_flush_min_interval_ms"},
		{name: "interval -1", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "-1"}, errKey: "query.fresh_flush_min_interval_ms"},
		{name: "interval above the ceiling", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "3600001"}, errKey: "query.fresh_flush_min_interval_ms"},
		{name: "waiters 1024", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MAX_WAITERS": "1024"}, interval: 5000, waiters: 1024},
		{name: "waiters 0", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MAX_WAITERS": "0"}, errKey: "query.fresh_flush_max_waiters"},
		{name: "waiters 1025", env: map[string]string{"ARC_QUERY_FRESH_FLUSH_MAX_WAITERS": "1025"}, errKey: "query.fresh_flush_max_waiters"},
		{name: "on, cluster", env: with("ARC_CLUSTER_ENABLED", "true"), enabled: true, interval: 5000, waiters: 64,
			warn: []string{"query.fresh_flush_enabled:cluster.enabled"}},
		{name: "on, s3", env: with("ARC_STORAGE_BACKEND", "s3", "ARC_STORAGE_S3_BUCKET", "b"), enabled: true, interval: 5000, waiters: 64,
			warn: []string{"storage.backend:object storage"}},
		{name: "on, tiering", env: with("ARC_TIERED_STORAGE_ENABLED", "true"), enabled: true, interval: 5000, waiters: 64,
			warn: []string{"tiered_storage.enabled:tiered storage"}},
		{name: "on, age 15000: no warning", env: with("ARC_INGEST_MAX_BUFFER_AGE_MS", "15000", "ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS", "15000"), enabled: true, interval: 15000, waiters: 64},
		{name: "on, age 15001", env: with("ARC_INGEST_MAX_BUFFER_AGE_MS", "15001", "ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS", "15001"), enabled: true, interval: 15001, waiters: 64,
			warn: []string{"ingest.max_buffer_age_ms:above 15000"}},
		{name: "on, age at query.timeout", env: with("ARC_QUERY_TIMEOUT", "5", "ARC_INGEST_MAX_BUFFER_AGE_MS", "5000"), enabled: true, interval: 5000, waiters: 64,
			warn: []string{"ingest.max_buffer_age_ms:query.timeout"}},
		{name: "on, interval above the age", env: with("ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS", "5001"), enabled: true, interval: 5001, waiters: 64,
			warn: []string{"query.fresh_flush_min_interval_ms:above ingest.max_buffer_age_ms"}},
		{name: "on, age 0: no interval warning", env: with("ARC_INGEST_MAX_BUFFER_AGE_MS", "0"), enabled: true, interval: 5000, waiters: 64},
		{name: "off, everything that would warn", env: map[string]string{"ARC_CLUSTER_ENABLED": "true", "ARC_INGEST_MAX_BUFFER_AGE_MS": "30000",
			"ARC_QUERY_FRESH_FLUSH_MIN_INTERVAL_MS": "40000", "ARC_TIERED_STORAGE_ENABLED": "true"}, interval: 40000, waiters: 64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := Load()
			if tc.errKey != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errKey) {
					t.Fatalf("Load error = %v, want one naming %s", err, tc.errKey)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			q := cfg.Query
			if q.FreshFlushEnabled != tc.enabled || q.FreshFlushMinIntervalMS != tc.interval || q.FreshFlushMaxWaiters != tc.waiters {
				t.Fatalf("got enabled %v interval %d waiters %d, want %v %d %d", q.FreshFlushEnabled, q.FreshFlushMinIntervalMS, q.FreshFlushMaxWaiters, tc.enabled, tc.interval, tc.waiters)
			}
			var got []string
			for _, w := range cfg.Warnings {
				if strings.Contains(w.Message, "fresh_flush") || strings.Contains(w.Message, "x-arc-freshness") {
					got = append(got, w.Key+": "+w.Message)
				}
			}
			if len(got) != len(tc.warn) {
				t.Fatalf("fresh warnings %q, want %q", got, tc.warn)
			}
			for i, want := range tc.warn {
				key, sub, _ := strings.Cut(want, ":")
				if !strings.HasPrefix(got[i], key+": ") || !strings.Contains(got[i], sub) {
					t.Fatalf("warning %d = %q, want key %s containing %q", i, got[i], key, sub)
				}
			}
		})
	}
}
