package main

import (
	"testing"

	"github.com/basekick-labs/arc/internal/config"
)

// TestGCSClusterModeRefusal (#1105): a GCS primary is refused only on a node
// that will really run as a cluster node. cluster.enabled without a
// clustering licence runs standalone, which GCS supports, and other backends
// are untouched.
func TestGCSClusterModeRefusal(t *testing.T) {
	for _, c := range []struct {
		backend    string
		enabled    bool
		clustering bool
		refuse     bool
	}{
		{"gcs", true, true, true},
		{"gcs", true, false, false},
		{"gcs", false, true, false},
		{"gcs", false, false, false},
		{"s3", true, true, false},
		{"local", true, true, false},
	} {
		cfg := &config.Config{}
		cfg.Storage.Backend = c.backend
		cfg.Cluster.Enabled = c.enabled
		if err := gcsClusterModeRefusal(cfg, c.clustering); (err != nil) != c.refuse {
			t.Errorf("backend=%s cluster.enabled=%v clustering=%v: err %v, want refused=%v",
				c.backend, c.enabled, c.clustering, err, c.refuse)
		}
	}
}

// #1105 part 3: a GCS cold tier is refused on a licensed cluster node (gated
// like the cold tier: tiering and cold both on), and a GCS backup target is
// not.
func TestGCSColdTierClusterModeRefusal(t *testing.T) {
	for _, c := range []struct {
		name                string
		tiering, cold       bool
		coldBackend         string
		enabled, clustering bool
		refuse              bool
	}{
		{"gcs cold on a licensed cluster", true, true, "gcs", true, true, true},
		{"gcs cold, tiering off", false, true, "gcs", true, true, false},
		{"gcs cold, cold off", true, false, "gcs", true, true, false},
		{"gcs cold, unlicensed cluster", true, true, "gcs", true, false, false},
		{"gcs cold standalone", true, true, "gcs", false, false, false},
		{"s3 cold on a licensed cluster", true, true, "s3", true, true, false},
	} {
		cfg := &config.Config{}
		cfg.Storage.Backend = "s3"
		cfg.TieredStorage.Enabled, cfg.TieredStorage.Cold.Enabled = c.tiering, c.cold
		cfg.TieredStorage.Cold.Backend = c.coldBackend
		cfg.Cluster.Enabled = c.enabled
		if err := gcsClusterModeRefusal(cfg, c.clustering); (err != nil) != c.refuse {
			t.Errorf("%s: err %v, want refused=%v", c.name, err, c.refuse)
		}
	}
	cfg := &config.Config{}
	cfg.Storage.Backend = "local"
	cfg.Cluster.Enabled = true
	cfg.Backup.Enabled = true
	cfg.Backup.Targets = map[string]config.BackupTargetConfig{"audit": {Name: "audit", Type: "gcs", GCSBucket: "b"}}
	if err := gcsClusterModeRefusal(cfg, true); err != nil {
		t.Errorf("a GCS backup target on a cluster must be allowed: %v", err)
	}
}

func TestColdGCSFieldOnlyForAGCSColdTier(t *testing.T) {
	cfg := &config.Config{}
	cfg.TieredStorage.Enabled, cfg.TieredStorage.Cold.Enabled = true, true
	cfg.TieredStorage.Cold.Backend = "s3"
	if coldGCSField(cfg, "left-over") != "" {
		t.Fatal("a leftover gcs_bucket with an s3 cold tier must not reach DuckDB")
	}
	cfg.TieredStorage.Cold.Backend = "gcs"
	if coldGCSField(cfg, "b") != "b" {
		t.Fatal("a gcs cold tier's bucket must reach DuckDB")
	}
	cfg.TieredStorage.Cold.Enabled = false
	if coldGCSField(cfg, "b") != "" {
		t.Fatal("a disabled gcs cold tier must not reach DuckDB")
	}
	cfg.TieredStorage.Enabled, cfg.TieredStorage.Cold.Enabled = false, true
	if coldGCSField(cfg, "b") != "" {
		t.Fatal("with tiering off, a gcs cold block must not reach DuckDB")
	}
}
