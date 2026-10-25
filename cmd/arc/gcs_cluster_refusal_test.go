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
