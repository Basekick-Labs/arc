package main

import (
	"testing"

	"github.com/basekick-labs/arc/internal/config"
)

// coldGCSField hands DuckDB the GCS cold tier's bucket and prefix only when
// this node builds a GCS cold tier (#1105).
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
