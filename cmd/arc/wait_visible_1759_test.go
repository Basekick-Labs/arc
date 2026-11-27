package main

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/api"
	"github.com/basekick-labs/arc/internal/config"
)

// #1759: the wait-visible configuration comes from cfg alone, and both write
// handlers share the one instance, so the waiter limit is the node's.
func TestWaitVisibleWiring1759(t *testing.T) {
	cfg := &config.Config{}
	cfg.Ingest.WaitVisibleEnabled = true
	cfg.Cluster.Enabled = true
	cfg.Storage.Backend = "s3"
	cfg.Query.FileTimePruning = true
	cfg.Query.FileTimePruningMarginSeconds = 7
	want := api.WaitVisibleConfig{
		Enabled: true, Clustered: true, StorageBackend: "s3",
		FileTimePruning: true, FileTimePruningMargin: 7 * time.Second,
	}
	if got := waitVisibleConfig(cfg); got != want {
		t.Fatalf("waitVisibleConfig = %+v, want %+v", got, want)
	}

	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"waitVisible := api.NewWaitVisible(waitVisibleConfig(cfg))",
		"msgpackHandler.SetWaitVisible(waitVisible)",
		"lineProtocolHandler.SetWaitVisible(waitVisible)",
	} {
		if !strings.Contains(string(src), line) {
			t.Errorf("main.go no longer has %q", line)
		}
	}
}
