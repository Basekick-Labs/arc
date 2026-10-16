package main

import (
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
)

// Every cluster node gets a completion directory, so a watcher keeps the
// cluster manifest in step with compaction on shared storage too, where
// replication is off (#1207).
func TestCompletionDirFor(t *testing.T) {
	cfg := &config.Config{}
	cfg.Compaction.TempDirectory = "/tmp/arc-compaction"
	if got := completionDirFor(cfg); got != "" {
		t.Fatalf("outside a cluster: got %q, want none", got)
	}
	cfg.Cluster.Enabled = true
	cfg.Cluster.ReplicationEnabled = false
	cfg.Cluster.SharedStorageMode = true
	want := filepath.Join("/tmp/arc-compaction", ".completion", "pending")
	if got := completionDirFor(cfg); got != want {
		t.Fatalf("shared storage, replication off: got %q, want %q", got, want)
	}
	cfg.Cluster.SharedStorageMode = false
	if got := completionDirFor(cfg); got != want {
		t.Fatalf("single writer on a shared bucket: got %q, want %q", got, want)
	}
	cfg.Compaction.CompletionDir = "/elsewhere"
	if got := completionDirFor(cfg); got != "/elsewhere" {
		t.Fatalf("explicit completion_dir: got %q", got)
	}
}
