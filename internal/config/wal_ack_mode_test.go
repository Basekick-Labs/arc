package config

import (
	"strings"
	"testing"
)

func TestLoad_WALAckModeDefaultsToQueued(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckMode != "queued" {
		t.Errorf("WAL.AckMode = %q, want queued", cfg.WAL.AckMode)
	}
}

func TestLoad_WALAckModeDurable(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Setenv("ARC_WAL_ACK_MODE", "durable")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckMode != "durable" {
		t.Errorf("WAL.AckMode = %q, want durable", cfg.WAL.AckMode)
	}
}

func TestLoad_WALAckModeRejectsUnknown(t *testing.T) {
	t.Setenv("ARC_WAL_ACK_MODE", "fast")
	t.Chdir(t.TempDir())
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "wal.ack_mode") {
		t.Fatalf("Load() error = %v, want a wal.ack_mode error", err)
	}
}

// The covered append commits under the ingest shard lock; durable waits there
// would stall writers, so the combination is refused at startup.
func TestLoad_WALAckModeDurableRefusesCoverage(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Setenv("ARC_WAL_ACK_MODE", "durable")
	t.Setenv("ARC_CLUSTER_WAL_COVERAGE", "true")
	t.Chdir(t.TempDir())
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "cluster.wal_coverage") {
		t.Fatalf("Load() error = %v, want the durable + wal_coverage refusal", err)
	}
}

func TestLoad_WALAckModeDurableWithoutWALWarns(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "false")
	t.Setenv("ARC_WAL_ACK_MODE", "durable")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	for _, w := range cfg.Warnings {
		if w.Key == "wal.ack_mode" {
			return
		}
	}
	t.Fatalf("no wal.ack_mode warning with the WAL disabled: %+v", cfg.Warnings)
}
