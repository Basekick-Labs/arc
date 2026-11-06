package config

import (
	"strings"
	"testing"
)

// #1499: the default is "auto": durable with the WAL on, queued otherwise.
func TestLoad_WALAckModeDefaultIsDurableWithTheWALOn(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckMode != "durable" || !cfg.WAL.AckModeAuto {
		t.Errorf("WAL.AckMode = %q auto=%v, want durable from auto", cfg.WAL.AckMode, cfg.WAL.AckModeAuto)
	}
	for _, w := range cfg.Warnings {
		if w.Key == "wal.ack_mode" {
			t.Errorf("unexpected wal.ack_mode warning: %+v", w)
		}
	}
}

func TestLoad_WALAckModeDefaultIsQueuedWithTheWALOff(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.Enabled {
		t.Fatal("the WAL is on by default?")
	}
	if cfg.WAL.AckMode != "queued" || !cfg.WAL.AckModeAuto {
		t.Errorf("WAL.AckMode = %q auto=%v, want queued from auto", cfg.WAL.AckMode, cfg.WAL.AckModeAuto)
	}
	for _, w := range cfg.Warnings {
		if w.Key == "wal.ack_mode" {
			t.Errorf("unexpected wal.ack_mode warning with the WAL off: %+v", w)
		}
	}
}

// Durable is not supported with the covered append, so auto resolves to
// queued where coverage is in effect, says so, and the node starts.
func TestLoad_WALAckModeDefaultIsQueuedWithCoverageAndWarns(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Setenv("ARC_CLUSTER_ENABLED", "true")
	t.Setenv("ARC_CLUSTER_WAL_COVERAGE", "true")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckMode != "queued" || !cfg.WAL.AckModeAuto {
		t.Errorf("WAL.AckMode = %q auto=%v, want queued from auto", cfg.WAL.AckMode, cfg.WAL.AckModeAuto)
	}
	for _, w := range cfg.Warnings {
		if w.Key == "wal.ack_mode" && w.Value == "auto" && strings.Contains(w.Message, "cluster.wal_coverage") {
			return
		}
	}
	t.Fatalf("no wal.ack_mode warning naming cluster.wal_coverage: %+v", cfg.Warnings)
}

// The key alone does not make coverage: a standalone node ignores it
// (enableWALCoverage), so auto stays durable there, and durable is accepted.
func TestLoad_WALAckModeCoverageKeyOnAStandaloneNodeKeepsDurable(t *testing.T) {
	for _, mode := range []string{"", "durable"} {
		t.Run("ack_mode="+mode, func(t *testing.T) {
			t.Setenv("ARC_WAL_ENABLED", "true")
			t.Setenv("ARC_CLUSTER_WAL_COVERAGE", "true")
			if mode != "" {
				t.Setenv("ARC_WAL_ACK_MODE", mode)
			}
			t.Chdir(t.TempDir())
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.WAL.AckMode != "durable" {
				t.Errorf("WAL.AckMode = %q, want durable", cfg.WAL.AckMode)
			}
			for _, w := range cfg.Warnings {
				if w.Key == "wal.ack_mode" {
					t.Errorf("unexpected wal.ack_mode warning: %+v", w)
				}
			}
		})
	}
}

func TestLoad_WALAckModeExplicitAuto(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Setenv("ARC_WAL_ACK_MODE", "auto")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckMode != "durable" || !cfg.WAL.AckModeAuto {
		t.Errorf("WAL.AckMode = %q auto=%v, want durable from auto", cfg.WAL.AckMode, cfg.WAL.AckModeAuto)
	}
}

func TestLoad_WALAckModeExplicitQueued(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Setenv("ARC_WAL_ACK_MODE", "queued")
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckMode != "queued" || cfg.WAL.AckModeAuto {
		t.Errorf("WAL.AckMode = %q auto=%v, want queued, not auto", cfg.WAL.AckMode, cfg.WAL.AckModeAuto)
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
	if cfg.WAL.AckMode != "durable" || cfg.WAL.AckModeAuto {
		t.Errorf("WAL.AckMode = %q auto=%v, want durable, not auto", cfg.WAL.AckMode, cfg.WAL.AckModeAuto)
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
// would stall writers, so the explicit combination is refused at startup.
func TestLoad_WALAckModeDurableRefusesCoverage(t *testing.T) {
	t.Setenv("ARC_WAL_ENABLED", "true")
	t.Setenv("ARC_WAL_ACK_MODE", "durable")
	t.Setenv("ARC_CLUSTER_ENABLED", "true")
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

// #1501: the durable wait is bounded by wal.ack_timeout_seconds, 30 by default; 0 is no bound.
func TestLoad_WALAckTimeoutDefaultAndBounds(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.WAL.AckTimeoutSeconds != 30 {
		t.Errorf("WAL.AckTimeoutSeconds = %d, want 30", cfg.WAL.AckTimeoutSeconds)
	}
	t.Setenv("ARC_WAL_ACK_TIMEOUT_SECONDS", "0")
	if cfg, err = Load(); err != nil || cfg.WAL.AckTimeoutSeconds != 0 {
		t.Fatalf("0 (no bound) rejected: %v, %+v", err, cfg)
	}
	t.Setenv("ARC_WAL_ACK_TIMEOUT_SECONDS", "-5")
	if _, err = Load(); err == nil || !strings.Contains(err.Error(), "wal.ack_timeout_seconds") {
		t.Fatalf("Load() error = %v, want a wal.ack_timeout_seconds error", err)
	}
}
