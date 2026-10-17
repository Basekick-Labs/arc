package main

import (
	"errors"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// With wal.disk_pressure_enabled off no gate is installed: the default write
// path pays nothing for it.
func TestStartWALDiskMonitorOffSwitch(t *testing.T) {
	cfg := &config.Config{}
	cfg.WAL.Directory = t.TempDir()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: cfg.WAL.Directory, SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if m := startWALDiskMonitor(cfg, w); m != nil {
		t.Fatal("a monitor started with wal.disk_pressure_enabled off")
	}
	cfg.WAL.DiskPressureEnabled = true
	m := startWALDiskMonitor(cfg, w)
	if m == nil {
		t.Fatal("no monitor with the gate on")
	}
	defer m.Close()
	if reserve, off := m.Levels(); reserve <= 0 || off <= reserve {
		t.Fatalf("levels (%d, %d)", reserve, off)
	}
}

// Turning the feature off removes a reserve file an earlier start left, which
// nothing would maintain any more.
func TestPrepareWALReserveAtBootOffRemovesTheReserve(t *testing.T) {
	cfg := &config.Config{}
	cfg.WAL.Directory = t.TempDir()
	path := wal.ReserveFilePath(cfg.WAL.Directory)
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.WAL.Enabled, cfg.WAL.DiskPressureEnabled = true, true
	prepareWALReserveAtBoot(cfg, zerolog.Nop())
	if _, err := os.Stat(path); err != nil {
		t.Fatal("the reserve was removed with the feature on and free space")
	}
	cfg.WAL.DiskPressureEnabled = false
	prepareWALReserveAtBoot(cfg, zerolog.Nop())
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the reserve was kept with the feature off")
	}
}

// The WAL turned off later: nothing maintains the reserve any more.
func TestPrepareWALReserveAtBootWALOffRemovesTheReserve(t *testing.T) {
	cfg := &config.Config{}
	cfg.WAL.Directory = t.TempDir()
	cfg.WAL.DiskPressureEnabled = true // the default; the WAL itself is off
	path := wal.ReserveFilePath(cfg.WAL.Directory)
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	prepareWALReserveAtBoot(cfg, zerolog.Nop())
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the reserve was kept with the WAL off")
	}
}

func TestWALDiskOffPointFollowsTheConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.WAL.MaxSizeMB = 100
	if got := walDiskOffPoint(cfg)(100 << 30); got != 712<<20 {
		t.Fatalf("default off-point %d, want 712 MiB", got)
	}
	cfg.WAL.DiskMinFreeMB = 4096
	if got := walDiskOffPoint(cfg)(100 << 30); got != (4096+200)<<20 {
		t.Fatalf("off-point %d, want 4296 MiB", got)
	}
}
