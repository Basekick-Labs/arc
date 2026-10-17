package main

import (
	"errors"
	"io/fs"
	"os"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// prepareWALReserveAtBoot handles the WAL reserve file before anything writes
// to the data volume (#676): released when the volume is full, so the start
// has room. With the WAL or wal.disk_pressure_enabled off nothing keeps the
// reserve, so one left by an earlier start is removed instead of holding
// 64 MiB forever.
func prepareWALReserveAtBoot(cfg *config.Config, logger zerolog.Logger) {
	if !cfg.WAL.Enabled || !cfg.WAL.DiskPressureEnabled {
		err := os.Remove(wal.ReserveFilePath(cfg.WAL.Directory))
		switch {
		case err == nil:
			logger.Info().Msg("Removed the WAL reserve file: the WAL or wal.disk_pressure_enabled is off")
		case !errors.Is(err, fs.ErrNotExist):
			logger.Warn().Err(err).Msg("Cannot remove the WAL reserve file left by an earlier start")
		}
		return
	}
	wal.PrepareReserveAtBoot(cfg.WAL.Directory, logger)
}

// startWALDiskMonitor starts the WAL volume disk pressure monitor (#676), or
// returns nil when wal.disk_pressure_enabled is off: then no gate is
// installed and writes pay nothing for it.
func startWALDiskMonitor(cfg *config.Config, w *wal.Writer) *wal.DiskMonitor {
	if !cfg.WAL.DiskPressureEnabled || w == nil {
		return nil
	}
	m := wal.NewDiskMonitor(w, walDiskMonitorConfig(cfg, logger.Get("wal")))
	m.Start()
	return m
}

func walDiskMonitorConfig(cfg *config.Config, lg zerolog.Logger) wal.DiskMonitorConfig {
	return wal.DiskMonitorConfig{
		Dir:               cfg.WAL.Directory,
		ReserveBytes:      int64(cfg.WAL.DiskMinFreeMB) << 20,
		MaxSizeBytes:      walMaxSizeBytes(cfg),
		MaxDirectoryBytes: int64(cfg.WAL.MaxDirectoryMB) << 20,
		ReserveFile:       true,
		Logger:            lg,
	}
}

// walMaxSizeBytes is wal.max_size_mb in bytes, with the writer's default.
func walMaxSizeBytes(cfg *config.Config) int64 {
	if cfg.WAL.MaxSizeMB <= 0 {
		return 100 << 20
	}
	return int64(cfg.WAL.MaxSizeMB) << 20
}

// walDiskOffPoint is the WAL disk gate's off-point by volume size, for the
// standby store's reserve (#676).
func walDiskOffPoint(cfg *config.Config) func(total uint64) uint64 {
	reserve, maxSize := int64(cfg.WAL.DiskMinFreeMB)<<20, walMaxSizeBytes(cfg)
	return func(total uint64) uint64 {
		_, off, _ := wal.DiskPressureLevels(reserve, maxSize, total)
		return uint64(off)
	}
}
