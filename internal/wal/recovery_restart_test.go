package wal

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func testRecoveryFailureAcrossRestarts(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = w.Close()
		}
	})
	_, err = w.AppendTracked(rows)
	require.NoError(t, err)
	poison := w.CurrentFile()
	require.NoError(t, w.Rotate())
	_, err = w.AppendTracked([]map[string]interface{}{{"_measurement": "healthy", "value": 1}})
	require.NoError(t, err)
	healthy := w.CurrentFile()
	require.NoError(t, w.Close())
	closed = true
	original, err := os.ReadFile(poison)
	require.NoError(t, err)
	for attempt := 1; attempt <= 3; attempt++ {
		healthyRows := 0
		stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
			func(_ context.Context, records []map[string]interface{}) error {
				if records[0]["_measurement"] != "healthy" {
					return errors.New("permanent replay rejection")
				}
				healthyRows += len(records)
				return nil
			})
		require.NoError(t, err)
		if attempt < 3 {
			require.Zero(t, stats.QuarantinedFiles)
			require.Zero(t, healthyRows)
			require.FileExists(t, poison)
		} else {
			require.Equal(t, 1, stats.QuarantinedFiles, "third failed pass across restarts must quarantine")
			require.Equal(t, 1, healthyRows, "quarantine must unblock the next WAL file")
			require.NoFileExists(t, poison)
			require.NoFileExists(t, healthy)
			preserved, err := os.ReadFile(poison + ".failed")
			require.NoError(t, err)
			require.Equal(t, original, preserved, "quarantine must preserve the complete WAL bytes")
			require.NoFileExists(t, poison+".recovery")
		}
	}
}

func TestRecoveryAttemptMetadataFailureKeepsWAL(t *testing.T) {
	for _, kind := range []string{"invalid_json", "unknown_version", "directory"} {
		t.Run(kind, func(t *testing.T) {
			dir, files := writeRecoveryFiles(t, 1)
			path := files[0] + ".recovery"
			if kind == "directory" {
				require.NoError(t, os.Mkdir(path, 0700))
			} else {
				data := []byte("{")
				if kind == "unknown_version" {
					data = []byte(`{"version":2,"attempts":3}`)
				}
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			original, err := os.ReadFile(files[0])
			require.NoError(t, err)
			for attempt := 0; attempt < 4; attempt++ {
				stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
					func(context.Context, []map[string]interface{}) error { return errors.New("poison") })
				require.Error(t, err, "failed attempt accounting must not be silently discarded")
				require.Zero(t, stats.QuarantinedFiles)
				current, err := os.ReadFile(files[0])
				require.NoError(t, err)
				require.Equal(t, original, current)
			}
		})
	}
}

func TestRecoverySuccessfulReplayClearsPersistedFailure(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	_, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("poison") })
	require.NoError(t, err)
	require.FileExists(t, files[0]+".recovery")
	stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil })
	require.NoError(t, err)
	require.Equal(t, 1, stats.RecoveredFiles)
	require.NoFileExists(t, files[0]+".recovery")
}

func TestRecoveryAttemptErrorKeepsPendingBarrierFiles(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 2)
	require.NoError(t, os.WriteFile(files[1]+".recovery", []byte("{"), 0600))
	calls := 0
	barriers := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error {
			calls++
			if calls == 2 {
				return errors.New("poison")
			}
			return nil
		}, &RecoveryOptions{BeforeDelete: func(context.Context) error { barriers++; return nil }})
	require.Error(t, err)
	require.Equal(t, 2, stats.KeptFiles)
	require.Zero(t, barriers)
	for _, path := range files {
		require.FileExists(t, path)
	}
}

func TestRecoveryCompletesQuarantineAfterPersistedThreshold(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	// Model a crash after syncing attempt 3 but before renaming the WAL.
	require.NoError(t, writeReplayAttempts(files[0], 3))
	stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("poison") })
	require.NoError(t, err)
	require.Equal(t, 1, stats.QuarantinedFiles)
	require.FileExists(t, files[0]+".failed")
}

func TestRecoveryRepairedFileDoesNotInheritAttempts(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	require.NoError(t, writeReplayAttempts(files[0], 2))
	info, err := os.Stat(files[0])
	require.NoError(t, err)
	// An operator replacement with different metadata starts a new series.
	require.NoError(t, os.Chtimes(files[0], info.ModTime().Add(-time.Hour), info.ModTime().Add(-time.Hour)))
	stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("poison") })
	require.NoError(t, err)
	require.Zero(t, stats.QuarantinedFiles)
	require.FileExists(t, files[0])
}

func TestRecoveryQuarantineSurvivesRestart(t *testing.T) {
	testRecoveryFailureAcrossRestarts(t, []map[string]interface{}{{"_measurement": "poison", "value": 1}})
}

func TestRecoveryQuarantineAcrossProcessRestarts(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	for attempt := 1; attempt <= 3; attempt++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRecoveryRestartProcessHelper$")
		cmd.Env = append(os.Environ(), "ARC_WAL_RESTART_TEST_DIR="+dir)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "child replay %d: %s", attempt, output)
		if attempt < 3 {
			require.FileExists(t, files[0])
			require.NoFileExists(t, files[0]+".failed")
		} else {
			require.NoFileExists(t, files[0])
			require.FileExists(t, files[0]+".failed")
		}
	}
}

func TestRecoveryRestartProcessHelper(t *testing.T) {
	dir := os.Getenv("ARC_WAL_RESTART_TEST_DIR")
	if dir == "" {
		return
	}
	_, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("poison") })
	require.NoError(t, err)
}

func TestRecoveryCancellationDoesNotAdvanceQuarantine(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	for attempt := 0; attempt < 4; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		stats, err := NewRecovery(dir, zerolog.Nop()).Recover(ctx,
			func(context.Context, []map[string]interface{}) error { cancel(); return ctx.Err() })
		cancel()
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, stats.QuarantinedFiles)
		require.FileExists(t, files[0])
	}
	stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("first real failure") })
	require.NoError(t, err)
	require.Zero(t, stats.QuarantinedFiles)
	require.FileExists(t, files[0])
}

func TestRecoveryBarrierOutageDoesNotAdvanceQuarantine(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	for attempt := 0; attempt < 4; attempt++ {
		stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
			func(context.Context, []map[string]interface{}) error { return nil }, &RecoveryOptions{
				BeforeDelete: func(context.Context) error { return errors.New("storage unavailable") },
			})
		require.Error(t, err)
		require.Equal(t, 1, stats.BarrierFailures)
		require.Zero(t, stats.QuarantinedFiles)
		require.FileExists(t, files[0])
	}
	stats, err := NewRecovery(dir, zerolog.Nop()).Recover(context.Background(),
		func(context.Context, []map[string]interface{}) error { return errors.New("first real failure") })
	require.NoError(t, err)
	require.Zero(t, stats.QuarantinedFiles)
	require.FileExists(t, files[0])
}
