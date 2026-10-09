package wal

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Residue accumulates in a directory that is already under a capacity alert:
// a crash between CreateTemp and its deferred remove leaks a write temporary,
// and every purge path removes a *.wal while leaving its attempt sidecar behind
// (PurgeFlushed, and purgeWALFiles via PurgeAll and PurgeInactive). Nothing
// reclaimed either.
func TestSweepRecoveryArtifacts(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(body), 0600))
		return path
	}

	leakedTemp := write(walRecoveryTempPrefix+"987654321", "leaked")
	orphanSidecar := write("gone.wal"+replayAttemptsSuffix, `{"version":1,"attempts":2}`)

	liveWAL := write("live.wal", "data")
	liveSidecar := write("live.wal"+replayAttemptsSuffix, `{"version":1,"attempts":1}`)

	// Quarantine REMOVES the sidecar, so one that survived next to a
	// quarantined file means that removal failed and was already reported. It
	// is the strike history the operator reconciles the quarantined file
	// against, under both quarantine names.
	quarantined := write("poison.wal.failed", "poison")
	quarantinedSidecar := write("poison.wal"+replayAttemptsSuffix, `{"version":1,"attempts":3}`)
	renamedQuarantine := write("second.wal.1700000000.failed", "poison2")
	renamedSidecar := write("second.wal"+replayAttemptsSuffix, `{"version":1,"attempts":3}`)

	NewRecovery(dir, zerolog.Nop()).sweepRecoveryArtifacts()

	for _, path := range []string{leakedTemp, orphanSidecar} {
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err), "%s should have been reclaimed", filepath.Base(path))
	}
	for _, path := range []string{
		liveWAL, liveSidecar,
		quarantined, quarantinedSidecar,
		renamedQuarantine, renamedSidecar,
	} {
		require.FileExists(t, path, "%s must survive the sweep", filepath.Base(path))
	}
}

// A sweep failure must never fail the pass: reclaiming inodes is not worth
// refusing to recover data.
func TestSweepRecoveryArtifactsSurvivesAnUnreadableDirectory(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "absent")
	NewRecovery(missing, zerolog.Nop()).sweepRecoveryArtifacts()
}

// The sweep runs before the file list is taken, so it reclaims residue even
// when there is nothing to replay — and a clean directory must stay silent,
// because this path runs on every maintenance tick.
func TestRecoverySweepsResidueWithNoWALFiles(t *testing.T) {
	dir := t.TempDir()
	leaked := filepath.Join(dir, walRecoveryTempPrefix+"1")
	require.NoError(t, os.WriteFile(leaked, []byte("leaked"), 0600))

	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{})
	require.NoError(t, err)
	require.Zero(t, stats.RecoveredFiles)
	_, statErr := os.Stat(leaked)
	require.True(t, os.IsNotExist(statErr), "residue must be reclaimed even with no WAL files to replay")
}

func TestQuarantinedWALBase(t *testing.T) {
	for _, tc := range []struct {
		name string
		base string
		ok   bool
	}{
		{name: "arc-1.wal.failed", base: "arc-1.wal", ok: true},
		{name: "arc-1.wal.1700000000.failed", base: "arc-1.wal", ok: true},
		{name: "arc-1.wal", ok: false},
		{name: "arc-1.wal" + replayAttemptsSuffix, ok: false},
		// A name an operator left behind, not something quarantine produced.
		{name: "not-wal.failed.backup", ok: false},
		{name: "notes.failed", ok: false},
	} {
		base, ok := quarantinedWALBase(tc.name)
		require.Equal(t, tc.ok, ok, tc.name)
		require.Equal(t, tc.base, base, tc.name)
	}
}

// Absence must be os.IsNotExist, never a bare "stat failed". On a degraded
// volume — the state the capacity alert exists for — EACCES or EIO would
// otherwise wipe a LIVE poison file's strikes and reset its quarantine clock to
// zero on every pass, the inverse of what quarantine is for.
func TestSweepKeepsASidecarWhenTheWALFileCannotBeStatted(t *testing.T) {
	dir := t.TempDir()
	sidecar := filepath.Join(dir, "poison.wal"+replayAttemptsSuffix)
	require.NoError(t, os.WriteFile(sidecar, []byte(`{"version":1,"attempts":2}`), 0600))

	original := statWALFileFn
	statWALFileFn = func(string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: "poison.wal", Err: syscall.EACCES}
	}
	t.Cleanup(func() { statWALFileFn = original })

	NewRecovery(dir, zerolog.Nop()).sweepRecoveryArtifacts()
	require.FileExists(t, sidecar,
		"a sidecar whose WAL file could not be statted must be kept; deleting it resets a live poison file's quarantine clock")
}
