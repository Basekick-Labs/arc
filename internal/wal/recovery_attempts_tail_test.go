package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Size and mtime cannot discriminate a file repaired IN PLACE to the same
// length within one filesystem timestamp tick — 1 s granularity on some
// network mounts, which is where repairs happen. Such a file inherited its
// strikes and could be quarantined after the operator had already fixed it.
func TestReplayAttemptsTailDiscriminatesASameSizeSameMtimeRepair(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "arc-1.wal")
	require.NoError(t, os.WriteFile(path, []byte("AAAABBBBCCCC"), 0600))

	require.NoError(t, writeReplayAttempts(path, 2))
	attempts, err := readReplayAttempts(path)
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "an untouched file keeps its strikes")

	info, err := os.Stat(path)
	require.NoError(t, err)

	// A repair that rewrites the tail to the same LENGTH, then restores the
	// original timestamp exactly — the indistinguishable case.
	require.NoError(t, os.WriteFile(path, []byte("AAAABBBBDDDD"), 0600))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	repaired, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, info.Size(), repaired.Size())
	require.Equal(t, info.ModTime().UnixNano(), repaired.ModTime().UnixNano())

	attempts, err = readReplayAttempts(path)
	require.NoError(t, err)
	require.Zero(t, attempts, "a repaired file must start a new attempt series")
}

// A sidecar from a binary that predates the tail fields must read unchanged and
// keep that binary's size+mtime behaviour. Version stays 1 on purpose: bumping
// it would make the OLDER binary reject the sidecar, and a rejection aborts the
// whole recovery pass and leaves every later file unread after a rollback.
func TestReplayAttemptsWithoutTailFieldsStillReads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "arc-1.wal")
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0600))
	info, err := os.Stat(path)
	require.NoError(t, err)

	legacy, err := json.Marshal(replayAttempts{
		Version: 1, Attempts: 2, Size: info.Size(), Modified: info.ModTime().UnixNano(),
	})
	require.NoError(t, err)
	require.NotContains(t, string(legacy), "tail_crc32",
		"the tail fields must be omitempty, or an older binary sees changed JSON")
	require.NoError(t, os.WriteFile(path+replayAttemptsSuffix, legacy, 0600))

	attempts, err := readReplayAttempts(path)
	require.NoError(t, err)
	require.Equal(t, 2, attempts, "a sidecar with no tail discriminator keeps the size+mtime verdict")
}

// What this binary writes must still satisfy the version gate an older binary
// applies, or a rollback stalls recovery permanently.
func TestReplayAttemptsStaysAtVersionOne(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "arc-1.wal")
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0600))
	require.NoError(t, writeReplayAttempts(path, 1))

	raw, err := os.ReadFile(path + replayAttemptsSuffix)
	require.NoError(t, err)
	var state replayAttempts
	require.NoError(t, json.Unmarshal(raw, &state))
	require.Equal(t, 1, state.Version,
		"a version an older binary rejects aborts its whole recovery pass, which is the defect M3 removes")
	require.True(t, state.HasTailCRC)
}

// A size change alone still resets, so the tail check is additive rather than a
// replacement for the cheap comparison.
func TestReplayAttemptsSizeChangeStillResets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "arc-1.wal")
	require.NoError(t, os.WriteFile(path, []byte("AAAABBBB"), 0600))
	require.NoError(t, writeReplayAttempts(path, 3))
	require.NoError(t, os.WriteFile(path, []byte("AAAABBBBCC"), 0600))

	attempts, err := readReplayAttempts(path)
	require.NoError(t, err)
	require.Zero(t, attempts)
}

// An empty file has no tail to hash, and a tail that cannot be read is "no
// discriminator available" — both fall through to size+mtime rather than
// becoming a second way to abort a pass.
func TestReplayAttemptsEmptyFileHasNoTailDiscriminator(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "arc-1.wal")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	require.NoError(t, writeReplayAttempts(path, 2))

	raw, err := os.ReadFile(path + replayAttemptsSuffix)
	require.NoError(t, err)
	var state replayAttempts
	require.NoError(t, json.Unmarshal(raw, &state))
	require.False(t, state.HasTailCRC)

	attempts, err := readReplayAttempts(path)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

// The fingerprint reads a bounded window, not the whole file.
func TestWALFileFingerprintWindowIsBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "arc-1.wal")
	body := make([]byte, replayTailWindow+4096)
	for i := range body {
		body[i] = byte(i)
	}
	require.NoError(t, os.WriteFile(path, body, 0600))

	size, modified, tailCRC, tailOK, err := walFileFingerprint(path)
	require.NoError(t, err)
	require.True(t, tailOK)
	require.Equal(t, int64(len(body)), size)
	require.NotZero(t, modified)

	// A change outside the final window is invisible, which is the documented
	// residual: a repair confined to the middle of a large file still inherits
	// its strikes, and the escape is removing the sidecar.
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xFF}, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, afterCRC, _, err := walFileFingerprint(path)
	require.NoError(t, err)
	require.Equal(t, tailCRC, afterCRC)
}
