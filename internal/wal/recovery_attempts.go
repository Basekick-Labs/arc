package wal

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	// walFileSuffix is the extension every ordinary WAL file carries.
	walFileSuffix = ".wal"
	// replayAttemptsSuffix is appended to a WAL file's path to name its
	// attempt sidecar.
	replayAttemptsSuffix = ".recovery"
	// walRecoveryTempPrefix is the os.CreateTemp pattern prefix for a sidecar
	// write's temporary file. Named so the artifact sweep can recognise one
	// that a crash leaked.
	walRecoveryTempPrefix = ".wal-recovery-"
	// replayTailWindow is how much of a WAL file's tail the sidecar
	// fingerprints. A torn-tail repair truncates to the last good entry or
	// rewrites the tail, so this window is where a real repair shows up, and
	// it is bounded work on a multi-GB file.
	replayTailWindow = 64 * 1024
)

// Replay attempts belong to an immutable, closed WAL file. Size and mtime
// prevent a replacement/repaired file at the same path inheriting old strikes.
// Each node must own its WAL directory; it must not be shared by processes.
type replayAttempts struct {
	Version  int   `json:"version"`
	Attempts int   `json:"attempts"`
	Size     int64 `json:"size"`
	Modified int64 `json:"modified_unix_nano"`
	// TailCRC discriminates the one repair size and mtime cannot see: a file
	// rewritten IN PLACE to the same length within one filesystem timestamp
	// tick (1 s granularity on some network mounts, which is where repairs
	// happen). HasTailCRC is explicit rather than zero-means-absent, because a
	// real CRC of 0 is legal.
	//
	// Both fields are OPTIONAL and Version stays 1 deliberately. encoding/json
	// ignores unknown fields, so a sidecar written here still reads on a binary
	// that predates them and keeps that binary's size+mtime behaviour. Bumping
	// the version instead would make the older binary REJECT the sidecar at the
	// version gate below - and a rejection aborts the whole recovery pass and
	// leaves every later file unread, permanently, after a rollback.
	TailCRC    uint32 `json:"tail_crc32,omitempty"`
	HasTailCRC bool   `json:"has_tail_crc32,omitempty"`
}

// walFileFingerprint observes a WAL file's size, modification time and a CRC
// over its final replayTailWindow bytes through ONE descriptor, so all three
// describe the same observation. A stat followed by a separate read can record
// a size that does not match the bytes hashed.
//
// tailOK is false when there is no tail to hash or it could not be read. That
// is "no discriminator available", never an error: the caller falls through to
// the size+mtime comparison rather than gaining a second way to abort a pass.
func walFileFingerprint(path string) (size int64, modified int64, tailCRC uint32, tailOK bool, err error) {
	// Stat first, and keep stat as the ONLY failure that propagates. Opening is
	// strictly more restrictive — a mode that allows stat but not read, fd
	// exhaustion (stat needs no descriptor) — and an error out of here aborts
	// the whole recovery pass, which is the defect the out-of-range checkpoint
	// fix exists to remove. A non-regular file is never opened at all: a FIFO
	// left at a `*.wal` path would block os.Open indefinitely with no context.
	outer, err := os.Stat(path)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !outer.Mode().IsRegular() || outer.Size() == 0 {
		return outer.Size(), outer.ModTime().UnixNano(), 0, false, nil
	}
	f, openErr := os.Open(path)
	if openErr != nil {
		return outer.Size(), outer.ModTime().UnixNano(), 0, false, nil
	}
	defer f.Close()
	// Size, mtime and CRC from the SAME descriptor, so all three describe one
	// observation; a stat followed by a separate read can record a size that
	// does not match the bytes hashed.
	info, statErr := f.Stat()
	if statErr != nil {
		return outer.Size(), outer.ModTime().UnixNano(), 0, false, nil
	}
	size = info.Size()
	modified = info.ModTime().UnixNano()
	window := int64(replayTailWindow)
	if size < window {
		window = size
	}
	if window <= 0 {
		return size, modified, 0, false, nil
	}
	buf := make([]byte, window)
	if _, readErr := f.ReadAt(buf, size-window); readErr != nil {
		return size, modified, 0, false, nil
	}
	return size, modified, crc32.ChecksumIEEE(buf), true, nil
}

func readReplayAttempts(path string) (int, error) {
	data, err := os.ReadFile(path + replayAttemptsSuffix)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var state replayAttempts
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, fmt.Errorf("decode replay attempts: %w", err)
	}
	if state.Version != 1 || state.Attempts < 1 {
		return 0, fmt.Errorf("invalid replay attempts metadata")
	}
	size, modified, tailCRC, tailOK, err := walFileFingerprint(path)
	if err != nil {
		return 0, err
	}
	if size != state.Size || modified != state.Modified {
		return 0, nil
	}
	// A sidecar from a binary without the tail fields, or a tail that could not
	// be read, leaves the size+mtime verdict above standing rather than
	// inventing one.
	if state.HasTailCRC && tailOK && state.TailCRC != tailCRC {
		return 0, nil
	}
	return state.Attempts, nil
}

// writeReplayAttemptsFn is the sidecar write noteReplayFailure performs. It is
// a variable so a test can inject the out-of-space failure that the feature
// has to survive: planting an obstruction at the sidecar path fails the READ
// instead, which is a different path.
var writeReplayAttemptsFn = writeReplayAttempts

func writeReplayAttempts(path string, attempts int) error {
	size, modified, tailCRC, tailOK, err := walFileFingerprint(path)
	if err != nil {
		return err
	}
	data, err := json.Marshal(replayAttempts{Version: 1, Attempts: attempts,
		Size: size, Modified: modified, TailCRC: tailCRC, HasTailCRC: tailOK})
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, walRecoveryTempPrefix+"*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(temp, path+replayAttemptsSuffix); err != nil {
		return err
	}
	return syncRecoveryDirectory(dir)
}

func syncRecoveryDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// A stale sidecar cannot remove WAL data. Report cleanup failures, but keep a
// successfully replayed/quarantined file's outcome. Deletion still requires
// the normal flush barrier. A subsequent failure reset-checks the sidecar.
func (r *Recovery) removeReplayAttempts(path string) {
	err := os.Remove(path + replayAttemptsSuffix)
	if os.IsNotExist(err) {
		return
	}
	if err == nil {
		err = syncRecoveryDirectory(filepath.Dir(path))
	}
	if err != nil {
		r.logger.Warn().Err(err).Str("file", path).Msg("Failed to remove WAL replay attempt metadata")
	}
}
