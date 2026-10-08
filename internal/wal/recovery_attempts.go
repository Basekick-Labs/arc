package wal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Replay attempts belong to an immutable, closed WAL file. Size and mtime
// prevent a replacement/repaired file at the same path inheriting old strikes.
// Each node must own its WAL directory; it must not be shared by processes.
type replayAttempts struct {
	Version  int   `json:"version"`
	Attempts int   `json:"attempts"`
	Size     int64 `json:"size"`
	Modified int64 `json:"modified_unix_nano"`
}

func readReplayAttempts(path string) (int, error) {
	data, err := os.ReadFile(path + ".recovery")
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
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if info.Size() != state.Size || info.ModTime().UnixNano() != state.Modified {
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
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	data, err := json.Marshal(replayAttempts{Version: 1, Attempts: attempts,
		Size: info.Size(), Modified: info.ModTime().UnixNano()})
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".wal-recovery-*")
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
	if err := os.Rename(temp, path+".recovery"); err != nil {
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
	err := os.Remove(path + ".recovery")
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
