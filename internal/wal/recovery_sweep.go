package wal

import (
	"os"
	"path/filepath"
	"strings"
)

// statWALFileFn is the existence probe the sidecar branch performs. It is a
// variable so a test can inject the EACCES/EIO a degraded volume returns, which
// cannot be produced from a flat file name — the same reason
// writeReplayAttemptsFn exists. Nothing in this package calls t.Parallel(); a
// test that adds it must not swap this.
var statWALFileFn = os.Stat

// sweepRecoveryArtifacts removes WAL-directory residue that nothing else
// reclaims, so a directory under a capacity alert does not accumulate inodes
// for the life of the node:
//
//   - `.wal-recovery-*` temporaries, one per sidecar write, removed by
//     writeReplayAttempts' deferred remove — so a crash in between leaks one.
//   - attempt sidecars whose WAL file is gone. Quarantine removes the sidecar
//     and a successful replay clears it, but no purge does: PurgeFlushed, and
//     purgeWALFiles via PurgeAll (the clean-shutdown purge) and PurgeInactive,
//     all remove `*.wal` and leave `*.wal.recovery` behind.
//
// Errors are reported and never fail the pass; reclaiming inodes is not worth
// refusing to recover data.
func (r *Recovery) sweepRecoveryArtifacts() {
	entries, err := os.ReadDir(r.walDir)
	if err != nil {
		r.logger.Warn().Err(err).Msg("Failed to scan the WAL directory for stale recovery artifacts")
		return
	}

	// One ReadDir, classified in memory. Deliberately not a filepath.Glob per
	// sidecar: that is a syscall pass each, and Glob silently matches NOTHING
	// when the directory portion of the pattern contains `[`, `*` or `?`, which
	// an operator-chosen WAL path can.
	quarantinedBases := make(map[string]struct{})
	for _, entry := range entries {
		if base, ok := quarantinedWALBase(entry.Name()); ok {
			quarantinedBases[base] = struct{}{}
		}
	}

	removed := 0
	var freed int64
	remove := func(name string) {
		path := filepath.Join(r.walDir, name)
		info, statErr := os.Lstat(path)
		if err := os.Remove(path); err != nil {
			if !os.IsNotExist(err) {
				r.logger.Warn().Err(err).Str("file", name).
					Msg("Failed to remove a stale WAL recovery artifact")
			}
			return
		}
		removed++
		if statErr == nil {
			freed += info.Size()
		}
	}

	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasPrefix(name, walRecoveryTempPrefix):
			// A temp here is leaked, not in flight. Recovery passes are
			// serialized — startup recovery completes before the single
			// maintenance goroutine starts — and the operations guide requires
			// each node to own its WAL directory exclusively. A second process
			// would see its rename fail and fall back to the in-memory attempt
			// count: degraded, not wrong.
			remove(name)
		case strings.HasSuffix(name, replayAttemptsSuffix):
			base := strings.TrimSuffix(name, replayAttemptsSuffix)
			if !strings.HasSuffix(base, walFileSuffix) {
				// Arc only ever writes `<file>.wal.recovery`. Anything else
				// ending in `.recovery` is an operator's file, and this sweep
				// does not get to decide its fate.
				continue
			}
			if _, ok := quarantinedBases[base]; ok {
				// The sidecar is removed ON quarantine, so one that survived
				// means that removal failed and was already reported. It is the
				// strike history an operator reconciles the quarantined file
				// against; deleting it here discards that.
				continue
			}
			_, statErr := statWALFileFn(filepath.Join(r.walDir, base))
			if statErr == nil {
				continue
			}
			if !os.IsNotExist(statErr) {
				// Never delete on a bare "stat failed". On a degraded volume —
				// the state the capacity alert exists for — EACCES or EIO would
				// wipe a LIVE poison file's strikes and reset its quarantine
				// clock to zero on every pass, the inverse of the intent.
				r.logger.Warn().Err(statErr).Str("file", name).
					Msg("Could not confirm the WAL file for an attempt sidecar; keeping the sidecar")
				continue
			}
			remove(name)
		}
	}

	if removed > 0 {
		r.logger.Info().Int("removed", removed).Int64("bytes", freed).
			Msg("Reclaimed stale WAL recovery artifacts")
	}
}

// quarantinedWALBase maps a quarantine file name back to the WAL file it was
// renamed from: `<file>.wal.failed`, or `<file>.wal.<nano>.failed` when the
// first name was already taken (noteReplayFailure).
func quarantinedWALBase(name string) (string, bool) {
	trimmed := strings.TrimSuffix(name, ".failed")
	if trimmed == name {
		return "", false
	}
	if strings.HasSuffix(trimmed, walFileSuffix) {
		return trimmed, true
	}
	if idx := strings.LastIndex(trimmed, "."); idx > 0 {
		if base := trimmed[:idx]; strings.HasSuffix(base, walFileSuffix) {
			return base, true
		}
	}
	return "", false
}
