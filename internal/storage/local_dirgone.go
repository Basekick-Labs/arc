package storage

import (
	"errors"
	"io/fs"
	"runtime"
	"syscall"
)

// dirGone reports whether err from creating an entry in a directory (a file,
// a subdirectory, a rename or link target) means the directory was removed
// under the call, which the local backend answers by forgetting its cached
// directories, recreating them and retrying (#1425).
func dirGone(err error) bool {
	return dirGoneOn(dirGoneGOOS, err)
}

// IsDirGone is dirGone for code outside the package that creates entries
// under the storage root directly (internal/cluster/stale_local.go).
func IsDirGone(err error) bool {
	return dirGone(err)
}

// dirGoneGOOS is the platform dirGone classifies for, a variable so Linux CI
// can run each retry site under the darwin rule.
var dirGoneGOOS = runtime.GOOS

// dirGoneOn is dirGone for a given GOOS, so the darwin rule is tested on every
// platform. Linux reports the removal as ENOENT. macOS APFS reports it as
// ENOENT or, when the call found the directory before the rmdir finished
// with it, EINVAL: measured for open(O_CREAT), mkdir, rename and link into a
// directory being removed (#1767); the name is already gone when the EINVAL
// returns, so one retry recreates it. EINVAL also has its usual meaning
// there, a request the filesystem rejects, so a caller treating it as gone
// retries a bounded number of times (once per directory level in ensureDir,
// once around the create in Write) and then reports what the last attempt
// returned. Each retry forgets the cached directory chain, as an ENOENT
// retry does, so a path the filesystem rejects on every write also re-walks
// its directories, with their fsyncs, up to that bound on every write, and
// bumps dirGen, so a directory chain another write is creating at that
// moment is not cached either. Keys are validated, so a valid key is not
// expected to meet a genuine EINVAL here; a persistent ENOENT (a missing
// storage root) fails at the first mkdir, before any fsync.
func dirGoneOn(goos string, err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	return goos == "darwin" && errors.Is(err, syscall.EINVAL)
}
