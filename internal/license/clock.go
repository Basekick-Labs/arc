package license

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// The license clock: wall time, but never earlier than a high-water mark that
// survives restarts, so setting the host clock back does not hand an expired
// license more time. It guards an honest installation, not a determined one:
// the mark is a local file, and deleting it resets the guard.
//
// The mark only advances by elapsed monotonic time from the last value saved,
// never by the wall clock, so a forward jump of the wall clock (a VM resumed
// with a wrong clock, an operator testing a future date) is never saved and
// cannot expire a license once the clock is corrected. Time a host spends
// off is not counted either, so the mark trails real time; it only has to
// stay ahead of a clock set back.

// clockFileName is the high-water file, kept beside the auth database.
const clockFileName = "license_clock.json"

// clockSaveEvery is how far the mark must move before it is written again.
const clockSaveEvery = time.Minute

type clockState struct {
	// loaded is the mark read at startup; base is when it was read, carrying
	// the monotonic reading the elapsed time is measured from.
	loaded time.Time
	base   time.Time
}

var (
	clock     atomic.Pointer[clockState]
	clockMu   sync.Mutex // serialises saves
	clockPath string     // guarded by clockMu
	clockSave time.Time  // last value written; guarded by clockMu
)

// licenseNow is the time the license gates use: the wall clock, or the
// high-water mark plus the monotonic time since it was loaded when the wall
// clock reads earlier. No I/O, no lock: it is on the per-request gate path.
func licenseNow() time.Time {
	now := time.Now()
	st := clock.Load()
	if st == nil {
		return now
	}
	if est := st.loaded.Add(time.Since(st.base)); now.Before(est) {
		return est
	}
	return now
}

// InitClock loads the high-water mark from dir (the auth database's
// directory). A missing, unreadable or corrupt file starts the guard from the
// current wall clock; it never fails startup.
func InitClock(dir string) {
	clockMu.Lock()
	defer clockMu.Unlock()
	clockPath = filepath.Join(dir, clockFileName)
	loaded := time.Now()
	if b, err := os.ReadFile(clockPath); err == nil {
		var f struct {
			HighWater time.Time `json:"high_water"`
		}
		if json.Unmarshal(b, &f) == nil && f.HighWater.After(loaded) {
			loaded = f.HighWater
		}
	}
	clock.Store(&clockState{loaded: loaded.Round(0), base: time.Now()})
	clockSave = loaded
}

// saveClock writes the mark when it has moved at least clockSaveEvery since
// the last write. The value written is the monotonic estimate, never the wall
// clock, so a forward wall-clock jump is never persisted.
func saveClock() {
	st := clock.Load()
	if st == nil {
		return
	}
	est := st.loaded.Add(time.Since(st.base)).Round(0)
	clockMu.Lock()
	defer clockMu.Unlock()
	if clockPath == "" || est.Sub(clockSave) < clockSaveEvery {
		return
	}
	b, err := json.Marshal(struct {
		HighWater time.Time `json:"high_water"`
	}{est.UTC()})
	if err != nil {
		return
	}
	tmp := clockPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return
	}
	if err := os.Rename(tmp, clockPath); err != nil {
		_ = os.Remove(tmp)
		return
	}
	clockSave = est
}
