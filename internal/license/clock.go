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
// The mark comes only from time the host clock did not produce: the license
// server's Date header on a response whose license verified, or a license
// file's signed IssuedAt. From there it advances by monotonic elapsed time,
// and that estimate is what is saved. So a host clock that is wrong in either
// direction, at startup or later, is never written into the mark and is never
// held against the license once corrected. The cost: time the host spends off
// is not counted, so the mark trails real time and catches a clock set back
// further than that, not less.

// clockFileName is the high-water file, kept beside the auth database.
const clockFileName = "license_clock.json"

// clockSaveEvery is how far the mark must move before it is written again.
const clockSaveEvery = time.Minute

type clockState struct {
	// mark is the high-water mark; base is when it was set, carrying the
	// monotonic reading the elapsed time is measured from.
	mark time.Time
	base time.Time
}

func (st *clockState) estimate() time.Time { return st.mark.Add(time.Since(st.base)) }

var (
	clock     atomic.Pointer[clockState]
	clockMu   sync.Mutex // serialises AdvanceClock and saves
	clockPath string     // guarded by clockMu
	clockSave time.Time  // last value written; guarded by clockMu
)

// licenseNow is the time the license gates use: the wall clock, or the
// high-water mark plus the monotonic time since it was set when the wall
// clock reads earlier. No I/O, no lock: it is on the per-request gate path.
func licenseNow() time.Time {
	now := time.Now()
	if st := clock.Load(); st != nil {
		if est := st.estimate(); now.Before(est) {
			return est
		}
	}
	return now
}

// InitClock loads the high-water mark from dir (the auth database's
// directory). A missing, unreadable or corrupt file leaves no mark until a
// server time or signed issue time sets one; it never fails startup. It
// returns how far the loaded mark is ahead of the wall clock, for the caller
// to log when the host clock looks set back.
func InitClock(dir string) (ahead time.Duration) {
	clockMu.Lock()
	defer clockMu.Unlock()
	clockPath = filepath.Join(dir, clockFileName)
	clock.Store(nil)
	clockSave = time.Time{}
	b, err := os.ReadFile(clockPath)
	if err != nil {
		return 0
	}
	var f struct {
		HighWater time.Time `json:"high_water"`
	}
	if json.Unmarshal(b, &f) != nil || f.HighWater.IsZero() {
		return 0
	}
	clock.Store(&clockState{mark: f.HighWater.Round(0), base: time.Now()})
	clockSave = f.HighWater
	return time.Until(f.HighWater)
}

// AdvanceClock raises the mark to t, a time the host clock did not produce
// (the license server's Date header, a license file's signed IssuedAt), when
// t is later than the current estimate. Never lowers it.
func AdvanceClock(t time.Time) {
	if t.IsZero() {
		return
	}
	clockMu.Lock()
	defer clockMu.Unlock()
	if st := clock.Load(); st != nil && !t.After(st.estimate()) {
		return
	}
	clock.Store(&clockState{mark: t.Round(0), base: time.Now()})
}

// saveClock writes the mark's current estimate when it has moved at least
// clockSaveEvery since the last write. Nothing is written before a mark
// exists.
func saveClock() error {
	st := clock.Load()
	if st == nil {
		return nil
	}
	est := st.estimate().Round(0)
	clockMu.Lock()
	defer clockMu.Unlock()
	if clockPath == "" || est.Sub(clockSave) < clockSaveEvery {
		return nil
	}
	b, err := json.Marshal(struct {
		HighWater time.Time `json:"high_water"`
	}{est.UTC()})
	if err != nil {
		return err
	}
	tmp := clockPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, clockPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	clockSave = est
	return nil
}
