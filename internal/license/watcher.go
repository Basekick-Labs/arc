package license

import (
	"sync"
	"sync/atomic"
	"time"
)

// watcherEvery is how often the watcher re-reads the phase. Boundaries are
// whole days apart, so half a minute late is fine, and it keeps the clock
// mark moving.
const watcherEvery = 30 * time.Second

// graceWarnEvery is how often grace days 4-6 log their warning.
const graceWarnEvery = time.Hour

// licenseWatch is the watcher's state; the zero value is ready.
type licenseWatch struct {
	writesStopped atomic.Bool
	listenerMu    sync.Mutex
	listener      func(stopped bool) // guarded by listenerMu
	// deliverMu serialises calls to the listener, made outside listenerMu
	// so a slow listener (MQTT reconnects) never blocks registration.
	deliverMu sync.Mutex
	wake      chan struct{} // made once by wakeCh
	wakeOnce  sync.Once
	started   atomic.Bool
}

func (w *licenseWatch) wakeCh() chan struct{} {
	w.wakeOnce.Do(func() { w.wake = make(chan struct{}, 1) })
	return w.wake
}

// WritesStopped reports whether the license is on grace days 7-14, when live
// ingestion is refused. Lock-free: it is on the write admission path.
func (c *Client) WritesStopped() bool {
	return c != nil && c.watch.writesStopped.Load()
}

// OnWritesStoppedChange registers fn to be told when ingestion stops or
// resumes because of the license, and tells it the current state at once. fn
// must return quickly and not call back into the client.
func (c *Client) OnWritesStoppedChange(fn func(stopped bool)) {
	c.evaluatePhase()
	c.watch.listenerMu.Lock()
	c.watch.listener = fn
	c.watch.listenerMu.Unlock()
	c.deliver(fn)
}

// notifyLicenseChange asks the watcher to re-read the phase now: the license
// was replaced, cleared or kept.
func (c *Client) notifyLicenseChange() {
	select {
	case c.watch.wakeCh() <- struct{}{}:
	default:
	}
}

// evaluatePhase recomputes whether ingestion is stopped and tells the
// listener when that changes. Returns the phase and grace day.
func (c *Client) evaluatePhase() (*License, Phase, int) {
	lic := c.GetLicense()
	phase, day := PhaseExpired, 0
	if lic != nil {
		phase, day = lic.Phase()
	}
	stopped := lic != nil && phase == PhaseWritesStopped
	c.watch.listenerMu.Lock()
	changed := c.watch.writesStopped.Swap(stopped) != stopped
	fn := c.watch.listener
	c.watch.listenerMu.Unlock()
	if changed && fn != nil {
		c.deliver(fn)
	}
	return lic, phase, day
}

// deliver tells fn the current state. Serialised, and it reads the state at
// delivery, so two deliveries racing never leave the listener on a stale one.
func (c *Client) deliver(fn func(bool)) {
	c.watch.deliverMu.Lock()
	defer c.watch.deliverMu.Unlock()
	fn(c.watch.writesStopped.Load())
}

// StartLicenseWatcher follows the license along its grace schedule in both
// online and offline mode: it logs each phase change, warns every hour on
// grace days 4-6, keeps WritesStopped current for the ingestion gate and its
// listener, and advances the clock mark. Idempotent.
func (c *Client) StartLicenseWatcher() {
	if !c.watch.started.CompareAndSwap(false, true) {
		return
	}
	go func() {
		ticker := time.NewTicker(watcherEvery)
		defer ticker.Stop()
		last := Phase(-1)
		var lastKey string
		var lastWarn time.Time
		saveWarned := false
		for {
			lic, phase, day := c.evaluatePhase()
			if err := saveClock(); err != nil && !saveWarned {
				saveWarned = true
				c.logger.Warn().Err(err).Msg("Could not save the license clock mark; setting the host clock back is not caught across restarts until it can be saved")
			}
			key := ""
			if lic != nil {
				key = lic.LicenseKey + "|" + lic.ExpiresAt.String()
			}
			if lic != nil && (phase != last || key != lastKey) {
				c.logPhase(lic, phase, day)
				lastWarn = time.Now()
			} else if lic != nil && phase == PhaseGraceWarn && time.Since(lastWarn) >= graceWarnEvery {
				c.logPhase(lic, phase, day)
				lastWarn = time.Now()
			}
			last, lastKey = phase, key
			select {
			case <-ticker.C:
			case <-c.watch.wakeCh():
			case <-c.stopCh:
				return
			}
		}
	}()
}

func (c *Client) logPhase(lic *License, phase Phase, day int) {
	s := lic.Schedule()
	switch phase {
	case PhaseActive:
		c.logger.Info().Time("expires_at", lic.ExpiresAt).Msg("License active")
	case PhaseGrace, PhaseGraceWarn:
		c.logger.Warn().Str("phase", phase.String()).Time("expires_at", lic.ExpiresAt).
			Int("grace_day", day).Time("writes_stop_at", s.WritesStopAt).Time("cutoff", s.Cutoff).
			Msg("License expired and is in its grace period: Enterprise features continue, ingestion stops at writes_stop_at and the license ends at cutoff; renew it to keep them")
	case PhaseWritesStopped:
		c.logger.Warn().Str("phase", phase.String()).Time("expires_at", lic.ExpiresAt).
			Int("grace_day", day).Time("cutoff", s.Cutoff).
			Msg("License expired: grace days 7-14, live ingestion is refused until the license is renewed; Enterprise features end at cutoff")
	case PhaseExpired:
		msg := "License expired past its grace period: Enterprise features are disabled"
		if lic.IsTrial() {
			msg = "Trial license expired (trials have no grace period): Enterprise features are disabled"
		}
		c.logger.Warn().Str("phase", phase.String()).Time("expires_at", lic.ExpiresAt).Msg(msg)
	}
}
