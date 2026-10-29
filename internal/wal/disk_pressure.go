package wal

import (
	"context"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// WAL volume disk pressure (#676). The writer PVC holds the WAL, Raft, the
// registration journal, arc.db, compaction temp and the standby store, so the
// constraint is the volume's free space, not the WAL's bytes. A WAL write
// that fails for space loses an entry the client was already told was
// accepted; refusing the write up front (503, retryable) loses nothing.
const (
	// DefaultDiskReserveMin is the least default reserve: below this much
	// free space on the WAL volume, live writes are refused.
	DefaultDiskReserveMin = int64(512) << 20
	// diskReserveMaxShareDiv caps the reserve, and the hysteresis band, at a
	// fifth of the volume (or of the directory cap): a setting larger than a
	// small volume would otherwise refuse every write forever.
	diskReserveMaxShareDiv = 5
	// DiskPressureMinOn is the least time the gate stays on once it trips, so
	// clients and MQTT subscriptions are not flapped by every purge.
	DiskPressureMinOn = 5 * time.Second
	// diskSampleEvery is the statfs cadence; rotations and disk-full writes
	// sample at once.
	diskSampleEvery = time.Second
	// diskWarnEvery samples the monitor's warnings.
	diskWarnEvery = time.Minute
	// diskRotateEvery bounds the rotations the monitor asks for while the
	// gate is on and cannot clear (see DiskMonitor.sample).
	diskRotateEvery = 10 * time.Second
)

// What turned the gate on, for the log and the operator.
const (
	causeNone int32 = iota
	causeFree
	causeCap
	causeDiskFull
)

// DefaultDiskReserve is the reserve wal.disk_min_free_mb = 0 selects:
// max(512 MiB, two WAL files).
func DefaultDiskReserve(maxSizeBytes int64) int64 {
	return max(DefaultDiskReserveMin, 2*maxSizeBytes)
}

// DiskPressureLevels returns the effective free-space levels of the gate on a
// volume of total bytes: it turns on below reserve and off at or above
// offPoint. reserve <= 0 selects DefaultDiskReserve. The reserve and the
// band above it (two WAL files) are each clamped to a fifth of the volume;
// clamped reports that the reserve was. total 0 (unknown) clamps nothing.
//
// The standby store's own free-space reserve on the same volume (#1266) must
// stay above offPoint, or a standby fills the volume to where its promotion
// starts under disk pressure.
func DiskPressureLevels(reserve, maxSizeBytes int64, total uint64) (on, offPoint int64, clamped bool) {
	if reserve <= 0 {
		reserve = DefaultDiskReserve(maxSizeBytes)
	}
	band := 2 * maxSizeBytes
	if total > 0 {
		limit := int64(min(total/diskReserveMaxShareDiv, math.MaxInt64/4))
		if reserve > limit {
			reserve, clamped = limit, true
		}
		band = min(band, limit)
	}
	return reserve, reserve + band, clamped
}

// DiskMonitorConfig configures a DiskMonitor.
type DiskMonitorConfig struct {
	// Dir is the WAL directory; its volume is the one sampled.
	Dir string
	// ReserveBytes is wal.disk_min_free_mb in bytes; 0 = DefaultDiskReserve.
	ReserveBytes int64
	// MaxSizeBytes is wal.max_size_mb in bytes (the hysteresis band is two).
	MaxSizeBytes int64
	// MaxDirectoryBytes is wal.max_directory_mb in bytes; 0 = no cap. For a
	// volume whose statfs does not describe the limit that applies (an
	// emptyDir with a sizeLimit reports the node's disk).
	MaxDirectoryBytes int64
	// ReserveFile keeps the reserve file (ReserveFileName) in Dir.
	ReserveFile bool
	Logger      zerolog.Logger

	// ActiveBytes and Rotate let the gate clear when the active WAL file is
	// what holds it on (NewDiskMonitor sets both from the writer): while
	// writes are refused nothing rotates that file, so its bytes, flushed or
	// not, would never be purged. Nil: never rotate.
	ActiveBytes func() int64
	Rotate      func() error
}

// diskSnap is what the hot path needs from the last sample: writes enqueued
// since base are charged against both budgets. A budget is MaxInt64 when its
// signal is off or unreadable (fail open).
type diskSnap struct {
	base       uint64
	freeBudget int64 // free - reserve - in-flight, at the sample
	dirBudget  int64 // cap - directory bytes - in-flight, at the sample
}

// DiskMonitor decides WAL volume disk pressure (#676). A goroutine of its own
// samples the volume every second and at once on a rotation or a disk-full
// write: the WAL maintenance goroutine can block for minutes in recovery.
// Between samples the gate is exact: every byte admitted to the WAL queue
// since the sample is charged against the free space it saw, so a burst
// cannot run past the reserve before the next sample.
//
// It trips when free - bytes since the sample - reservations < reserve, when
// the directory cap is set and directory bytes + bytes since + reservations >
// cap, or on any write that failed for space (DiskFullObserved only trips).
// Requests reserve their size at admission (Reserve), so a large request, or
// several at once, is refused before it can cross the reserve. It clears only at or above
// the off-point (reserve + two WAL files, both clamped) and after
// DiskPressureMinOn: a trip is never delayed by a minimum time in the off
// state, because the trip is what keeps the volume from filling.
type DiskMonitor struct {
	cfg    DiskMonitorConfig
	logger zerolog.Logger

	statfs   func(string) (total, avail uint64, err error)
	dirBytes func(string) (int64, error)
	now      func() time.Time
	every    time.Duration
	minOn    time.Duration

	enqueued   *atomic.Uint64
	processed  *atomic.Uint64
	fullEvents *atomic.Uint64
	hint       <-chan struct{}

	on          atomic.Bool
	onSinceNano atomic.Int64
	snap        atomic.Pointer[diskSnap]
	kick        chan struct{}
	reserveOn   atomic.Int64 // effective reserve (levels of the last sample)
	offPoint    atomic.Int64
	// reserved is the WAL bytes admitted requests (Reserve) have not queued
	// yet; charged against both budgets beside the bytes queued since the
	// sample.
	reserved atomic.Int64
	cause    atomic.Int32
	rotating atomic.Bool

	// Sampler goroutine only.
	lastFull     uint64
	reserveHeld  bool
	clampWarned  bool
	lastStatWarn time.Time
	lastDirWarn  time.Time
	lastResvWarn time.Time
	lastRotate   time.Time

	// notifyMu serialises listener calls with registration, so a listener
	// sees every transition once, in order.
	notifyMu  sync.Mutex
	notified  bool
	listeners []func(on bool)

	startOnce sync.Once
	started   atomic.Bool
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// NewDiskMonitor builds the disk pressure monitor of w. Start it before
// serving writes.
func NewDiskMonitor(w *Writer, cfg DiskMonitorConfig) *DiskMonitor {
	if cfg.ActiveBytes == nil {
		cfg.ActiveBytes = w.ActiveFileBytes
	}
	if cfg.Rotate == nil {
		cfg.Rotate = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			return w.RotateContext(ctx)
		}
	}
	return newDiskMonitor(cfg, &w.enqueuedBytes, &w.processedBytes, &w.diskFullEvents, w.diskHint)
}

func newDiskMonitor(cfg DiskMonitorConfig, enqueued, processed, fullEvents *atomic.Uint64, hint <-chan struct{}) *DiskMonitor {
	m := &DiskMonitor{
		cfg:        cfg,
		logger:     cfg.Logger.With().Str("component", "wal-disk").Logger(),
		statfs:     diskSpaceFn,
		dirBytes:   capDirectoryBytes,
		now:        time.Now,
		every:      diskSampleEvery,
		minOn:      DiskPressureMinOn,
		enqueued:   enqueued,
		processed:  processed,
		fullEvents: fullEvents,
		hint:       hint,
		kick:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	on, off, _ := DiskPressureLevels(cfg.ReserveBytes, cfg.MaxSizeBytes, 0)
	m.reserveOn.Store(on)
	m.offPoint.Store(off)
	m.lastFull = fullEvents.Load()
	return m
}

// Start takes the first sample, so the gate is armed before the first write,
// and starts the sampler goroutine. Idempotent.
func (m *DiskMonitor) Start() {
	m.startOnce.Do(func() {
		if m.cfg.ReserveFile {
			if _, err := os.Stat(ReserveFilePath(m.cfg.Dir)); err == nil {
				m.reserveHeld = true
			}
		}
		m.sample()
		m.started.Store(true)
		go m.run()
	})
}

// Close stops the sampler goroutine. The gate keeps its last state.
func (m *DiskMonitor) Close() error {
	m.stopOnce.Do(func() { close(m.stop) })
	if m.started.Load() {
		<-m.done
	}
	return nil
}

func (m *DiskMonitor) run() {
	defer close(m.done)
	t := time.NewTicker(m.every)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
		case <-m.hint:
		case <-m.kick:
		}
		m.sample()
	}
}

// Pressure reports whether live writes are refused now. The hot path: an
// atomic flag, one snapshot load and two counter loads. Writes that would
// cross a budget trip the gate here, at once, and wake the sampler.
func (m *DiskMonitor) Pressure() bool {
	if m.on.Load() {
		return true
	}
	s := m.snap.Load()
	if s == nil {
		return false
	}
	if c := s.over(int64(m.enqueued.Load()-s.base) + m.reserved.Load()); c != causeNone {
		m.trip(c)
		return true
	}
	return false
}

// over reports which budget charged bytes exceed, causeNone when neither.
func (s *diskSnap) over(charged int64) int32 {
	switch {
	case charged > s.freeBudget:
		return causeFree
	case charged > s.dirBudget:
		return causeCap
	}
	return causeNone
}

// Reserve admits a request of about n WAL bytes (#676): it fits when the
// bytes queued since the sample, every reservation not yet queued and n stay
// within both budgets; it is then charged until Release. A request that does
// not fit is refused without turning the gate on, unless what is already
// charged is over a budget. Concurrent reservations see each other.
func (m *DiskMonitor) Reserve(n int64) bool {
	if m.on.Load() {
		return false
	}
	n = max(n, 0)
	s := m.snap.Load()
	if s == nil {
		m.reserved.Add(n)
		return true
	}
	for {
		r := m.reserved.Load()
		charged := int64(m.enqueued.Load()-s.base) + r
		if s.over(charged+n) != causeNone {
			if c := s.over(charged); c != causeNone {
				m.trip(c)
			}
			return false
		}
		if m.reserved.CompareAndSwap(r, r+n) {
			return true
		}
	}
}

// Release returns n reserved bytes: queued since (now charged as queued),
// or never written.
func (m *DiskMonitor) Release(n int64) {
	if n > 0 {
		m.reserved.Add(-n)
	}
}

// trip turns the gate on; the first caller stamps the time and the cause and
// wakes the sampler, which notifies the listeners.
func (m *DiskMonitor) trip(cause int32) {
	if m.on.CompareAndSwap(false, true) {
		m.cause.Store(cause)
		m.onSinceNano.Store(m.now().UnixNano())
		select {
		case m.kick <- struct{}{}:
		default:
		}
	}
}

// Levels returns the effective reserve (the gate is on below it) and
// off-point (it clears at or above it), in free bytes on the WAL volume, as
// of the last sample.
func (m *DiskMonitor) Levels() (reserve, offPoint int64) {
	return m.reserveOn.Load(), m.offPoint.Load()
}

// OnChange registers fn for every change of the gate, called from the
// sampler goroutine; fn(true) is called at once when the gate is already on.
// fn must not block for long and must not call OnChange.
func (m *DiskMonitor) OnChange(fn func(on bool)) {
	m.notifyMu.Lock()
	defer m.notifyMu.Unlock()
	m.listeners = append(m.listeners, fn)
	if m.notified {
		fn(true)
	}
}

func (m *DiskMonitor) notify(on bool) {
	m.notifyMu.Lock()
	defer m.notifyMu.Unlock()
	if m.notified == on {
		return
	}
	m.notified = on
	metrics.Get().SetWALDiskPressure(on)
	reserve, off := m.Levels()
	if on {
		switch m.cause.Load() {
		case causeCap:
			m.logger.Warn().Int64("max_directory_bytes", m.cfg.MaxDirectoryBytes).
				Msg("WAL directory at wal.max_directory_mb: live writes are refused (503, retryable) until the WAL directory shrinks; see arc_wal_dir_bytes (quarantined *.failed files do not count)")
		case causeDiskFull:
			m.logger.Warn().Int64("reserve_bytes", reserve).Int64("off_point_bytes", off).
				Msg("A WAL write failed for lack of space: live writes are refused (503, retryable) until free space returns; see arc_wal_disk_free_bytes")
		default:
			m.logger.Warn().Int64("reserve_bytes", reserve).Int64("off_point_bytes", off).
				Msg("WAL volume disk pressure: live writes are refused (503, retryable) until free space returns; see arc_wal_disk_free_bytes")
		}
	} else {
		m.logger.Info().Int64("off_point_bytes", off).Msg("WAL volume disk pressure cleared: live writes are accepted again")
	}
	for _, fn := range m.listeners {
		fn(on)
	}
}

// inFlight is what the queue holds and the writer has not finished with:
// bytes the volume's free space does not show yet. processed is read first;
// a racing entry can count in processed before enqueued, hence the floor.
func (m *DiskMonitor) inFlight() (enqueued uint64, inflight int64) {
	p := m.processed.Load()
	e := m.enqueued.Load()
	if e < p {
		return e, 0
	}
	return e, int64(min(e-p, math.MaxInt64/4))
}

func (m *DiskMonitor) sample() {
	now := m.now()
	enq, inflight := m.inFlight()
	snap := diskSnap{base: enq, freeBudget: math.MaxInt64, dirBudget: math.MaxInt64}
	freeOK, capOK := true, true
	// Reservations of admitted requests are not on disk yet: they count
	// against tripping and clearing alike.
	reserved := m.reserved.Load()

	total, avail, err := m.statfs(m.cfg.Dir)
	statOK := err == nil && total > 0
	var free int64
	if statOK {
		on, off, clamped := DiskPressureLevels(m.cfg.ReserveBytes, m.cfg.MaxSizeBytes, total)
		m.reserveOn.Store(on)
		m.offPoint.Store(off)
		if clamped && !m.clampWarned {
			m.clampWarned = true
			m.logger.Warn().Int64("configured_bytes", m.cfg.ReserveBytes).Int64("reserve_bytes", on).Uint64("volume_bytes", total).
				Msg("wal.disk_min_free_mb is more than a fifth of the WAL volume; clamped so the volume can still take writes")
		}
		free = int64(min(avail, math.MaxInt64/4)) - inflight
		snap.freeBudget = free - on
		freeOK = free-reserved >= off
		metrics.Get().SetWALDiskFreeBytes(int64(min(avail, math.MaxInt64)))
	} else {
		metrics.Get().IncWALDiskStatErrors()
		if now.Sub(m.lastStatWarn) >= diskWarnEvery {
			m.lastStatWarn = now
			m.logger.Warn().Err(err).Str("dir", m.cfg.Dir).
				Msg("Cannot read the WAL volume's free space; the disk pressure gate does not refuse on free space until it can (sampled: at most one line a minute)")
		}
	}

	if limit := m.cfg.MaxDirectoryBytes; limit > 0 {
		used, derr := m.dirBytes(m.cfg.Dir)
		if derr == nil {
			used += inflight
			band := min(2*m.cfg.MaxSizeBytes, limit/diskReserveMaxShareDiv)
			snap.dirBudget = limit - used
			capOK = used+reserved <= limit-band
		} else if now.Sub(m.lastDirWarn) >= diskWarnEvery {
			m.lastDirWarn = now
			m.logger.Warn().Err(derr).Str("dir", m.cfg.Dir).
				Msg("Cannot size the WAL directory; wal.max_directory_mb is not enforced until it can (sampled: at most one line a minute)")
		}
	}
	m.snap.Store(&snap)

	events := m.fullEvents.Load()
	newFull := events != m.lastFull
	m.lastFull = events

	switch {
	case newFull:
		m.trip(causeDiskFull)
	case snap.over(reserved) != causeNone:
		m.trip(snap.over(reserved))
	case m.on.Load() && freeOK && capOK && now.Sub(time.Unix(0, m.onSinceNano.Load())) >= m.minOn:
		m.on.Store(false)
		m.cause.Store(causeNone)
	}
	m.notify(m.on.Load())

	// While writes are refused nothing rotates the active WAL file, so its
	// bytes are never purged; with a small wal.max_directory_mb they alone
	// can keep the gate on forever. Rotate it so the purge can take what is
	// flushed (an unflushed entry still pins its file). Bounded, and off the
	// sampler's goroutine: a rotation waits for the write queue.
	if m.on.Load() && !(freeOK && capOK) && m.cfg.Rotate != nil && m.cfg.ActiveBytes != nil &&
		now.Sub(m.lastRotate) >= diskRotateEvery && m.cfg.ActiveBytes() > WALFileHeaderSize &&
		m.rotating.CompareAndSwap(false, true) {
		m.lastRotate = now
		go func() {
			defer m.rotating.Store(false)
			if err := m.cfg.Rotate(); err != nil {
				m.logger.Warn().Err(err).Msg("Could not rotate the WAL file while writes are refused for disk pressure")
			}
		}()
	}

	// The reserve file comes back only with writes accepted and room for it
	// above the off-point: creating it must never be what turns the gate on.
	if m.cfg.ReserveFile && !m.reserveHeld && statOK && !m.on.Load() && free >= m.offPoint.Load()+ReserveFileSize {
		if err := createReserveFile(ReserveFilePath(m.cfg.Dir), ReserveFileSize); err != nil {
			if now.Sub(m.lastResvWarn) >= diskWarnEvery {
				m.lastResvWarn = now
				m.logger.Warn().Err(err).Msg("Cannot create the WAL reserve file; retried while there is room (sampled: at most one line a minute)")
			}
		} else {
			m.reserveHeld = true
			m.logger.Info().Str("file", ReserveFilePath(m.cfg.Dir)).Int64("bytes", ReserveFileSize).
				Msg("WAL reserve file created: released at start if the volume is ever full, so Arc can boot")
		}
	}
}
