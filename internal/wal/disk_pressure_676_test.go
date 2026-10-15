package wal

// #676 part B: the WAL volume disk pressure gate. Free space is sampled; between
// samples every byte admitted to the WAL queue is charged against it, so the
// gate is exact; it clears only at the off-point and after a minimum time on.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

const mib = int64(1) << 20

// fakeDisk drives a DiskMonitor without a volume, a writer or a clock.
type fakeDisk struct {
	mu         sync.Mutex
	total      uint64
	avail      uint64
	statErr    error
	dir        int64
	enqueued   atomic.Uint64
	processed  atomic.Uint64
	fullEvents atomic.Uint64
	now        time.Time
}

func (f *fakeDisk) statfs(string) (uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total, f.avail, f.statErr
}

func (f *fakeDisk) dirBytes(string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dir, nil
}

func (f *fakeDisk) set(fn func(f *fakeDisk)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeDisk) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeDisk) advance(d time.Duration) { f.set(func(f *fakeDisk) { f.now = f.now.Add(d) }) }

// newFakeMonitor builds a monitor on f with a 1 GiB reserve, 100 MiB WAL files
// (off-point 1.2 GiB) on a 100 GiB volume. It is sampled by hand: Start is
// never called, so no goroutine samples behind the test.
func newFakeMonitor(t *testing.T, f *fakeDisk, cfg DiskMonitorConfig) *DiskMonitor {
	t.Helper()
	if cfg.ReserveBytes == 0 {
		cfg.ReserveBytes = 1024 * mib
	}
	if cfg.MaxSizeBytes == 0 {
		cfg.MaxSizeBytes = 100 * mib
	}
	if f.total == 0 {
		f.total = 100 << 30
	}
	if f.now.IsZero() {
		f.now = time.Unix(1_000_000, 0)
	}
	cfg.Logger = zerolog.Nop()
	m := newDiskMonitor(cfg, &f.enqueued, &f.processed, &f.fullEvents, nil)
	m.statfs = f.statfs
	m.dirBytes = f.dirBytes
	m.now = f.clock
	return m
}

func TestDiskPressureLevels(t *testing.T) {
	cases := []struct {
		name             string
		reserve, maxSize int64
		total            uint64
		wantOn, wantOff  int64
		wantClamped      bool
	}{
		{"default is 512 MiB at the default file size", 0, 100 * mib, 100 << 30, 512 * mib, 712 * mib, false},
		{"default is two files when they are larger", 0, 400 * mib, 100 << 30, 800 * mib, 1600 * mib, false},
		{"explicit", 2048 * mib, 100 * mib, 100 << 30, 2048 * mib, 2248 * mib, false},
		{"clamped to a fifth of the volume", 0, 100 * mib, 1 << 30, 1024 * mib / 5, 1024*mib/5 + 200*mib, true},
		{"band clamped too", 0, 100 * mib, 500 << 20, 100 * mib, 200 * mib, true},
		{"unknown volume clamps nothing", 0, 100 * mib, 0, 512 * mib, 712 * mib, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			on, off, clamped := DiskPressureLevels(tc.reserve, tc.maxSize, tc.total)
			if on != tc.wantOn || off != tc.wantOff || clamped != tc.wantClamped {
				t.Fatalf("got (%d, %d, %v), want (%d, %d, %v)", on, off, clamped, tc.wantOn, tc.wantOff, tc.wantClamped)
			}
		})
	}
}

// Between samples, bytes admitted to the WAL queue are charged against the
// free space the last sample saw: a burst trips the gate without a new sample.
func TestDiskPressureTripsOnBytesSinceTheSample(t *testing.T) {
	f := &fakeDisk{avail: uint64(1024*mib + 10*mib)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	if m.Pressure() {
		t.Fatal("on with 10 MiB above the reserve and nothing written")
	}
	f.enqueued.Add(uint64(10 * mib)) // exactly at the reserve: still admitted
	if m.Pressure() {
		t.Fatal("on at exactly the reserve")
	}
	f.enqueued.Add(1)
	if !m.Pressure() {
		t.Fatal("a write past the reserve was admitted between samples")
	}
	select {
	case <-m.kick:
	default:
		t.Fatal("tripping between samples did not wake the sampler")
	}
}

// Bytes queued but not yet written at the sample are not in its free space;
// they count against the budget too.
func TestDiskPressureCountsInFlightBytesAtTheSample(t *testing.T) {
	f := &fakeDisk{avail: uint64(1024*mib + 10*mib)}
	f.enqueued.Store(uint64(11 * mib)) // queued, none written yet
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	if !m.Pressure() {
		t.Fatal("11 MiB in flight over 10 MiB of headroom did not trip the gate")
	}
}

// A racing entry can be counted processed before enqueued; that must not read
// as an enormous in-flight figure.
func TestDiskPressureInFlightNeverUnderflows(t *testing.T) {
	f := &fakeDisk{avail: uint64(2048 * mib)}
	f.processed.Store(100)
	f.enqueued.Store(50)
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	if m.Pressure() {
		t.Fatal("processed > enqueued tripped the gate")
	}
}

// On below the reserve, off only at the off-point (reserve + 2 files) and not
// before DiskPressureMinOn.
func TestDiskPressureHysteresisAndMinimumTimeOn(t *testing.T) {
	f := &fakeDisk{avail: uint64(1000 * mib)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	var changes []bool
	m.OnChange(func(on bool) { changes = append(changes, on) })
	m.sample()
	if !m.Pressure() {
		t.Fatal("not on below the reserve")
	}
	// Above the reserve, below the off-point: stays on, however long.
	f.set(func(f *fakeDisk) { f.avail = uint64(1100 * mib) })
	f.advance(time.Hour)
	m.sample()
	if !m.Pressure() {
		t.Fatal("cleared below the off-point")
	}
	// Re-trip so the dwell starts now, then reach the off-point at once.
	f.set(func(f *fakeDisk) { f.avail = uint64(1000 * mib) })
	m.on.Store(false)
	m.sample()
	f.set(func(f *fakeDisk) { f.avail = uint64(1224 * mib) })
	f.advance(DiskPressureMinOn - time.Millisecond)
	m.sample()
	if !m.Pressure() {
		t.Fatal("cleared before DiskPressureMinOn")
	}
	f.advance(time.Millisecond)
	m.sample()
	if m.Pressure() {
		t.Fatal("still on at the off-point after the minimum time")
	}
	if len(changes) < 2 || !changes[0] || changes[len(changes)-1] {
		t.Fatalf("listener saw %v, want on ... off", changes)
	}
}

// The trip is never delayed by a minimum time off: it is what keeps the
// volume from filling.
func TestDiskPressureTripsAtOnceAfterClearing(t *testing.T) {
	f := &fakeDisk{avail: uint64(1000 * mib)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	f.set(func(f *fakeDisk) { f.avail = uint64(2048 * mib) })
	f.advance(DiskPressureMinOn)
	m.sample()
	if m.Pressure() {
		t.Fatal("did not clear")
	}
	f.enqueued.Add(uint64(1024*mib) + 1)
	if !m.Pressure() {
		t.Fatal("a write right after clearing ran past the reserve")
	}
}

// A statfs error fails open, counted and sampled.
func TestDiskPressureStatErrorFailsOpen(t *testing.T) {
	f := &fakeDisk{avail: uint64(10 * mib), statErr: errors.New("EIO")}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	before := metrics.Get().Snapshot()["wal_disk_stat_errors"].(int64)
	m.sample()
	f.enqueued.Add(uint64(10 << 30))
	if m.Pressure() {
		t.Fatal("an unreadable volume refused writes")
	}
	if after := metrics.Get().Snapshot()["wal_disk_stat_errors"].(int64); after != before+1 {
		t.Fatalf("stat errors %d -> %d", before, after)
	}
}

// A write that failed for space trips the gate; the writer's sticky
// diskFull flag (DiskFullObserved) never holds it on once space is back.
func TestDiskPressureDiskFullOnlyTrips(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	if m.Pressure() {
		t.Fatal("on with space")
	}
	f.fullEvents.Add(1)
	m.sample()
	if !m.Pressure() {
		t.Fatal("a disk-full write did not trip the gate")
	}
	if m.Reserve(1) {
		t.Fatal("a request was admitted while writes are refused, with free space to spare")
	}
	f.advance(DiskPressureMinOn)
	m.sample() // no new disk-full write since: free space decides
	if m.Pressure() {
		t.Fatal("an old disk-full write held the gate on with free space back")
	}
	f.fullEvents.Add(1)
	f.advance(time.Hour)
	m.sample()
	if !m.Pressure() {
		t.Fatal("a second disk-full write did not trip the gate again")
	}
}

// wal.max_directory_mb: bytes in the directory plus bytes since the sample
// against the cap, cleared a band below it.
func TestDiskPressureDirectoryCap(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30), dir: 900 * mib}
	m := newFakeMonitor(t, f, DiskMonitorConfig{MaxDirectoryBytes: 1000 * mib})
	m.sample()
	if m.Pressure() {
		t.Fatal("on under the cap")
	}
	f.enqueued.Add(uint64(100*mib) + 1)
	if !m.Pressure() {
		t.Fatal("a write past the cap was admitted")
	}
	// Band = min(2 x 100 MiB, cap/5 = 200 MiB): off at <= 800 MiB.
	f.set(func(f *fakeDisk) { f.dir = 850 * mib })
	f.processed.Store(f.enqueued.Load())
	f.advance(time.Hour)
	m.sample()
	if !m.Pressure() {
		t.Fatal("cleared inside the band")
	}
	f.set(func(f *fakeDisk) { f.dir = 800 * mib })
	m.sample()
	if m.Pressure() {
		t.Fatal("did not clear below the band")
	}
}

// With no cap the directory is never sized: the cap is off by default.
func TestDiskPressureNoCapNeverSizesTheDirectory(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.dirBytes = func(string) (int64, error) { t.Fatal("sized the directory with no cap"); return 0, nil }
	m.sample()
}

// The off-point a sample computes is what Levels reports, clamp included.
func TestDiskMonitorLevelsFollowTheVolume(t *testing.T) {
	f := &fakeDisk{total: 1 << 30, avail: 1 << 29}
	m := newFakeMonitor(t, f, DiskMonitorConfig{ReserveBytes: 0, MaxSizeBytes: 100 * mib})
	m.cfg.ReserveBytes = 0
	m.sample()
	on, off := m.Levels()
	wantOn, wantOff, _ := DiskPressureLevels(0, 100*mib, 1<<30)
	if on != wantOn || off != wantOff {
		t.Fatalf("Levels = (%d, %d), want (%d, %d)", on, off, wantOn, wantOff)
	}
}

// A listener registered while the gate is on hears it at once (MQTT is
// wired after the monitor starts).
func TestDiskMonitorLateListenerHearsCurrentState(t *testing.T) {
	f := &fakeDisk{avail: uint64(10 * mib)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	var got []bool
	m.OnChange(func(on bool) { got = append(got, on) })
	if len(got) != 1 || !got[0] {
		t.Fatalf("late listener got %v, want [true]", got)
	}
}

// Only an entry admitted to the queue is charged: a dropped one never reaches
// the disk.
func TestEnqueuedBytesCountOnlyAdmittedEntries(t *testing.T) {
	w := &Writer{entryChan: make(chan walEntry, 1)}
	if err := w.tryEnqueueEntry(walEntry{data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if err := w.tryEnqueueEntry(walEntry{data: make([]byte, 50)}); !errors.Is(err, ErrWALDropped) {
		t.Fatalf("err = %v, want ErrWALDropped", err)
	}
	if got := w.enqueuedBytes.Load(); got != 100 {
		t.Fatalf("enqueued = %d, want 100 (the dropped entry counted)", got)
	}
}

// A real writer feeds both counters and wakes the monitor on rotation.
func TestWriterFeedsTheDiskMonitor(t *testing.T) {
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeAsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.AppendRaw([]byte{0x90}); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate(); err != nil { // behind the append in the queue
		t.Fatal(err)
	}
	if got := w.ActiveFileBytes(); got != WALFileHeaderSize {
		t.Fatalf("active file %d bytes after a rotation, want the header", got)
	}
	if w.enqueuedBytes.Load() == 0 || w.processedBytes.Load() != w.enqueuedBytes.Load() {
		t.Fatalf("enqueued %d processed %d", w.enqueuedBytes.Load(), w.processedBytes.Load())
	}
	select {
	case <-w.diskHint:
	default:
		t.Fatal("a rotation did not wake the disk monitor")
	}
	w.noteDiskFull()
	if w.diskFullEvents.Load() != 1 || !w.DiskFullObserved() {
		t.Fatal("noteDiskFull recorded nothing")
	}
}

// The monitor's goroutine samples on a hint and stops on Close.
func TestDiskMonitorRunsAndStops(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30)}
	hint := make(chan struct{}, 1)
	cfg := DiskMonitorConfig{ReserveBytes: 1024 * mib, MaxSizeBytes: 100 * mib, Logger: zerolog.Nop()}
	f.total, f.now = 100<<30, time.Unix(1_000_000, 0)
	m := newDiskMonitor(cfg, &f.enqueued, &f.processed, &f.fullEvents, hint)
	m.statfs, m.dirBytes, m.now = f.statfs, f.dirBytes, f.clock
	m.every = time.Hour
	m.Start()
	if m.Pressure() {
		t.Fatal("on with space")
	}
	f.set(func(f *fakeDisk) { f.avail = uint64(mib) })
	hint <- struct{}{}
	deadline := time.Now().Add(5 * time.Second)
	for !m.Pressure() {
		if time.Now().After(deadline) {
			t.Fatal("a hint did not sample")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	_ = m.Close() // idempotent
}

func TestDiskMonitorCloseWithoutStart(t *testing.T) {
	f := &fakeDisk{}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReserveNameIsNotAWALFile(t *testing.T) {
	for _, n := range []string{ReserveFileName, ReserveFileName + reserveTempSuffix} {
		if strings.HasSuffix(n, walFileSuffix) || strings.HasPrefix(n, walRecoveryTempPrefix) || strings.HasSuffix(n, replayAttemptsSuffix) {
			t.Fatalf("%q matches a WAL artifact pattern", n)
		}
	}
}

// Requests reserve their size at admission: two that each fit but not both
// together cannot both pass, however they interleave; both pass when the
// budget covers both. Releases return the room.
func TestDiskReserveConcurrentRequests(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headroom int64 // free space above the reserve
		want     int
	}{
		{"budget for one", 150 * mib, 1},
		{"budget for both", 200 * mib, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 50; round++ {
				f := &fakeDisk{avail: uint64(1024*mib + tc.headroom)}
				m := newFakeMonitor(t, f, DiskMonitorConfig{})
				m.sample()
				var admitted atomic.Int32
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := 0; i < 2; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						if m.Reserve(100 * mib) {
							admitted.Add(1)
						}
					}()
				}
				close(start)
				wg.Wait()
				if got := int(admitted.Load()); got != tc.want {
					t.Fatalf("round %d: %d admitted, want %d", round, got, tc.want)
				}
			}
		})
	}
}

// A request that does not fit is refused without refusing everyone; what is
// reserved counts against later writes until released.
func TestDiskReserveChargesUntilReleased(t *testing.T) {
	f := &fakeDisk{avail: uint64(1024*mib + 100*mib)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	if m.Reserve(100*mib + 1) {
		t.Fatal("a request larger than the headroom was admitted")
	}
	if m.Pressure() {
		t.Fatal("refusing one large request turned the gate on for everyone")
	}
	if !m.Reserve(60 * mib) {
		t.Fatal("a request that fits was refused")
	}
	if m.Reserve(50 * mib) {
		t.Fatal("a second request ignored the first one's reservation")
	}
	f.enqueued.Add(uint64(41 * mib)) // unreserved writes past what is left
	if !m.Pressure() {
		t.Fatal("reserved bytes were not charged against other writes")
	}
	m.Release(60 * mib)
	if m.reserved.Load() != 0 {
		t.Fatalf("reserved %d after release", m.reserved.Load())
	}
}

// The sample counts outstanding reservations both to trip and to clear.
func TestDiskSampleCountsReservations(t *testing.T) {
	f := &fakeDisk{avail: uint64(2048 * mib)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{})
	m.sample()
	if !m.Reserve(900 * mib) {
		t.Fatal("refused")
	}
	f.set(func(f *fakeDisk) { f.avail = uint64(1900 * mib) }) // other data grew
	m.sample()
	if !m.on.Load() {
		t.Fatal("reservations past the budget did not trip the gate at the sample")
	}
	if m.Reserve(1) {
		t.Fatal("a request was admitted while writes are refused")
	}
	f.set(func(f *fakeDisk) { f.avail = uint64(2048 * mib) })
	f.advance(time.Hour)
	m.sample()
	if !m.Pressure() {
		t.Fatal("cleared while free space less reservations is below the off-point")
	}
	m.Release(900 * mib)
	m.sample()
	if m.Pressure() {
		t.Fatal("did not clear once released")
	}
	if m.Reserve(1) == false {
		t.Fatal("refused after clearing")
	}
}

// While the gate is on and cannot clear, the active WAL file is rotated so
// the purge can take what it holds, at most every diskRotateEvery.
func TestDiskMonitorRotatesTheActiveFileWhileStuck(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30), dir: 1001 * mib}
	var rotations atomic.Int32
	m := newFakeMonitor(t, f, DiskMonitorConfig{
		MaxDirectoryBytes: 1000 * mib,
		ActiveBytes:       func() int64 { return 50 * mib },
		Rotate:            func() error { rotations.Add(1); return nil },
	})
	m.sample()
	if !m.Pressure() {
		t.Fatal("not on at the cap")
	}
	m.sample()
	eventually(t, func() bool { return rotations.Load() == 1 && !m.rotating.Load() })
	f.advance(diskRotateEvery - time.Millisecond)
	m.sample()
	time.Sleep(50 * time.Millisecond) // a rotation runs on its own goroutine
	if rotations.Load() != 1 {
		t.Fatal("rotated again inside diskRotateEvery")
	}
	f.advance(time.Millisecond)
	m.sample()
	eventually(t, func() bool { return rotations.Load() == 2 })
}

func TestDiskMonitorDoesNotRotateWhenClearOrEmpty(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30), dir: 1001 * mib}
	var rotations atomic.Int32
	active := int64(WALFileHeaderSize)
	m := newFakeMonitor(t, f, DiskMonitorConfig{
		MaxDirectoryBytes: 1000 * mib,
		ActiveBytes:       func() int64 { return active },
		Rotate:            func() error { rotations.Add(1); return nil },
	})
	m.sample()
	m.sample() // on, but the active file holds only its header
	f.set(func(f *fakeDisk) { f.dir = 100 * mib })
	active = 50 * mib
	f.advance(time.Hour)
	m.sample() // clears
	m.sample()
	time.Sleep(20 * time.Millisecond)
	if rotations.Load() != 0 {
		t.Fatalf("%d rotations", rotations.Load())
	}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// The trip log names what tripped: the cap, not free space.
func TestDiskPressureLogNamesTheCause(t *testing.T) {
	var buf strings.Builder
	var mu sync.Mutex
	f := &fakeDisk{avail: uint64(50 << 30), dir: 1001 * mib}
	m := newFakeMonitor(t, f, DiskMonitorConfig{MaxDirectoryBytes: 1000 * mib})
	m.logger = zerolog.New(writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return buf.Write(p) }))
	m.sample()
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(buf.String(), "wal.max_directory_mb") {
		t.Fatalf("trip log does not name the cap:\n%s", buf.String())
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// Quarantined files are never deleted: they must not hold the cap's gate on.
func TestCapDirectoryBytesExcludesQuarantinedFiles(t *testing.T) {
	dir := t.TempDir()
	for name, n := range map[string]int{
		"arc-1.wal":            10,
		"arc-2.wal.failed":     100,
		"arc-3.wal.123.failed": 1000,
		ReserveFileName:        10000,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := capDirectoryBytes(dir); err != nil || got != 10 {
		t.Fatalf("capDirectoryBytes = %d, %v; want 10", got, err)
	}
	if got, _ := DirectoryBytes(dir); got != 1110 {
		t.Fatalf("DirectoryBytes = %d, want 1110 (the gauge still shows quarantined files)", got)
	}
}

// A flush checkpoint enqueued by the blocking path is charged too: it lands
// on the same volume.
func TestBlockingCheckpointBytesAreCharged(t *testing.T) {
	w, err := NewWriter(&WriterConfig{WALDir: t.TempDir(), SyncMode: SyncModeAsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	hashes, err := w.AppendRawWithMetaTracked("db", []byte{0x90})
	if err != nil {
		t.Fatal(err)
	}
	before := w.enqueuedBytes.Load()
	if err := w.MarkFlushedContext(context.Background(), hashes); err != nil {
		t.Fatal(err)
	}
	if w.enqueuedBytes.Load() <= before {
		t.Fatal("a checkpoint enqueued by the blocking path was not charged")
	}
}

// Under the directory cap, reservations count against clearing too.
func TestDiskCapClearingCountsReservations(t *testing.T) {
	f := &fakeDisk{avail: uint64(50 << 30), dir: 700 * mib}
	m := newFakeMonitor(t, f, DiskMonitorConfig{MaxDirectoryBytes: 1000 * mib})
	m.sample()
	if !m.Reserve(100 * mib) {
		t.Fatal("refused under the cap")
	}
	m.trip(causeCap)
	f.set(func(f *fakeDisk) { f.dir = 750 * mib })
	f.advance(time.Hour)
	m.sample() // 750 + 100 reserved > 1000 - 200 band
	if !m.on.Load() {
		t.Fatal("cleared with directory bytes plus reservations inside the band")
	}
	f.set(func(f *fakeDisk) { f.dir = 700 * mib })
	m.sample()
	if m.on.Load() {
		t.Fatal("did not clear at the band")
	}
}
