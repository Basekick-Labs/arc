package originstream

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// parkFsync makes the store's fsyncs wait on release, announcing each on
// started.
func parkFsync(s *Store) (started chan *os.File, release chan struct{}) {
	started, release = make(chan *os.File, 16), make(chan struct{})
	s.fsync = func(f *os.File) error {
		started <- f
		<-release
		return f.Sync()
	}
	return started, release
}

func within(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s blocked behind an fsync", what)
	}
}

// #1287: the receiving session appends while a Sync fsyncs; a Sync never
// holds the store lock across the disk.
func TestAppendDoesNotWaitForSync(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	started, release := parkFsync(s)
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	<-started
	within(t, "Append", func() {
		if err := s.Append("p", ent(1, "db/a", 2)); err != nil {
			t.Error(err)
		}
	})
	within(t, "AcceptFloors", func() {
		if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 1}}}); err != nil {
			t.Error(err)
		}
	})
	within(t, "Stats", func() { _ = s.Stats() })
	close(release)
	if err := <-synced; err != nil {
		t.Fatal(err)
	}
}

// A second Sync (the session's last one, or Close) returns only once the
// first one's fsyncs are done: otherwise it finds nothing dirty and claims
// durability early.
func TestOverlappingSyncWaits(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	started, release := parkFsync(s)
	go func() { _ = s.Sync() }()
	<-started
	var firstDone atomic.Bool
	second := make(chan bool, 1)
	go func() { _ = s.Sync(); second <- firstDone.Load() }()
	select {
	case <-second:
		t.Fatal("a second Sync returned while the first one's fsync was still running")
	case <-time.After(200 * time.Millisecond):
	}
	firstDone.Store(true)
	close(release)
	if !<-second {
		t.Fatal("the second Sync returned before the first finished")
	}
}

// Floors apply only once their file is durable: a restart reads back what
// Trim trimmed against.
func TestFloorsApplyOnceDurable(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 1}}}); err != nil {
		t.Fatal(err)
	}
	applied := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.instances[instanceID{"p", 1}].floors != nil
	}
	if applied() {
		t.Fatal("floors applied before they were written")
	}
	started, release := parkFsync(s)
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	f := <-started // the floors file's fsync (nothing else is dirty)
	if filepath.Base(f.Name()) != floorsFile+".tmp" {
		t.Fatalf("first fsync was %s, want the floors file", f.Name())
	}
	if applied() {
		t.Fatal("floors applied while their file was not durable")
	}
	close(release)
	if err := <-synced; err != nil {
		t.Fatal(err)
	}
	if !applied() {
		t.Fatal("floors not applied after the Sync wrote them")
	}
}

// A rolled-over segment is synced and closed by the next Sync, not by the
// append that rolled it; a drop before that Sync closes it unsynced.
func TestRollDefersTheOldSegmentsSync(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 1 // one record per segment
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	started, release := parkFsync(s)
	within(t, "a rolling Append", func() {
		if err := s.Append("p", ent(1, "db/a", 2)); err != nil {
			t.Error(err)
		}
	})
	s.mu.Lock()
	n := len(s.retired)
	s.mu.Unlock()
	if n != 1 {
		t.Fatalf("retired files = %d, want the rolled-over segment", n)
	}
	close(release)
	go func() {
		for range started {
		}
	}()
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	n = len(s.retired)
	s.mu.Unlock()
	if n != 0 {
		t.Fatalf("retired files after Sync = %d", n)
	}
	// Roll again, then drop before any Sync: the retired file is closed with
	// its instance, not left open.
	s.fsync = nil
	if err := s.Append("p", ent(1, "db/a", 3)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	n = len(s.retired)
	s.mu.Unlock()
	if n != 1 {
		t.Fatalf("retired files = %d, want 1", n)
	}
	if err := s.DropInstance("p", 1); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	n = len(s.retired)
	s.mu.Unlock()
	if n != 0 {
		t.Fatal("a dropped instance's retired file stayed open")
	}
}

// Trim, eviction and drop rename under the lock and remove after it; a
// restart deletes what a crash left renamed.
func TestRemovalsAfterTheLock(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 64)
	s.segBytes = 1
	s.SetCurrent("p", 2) // instance 1 is replaced: the cap evicts it first
	for c := uint64(1); c <= 3; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	segs := func(pat string) []string {
		m, _ := filepath.Glob(filepath.Join(dir, "p", "*", pat))
		return m
	}
	before := segs("seg-*.recv")
	if len(before) == 0 {
		t.Fatal("no segments")
	}
	// Instance 2's appends pass the cap: instance 1 is evicted, on the
	// appending goroutine, which only renames.
	for c := uint64(1); c <= 3; c++ {
		if err := s.Append("p", ent(2, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	if left := segs("seg-*" + trimSuffix); len(left) == 0 {
		t.Fatal("eviction removed files on the appending goroutine (nothing renamed)")
	}
	for _, p := range before {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("evicted segment %s still under its name", p)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if left := segs("*.trim"); len(left) != 0 {
		t.Fatalf("renamed files not removed by Sync: %v", left)
	}

	// A crash between the rename and the removal: open deletes the leftovers
	// and loads the rest.
	live := segs("seg-*.recv")
	if len(live) == 0 {
		t.Fatal("no live segment")
	}
	if err := os.WriteFile(live[0]+".x"+trimSuffix, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	gone := filepath.Join(dir, "p", "00000000000000ff"+trimSuffix)
	if err := os.MkdirAll(gone, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gone, "seg-00000000.recv"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := openTestStore(t, dir, 64)
	if left := segs("*.trim"); len(left) != 0 {
		t.Fatalf("open left renamed files: %v", left)
	}
	if _, err := os.Stat(gone); !os.IsNotExist(err) {
		t.Fatal("open left a dropped instance directory")
	}
	if r.Stats().Segments != len(live) {
		t.Fatalf("segments after reopen = %d, want %d", r.Stats().Segments, len(live))
	}
}

// A failed fsync may have lost the segment's unsynced pages, and a later
// fsync would not say so: the next append starts a new segment.
func TestFsyncErrorRollsTheSegment(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	s.fsync = func(*os.File) error { return os.ErrInvalid }
	if err := s.Sync(); err == nil {
		t.Fatal("Sync hid an fsync error")
	}
	s.fsync = nil
	if err := s.Append("p", ent(1, "db/a", 2)); err != nil {
		t.Fatal(err)
	}
	if got := s.Stats().Segments; got != 2 {
		t.Fatalf("segments = %d, want a new one after the failed fsync", got)
	}
}

// A write error keeps the segment's earlier records for the next Sync
// instead of closing the file unsynced.
func TestWriteErrorRetiresTheSegment(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	d := s.instances[instanceID{"p", 1}]
	path := d.segs[0].path
	ro, err := os.Open(path) // writes fail on it
	if err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	_ = d.cur.Close()
	d.cur = ro
	s.mu.Unlock()
	if err := s.Append("p", ent(1, "db/a", 2)); err == nil {
		t.Fatal("append to a read-only file succeeded")
	}
	s.mu.Lock()
	_, retired := s.retired[d.segs[0]]
	s.mu.Unlock()
	if !retired {
		t.Fatal("the segment of a failed write was closed instead of synced")
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

// A floors write that fails stays queued: the next Sync writes it. A floors
// slot stuck after a failure would stall every trim.
func TestFloorsWriteRetried(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 1}}}); err != nil {
		t.Fatal(err)
	}
	s.fsync = func(*os.File) error { return os.ErrInvalid }
	_ = s.Sync()
	s.fsync = nil
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	ok := s.instances[instanceID{"p", 1}].floors != nil
	s.mu.Unlock()
	if !ok {
		t.Fatal("floors whose write failed were never written again")
	}
}

// Trim and DropInstance remove their files themselves, after releasing the
// store lock.
func TestTrimAndDropRemoveAfterUnlock(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 1
	var removed []string
	locked := false
	s.removeAll = func(p string) error {
		if s.mu.TryLock() {
			s.mu.Unlock()
		} else {
			locked = true
		}
		removed = append(removed, filepath.Base(p))
		return os.RemoveAll(p)
	}
	for c := uint64(1); c <= 2; c++ {
		if err := s.Append("p", ent(1, "db/a", c)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AcceptFloors("p", &Floors{Instance: 1, AppliedIndex: 9, Keys: map[string]KeyFloor{"db/a": {Floor: 2}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	removed = nil
	if _, err := s.Trim(context.Background(), allReady); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 {
		t.Fatalf("Trim removed %v, want its two segments", removed)
	}
	if err := s.Append("p", ent(7, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	removed = nil
	if err := s.DropInstance("p", 7); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || !strings.HasPrefix(removed[0], "0000000000000007.") || !strings.HasSuffix(removed[0], trimSuffix) {
		t.Fatalf("DropInstance removed %v, want its renamed directory", removed)
	}
	if locked {
		t.Fatal("a removal ran under the store lock")
	}
}

// #1287 review B1: a roll while a Sync is in flight must not close the file
// that Sync still owes an fsync (it cleared dirty before syncing). Sync
// fails rather than claiming durability when a file it syncs was closed for
// any other reason than its segment's removal.
func TestRollDuringSyncKeepsTheFileForIt(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	s.segBytes = 1
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	var errs []error
	started, release := make(chan struct{}, 4), make(chan struct{})
	s.fsync = func(f *os.File) error {
		started <- struct{}{}
		<-release
		err := f.Sync()
		errs = append(errs, err)
		return err
	}
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	<-started
	if err := s.Append("p", ent(1, "db/a", 2)); err != nil { // rolls
		t.Fatal(err)
	}
	close(release)
	if err := <-synced; err != nil {
		t.Fatal(err)
	}
	for _, err := range errs {
		if err != nil {
			t.Fatalf("an fsync the Sync owed found its file closed: %v", err)
		}
	}
}

// A segment dropped while its fsync is in flight is no longer needed: that
// fsync's closed-file error is not a Sync failure.
func TestDropDuringSyncIsNotAFailure(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 4), make(chan struct{})
	s.fsync = func(f *os.File) error {
		started <- struct{}{}
		<-release
		return os.ErrClosed // the drop below closed it
	}
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	<-started
	if err := s.DropInstance("p", 1); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-synced; err != nil {
		t.Fatalf("Sync failed on a dropped segment: %v", err)
	}
}

// Close stops appends: a session still running past its stop must not
// retire files nobody will sync or close.
func TestAppendAfterClose(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("p", ent(1, "db/a", 1)); err == nil {
		t.Fatal("append after Close succeeded")
	}
}

// A file closed under a Sync for any reason other than its segment's
// removal is a failure: Sync must not report durability it did not get.
func TestUnexpectedCloseFailsSync(t *testing.T) {
	s := openTestStore(t, t.TempDir(), 0)
	if err := s.Append("p", ent(1, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan *os.File, 4), make(chan struct{})
	s.fsync = func(f *os.File) error {
		started <- f
		<-release
		return f.Sync()
	}
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	f := <-started
	_ = f.Close() // not through a removal
	close(release)
	if err := <-synced; err == nil {
		t.Fatal("Sync reported success for a file closed under it")
	}
}

// A drop succeeds even when an earlier removal of the same directory left
// it renamed but not deleted.
func TestDropWithALeftoverTrimDir(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	if err := s.Append("p", ent(7, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	inst := filepath.Join(dir, "p", "0000000000000007")
	left := inst + trimSuffix
	if err := os.MkdirAll(left, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(left, "x"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.DropInstance("p", 7); err != nil {
		t.Fatalf("drop with a leftover renamed directory: %v", err)
	}
}

// Floors taken by a Sync for an instance dropped (and made again) before
// they are written are not written into the new directory.
func TestFloorsOfADroppedInstanceNotWritten(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir, 0)
	if err := s.Append("p", ent(7, "db/a", 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.AcceptFloors("p", &Floors{Instance: 7, AppliedIndex: 1, Keys: map[string]KeyFloor{"db/a": {Floor: 1}}}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}, 8), make(chan struct{})
	s.fsync = func(f *os.File) error {
		started <- struct{}{}
		<-release
		return f.Sync()
	}
	synced := make(chan error, 1)
	go func() { synced <- s.Sync() }()
	<-started // the segment's fsync; the floors come after it
	if err := s.DropInstance("p", 7); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("p", ent(7, "db/a", 5)); err != nil { // the same instance again
		t.Fatal(err)
	}
	close(release)
	if err := <-synced; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "p", "0000000000000007", floorsFile)); err == nil {
		t.Fatal("a dropped instance's floors were written into its new directory")
	}
}
