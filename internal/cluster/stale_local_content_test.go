package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// runFeed starts the sweep goroutine on the rig's journal directory, as a
// start does, and returns its stop.
func (r *staleRigT) runFeed() func() {
	r.t.Helper()
	r.c.stale.seen.Load().close()
	r.c.stale.seen.Store(nil)
	r.c.stale.opened = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	r.c.stale.wg.Add(1)
	go r.c.staleLocalRun(ctx)
	r.c.waitStaleLocalOpen(context.Background())
	return func() {
		cancel()
		r.c.stopStaleLocal()
	}
}

// restart reopens the journal as a new process would.
func (r *staleRigT) restart() {
	r.t.Helper()
	r.c.stale.seen.Load().close()
	seen, err := openSeenLocal(r.c.cfg.RaftDataDir, r.c.cfg.ClusterName)
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(seen.close)
	r.c.stale.seen.Store(seen)
}

// #1182 as filed: a node holds A at P; the manifest lists B at P, same size;
// the registration reaches the sweep goroutine through the real feed while the
// forced refresh fails; the node restarts. The walk must not trust A.
func TestStaleLocalLiveChangeSurvivesARestartAsStale(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.registerWritten(key, "rows a")
	stop := r.runFeed()
	r.c.noteLocalRegistered(key)
	waitFSM(t, func() bool { _, ok := r.c.stale.seen.Load().get(key); return ok })
	if v := r.walk()[key]; v != filereplication.ListedTrusted {
		t.Fatalf("before the change: %v", v)
	}
	b := raft.FileEntry{Path: key, SHA256: strings.Repeat("c", 64), SizeBytes: 6, Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(b, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	cur, _ := r.c.raftNode.FSM().GetFile(key)
	r.c.noteLocalRegistered(key) // the content-change callback's feed
	waitFSM(t, func() bool { rec, _ := r.c.stale.seen.Load().get(key); return rec.lsn == cur.LSN })
	stop()
	r.restart()
	if v := r.walk()[key]; v != filereplication.ListedStale {
		t.Fatalf("after the restart: %v, want stale", v)
	}
}

// A node upgraded from a release without the journal holds copies it has no
// record of: each is unverified once (the user's "hash once"); a file this
// node wrote and reported is trusted; evidence for another file is not used.
func TestStaleLocalUnrecordedCopiesAreUnverified(t *testing.T) {
	r := staleRig(t)
	legacy := staleHour + "legacy.parquet"
	r.register(legacy, "rows l")
	if v := r.walk()[legacy]; v != filereplication.ListedUnverified {
		t.Fatalf("unrecorded copy: %v", v)
	}
	own := staleHour + "own.parquet"
	r.registerWritten(own, "rows o")
	if v := r.walk()[own]; v != filereplication.ListedTrusted {
		t.Fatalf("reported copy: %v", v)
	}
	// Evidence for a file replaced before it was listed is not taken.
	other := staleHour + "other.parquet"
	r.write(other, "rows x")
	r.c.NoteLocalWritten(other, strings.Repeat("a", 64))
	_ = os.Remove(filepath.Join(r.root, other))
	r.register(other, "rows y")
	if v := r.walk()[other]; v != filereplication.ListedUnverified {
		t.Fatalf("evidence for another file: %v", v)
	}
	if len(r.c.stale.written) != 0 {
		t.Fatalf("evidence left over: %v", r.c.stale.written)
	}
}

// A file replaced under a record with no evidence (a merge restore that wrote
// other bytes and failed to register) is unverified, not trusted.
func TestStaleLocalReplacedFileIsUnverified(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.registerWritten(key, "rows a")
	r.walk()
	r.write(key+".tmp", "rows z")
	_ = os.Rename(filepath.Join(r.root, key+".tmp"), filepath.Join(r.root, key))
	if v := r.walk()[key]; v != filereplication.ListedUnverified {
		t.Fatalf("replaced file: %v", v)
	}
}

// A hash is recorded only for the file that was read, and only for a path the
// manifest lists; what it found is recorded even when it is not the
// manifest's checksum.
func TestStaleLocalRecordLocalContent(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	e, _ := r.c.raftNode.FSM().GetFile(key)
	stamp, ok := r.c.localStampFor(key)
	if !ok {
		t.Fatal("no stamp")
	}
	r.c.recordLocalContent(e, "1:2:3", e.SHA256) // another file
	if v := r.walk()[key]; v != filereplication.ListedUnverified {
		t.Fatalf("a hash of another file was recorded: %v", v)
	}
	r.c.recordLocalContent(e, stamp, strings.Repeat("d", 64))
	if v := r.walk()[key]; v != filereplication.ListedStale {
		t.Fatalf("a mismatching hash: %v", v)
	}
	r.c.recordLocalContent(e, stamp, e.SHA256)
	if v := r.walk()[key]; v != filereplication.ListedTrusted {
		t.Fatalf("a matching hash: %v", v)
	}
	unlisted := staleHour + "u.parquet"
	r.write(unlisted, "rows u")
	r.c.recordLocalContent(&raft.FileEntry{Path: unlisted, SizeBytes: 6, SHA256: e.SHA256}, "", e.SHA256)
	if _, ok := r.c.stale.seen.Load().get(unlisted); ok {
		t.Fatal("a record for a path the manifest does not list")
	}
}

// No tracking (not running, or failed): the size decides, as before.
func TestStaleLocalNoTrackingTrustsTheSize(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.c.stale.seen.Load().close()
	if v := r.walk()[key]; v != filereplication.ListedTrusted {
		t.Fatalf("failed tracking: %v", v)
	}
	r.c.stale.seen.Store(nil)
	if v := r.walk()[key]; v != filereplication.ListedTrusted {
		t.Fatalf("no tracking: %v", v)
	}
}

// cluster.stale_local_sweep=false stops the removals only: copies are still
// tracked for what they hold, and records of files gone from disk are dropped.
func TestStaleLocalSweepOffStillTracks(t *testing.T) {
	r := staleRig(t)
	r.c.cfg.StaleLocalSweep = false
	if !r.c.staleLocalEnabled() {
		t.Fatal("tracking off with the sweep off")
	}
	key, gone := staleHour+"a.parquet", staleHour+"g.parquet"
	r.registerWritten(key, "rows a")
	r.registerWritten(gone, "rows g")
	r.pass()
	r.unregister(key)
	r.unregister(gone)
	_ = os.Remove(filepath.Join(r.root, gone))
	r.pass()
	r.pass()
	if !r.exists(key) {
		t.Fatal("removed with the sweep off")
	}
	if _, ok := r.c.stale.seen.Load().get(gone); ok {
		t.Fatal("record of a file gone from disk kept")
	}
	if st := r.c.StaleLocalStatus(); st == nil || !st.RemovalsOff {
		t.Fatalf("status %+v", st)
	}
}

// The startup walk waits for the journal, with no timer.
func TestWaitStaleLocalOpen(t *testing.T) {
	r := staleRig(t)
	r.c.waitStaleLocalOpen(context.Background()) // not running: returns
	r.c.stale.opened = make(chan struct{})
	done := make(chan struct{})
	go func() { r.c.waitStaleLocalOpen(context.Background()); close(done) }()
	select {
	case <-done:
		t.Fatal("returned before the journal opened")
	case <-time.After(50 * time.Millisecond):
	}
	r.c.stale.markOpened()
	<-done
}

// A v1 journal (whose sha was the listed checksum, not the file's) is dropped.
func TestSeenLocalDropsAV1Journal(t *testing.T) {
	dir := t.TempDir()
	v1 := "arc-seen-local v1 c\na\t" + staleHour + "a.parquet\t6\t1\t2\t3\t4\n"
	if err := os.WriteFile(filepath.Join(dir, seenLocalJournal), []byte(v1), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := openSeenLocal(dir, "c")
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if s.len() != 0 {
		t.Fatalf("v1 records loaded: %d", s.len())
	}
}
