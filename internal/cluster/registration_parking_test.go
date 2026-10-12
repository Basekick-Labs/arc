package cluster

// #1207 S4b: a registration the stream takeover rules refuse is parked, not
// deleted and not left in the query path.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/rs/zerolog"
)

// takeoverManifest is the fake manifest plus a takeover reader.
type takeoverManifest struct {
	*fakeOutboxManifest
	tmu      sync.Mutex
	covered  func(inst uint64, p raft.CoveragePiece) bool
	covering []raft.FileEntry
	gen      uint64
	syncErr  error
}

func (m *takeoverManifest) CoverageCovered(inst uint64, _ string, p raft.CoveragePiece, _ time.Time) bool {
	m.tmu.Lock()
	defer m.tmu.Unlock()
	return m.covered != nil && m.covered(inst, p)
}

func (m *takeoverManifest) UncoveredBatches(inst uint64, _ string, lo, hi uint64, _ time.Time) raft.Intervals {
	m.tmu.Lock()
	defer m.tmu.Unlock()
	var out raft.Intervals
	for c := lo; c <= hi; c++ {
		if m.covered == nil || !m.covered(inst, raft.CoveragePiece{CSeq: c}) {
			out = append(out, [2]uint64{c, c})
		}
	}
	return out
}

func (m *takeoverManifest) CoveringFilesOf(string, string, time.Time, uint64, string, raft.Intervals) []raft.FileEntry {
	m.tmu.Lock()
	defer m.tmu.Unlock()
	return m.covering
}

func (m *takeoverManifest) TakeoverGeneration() uint64 {
	m.tmu.Lock()
	defer m.tmu.Unlock()
	return m.gen
}

func (m *takeoverManifest) SyncManifest(ctx context.Context) error {
	m.tmu.Lock()
	defer m.tmu.Unlock()
	return m.syncErr
}

func (m *takeoverManifest) set(covered func(uint64, raft.CoveragePiece) bool, covering []raft.FileEntry) {
	m.tmu.Lock()
	m.covered, m.covering = covered, covering
	m.gen++
	m.tmu.Unlock()
}

// parkingRig: an outbox over a real directory as local storage, outPathA
// written there and refused by the manifest.
func parkingRig(t *testing.T) (dir, base string, r *RegistrationOutbox, m *takeoverManifest, l *settleLog) {
	t.Helper()
	dir, base = t.TempDir(), t.TempDir()
	r = newTestOutbox(t, dir)
	r.SetLocalParking(LocalParking{Base: base})
	writeData(t, base, outPathA, 10)
	l = &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	m = &takeoverManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	m.refuse = func(p string) bool { return p == outPathA }
	return dir, base, r, m, l
}

func writeData(t *testing.T, base, rel string, size int) {
	t.Helper()
	p := filepath.Join(base, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
}

func exists(base, rel string) bool {
	_, err := os.Stat(filepath.Join(base, filepath.FromSlash(rel)))
	return err == nil
}

func TestRefusedRegistrationIsParked(t *testing.T) {
	dir, base, r, m, l := parkingRig(t)
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	if got := l.wait(t, outPathA); got != ingest.CoverageRefused {
		t.Fatalf("outcome %v", got)
	}
	if exists(base, outPathA) || !exists(base, "_refused/"+outPathA) {
		t.Fatal("refused file not moved out of the query path")
	}
	if !r.IsParked(outPathA) || !r.HoldsRegistration(outPathA) || r.Pending() != 0 {
		t.Fatalf("parked %v holds %v pending %d", r.IsParked(outPathA), r.HoldsRegistration(outPathA), r.Pending())
	}
	// Held in the WAL while nothing covers its rows.
	if !r.CoverageHold().HoldsAny(9, "db/cpu", 3, 7) {
		t.Fatal("a parked file does not hold its batches")
	}
	// Across a restart: still parked, not pending again.
	crash(r)
	r2 := newTestOutbox(t, dir)
	if !r2.IsParked(outPathA) || r2.Pending() != 0 || !r2.CoverageHold().HoldsAny(9, "db/cpu", 3, 7) {
		t.Fatalf("after restart: parked %v pending %d", r2.IsParked(outPathA), r2.Pending())
	}
}

// Crash after the refused record, before the move and the ack: the next
// start finishes the move, and the refused record wins over the pending add.
func TestParkingCompletesAfterACrash(t *testing.T) {
	dir, base := t.TempDir(), t.TempDir()
	r := newTestOutbox(t, dir)
	writeData(t, base, outPathA, 10)
	registerCovered(r, outPathA, 10, testCov(&settleLog{}))
	if err := r.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	if err := r.appendRefused(parkedRecord{Op: parkOpPark, Rec: r.byPath[outPathA].rec, Kind: raft.RefusedClaimed,
		Held: "_refused/" + outPathA, At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	crash(r)

	r2 := newTestOutbox(t, dir)
	if r2.Pending() != 0 || !r2.IsParked(outPathA) {
		t.Fatalf("pending %d parked %v", r2.Pending(), r2.IsParked(outPathA))
	}
	var dropped []string
	r2.SetLocalParking(LocalParking{Base: base, DropTiering: func(_ context.Context, p string) error {
		dropped = append(dropped, p)
		return nil
	}})
	if exists(base, outPathA) || !exists(base, "_refused/"+outPathA) {
		t.Fatal("interrupted move not completed")
	}
	if len(dropped) != 1 || dropped[0] != outPathA {
		t.Fatalf("tiering rows dropped: %v", dropped)
	}
}

// The parked file's rows are all registered elsewhere and this node holds
// the covering file: the parked copy goes, and the group may release its WAL.
func TestParkedFileResolves(t *testing.T) {
	_, base, r, m, l := parkingRig(t)
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}
	r.HandOff(m, store, "writer-1")
	l.wait(t, outPathA)
	ctx := context.Background()

	// Not covered: stays.
	r.resolveParked(ctx, m, m, store)
	if !r.IsParked(outPathA) {
		t.Fatal("resolved while nothing covers it")
	}
	// Covered, but the covering file is not here: stays.
	m.set(func(uint64, raft.CoveragePiece) bool { return true }, []raft.FileEntry{{Path: "db/cpu/2026/10/11/10/other.parquet", SizeBytes: 5}})
	r.resolveParked(ctx, m, m, store)
	if !r.IsParked(outPathA) {
		t.Fatal("resolved without a local copy of the covering file")
	}
	// Covered, partly: stays.
	m.set(func(_ uint64, p raft.CoveragePiece) bool { return p.CSeq != 5 }, nil)
	r.resolveParked(ctx, m, m, store)
	if !r.IsParked(outPathA) || !r.CoverageHold().HoldsAny(9, "db/cpu", 5, 5) || r.CoverageHold().HoldsAny(9, "db/cpu", 3, 4) {
		t.Fatal("partly covered: must stay, holding only the uncovered batch")
	}
	// Covered, but the manifest barrier fails: no decision.
	m.set(func(uint64, raft.CoveragePiece) bool { return true }, []raft.FileEntry{{Path: outPathB, SizeBytes: 20}})
	m.tmu.Lock()
	m.syncErr = errors.New("no leader")
	m.tmu.Unlock()
	r.resolveParked(ctx, m, m, store)
	if !r.IsParked(outPathA) {
		t.Fatal("resolved on a stale manifest")
	}
	m.tmu.Lock()
	m.syncErr = nil
	m.tmu.Unlock()
	r.resolveParked(ctx, m, m, store)
	if r.IsParked(outPathA) || exists(base, "_refused/"+outPathA) {
		t.Fatal("not resolved")
	}
	if got := l.outcomes(outPathA); got[len(got)-1] != ingest.CoverageResolved {
		t.Fatalf("outcomes %v", got)
	}
	if r.HoldsRegistration(outPathA) || r.CoverageHold().HoldsAny(9, "db/cpu", 3, 7) {
		t.Fatal("still held after resolution")
	}
}

// A DELETE of a parked file is refused: its rows come back later.
func TestDeleteOfAParkedFile(t *testing.T) {
	_, _, r, m, l := parkingRig(t)
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	l.wait(t, outPathA)
	ctx := context.Background()
	if outcome, _, err := r.ReplacePending(ctx, outPathA, outPathB, 5, "x"); err != nil || outcome != ReplaceParked {
		t.Fatalf("ReplacePending: %v %v", outcome, err)
	}
	if _, outcome, err := r.Withdraw(ctx, outPathA); !errors.Is(err, ErrRegistrationParked) || outcome != ReplaceParked {
		t.Fatalf("Withdraw: %v %v", outcome, err)
	}
}

// A refusal that cannot be recorded leaves the registration pending.
func TestParkingFailureKeepsTheRegistrationPending(t *testing.T) {
	dir, _, r, m, _ := parkingRig(t)
	if err := os.Mkdir(filepath.Join(dir, RefusedRegistrationJournal), 0o700); err != nil {
		t.Fatal(err) // a directory where the journal goes: appends fail
	}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "refused at least once", func() bool { return len(m.batchLog()) > 0 })
	time.Sleep(50 * time.Millisecond)
	if r.IsParked(outPathA) || r.Pending() != 1 {
		t.Fatalf("parked %v pending %d", r.IsParked(outPathA), r.Pending())
	}
}

// Without local parking (shared storage) the file stays where it is and
// still answers IsPending, so compaction and the reconciler leave it alone.
func TestParkedInPlaceWithoutLocalStorage(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	m := &takeoverManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	m.refuse = func(p string) bool { return p == outPathA }
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	l.wait(t, outPathA)
	if !r.IsParked(outPathA) || !r.IsPending(outPathA) {
		t.Fatal("left in place but not pending")
	}
}

func TestLoadRefusedSkipsOtherClustersAndResolved(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	rec := journalRecord{ClusterName: "test-cluster", Path: "a"}
	other := journalRecord{ClusterName: "other", Path: "b"}
	for _, pr := range []parkedRecord{{Op: parkOpPark, Rec: rec}, {Op: parkOpPark, Rec: other}, {Op: parkOpPark, Rec: journalRecord{ClusterName: "test-cluster", Path: "c"}}, {Op: parkOpResolved, Rec: journalRecord{ClusterName: "test-cluster", Path: "c"}}} {
		if err := r.appendRefused(pr); err != nil {
			t.Fatal(err)
		}
	}
	got, err := loadRefused(filepath.Join(dir, RefusedRegistrationJournal), "test-cluster")
	if err != nil || len(got) != 1 || got["a"] == nil {
		t.Fatalf("loaded %v, %v", got, err)
	}
}

// While the send gate is closed nothing registers; once open it does.
func TestOutboxSendGate(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	open := false
	var mu sync.Mutex
	r.SetSendGate(func() bool { mu.Lock(); defer mu.Unlock(); return open })
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	time.Sleep(300 * time.Millisecond)
	if m.isRegistered(outPathA) {
		t.Fatal("registered through a closed send gate")
	}
	mu.Lock()
	open = true
	mu.Unlock()
	waitOutbox(t, "registered once open", func() bool { return m.isRegistered(outPathA) })
}

// A file parked in place (shared storage: not moved) is never resolved: it
// would stay in the query path with nothing protecting it.
func TestUnmovedParkedFileIsNotResolved(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	m := &takeoverManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	m.refuse = func(p string) bool { return p == outPathA }
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}
	r.HandOff(m, store, "writer-1")
	l.wait(t, outPathA)
	m.set(func(uint64, raft.CoveragePiece) bool { return true }, nil)
	r.resolveParked(context.Background(), m, m, store)
	if !r.IsParked(outPathA) {
		t.Fatal("a parked file left in place was resolved")
	}
}

// A parked entry still pending (its ack and Refused settle not done yet) is
// not resolved: the settle must come first, or the group never releases.
func TestResolveWaitsForTheRefusedSettle(t *testing.T) {
	_, base, r, m, l := parkingRig(t)
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}
	r.HandOff(m, store, "writer-1")
	l.wait(t, outPathA)
	// Simulate the window: the same path pending again while parked.
	r.mu.Lock()
	e := &outboxEntry{rec: r.parked[outPathA].rec.Rec}
	r.byPath[outPathA] = e
	r.mu.Unlock()
	m.set(func(uint64, raft.CoveragePiece) bool { return true }, nil)
	r.resolveParked(context.Background(), m, m, store)
	if !r.IsParked(outPathA) || !exists(base, "_refused/"+outPathA) {
		t.Fatal("resolved while still pending")
	}
}

// The shutdown drain respects the send gate.
func TestStopDrainRespectsTheSendGate(t *testing.T) {
	r, err := NewRegistrationOutbox(t.TempDir(), "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	r.SetSendGate(func() bool { return false })
	register(r, outPathA, 10)
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	r.Stop()
	if m.isRegistered(outPathA) {
		t.Fatal("the shutdown drain registered through a closed send gate")
	}
}

// A crash after the parked copy was removed and before the resolved record:
// the next start drops the record.
func TestCrashBetweenRemoveAndResolvedRecord(t *testing.T) {
	dir, base, r, m, l := parkingRig(t)
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	l.wait(t, outPathA)
	if err := os.Remove(filepath.Join(base, "_refused", filepath.FromSlash(outPathA))); err != nil {
		t.Fatal(err)
	}
	crash(r)
	r2 := newTestOutbox(t, dir)
	r2.SetLocalParking(LocalParking{Base: base})
	if r2.IsParked(outPathA) {
		t.Fatal("a parked file at neither path kept its record")
	}
}

// InstanceFiles lists pending and parked files with the pieces of one
// instance and the claim they publish it under.
func TestInstanceFiles(t *testing.T) {
	_, base, r, m, l := parkingRig(t)
	writeData(t, base, outPathB, 20)
	cov := testCov(&settleLog{})
	cov.Ranges = []ingest.CoverageRange{{Instance: 4, Lo: 1, Hi: 2}, {Instance: 9, Lo: 10, Hi: 12}}
	cov.Partial = []ingest.CoveragePiece{{Instance: 9, CSeq: 20, RowLo: 0, RowHi: 5, RowTotal: 10}}
	cov.Epochs = map[uint64]uint64{9: 77}
	registerCovered(r, outPathB, 20, cov)
	if got := r.InstanceFiles(4); len(got) != 1 || got[0].Path != outPathB || got[0].Epoch != 0 || len(got[0].Ranges) != 1 {
		t.Fatalf("instance 4: %+v", got)
	}
	got := r.InstanceFiles(9)
	if len(got) != 2 || got[1].Path != outPathB || got[1].Epoch != 77 || len(got[1].Partial) != 1 || got[1].Parked || !got[1].Exact {
		t.Fatalf("instance 9 pending: %+v", got)
	}
	m.refuse = func(p string) bool { return p == outPathA }
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}, "writer-1")
	l.wait(t, outPathA)
	waitOutbox(t, "drained", func() bool { return r.Pending() == 0 })
	if err := r.SyncRegistrations(); err != nil { // the ack of B is durable
		t.Fatal(err)
	}
	got = r.InstanceFiles(9)
	if len(got) != 1 || got[0].Path != outPathA || !got[0].Parked || got[0].Kind != raft.RefusedClaimed {
		t.Fatalf("instance 9 after the drain: %+v", got)
	}
	if len(r.InstanceFiles(5)) != 0 {
		t.Fatal("an instance with no files")
	}
}
