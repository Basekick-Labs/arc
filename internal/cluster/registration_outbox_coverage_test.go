package cluster

// #1207 S1: the outbox journals a file's flush-group coverage and reports how
// each covered file left it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/rs/zerolog"
)

type settleLog struct {
	mu  sync.Mutex
	got map[string][]ingest.CoverageOutcome
}

func (l *settleLog) record(path string, o ingest.CoverageOutcome) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.got == nil {
		l.got = map[string][]ingest.CoverageOutcome{}
	}
	l.got[path] = append(l.got[path], o)
}

func (l *settleLog) outcomes(path string) []ingest.CoverageOutcome {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ingest.CoverageOutcome(nil), l.got[path]...)
}

func (l *settleLog) wait(t *testing.T, path string) ingest.CoverageOutcome {
	t.Helper()
	waitOutbox(t, "settle of "+path, func() bool { return len(l.outcomes(path)) > 0 })
	got := l.outcomes(path)
	if len(got) != 1 {
		t.Fatalf("%s settled %d times: %v", path, len(got), got)
	}
	return got[0]
}

func testCov(l *settleLog) *ingest.GroupCoverage {
	cov := ingest.NewGroupCoverage("g1", "db/cpu", 1, l.record)
	cov.Ranges = []ingest.CoverageRange{{Instance: 9, Lo: 3, Hi: 7}}
	return cov
}

func registerCovered(r *RegistrationOutbox, path string, size int64, cov *ingest.GroupCoverage) {
	r.RegisterCoveredFile("db", "cpu", path, time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), size, "abc", cov)
}

// The coverage survives a restart in the journal (version 2 records), and a
// version-1 reader's view — the record without "cov" — is unchanged.
func TestOutboxJournalsCoverage(t *testing.T) {
	dir := t.TempDir()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	registerCovered(first, outPathA, 10, testCov(&settleLog{}))
	register(first, outPathB, 20)
	if err := first.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(first)
	raw, _ := os.ReadFile(filepath.Join(dir, RegistrationJournal))
	if !strings.Contains(string(raw), `"v":2`) || !strings.Contains(string(raw), `"cov":{"id":"g1","key":"db/cpu","files":1,"ranges":[{"i":9,"lo":3,"hi":7}]}`) {
		t.Fatalf("journal does not carry the coverage:\n%s", raw)
	}
	second := newTestOutbox(t, dir)
	second.mu.Lock()
	a, b := second.byPath[outPathA], second.byPath[outPathB]
	second.mu.Unlock()
	if a == nil || a.rec.Cov == nil || a.rec.Cov.Key != "db/cpu" || len(a.rec.Cov.Ranges) != 1 || a.rec.Cov.Ranges[0].Hi != 7 {
		t.Fatalf("reloaded coverage = %+v", a)
	}
	if b == nil || b.rec.Cov != nil {
		t.Fatalf("an uncovered record came back with coverage: %+v", b)
	}
	// A reloaded coverage has no group to report to: settling it is a no-op.
	a.rec.Cov.Settle(outPathA, ingest.CoverageRegistered)
}

// One over-long line no longer fails the whole journal: it is skipped like
// any line that does not parse, and the records around it load.
func TestOutboxJournalSkipsAnOverlongLine(t *testing.T) {
	dir := t.TempDir()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	register(first, outPathA, 10)
	if err := first.SyncRegistrations(); err != nil {
		t.Fatal(err)
	}
	crash(first)
	f, err := os.OpenFile(filepath.Join(dir, RegistrationJournal), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 2 MiB, past the 1 MiB the previous reader allowed for a whole line.
	_, _ = f.WriteString(`{"op":"add","cluster_name":"test-cluster","path":"x","sha256":"` + strings.Repeat("a", 2<<20) + "\"}\n")
	_, _ = f.WriteString(`{"op":"add","cluster_name":"test-cluster","path":"` + outPathB + `","size_bytes":20}` + "\n")
	_ = f.Close()
	second := newTestOutbox(t, dir)
	if got := second.Pending(); got != 3 {
		t.Fatalf("pending = %d, want 3 (the long line is a valid record and loads too)", got)
	}
}

func TestOutboxSettlesRegistered(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	r.HandOff(newFakeOutboxManifest(), &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	if got := l.wait(t, outPathA); got != ingest.CoverageRegistered {
		t.Fatalf("outcome = %v, want registered", got)
	}
}

// Gone, rewritten, or vanished during the commit: the rows are in no
// registered file, and the group is told so.
func TestOutboxSettlesLost(t *testing.T) {
	dir := t.TempDir()
	first, err := NewRegistrationOutbox(dir, "test-cluster", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	crash(first)
	r := newTestOutbox(t, dir)
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	registerCovered(r, outPathB, 20, testCov(l))
	// Make both "not fresh" so the storage check runs.
	r.mu.Lock()
	for _, e := range r.byPath {
		e.fresh = false
	}
	r.mu.Unlock()
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathB: 20}} // A is gone
	m := newFakeOutboxManifest()
	m.beforeBatch = func([]string) { store.remove(outPathB) } // B vanishes mid-commit
	r.HandOff(m, store, "writer-1")
	if got := l.wait(t, outPathA); got != ingest.CoverageLost {
		t.Fatalf("gone file outcome = %v, want lost", got)
	}
	if got := l.wait(t, outPathB); got != ingest.CoverageLost {
		t.Fatalf("vanished file outcome = %v, want lost", got)
	}
}

// A DELETE that removes a pending file: its rows are gone on purpose.
func TestOutboxSettlesWithdrawn(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	if withdrawn, _, err := r.Withdraw(context.Background(), outPathA); err != nil || !withdrawn {
		t.Fatalf("Withdraw = %v, %v", withdrawn, err)
	}
	if got := l.wait(t, outPathA); got != ingest.CoverageWithdrawn {
		t.Fatalf("outcome = %v, want withdrawn", got)
	}
}

// A DELETE rewrite moves the coverage to the new path: the old path never
// settles, the rewrite settles for the group.
func TestOutboxReplacePendingCarriesCoverage(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	cov := testCov(l)
	registerCovered(r, outPathA, 10, cov)
	if outcome, _, err := r.ReplacePending(context.Background(), outPathA, outPathB, 7, "new"); err != nil || outcome != ReplaceSwapped {
		t.Fatalf("ReplacePending = %v, %v", outcome, err)
	}
	r.mu.Lock()
	moved := r.byPath[outPathB].rec.Cov
	r.mu.Unlock()
	if moved != cov {
		t.Fatal("the rewrite does not carry the group coverage")
	}
	r.HandOff(newFakeOutboxManifest(), &fakeOutboxStorage{sizes: map[string]int64{outPathB: 7}}, "writer-1")
	if got := l.wait(t, outPathB); got != ingest.CoverageRegistered {
		t.Fatalf("rewrite outcome = %v, want registered", got)
	}
	if got := l.outcomes(outPathA); len(got) != 0 {
		t.Fatalf("the replaced path settled: %v", got)
	}
}

// Abandon: nothing in this process registers the pending files or any later
// one, so their groups stop holding the WAL.
func TestOutboxAbandonSettlesCoverage(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	r.Abandon()
	if got := l.wait(t, outPathA); got != ingest.CoverageAbandoned {
		t.Fatalf("pending outcome = %v, want abandoned", got)
	}
	registerCovered(r, outPathB, 20, testCov(l))
	if got := l.wait(t, outPathB); got != ingest.CoverageAbandoned {
		t.Fatalf("post-abandon outcome = %v, want abandoned", got)
	}
}

// Abandon after the hand-off is a no-op and says so: main.go turns coverage
// off only when the outbox was really abandoned.
func TestOutboxAbandonAfterHandOffReportsFalse(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	r.HandOff(newFakeOutboxManifest(), &fakeOutboxStorage{}, "writer-1")
	if r.Abandon() {
		t.Fatal("Abandon after HandOff reported that it abandoned the outbox")
	}
	fresh := newTestOutbox(t, t.TempDir())
	if !fresh.Abandon() {
		t.Fatal("Abandon before HandOff did not report abandoning")
	}
	if fresh.Abandon() {
		t.Fatal("a second Abandon reported abandoning again")
	}
}

// A second flush to the same pending path overwrote the first file: the
// earlier group is told its rows are lost.
func TestOutboxSecondAddToAPathSettlesTheFirstLost(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	first, second := &settleLog{}, &settleLog{}
	registerCovered(r, outPathA, 10, testCov(first))
	registerCovered(r, outPathA, 12, testCov(second))
	if got := first.wait(t, outPathA); got != ingest.CoverageLost {
		t.Fatalf("first outcome = %v, want lost", got)
	}
	if got := second.outcomes(outPathA); len(got) != 0 {
		t.Fatalf("the current registration settled early: %v", got)
	}
}

// laggingManifest commits like the leader would, but this node's FSM does not
// show the commit (and the barrier fails) until caughtUp is set.
type laggingManifest struct {
	*fakeOutboxManifest
	caughtUp atomic.Bool
	syncs    atomic.Int64
}

func (m *laggingManifest) GetFileEntry(path string) (*raft.FileEntry, bool) {
	if !m.caughtUp.Load() {
		return nil, false
	}
	return m.fakeOutboxManifest.GetFileEntry(path)
}

// SyncManifest: the outbox's start-up catch-up (the first call) succeeds;
// barriers after a commit fail until caughtUp.
func (m *laggingManifest) SyncManifest(context.Context) error {
	if m.syncs.Add(1) == 1 || m.caughtUp.Load() {
		return nil
	}
	return errors.New("barrier timed out")
}

// A covered file settles as registered only once this node's FSM shows it:
// the floors it advances are published with this node's applied index
// (#1207 S3). It is not sent again meanwhile.
func TestOutboxCoveredSettlesOnlyOnceVisibleHere(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	m := &laggingManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	// The commit's barrier fails; the file is then only re-checked against
	// this node's FSM, never sent again.
	waitOutbox(t, "the post-commit barrier", func() bool { return m.syncs.Load() >= 2 })
	time.Sleep(100 * time.Millisecond) // several retry rounds at the test backoff
	if got := l.outcomes(outPathA); len(got) != 0 {
		t.Fatalf("settled %v before this node applied the registration", got)
	}
	if n := len(m.batchLog()); n != 1 {
		t.Fatalf("committed file sent %d times, want once", n)
	}
	m.caughtUp.Store(true)
	if got := l.wait(t, outPathA); got != ingest.CoverageRegistered {
		t.Fatalf("outcome = %v, want registered", got)
	}
}

// Uncovered files keep settling at once, without the barrier.
func TestOutboxUncoveredSkipsTheBarrier(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	register(r, outPathA, 10)
	m := &laggingManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "settled", func() bool { return r.Pending() == 0 })
	if n := m.syncs.Load(); n != 1 {
		t.Fatalf("barriers = %d, want only the start-up catch-up", n)
	}
}

// A committed file that compaction or a DELETE removed before this node
// caught up settles once a barrier succeeds: it was registered.
func TestOutboxCommittedThenRemovedSettles(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	m := &laggingManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "the post-commit barrier", func() bool { return m.syncs.Load() >= 2 })
	m.mu.Lock()
	delete(m.registered, outPathA) // compacted away before this FSM saw it
	m.mu.Unlock()
	m.caughtUp.Store(true)
	if got := l.wait(t, outPathA); got != ingest.CoverageRegistered {
		t.Fatalf("outcome = %v, want registered", got)
	}
}

// Registrations carry their exact coverage per instance whether or not
// stream takeover is on (an FSM without it ignores the field; one with it
// must have it from the first file), and only this file's part of a group.
func TestOutboxSendsExactCoverage(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	cov := testCov(&settleLog{})
	cov.Ranges = []ingest.CoverageRange{{Instance: 9, Lo: 3, Hi: 7}, {Instance: 4, Lo: 20, Hi: 30}}
	registerCovered(r, outPathA, 10, cov)
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	waitOutbox(t, "registered", func() bool { return m.isRegistered(outPathA) })
	m.mu.Lock()
	got := fmt.Sprint(m.coverage)
	m.mu.Unlock()
	if got != "[{4 db/cpu [[20 30]] [] false 0} {9 db/cpu [[3 7]] [] false 0}]" {
		t.Fatalf("coverage sent = %s", got)
	}
}

func TestFileCoverageUsesTheFilePartAndCapsItsSize(t *testing.T) {
	cov := testCov(&settleLog{})
	cov.Ranges = []ingest.CoverageRange{{Instance: 9, Lo: 1, Hi: 9}}
	cov.File = &ingest.FileCoverage{Ranges: []ingest.CoverageRange{{Instance: 9, Lo: 4, Hi: 5}},
		Partial: []ingest.CoveragePiece{{Instance: 9, CSeq: 7, Chunk: 1, Chunks: 2}}}
	got := fileCoverage(cov)
	if len(got) != 1 || fmt.Sprint(got[0].Ranges) != "[[4 5]]" || len(got[0].Partial) != 1 || got[0].Partial[0].Chunks != 2 || got[0].Truncated {
		t.Fatalf("file coverage %+v", got)
	}
	cov.File.Omitted = true
	if got := fileCoverage(cov); !got[0].Truncated {
		t.Fatal("an omitted file coverage was sent as exact")
	}
	big := testCov(&settleLog{})
	for i := uint64(0); i < 4000; i++ {
		big.Ranges = append(big.Ranges, ingest.CoverageRange{Instance: 9, Lo: 1e12 + 2*i, Hi: 1e12 + 2*i})
	}
	got = fileCoverage(big)
	if !got[0].Truncated || len(got[0].Ranges) != 0 {
		t.Fatal("a coverage over the size cap was sent in full")
	}
}

// A claim refusal is terminal for the named file only: it settles as
// refused, the file is not deleted, and the rest of the batch registers.
func TestOutboxClaimRefusalSettlesOnlyTheRefusedFile(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	registerCovered(r, outPathB, 20, testCov(l))
	m := newFakeOutboxManifest()
	m.refuse = func(p string) bool { return p == outPathA }
	store := &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}
	r.HandOff(m, store, "writer-1")
	if got := l.wait(t, outPathA); got != ingest.CoverageRefused {
		t.Fatalf("refused file outcome = %v", got)
	}
	if got := l.wait(t, outPathB); got != ingest.CoverageRegistered {
		t.Fatalf("other file outcome = %v", got)
	}
	if m.isRegistered(outPathA) {
		t.Fatal("refused file registered")
	}
	if size, _ := store.StatFile(context.Background(), outPathA); size != 10 {
		t.Fatal("refused file deleted")
	}
	waitOutbox(t, "nothing pending", func() bool { return r.Pending() == 0 })
}
