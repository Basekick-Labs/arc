package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
)

// #1207 d1: the outbox applies DELETE records to pending files.

type recordHooks struct {
	mu       sync.Mutex
	recs     []raft.DeleteRecord
	straddle bool
	result   DeleteRewriteResult
	rwErr    error
	rewrites []string
	settled  []raft.SettleDeletedPayload
	settleE  error
	removed  []string
	instance uint64
}

func (h *recordHooks) hooks() *DeleteRecordHooks {
	return &DeleteRecordHooks{
		Records: func(_ []raft.InstanceCoverage, after uint64) ([]raft.DeleteRecord, bool) {
			h.mu.Lock()
			defer h.mu.Unlock()
			var out []raft.DeleteRecord
			for _, r := range h.recs {
				if r.ID > after {
					out = append(out, r)
				}
			}
			return out, h.straddle
		},
		Rewrite: func(_ context.Context, path string, wheres []string) (DeleteRewriteResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.rewrites = append(h.rewrites, path)
			return h.result, h.rwErr
		},
		Settle: func(_ context.Context, p raft.SettleDeletedPayload) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.settleE != nil {
				return h.settleE
			}
			h.settled = append(h.settled, p)
			return nil
		},
		Instance: func() uint64 { return h.instance },
		Remove: func(_ context.Context, path string) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.removed = append(h.removed, path)
			return nil
		},
	}
}

func (h *recordHooks) set(fn func(h *recordHooks)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fn(h)
}

func (h *recordHooks) get(fn func(h *recordHooks)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fn(h)
}

const rwPath = "db/cpu/2026/10/11/10/f_rewrite.parquet"

func recordsOutbox(t *testing.T, h *recordHooks, m *fakeOutboxManifest) (*RegistrationOutbox, *settleLog) {
	t.Helper()
	r := newTestOutbox(t, t.TempDir())
	r.SetDeleteRecordHooks(h.hooks())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, rwPath: 6}}, "writer-1")
	return r, l
}

func TestOutboxRewritesAFileADeleteRecordHolds(t *testing.T) {
	h := &recordHooks{recs: []raft.DeleteRecord{{ID: 40, Where: "v > 1"}}, instance: 9,
		result: DeleteRewriteResult{Matched: 2, Left: 3, NewPath: rwPath, Size: 6, SHA256: "def"}}
	m := newFakeOutboxManifest()
	_, l := recordsOutbox(t, h, m)
	if got := l.wait(t, rwPath); got != ingest.CoverageRegistered {
		t.Fatalf("rewrite settled %v", got)
	}
	if m.isRegistered(outPathA) || !m.isRegistered(rwPath) {
		t.Fatal("the original registered, or the rewrite not")
	}
	m.mu.Lock()
	seen, sup := m.deleteSeen[rwPath], m.supersedes
	m.mu.Unlock()
	if seen != 40 || len(sup) != 1 || sup[0] != outPathA {
		t.Fatalf("delete seen %d supersedes %v", seen, sup)
	}
	h.get(func(h *recordHooks) {
		if len(h.removed) != 1 || h.removed[0] != outPathA || len(h.rewrites) != 1 {
			t.Fatalf("removed %v rewrites %v", h.removed, h.rewrites)
		}
	})
	if len(l.outcomes(outPathA)) != 0 {
		t.Fatal("the replaced path settled")
	}
}

func TestOutboxBumpsDeleteSeenWhenNothingMatches(t *testing.T) {
	h := &recordHooks{recs: []raft.DeleteRecord{{ID: 40}, {ID: 41}}, result: DeleteRewriteResult{Left: 5}}
	m := newFakeOutboxManifest()
	_, l := recordsOutbox(t, h, m)
	if got := l.wait(t, outPathA); got != ingest.CoverageRegistered {
		t.Fatalf("settled %v", got)
	}
	m.mu.Lock()
	seen := m.deleteSeen[outPathA]
	m.mu.Unlock()
	if seen != 41 {
		t.Fatalf("delete seen %d", seen)
	}
	h.get(func(h *recordHooks) {
		if len(h.removed) != 0 {
			t.Fatalf("removed %v", h.removed)
		}
	})
}

// Every row deleted: the file is withdrawn. Batches of this process settle
// through its floors; another instance's through the manifest.
func TestOutboxWithdrawsAFileADeleteEmptied(t *testing.T) {
	for _, own := range []bool{true, false} {
		h := &recordHooks{recs: []raft.DeleteRecord{{ID: 40}}, result: DeleteRewriteResult{Matched: 5}, instance: 1}
		if own {
			h.instance = 9
		}
		m := newFakeOutboxManifest()
		_, l := recordsOutbox(t, h, m)
		if got := l.wait(t, outPathA); got != ingest.CoverageWithdrawn {
			t.Fatalf("own=%v settled %v", own, got)
		}
		if m.isRegistered(outPathA) {
			t.Fatal("registered")
		}
		h.get(func(h *recordHooks) {
			if own && len(h.settled) != 0 {
				t.Fatalf("own batches settled through the manifest: %+v", h.settled)
			}
			if !own && (len(h.settled) != 1 || h.settled[0].By != "writer-1" || h.settled[0].Coverage.Instance != 9 ||
				len(h.settled[0].Records) != 1 || h.settled[0].Records[0] != 40) {
				t.Fatalf("settled %+v", h.settled)
			}
			if len(h.removed) != 1 {
				t.Fatalf("removed %v", h.removed)
			}
		})
	}
}

func TestOutboxHoldsAFileItCannotRewrite(t *testing.T) {
	for name, set := range map[string]func(h *recordHooks){
		"straddles":     func(h *recordHooks) { h.straddle = true },
		"rewrite fails": func(h *recordHooks) { h.rwErr = errors.New("disk") },
		"busy":          func(h *recordHooks) { h.rwErr = ErrDeleteRewriteBusy },
		"settle fails":  func(h *recordHooks) { h.result, h.settleE = DeleteRewriteResult{Matched: 5}, errors.New("no leader") },
	} {
		h := &recordHooks{recs: []raft.DeleteRecord{{ID: 40}}, result: DeleteRewriteResult{Left: 5}, instance: 1}
		set(h)
		m := newFakeOutboxManifest()
		r, l := recordsOutbox(t, h, m)
		if h.straddle {
			time.Sleep(200 * time.Millisecond) // many attempts at the test backoff
		} else {
			waitOutbox(t, name+": attempts", func() bool {
				n := 0
				h.get(func(h *recordHooks) { n = len(h.rewrites) })
				return n >= 2
			})
		}
		if m.isRegistered(outPathA) || len(l.outcomes(outPathA)) != 0 || r.Pending() != 1 {
			t.Fatalf("%s: registered or settled while it could not be rewritten", name)
		}
		h.get(func(h *recordHooks) {
			if h.straddle && len(h.rewrites) != 0 {
				t.Fatalf("a straddling file was rewritten: %v", h.rewrites)
			}
		})
		// Fixed: it goes.
		h.set(func(h *recordHooks) {
			h.straddle, h.rwErr, h.settleE, h.result = false, nil, nil, DeleteRewriteResult{Left: 5}
		})
		if got := l.wait(t, outPathA); got != ingest.CoverageRegistered {
			t.Fatalf("%s: settled %v", name, got)
		}
	}
}

// No hooks (not wired yet): a file the manifest refuses for a DELETE record
// waits, pending, instead of being parked.
func TestOutboxDeletedRefusalWaits(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	m := newFakeOutboxManifest()
	m.refuseKind = raft.RefusedDeleted
	m.refuse = func(string) bool { return true }
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, rwPath: 6}}, "writer-1")
	waitOutbox(t, "refusals", func() bool { return len(m.batchLog()) >= 3 })
	if r.Pending() != 1 || len(l.outcomes(outPathA)) != 0 {
		t.Fatal("a deleted refusal settled or parked the file")
	}
	r.mu.Lock()
	_, parked := r.parked[outPathA]
	r.mu.Unlock()
	if parked {
		t.Fatal("parked")
	}
	// Wired, with the record now visible here: rewritten, registered.
	h := &recordHooks{recs: []raft.DeleteRecord{{ID: 40}}, result: DeleteRewriteResult{Matched: 1, Left: 4, NewPath: rwPath, Size: 6, SHA256: "x"}}
	m.mu.Lock()
	m.refuse = func(p string) bool { return p == outPathA }
	m.mu.Unlock()
	r.SetDeleteRecordHooks(h.hooks())
	if got := l.wait(t, rwPath); got != ingest.CoverageRegistered {
		t.Fatalf("settled %v", got)
	}
}

func TestOutboxWithdrawDatabase(t *testing.T) {
	r := newTestOutbox(t, t.TempDir())
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	r.RegisterFile("other", "cpu", outPathB, time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), 10, "abc")
	if err := r.WithdrawDatabase(context.Background(), "db", 0); err != nil {
		t.Fatal(err)
	}
	if got := l.wait(t, outPathA); got != ingest.CoverageWithdrawn {
		t.Fatalf("settled %v", got)
	}
	if !r.IsPending(outPathB) || r.IsPending(outPathA) {
		t.Fatal("withdrew the wrong database")
	}
}

// A pending file a DELETE emptied: another process's batches settle as
// deleted by its record, this process's own do not; a DELETE's own rewrite
// is marked as written with its record.
func TestOutboxWithdrawForDelete(t *testing.T) {
	for _, own := range []bool{false, true} {
		h := &recordHooks{instance: 1}
		if own {
			h.instance = 9
		}
		r := newTestOutbox(t, t.TempDir())
		r.SetDeleteRecordHooks(h.hooks())
		l := &settleLog{}
		registerCovered(r, outPathA, 10, testCov(l))
		if ok, _, err := r.WithdrawForDelete(context.Background(), outPathA, 40); !ok || err != nil {
			t.Fatalf("withdraw %v %v", ok, err)
		}
		if got := l.wait(t, outPathA); got != ingest.CoverageWithdrawn {
			t.Fatalf("settled %v", got)
		}
		h.get(func(h *recordHooks) {
			if own != (len(h.settled) == 0) || (!own && (h.settled[0].Records[0] != 40 || h.settled[0].Coverage.Instance != 9)) {
				t.Fatalf("own=%v settled %+v", own, h.settled)
			}
		})
	}
	r := newTestOutbox(t, t.TempDir())
	registerCovered(r, outPathA, 10, testCov(&settleLog{}))
	r.SetDeleteSeen(outPathA, 40)
	r.SetDeleteSeen(outPathA, 30)
	r.mu.Lock()
	seen := r.byPath[outPathA].rec.DeleteSeen
	r.mu.Unlock()
	if seen != 40 {
		t.Fatalf("delete seen %d", seen)
	}
}

// While a DELETE settles a pending file it withdraws, no attempt takes it:
// it is never registered.
func TestOutboxWithdrawForDeleteHoldsTheEntry(t *testing.T) {
	h := &recordHooks{instance: 1}
	release := make(chan struct{})
	entered := make(chan struct{})
	hooks := h.hooks()
	hooks.Settle = func(context.Context, raft.SettleDeletedPayload) error {
		close(entered)
		<-release
		return nil
	}
	r := newTestOutbox(t, t.TempDir())
	r.SetDeleteRecordHooks(hooks)
	l := &settleLog{}
	registerCovered(r, outPathA, 10, testCov(l))
	done := make(chan error, 1)
	go func() {
		_, _, err := r.WithdrawForDelete(context.Background(), outPathA, 40)
		done <- err
	}()
	<-entered
	m := newFakeOutboxManifest()
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10}}, "writer-1")
	time.Sleep(100 * time.Millisecond)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := l.wait(t, outPathA); got != ingest.CoverageWithdrawn {
		t.Fatalf("settled %v", got)
	}
	if len(m.batchLog()) != 0 || m.isRegistered(outPathA) {
		t.Fatalf("an attempt took the held entry: %v", m.batchLog())
	}
}
