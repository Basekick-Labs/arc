package ingest

import (
	"context"
	"testing"
	"time"
)

// #1942: ForceFlushKey's reply. Forced carries the task its send created,
// LostRace the detach seen after the due check, at each of the three sites
// where one shows up; NothingToForce is left with "nothing buffered, no
// detach since the due check".

func (f *forceFixture) assertReply(what string, got, want ForceReply) {
	f.t.Helper()
	if got != want {
		f.t.Fatalf("%s: ForceFlushKey = %+v, want %+v", what, got, want)
	}
}

// Due only on NotDue, TaskID only on Forced (the sent task, with tracking off:
// the id comes from the send, not the tracker).
func TestForceReply_DueAndTaskIDByResult1942(t *testing.T) {
	off := newForceFixture(t, forceConfig(2, 100), nil)
	off.write("m", 5)
	off.assertReply("ledger off", off.force("db/m", nil), ForceReply{Result: LedgerOff})

	f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{})
	l := f.buf.ledger.Load()
	f.write("m", 5)
	forced := f.force("db/m", nil)
	d, ok := l.lastDetach("db/m")
	if !ok || d.id == 0 {
		t.Fatal("fixture: the forced send recorded no detach")
	}
	f.assertReply("forced", forced, ForceReply{Result: Forced, TaskID: d.id})
	f.assertReply("not due", f.force("db/m", nil), ForceReply{Result: NotDue, Due: d.at.Add(5 * time.Second)})

	// Due again, nothing buffered: the last detach is the one the due check
	// saw, so no id, though the key has one.
	f.waitFor("the forced slot released", func() bool { return f.inFlight() == 0 })
	f.clock.Advance(6 * time.Second)
	f.assertReply("nothing, detached before", f.force("db/m", nil), ForceReply{Result: NothingToForce})
	f.assertReply("nothing, with want", f.force("db/m", func([]interface{}) bool { return true }), ForceReply{Result: NothingToForce})

	f.write("m", 5)
	f.assertReply("not wanted", f.force("db/m", func([]interface{}) bool { return false }), ForceReply{Result: NotWanted})
	l.noteDeferral()
	f.assertReply("over budget", f.force("db/m", nil), ForceReply{Result: OverBudget})

	// Closing, on a key detached before (so a stray id would show): under the
	// write lock, and at entry after Close.
	c := newForceFixture(t, forceConfig(2, 100), &ForceBudget{})
	c.write("m", 5)
	c.flushAll()
	c.clock.Advance(6 * time.Second)
	c.write("m", 5)
	forceAfterScanHook = func(string) { c.buf.closing.Store(true) }
	c.assertReply("closing under the write lock", c.force("db/m", nil), ForceReply{Result: Closing})
	forceAfterScanHook = nil
	_ = c.buf.Close()
	c.assertReply("closing at entry", c.force("db/m", nil), ForceReply{Result: Closing})
}

// Site 1: the want scan finds the key empty because a detach came after the
// due check (want non-nil: the write side's call). Counted as lost_race, not
// nothing.
func TestForceReply_LostRaceAtTheScan1942(t *testing.T) {
	f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{})
	f.write("m", 5)
	var won *extractedFlush
	forceAfterDueCheckHook = func(k string) { won = f.extract(k) }
	lost, nothing := forcedFlushCount(t, "write", "lost_race"), forcedFlushCount(t, "write", "nothing")
	rep := f.buf.ForceFlushKey("db/m", time.Second, ForceWrite, func([]interface{}) bool {
		t.Error("want ran on an empty key")
		return true
	})
	forceAfterDueCheckHook = nil
	if won == nil || won.taskID == 0 {
		t.Fatal("fixture: nothing extracted")
	}
	f.assertReply("detach before the scan", rep, ForceReply{Result: LostRace, TaskID: won.taskID})
	if got := forcedFlushCount(t, "write", "lost_race"); got != lost+1 {
		t.Fatalf("{write,lost_race} = %d, want %d", got, lost+1)
	}
	if got := forcedFlushCount(t, "write", "nothing"); got != nothing {
		t.Fatalf("{write,nothing} moved %d -> %d on a lost race", nothing, got)
	}
	_ = f.buf.writeExtracted(context.Background(), won, "db/m", "db", "m")
}

// Site 2: the key is empty under the write lock because a detach came after
// the scan.
func TestForceReply_LostRaceUnderTheWriteLock1942(t *testing.T) {
	for _, tc := range []struct {
		name string
		want func([]interface{}) bool
	}{
		{"no want (the read side)", nil},
		{"want accepted", func([]interface{}) bool { return true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{})
			f.write("m", 5)
			var won *extractedFlush
			forceAfterScanHook = func(k string) { won = f.extract(k) }
			rep := f.force("db/m", tc.want)
			forceAfterScanHook = nil
			if won == nil || won.taskID == 0 {
				t.Fatal("fixture: nothing extracted")
			}
			f.assertReply("detach after the scan", rep, ForceReply{Result: LostRace, TaskID: won.taskID})
			_ = f.buf.writeExtracted(context.Background(), won, "db/m", "db", "m")
		})
	}
}

// A pruned entry is no detach at either empty-key site: NothingToForce, no
// id. The key's last detach is older than retain; the prune lands after the
// due check (site 1: before the scan; site 2: after it).
func TestForceReply_PrunedEntryAtAnEmptyKeyIsNothing1942(t *testing.T) {
	for _, tc := range []struct {
		name string
		hook *func(string)
		want func([]interface{}) bool
	}{
		{"at the scan", &forceAfterDueCheckHook, func([]interface{}) bool { return true }},
		{"under the write lock", &forceAfterScanHook, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForceFixture(t, forceConfig(2, 100), &ForceBudget{Retain: 2 * time.Second})
			l := f.buf.ledger.Load()
			f.write("m", 5)
			f.flushAll()
			f.clock.Advance(3 * time.Second)
			if _, ok := l.lastDetach("db/m"); !ok {
				t.Fatal("fixture: no detach recorded")
			}
			*tc.hook = func(string) {
				l.lastPrune = time.Time{}
				if n := l.maybePrune(); n != 1 {
					t.Errorf("hook pruned %d, want 1", n)
				}
			}
			// An interval within retain, as ForceBudget requires.
			rep := f.buf.ForceFlushKey("db/m", time.Second, ForceRead, tc.want)
			*tc.hook = nil
			f.assertReply("pruned, empty", rep, ForceReply{Result: NothingToForce})
		})
	}
}

// H1 of the plan review: two detaches between the due check and the
// re-check. The reply carries the newest (D_b), which is not the one that
// took the waiter's rows (D_a). The waiter follows the tracker, which joined
// D_a: D_b settling alone does not answer it.
func TestWaitKeysFlushed_LostRaceTaskIDIsTheNewestDetach1942(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	f.write("k", 10)
	type pair struct{ a, b *extractedFlush }
	won := make(chan pair, 1)
	forceAfterScanHook = func(k string) {
		if k == "db/k" && len(won) == 0 {
			a := f.extract("db/k") // took the waiter's rows
			f.write("k", 10)
			b := f.extract("db/k")
			f.write("k", 10) // buffered at the re-check: site 3
			won <- pair{a, b}
		}
	}
	replies := make(chan ForceReply, 1)
	waitAfterForceHook = func(_ string, rep ForceReply) { replies <- rep }
	ch := f.startWait(5*time.Second, time.Hour, "db/k")
	p := <-won
	rep := <-replies
	if rep.Result != LostRace || rep.TaskID != p.b.taskID || p.a.taskID == p.b.taskID {
		t.Fatalf("reply %+v, want lost_race with the newest detach %d (the first was %d)", rep, p.b.taskID, p.a.taskID)
	}
	ws := f.waiterStates("db/k")
	if len(ws) != 1 || ws[0].joined != p.a.taskID {
		t.Fatalf("the waiter joined %+v, want the first detach %d", ws, p.a.taskID)
	}
	_ = f.buf.writeExtracted(context.Background(), p.b, "db/k", "db", "k")
	f.assertWaiting(ch, "only the newest detach settled; the one with the waiter's rows has not")
	_ = f.buf.writeExtracted(context.Background(), p.a, "db/k", "db", "k")
	if err := f.waitResult(ch); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
}

// A reader's own force: the reply's id is the task the tracker joined (one
// detach in the window).
func TestWaitKeysFlushed_ForcedTaskIDIsTheJoinedTask1942(t *testing.T) {
	f := newWaitFixture(t, forceConfig(4, 8), &ForceBudget{}, true, false)
	f.write("k", 10)
	type seen struct {
		rep    ForceReply
		joined uint64
	}
	got := make(chan seen, 1)
	waitAfterForceHook = func(k string, rep ForceReply) {
		ws := f.waiterStates(k)
		s := seen{rep: rep}
		if len(ws) == 1 {
			s.joined = ws[0].joined
		}
		got <- s
	}
	ch := f.startWait(5*time.Second, time.Hour, "db/k")
	s := <-got
	if s.rep.Result != Forced || s.rep.TaskID == 0 || s.rep.TaskID != s.joined {
		t.Fatalf("reply %+v, joined %d: want forced with the joined task", s.rep, s.joined)
	}
	if err := f.waitResult(ch); err != nil {
		t.Fatalf("WaitKeysFlushed = %v", err)
	}
}

// The write side drops a LostRace key, as it did NothingToForce: asked once,
// and the other key's forcing goes on (k1: not due, then forced at its due).
func TestVisibleForceLostRaceDropsTheKey1942(t *testing.T) {
	o := NewVisibleOutcome(time.Now(), false, 0)
	visibleBatch(o, 3, true)
	noteVisibleKey(o, "db/k0")
	visibleBatch(o, 3, true)
	noteVisibleKey(o, "db/k1")
	sf := newScriptForcer(o, 6,
		map[string][]ForceResult{"db/k0": {LostRace}, "db/k1": {NotDue, Forced}},
		map[string]time.Duration{"db/k1": 50 * time.Millisecond})
	v := forceWait(o, 6, sf, time.Second, 2*time.Second)
	if !v.Visible {
		t.Fatalf("%+v (k0 %d calls, k1 %d): a lost race ended or stalled forcing", v, sf.callsOf("db/k0"), sf.callsOf("db/k1"))
	}
	if n0, n1 := sf.callsOf("db/k0"), sf.callsOf("db/k1"); n0 != 1 || n1 != 2 {
		t.Fatalf("k0 asked %d times (want 1: dropped), k1 %d (want 2)", n0, n1)
	}
}

func TestForceResultString1942(t *testing.T) {
	for r, want := range map[ForceResult]string{
		Forced: "forced", NotDue: "not_due", OverBudget: "over_budget", NothingToForce: "nothing",
		NotWanted: "not_wanted", Closing: "closing", LedgerOff: "ledger_off", LostRace: "lost_race",
	} {
		if got := r.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", r, got, want)
		}
	}
}
