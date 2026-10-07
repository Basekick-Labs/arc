package raft

// FSM tests for the cluster-wide compaction pause (#1087). Apply is driven
// entirely by the payload and the current state: no test here depends on the
// wall clock, and one proves Apply does not read it.

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var pauseT0 = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

func pausePayload(gen uint64, by string, now time.Time) SetCompactionPausePayload {
	return SetCompactionPausePayload{
		Paused: true, Generation: gen, RequestedBy: by, Reason: "restore b-" + by,
		Now: now, ExpiresAt: now.Add(2 * time.Minute),
	}
}

func applyPauseCmd(t *testing.T, fsm *ClusterFSM, p SetCompactionPausePayload) error {
	t.Helper()
	return applyResultErr(t, fsm.Apply(&raft.Log{Data: makeCommand(t, CommandSetCompactionPause, p)}))
}

func applyAckCmd(t *testing.T, fsm *ClusterFSM, p AckCompactionPausePayload) error {
	t.Helper()
	return applyResultErr(t, fsm.Apply(&raft.Log{Data: makeCommand(t, CommandAckCompactionPause, p)}))
}

func applyResultErr(t *testing.T, resp interface{}) error {
	t.Helper()
	if resp == nil {
		return nil
	}
	err, ok := resp.(error)
	if !ok {
		t.Fatalf("Apply returned %T, want error or nil", resp)
	}
	return err
}

type pauseEvent struct {
	paused bool
	gen    uint64
}

func recordPauseEvents(fsm *ClusterFSM) *[]pauseEvent {
	events := &[]pauseEvent{}
	fsm.SetCompactionPauseCallback(func(paused bool, gen uint64) {
		*events = append(*events, pauseEvent{paused, gen})
	})
	return events
}

func TestFSMCompactionPause_GenerationCASRefreshTakeoverResume(t *testing.T) {
	fsm := newTestFSM()
	events := recordPauseEvents(fsm)

	// A fresh FSM is at generation 0; a pause must propose exactly 1.
	err := applyPauseCmd(t, fsm, pausePayload(2, "writer-A", pauseT0))
	if !IsCompactionPauseConflict(err) {
		t.Fatalf("gen 2 on a fresh FSM: err = %v, want a generation conflict", err)
	}
	if err := applyPauseCmd(t, fsm, pausePayload(1, "writer-A", pauseT0)); err != nil {
		t.Fatalf("pause gen 1: %v", err)
	}
	s := fsm.GetCompactionPause()
	if !s.Active || s.Generation != 1 || s.RequestedBy != "writer-A" || s.Reason != "restore b-writer-A" ||
		!s.RequestedAt.Equal(pauseT0) || !s.ExpiresAt.Equal(pauseT0.Add(2*time.Minute)) || len(s.Acks) != 0 {
		t.Fatalf("state after pause = %+v", s)
	}

	// An ack for the active generation is recorded; one for another is not.
	if err := applyAckCmd(t, fsm, AckCompactionPausePayload{NodeID: "compactor-X", Generation: 1}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := applyAckCmd(t, fsm, AckCompactionPausePayload{NodeID: "reader-Y", Generation: 7}); err != nil {
		t.Fatalf("ack for another generation must be ignored, not refused: %v", err)
	}
	if s = fsm.GetCompactionPause(); len(s.Acks) != 1 || s.Acks["compactor-X"] != 1 {
		t.Fatalf("acks = %v, want compactor-X only", s.Acks)
	}

	// Same generation, same requester: a refresh. ExpiresAt moves, the acks
	// stay, no callback.
	refresh := pausePayload(1, "writer-A", pauseT0.Add(30*time.Second))
	if err := applyPauseCmd(t, fsm, refresh); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if s = fsm.GetCompactionPause(); !s.ExpiresAt.Equal(refresh.ExpiresAt) || s.Acks["compactor-X"] != 1 || !s.RequestedAt.Equal(pauseT0) {
		t.Fatalf("state after refresh = %+v, want expires_at %s with the ack kept", s, refresh.ExpiresAt)
	}

	// Another node, while the pause is in force by the proposer's clock.
	err = applyPauseCmd(t, fsm, pausePayload(2, "writer-B", pauseT0.Add(time.Minute)))
	if err == nil || !strings.Contains(err.Error(), "already paused by writer-A") {
		t.Fatalf("foreign pause while in force: err = %v, want already paused by writer-A", err)
	}
	// A resume naming a stale generation is refused; the right one clears.
	if err := applyPauseCmd(t, fsm, SetCompactionPausePayload{Paused: false, Generation: 7, RequestedBy: "writer-A"}); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale resume: err = %v, want stale", err)
	}
	// The same requester takes its own pause over (a restarted restore).
	if err := applyPauseCmd(t, fsm, pausePayload(2, "writer-A", pauseT0.Add(time.Minute))); err != nil {
		t.Fatalf("same-requester takeover: %v", err)
	}
	if s = fsm.GetCompactionPause(); s.Generation != 2 || len(s.Acks) != 0 {
		t.Fatalf("takeover must start generation 2 with no acks: %+v", s)
	}
	if err := applyPauseCmd(t, fsm, SetCompactionPausePayload{Paused: false, Generation: 2, RequestedBy: "writer-A"}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if s = fsm.GetCompactionPause(); s.Active || s.Generation != 2 || len(s.Acks) != 0 {
		t.Fatalf("state after resume = %+v, want inactive at generation 2", s)
	}
	// Resume of nothing is idempotent; an ack with nothing active is ignored.
	if err := applyPauseCmd(t, fsm, SetCompactionPausePayload{Paused: false, Generation: 2, RequestedBy: "writer-A"}); err != nil {
		t.Fatalf("second resume: %v", err)
	}
	if err := applyAckCmd(t, fsm, AckCompactionPausePayload{NodeID: "compactor-X", Generation: 2}); err != nil {
		t.Fatalf("ack with no pause: %v", err)
	}
	if s = fsm.GetCompactionPause(); len(s.Acks) != 0 {
		t.Fatalf("an ack with no active pause was recorded: %v", s.Acks)
	}

	// The generation survives the resume: the next pause is 3.
	if err := applyPauseCmd(t, fsm, pausePayload(1, "writer-B", pauseT0.Add(5*time.Minute))); !IsCompactionPauseConflict(err) {
		t.Fatalf("gen 1 after a resume at 2: err = %v, want a conflict", err)
	}
	if err := applyPauseCmd(t, fsm, pausePayload(3, "writer-B", pauseT0.Add(5*time.Minute))); err != nil {
		t.Fatalf("pause gen 3: %v", err)
	}

	// Callbacks only on transitions: pause(1), pause(2) takeover, resume(2), pause(3).
	want := []pauseEvent{{true, 1}, {true, 2}, {false, 2}, {true, 3}}
	if len(*events) != len(want) {
		t.Fatalf("events = %v, want %v", *events, want)
	}
	for i := range want {
		if (*events)[i] != want[i] {
			t.Fatalf("events = %v, want %v", *events, want)
		}
	}
}

// Another requester may take over a pause whose ExpiresAt has passed by the
// PROPOSER's clock. The expiry carried in the state is from the first
// proposer; the takeover's Now is the second proposer's.
func TestFSMCompactionPause_ExpiredPauseIsTakenOver(t *testing.T) {
	fsm := newTestFSM()
	if err := applyPauseCmd(t, fsm, pausePayload(1, "writer-A", pauseT0)); err != nil {
		t.Fatal(err)
	}
	if err := applyAckCmd(t, fsm, AckCompactionPausePayload{NodeID: "compactor-X", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := applyPauseCmd(t, fsm, pausePayload(2, "writer-B", pauseT0.Add(3*time.Minute))); err != nil {
		t.Fatalf("takeover of an expired pause: %v", err)
	}
	s := fsm.GetCompactionPause()
	if !s.Active || s.Generation != 2 || s.RequestedBy != "writer-B" || len(s.Acks) != 0 {
		t.Fatalf("state after takeover = %+v", s)
	}
}

// Apply decides expiry from the payload's Now, never from time.Now: a pause
// whose ExpiresAt is years in the past by the wall clock is still in force
// for a proposer whose Now precedes it, and a pause expiring years in the
// future is expired for a proposer whose Now is after it.
func TestFSMCompactionPause_ApplyNeverReadsTheClock(t *testing.T) {
	fsm := newTestFSM()
	longAgo := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := applyPauseCmd(t, fsm, pausePayload(1, "writer-A", longAgo)); err != nil {
		t.Fatal(err)
	}
	err := applyPauseCmd(t, fsm, pausePayload(2, "writer-B", longAgo.Add(time.Minute)))
	if err == nil || !strings.Contains(err.Error(), "already paused by writer-A") {
		t.Fatalf("a proposer whose clock precedes the expiry must be refused even though the wall clock is past it: err = %v", err)
	}

	fsm2 := newTestFSM()
	farAhead := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := applyPauseCmd(t, fsm2, pausePayload(1, "writer-A", farAhead)); err != nil {
		t.Fatal(err)
	}
	if err := applyPauseCmd(t, fsm2, pausePayload(2, "writer-B", farAhead.Add(3*time.Minute))); err != nil {
		t.Fatalf("a proposer whose clock is past the expiry must take over even though the wall clock is before it: %v", err)
	}
}

func TestFSMCompactionPause_SnapshotRestoreRoundTrip(t *testing.T) {
	fsm := newTestFSM()
	if err := applyPauseCmd(t, fsm, pausePayload(1, "writer-A", pauseT0)); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"compactor-X", "reader-Y"} {
		if err := applyAckCmd(t, fsm, AckCompactionPausePayload{NodeID: n, Generation: 1}); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	if !bytes.Contains(raw, []byte(`"compaction_pause"`)) {
		t.Fatalf("snapshot does not carry the pause: %s", raw)
	}

	// A running follower that catches up by snapshot install must learn of
	// the pause: the callback fires when the restore makes one active.
	fsm2 := newTestFSM()
	events := recordPauseEvents(fsm2)
	if err := fsm2.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	want := fsm.GetCompactionPause()
	got := fsm2.GetCompactionPause()
	if got.Active != want.Active || got.Generation != want.Generation || got.RequestedBy != want.RequestedBy ||
		got.Reason != want.Reason || !got.RequestedAt.Equal(want.RequestedAt) || !got.ExpiresAt.Equal(want.ExpiresAt) ||
		len(got.Acks) != 2 || got.Acks["compactor-X"] != 1 || got.Acks["reader-Y"] != 1 {
		t.Fatalf("restored pause = %+v, want %+v", got, want)
	}
	if len(*events) != 1 || (*events)[0] != (pauseEvent{true, 1}) {
		t.Fatalf("restore events = %v, want one pause(1)", *events)
	}

	// Restoring the same snapshot again changes nothing and fires nothing.
	if err := fsm2.Restore(io.NopCloser(bytes.NewReader(raw))); err != nil {
		t.Fatal(err)
	}
	if len(*events) != 1 {
		t.Fatalf("a restore that changes nothing fired %v", *events)
	}

	// A snapshot from a cluster that never paused (no key at all) restored
	// over an active pause ends it, and says so.
	fsm3 := newTestFSM()
	plain, err := fsm3.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var plainBuf bytes.Buffer
	if err := plain.Persist(&testSnapshotSink{Writer: &plainBuf}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(plainBuf.Bytes(), []byte(`"compaction_pause"`)) {
		t.Fatalf("a never-paused FSM must not persist the key: %s", plainBuf.Bytes())
	}
	if err := fsm2.Restore(io.NopCloser(bytes.NewReader(plainBuf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if s := fsm2.GetCompactionPause(); s.Active {
		t.Fatalf("pause survived a snapshot without one: %+v", s)
	}
	if len(*events) != 2 || (*events)[1].paused {
		t.Fatalf("events after restoring a pause-less snapshot = %v, want a resume event last", *events)
	}
}

// The TTL is bounded from the payload alone, deterministically: expires_at
// must be after now and at most MaxCompactionPauseTTL later, for a pause and
// for a refresh, so a buggy proposer cannot wedge compaction until a resume.
func TestFSMCompactionPause_TTLBound(t *testing.T) {
	fsm := newTestFSM()
	base := SetCompactionPausePayload{Paused: true, Generation: 1, RequestedBy: "writer-A", Reason: "restore b1", Now: pauseT0}
	for name, expires := range map[string]time.Time{
		"equal to now":   pauseT0,
		"before now":     pauseT0.Add(-time.Second),
		"over the bound": pauseT0.Add(MaxCompactionPauseTTL + time.Nanosecond),
		"far over bound": pauseT0.Add(24 * time.Hour),
	} {
		p := base
		p.ExpiresAt = expires
		if err := applyPauseCmd(t, fsm, p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if s := fsm.GetCompactionPause(); s.Active || s.Generation != 0 {
		t.Fatalf("a refused pause changed the state: %+v", s)
	}
	atBound := base
	atBound.ExpiresAt = pauseT0.Add(MaxCompactionPauseTTL)
	if err := applyPauseCmd(t, fsm, atBound); err != nil {
		t.Fatalf("exactly the bound must be accepted: %v", err)
	}
	// A refresh is bounded the same way and a refused one moves nothing.
	refresh := atBound
	refresh.Now = pauseT0.Add(time.Minute)
	refresh.ExpiresAt = refresh.Now.Add(MaxCompactionPauseTTL + time.Second)
	if err := applyPauseCmd(t, fsm, refresh); err == nil {
		t.Fatal("an over-long refresh was accepted")
	}
	if s := fsm.GetCompactionPause(); !s.ExpiresAt.Equal(atBound.ExpiresAt) {
		t.Fatalf("a refused refresh moved expires_at to %s", s.ExpiresAt)
	}
}

func TestFSMCompactionPause_PayloadBounds(t *testing.T) {
	fsm := newTestFSM()
	long := strings.Repeat("n", MaxCompactionPauseFieldLen+1)
	cases := map[string]SetCompactionPausePayload{
		"no requester":   {Paused: true, Generation: 1, Now: pauseT0, ExpiresAt: pauseT0.Add(time.Minute)},
		"long requester": pausePayload(1, long, pauseT0),
		"long reason": {Paused: true, Generation: 1, RequestedBy: "writer-A", Reason: long,
			Now: pauseT0, ExpiresAt: pauseT0.Add(time.Minute)},
		"zero clock": {Paused: true, Generation: 1, RequestedBy: "writer-A"},
	}
	for name, p := range cases {
		if err := applyPauseCmd(t, fsm, p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if s := fsm.GetCompactionPause(); s.Active || s.Generation != 0 {
		t.Fatalf("a refused pause changed the state: %+v", s)
	}
	if err := applyPauseCmd(t, fsm, pausePayload(1, "writer-A", pauseT0)); err != nil {
		t.Fatal(err)
	}
	if err := applyAckCmd(t, fsm, AckCompactionPausePayload{NodeID: long, Generation: 1}); err == nil {
		t.Error("an ack with an over-long node id was accepted")
	}
	if err := applyAckCmd(t, fsm, AckCompactionPausePayload{Generation: 1}); err == nil {
		t.Error("an ack with no node id was accepted")
	}
	if s := fsm.GetCompactionPause(); len(s.Acks) != 0 {
		t.Fatalf("refused acks were recorded: %v", s.Acks)
	}
}
