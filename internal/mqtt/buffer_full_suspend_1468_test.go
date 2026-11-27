package mqtt

// #1468: subscriptions are suspended while the rows waiting to be flushed are
// at ingest.max_buffered_bytes, so the broker holds messages Arc would
// otherwise acknowledge and drop, and resume when that clears and no other
// reason holds them.

import (
	"strings"
	"testing"
	"time"
)

func settleBufferFull(t *testing.T, m *SubscriptionManager) {
	t.Helper()
	eventually(t, "a buffer-full change never applied", func() bool {
		m.bufferFull.mu.Lock()
		defer m.bufferFull.mu.Unlock()
		return m.bufferFull.timer == nil
	})
	m.applyMu.Lock()
	m.applyMu.Unlock()
}

func TestBufferFullSuspendsAtOnceAndResumesAfterTheMinimumTime(t *testing.T) {
	m, s, client := newSuspendableManager(t)
	m.bufferFull.minState = 300 * time.Millisecond
	began := time.Now()
	m.SetBufferFull(true)
	eventually(t, "suspension not applied", s.suspended.Load)
	if took := time.Since(began); took >= m.bufferFull.minState {
		t.Fatalf("suspension took %v: it must not wait", took)
	}
	m.SetBufferFull(false) // inside the minimum time: deferred
	time.Sleep(100 * time.Millisecond)
	if !s.suspended.Load() {
		t.Fatal("resumed inside the minimum time")
	}
	eventually(t, "the deferred resume never came", func() bool { return !s.suspended.Load() })
	settleBufferFull(t, m)
	if client.disconnects.Load() != 1 || client.connects.Load() != 1 {
		t.Fatalf("disconnects=%d connects=%d, want 1 each", client.disconnects.Load(), client.connects.Load())
	}
}

// The buffer-full reason and the disk reason are held apart: either still set
// keeps the subscriptions suspended.
func TestBufferFullAndDiskPressureAreSeparateReasons(t *testing.T) {
	m, s, client := newSuspendableManager(t)
	m.SetBufferFull(true)
	m.SetWALDiskPressure(true)
	settleBufferFull(t, m)
	settle(t, m)
	m.bufferFull.mu.Lock()
	m.bufferFull.changed = time.Time{} // past the minimum time
	m.bufferFull.mu.Unlock()
	m.SetBufferFull(false)
	settleBufferFull(t, m)
	if !s.suspended.Load() || client.connects.Load() != 0 {
		t.Fatal("the buffer clearing resumed a subscription still held by disk pressure")
	}
	m.disk.mu.Lock()
	m.disk.changed = time.Time{}
	m.disk.mu.Unlock()
	m.SetWALDiskPressure(false)
	settle(t, m)
	if s.suspended.Load() || client.connects.Load() != 1 {
		t.Fatalf("not resumed once no reason is left (connects=%d)", client.connects.Load())
	}
}

func TestSuspendReasonsDescribeNamesEachReason(t *testing.T) {
	for r, want := range map[suspendReasons][]string{
		suspendBufferFull:                                  {"max_buffered_bytes"},
		suspendWALDisk | suspendBufferFull:                 {"disk pressure", "max_buffered_bytes"},
		suspendFenced | suspendWALDisk | suspendBufferFull: {"fenced", "disk pressure", "max_buffered_bytes"},
	} {
		got := r.describe()
		for _, w := range want {
			if !strings.Contains(got, w) {
				t.Errorf("%d: %q lacks %q", r, got, w)
			}
		}
		if r&suspendFenced == 0 && strings.Contains(got, "fenced") {
			t.Errorf("%d: %q names a fence that is not set", r, got)
		}
	}
}
