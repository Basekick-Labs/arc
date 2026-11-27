package mqtt

import (
	"strings"
	"testing"
	"time"
)

func settleNotDurable(t *testing.T, m *SubscriptionManager) {
	t.Helper()
	eventually(t, "a not-durable change never applied", func() bool {
		m.notDurable.mu.Lock()
		defer m.notDurable.mu.Unlock()
		return m.notDurable.timer == nil
	})
	m.applyMu.Lock()
	m.applyMu.Unlock()
}

// #1502: while the WAL refuses durable acknowledgements, subscriptions are
// suspended at once and resume after the minimum time once it recovers.
func TestWALNotDurableSuspendsAtOnceAndResumesAfterTheMinimumTime(t *testing.T) {
	m, s, _ := newSuspendableManager(t)
	m.notDurable.minState = 300 * time.Millisecond
	began := time.Now()
	m.SetWALNotDurable(true)
	eventually(t, "suspension not applied", s.suspended.Load)
	if took := time.Since(began); took >= m.notDurable.minState {
		t.Fatalf("suspension took %v: delayed", took)
	}
	m.SetWALNotDurable(false) // inside the minimum time: deferred
	time.Sleep(100 * time.Millisecond)
	if !s.suspended.Load() {
		t.Fatal("resumed inside the minimum time")
	}
	eventually(t, "the deferred resume never came", func() bool { return !s.suspended.Load() })
	settleNotDurable(t, m)
}

// The reason is independent of the others: clearing it does not resume a
// subscription another reason holds, and the other way round.
func TestWALNotDurableIsASeparateReason(t *testing.T) {
	m, s, _ := newSuspendableManager(t)
	m.notDurable.minState = time.Millisecond
	m.disk.minState = time.Millisecond
	m.SetWALNotDurable(true)
	m.SetWALDiskPressure(true)
	eventually(t, "not suspended", s.suspended.Load)
	settleNotDurable(t, m)
	settle(t, m)
	m.SetWALNotDurable(false)
	time.Sleep(50 * time.Millisecond)
	settleNotDurable(t, m)
	if !s.suspended.Load() {
		t.Fatal("resumed while the WAL volume is still under disk pressure")
	}
	m.SetWALDiskPressure(false)
	eventually(t, "never resumed once both cleared", func() bool { return !s.suspended.Load() })
}

func TestSuspendReasonsDescribeNotDurable(t *testing.T) {
	if d := (suspendWALNotDurable).describe(); !strings.Contains(d, "durable") {
		t.Fatalf("%q lacks the not-durable reason", d)
	}
	if d := (suspendWALNotDurable | suspendWALDisk).describe(); !strings.Contains(d, "durable") || !strings.Contains(d, "disk pressure") {
		t.Fatalf("%q lacks a reason", d)
	}
}
