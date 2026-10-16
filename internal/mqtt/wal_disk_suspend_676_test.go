package mqtt

// #676: subscriptions are suspended for a SET of reasons. A cluster unfence
// must not resume them while the WAL volume is under disk pressure, and the
// disk reason holds each state a minimum time.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

type doneToken struct{}

func (doneToken) Wait() bool                     { return true }
func (doneToken) WaitTimeout(time.Duration) bool { return true }
func (doneToken) Done() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}
func (doneToken) Error() error { return nil }

// countingClient records disconnects and connects; nothing else is called.
type countingClient struct {
	pahomqtt.Client
	disconnects, connects atomic.Int64
}

func (c *countingClient) Disconnect(uint) { c.disconnects.Add(1) }
func (c *countingClient) Connect() pahomqtt.Token {
	c.connects.Add(1)
	return doneToken{}
}

// eventually waits for cond: disk pressure is applied off the caller's
// goroutine.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// settle waits until no disk pressure apply is pending.
func settle(t *testing.T, m *SubscriptionManager) {
	t.Helper()
	eventually(t, "a disk pressure change never applied", func() bool {
		m.diskMu.Lock()
		defer m.diskMu.Unlock()
		return m.diskTimer == nil
	})
	m.applyMu.Lock() // an apply that cleared diskTimer has finished
	m.applyMu.Unlock()
}

func newSuspendableManager(t *testing.T) (*SubscriptionManager, *Subscriber, *countingClient) {
	t.Helper()
	client := &countingClient{}
	s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", ConnectTimeoutSeconds: 1},
		client: client, running: true, logger: zerolog.Nop()}
	m := &SubscriptionManager{subscribers: map[string]*Subscriber{"s": s}, logger: zerolog.Nop()}
	return m, s, client
}

func TestUnfenceDoesNotResumeUnderDiskPressure(t *testing.T) {
	m, s, client := newSuspendableManager(t)
	m.SetIngestFenced(true)
	m.SetWALDiskPressure(true)
	settle(t, m)
	if !s.suspended.Load() || client.disconnects.Load() != 1 {
		t.Fatalf("suspended=%v disconnects=%d", s.suspended.Load(), client.disconnects.Load())
	}
	m.SetIngestFenced(false)
	if !s.suspended.Load() || client.connects.Load() != 0 {
		t.Fatal("a cluster unfence resumed the subscription while the WAL volume is under disk pressure")
	}
	m.diskMu.Lock()
	m.diskChanged = time.Time{} // past the minimum time in the state
	m.diskMu.Unlock()
	m.SetWALDiskPressure(false)
	settle(t, m)
	if s.suspended.Load() || client.connects.Load() != 1 {
		t.Fatalf("not resumed once no reason is left (connects=%d)", client.connects.Load())
	}
}

func TestDiskPressureClearDoesNotResumeWhileFenced(t *testing.T) {
	m, s, client := newSuspendableManager(t)
	m.SetWALDiskPressure(true)
	settle(t, m)
	m.SetIngestFenced(true)
	m.diskMu.Lock()
	m.diskChanged = time.Time{}
	m.diskMu.Unlock()
	m.SetWALDiskPressure(false)
	settle(t, m)
	if !s.suspended.Load() || client.connects.Load() != 0 {
		t.Fatal("disk pressure clearing resumed a fenced subscription")
	}
}

// A change inside the minimum time is applied when it ends, if it still
// stands; one undone in time is never applied.
// Suspension is never delayed (every message taken while writes are refused
// is lost); resuming waits until the subscriptions have been suspended for
// the minimum time, and a clear undone inside it never resumes them.
func TestDiskPressureSuspendsAtOnceAndResumesAfterTheMinimumTime(t *testing.T) {
	m, s, client := newSuspendableManager(t)
	m.diskMinState = 300 * time.Millisecond
	m.SetWALDiskPressure(true)
	eventually(t, "suspension not applied", s.suspended.Load)
	m.SetWALDiskPressure(false) // inside the minimum time: deferred
	time.Sleep(100 * time.Millisecond)
	if !s.suspended.Load() {
		t.Fatal("resumed inside the minimum time")
	}
	eventually(t, "the deferred resume never came", func() bool { return !s.suspended.Load() })
	settle(t, m)

	// Right after resuming, pressure again: suspended at once, no minimum
	// time in the resumed state.
	began := time.Now()
	m.SetWALDiskPressure(true)
	eventually(t, "suspension delayed", s.suspended.Load)
	if took := time.Since(began); took >= m.diskMinState {
		t.Fatalf("suspension took %v: delayed by a minimum time in the resumed state", took)
	}
	settle(t, m)

	// A clear undone inside the minimum time never resumes.
	m.SetWALDiskPressure(false)
	m.SetWALDiskPressure(true)
	time.Sleep(2 * m.diskMinState)
	settle(t, m)
	if !s.suspended.Load() || client.connects.Load() != 1 {
		t.Fatalf("a clear undone in time resumed the subscription (connects=%d)", client.connects.Load())
	}
}

// A subscription registered while a reason is being set or cleared ends in
// the state the reasons call for: suspended when fenced, running once the
// fence lifted (read "fenced", then the unfence resumes everything before it
// suspends: stuck with no reason).
func TestRegisterStartedDuringATransition(t *testing.T) {
	for _, fence := range []bool{true, false} {
		for round := 0; round < 300; round++ {
			m := &SubscriptionManager{subscribers: map[string]*Subscriber{"s": nil}, logger: zerolog.Nop()}
			if !fence {
				m.suspended = suspendFenced
			}
			s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", ConnectTimeoutSeconds: 1},
				client: &countingClient{}, running: true, logger: zerolog.Nop()}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); m.SetIngestFenced(fence) }()
			go func() {
				defer wg.Done()
				if err := m.registerStarted("s", s); err != nil {
					t.Error(err)
				}
			}()
			wg.Wait()
			if s.suspended.Load() != fence {
				t.Fatalf("fence=%v round %d: suspended=%v", fence, round, s.suspended.Load())
			}
		}
	}
}

func TestRegisterStartedRefusesAStoppedSlot(t *testing.T) {
	m := &SubscriptionManager{subscribers: map[string]*Subscriber{}, logger: zerolog.Nop()}
	if err := m.registerStarted("s", &Subscriber{}); err != ErrSubscriptionNotRunning {
		t.Fatalf("err = %v", err)
	}
}

func TestSuspendReasonsDescribe(t *testing.T) {
	for r, want := range map[suspendReasons]string{
		suspendFenced:                  "fenced",
		suspendWALDisk:                 "disk pressure",
		suspendFenced | suspendWALDisk: "and its WAL volume",
	} {
		if !strings.Contains(r.describe(), want) {
			t.Errorf("%d: %q lacks %q", r, r.describe(), want)
		}
	}
}

// A message whose write is refused for disk pressure is held briefly like a
// fenced one, then counted failed and logged by reason, not per message.
func TestOnMessageTreatsDiskPressureAsARefusal(t *testing.T) {
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 2, FlushWorkers: 1, FlushQueueSize: 4,
	}, store, zerolog.Nop())
	defer buf.Close()
	var checks atomic.Int64
	buf.SetWALDiskGate(ingest.PressureFunc(func() bool { return checks.Add(1) <= 2 }))

	var mu sync.Mutex
	var subLog, refusals strings.Builder
	lw := writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return subLog.Write(p) })
	rw := writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return refusals.Write(p) })
	defer ingest.CaptureRefusalLog(zerolog.New(rw), time.Now)()
	s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", Database: "iot"},
		arrowBuffer: buf, ctx: context.Background(), logger: zerolog.New(lw)}
	s.onMessage(nil, fakeMessage{topic: "sensors/a", payload: []byte(`{"m":"cpu","v":1}`)})
	if checks.Load() < 3 || s.messagesFailed.Load() != 0 {
		t.Fatalf("checks=%d failed=%d: a briefly refused message was not held", checks.Load(), s.messagesFailed.Load())
	}

	buf.SetWALDiskGate(ingest.PressureFunc(func() bool { return true }))
	before := metrics.Get().Snapshot()["wal_disk_pressure_rejections"].(int64)
	s.onMessage(nil, fakeMessage{topic: "sensors/a", payload: []byte(`{"m":"cpu","v":1}`)})
	if after := metrics.Get().Snapshot()["wal_disk_pressure_rejections"].(int64); after != before+1 {
		t.Fatalf("rejections %d -> %d: a message retried under pressure counted more than once", before, after)
	}
	mu.Lock()
	defer mu.Unlock()
	if s.messagesFailed.Load() != 1 || strings.Contains(subLog.String(), "Failed to process MQTT message") {
		t.Fatalf("failed=%d log:\n%s", s.messagesFailed.Load(), subLog.String())
	}
	if !strings.Contains(refusals.String(), `"reason":"wal_disk"`) {
		t.Fatalf("not logged by reason:\n%s", refusals.String())
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// sizedGate admits only reservations up to max bytes.
type sizedGate struct{ max int64 }

func (g sizedGate) Pressure() bool       { return false }
func (g sizedGate) Reserve(n int64) bool { return n <= g.max }
func (g sizedGate) Release(int64)        {}

// A message reserves its payload's size: one too large for the room left is
// refused, a small one admitted.
func TestMQTTMessageReservesItsPayload(t *testing.T) {
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 2, FlushWorkers: 1, FlushQueueSize: 4,
	}, store, zerolog.Nop())
	defer buf.Close()
	payload := []byte(`{"m":"cpu","v":1}`)
	s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", Database: "iot"},
		arrowBuffer: buf, ctx: context.Background(), logger: zerolog.Nop()}
	buf.SetWALDiskGate(sizedGate{max: int64(len(payload)) - 1})
	if err := s.processMessage(context.Background(), "t", payload, "iot"); !errors.Is(err, ingest.ErrWALDiskPressure) {
		t.Fatalf("err = %v: a message larger than the room left was admitted", err)
	}
	buf.SetWALDiskGate(sizedGate{max: int64(len(payload))})
	if err := s.processMessage(context.Background(), "t", payload, "iot"); err != nil {
		t.Fatal(err)
	}
}
