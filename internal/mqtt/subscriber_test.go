package mqtt

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// newStatsSubscriber builds a minimal Subscriber sufficient to exercise the
// stats path (GetStats reads config.Name). It does not connect to a broker.
func newStatsSubscriber() *Subscriber {
	return &Subscriber{
		id:     "sub-1",
		config: &Subscription{ID: "sub-1", Name: "test"},
		logger: zerolog.Nop(),
	}
}

// TestGetStats_LastMessageAt verifies the atomic lastMessageAt (#328): it is the
// zero time.Time before any message (so omitempty omits it), and reflects the
// stored nanos after a message.
func TestGetStats_LastMessageAt(t *testing.T) {
	s := newStatsSubscriber()

	// Before any message: zero value, so the JSON omitempty tag drops it.
	if got := s.GetStats().LastMessageAt; !got.IsZero() {
		t.Errorf("LastMessageAt before any message = %v, want zero", got)
	}

	// Simulate a message arriving: store the current time as nanos (this is what
	// onMessage does on the hot path).
	now := time.Now()
	s.lastMessageAtNanos.Store(now.UnixNano())

	got := s.GetStats().LastMessageAt
	if got.IsZero() {
		t.Fatal("LastMessageAt after a message should be non-zero")
	}
	// Reconstructed time must equal the stored instant to nanosecond precision.
	if !got.Equal(now) {
		t.Errorf("LastMessageAt = %v, want %v (nanos round-trip)", got, now)
	}
}

// TestGetStats_ConcurrentMessageUpdates is the #328 race guard: concurrent
// hot-path updates (lastMessageAtNanos.Store) and GetStats reads must be
// race-free. Run with -race. Before the fix this path took a full mutex on
// every message; the atomic must be equally safe.
func TestGetStats_ConcurrentMessageUpdates(t *testing.T) {
	s := newStatsSubscriber()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: mimic onMessage's hot-path stat updates.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					s.messagesReceived.Add(1)
					s.bytesReceived.Add(128)
					s.lastMessageAtNanos.Store(time.Now().UnixNano())
				}
			}
		}()
	}

	// Readers: mimic the stats endpoint.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = s.GetStats()
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Sanity: at least one message was counted and a last-message time was set.
	if s.GetStats().MessagesReceived == 0 {
		t.Error("expected some messages counted")
	}
	if s.GetStats().LastMessageAt.IsZero() {
		t.Error("expected LastMessageAt to be set after concurrent updates")
	}
}

// BenchmarkOnMessageStatUpdate isolates the per-message stat bookkeeping that
// onMessage does (the part changed in #328): counters + lastMessageAt. Run with
// -cpu to see contention. The atomic path replaces a full mutex lock per message.
func BenchmarkOnMessageStatUpdate(b *testing.B) {
	s := newStatsSubscriber()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.messagesReceived.Add(1)
			s.bytesReceived.Add(128)
			s.lastMessageAtNanos.Store(time.Now().UnixNano())
		}
	})
}
