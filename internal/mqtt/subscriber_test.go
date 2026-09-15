package mqtt

import (
	"errors"
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

// TestGetStats_LastMessageAt verifies lastMessageAt (#328/#546): the pointer is
// nil before any message (so omitempty omits the field), and after a message it
// points to the stored instant, reconstructed in UTC.
func TestGetStats_LastMessageAt(t *testing.T) {
	s := newStatsSubscriber()

	// Before any message: nil pointer, so the JSON omitempty tag drops the field.
	if got := s.GetStats().LastMessageAt; got != nil {
		t.Errorf("LastMessageAt before any message = %v, want nil", got)
	}

	// Simulate a message arriving: store the current time as nanos (this is what
	// onMessage does on the hot path).
	now := time.Now()
	s.lastMessageAtNanos.Store(now.UnixNano())

	got := s.GetStats().LastMessageAt
	if got == nil {
		t.Fatal("LastMessageAt after a message should be non-nil")
	}
	// Reconstructed time must equal the stored instant to nanosecond precision...
	if !got.Equal(now) {
		t.Errorf("LastMessageAt = %v, want %v (nanos round-trip)", got, now)
	}
	// ...and be rendered in UTC (Arc's timestamp convention, #546).
	if got.Location() != time.UTC {
		t.Errorf("LastMessageAt location = %v, want UTC", got.Location())
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
	if s.GetStats().LastMessageAt == nil {
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

type unsubscribeToken struct {
	err      error
	finished bool
	done     chan struct{}
}

func (t *unsubscribeToken) Wait() bool {
	return t.finished
}

func (t *unsubscribeToken) WaitTimeout(time.Duration) bool {
	return t.finished
}

func (t *unsubscribeToken) Done() <-chan struct{} {
	return t.done
}

func (t *unsubscribeToken) Error() error {
	return t.err
}

func TestWaitForUnsubscribe(t *testing.T) {
	sentinel := errors.New("broker rejected unsubscribe")
	tests := []struct {
		name        string
		token       *unsubscribeToken
		wantWrapped error
		wantErr     bool
	}{
		{name: "completed", token: &unsubscribeToken{finished: true, done: make(chan struct{})}},
		{name: "broker error", token: &unsubscribeToken{finished: true, err: sentinel, done: make(chan struct{})}, wantWrapped: sentinel, wantErr: true},
		{name: "timeout", token: &unsubscribeToken{done: make(chan struct{})}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := waitForUnsubscribe("events", test.token)
			if (err != nil) != test.wantErr {
				t.Fatalf("waitForUnsubscribe() error = %v, wantErr %v", err, test.wantErr)
			}
			if test.wantWrapped != nil && !errors.Is(err, test.wantWrapped) {
				t.Fatalf("waitForUnsubscribe() error = %v, want wrapped %v", err, test.wantWrapped)
			}
		})
	}
}

// fakeMessage is the minimal paho Message onMessage reads.
type fakeMessage struct {
	topic   string
	payload []byte
}

func (m fakeMessage) Duplicate() bool   { return false }
func (m fakeMessage) Qos() byte         { return 0 }
func (m fakeMessage) Retained() bool    { return false }
func (m fakeMessage) Topic() string     { return m.topic }
func (m fakeMessage) MessageID() uint16 { return 0 }
func (m fakeMessage) Payload() []byte   { return m.payload }
func (m fakeMessage) Ack()              {}

// TestOnMessage_DropsMessageForInvalidDatabase (regression, #300): a message
// whose resolved database is not a valid storage segment is dropped and
// counted before decoding. The subscriber here has no buffer, so reaching the
// write path would dereference nil: the test passing without a panic is the
// proof that the guard returned first. The payload is valid JSON on purpose,
// so a decode failure cannot be what stops it.
func TestOnMessage_DropsMessageForInvalidDatabase(t *testing.T) {
	payload := []byte(`{"m":"cpu","v":1}`)
	cases := map[string]*Subscription{
		"mapped_traversal": {ID: "s", Name: "s", Database: "iot", TopicMapping: map[string]string{"sensors/a": "../other-db"}},
		"mapped_slash":     {ID: "s", Name: "s", Database: "iot", TopicMapping: map[string]string{"sensors/a": "a/b"}},
		"default_invalid":  {ID: "s", Name: "s", Database: "../x"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("onMessage reached the write path with an invalid database (panic: %v)", r)
				}
			}()
			// No buffer and no ctx on purpose: the guard must return before
			// either is touched.
			s := &Subscriber{id: "s", config: cfg, logger: zerolog.Nop()}
			s.onMessage(nil, fakeMessage{topic: "sensors/a", payload: payload})
			s.onMessage(nil, fakeMessage{topic: "sensors/a", payload: payload})
			if got := s.messagesFailed.Load(); got != 2 {
				t.Fatalf("messagesFailed = %d, want 2", got)
			}
			if got := s.messagesReceived.Load(); got != 2 {
				t.Fatalf("messagesReceived = %d, want 2 (received is counted before the drop)", got)
			}
		})
	}
}

// TestMapToRecord_RejectsInvalidMeasurement (regression, #300): the measurement
// is publisher controlled and becomes the second storage-key segment. A name
// the storage key contract would refuse at flush, or that would split into two
// segments, is refused as a decode error before it reaches the buffer.
func TestMapToRecord_RejectsInvalidMeasurement(t *testing.T) {
	s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", Database: "iot"}, logger: zerolog.Nop()}
	for _, good := range []string{"cpu", "cpu_load-1", "C"} {
		if _, err := s.mapToRecord(map[string]interface{}{"m": good, "v": 1.0}); err != nil {
			t.Errorf("measurement %q rejected: %v", good, err)
		}
	}
	if rec, err := s.mapToRecord(map[string]interface{}{"v": 1.0}); err != nil || rec.Measurement != "mqtt" {
		t.Errorf("default measurement: rec=%+v err=%v", rec, err)
	}
	for _, bad := range []string{"../x", "a/b", "1abc", "a b", "a\\b", string(make([]byte, 129))} {
		if _, err := s.mapToRecord(map[string]interface{}{"m": bad, "v": 1.0}); err == nil {
			t.Errorf("measurement %q accepted", bad)
		}
		if _, err := s.mapToRecord(map[string]interface{}{"measurement": bad, "v": 1.0}); err == nil {
			t.Errorf("measurement (long key) %q accepted", bad)
		}
	}
}
