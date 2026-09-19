package replication

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

func TestSenderStatsLagNeverUnderflowsIssue819(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize: 10,
		Logger:     zerolog.Nop(),
	})

	// This can occur when sequence spaces change or an ack reports
	// a position ahead of the writer's current in-memory counter.
	sender.sequence.Store(3)

	reader := &ReaderConnection{id: "reader-1"}
	reader.lastAck.Store(4)
	sender.readers[reader.id] = reader

	stats := sender.Stats()
	readers := stats["readers"].([]map[string]interface{})
	if len(readers) != 1 {
		t.Fatalf("got %d readers, want 1", len(readers))
	}

	lag, ok := readers[0]["lag"].(uint64)
	if !ok {
		t.Fatalf("unexpected lag type: %T", readers[0]["lag"])
	}
	if lag != 0 {
		t.Fatalf("unsigned lag underflow: got %d, want 0", lag)
	}
}

func TestReplicationLagGaugesDeclaredIssue819(t *testing.T) {
	output := metrics.Get().PrometheusFormat()

	for _, name := range []string{
		"arc_replication_lag_entries",
		"arc_replication_lag_seconds",
	} {
		if !strings.Contains(output, "# HELP "+name+" ") {
			t.Errorf("missing gauge HELP declaration: %s", name)
		}
		if !strings.Contains(output, "# TYPE "+name+" gauge") {
			t.Errorf("missing gauge TYPE declaration: %s", name)
		}
	}
}

// issue819Sample extracts an actual Prometheus sample rather than only
// looking for HELP/TYPE declarations.
func issue819Sample(t *testing.T, output, name, peer string) (float64, bool) {
	t.Helper()

	prefix := name + `{peer="` + peer + `"} `
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimPrefix(line, prefix), 64)
		if err != nil {
			t.Fatalf("invalid Prometheus sample %q: %v", line, err)
		}
		return value, true
	}
	return 0, false
}

func TestReplicationPeerLagLifecycleIssue819(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   32,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
	})

	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sender.Stop()

	metric := metrics.Get()

	// HELP and TYPE exist, but an empty writer has no peer samples.
	empty := metric.PrometheusFormat()
	if strings.Contains(empty, "arc_replication_lag_entries{peer=") ||
		strings.Contains(empty, "arc_replication_lag_seconds{peer=") {
		t.Fatalf("standalone writer has misleading peer samples")
	}

	// Populate the real Replicate path before attaching readers.
	// Both timestamps remain within the sender's bounded ring.
	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().Add(-20 * time.Second).UnixMicro()),
		Payload:     []byte("first"),
	})

	addReader := func(id string, acknowledged uint64) *ReaderConnection {
		t.Helper()

		conn, peer := net.Pipe()
		reader, err := sender.PrepareReader(
			conn, id, testSenderNonce+"-"+id, acknowledged,
		)
		if err != nil {
			peer.Close()
			t.Fatalf("prepare test reader %s: %v", id, err)
		}

		// If the distribution goroutine sees the new reader, drain
		// writes so the test cannot block on net.Pipe.
		go func() {
			_, _ = io.Copy(io.Discard, peer)
		}()
		t.Cleanup(func() { peer.Close() })

		sender.mu.Lock()
		sender.readers[id] = reader
		sender.mu.Unlock()
		return reader
	}

	a := addReader("reader-a", 1)
	b := addReader("reader-b", 0)

	first := metric.PrometheusFormat()
	if got, ok := issue819Sample(t, first, "arc_replication_lag_entries", "reader-a"); !ok || got != 0 {
		t.Fatalf("caught-up reader-a lag: %v, present=%v", got, ok)
	}
	if got, ok := issue819Sample(t, first, "arc_replication_lag_seconds", "reader-a"); !ok || got != 0 {
		t.Fatalf("idle reader-a accumulated fake time lag: %v, present=%v", got, ok)
	}
	if got, ok := issue819Sample(t, first, "arc_replication_lag_entries", "reader-b"); !ok || got != 1 {
		t.Fatalf("reader-b initial lag: %v, present=%v", got, ok)
	}

	// Writer advances while readers remain at their prior positions.
	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().Add(-8 * time.Second).UnixMicro()),
		Payload:     []byte("second"),
	})

	behind := metric.PrometheusFormat()
	if got, ok := issue819Sample(t, behind, "arc_replication_lag_entries", "reader-a"); !ok || got != 1 {
		t.Fatalf("reader-a lag after writer advances: %v, present=%v", got, ok)
	}
	if got, ok := issue819Sample(t, behind, "arc_replication_lag_entries", "reader-b"); !ok || got != 2 {
		t.Fatalf("reader-b lag after writer advances: %v, present=%v", got, ok)
	}

	if seconds, ok := issue819Sample(t, behind, "arc_replication_lag_seconds", "reader-a"); !ok || seconds < 7 || seconds > 12 {
		t.Fatalf("reader-a outstanding entry age: %v, present=%v", seconds, ok)
	}
	if seconds, ok := issue819Sample(t, behind, "arc_replication_lag_seconds", "reader-b"); !ok || seconds < 19 || seconds > 24 {
		t.Fatalf("reader-b outstanding entry age: %v, present=%v", seconds, ok)
	}

	// The first reader catches up; the second remains independently behind.
	a.lastAck.Store(2)
	caughtUp := metric.PrometheusFormat()
	if got, ok := issue819Sample(t, caughtUp, "arc_replication_lag_entries", "reader-a"); !ok || got != 0 {
		t.Fatalf("reader-a failed to catch up: %v, present=%v", got, ok)
	}
	if got, ok := issue819Sample(t, caughtUp, "arc_replication_lag_seconds", "reader-a"); !ok || got != 0 {
		t.Fatalf("caught-up reader-a has nonzero seconds: %v, present=%v", got, ok)
	}
	if got, ok := issue819Sample(t, caughtUp, "arc_replication_lag_entries", "reader-b"); !ok || got != 2 {
		t.Fatalf("reader-b was incorrectly affected by reader-a: %v, present=%v", got, ok)
	}

	// Sequence reset / future acknowledgment must never wrap around.
	b.lastAck.Store(3)
	reset := metric.PrometheusFormat()
	if got, ok := issue819Sample(t, reset, "arc_replication_lag_entries", "reader-b"); !ok || got != 0 {
		t.Fatalf("future acknowledgment underflow: %v, present=%v", got, ok)
	}

	// Removed readers disappear immediately, not after process restart.
	sender.RemoveReader("reader-a")
	removed := metric.PrometheusFormat()
	if strings.Contains(removed, `peer="reader-a"`) {
		t.Fatal("removed reader left stale Prometheus samples")
	}

	// A new connection with the same ID has its own acknowledgment state.
	addReader("reader-a", 2)
	reconnected := metric.PrometheusFormat()
	if got, ok := issue819Sample(t, reconnected, "arc_replication_lag_entries", "reader-a"); !ok || got != 0 {
		t.Fatalf("same-ID reconnect retained old lag: %v, present=%v", got, ok)
	}

	sender.RemoveReader("reader-a")
	sender.RemoveReader("reader-b")

	if err := sender.Stop(); err != nil {
		t.Fatal(err)
	}

	stopped := metric.PrometheusFormat()
	if strings.Contains(stopped, "arc_replication_lag_entries{peer=") ||
		strings.Contains(stopped, "arc_replication_lag_seconds{peer=") {
		t.Fatal("stopped sender left stale Prometheus series")
	}
}

// A disconnected old connection may finish its receive loop after a
// replacement with the same ID has already been registered. Cleanup of
// the old connection must not remove the replacement or its lag series.
func TestSenderReconnectKeepsReplacementIssue819(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   16,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
	})

	if err := sender.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sender.Stop()

	makeReader := func(nonce string, acknowledged uint64) *ReaderConnection {
		t.Helper()

		conn, peer := net.Pipe()
		t.Cleanup(func() { peer.Close() })

		reader, err := sender.PrepareReader(
			conn,
			"reader-1",
			nonce,
			acknowledged,
		)
		if err != nil {
			t.Fatalf("prepare reader: %v", err)
		}

		return reader
	}

	old := makeReader(testSenderNonce+"-old", 0)
	sender.ActivateReader(old)

	replacement := makeReader(testSenderNonce+"-new", 2)
	sender.ActivateReader(replacement)

	// Reproduce old-connection cleanup after replacement. The real
	// receive loop calls this when the old socket closes.
	sender.removeReaderIfCurrent(old)

	sender.mu.RLock()
	current := sender.readers["reader-1"]
	count := len(sender.readers)
	sender.mu.RUnlock()

	if current != replacement || count != 1 {
		t.Fatalf(
			"old cleanup removed replacement: current=%p, want=%p, readers=%d",
			current, replacement, count,
		)
	}

	sender.sequence.Store(4)

	output := metrics.Get().PrometheusFormat()

	lag, present := issue819Sample(
		t, output, "arc_replication_lag_entries", "reader-1",
	)
	if !present || lag != 2 {
		t.Fatalf(
			"replacement lag missing or stale: got=%v, present=%v",
			lag, present,
		)
	}

	// Once the actual current connection is removed, its metric must
	// disappear rather than remain under a historical peer ID.
	sender.RemoveReader("reader-1")

	if strings.Contains(
		metrics.Get().PrometheusFormat(),
		`arc_replication_lag_entries{peer="reader-1"}`,
	) {
		t.Fatal("removed replacement left a stale lag series")
	}
}

// Entries must remain visible even if their timestamps have aged out
// of the bounded ring. An unavailable timestamp is not a zero-second
// lag measurement.
func TestReplicationLagTimestampEvictionIssue819(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize: 2,
		Logger:     zerolog.Nop(),
	})

	sender.sequence.Store(3)

	reader := &ReaderConnection{id: "reader-1"}
	sender.readers[reader.id] = reader

	// Sequence 3 occupies the same two-slot ring position that
	// sequence 1 previously occupied.
	sender.timestamps[3%uint64(len(sender.timestamps))] =
		replicationTimestamp{
			sequence:  3,
			timestamp: uint64(time.Now().Add(-5 * time.Second).UnixMicro()),
		}

	samples := sender.replicationLagSamples()
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want 1", len(samples))
	}

	if samples[0].Entries != 3 {
		t.Fatalf("entries after eviction = %d, want 3", samples[0].Entries)
	}
	if samples[0].HasSeconds {
		t.Fatalf("evicted timestamp produced fabricated seconds: %+v", samples[0])
	}

	// Once the reader reaches an entry whose timestamp is known,
	// seconds should become available again.
	reader.lastAck.Store(2)

	samples = sender.replicationLagSamples()
	if len(samples) != 1 || samples[0].Entries != 1 ||
		!samples[0].HasSeconds ||
		samples[0].Seconds < 4 ||
		samples[0].Seconds > 10 {
		t.Fatalf("available timestamp not reported correctly: %+v", samples)
	}

	// An idle, caught-up connection must always report zero.
	reader.lastAck.Store(3)

	samples = sender.replicationLagSamples()
	if len(samples) != 1 || samples[0].Entries != 0 ||
		!samples[0].HasSeconds || samples[0].Seconds != 0 {
		t.Fatalf("caught-up reader has misleading lag: %+v", samples)
	}
}
