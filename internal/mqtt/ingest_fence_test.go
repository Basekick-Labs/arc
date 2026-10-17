package mqtt

// #1207 S2: a message whose write is refused by ingest fencing is held and
// retried until the fence lifts. Returning would let the client library
// acknowledge it and drop it.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestOnMessageHoldsAFencedWriteUntilItIsAccepted(t *testing.T) {
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

	var refusals atomic.Int64
	buf.SetIngestGate(func() error {
		if refusals.Add(1) <= 2 {
			return fmt.Errorf("%w: test", ingest.ErrIngestFenced)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", Database: "iot"},
		arrowBuffer: buf, ctx: ctx, logger: zerolog.Nop()}

	done := make(chan struct{})
	go func() {
		s.onMessage(nil, fakeMessage{topic: "sensors/a", payload: []byte(`{"m":"cpu","v":1}`)})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("onMessage never returned after the fence lifted")
	}
	if refusals.Load() < 3 {
		t.Fatalf("returned after %d gate checks: the message was not held until accepted", refusals.Load())
	}
	if failed := s.messagesFailed.Load(); failed != 0 {
		t.Fatalf("%d messages counted failed: a fenced write was dropped", failed)
	}
}

// A write that stays fenced is given up within the bound: blocking the paho
// handler longer stalls the client and its keepalive.
func TestOnMessageDoesNotBlockOnAPersistentFence(t *testing.T) {
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
	buf.SetIngestGate(func() error { return fmt.Errorf("%w: test", ingest.ErrIngestFenced) })

	var subLog, refusals bytes.Buffer
	defer ingest.CaptureRefusalLog(zerolog.New(&refusals), time.Now)()
	s := &Subscriber{id: "s", config: &Subscription{ID: "s", Name: "s", Database: "iot"},
		arrowBuffer: buf, ctx: context.Background(), logger: zerolog.New(&subLog)}
	start := time.Now()
	s.onMessage(nil, fakeMessage{topic: "sensors/a", payload: []byte(`{"m":"cpu","v":1}`)})
	if took := time.Since(start); took > mqttFencedWait+time.Second {
		t.Fatalf("onMessage blocked %v on a persistent fence", took)
	}
	if s.messagesFailed.Load() != 1 {
		t.Fatal("the refused message was not counted failed")
	}
	// #1268: logged by reason, not as an ERR per message.
	if strings.Contains(subLog.String(), "Failed to process MQTT message") {
		t.Fatalf("a fence refusal logged per message:\n%s", subLog.String())
	}
	if !strings.Contains(refusals.String(), `"source":"mqtt sensors/a"`) {
		t.Fatalf("the refusal was not logged by reason:\n%s", refusals.String())
	}
}
