package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// latchFailBackend fails every write while fail is set.
type latchFailBackend struct {
	storage.Backend
	fail atomic.Bool
}

func (b *latchFailBackend) Write(ctx context.Context, path string, data []byte) error {
	if b.fail.Load() {
		return errors.New("injected storage failure")
	}
	return b.Backend.Write(ctx, path, data)
}

func (b *latchFailBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if b.fail.Load() {
		return errors.New("injected storage failure")
	}
	return b.Backend.WriteReader(ctx, path, r, size)
}

// latchPeer is a coordinator answering a peer flush from a real buffer whose
// storage can be made to fail, logging to the returned buffer.
func latchPeer(t *testing.T) (*Coordinator, *ingest.ArrowBuffer, *latchFailBackend, *bytes.Buffer) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	backend := &latchFailBackend{Backend: local}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 8}, backend, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })
	logs := &bytes.Buffer{}
	c := &Coordinator{logger: zerolog.New(logs)}
	c.SetDeleteScopeFlusher(buf)
	return c, buf, backend, logs
}

// latchFailFlush makes a real flush of db/cpu fail.
func latchFailFlush(t *testing.T, buf *ingest.ArrowBuffer, backend *latchFailBackend) {
	t.Helper()
	backend.fail.Store(true)
	defer backend.fail.Store(false)
	before := buf.FlushFailureGeneration()
	ts := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC).UnixMicro()
	if err := buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{"time": {ts, ts + 1}, "value": {1.0, 2.0}}); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAllAndWait(context.Background()); err == nil {
		t.Fatal("the flush did not fail")
	}
	for deadline := time.Now().Add(5 * time.Second); buf.FlushFailureGeneration() == before; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the flush failed without moving the failure generation")
		}
	}
}

// #1938 W1 on the peer flush a shared-storage DELETE asks of this node: a
// flush failing between the latch read and the flush answers failed, not ok.
func TestPeerFlushRefusesAFlushFailureBetweenTheCheckAndTheFlush1938(t *testing.T) {
	c, buf, backend, _ := latchPeer(t)
	localFlushAfterState = func() { latchFailFlush(t, buf, backend) } // runs on the test goroutine
	t.Cleanup(func() { localFlushAfterState = nil })

	if status, _ := c.localFlushAnswer("db", "cpu"); status != protocol.FlushStatusFailed {
		t.Fatalf("status %q, want %q: the DELETE would not see the failed rows, which recovery brings back", status, protocol.FlushStatusFailed)
	}
}

// Each reason is refused and logged as itself (#1170).
func TestPeerFlushNamesTheReason1938(t *testing.T) {
	t.Run("a flush failed", func(t *testing.T) {
		c, buf, backend, logs := latchPeer(t)
		latchFailFlush(t, buf, backend)
		if status, _ := c.localFlushAnswer("db", "cpu"); status != protocol.FlushStatusFailed {
			t.Fatalf("status %q", status)
		}
		if !bytes.Contains(logs.Bytes(), []byte("an earlier flush failed and its rows wait for WAL recovery")) {
			t.Fatalf("log %s", logs)
		}
	})
	t.Run("a recovery pass left files", func(t *testing.T) {
		c, buf, _, logs := latchPeer(t)
		buf.MarkWALRecoveryPending()
		if status, _ := c.localFlushAnswer("db", "cpu"); status != protocol.FlushStatusFailed {
			t.Fatalf("status %q", status)
		}
		if !bytes.Contains(logs.Bytes(), []byte("a WAL recovery pass left files to replay")) || bytes.Contains(logs.Bytes(), []byte("flush failed")) {
			t.Fatalf("log %s", logs)
		}
	})
	t.Run("clear", func(t *testing.T) {
		c, _, _, _ := latchPeer(t)
		if status, _ := c.localFlushAnswer("db", "cpu"); status == protocol.FlushStatusFailed {
			t.Fatalf("status %q", status)
		}
	})
}
