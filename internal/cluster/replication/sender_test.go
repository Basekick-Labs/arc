package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pinned values for sender tests. The exact strings don't matter — they
// just have to be non-empty so AcceptReader doesn't take the
// refuse-when-unconfigured path.
const (
	testSenderSecret = "test-replication-shared-secret"
	testSenderNonce  = "00000000000000000000000000000001"
)

func TestSenderStartStop(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	assert.True(t, sender.running.Load())

	err = sender.Stop()
	require.NoError(t, err)
	assert.False(t, sender.running.Load())
}

func TestSenderAcceptReader(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Create a pipe to simulate connection
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// AcceptReader no longer writes the sync ack — the coordinator
	// now owns that wire framing (it sends a protocol-format ack
	// instead of the replication-format ack). The test just verifies
	// that AcceptReader returns success and the reader is registered.
	require.NoError(t, sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0))

	currentSeq, canResume := sender.CurrentSequenceAndCanResume(0)
	assert.Equal(t, uint64(0), currentSeq)
	assert.True(t, canResume)

	assert.Equal(t, 1, sender.ReaderCount())
}

func TestSenderReplicate(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Create a pipe to simulate connection
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Accept reader (no sync ack drain — coordinator owns that now).
	require.NoError(t, sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0))

	// Send entry through sender
	testPayload := []byte(`{"test": "data"}`)
	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().UnixMicro()),
		Payload:     testPayload,
	})

	// Read entry from client side
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, payload, err := ReadMessage(clientConn)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateEntry, msgType)

	entry, err := ParseEntry(payload)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), entry.Sequence)
	assert.Equal(t, testPayload, entry.Payload)
}

func TestSenderBroadcastToMultipleReaders(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Create two reader connections
	serverConn1, clientConn1 := net.Pipe()
	defer serverConn1.Close()
	defer clientConn1.Close()

	serverConn2, clientConn2 := net.Pipe()
	defer serverConn2.Close()
	defer clientConn2.Close()

	// Accept readers sequentially. AcceptReader no longer writes a
	// sync ack — coordinator owns that — so we just call it directly.
	require.NoError(t, sender.AcceptReader(serverConn1, "reader-1", testSenderNonce, 0))
	// Distinct nonce per connection so each derives a distinct
	// session key (HKDF is deterministic given the same inputs).
	require.NoError(t, sender.AcceptReader(serverConn2, "reader-2", testSenderNonce+"-2", 0))

	assert.Equal(t, 2, sender.ReaderCount())

	// Set read deadlines BEFORE Replicate so both conns are ready to drain
	// the broadcast as soon as the writes start. Broadcast iterates readers
	// in map order (non-deterministic); if we drain serially we head-of-
	// line-block whichever conn the broadcast picks second, the
	// distributionLoop's per-reader WriteTimeout (1s) trips, and that
	// reader gets removed via RemoveReader → conn.Close, leaving the test
	// to read EOF from the now-closed pipe. Drain both in parallel.
	clientConn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientConn2.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Send entry
	testPayload := []byte(`{"test": "broadcast"}`)
	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().UnixMicro()),
		Payload:     testPayload,
	})

	// Read both conns concurrently to avoid head-of-line blocking the
	// broadcast loop.
	type readResult struct {
		msgType byte
		payload []byte
		err     error
	}
	read := func(conn net.Conn) <-chan readResult {
		ch := make(chan readResult, 1)
		go func() {
			msgType, payload, err := ReadMessage(conn)
			ch <- readResult{msgType, payload, err}
		}()
		return ch
	}

	r1 := read(clientConn1)
	r2 := read(clientConn2)

	res1 := <-r1
	require.NoError(t, res1.err)
	assert.Equal(t, MsgReplicateEntry, res1.msgType)
	entry1, _ := ParseEntry(res1.payload)
	assert.Equal(t, testPayload, entry1.Payload)

	res2 := <-r2
	require.NoError(t, res2.err)
	assert.Equal(t, MsgReplicateEntry, res2.msgType)
	entry2, _ := ParseEntry(res2.payload)
	assert.Equal(t, testPayload, entry2.Payload)
}

func TestSenderStats(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	stats := sender.Stats()
	assert.True(t, stats["running"].(bool))
	assert.Equal(t, 100, stats["buffer_size"].(int))
	assert.Equal(t, 0, stats["reader_count"].(int))
}

func TestSenderRemoveReader(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: testSenderSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Create reader connection
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	require.NoError(t, sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0))
	assert.Equal(t, 1, sender.ReaderCount())

	// Remove reader
	sender.RemoveReader("reader-1")
	assert.Equal(t, 0, sender.ReaderCount())
}
