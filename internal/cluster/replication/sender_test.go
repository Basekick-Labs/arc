package replication

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSenderStartStop(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
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

	// Accept reader in goroutine (it will write sync ack)
	var acceptErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		acceptErr = sender.AcceptReader(serverConn, "reader-1", 0)
	}()

	// Read sync ack from client side
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, payload, err := ReadMessage(clientConn)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateSyncAck, msgType)

	syncAck, err := ParseSyncAck(payload)
	require.NoError(t, err)
	assert.True(t, syncAck.CanResume)

	wg.Wait()
	require.NoError(t, acceptErr)

	// Check reader count
	assert.Equal(t, 1, sender.ReaderCount())
}

func TestSenderReplicate(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
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

	// Accept reader
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sender.AcceptReader(serverConn, "reader-1", 0)
	}()

	// Read sync ack
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ReadMessage(clientConn) // Consume sync ack
	wg.Wait()

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

	// Accept readers sequentially to avoid deadlock on mutex
	// First reader
	go func() {
		sender.AcceptReader(serverConn1, "reader-1", 0)
	}()
	clientConn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	ReadMessage(clientConn1) // Consume sync ack
	time.Sleep(10 * time.Millisecond)

	// Second reader
	go func() {
		sender.AcceptReader(serverConn2, "reader-2", 0)
	}()
	clientConn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	ReadMessage(clientConn2) // Consume sync ack
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, 2, sender.ReaderCount())

	// Send entry
	testPayload := []byte(`{"test": "broadcast"}`)
	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().UnixMicro()),
		Payload:     testPayload,
	})

	// Both readers should receive the entry
	clientConn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	clientConn2.SetReadDeadline(time.Now().Add(2 * time.Second))

	msgType1, payload1, err1 := ReadMessage(clientConn1)
	require.NoError(t, err1)
	assert.Equal(t, MsgReplicateEntry, msgType1)
	entry1, _ := ParseEntry(payload1)
	assert.Equal(t, testPayload, entry1.Payload)

	msgType2, payload2, err2 := ReadMessage(clientConn2)
	require.NoError(t, err2)
	assert.Equal(t, MsgReplicateEntry, msgType2)
	entry2, _ := ParseEntry(payload2)
	assert.Equal(t, testPayload, entry2.Payload)
}

func TestSenderStats(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
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
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sender.Start(ctx)
	require.NoError(t, err)
	defer sender.Stop()

	// Create reader connection
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sender.AcceptReader(serverConn, "reader-1", 0)
	}()

	// Consume sync ack
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ReadMessage(clientConn)
	wg.Wait()

	assert.Equal(t, 1, sender.ReaderCount())

	// Remove reader
	sender.RemoveReader("reader-1")
	assert.Equal(t, 0, sender.ReaderCount())
}
