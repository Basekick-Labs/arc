// #887: the replication sequence is a per-process counter that restarts at
// zero, while the receiver's high-water mark persists across reconnects and was
// never reset. A writer restart therefore made every entry it sent fail the
// receiver's strict-advance check, dropping the connection on each attempt —
// an infinite reconnect loop applying nothing until the writer's sequence
// climbed back past the receiver's old mark.
//
// The same mechanism is what makes re-targeting a receiver to a different
// writer unsafe, which is why #885 replaces the Receiver rather than
// re-pointing it.
package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const seqTestSecret = "sequence-space-test-shared-secret"

// youngWriter seeds a Sender's sequence without queuing entries, and serves
// exactly one replication connection on a real listener. It returns the
// address to dial.
//
// It mirrors Coordinator.AcceptReplicationConnection's ordering deliberately:
// CurrentSequence is read, the ack is written, and only then is the reader
// published to the broadcast map. With no pending entries, only entries
// explicitly replicated after activation can reach this reader.
func youngWriter(t *testing.T, sender *Sender, sequence uint64) (addr string, activated <-chan struct{}) {
	t.Helper()

	// Replicate queues entries asynchronously. Warmup entries could otherwise
	// remain queued until the reader connects and advance its mark before the
	// handshake assertion, or be mistaken for the first post-restart entry.
	sender.sequence.Store(sequence)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	ready := make(chan struct{})
	go func() {
		defer close(ready)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
		if err != nil {
			conn.Close()
			return
		}
		syncReq, ok := msg.Payload.(*protocol.ReplicateSync)
		if !ok {
			conn.Close()
			return
		}
		reader, err := sender.PrepareReader(conn, syncReq.ReaderID, syncReq.Nonce, syncReq.LastKnownSequence)
		if err != nil {
			return
		}
		if syncReq.SupportsBinaryEntries {
			reader.EnableBinaryEntries()
		}
		currentSeq, canResume := sender.CurrentSequenceAndCanResume(syncReq.LastKnownSequence)
		if err := protocol.SendMessage(conn, &protocol.Message{
			Type:    protocol.MsgReplicateSyncAck,
			Payload: &protocol.ReplicateSyncAck{CurrentSequence: currentSeq, CanResume: canResume},
		}, 5*time.Second); err != nil {
			reader.Discard()
			return
		}
		sender.ActivateReader(reader)
	}()

	return ln.Addr().String(), ready
}

func newSeqTestSender(t *testing.T) *Sender {
	t.Helper()
	sender := NewSender(&SenderConfig{
		BufferSize:         100,
		WriteTimeout:       time.Second,
		Logger:             zerolog.Nop(),
		SharedSecret:       seqTestSecret,
		ClusterName:        "test-cluster",
		LocalNodeID:        "writer-1",
		CheckpointInterval: 1000,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, sender.Start(ctx))
	t.Cleanup(func() { sender.Stop() })
	return sender
}

// The regression. A receiver carrying a high mark meets a writer that restarted
// and is only twelve entries into its new life. Before the fix the connection
// was dropped on the first entry and nothing was ever applied.
func TestReceiverRewindsWhenWriterSequenceMovesBackwards(t *testing.T) {
	sender := newSeqTestSender(t)
	addr, activated := youngWriter(t, sender, 12)

	appliedCh := make(chan []byte, 8)
	r := NewReceiver(&ReceiverConfig{
		ReaderID:          "reader-1",
		WriterAddr:        addr,
		ReconnectInterval: 200 * time.Millisecond,
		AckInterval:       time.Hour, // no acks needed for this assertion
		Logger:            zerolog.Nop(),
		SharedSecret:      seqTestSecret,
		ClusterName:       "test-cluster",
		IngestHandler: IngestHandlerFunc(func(_ context.Context, payload []byte) error {
			appliedCh <- payload
			return nil
		}),
	})

	// The receiver has been running against this writer's PREVIOUS process and
	// applied five thousand entries from that sequence space.
	r.lastSeq.Store(5000)

	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { r.Stop() })

	select {
	case <-activated:
	case <-time.After(10 * time.Second):
		t.Fatal("writer never activated the reader")
	}

	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().UnixMicro()),
		Payload:     []byte("after-restart"),
	})

	select {
	case got := <-appliedCh:
		require.Equal(t, "after-restart", string(got))
	case <-time.After(10 * time.Second):
		t.Fatal("the receiver never applied an entry from the restarted writer: it kept its stale sequence mark, " +
			"so every entry failed the strict-advance check and the connection was dropped on each reconnect (#887)")
	}
}

// The other half of the line: when the writer is AHEAD of the receiver — the
// ordinary "we missed entries while disconnected" case — the mark must be left
// alone, or the rewind would quietly disarm the replay check it is reconciling.
func TestReceiverKeepsItsMarkWhenWriterIsAhead(t *testing.T) {
	sender := newSeqTestSender(t)
	addr, activated := youngWriter(t, sender, 900)

	r := NewReceiver(&ReceiverConfig{
		ReaderID:          "reader-1",
		WriterAddr:        addr,
		ReconnectInterval: 200 * time.Millisecond,
		AckInterval:       time.Hour,
		Logger:            zerolog.Nop(),
		SharedSecret:      seqTestSecret,
		ClusterName:       "test-cluster",
	})

	// Behind the writer, but in the SAME sequence space.
	r.lastSeq.Store(100)
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { r.Stop() })

	select {
	case <-activated:
	case <-time.After(10 * time.Second):
		t.Fatal("writer never activated the reader")
	}

	// Give connect() time to have run its reconciliation.
	require.Eventually(t, func() bool { return r.IsConnected() }, 10*time.Second, 50*time.Millisecond)

	require.Equal(t, uint64(100), r.LastSequence(),
		"the receiver rewound its mark to a writer that was ahead of it; only a BACKWARDS move means a new sequence space")
}

// The underflow in CurrentSequenceAndCanResume. currentSeq-BufferSize wraps to
// ~1.8e19 while the writer is younger than one buffer's worth of entries, so
// canResume came back false for exactly the connections most likely to want it
// — a writer that just restarted.
//
// Nothing branches on the value today (it is carried to the receiver and
// logged), so this pins the arithmetic before something does.
func TestCanResumeDoesNotUnderflowOnAYoungWriter(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   10000,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: seqTestSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, sender.Start(ctx))
	t.Cleanup(func() { sender.Stop() })

	// Ten entries emitted against a ten-thousand-entry buffer: every sequence
	// the writer has ever issued is still within the window, so any reader
	// position can be resumed.
	for i := 0; i < 10; i++ {
		sender.Replicate(&ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("warmup"),
		})
	}

	currentSeq, canResume := sender.CurrentSequenceAndCanResume(5)
	require.Equal(t, uint64(10), currentSeq)
	require.True(t, canResume,
		"canResume was false for a reader at sequence 5 against a writer at 10 with a 10000-entry buffer; "+
			"currentSeq-BufferSize underflowed")
}

// The window must still mean something once the writer is past it: a reader far
// behind a long-running writer cannot resume.
func TestCanResumeIsFalseOutsideTheWindow(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:   10,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: seqTestSecret,
		ClusterName:  "test-cluster",
		LocalNodeID:  "writer-1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, sender.Start(ctx))
	t.Cleanup(func() { sender.Stop() })

	for i := 0; i < 100; i++ {
		sender.Replicate(&ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("warmup"),
		})
	}

	_, canResume := sender.CurrentSequenceAndCanResume(5)
	require.False(t, canResume,
		"a reader at sequence 5 is 95 entries behind a writer with a 10-entry window; that is not resumable")
}
