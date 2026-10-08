package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

type trackedReceiverWAL struct {
	raw, received int
	payload       []byte
	err           error
}

func (w *trackedReceiverWAL) AppendRaw([]byte) error {
	w.raw++
	return errors.New("received entry used originating append")
}
func (w *trackedReceiverWAL) AppendReplicated(payload []byte) error {
	w.received++
	w.payload = bytes.Clone(payload)
	return w.err
}

func TestReceiverTrackedEntryPersistsBeforeApply(t *testing.T) {
	payload := make([]byte, 18)
	payload[0] = wal.WALTrackedMarker
	binary.BigEndian.PutUint64(payload[1:9], 123)
	binary.BigEndian.PutUint64(payload[9:17], 1)
	payload[17] = 0x80
	local := &trackedReceiverWAL{}
	applied := 0
	receiver := NewReceiver(&ReceiverConfig{TrackedEntries: true, LocalWAL: local, Logger: zerolog.Nop(), IngestHandler: IngestHandlerFunc(func(_ context.Context, got []byte) error {
		if local.received != 1 {
			t.Fatal("applied entry before received WAL append")
		}
		if !bytes.Equal(got, payload) {
			t.Fatal("identity stripped before replica view")
		}
		applied++
		return nil
	})})
	receiver.ctx = context.Background()
	if err := receiver.applyEntry(&ReplicateEntry{Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if local.raw != 0 || local.received != 1 || applied != 1 {
		t.Fatalf("raw=%d received=%d applied=%d", local.raw, local.received, applied)
	}
	local.err = errors.New("disk failure")
	if err := receiver.applyEntry(&ReplicateEntry{Payload: payload}); err == nil {
		t.Fatal("ignored local WAL failure")
	}
	if applied != 1 {
		t.Fatal("applied after failed WAL append")
	}
}

func TestReceiverTrackedEntryRejectsLegacyAndReceivedMarkers(t *testing.T) {
	local := &trackedReceiverWAL{}
	receiver := NewReceiver(&ReceiverConfig{TrackedEntries: true, LocalWAL: local, Logger: zerolog.Nop()})
	receiver.ctx = context.Background()
	for _, payload := range [][]byte{nil, {0x80}, make([]byte, 17), append([]byte{wal.WALReplicatedMarker}, make([]byte, 17)...)} {
		if err := receiver.applyEntry(&ReplicateEntry{Payload: payload}); err == nil {
			t.Fatal("accepted payload without originating identity")
		}
	}
	if local.received != 0 || local.raw != 0 {
		t.Fatal("persisted unsupported wire payload")
	}
}

func TestReceiverRejectsIncompatibleIdentityAck(t *testing.T) {
	for _, tracked := range []bool{false, true} {
		t.Run(fmt.Sprint(tracked), func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()
			result := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					result <- err
					return
				}
				defer conn.Close()
				msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
				if err != nil {
					result <- err
					return
				}
				req := msg.Payload.(*protocol.ReplicateSync)
				if req.SupportsTrackedEntries != tracked {
					result <- fmt.Errorf("receiver advertised wrong identity mode")
					return
				}
				result <- protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgReplicateSyncAck, Payload: &protocol.ReplicateSyncAck{TrackedEntries: !tracked}}, 5*time.Second)
			}()
			r := NewReceiver(&ReceiverConfig{ReaderID: "reader", WriterAddr: ln.Addr().String(), SharedSecret: "test-secret", ClusterName: "test", TrackedEntries: tracked, Logger: zerolog.Nop()})
			r.ctx = context.Background()
			r.lastSeq.Store(42)
			err = r.connect()
			require.ErrorContains(t, err, "incompatible WAL identity protocol")
			require.False(t, r.connected.Load())
			require.Equal(t, uint64(42), r.lastSeq.Load(), "rejection must precede any sequence-space reset")
			require.NoError(t, <-result)
		})
	}
}
