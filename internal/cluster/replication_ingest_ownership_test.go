package cluster

import (
	"context"
	"encoding/binary"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Exercise the actual authenticated receiver/coordinator path, including the
// local WAL append and its ownership transfer. No handler adapter hides the
// coordinator's interfaces from the receiver.
func replicationOwnershipFixture(t *testing.T, withBuffer bool) (*replication.Sender, *replication.Receiver, *wal.Writer, *ingest.ArrowBuffer) {
	t.Helper()
	logger := zerolog.Nop()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeFsync, Logger: logger})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	var buffer *ingest.ArrowBuffer
	if withBuffer {
		backend, err := storage.NewLocalBackend(t.TempDir(), logger)
		require.NoError(t, err)
		buffer = ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 600000,
			Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 8}, backend, logger)
		buffer.SetWAL(w)
		t.Cleanup(func() { require.NoError(t, buffer.Close()) })
	}
	c := &Coordinator{ingestBuffer: buffer, logger: logger}
	const secret = "replication-ownership-test-secret"
	sender := replication.NewSender(&replication.SenderConfig{BufferSize: 100, WriteTimeout: time.Second,
		SharedSecret: secret, ClusterName: "ownership-test", LocalNodeID: "writer", Logger: logger})
	require.NoError(t, sender.Start(context.Background()))
	t.Cleanup(func() { sender.Stop() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	activated := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			activated <- err
			return
		}
		msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
		if err != nil {
			_ = conn.Close()
			activated <- err
			return
		}
		syncReq := msg.Payload.(*protocol.ReplicateSync)
		reader, err := sender.PrepareReader(conn, syncReq.ReaderID, syncReq.Nonce, syncReq.LastKnownSequence)
		if err != nil {
			_ = conn.Close()
			activated <- err
			return
		}
		if syncReq.SupportsBinaryEntries {
			reader.EnableBinaryEntries()
		}
		err = protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgReplicateSyncAck,
			Payload: &protocol.ReplicateSyncAck{CurrentSequence: 0, CanResume: true}}, 5*time.Second)
		if err != nil {
			reader.Discard()
			activated <- err
			return
		}
		sender.ActivateReader(reader)
		activated <- nil
	}()
	receiver := replication.NewReceiver(&replication.ReceiverConfig{ReaderID: "reader", WriterAddr: ln.Addr().String(),
		SharedSecret: secret, ClusterName: "ownership-test", LocalWAL: w, IngestHandler: c.buildReplicationIngestHandler(),
		AckInterval: time.Hour, ReconnectInterval: time.Hour, Logger: logger})
	require.NoError(t, receiver.Start(context.Background()))
	t.Cleanup(func() { receiver.Stop() })
	select {
	case err := <-activated:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("replication handshake timed out")
	}
	return sender, receiver, w, buffer
}

func sendOwnershipPayload(t *testing.T, sender *replication.Sender, receiver *replication.Receiver, payload interface{}) {
	t.Helper()
	encoded, err := msgpack.Marshal(payload)
	require.NoError(t, err)
	sendOwnershipBytes(t, sender, receiver, encoded)
}

func sendOwnershipBytes(t *testing.T, sender *replication.Sender, receiver *replication.Receiver, encoded []byte) {
	t.Helper()
	sender.Replicate(&replication.ReplicateEntry{TimestampUS: uint64(time.Now().UnixMicro()), Payload: encoded})
	require.Eventually(t, func() bool { return receiver.LastSequence() == 1 }, 5*time.Second, time.Millisecond,
		"receiver did not finish applying the entry")
}

func TestReplicationIgnoredPayloadDoesNotPinWAL(t *testing.T) {
	for _, test := range []struct {
		name    string
		buffer  bool
		payload interface{}
	}{
		{"no_buffer", false, []map[string]interface{}{{"m": "cpu", "value": int64(1)}}},
		{"unknown_format", true, map[string]interface{}{"unrecognized": true}},
		{"no_measurement", true, []map[string]interface{}{{"value": int64(1)}}},
		{"no_columns", true, []map[string]interface{}{{"m": "cpu"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			sender, receiver, w, _ := replicationOwnershipFixture(t, test.buffer)
			sendOwnershipPayload(t, sender, receiver, test.payload)
			require.Zero(t, w.PendingUnflushedCount(), "ignored data left a sequence-floor pin with no buffer owner")
		})
	}
}

func TestReplicationMeasurementsOwnIndependentWALIdentities(t *testing.T) {
	for _, database := range []string{"default", "tenant_owned"} {
		t.Run(database, func(t *testing.T) {
			testReplicationOwnershipDatabase(t, 1, nil, database)
		})
	}
}

func testReplicationMeasurementsOwnIndependentWALIdentities(t *testing.T, rowsPerMeasurement int, sourceLines []string) {
	t.Helper()
	testReplicationOwnershipDatabase(t, rowsPerMeasurement, sourceLines, "default")
}

func ownershipEnvelope(database string, encoded []byte) []byte {
	if database == "default" {
		return encoded
	}
	prefix := make([]byte, 3+len(database))
	prefix[0] = wal.WALEnvelopeMarker
	binary.BigEndian.PutUint16(prefix[1:3], uint16(len(database)))
	copy(prefix[3:], database)
	return append(prefix, encoded...)
}

func testReplicationOwnershipDatabase(t *testing.T, rowsPerMeasurement int, sourceLines []string, database string) {
	t.Helper()
	sender, receiver, w, buffer := replicationOwnershipFixture(t, true)
	rows := make([]map[string]interface{}, 0, 2*rowsPerMeasurement)
	for _, measurement := range []string{"cpu", "memory"} {
		for i := 0; i < rowsPerMeasurement; i++ {
			row := map[string]interface{}{"m": measurement, "time": int64(1700000000000000 + i), "value": int64(i)}
			if len(sourceLines) > 0 {
				row["source_line"] = sourceLines[len(rows)]
			}
			rows = append(rows, row)
		}
	}
	encoded, err := msgpack.Marshal(rows)
	require.NoError(t, err)
	sendOwnershipBytes(t, sender, receiver, ownershipEnvelope(database, encoded))
	if got := w.PendingUnflushedCount(); got != 2 {
		t.Errorf("each measurement needs its own WAL identity: got %d pending, want 2", got)
	}
	// Force only cpu's original batch to storage via schema evolution. The
	// new batch has no WAL identity; memory must remain pinned independently.
	require.NoError(t, buffer.WriteColumnarDirectNoWAL(context.Background(), database, "cpu",
		map[string][]interface{}{"time": {int64(1700000001000000)}, "new_field": {int64(1)}}))
	if got := w.PendingUnflushedCount(); got != 1 {
		t.Errorf("first measurement released another buffer's identity: got %d pending, want 1", got)
	}
	require.NoError(t, w.Rotate())
	files, err := filepath.Glob(filepath.Join(filepath.Dir(w.CurrentFile()), "*.wal"))
	require.NoError(t, err)
	unflushed := map[string]int{}
	checkpoints := map[string]bool{}
	var entries []wal.Entry
	for _, file := range files {
		read, err := wal.NewReader(file, zerolog.Nop()).ReadAll()
		require.NoError(t, err)
		entries = append(entries, read...)
		for _, entry := range read {
			for _, id := range entry.CheckpointHashes {
				checkpoints[id] = true
			}
		}
	}
	for _, entry := range entries {
		if !checkpoints[entry.PayloadHash] {
			for _, row := range entry.Records {
				unflushed[row["m"].(string)]++
			}
			if entry.ColumnarData != nil {
				recoveredDB := entry.ColumnarData.Database
				if recoveredDB == "" {
					recoveredDB = "default"
				}
				require.Equal(t, database, recoveredDB, "split WAL part lost its database")
				unflushed[entry.ColumnarData.Measurement] += len(entry.ColumnarData.Columns["value"])
			}
		}
	}
	require.Zero(t, unflushed["cpu"], "flushed cpu data remains replayable")
	require.Equal(t, rowsPerMeasurement, unflushed["memory"], "pending memory data lost its independent WAL proof")
	require.NoError(t, buffer.NewRecoveryFlushBarrier()(context.Background()))
	require.Zero(t, w.PendingUnflushedCount())
}

func TestReplicationPreparationPreservesSingleMeasurementPayload(t *testing.T) {
	_, _, _, buffer := replicationOwnershipFixture(t, true)
	c := &Coordinator{ingestBuffer: buffer, logger: zerolog.Nop()}
	preparer := c.buildReplicationIngestHandler().(replication.WALIngestPreparer)
	for _, payload := range []interface{}{
		[]map[string]interface{}{{"m": "cpu", "value": int64(1)}},
		map[string]interface{}{"m": "cpu", "columns": map[string][]interface{}{"value": {int64(1)}}},
	} {
		for _, database := range []string{"default", "tenant_owned"} {
			encoded, err := msgpack.Marshal(payload)
			require.NoError(t, err)
			encoded = ownershipEnvelope(database, encoded)
			parts, err := preparer.PrepareReplicatedEntry(context.Background(), encoded)
			require.NoError(t, err)
			require.Len(t, parts, 1)
			require.Equal(t, encoded, parts[0].Payload, "normal replication must preserve the original WAL payload")
		}
	}
}

func TestReplicationRejectedMeasurementKeepsEarlierOwner(t *testing.T) {
	sender, receiver, w, buffer := replicationOwnershipFixture(t, true)
	encoded, err := msgpack.Marshal([]map[string]interface{}{
		{"m": "cpu", "time": int64(1700000000000000), "value": int64(1)},
		{"m": "memory", "time": "invalid timestamp", "value": int64(2)},
	})
	require.NoError(t, err)
	sender.Replicate(&replication.ReplicateEntry{TimestampUS: uint64(time.Now().UnixMicro()), Payload: encoded})
	require.Eventually(t, func() bool { return receiver.Stats()["total_errors"].(int64) == 1 }, 5*time.Second, time.Millisecond)
	require.Zero(t, receiver.LastSequence(), "a partially rejected entry must not be acknowledged as complete")
	require.Equal(t, 1, w.PendingUnflushedCount(), "the accepted cpu buffer must retain its independent identity")
	require.NoError(t, buffer.NewRecoveryFlushBarrier()(context.Background()))
	require.Zero(t, w.PendingUnflushedCount(), "only the accepted buffer owns a pending identity")
}
