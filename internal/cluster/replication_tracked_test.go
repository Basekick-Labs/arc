package cluster

import (
	"context"
	"encoding/hex"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/replicaview"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestReplicationIdentityNegotiation(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		writer, reader, tamper bool
		rejection              string
	}{
		{name: "tracked", writer: true, reader: true},
		{name: "legacy"},
		{name: "legacy_reader", writer: true, rejection: "incompatible WAL identity protocol"},
		{name: "legacy_writer", reader: true, rejection: "incompatible WAL identity protocol"},
		{name: "downgrade", writer: true, reader: true, tamper: true, rejection: "authentication failed"},
		{name: "upgrade", tamper: true, rejection: "authentication failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w, err := wal.NewWriter(&wal.WriterConfig{WALDir: t.TempDir(), Logger: zerolog.Nop()})
			require.NoError(t, err)
			defer w.Close()
			buf := &ingest.ArrowBuffer{}
			if tc.writer {
				buf.SetReplicationPublisher(replicaview.NewView())
			}
			c := &Coordinator{ctx: ctx, cfg: &config.ClusterConfig{ReplicationEnabled: true, SharedSecret: "identity-test-secret", ClusterName: "handoff"}, localNode: NewNode("writer", "writer", RoleWriter, "handoff"), logger: zerolog.Nop(), walWriter: w, ingestBuffer: buf, nonceCache: security.NewNonceCache(security.HMACTimestampTolerance)}
			require.NoError(t, c.StartReplication())
			defer c.replicationSender.Stop()
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			nonce, err := security.GenerateNonce()
			require.NoError(t, err)
			req := &protocol.ReplicateSync{ReaderID: "reader", Nonce: nonce, ClusterName: "handoff", Timestamp: time.Now().Unix(), SupportsBinaryEntries: true, SupportsTrackedEntries: tc.reader}
			req.HMAC = security.ComputeReplicateSyncHMAC(c.cfg.SharedSecret, nonce, req.ReaderID, req.ClusterName, 0, true, req.Timestamp, tc.reader)
			if tc.tamper {
				req.SupportsTrackedEntries = !req.SupportsTrackedEntries
			}
			done := make(chan struct{})
			go func() { defer close(done); c.handleReplicateSync(server, req) }()
			msg, err := protocol.ReceiveMessage(client, 5*time.Second)
			require.NoError(t, err)
			ack, ok := msg.Payload.(*protocol.ReplicateSyncAck)
			require.True(t, ok)
			<-done
			if tc.rejection != "" {
				require.Contains(t, ack.Error, tc.rejection)
				_ = client.SetReadDeadline(time.Now().Add(time.Second))
				_, err = client.Read(make([]byte, 1))
				require.Error(t, err, "rejected connection must close")
				return
			}
			require.Empty(t, ack.Error)
			require.Equal(t, tc.writer, ack.TrackedEntries)
			ids, err := w.AppendTracked([]map[string]interface{}{{"m": "cpu", "time": int64(1700000000000000), "v": int64(42)}})
			require.NoError(t, err)
			require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
			kind, body, err := replication.ReadMessage(client)
			require.NoError(t, err)
			require.Equal(t, replication.MsgReplicateEntryBin, kind)
			entry, err := replication.ParseEntryBinary(body)
			require.NoError(t, err)
			key, err := security.DeriveReplicationSessionKey(c.cfg.SharedSecret, nonce)
			require.NoError(t, err)
			tag, err := hex.DecodeString(entry.Tag)
			require.NoError(t, err)
			require.NoError(t, security.ValidateReplicationEntryTag(key, entry.Sequence, entry.Payload, tag))
			if tc.writer {
				identity, _, err := wal.TrackedPayload(entry.Payload)
				require.NoError(t, err)
				require.Equal(t, ids[0], identity, "transport must preserve the identity already persisted by the primary WAL")
			} else {
				require.NotEqual(t, wal.WALTrackedMarker, entry.Payload[0])
			}
		})
	}
}
