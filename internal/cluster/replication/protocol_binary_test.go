// Tests for the MsgReplicateEntryBin binary framing (#698): a WAL entry
// near the 100MB payload cap could not be JSON-framed (base64 inflates
// 4/3, blowing the 100MB message cap), stalling replication behind it.
// Binary frames carry the payload as raw bytes and are only sent to
// readers that advertised support in their handshake.
package replication

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadEntryBinary(t *testing.T) {
	tests := []struct {
		name  string
		entry *ReplicateEntry
	}{
		{name: "with tag", entry: &ReplicateEntry{Sequence: 42, TimestampUS: 1234567890, Tag: "00112233445566778899aabbccddeeff"[:16], Payload: []byte(`{"test": "data"}`)}},
		{name: "empty tag", entry: &ReplicateEntry{Sequence: 1, TimestampUS: 9, Payload: []byte{0x01}}},
		{name: "empty payload", entry: &ReplicateEntry{Sequence: 7, TimestampUS: 8, Tag: "aabbccddeeff0011", Payload: nil}},
		{name: "binary payload with frame-header bytes", entry: &ReplicateEntry{Sequence: 1 << 40, TimestampUS: 1 << 50, Tag: "ffeeddccbbaa9988", Payload: []byte{0x00, 0x00, 0x00, 0x05, 0x10, 0xff}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, WriteEntryBinary(&buf, tt.entry))

			msgType, payload, err := ReadMessage(&buf)
			require.NoError(t, err)
			assert.Equal(t, MsgReplicateEntryBin, msgType)

			parsed, err := ParseEntryBinary(payload)
			require.NoError(t, err)
			assert.Equal(t, tt.entry.Sequence, parsed.Sequence)
			assert.Equal(t, tt.entry.TimestampUS, parsed.TimestampUS)
			assert.Equal(t, tt.entry.Tag, parsed.Tag)
			if len(tt.entry.Payload) == 0 {
				assert.Empty(t, parsed.Payload)
			} else {
				assert.Equal(t, tt.entry.Payload, parsed.Payload)
			}
		})
	}
}

// TestBinaryFramingCarriesWALCapPayload is the #698 regression: a payload
// in the ~75MB to 100MB band fails JSON framing (base64 inflation) but
// must round-trip through the binary framing.
func TestBinaryFramingCarriesWALCapPayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0xA5}, 90*1024*1024)
	payload[0], payload[len(payload)-1] = 0x01, 0x02
	entry := &ReplicateEntry{Sequence: 9, TimestampUS: 10, Tag: "0011223344556677", Payload: payload}

	// JSON framing rejects it — this is the pre-existing stall.
	var jsonBuf bytes.Buffer
	err := WriteEntry(&jsonBuf, entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "message too large")

	// Binary framing carries it.
	var buf bytes.Buffer
	require.NoError(t, WriteEntryBinary(&buf, entry))
	msgType, body, err := ReadMessage(&buf)
	require.NoError(t, err)
	require.Equal(t, MsgReplicateEntryBin, msgType)
	parsed, err := ParseEntryBinary(body)
	require.NoError(t, err)
	require.Equal(t, len(payload), len(parsed.Payload))
	assert.Equal(t, byte(0x01), parsed.Payload[0])
	assert.Equal(t, byte(0x02), parsed.Payload[len(parsed.Payload)-1])
	assert.True(t, bytes.Equal(payload, parsed.Payload))
}

func TestWriteEntryBinaryRejectsOversizedFrame(t *testing.T) {
	entry := &ReplicateEntry{Sequence: 1, TimestampUS: 1, Payload: make([]byte, MaxEntryFrameSize)}
	var buf bytes.Buffer
	err := WriteEntryBinary(&buf, entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "message too large")
}

func TestWriteEntryBinaryRejectsOverlongTag(t *testing.T) {
	entry := &ReplicateEntry{Sequence: 1, TimestampUS: 1, Tag: strings.Repeat("a", 256), Payload: []byte{0x01}}
	var buf bytes.Buffer
	err := WriteEntryBinary(&buf, entry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tag too long")
}

func TestParseEntryBinaryTruncated(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "empty", body: nil},
		{name: "short header", body: make([]byte, 5)},
		{name: "header minus one", body: make([]byte, 16)},
		{name: "tag overruns body", body: append(make([]byte, 16), 0xFF)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseEntryBinary(tt.body)
			require.Error(t, err)
		})
	}
}

// TestSenderFramingFollowsNegotiation pins both sides of the capability:
// a reader that negotiated binary entries gets MsgReplicateEntryBin with
// a tag that still validates under the session key, and a reader that
// did not (an old receiver) keeps getting JSON MsgReplicateEntry.
func TestSenderFramingFollowsNegotiation(t *testing.T) {
	for _, binary := range []bool{true, false} {
		name := "json"
		if binary {
			name = "binary"
		}
		t.Run(name, func(t *testing.T) {
			sender := NewSender(&SenderConfig{
				BufferSize:         100,
				WriteTimeout:       time.Second,
				Logger:             zerolog.Nop(),
				SharedSecret:       testSenderSecret,
				ClusterName:        "test-cluster",
				LocalNodeID:        "writer-1",
				CheckpointInterval: 1000,
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			require.NoError(t, sender.Start(ctx))
			defer sender.Stop()

			serverConn, clientConn := net.Pipe()
			defer serverConn.Close()
			defer clientConn.Close()

			reader, err := sender.PrepareReader(serverConn, "reader-1", testSenderNonce, 0)
			require.NoError(t, err)
			if binary {
				reader.EnableBinaryEntries()
			}
			sender.ActivateReader(reader)

			sessionKey, err := security.DeriveReplicationSessionKey(testSenderSecret, testSenderNonce)
			require.NoError(t, err)

			sender.Replicate(&ReplicateEntry{
				TimestampUS: uint64(time.Now().UnixMicro()),
				Payload:     []byte("negotiation-payload"),
			})

			clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			msgType, body, err := ReadMessage(clientConn)
			require.NoError(t, err)

			var entry *ReplicateEntry
			if binary {
				require.Equal(t, MsgReplicateEntryBin, msgType)
				entry, err = ParseEntryBinary(body)
			} else {
				require.Equal(t, MsgReplicateEntry, msgType)
				entry, err = ParseEntry(body)
			}
			require.NoError(t, err)
			assert.Equal(t, []byte("negotiation-payload"), entry.Payload)

			// The MAC tag is framing-agnostic: same validation as the
			// receiver performs on either framing.
			require.Len(t, entry.Tag, security.ReplicationEntryTagLen*2)
			tagBytes, err := hex.DecodeString(entry.Tag)
			require.NoError(t, err)
			require.NoError(t,
				security.ValidateReplicationEntryTag(sessionKey, entry.Sequence, entry.Payload, tagBytes))
		})
	}
}
