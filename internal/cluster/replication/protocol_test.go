package replication

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadEntry(t *testing.T) {
	entry := &ReplicateEntry{
		Sequence:    42,
		TimestampUS: 1234567890,
		Payload:     []byte(`{"test": "data"}`),
	}

	var buf bytes.Buffer
	err := WriteEntry(&buf, entry)
	require.NoError(t, err)

	msgType, payload, err := ReadMessage(&buf)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateEntry, msgType)

	parsed, err := ParseEntry(payload)
	require.NoError(t, err)
	assert.Equal(t, entry.Sequence, parsed.Sequence)
	assert.Equal(t, entry.TimestampUS, parsed.TimestampUS)
	assert.Equal(t, entry.Payload, parsed.Payload)
}

func TestWriteReadAck(t *testing.T) {
	ack := &ReplicateAck{
		LastSequence: 100,
		ReaderID:     "reader-1",
	}

	var buf bytes.Buffer
	err := WriteAck(&buf, ack)
	require.NoError(t, err)

	msgType, payload, err := ReadMessage(&buf)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateAck, msgType)

	parsed, err := ParseAck(payload)
	require.NoError(t, err)
	assert.Equal(t, ack.LastSequence, parsed.LastSequence)
	assert.Equal(t, ack.ReaderID, parsed.ReaderID)
}

func TestWriteReadSync(t *testing.T) {
	sync := &ReplicateSync{
		ReaderID:          "reader-1",
		LastKnownSequence: 50,
	}

	var buf bytes.Buffer
	err := WriteSync(&buf, sync)
	require.NoError(t, err)

	msgType, payload, err := ReadMessage(&buf)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateSync, msgType)

	parsed, err := ParseSync(payload)
	require.NoError(t, err)
	assert.Equal(t, sync.ReaderID, parsed.ReaderID)
	assert.Equal(t, sync.LastKnownSequence, parsed.LastKnownSequence)
}

func TestWriteReadSyncAck(t *testing.T) {
	syncAck := &ReplicateSyncAck{
		CurrentSequence: 200,
		CanResume:       true,
	}

	var buf bytes.Buffer
	err := WriteSyncAck(&buf, syncAck)
	require.NoError(t, err)

	msgType, payload, err := ReadMessage(&buf)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateSyncAck, msgType)

	parsed, err := ParseSyncAck(payload)
	require.NoError(t, err)
	assert.Equal(t, syncAck.CurrentSequence, parsed.CurrentSequence)
	assert.Equal(t, syncAck.CanResume, parsed.CanResume)
}

func TestWriteReadError(t *testing.T) {
	errMsg := &ReplicateError{
		Code:    ErrCodeSequenceGap,
		Message: "Reader is too far behind",
	}

	var buf bytes.Buffer
	err := WriteError(&buf, errMsg)
	require.NoError(t, err)

	msgType, payload, err := ReadMessage(&buf)
	require.NoError(t, err)
	assert.Equal(t, MsgReplicateError, msgType)

	parsed, err := ParseError(payload)
	require.NoError(t, err)
	assert.Equal(t, errMsg.Code, parsed.Code)
	assert.Equal(t, errMsg.Message, parsed.Message)
}

func TestMessageTooLarge(t *testing.T) {
	// Create a payload that exceeds MaxMessageSize
	largePayload := make([]byte, MaxMessageSize+1)

	entry := &ReplicateEntry{
		Sequence:    1,
		TimestampUS: 1234567890,
		Payload:     largePayload,
	}

	var buf bytes.Buffer
	err := WriteEntry(&buf, entry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too large")
}
