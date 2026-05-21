// Round-trip tests for the GHSA-wfgr-8x84-22q7 / CVE-2026-48106
// stream-authentication wiring: per-entry tag, periodic full-HMAC
// checkpoint, and refuse-when-unconfigured on the sender. The receiver
// side of the verification is exercised by re-running the same validate
// functions the receiver uses, without spinning up a full Receiver
// goroutine (which would otherwise need a live WAL writer).
package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcceptReaderRefusesWhenSharedSecretEmpty(t *testing.T) {
	// Refuse-when-unconfigured: a sender with no shared secret must
	// reject every AcceptReader. Matches handleReplicateSync.
	sender := NewSender(&SenderConfig{
		BufferSize:   100,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		// SharedSecret intentionally empty.
		ClusterName: "test-cluster",
		LocalNodeID: "writer-1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, sender.Start(ctx))
	defer sender.Stop()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	err := sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errSharedSecretRequired),
		"expected errSharedSecretRequired, got %v", err)
}

func TestAcceptReaderRefusesWhenHandshakeNonceEmpty(t *testing.T) {
	// Defensive: even with a shared secret, an empty handshake nonce
	// is a misconfiguration — the coordinator should have rejected
	// the MsgReplicateSync before reaching AcceptReader.
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
	require.NoError(t, sender.Start(ctx))
	defer sender.Stop()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	err := sender.AcceptReader(serverConn, "reader-1", "", 0)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errHandshakeNonceRequired),
		"expected errHandshakeNonceRequired, got %v", err)
}

// TestSenderStampsValidTagOnEveryEntry drives entries through the
// sender and verifies that what shows up on the wire (a) carries a tag
// and (b) the tag verifies against the same session key the receiver
// would derive. This is the green-path test for the per-entry MAC.
func TestSenderStampsValidTagOnEveryEntry(t *testing.T) {
	sender := NewSender(&SenderConfig{
		BufferSize:         100,
		WriteTimeout:       time.Second,
		Logger:             zerolog.Nop(),
		SharedSecret:       testSenderSecret,
		ClusterName:        "test-cluster",
		LocalNodeID:        "writer-1",
		CheckpointInterval: 1000, // higher than entry count below
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, sender.Start(ctx))
	defer sender.Stop()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// AcceptReader returns immediately now — the success ack is
	// sent by Coordinator.AcceptReplicationConnection via the
	// cluster-protocol wire format, not by AcceptReader itself.
	require.NoError(t, sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0))

	// Derive the same session key the sender derived, so we can
	// verify the per-entry tag like the receiver would.
	sessionKey, err := security.DeriveReplicationSessionKey(testSenderSecret, testSenderNonce)
	require.NoError(t, err)

	// Push 5 entries through the sender and read them off the wire.
	const n = 5
	for i := 0; i < n; i++ {
		sender.Replicate(&ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("payload-" + string(rune('A'+i))),
		})
	}

	for i := 0; i < n; i++ {
		clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		msgType, payload, err := ReadMessage(clientConn)
		require.NoError(t, err)
		require.Equal(t, MsgReplicateEntry, msgType,
			"expected entry, got msg type %d (no checkpoint expected at this interval)", msgType)

		entry, err := ParseEntry(payload)
		require.NoError(t, err)

		assert.NotEmpty(t, entry.Tag, "every entry must carry a MAC tag")
		tagBytes, err := hex.DecodeString(entry.Tag)
		require.NoError(t, err)
		require.Equal(t, security.ReplicationEntryTagLen, len(tagBytes))

		// Receiver-side verification: same key, same sequence, same
		// payload — must verify.
		require.NoError(t,
			security.ValidateReplicationEntryTag(sessionKey, entry.Sequence, entry.Payload, tagBytes),
			"entry %d tag verification failed", i)

		// Negative: flipping a single bit in the payload must
		// invalidate the tag.
		tamperedPayload := make([]byte, len(entry.Payload))
		copy(tamperedPayload, entry.Payload)
		tamperedPayload[0] ^= 0x01
		require.Error(t,
			security.ValidateReplicationEntryTag(sessionKey, entry.Sequence, tamperedPayload, tagBytes),
			"entry %d tampered payload accepted", i)

		// Negative: swapping the tag from another sequence must fail.
		require.Error(t,
			security.ValidateReplicationEntryTag(sessionKey, entry.Sequence+1, entry.Payload, tagBytes),
			"entry %d sequence-shifted tag accepted", i)
	}
}

// TestSenderEmitsCheckpointAtInterval drives exactly the configured
// CheckpointInterval entries and verifies a MsgReplicateCheckpoint
// follows, and that its HMAC + cumulative hash both verify.
func TestSenderEmitsCheckpointAtInterval(t *testing.T) {
	const checkpointInterval = 4
	sender := NewSender(&SenderConfig{
		BufferSize:         100,
		WriteTimeout:       time.Second,
		Logger:             zerolog.Nop(),
		SharedSecret:       testSenderSecret,
		ClusterName:        "test-cluster",
		LocalNodeID:        "writer-1",
		CheckpointInterval: checkpointInterval,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, sender.Start(ctx))
	defer sender.Stop()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	require.NoError(t, sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0))

	// Maintain the receiver's running hash as we drain entries off
	// the wire, exactly like receiveLoop would.
	cumulative := sha256.New()
	payloads := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
		[]byte("delta"),
	}
	require.Equal(t, checkpointInterval, len(payloads))

	for _, p := range payloads {
		sender.Replicate(&ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     p,
		})
	}

	for i := 0; i < checkpointInterval; i++ {
		clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		msgType, payload, err := ReadMessage(clientConn)
		require.NoError(t, err)
		require.Equal(t, MsgReplicateEntry, msgType)
		entry, err := ParseEntry(payload)
		require.NoError(t, err)
		cumulative.Write(entry.Payload)
	}

	// Next message must be the checkpoint.
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, payload, err := ReadMessage(clientConn)
	require.NoError(t, err)
	require.Equal(t, MsgReplicateCheckpoint, msgType,
		"expected checkpoint after %d entries, got msg type %d", checkpointInterval, msgType)

	cp, err := ParseCheckpoint(payload)
	require.NoError(t, err)

	// Hash on the wire must equal what we computed locally.
	theirHash, err := hex.DecodeString(cp.CumulativePayloadHashHex)
	require.NoError(t, err)
	assert.Equal(t, cumulative.Sum(nil), theirHash,
		"checkpoint cumulative hash diverges from receiver-side hash")

	// Full HMAC must verify.
	var hashArr [32]byte
	copy(hashArr[:], theirHash)
	require.NoError(t, security.ValidateReplicationCheckpointHMAC(
		testSenderSecret, cp.Nonce, cp.SenderNodeID, cp.ClusterName,
		hashArr, cp.LastSequence, cp.Timestamp, cp.HMAC, 5*time.Minute,
	))

	// LastSequence must be the last entry's sequence.
	assert.Equal(t, uint64(checkpointInterval), cp.LastSequence)
}

// TestSenderDoesNotEmitCheckpointBeforeInterval verifies the sender
// holds off until exactly CheckpointInterval entries have streamed.
func TestSenderDoesNotEmitCheckpointBeforeInterval(t *testing.T) {
	const checkpointInterval = 4
	sender := NewSender(&SenderConfig{
		BufferSize:         100,
		WriteTimeout:       time.Second,
		Logger:             zerolog.Nop(),
		SharedSecret:       testSenderSecret,
		ClusterName:        "test-cluster",
		LocalNodeID:        "writer-1",
		CheckpointInterval: checkpointInterval,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, sender.Start(ctx))
	defer sender.Stop()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	require.NoError(t, sender.AcceptReader(serverConn, "reader-1", testSenderNonce, 0))

	// Send checkpointInterval-1 entries — should NOT produce a
	// checkpoint yet.
	for i := 0; i < checkpointInterval-1; i++ {
		sender.Replicate(&ReplicateEntry{
			TimestampUS: uint64(time.Now().UnixMicro()),
			Payload:     []byte("p"),
		})
	}
	for i := 0; i < checkpointInterval-1; i++ {
		clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		msgType, _, err := ReadMessage(clientConn)
		require.NoError(t, err)
		require.Equal(t, MsgReplicateEntry, msgType)
	}

	// Anything additional should time out — no checkpoint yet.
	clientConn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	_, _, err := ReadMessage(clientConn)
	require.Error(t, err, "no message should have been emitted before checkpointInterval")
}

// TestSenderStampsDistinctTagsForEachReader verifies that broadcasting
// to multiple readers produces a different tag on each reader's wire
// copy of the entry, even though the sender mutates a shared
// *ReplicateEntry. This pins the "different session keys → different
// tags" property and would fail loudly if broadcastEntry is ever
// parallelized without making Tag per-reader. See the invariant
// documented on ReplicateEntry.Tag.
func TestSenderStampsDistinctTagsForEachReader(t *testing.T) {
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

	srvA, cliA := net.Pipe()
	defer srvA.Close()
	defer cliA.Close()
	srvB, cliB := net.Pipe()
	defer srvB.Close()
	defer cliB.Close()

	require.NoError(t, sender.AcceptReader(srvA, "reader-A", "nonce-a-aaaaaaaaaaaaaaaaaaaa", 0))
	require.NoError(t, sender.AcceptReader(srvB, "reader-B", "nonce-b-bbbbbbbbbbbbbbbbbbbb", 0))

	// One entry, broadcast to both readers.
	payload := []byte("broadcast-payload")
	// Drain both pipes concurrently — broadcast iterates readers in
	// map order, and a synchronous pipe write to one head-of-line-
	// blocks the other if we drain serially.
	cliA.SetReadDeadline(time.Now().Add(2 * time.Second))
	cliB.SetReadDeadline(time.Now().Add(2 * time.Second))

	type readResult struct {
		payload []byte
		err     error
	}
	chA := make(chan readResult, 1)
	chB := make(chan readResult, 1)
	go func() {
		_, p, e := ReadMessage(cliA)
		chA <- readResult{p, e}
	}()
	go func() {
		_, p, e := ReadMessage(cliB)
		chB <- readResult{p, e}
	}()

	sender.Replicate(&ReplicateEntry{
		TimestampUS: uint64(time.Now().UnixMicro()),
		Payload:     payload,
	})

	resA := <-chA
	require.NoError(t, resA.err)
	entryA, err := ParseEntry(resA.payload)
	require.NoError(t, err)

	resB := <-chB
	require.NoError(t, resB.err)
	entryB, err := ParseEntry(resB.payload)
	require.NoError(t, err)

	// Same sequence + same payload but DIFFERENT session keys → DIFFERENT tags.
	assert.Equal(t, entryA.Sequence, entryB.Sequence)
	assert.Equal(t, entryA.Payload, entryB.Payload)
	assert.NotEqual(t, entryA.Tag, entryB.Tag,
		"two readers must receive distinct tags on the same entry (per-connection key isolation)")
}

// TestEntryTagPerConnectionKeyIsolation verifies two different
// handshake nonces produce different session keys (HKDF determinism +
// nonce isolation): a tag valid on connection A must NOT verify under
// connection B's key.
func TestEntryTagPerConnectionKeyIsolation(t *testing.T) {
	keyA, err := security.DeriveReplicationSessionKey(testSenderSecret, "nonce-a-aaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	keyB, err := security.DeriveReplicationSessionKey(testSenderSecret, "nonce-b-bbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	require.NotEqual(t, keyA, keyB, "different nonces must yield different session keys")

	payload := []byte("isolated-payload")
	tagA := security.ComputeReplicationEntryTag(keyA, 1, payload)

	// Tag from connection A must NOT verify under connection B's key.
	require.Error(t,
		security.ValidateReplicationEntryTag(keyB, 1, payload, tagA),
		"per-connection key isolation broken: cross-connection tag accepted")
}
