package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// Message types for WAL replication protocol
const (
	// MsgReplicateEntry is a WAL entry from writer to reader
	MsgReplicateEntry byte = 0x10

	// MsgReplicateAck is an acknowledgment from reader to writer
	MsgReplicateAck byte = 0x11

	// MsgReplicateSync requests current replication position
	MsgReplicateSync byte = 0x12

	// MsgReplicateSyncAck responds with current replication position
	MsgReplicateSyncAck byte = 0x13

	// MsgReplicateCheckpoint is a periodic full-HMAC checkpoint sent by
	// the writer every N entries (N = SenderConfig.CheckpointInterval,
	// defaults to defaultCheckpointInterval = 1024 — see sender.go). It
	// carries the running SHA-256 hash of every entry payload observed
	// since the connection's handshake plus the last sequence covered,
	// signed with the cluster shared secret. The receiver verifies it
	// against its own running hash; mismatch drops the connection. This
	// anchors the per-entry truncated MAC tags (which are 8-byte
	// session-keyed) against a full-strength HMAC so an attacker who
	// somehow forges a stream-tag still gets caught at the next
	// checkpoint. See GHSA-wfgr-8x84-22q7.
	MsgReplicateCheckpoint byte = 0x14

	// MsgReplicateEntryBin is a WAL entry in binary framing (#698).
	// Same logical content as MsgReplicateEntry, but the payload rides
	// the wire as raw bytes instead of JSON + base64, so a WAL entry
	// near MaxWALPayloadSize still fits a frame (base64 inflates 4/3,
	// which made every JSON-framed entry above ~75MB unsendable while
	// the WAL happily stored it — a deterministic replication stall).
	// Sent only to readers that advertised support in their handshake
	// (protocol.ReplicateSync.SupportsBinaryEntries), so mixed-version
	// pairs keep speaking JSON.
	//
	// Frame layout after the [4-byte length][1-byte type] header:
	//   [8-byte sequence BE][8-byte timestampUS BE]
	//   [1-byte tag length][tag bytes (hex string, as in JSON framing)]
	//   [payload bytes to end of frame]
	MsgReplicateEntryBin byte = 0x15

	// MsgReplicateError indicates a replication error
	MsgReplicateError byte = 0x1F
)

// ReplicateEntry is a single WAL entry sent from writer to reader.
// This is the primary message type for streaming WAL data.
type ReplicateEntry struct {
	// Sequence is a monotonically increasing number for ordering and deduplication
	Sequence uint64 `json:"seq"`

	// TimestampUS is the original entry timestamp in microseconds since epoch
	TimestampUS uint64 `json:"ts"`

	// Payload is the raw msgpack payload (zero-copy from WAL)
	Payload []byte `json:"payload"`

	// Tag is the per-entry truncated MAC tag (8 bytes, hex-encoded) over
	// (label, sequence, sha256(payload)[:8]) using the per-connection
	// HKDF-derived session key. Required when shared_secret is
	// configured; receivers refuse entries with a missing or invalid
	// tag. The cost is ~1µs per entry on commodity hardware vs. ~5µs
	// for a full HMAC — chosen to keep per-record overhead negligible
	// at 19.9M records/sec ingest. The 8-byte truncation gives 2^-64
	// forgery probability per entry; the periodic checkpoint message
	// (MsgReplicateCheckpoint) is the full-strength backstop.
	// See GHSA-wfgr-8x84-22q7 / CVE-2026-48106.
	//
	// Thread safety: the sender stamps Tag on a shallow copy of the
	// shared *ReplicateEntry inside sendToReader (see sender.go),
	// never on the broadcast-shared pointer — so a future parallel
	// broadcastEntry can stamp distinct tags into distinct
	// ReplicateEntry values without a race.
	Tag string `json:"tag,omitempty"`
}

// ReplicateCheckpoint is a periodic full-HMAC checkpoint covering every
// entry payload observed since the connection's handshake. The sender
// keeps a running SHA-256 hash (cumulativePayloadHash) of every payload
// it has streamed and signs (cumulativePayloadHash, lastSeq) plus the
// usual (nonce, senderNodeID, clusterName, timestamp) tuple with the
// cluster shared secret. The receiver maintains its own running hash and
// verifies the checkpoint against it; on mismatch the connection is
// dropped. This is the full-HMAC backstop for the truncated per-entry
// tag. See GHSA-wfgr-8x84-22q7 / CVE-2026-48106.
type ReplicateCheckpoint struct {
	// CumulativePayloadHashHex is the running SHA-256 over every entry
	// payload observed on this connection since the handshake, hex-encoded.
	CumulativePayloadHashHex string `json:"cumulative_payload_hash"`

	// LastSequence is the last entry sequence covered by this checkpoint.
	LastSequence uint64 `json:"last_seq"`

	// Nonce, SenderNodeID, ClusterName, Timestamp, HMAC bind the
	// checkpoint to a single emission. ValidateReplicationCheckpointHMAC
	// at the receiver re-binds them against the cluster shared secret.
	Nonce        string `json:"nonce"`
	SenderNodeID string `json:"sender_node_id"`
	ClusterName  string `json:"cluster_name"`
	Timestamp    int64  `json:"timestamp"`
	HMAC         string `json:"hmac"`
}

// ReplicateAck acknowledges receipt of entries up to a sequence number.
// Sent periodically by readers to inform writers of progress.
type ReplicateAck struct {
	// LastSequence is the last successfully received and applied sequence
	LastSequence uint64 `json:"last_seq"`

	// ReaderID identifies which reader is sending the ack
	ReaderID string `json:"reader_id"`
}

// ReplicateSync requests the current replication position.
// Sent by readers when connecting or reconnecting to sync state.
//
// HandshakeNonce is the same nonce the reader sent in the protocol-
// level MsgReplicateSync (it is the input to HKDF on both ends).
// Carrying it through this internal struct lets the sender derive the
// per-connection session key without re-parsing the protocol message.
// Not serialized — this struct is the internal hand-off shape, not
// the wire format.
type ReplicateSync struct {
	// ReaderID identifies the reader requesting sync
	ReaderID string `json:"reader_id"`

	// LastKnownSequence is the last sequence the reader has (0 if new)
	LastKnownSequence uint64 `json:"last_known_seq"`

	// HandshakeNonce is the nonce from the protocol-level
	// MsgReplicateSync. Used to derive the HKDF session key. Not
	// serialized — internal hand-off only.
	HandshakeNonce string `json:"-"`

	// SupportsBinaryEntries carries the reader's MsgReplicateEntryBin
	// capability from the protocol-level handshake (#698). Internal
	// hand-off only, like HandshakeNonce.
	SupportsBinaryEntries bool `json:"-"`
}

// ReplicateSyncAck responds with the writer's current position.
// Allows readers to understand their lag and prepare for streaming.
type ReplicateSyncAck struct {
	// CurrentSequence is the writer's current sequence number
	CurrentSequence uint64 `json:"current_seq"`

	// CanResume indicates if the reader can resume from LastKnownSequence
	// If false, reader needs to bootstrap from scratch (WAL was rotated)
	CanResume bool `json:"can_resume"`

	// Error contains any error message (empty if success)
	Error string `json:"error,omitempty"`
}

// ReplicateError indicates a replication error.
type ReplicateError struct {
	// Code is a machine-readable error code
	Code string `json:"code"`

	// Message is a human-readable error description
	Message string `json:"message"`
}

// Error codes for replication failures
const (
	ErrCodeSequenceGap = "SEQUENCE_GAP" // Reader is too far behind to resume
	ErrCodeWALRotated  = "WAL_ROTATED"  // WAL was rotated, need full resync
	ErrCodeInvalidMsg  = "INVALID_MSG"  // Invalid message format
	ErrCodeNotWriter   = "NOT_WRITER"   // Connected to non-writer node
	ErrCodeBufferFull  = "BUFFER_FULL"  // Replication buffer is full
	ErrCodeWriteFailed = "WRITE_FAILED" // Failed to write entry
	ErrCodeApplyFailed = "APPLY_FAILED" // Failed to apply entry
)

// Wire format: [4-byte length (big-endian)][1-byte type][JSON payload]
// Maximum message size is 100MB to match WAL limits

const (
	// HeaderSize is the size of the message header (length + type)
	HeaderSize = 5

	// MaxMessageSize is the maximum allowed message size (100MB)
	MaxMessageSize = 100 * 1024 * 1024

	// binaryEntryHeaderSize is the fixed prefix of a MsgReplicateEntryBin
	// frame body: sequence (8) + timestamp (8) + tag length (1).
	binaryEntryHeaderSize = 8 + 8 + 1

	// MaxEntryFrameSize bounds a MsgReplicateEntryBin frame. A WAL payload
	// can legally reach MaxWALPayloadSize == MaxMessageSize exactly, and the
	// binary frame adds the type byte plus binaryEntryHeaderSize plus the
	// tag — so the frame cap gets 1KB of headroom over the message cap.
	// JSON-framed messages keep the plain MaxMessageSize bound at write
	// time; ReadMessage accepts up to this larger bound for every type,
	// which admits at most 1KB extra on the legacy frames.
	MaxEntryFrameSize = MaxMessageSize + 1024
)

// WriteMessage writes a typed message to the writer.
// Format: [4-byte length][1-byte type][payload]
func WriteMessage(w io.Writer, msgType byte, msg interface{}) error {
	// Marshal payload
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Check size
	totalSize := 1 + len(payload) // type + payload
	if totalSize > MaxMessageSize {
		return fmt.Errorf("message too large: %d > %d", totalSize, MaxMessageSize)
	}

	// Write length (big-endian)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(totalSize))
	if _, err := w.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}

	// Write type
	if _, err := w.Write([]byte{msgType}); err != nil {
		return fmt.Errorf("write type: %w", err)
	}

	// Write payload
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	return nil
}

// ReadMessage reads a typed message from the reader.
// Returns the message type and unmarshaled message.
func ReadMessage(r io.Reader) (byte, []byte, error) {
	// Read length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return 0, nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// Validate length. MaxEntryFrameSize (not MaxMessageSize) so a
	// binary entry frame carrying a payload at the WAL cap still fits;
	// see the constant's doc for why the two bounds differ.
	if length > MaxEntryFrameSize {
		return 0, nil, fmt.Errorf("message too large: %d > %d", length, MaxEntryFrameSize)
	}
	if length < 1 {
		return 0, nil, fmt.Errorf("message too small: %d", length)
	}

	// Read type
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return 0, nil, fmt.Errorf("read type: %w", err)
	}
	msgType := typeBuf[0]

	// Read payload
	payloadLen := length - 1
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("read payload: %w", err)
		}
	}

	return msgType, payload, nil
}

// WriteEntry writes a ReplicateEntry to the writer.
func WriteEntry(w io.Writer, entry *ReplicateEntry) error {
	return WriteMessage(w, MsgReplicateEntry, entry)
}

// WriteEntryBinary writes a ReplicateEntry as a MsgReplicateEntryBin
// frame: the payload goes on the wire as raw bytes instead of JSON +
// base64, so an entry near the WAL payload cap remains sendable (#698).
// Callers must only use this for readers that advertised
// SupportsBinaryEntries in their handshake.
func WriteEntryBinary(w io.Writer, entry *ReplicateEntry) error {
	if len(entry.Tag) > 255 {
		return fmt.Errorf("entry tag too long: %d > 255", len(entry.Tag))
	}
	totalSize := 1 + binaryEntryHeaderSize + len(entry.Tag) + len(entry.Payload)
	if totalSize > MaxEntryFrameSize {
		return fmt.Errorf("message too large: %d > %d", totalSize, MaxEntryFrameSize)
	}

	// Build the frame header (length + type + fixed fields + tag) in one
	// buffer, then write the payload from its own slice — no copy of the
	// payload bytes.
	header := make([]byte, 4+1+binaryEntryHeaderSize+len(entry.Tag))
	binary.BigEndian.PutUint32(header[0:4], uint32(totalSize))
	header[4] = MsgReplicateEntryBin
	binary.BigEndian.PutUint64(header[5:13], entry.Sequence)
	binary.BigEndian.PutUint64(header[13:21], entry.TimestampUS)
	header[21] = byte(len(entry.Tag))
	copy(header[22:], entry.Tag)
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write entry header: %w", err)
	}
	if len(entry.Payload) > 0 {
		if _, err := w.Write(entry.Payload); err != nil {
			return fmt.Errorf("write entry payload: %w", err)
		}
	}
	return nil
}

// ParseEntryBinary parses a MsgReplicateEntryBin frame body (the bytes
// after the length+type header) into a ReplicateEntry. Every length is
// bounds-checked against the frame; the payload slice aliases the frame
// buffer (ReadMessage allocates a fresh buffer per message, so the alias
// never outlives its message).
func ParseEntryBinary(body []byte) (*ReplicateEntry, error) {
	if len(body) < binaryEntryHeaderSize {
		return nil, fmt.Errorf("binary entry too short: %d < %d", len(body), binaryEntryHeaderSize)
	}
	tagLen := int(body[16])
	if len(body) < binaryEntryHeaderSize+tagLen {
		return nil, fmt.Errorf("binary entry tag truncated: need %d bytes, have %d", binaryEntryHeaderSize+tagLen, len(body))
	}
	return &ReplicateEntry{
		Sequence:    binary.BigEndian.Uint64(body[0:8]),
		TimestampUS: binary.BigEndian.Uint64(body[8:16]),
		Tag:         string(body[17 : 17+tagLen]),
		Payload:     body[binaryEntryHeaderSize+tagLen:],
	}, nil
}

// WriteAck writes a ReplicateAck to the writer.
func WriteAck(w io.Writer, ack *ReplicateAck) error {
	return WriteMessage(w, MsgReplicateAck, ack)
}

// WriteSync writes a ReplicateSync to the writer.
func WriteSync(w io.Writer, sync *ReplicateSync) error {
	return WriteMessage(w, MsgReplicateSync, sync)
}

// WriteSyncAck writes a ReplicateSyncAck to the writer.
func WriteSyncAck(w io.Writer, ack *ReplicateSyncAck) error {
	return WriteMessage(w, MsgReplicateSyncAck, ack)
}

// WriteError writes a ReplicateError to the writer.
func WriteError(w io.Writer, err *ReplicateError) error {
	return WriteMessage(w, MsgReplicateError, err)
}

// WriteCheckpoint writes a ReplicateCheckpoint to the writer.
func WriteCheckpoint(w io.Writer, cp *ReplicateCheckpoint) error {
	return WriteMessage(w, MsgReplicateCheckpoint, cp)
}

// ParseEntry parses a ReplicateEntry from JSON payload.
func ParseEntry(payload []byte) (*ReplicateEntry, error) {
	var entry ReplicateEntry
	if err := json.Unmarshal(payload, &entry); err != nil {
		return nil, fmt.Errorf("parse entry: %w", err)
	}
	return &entry, nil
}

// ParseAck parses a ReplicateAck from JSON payload.
func ParseAck(payload []byte) (*ReplicateAck, error) {
	var ack ReplicateAck
	if err := json.Unmarshal(payload, &ack); err != nil {
		return nil, fmt.Errorf("parse ack: %w", err)
	}
	return &ack, nil
}

// ParseSync parses a ReplicateSync from JSON payload.
func ParseSync(payload []byte) (*ReplicateSync, error) {
	var sync ReplicateSync
	if err := json.Unmarshal(payload, &sync); err != nil {
		return nil, fmt.Errorf("parse sync: %w", err)
	}
	return &sync, nil
}

// ParseSyncAck parses a ReplicateSyncAck from JSON payload.
func ParseSyncAck(payload []byte) (*ReplicateSyncAck, error) {
	var ack ReplicateSyncAck
	if err := json.Unmarshal(payload, &ack); err != nil {
		return nil, fmt.Errorf("parse sync ack: %w", err)
	}
	return &ack, nil
}

// ParseError parses a ReplicateError from JSON payload.
func ParseError(payload []byte) (*ReplicateError, error) {
	var errMsg ReplicateError
	if err := json.Unmarshal(payload, &errMsg); err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return &errMsg, nil
}

// ParseCheckpoint parses a ReplicateCheckpoint from JSON payload.
func ParseCheckpoint(payload []byte) (*ReplicateCheckpoint, error) {
	var cp ReplicateCheckpoint
	if err := json.Unmarshal(payload, &cp); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	return &cp, nil
}
