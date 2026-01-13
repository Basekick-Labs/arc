package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	// MaxMessageSize is the maximum size of a protocol message (1MB).
	MaxMessageSize = 1 << 20
	// HeaderSize is the size of the message header (4 bytes length + 1 byte type).
	HeaderSize = 5
)

// Message represents a protocol message with its type and payload.
type Message struct {
	Type    MessageType
	Payload interface{}
}

// Encoder writes protocol messages to a connection.
type Encoder struct {
	w io.Writer
}

// NewEncoder creates a new Encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode writes a message to the underlying writer.
// Wire format: [4-byte length (big-endian)][1-byte type][JSON payload]
func (e *Encoder) Encode(msg *Message) error {
	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Check message size
	totalLen := 1 + len(payload) // type byte + payload
	if totalLen > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes (max %d)", totalLen, MaxMessageSize)
	}

	// Write length (4 bytes, big-endian)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(totalLen))
	if _, err := e.w.Write(lenBuf); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write message type (1 byte)
	if _, err := e.w.Write([]byte{byte(msg.Type)}); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}

	// Write payload
	if _, err := e.w.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	return nil
}

// Decoder reads protocol messages from a connection.
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a new Decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode reads a message from the underlying reader.
func (d *Decoder) Decode() (*Message, error) {
	// Read length (4 bytes)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(d.r, lenBuf); err != nil {
		return nil, fmt.Errorf("failed to read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// Validate length
	if length == 0 {
		return nil, fmt.Errorf("invalid message length: 0")
	}
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", length, MaxMessageSize)
	}

	// Read message type (1 byte)
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(d.r, typeBuf); err != nil {
		return nil, fmt.Errorf("failed to read message type: %w", err)
	}
	msgType := MessageType(typeBuf[0])

	// Read payload
	payloadLen := length - 1
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(d.r, payload); err != nil {
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	// Unmarshal based on message type
	msg := &Message{Type: msgType}
	var err error
	msg.Payload, err = unmarshalPayload(msgType, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return msg, nil
}

// unmarshalPayload unmarshals the payload based on message type.
func unmarshalPayload(msgType MessageType, payload []byte) (interface{}, error) {
	switch msgType {
	case MsgJoinRequest:
		var req JoinRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, err
		}
		return &req, nil

	case MsgJoinResponse:
		var resp JoinResponse
		if err := json.Unmarshal(payload, &resp); err != nil {
			return nil, err
		}
		return &resp, nil

	case MsgLeaderInfo:
		var info LeaderInfo
		if err := json.Unmarshal(payload, &info); err != nil {
			return nil, err
		}
		return &info, nil

	case MsgHeartbeat:
		var hb Heartbeat
		if err := json.Unmarshal(payload, &hb); err != nil {
			return nil, err
		}
		return &hb, nil

	case MsgHeartbeatAck:
		var ack HeartbeatAck
		if err := json.Unmarshal(payload, &ack); err != nil {
			return nil, err
		}
		return &ack, nil

	case MsgLeaveNotify:
		var leave LeaveNotify
		if err := json.Unmarshal(payload, &leave); err != nil {
			return nil, err
		}
		return &leave, nil

	case MsgReplicateSync:
		var sync ReplicateSync
		if err := json.Unmarshal(payload, &sync); err != nil {
			return nil, err
		}
		return &sync, nil

	case MsgReplicateSyncAck:
		var ack ReplicateSyncAck
		if err := json.Unmarshal(payload, &ack); err != nil {
			return nil, err
		}
		return &ack, nil

	default:
		return nil, fmt.Errorf("unknown message type: %d", msgType)
	}
}

// SendMessage sends a message over a connection with timeout.
func SendMessage(conn net.Conn, msg *Message, timeout time.Duration) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer conn.SetWriteDeadline(time.Time{})
	}

	encoder := NewEncoder(conn)
	return encoder.Encode(msg)
}

// ReceiveMessage reads a message from a connection with timeout.
func ReceiveMessage(conn net.Conn, timeout time.Duration) (*Message, error) {
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, fmt.Errorf("failed to set read deadline: %w", err)
		}
		defer conn.SetReadDeadline(time.Time{})
	}

	decoder := NewDecoder(conn)
	return decoder.Decode()
}

// Helper functions to create messages

// NewJoinRequest creates a JoinRequest message.
func NewJoinRequest(req *JoinRequest) *Message {
	return &Message{Type: MsgJoinRequest, Payload: req}
}

// NewJoinResponse creates a JoinResponse message.
func NewJoinResponse(resp *JoinResponse) *Message {
	return &Message{Type: MsgJoinResponse, Payload: resp}
}

// NewLeaderInfo creates a LeaderInfo message.
func NewLeaderInfo(info *LeaderInfo) *Message {
	return &Message{Type: MsgLeaderInfo, Payload: info}
}

// NewHeartbeat creates a Heartbeat message.
func NewHeartbeat(hb *Heartbeat) *Message {
	return &Message{Type: MsgHeartbeat, Payload: hb}
}

// NewHeartbeatAck creates a HeartbeatAck message.
func NewHeartbeatAck(ack *HeartbeatAck) *Message {
	return &Message{Type: MsgHeartbeatAck, Payload: ack}
}

// NewLeaveNotify creates a LeaveNotify message.
func NewLeaveNotify(leave *LeaveNotify) *Message {
	return &Message{Type: MsgLeaveNotify, Payload: leave}
}
