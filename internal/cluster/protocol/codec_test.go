package protocol

import (
	"bytes"
	"testing"
	"time"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
	}{
		{
			name: "JoinRequest",
			msg: NewJoinRequest(&JoinRequest{
				NodeID:      "node-1",
				NodeName:    "Node 1",
				Role:        "writer",
				ClusterName: "test-cluster",
				RaftAddr:    "10.0.0.1:9200",
				APIAddr:     "10.0.0.1:8000",
				CoordAddr:   "10.0.0.1:9100",
				Version:     "1.0.0",
			}),
		},
		{
			name: "JoinResponse success",
			msg: NewJoinResponse(&JoinResponse{
				Success:    true,
				LeaderID:   "leader-1",
				LeaderAddr: "10.0.0.1:9100",
				RaftLeader: "10.0.0.1:9200",
				Nodes: []NodeInfo{
					{ID: "node-1", Name: "Node 1", Role: "writer", State: "healthy"},
					{ID: "node-2", Name: "Node 2", Role: "reader", State: "healthy"},
				},
			}),
		},
		{
			name: "JoinResponse failure",
			msg: NewJoinResponse(&JoinResponse{
				Success: false,
				Error:   "cluster name mismatch",
			}),
		},
		{
			name: "LeaderInfo",
			msg: NewLeaderInfo(&LeaderInfo{
				LeaderID:        "leader-1",
				LeaderCoordAddr: "10.0.0.1:9100",
				LeaderRaftAddr:  "10.0.0.1:9200",
			}),
		},
		{
			name: "Heartbeat",
			msg: NewHeartbeat(&Heartbeat{
				NodeID:    "node-1",
				State:     "healthy",
				IsLeader:  true,
				Timestamp: time.Now().UTC().Truncate(time.Millisecond),
			}),
		},
		{
			name: "HeartbeatAck",
			msg: NewHeartbeatAck(&HeartbeatAck{
				NodeID:    "node-1",
				Timestamp: time.Now().UTC().Truncate(time.Millisecond),
			}),
		},
		{
			name: "LeaveNotify",
			msg: NewLeaveNotify(&LeaveNotify{
				NodeID: "node-1",
				Reason: "graceful shutdown",
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			var buf bytes.Buffer
			encoder := NewEncoder(&buf)
			if err := encoder.Encode(tt.msg); err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Decode
			decoder := NewDecoder(&buf)
			decoded, err := decoder.Decode()
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Verify type
			if decoded.Type != tt.msg.Type {
				t.Errorf("Type mismatch: got %v, want %v", decoded.Type, tt.msg.Type)
			}

			// Type-specific verification
			switch tt.msg.Type {
			case MsgJoinRequest:
				orig := tt.msg.Payload.(*JoinRequest)
				got := decoded.Payload.(*JoinRequest)
				if got.NodeID != orig.NodeID || got.Role != orig.Role {
					t.Errorf("JoinRequest mismatch")
				}

			case MsgJoinResponse:
				orig := tt.msg.Payload.(*JoinResponse)
				got := decoded.Payload.(*JoinResponse)
				if got.Success != orig.Success || got.Error != orig.Error {
					t.Errorf("JoinResponse mismatch")
				}
				if len(got.Nodes) != len(orig.Nodes) {
					t.Errorf("Nodes count mismatch: got %d, want %d", len(got.Nodes), len(orig.Nodes))
				}

			case MsgLeaderInfo:
				orig := tt.msg.Payload.(*LeaderInfo)
				got := decoded.Payload.(*LeaderInfo)
				if got.LeaderID != orig.LeaderID {
					t.Errorf("LeaderInfo mismatch")
				}

			case MsgHeartbeat:
				orig := tt.msg.Payload.(*Heartbeat)
				got := decoded.Payload.(*Heartbeat)
				if got.NodeID != orig.NodeID || got.IsLeader != orig.IsLeader {
					t.Errorf("Heartbeat mismatch")
				}

			case MsgHeartbeatAck:
				orig := tt.msg.Payload.(*HeartbeatAck)
				got := decoded.Payload.(*HeartbeatAck)
				if got.NodeID != orig.NodeID {
					t.Errorf("HeartbeatAck mismatch")
				}

			case MsgLeaveNotify:
				orig := tt.msg.Payload.(*LeaveNotify)
				got := decoded.Payload.(*LeaveNotify)
				if got.NodeID != orig.NodeID || got.Reason != orig.Reason {
					t.Errorf("LeaveNotify mismatch")
				}
			}
		})
	}
}

func TestDecodeInvalidMessage(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "empty",
			data:    []byte{},
			wantErr: "failed to read length",
		},
		{
			name:    "zero length",
			data:    []byte{0, 0, 0, 0},
			wantErr: "invalid message length: 0",
		},
		{
			name:    "length too large",
			data:    []byte{0x10, 0x00, 0x00, 0x00}, // 256MB
			wantErr: "message too large",
		},
		{
			name:    "truncated payload",
			data:    []byte{0, 0, 0, 10, 1}, // claims 10 bytes but only has 1
			wantErr: "failed to read payload",
		},
		{
			name:    "unknown message type",
			data:    []byte{0, 0, 0, 3, 255, '{', '}'}, // type 255
			wantErr: "unknown message type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder := NewDecoder(bytes.NewReader(tt.data))
			_, err := decoder.Decode()
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if tt.wantErr != "" && !bytes.Contains([]byte(err.Error()), []byte(tt.wantErr)) {
				t.Errorf("Error mismatch: got %q, want to contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestMessageTypeString(t *testing.T) {
	tests := []struct {
		msgType MessageType
		want    string
	}{
		{MsgJoinRequest, "JoinRequest"},
		{MsgJoinResponse, "JoinResponse"},
		{MsgLeaderInfo, "LeaderInfo"},
		{MsgHeartbeat, "Heartbeat"},
		{MsgHeartbeatAck, "HeartbeatAck"},
		{MsgLeaveNotify, "LeaveNotify"},
		{MessageType(255), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.msgType.String(); got != tt.want {
			t.Errorf("MessageType(%d).String() = %q, want %q", tt.msgType, got, tt.want)
		}
	}
}

func TestMultipleMessages(t *testing.T) {
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	// Encode multiple messages
	messages := []*Message{
		NewJoinRequest(&JoinRequest{NodeID: "node-1", Role: "writer"}),
		NewHeartbeat(&Heartbeat{NodeID: "node-1", State: "healthy"}),
		NewLeaveNotify(&LeaveNotify{NodeID: "node-1"}),
	}

	for _, msg := range messages {
		if err := encoder.Encode(msg); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	// Decode all messages
	decoder := NewDecoder(&buf)
	for i, expected := range messages {
		decoded, err := decoder.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded.Type != expected.Type {
			t.Errorf("Message %d type mismatch: got %v, want %v", i, decoded.Type, expected.Type)
		}
	}
}
