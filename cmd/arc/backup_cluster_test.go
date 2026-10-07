package main

// Tests for the backup manager's cluster adapter (#1083).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
)

// TestBackupClusterManifest_ChunksFitTheForwardFrame pins that a register
// batch the backup manager fills to backup.ManifestBatchBytes by its own
// estimate (backup.RegisterOpBytes) still fits protocol.MaxMessageSize once
// this adapter has built the real payloads and they travel to the leader as a
// ForwardApplyRequest (three base64 encodings, see
// TestCoordinatorFileRegistrar_DrainChunksFitTheForwardFrame). Worst case on
// both axes: a 253-byte node ID and maximum-length storage keys.
func TestBackupClusterManifest_ChunksFitTheForwardFrame(t *testing.T) {
	nodeID := strings.Repeat("n", 253)
	measurement := strings.Repeat("m", 400)
	mk := func(i int) backup.ManifestFile {
		head := fmt.Sprintf("product_analytics/%s/2026/10/04/%02d/", measurement, i%24)
		tail := fmt.Sprintf("_%09d.parquet", i)
		path := head + strings.Repeat("f", storage.MaxUsableKeyLen-len(head)-len(tail)) + tail
		if len(path) != storage.MaxUsableKeyLen {
			t.Fatalf("fixture path is %d bytes, want %d", len(path), storage.MaxUsableKeyLen)
		}
		return backup.ManifestFile{
			Path: path, SHA256: strings.Repeat("ab", 32), SizeBytes: 1 << 20,
			Database: "product_analytics", Measurement: measurement,
			PartitionTime: time.Now().UTC(), CreatedAt: time.Now().UTC(),
		}
	}
	frameSize := func(ops []clusterraft.BatchFileOp) int {
		payload, err := json.Marshal(clusterraft.BatchFileOpsPayload{Ops: ops})
		if err != nil {
			t.Fatal(err)
		}
		cmdJSON, err := json.Marshal(&clusterraft.Command{Type: clusterraft.CommandBatchFileOps, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		err = protocol.NewEncoder(&buf).Encode(&protocol.Message{Type: protocol.MsgForwardApply, Payload: &protocol.ForwardApplyRequest{
			CommandJSON: cmdJSON,
			NodeID:      nodeID,
			Nonce:       strings.Repeat("0123456789abcdef", 4),
			Timestamp:   time.Now().Unix(),
			HMAC:        strings.Repeat("0123456789abcdef", 4),
		}})
		if err != nil {
			// Encode refuses an oversized frame before writing a byte.
			return protocol.MaxMessageSize + 1
		}
		return buf.Len()
	}

	// Precondition: a count-capped batch of such entries does not fit, or the
	// test proves nothing about the byte cap.
	whole := make([]backup.ManifestFile, backup.ManifestBatchOps)
	for i := range whole {
		whole[i] = mk(i)
	}
	ops, err := buildRegisterOps(whole, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if size := frameSize(ops); size <= protocol.MaxMessageSize {
		t.Fatalf("a %d-op batch encodes to %d bytes, under the %d cap; the fixture is too small", len(ops), size, protocol.MaxMessageSize)
	}

	// Fill one batch exactly as the manager does.
	var chunk []backup.ManifestFile
	total := 0
	for i := 0; len(chunk) < backup.ManifestBatchOps; i++ {
		f := mk(i)
		sz := backup.RegisterOpBytes(f, nodeID)
		if len(chunk) > 0 && total+sz > backup.ManifestBatchBytes {
			break
		}
		chunk = append(chunk, f)
		total += sz
	}
	if len(chunk) < 2 || len(chunk) >= backup.ManifestBatchOps {
		t.Fatalf("byte cap filled a batch of %d entries; want the byte cap to bind", len(chunk))
	}
	ops, err = buildRegisterOps(chunk, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if size := frameSize(ops); size > protocol.MaxMessageSize {
		t.Fatalf("a batch of %d entries estimated at %d bytes encodes to %d bytes, over the %d frame", len(chunk), total, size, protocol.MaxMessageSize)
	}
}

// Without a Raft manifest every write refuses instead of inheriting the
// coordinator's nil-success, and Sync refuses too; the read side is empty.
func TestBackupClusterManifest_RefusesWithoutRaft(t *testing.T) {
	b := &backupClusterManifest{coordinator: &cluster.Coordinator{}}
	ctx := context.Background()
	row := backup.ManifestFile{Path: "db/cpu/2026/01/01/00/f.parquet", SHA256: strings.Repeat("ab", 32), SizeBytes: 1, CreatedAt: time.Now()}
	if err := b.BatchRegister(ctx, []backup.ManifestFile{row}); !errors.Is(err, errNoRaftManifest) {
		t.Errorf("BatchRegister without Raft: err = %v, want errNoRaftManifest", err)
	}
	if err := b.BatchDelete(ctx, []string{row.Path}, "restore:replace"); !errors.Is(err, errNoRaftManifest) {
		t.Errorf("BatchDelete without Raft: err = %v, want errNoRaftManifest", err)
	}
	if err := b.Sync(ctx); !errors.Is(err, errNoRaftManifest) {
		t.Errorf("Sync without Raft: err = %v, want errNoRaftManifest", err)
	}
	if got := b.ManifestFiles(); len(got) != 0 {
		t.Errorf("ManifestFiles without Raft = %d entries, want 0", len(got))
	}
}

// buildRegisterOps refuses a path the FSM would refuse, naming it, so a
// refused batch is actionable rather than refused wholesale by the FSM.
func TestBuildRegisterOps_NamesARefusedPath(t *testing.T) {
	_, err := buildRegisterOps([]backup.ManifestFile{{Path: "s3://bucket/x.parquet", CreatedAt: time.Now()}}, "node")
	if err == nil || !strings.Contains(err.Error(), "s3://bucket/x.parquet") {
		t.Fatalf("err = %v, want the refused path named", err)
	}
}

// applyManifestBatchWithRetry retries only the transient leader errors, for
// the budget, and returns anything else at once.
func TestApplyManifestBatchWithRetry(t *testing.T) {
	ctx := context.Background()
	t.Run("transient then ok", func(t *testing.T) {
		calls := 0
		err := applyManifestBatchWithRetry(ctx, func(context.Context) error {
			calls++
			if calls < 3 {
				return cluster.ErrNoLeaderKnown
			}
			return nil
		}, time.Second, time.Millisecond)
		if err != nil || calls != 3 {
			t.Fatalf("err = %v calls = %d, want nil after 3", err, calls)
		}
	})
	t.Run("permanent returns at once", func(t *testing.T) {
		calls := 0
		permanent := errors.New("raft: manifest apply failed: no quorum")
		err := applyManifestBatchWithRetry(ctx, func(context.Context) error {
			calls++
			return permanent
		}, time.Second, time.Millisecond)
		if !errors.Is(err, permanent) || calls != 1 {
			t.Fatalf("err = %v calls = %d, want the permanent error after 1 call", err, calls)
		}
	})
	t.Run("budget exhausted returns the transient error", func(t *testing.T) {
		calls := 0
		err := applyManifestBatchWithRetry(ctx, func(context.Context) error {
			calls++
			return cluster.ErrLeaderUnreachable
		}, 30*time.Millisecond, 5*time.Millisecond)
		if !errors.Is(err, cluster.ErrLeaderUnreachable) || calls < 2 {
			t.Fatalf("err = %v calls = %d, want the transient error after several calls", err, calls)
		}
	})
	t.Run("cancelled context stops retrying", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		calls := 0
		err := applyManifestBatchWithRetry(cctx, func(context.Context) error {
			calls++
			return cluster.ErrNoLeaderKnown
		}, time.Minute, time.Minute)
		if !errors.Is(err, cluster.ErrNoLeaderKnown) || calls != 1 {
			t.Fatalf("err = %v calls = %d, want one call then the transient error", err, calls)
		}
	})
}
