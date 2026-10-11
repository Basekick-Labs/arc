package ingest

// #1207 S2: a fenced node refuses live writes before anything is written, the
// WAL included, so a rejected write never resurfaces on recovery. Replay paths
// are never gated.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/pkg/models"
)

func fencedGate() error { return fmt.Errorf("%w: test", ErrIngestFenced) }

func TestFencedWritesReachNeitherTheWALNorTheBuffer(t *testing.T) {
	ctx := context.Background()
	cols := func() map[string][]interface{} {
		return map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	}
	entries := map[string]func(*ArrowBuffer) error{
		"WriteColumnarRecord": func(b *ArrowBuffer) error { return b.WriteColumnarRecord(ctx, "db", checkpointTestRecord()) },
		"WriteColumnarDirect": func(b *ArrowBuffer) error { return b.WriteColumnarDirect(ctx, "db", "m", cols()) },
		"Write (rows)": func(b *ArrowBuffer) error {
			return b.Write(ctx, "db", []interface{}{&models.Record{Measurement: "m", Time: time.Now(), Fields: map[string]interface{}{"v": 1.0}}})
		},
		"WriteTypedColumnarDirect": func(b *ArrowBuffer) error {
			batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "value": []float64{1}}}
			return b.WriteTypedColumnarDirect(ctx, "db", "m", batch, 1)
		},
	}
	for name, write := range entries {
		t.Run(name, func(t *testing.T) {
			buffer, wal := newCheckpointBuffer(1000)
			defer buffer.Close()
			buffer.SetIngestGate(fencedGate)
			if err := write(buffer); !errors.Is(err, ErrIngestFenced) {
				t.Fatalf("err = %v, want ErrIngestFenced", err)
			}
			if handedOut, _ := wal.counts(); handedOut != 0 {
				t.Fatalf("a fenced write reached the WAL (%d entries): it would resurface on recovery", handedOut)
			}
		})
	}
}

// Replay carries rows the cluster already accepted; fencing them would strand
// recovered data.
func TestReplayIsNotFenced(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetIngestGate(fencedGate)
	cols := map[string][]interface{}{"time": {time.Now().UnixMicro()}, "value": {1.0}}
	if err := buffer.WriteColumnarDirectReplay(context.Background(), "db", "m", cols, ""); err != nil {
		t.Fatalf("replay refused by the ingest fence: %v", err)
	}
}

func TestOpenGateAdmits(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000)
	defer buffer.Close()
	buffer.SetIngestGate(func() error { return nil })
	if err := buffer.WriteColumnarRecord(context.Background(), "db", checkpointTestRecord()); err != nil {
		t.Fatal(err)
	}
}
