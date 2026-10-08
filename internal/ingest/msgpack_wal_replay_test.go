package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

func TestMessagePackTagColumnsSurviveWALRecovery(t *testing.T) {
	tmp := t.TempDir()
	writer, err := wal.NewWriter(&wal.WriterConfig{
		WALDir:       filepath.Join(tmp, "wal"),
		SyncMode:     wal.SyncModeFsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("create WAL writer: %v", err)
	}

	liveBuffer := newReplayTestBuffer(t, filepath.Join(tmp, "live"))
	liveBuffer.SetWAL(writer)
	t.Cleanup(func() { _ = liveBuffer.Close() })

	payload, err := msgpack.Marshal(map[string]interface{}{
		"m":        "cpu",
		"tag_keys": []string{"host", "region"},
		"columns": map[string]interface{}{
			"time":   []interface{}{int64(1_700_000_000_000_000)},
			"host":   []interface{}{"node-1"},
			"region": []interface{}{"west"},
			"value":  []interface{}{float64(1)},
		},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if err := liveBuffer.Write(context.Background(), "default", decoded); err != nil {
		t.Fatalf("write live payload: %v", err)
	}
	walPath := writer.CurrentFile()
	if err := writer.Close(); err != nil {
		t.Fatalf("close WAL writer: %v", err)
	}

	replayBuffer := newReplayTestBuffer(t, filepath.Join(tmp, "replayed"))
	t.Cleanup(func() { _ = replayBuffer.Close() })
	recovery := wal.NewRecovery(filepath.Join(tmp, "wal"), zerolog.Nop())
	rowCallback := func(context.Context, []map[string]interface{}) error { return nil }
	_, err = recovery.RecoverWithOptions(context.Background(), rowCallback, &wal.RecoveryOptions{
		ColumnarCallback: func(ctx context.Context, database, measurement string, columns map[string][]interface{}, walIdentity string, tagColumns []string) error {
			if !reflect.DeepEqual(tagColumns, []string{"host", "region"}) {
				return fmt.Errorf("recovered tag columns = %v", tagColumns)
			}
			return replayBuffer.WriteColumnarDirectReplayWithTagColumns(ctx, database, measurement, columns, walIdentity, tagColumns)
		},
	})
	if err != nil {
		t.Fatalf("recover WAL %s: %v", walPath, err)
	}

	key := "default/cpu"
	shard := replayBuffer.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	items := shard.buffers[key]
	if len(items) != 1 {
		t.Fatalf("replayed buffer entries = %d, want 1", len(items))
	}
	batch, ok := items[0].(*TypedColumnBatch)
	if !ok {
		t.Fatalf("replayed entry type = %T, want *TypedColumnBatch", items[0])
	}
	if !reflect.DeepEqual(batch.TagColumns, []string{"host", "region"}) {
		t.Fatalf("replayed Parquet tag metadata = %v, want [host region]", batch.TagColumns)
	}
}
