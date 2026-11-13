package ingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

// hexOrderedWAL is orderedWAL with tracked (32-hex) identities, so its batches
// get row spans, and with row checkpoints logged apart from parent ones.
type hexOrderedWAL struct {
	orderedWAL
	issued int
}

func (w *hexOrderedWAL) AppendRawWithMetaTracked(string, []byte) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.issued++
	tok := fmt.Sprintf("%016x%016x", 0xabc, w.issued)
	w.handedOut = append(w.handedOut, tok)
	return []string{tok}, nil
}

func (w *hexOrderedWAL) MarkFlushed(ids []string) error {
	kind := "checkpoint"
	if _, _, _, ok := wal.ParseRecoveryRowIdentity(ids[0]); ok {
		kind = "rowcheckpoint"
	}
	w.log.add(kind)
	return w.checkpointRecordingWAL.MarkFlushed(ids)
}

// #1492 keeps #1209 per hour file: each file is registered, the registration
// made durable, and only then its rows checkpointed; the flush's own
// checkpoint follows the last file, after a sync of its own.
func TestMultiHourFlushRegistersAndSyncsEachHourBeforeItsRowCheckpoint(t *testing.T) {
	buffer, _ := newCheckpointBuffer(1000) // nothing flushes until FlushAll
	log := &orderLog{}
	buffer.SetWAL(&hexOrderedWAL{orderedWAL: orderedWAL{log: log}})
	buffer.SetFileRegistrar(&syncingRegistrar{log: log})
	defer buffer.Close()

	hour := int64(3_600_000_000)
	base := int64(1_790_856_000_000_000) // 2026-10-01 12:00 UTC
	for i := 0; i < 2; i++ {
		rec := &models.ColumnarRecord{
			Measurement: "ticks",
			Columnar:    true,
			Columns: map[string][]interface{}{
				"time":  {base + int64(i), base + hour + int64(i), base + 2*hour + int64(i)},
				"value": {1.0, 2.0, 3.0},
			},
			TimeUnit:   "us",
			RawPayload: []byte{0x01, byte(i)},
		}
		require.NoError(t, buffer.writeColumnarInternal(context.Background(), "default", rec, false, ""))
	}
	require.NoError(t, buffer.FlushAll(context.Background()))

	var want []string
	for i := 0; i < 3; i++ {
		want = append(want, "register", "sync", "rowcheckpoint")
	}
	want = append(want, "sync", "checkpoint")
	require.Equal(t, want, log.snapshot())
}
