package ingest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplayRejectsWrongRowRangeLength(t *testing.T) {
	b, w := newCheckpointBuffer(10)
	defer b.Close()
	identity := "arc:rows:v1:0123456789abcdef0123456789abcdef:3:5"
	err := b.WriteColumnarDirectReplay(context.Background(), "test", "events",
		map[string][]interface{}{"time": {int64(1700000000000000)}, "value": {1.0}}, identity)
	require.ErrorContains(t, err, "range length")
	w.mu.Lock()
	defer w.mu.Unlock()
	require.Empty(t, w.handedOut)
	require.Empty(t, w.flushed)
}
