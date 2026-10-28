package ingest

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func sortKeysBuffer(logs *bytes.Buffer, perMeasurement map[string][]string, defaults []string) *ArrowBuffer {
	return &ArrowBuffer{
		sortKeysConfig:  perMeasurement,
		defaultSortKeys: defaults,
		logger:          zerolog.New(logs),
	}
}

func TestFlushSortKeys(t *testing.T) {
	data := map[string]interface{}{"time": []int64{1}, "host": []string{"a"}, "Region": []string{"x"}}

	t.Run("all present returns the configured keys", func(t *testing.T) {
		var logs bytes.Buffer
		b := sortKeysBuffer(&logs, map[string][]string{"m": {"host"}}, []string{"time"})
		require.Equal(t, []string{"host", "time"}, b.flushSortKeys("d", "m", data))
		require.Empty(t, logs.String())
	})

	t.Run("case mismatch resolves to the data's spelling", func(t *testing.T) {
		var logs bytes.Buffer
		b := sortKeysBuffer(&logs, map[string][]string{"m": {"HOST", "region"}}, []string{"time"})
		require.Equal(t, []string{"host", "Region", "time"}, b.flushSortKeys("d", "m", data))
		require.Empty(t, logs.String(), "a key that resolves is not missing")
	})

	t.Run("explicit missing key: dropped, Warn once a minute", func(t *testing.T) {
		var logs bytes.Buffer
		b := sortKeysBuffer(&logs, map[string][]string{"m": {"site", "host"}}, []string{"time"})
		require.Equal(t, []string{"host", "time"}, b.flushSortKeys("d", "m", data))
		require.Equal(t, []string{"host", "time"}, b.flushSortKeys("d", "m", data))
		lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
		require.Len(t, lines, 1, "rate-limited to one line a minute")
		require.Contains(t, lines[0], `"level":"warn"`)
		require.Contains(t, lines[0], `"missing_sort_keys":["site"]`)
		require.Contains(t, lines[0], `"measurement":"m"`)
	})

	t.Run("default_sort_keys missing key: dropped, Debug", func(t *testing.T) {
		var logs bytes.Buffer
		b := sortKeysBuffer(&logs, map[string][]string{}, []string{"site"})
		require.Equal(t, []string{"time"}, b.flushSortKeys("d", "m", data))
		require.Contains(t, logs.String(), `"level":"debug"`)
	})

	t.Run("never writes into the config slice", func(t *testing.T) {
		var logs bytes.Buffer
		shared := make([]string, 2, 8)
		shared[0], shared[1] = "site", "host"
		b := sortKeysBuffer(&logs, map[string][]string{"m": shared}, []string{"time"})
		got := b.flushSortKeys("d", "m", data)
		got[0] = "changed"
		require.Equal(t, []string{"site", "host"}, shared)
		require.Equal(t, "", shared[:3][2], "nothing appended into the config slice's spare capacity")
	})
}

// sortFlushBatch never writes a flush unsorted when its keys fail: it falls
// back to time order and says so.
func TestSortFlushBatchFallsBackToTime(t *testing.T) {
	var logs bytes.Buffer
	b := sortKeysBuffer(&logs, map[string][]string{}, []string{"time"})
	batch := &TypedColumnBatch{Data: map[string]interface{}{
		"time": []int64{3, 1, 2},
		"v":    []float64{30, 10, 20},
	}}
	got := b.sortFlushBatch("d", "m", batch, []string{"nope", "time"})
	require.Equal(t, []int64{1, 2, 3}, got.Data["time"])
	require.Equal(t, []float64{10, 20, 30}, got.Data["v"])
	require.Contains(t, logs.String(), `"level":"error"`)
	require.Contains(t, logs.String(), "sorted by time only")
}
