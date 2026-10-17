package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const hourUS = int64(3600_000_000)

type groupedWrite struct {
	m    string
	b    *TypedColumnBatch
	rows int
}

func drainAll(t *testing.T, g *HourGrouper, cells int) []groupedWrite {
	t.Helper()
	var out []groupedWrite
	require.NoError(t, g.Drain(cells, func(m string, b *TypedColumnBatch, n int) error {
		out = append(out, groupedWrite{m, b, n})
		return nil
	}))
	return out
}

// Rows of different chunks are regrouped by hour; a column one chunk lacks,
// or holds NULL, is NULL in the group; every group of a measurement carries
// the window's full column set.
func TestHourGrouperRegroupsByHour(t *testing.T) {
	g := NewHourGrouper(1 << 30)
	require.False(t, g.Add("m", &TypedColumnBatch{
		Data:       map[string]interface{}{"time": []int64{2 * hourUS, 0, 2*hourUS + 1}, "a": []int64{1, 2, 3}, "host": []string{"x", "y", ""}},
		Validity:   map[string][]bool{"host": {true, true, false}},
		TagColumns: []string{"host"},
	}, 3))
	require.False(t, g.Add("m", &TypedColumnBatch{
		Data: map[string]interface{}{"time": []int64{1, 2*hourUS + 2}, "b": []float64{1.5, 2.5}},
	}, 2))
	ws := drainAll(t, g, 1<<20)
	require.Len(t, ws, 2) // hour 0 and hour 2
	require.True(t, g.Empty())

	h0, h2 := ws[0].b, ws[1].b
	require.Equal(t, []int64{0, 1}, h0.Data["time"])
	require.Equal(t, []int64{2 * hourUS, 2*hourUS + 1, 2*hourUS + 2}, h2.Data["time"])
	for _, b := range []*TypedColumnBatch{h0, h2} {
		require.Len(t, b.Data, 4, "every group carries the window's columns")
		require.Equal(t, []string{"host"}, b.TagColumns)
		require.Empty(t, b.Signature)
	}
	// hour 0: rows (chunk0 row1: a=2 host=y), (chunk1 row0: b=1.5)
	require.Equal(t, []int64{2, 0}, h0.Data["a"])
	require.Equal(t, []bool{true, false}, h0.Validity["a"])
	require.Equal(t, []float64{0, 1.5}, h0.Data["b"])
	require.Equal(t, []bool{false, true}, h0.Validity["b"])
	// hour 2: host NULL in chunk0 row2 and absent in chunk1
	require.Equal(t, []string{"x", "", ""}, h2.Data["host"])
	require.Equal(t, []bool{true, false, false}, h2.Validity["host"])
	_, hasTimeValidity := h2.Validity["time"]
	require.False(t, hasTimeValidity)
}

// A group is split at the cell cap (rows x columns).
func TestHourGrouperCellCap(t *testing.T) {
	g := NewHourGrouper(1 << 30)
	tm := make([]int64, 10)
	v := make([]int64, 10)
	g.Add("m", &TypedColumnBatch{Data: map[string]interface{}{"time": tm, "v": v}}, 10)
	ws := drainAll(t, g, 8) // 2 columns -> 4 rows per write
	require.Len(t, ws, 3)
	require.Equal(t, []int{4, 4, 2}, []int{ws[0].rows, ws[1].rows, ws[2].rows})
}

// Add reports the budget; a batch alone over it drains at once.
func TestHourGrouperBudget(t *testing.T) {
	g := NewHourGrouper(100)
	require.True(t, g.Add("m", &TypedColumnBatch{Data: map[string]interface{}{"time": make([]int64, 20)}}, 20), "160 bytes over a 100-byte budget")
	require.True(t, NewHourGrouper(0).Add("m", &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}}}, 1))
}

// A column with two Go types across chunks is refused, not merged.
func TestHourGrouperTypeMismatch(t *testing.T) {
	g := NewHourGrouper(1 << 30)
	g.Add("m", &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}, "v": []int64{1}}}, 1)
	g.Add("m", &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{2}, "v": []float64{1}}}, 1)
	err := g.Drain(1<<20, func(string, *TypedColumnBatch, int) error { return nil })
	require.ErrorContains(t, err, "column \"v\"")
	require.True(t, g.Empty())
}

// Measurements drain in the order they came.
func TestHourGrouperMeasurementOrder(t *testing.T) {
	g := NewHourGrouper(1 << 30)
	g.Add("z", &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}}}, 1)
	g.Add("a", &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1}}}, 1)
	ws := drainAll(t, g, 1<<20)
	require.Equal(t, "z", ws[0].m)
	require.Equal(t, "a", ws[1].m)
}
