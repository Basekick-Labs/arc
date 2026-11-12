package ingest

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// uniqueKeyLP is a body in which every point brings a tag key no other point
// has (GHSA-gc2v-hfj6-mfpx): dense, it is rows x rows cells.
func uniqueKeyLP(rows int) []byte {
	var b strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&b, "m,k%d=x value=1 %d\n", i, 1700000000000000000+int64(i))
	}
	return []byte(b.String())
}

func allocated(t *testing.T, f func()) uint64 {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// Refused once the cells pass the body's budget, having allocated about the
// budget and no more: the body that held 1.16 GB is refused within ~100 MB.
func TestLPUniqueKeysRefusedWithinBudget(t *testing.T) {
	p := NewLineProtocolParser()
	body := uniqueKeyLP(8000)
	var err error
	got := allocated(t, func() { _, _, _, err = p.ParseLPWrite(body, "ns") })
	if !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("err = %v, want ErrLPCellBudget", err)
	}
	if got > 200<<20 {
		t.Fatalf("allocated %d MB before refusing, want under 200", got>>20)
	}
}

// Writes that are not pathological are built as before: dense, and sparse
// with each point carrying a few of a few hundred keys.
func TestLPCellBudgetAdmitsOrdinaryWrites(t *testing.T) {
	p := NewLineProtocolParser()
	var dense, sparse strings.Builder
	for i := 0; i < 100_000; i++ {
		fmt.Fprintf(&dense, "cpu,host=h%d,region=r usage=%d,idle=%d,sys=1i %d\n", i%50, i, i, 1700000000000000000+int64(i))
	}
	// 100k points, each with 6 of 60 field keys (10% filled): 6M cells,
	// past the floor.
	for i := 0; i < 100_000; i++ {
		fmt.Fprintf(&sparse, "dev,id=d%d ", i%40)
		for j := 0; j < 6; j++ {
			if j > 0 {
				sparse.WriteString(",")
			}
			fmt.Fprintf(&sparse, "f%d=%d", (i+j)%60, j)
		}
		fmt.Fprintf(&sparse, " %d\n", 1700000000000000000+int64(i))
	}
	for name, body := range map[string]string{"dense": dense.String(), "sparse": sparse.String()} {
		batches, rows, drops, err := p.ParseLPWrite([]byte(body), "ns")
		if err != nil || len(drops) != 0 {
			t.Fatalf("%s: err %v, drops %v", name, err, drops)
		}
		total := 0
		for _, n := range rows {
			total += n
		}
		if total == 0 || len(batches) != 1 {
			t.Fatalf("%s: %d rows in %d batches", name, total, len(batches))
		}
	}
}

// The bound: the floor, then a multiple of the filled cells.
func TestOverPadded(t *testing.T) {
	if overPadded(lpMinPaddingCells, 0) || !overPadded(lpMinPaddingCells+1, 0) {
		t.Fatal("floor")
	}
	f := lpMinPaddingCells
	if overPadded(lpPaddingPerFilled*f, f) || !overPadded(lpPaddingPerFilled*f+1, f) {
		t.Fatal("multiple of filled")
	}
}

func uniqueKeyRecords(rows int) []*models.Record {
	recs := make([]*models.Record, 0, rows)
	for i := 0; i < rows; i++ {
		recs = append(recs, &models.Record{Measurement: "m", Timestamp: int64(i),
			Tags: map[string]string{fmt.Sprintf("k%d", i): "x"}, Fields: map[string]interface{}{"value": 1.0}})
	}
	return recs
}

// The generic converters (decimal columns configured) are checked before they
// allocate: unique keys refused, ordinary records admitted, a field renamed
// for a tag of its record counted as its own column.
func TestCheckDenseCells(t *testing.T) {
	if err := CheckDenseCells(uniqueKeyRecords(8000)); !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("unique keys: %v", err)
	}
	var dense []*models.Record
	for i := 0; i < 50_000; i++ {
		dense = append(dense, &models.Record{Measurement: "cpu", Timestamp: int64(i),
			Tags: map[string]string{"host": "a"}, Fields: map[string]interface{}{"usage": 1.0, "idle": 2.0}})
	}
	if err := CheckDenseCells(dense); err != nil {
		t.Fatalf("dense: %v", err)
	}
	// 2047 rows, each with a tag and a same-named field: 2047 x 2048 cells
	// without the renamed columns, just under the floor, and twice that with
	// them.
	var renamed []*models.Record
	for i := 0; i < 2047; i++ {
		k := fmt.Sprintf("k%d", i)
		renamed = append(renamed, &models.Record{Measurement: "m", Timestamp: int64(i),
			Tags: map[string]string{k: "x"}, Fields: map[string]interface{}{k: 1.0}})
	}
	if err := CheckDenseCells(renamed); !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("renamed fields not counted: %v", err)
	}
}

// The buffer's own row conversion (MessagePack rows through the generic
// decoder) refuses before converting.
func TestBufferWriteRefusesUniqueKeyRows(t *testing.T) {
	buf := createTestArrowBuffer(t)
	recs := make([]interface{}, 0, 8000)
	for _, r := range uniqueKeyRecords(8000) {
		recs = append(recs, r)
	}
	if err := buf.Write(context.Background(), "db", recs); !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("Write: %v, want ErrLPCellBudget", err)
	}
}

// MessagePack rows share the builder: refused, not handed to the generic
// decoder, which would build the same dense batch.
func TestMsgPackRowsUniqueKeysRefused(t *testing.T) {
	items := make([]map[string]interface{}, 0, 8000)
	for i := 0; i < 8000; i++ {
		items = append(items, map[string]interface{}{
			"m": "m", "t": int64(1700000000000 + i),
			"tags":   map[string]interface{}{fmt.Sprintf("k%d", i): "x"},
			"fields": map[string]interface{}{"value": 1.0},
		})
	}
	body, err := msgpack.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	d := NewMessagePackDecoder(zerolog.Nop())
	d.SetTypedDecodeEnabled(true) // as the handler does without decimal columns
	var derr error
	got := allocated(t, func() { _, _, _, derr = d.DecodeWithDropsKnown(body, nil) })
	if !errors.Is(derr, ErrLPCellBudget) {
		t.Fatalf("err = %v, want ErrLPCellBudget", derr)
	}
	if got > 200<<20 {
		t.Fatalf("allocated %d MB before refusing, want under 200", got>>20)
	}
}

// Shapes the budget must also see (deep review): keys sent only as NULLs,
// which finish still builds; a first row with many keys over a body of many
// lines, each key reserving the body's rows; and columns every point shares,
// which add values but no padding. Each is refused or built within ~bounded
// memory.
func TestLPCellBudgetHardShapes(t *testing.T) {
	d := NewMessagePackDecoder(zerolog.Nop())
	d.SetTypedDecodeEnabled(true)
	items := make([]map[string]interface{}, 0, 8000)
	for i := 0; i < 8000; i++ {
		items = append(items, map[string]interface{}{
			"m": "m", "t": int64(1700000000000 + i),
			"fields": map[string]interface{}{"value": 1.0, fmt.Sprintf("k%d", i): nil},
		})
	}
	nullBody, err := msgpack.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	var derr error
	if got := allocated(t, func() { _, _, _, derr = d.DecodeWithDropsKnown(nullBody, nil) }); got > 200<<20 {
		t.Fatalf("NULL-only keys: allocated %d MB (err %v)", got>>20, derr)
	}
	if !errors.Is(derr, ErrLPCellBudget) {
		t.Fatalf("NULL-only keys: %v, want ErrLPCellBudget", derr)
	}

	p := NewLineProtocolParser()
	var wide strings.Builder
	wide.WriteString("m")
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&wide, ",k%d=x", i)
	}
	wide.WriteString(" value=1 1700000000000000000")
	wide.WriteString(strings.Repeat("\n", 65535))
	if got := allocated(t, func() { _, _, _, err = p.ParseLPWrite([]byte(wide.String()), "ns") }); got > 200<<20 {
		t.Fatalf("wide first row: allocated %d MB (err %v)", got>>20, err)
	}

	var shared strings.Builder
	for i := 0; i < 8228; i++ {
		fmt.Fprintf(&shared, "m,u%d=x", i)
		shared.WriteString(" ")
		for j := 0; j < 64; j++ {
			if j > 0 {
				shared.WriteString(",")
			}
			fmt.Fprintf(&shared, "b%d=t", j)
		}
		fmt.Fprintf(&shared, " %d\n", 1700000000000000000+int64(i))
	}
	if got := allocated(t, func() { _, _, _, err = p.ParseLPWrite([]byte(shared.String()), "ns") }); got > 400<<20 {
		t.Fatalf("shared columns: allocated %d MB (err %v)", got>>20, err)
	}
	if !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("shared columns: %v, want ErrLPCellBudget", err)
	}
}

// The generic check counts one budget across measurements, and renames a
// field named like a tag of any record of its measurement.
func TestCheckDenseCellsAcrossMeasurementsAndRenames(t *testing.T) {
	var multi []*models.Record
	for m := 0; m < 4; m++ {
		for i := 0; i < 1500; i++ {
			multi = append(multi, &models.Record{Measurement: fmt.Sprintf("m%d", m), Timestamp: int64(i),
				Tags: map[string]string{fmt.Sprintf("k%d", i): "x"}, Fields: map[string]interface{}{"v": 1.0}})
		}
	}
	// 1500 x 1502 cells each, under the floor alone, over it together.
	if err := CheckDenseCells(multi); !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("across measurements: %v", err)
	}
	// Record i has tag k_i and field k_(i-1): every field is renamed, so the
	// columns are 2R, not R.
	var renamed []*models.Record
	for i := 0; i < 1500; i++ {
		renamed = append(renamed, &models.Record{Measurement: "m", Timestamp: int64(i),
			Tags: map[string]string{fmt.Sprintf("k%d", i): "x"}, Fields: map[string]interface{}{fmt.Sprintf("k%d", i-1): 1.0}})
	}
	if err := CheckDenseCells(renamed); !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("renamed across records: %v", err)
	}
}

// Body size buys nothing (re-review): a unique-key body padded with blank
// lines or a long comment is refused as the unpadded one is; and the capacity
// a first row reserves shares the bound with the padding, so a write cannot
// spend each up to the bound.
func TestLPPaddingBoundIgnoresBodySize(t *testing.T) {
	p := NewLineProtocolParser()
	base := string(uniqueKeyLP(8000))
	for name, body := range map[string]string{
		"blank lines": base + strings.Repeat("\n", 10<<20),
		"comment":     base + "# " + strings.Repeat("x", 10<<20) + "\n",
	} {
		var err error
		got := allocated(t, func() { _, _, _, err = p.ParseLPWrite([]byte(body), "ns") })
		if !errors.Is(err, ErrLPCellBudget) {
			t.Fatalf("%s: %v, want ErrLPCellBudget", name, err)
		}
		if got > 300<<20 {
			t.Fatalf("%s: allocated %d MB", name, got>>20)
		}
	}

	var two strings.Builder
	two.WriteString("a")
	for i := 0; i < 64; i++ {
		fmt.Fprintf(&two, ",t%d=x", i)
	}
	two.WriteString(" v=1 1700000000000000000\n")
	two.WriteString(strings.Repeat("\n", 65536))
	two.Write(uniqueKeyLP(2900))
	var err error
	got := allocated(t, func() { _, _, _, err = p.ParseLPWrite([]byte(two.String()), "ns") })
	if !errors.Is(err, ErrLPCellBudget) {
		t.Fatalf("reservation plus padding: %v, want ErrLPCellBudget", err)
	}
	if got > 300<<20 {
		t.Fatalf("reservation plus padding: allocated %d MB", got>>20)
	}
}
