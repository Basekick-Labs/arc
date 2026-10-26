package ingest

import (
	"fmt"
	"sort"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

// EstimateTypedBatchBytes is the bytes a typed batch holds, as the ingest
// memory gauge counts them.
func EstimateTypedBatchBytes(batch *TypedColumnBatch) int64 {
	return estimateTypedColumnBatchBytes(batch)
}

// HourGrouper holds a bulk import's typed batches, as parsed, until a byte
// budget, then hands them back regrouped by hour: one batch per measurement
// and hour (split by a cell cap), so the flushes write about one file per
// hour per window instead of one per hour per chunk. An unsorted file used to
// make chunks x hours files (#1070 follow-up). The batches are kept as they
// came (never concatenated): the window's memory is the sum of its batches,
// and a batch with new columns does not pad every row held.
type HourGrouper struct {
	budget  int64
	bytes   int64
	windows map[string][]groupedBatch
	order   []string
}

type groupedBatch struct {
	batch *TypedColumnBatch
	rows  int
}

// NewHourGrouper groups up to budget bytes; budget 0 hands every batch back
// at the next Drain.
func NewHourGrouper(budget int64) *HourGrouper {
	return &HourGrouper{budget: budget, windows: map[string][]groupedBatch{}}
}

// Add takes a batch of measurement and reports whether the window is now at
// or over its budget (time to Drain).
func (g *HourGrouper) Add(measurement string, batch *TypedColumnBatch, rows int) bool {
	if rows <= 0 {
		return g.bytes >= g.budget
	}
	if _, ok := g.windows[measurement]; !ok {
		g.order = append(g.order, measurement)
	}
	g.windows[measurement] = append(g.windows[measurement], groupedBatch{batch: batch, rows: rows})
	g.bytes += EstimateTypedBatchBytes(batch)
	return g.bytes >= g.budget
}

// Bytes is what the window holds.
func (g *HourGrouper) Bytes() int64 { return g.bytes }

// Empty reports whether the window holds nothing.
func (g *HourGrouper) Empty() bool { return len(g.order) == 0 }

// Drain hands the window back to write, measurement by measurement (in the
// order they first came), hour by hour (ascending), in batches of at most
// maxCells cells (rows x columns): a write's WAL row maps stay the size of
// today's per-chunk write. Every batch of a measurement carries the window's
// full column set (NULL where a row had none), so they share one schema and
// no schema change flushes between them. The window is emptied whether or
// not write fails; rows not yet handed to write are dropped with it.
func (g *HourGrouper) Drain(maxCells int, write func(measurement string, batch *TypedColumnBatch, rows int) error) error {
	defer g.reset()
	for _, m := range g.order {
		if err := drainMeasurement(g.windows[m], maxCells, func(b *TypedColumnBatch, n int) error { return write(m, b, n) }); err != nil {
			return err
		}
	}
	return nil
}

func (g *HourGrouper) reset() {
	g.windows = map[string][]groupedBatch{}
	g.order = nil
	g.bytes = 0
}

// rowRef addresses a row of the window: chunk index and row index.
type rowRef struct{ chunk, row int32 }

func drainMeasurement(chunks []groupedBatch, maxCells int, write func(*TypedColumnBatch, int) error) error {
	// The window's column set, each with one Go type.
	colType := map[string]string{}
	var cols []string
	tags := map[string]struct{}{}
	dedup := false
	for _, c := range chunks {
		for name, data := range c.batch.Data {
			t := fmt.Sprintf("%T", data)
			if prev, ok := colType[name]; !ok {
				colType[name] = t
				cols = append(cols, name)
			} else if prev != t {
				return fmt.Errorf("column %q is %s in one part of the import and %s in another", name, prev, t)
			}
		}
		for _, tag := range c.batch.TagColumns {
			tags[tag] = struct{}{}
		}
		dedup = dedup || c.batch.DedupTime
	}
	sort.Strings(cols)
	tagColumns := make([]string, 0, len(tags))
	for tag := range tags {
		tagColumns = append(tagColumns, tag)
	}
	sort.Strings(tagColumns)

	// Rows by hour. int32 indices: a window is at most a few hundred MB of
	// rows (the grouping budget), far below 2^31 rows or chunks.
	byHour := map[int64][]rowRef{}
	for ci, c := range chunks {
		times, ok := c.batch.Data["time"].([]int64)
		if !ok {
			return fmt.Errorf("a batch has no time column")
		}
		for ri := 0; ri < c.rows && ri < len(times); ri++ {
			h := HourBucketID(times[ri])
			byHour[h] = append(byHour[h], rowRef{int32(ci), int32(ri)})
		}
	}
	hours := make([]int64, 0, len(byHour))
	for h := range byHour {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })

	maxRows := max(1, maxCells/max(len(cols), 1))
	for _, h := range hours {
		refs := byHour[h]
		for start := 0; start < len(refs); start += maxRows {
			part := refs[start:min(start+maxRows, len(refs))]
			b, err := gatherRows(chunks, cols, part)
			if err != nil {
				return err
			}
			b.TagColumns = tagColumns
			b.DedupTime = dedup
			if err := write(b, len(part)); err != nil {
				return err
			}
		}
		delete(byHour, h) // free the hour's index as it is written
	}
	return nil
}

// gatherRows copies rows of the window into a fresh batch over cols; a row
// whose chunk lacks a column, or holds NULL there, is NULL.
func gatherRows(chunks []groupedBatch, cols []string, refs []rowRef) (*TypedColumnBatch, error) {
	n := len(refs)
	out := &TypedColumnBatch{Data: make(map[string]interface{}, len(cols)), Validity: map[string][]bool{}}
	for _, name := range cols {
		var valid []bool
		setNull := func(i int) {
			if valid == nil {
				valid = make([]bool, n)
				for j := 0; j < i; j++ {
					valid[j] = true
				}
			}
		}
		setValid := func(i int) {
			if valid != nil {
				valid[i] = true
			}
		}
		present := func(r rowRef) (interface{}, bool) {
			c := chunks[r.chunk].batch
			data, ok := c.Data[name]
			if !ok {
				return nil, false
			}
			if v := c.Validity[name]; v != nil && int(r.row) < len(v) && !v[r.row] {
				return nil, false
			}
			return data, true
		}
		var dst interface{}
		switch chunkTypeOf(chunks, name).(type) {
		case []int64:
			arr := make([]int64, n)
			for i, r := range refs {
				if data, ok := present(r); ok {
					arr[i] = data.([]int64)[r.row]
					setValid(i)
				} else {
					setNull(i)
				}
			}
			dst = arr
		case []float64:
			arr := make([]float64, n)
			for i, r := range refs {
				if data, ok := present(r); ok {
					arr[i] = data.([]float64)[r.row]
					setValid(i)
				} else {
					setNull(i)
				}
			}
			dst = arr
		case []string:
			arr := make([]string, n)
			for i, r := range refs {
				if data, ok := present(r); ok {
					arr[i] = data.([]string)[r.row]
					setValid(i)
				} else {
					setNull(i)
				}
			}
			dst = arr
		case []bool:
			arr := make([]bool, n)
			for i, r := range refs {
				if data, ok := present(r); ok {
					arr[i] = data.([]bool)[r.row]
					setValid(i)
				} else {
					setNull(i)
				}
			}
			dst = arr
		case [][]byte:
			// Parquet import keeps binary columns as bytes (#440).
			arr := make([][]byte, n)
			for i, r := range refs {
				if data, ok := present(r); ok {
					arr[i] = data.([][]byte)[r.row]
					setValid(i)
				} else {
					setNull(i)
				}
			}
			dst = arr
		case []decimal128.Num:
			arr := make([]decimal128.Num, n)
			for i, r := range refs {
				if data, ok := present(r); ok {
					arr[i] = data.([]decimal128.Num)[r.row]
					setValid(i)
				} else {
					setNull(i)
				}
			}
			dst = arr
		default:
			return nil, fmt.Errorf("column %q has an unsupported type %T", name, chunkTypeOf(chunks, name))
		}
		out.Data[name] = dst
		if valid != nil {
			if name == "time" {
				return nil, fmt.Errorf("a row has no time")
			}
			out.Validity[name] = valid
		}
	}
	return out, nil
}

// chunkTypeOf is the column's data in the first chunk that has it (for its
// type: drainMeasurement checked they all agree).
func chunkTypeOf(chunks []groupedBatch, name string) interface{} {
	for _, c := range chunks {
		if d, ok := c.batch.Data[name]; ok {
			return d
		}
	}
	return nil
}
