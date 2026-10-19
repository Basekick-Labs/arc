package api

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// panicOnNext decodes `good` batches, then panics, as arrow-go does when a
// page decompressor panics on crafted input.
type panicOnNext struct{ good, calls int }

func (p *panicOnNext) Next() bool {
	p.calls++
	if p.calls > p.good {
		panic("runtime error: index out of range [33077] with length 32768")
	}
	return true
}
func (p *panicOnNext) RecordBatch() arrow.RecordBatch {
	return array.NewRecordBatch(arrow.NewSchema(nil, nil), nil, 1)
}
func (p *panicOnNext) Err() error { return nil }

// A panic while decoding an uploaded Parquet file is a corrupt-file import
// error (422), after the batches decoded before it; a panic in Arc's own
// batch callback is not swallowed.
func TestReadParquetBatchesDecodePanicIsAnError(t *testing.T) {
	seen := 0
	err := readParquetBatches(&panicOnNext{good: 2}, func(arrow.RecordBatch, int) error {
		seen++
		return nil
	})
	var ie *importError
	if !errors.As(err, &ie) || ie.StatusCode != 422 {
		t.Fatalf("err %v, want a 422 importError", err)
	}
	if seen != 2 {
		t.Fatalf("callback saw %d batches, want 2", seen)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("a panic in the callback was swallowed")
		}
	}()
	_ = readParquetBatches(&panicOnNext{good: 1}, func(arrow.RecordBatch, int) error { panic("arc bug") })
}
