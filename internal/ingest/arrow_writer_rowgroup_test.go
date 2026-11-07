package ingest

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/config"
)

// rowGroupCols builds a flush of n rows shaped like a tag + two fields.
func rowGroupCols(n int) map[string]interface{} {
	ts := make([]int64, n)
	host := make([]string, n)
	v := make([]float64, n)
	for i := 0; i < n; i++ {
		ts[i] = 1_700_000_000_000_000 + int64(i)*1000
		host[i] = "h" + string(rune('a'+i%7))
		v[i] = float64(i % 101)
	}
	return map[string]interface{}{"time": ts, "host": host, "value": v}
}

// ingest.max_row_group_rows caps the rows per Parquet row group: a 300k-row
// flush becomes 3 groups at the compaction-sized default, one group at 0 (the
// writer's own default, the pre-cap behaviour), and a non-default cap splits
// accordingly. Row groups are the unit of min/max pruning and of parallel
// decode for every reader, so this is observable query behaviour, not layout.
func TestWriteParquetColumnar_RowGroupCap(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	cols := rowGroupCols(300_000)
	cases := []struct {
		name   string
		cap    int
		groups int
	}{
		{"default 122880", 122880, 3},
		{"zero keeps one group per flush", 0, 1},
		{"non-default 50000", 50_000, 6},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &config.IngestConfig{Compression: "snappy", WriteStatistics: true, DataPageVersion: "2.0", MaxRowGroupRows: c.cap}
			w := NewArrowWriter(cfg, logger)
			data, err := w.WriteParquetColumnar(context.Background(), "cpu", cols, nil, []string{"host"}, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			rdr, err := file.NewParquetReader(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			defer rdr.Close()
			if got := rdr.MetaData().NumRowGroups(); got != c.groups {
				t.Fatalf("row groups = %d, want %d", got, c.groups)
			}
			if got := rdr.MetaData().GetNumRows(); got != 300_000 {
				t.Fatalf("rows = %d, want 300000", got)
			}
		})
	}
}

// ingest.write_page_index writes the column and offset index for every column
// chunk; off, neither exists (the pre-change file). Nothing in Arc reads them
// yet — the test pins that the bytes are there for the reader that will.
func TestWriteParquetColumnar_PageIndex(t *testing.T) {
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	cols := rowGroupCols(20_000)
	for _, on := range []bool{true, false} {
		cfg := &config.IngestConfig{Compression: "snappy", WriteStatistics: true, DataPageVersion: "2.0", MaxRowGroupRows: 122880, WritePageIndex: on}
		w := NewArrowWriter(cfg, logger)
		data, err := w.WriteParquetColumnar(context.Background(), "cpu", cols, nil, []string{"host"}, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		rdr, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		rg := rdr.MetaData().RowGroup(0)
		for i := 0; i < rg.NumColumns(); i++ {
			cc, err := rg.ColumnChunk(i)
			if err != nil {
				t.Fatal(err)
			}
			hasOffset := cc.GetOffsetIndexLocation() != nil
			hasColumn := cc.GetColumnIndexLocation() != nil
			if hasOffset != on || hasColumn != on {
				t.Fatalf("write_page_index=%v: column %d offset_index=%v column_index=%v", on, i, hasOffset, hasColumn)
			}
		}
		rdr.Close()
	}
}
