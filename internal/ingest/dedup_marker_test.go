package ingest

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// Continuous-query output is marked arc:dedup_exact, never arc:dedup_time:
// builds before 26.09.4 read the old key as "dedup on time alone" (#1232).
func TestDedupMarkerIsExactNotTime(t *testing.T) {
	w := NewArrowWriter(&config.IngestConfig{Compression: "snappy"}, zerolog.Nop())
	cols := map[string]interface{}{"time": []int64{1000000}, "value": []float64{1}}
	for _, marked := range []bool{true, false} {
		data, err := w.WriteParquetColumnar(context.Background(), "cq", cols, nil, nil, marked, nil)
		if err != nil {
			t.Fatal(err)
		}
		rdr, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		kv := rdr.MetaData().KeyValueMetadata()
		rdr.Close()
		exact := kv.FindValue("arc:dedup_exact")
		if marked != (exact != nil && *exact == "true") {
			t.Errorf("marked=%v: arc:dedup_exact = %v", marked, exact)
		}
		if kv.FindValue("arc:dedup_time") != nil {
			t.Errorf("marked=%v: the old arc:dedup_time key was written", marked)
		}
	}
}
