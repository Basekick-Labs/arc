package ingest

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// columnUsesDictionary reports whether the named column's first row-group
// chunk was written with a dictionary encoding.
func columnUsesDictionary(t *testing.T, data []byte, colName string) bool {
	t.Helper()
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer rdr.Close()

	rg := rdr.MetaData().RowGroup(0)
	for i := 0; i < rg.NumColumns(); i++ {
		col, err := rg.ColumnChunk(i)
		if err != nil {
			t.Fatalf("column chunk %d: %v", i, err)
		}
		if col.PathInSchema().String() != colName {
			continue
		}
		for _, enc := range col.Encodings() {
			if enc == parquet.Encodings.RLEDict || enc == parquet.Encodings.PlainDict {
				return true
			}
		}
		return false
	}
	t.Fatalf("column %q not found in parquet metadata", colName)
	return false
}

func writeIOTBatch(t *testing.T, cfg *config.IngestConfig) []byte {
	t.Helper()
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	writer := NewArrowWriter(cfg, logger)
	columns := map[string]interface{}{
		"time":  []int64{1000000, 2000000, 3000000},
		"value": []float64{1.5, 2.5, 3.5},
		"host":  []string{"h1", "h2", "h1"},
	}
	data, err := writer.WriteParquetColumnar(context.Background(), "cpu", columns, nil, nil, false, nil)
	if err != nil {
		t.Fatalf("WriteParquetColumnar: %v", err)
	}
	return data
}

// TestDictionaryPerColumnType verifies the use_dictionary=true middle tier:
// string columns dictionary-encoded, numeric columns plain. (The shipped
// default is use_dictionary=false — no dictionaries at all — covered by
// TestDictionaryFullyDisabled and the config-defaults test.)
func TestDictionaryPerColumnType(t *testing.T) {
	data := writeIOTBatch(t, &config.IngestConfig{Compression: "snappy", UseDictionary: true})

	if !columnUsesDictionary(t, data, "host") {
		t.Error("host (string) should be dictionary-encoded with use_dictionary=true")
	}
	if columnUsesDictionary(t, data, "value") {
		t.Error("value (float64) should NOT be dictionary-encoded with use_dictionary=true alone")
	}
	if columnUsesDictionary(t, data, "time") {
		t.Error("time (int64) should NOT be dictionary-encoded with use_dictionary=true alone")
	}
}

// TestDictionaryNumericOptIn verifies ingest.numeric_dictionary restores
// dictionary encoding on numeric columns (pre-26.09.1 behavior).
func TestDictionaryNumericOptIn(t *testing.T) {
	data := writeIOTBatch(t, &config.IngestConfig{Compression: "snappy", UseDictionary: true, NumericDictionary: true})

	if !columnUsesDictionary(t, data, "host") {
		t.Error("host (string) should be dictionary-encoded")
	}
	if !columnUsesDictionary(t, data, "value") {
		t.Error("value (float64) should be dictionary-encoded with numeric_dictionary=true")
	}
}

// TestDictionaryFullyDisabled verifies use_dictionary=false turns dictionary
// encoding off for every column, including strings.
func TestDictionaryFullyDisabled(t *testing.T) {
	data := writeIOTBatch(t, &config.IngestConfig{Compression: "snappy", UseDictionary: false})

	if columnUsesDictionary(t, data, "host") {
		t.Error("host should not be dictionary-encoded with use_dictionary=false")
	}
	if columnUsesDictionary(t, data, "value") {
		t.Error("value should not be dictionary-encoded with use_dictionary=false")
	}
}
