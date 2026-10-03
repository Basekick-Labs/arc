package ingest

import (
	"reflect"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

func TestMsgpackColumnarTagKeys(t *testing.T) {
	tests := []struct {
		name    string
		typed   bool
		withTag bool
		want    []string
	}{
		{
			name:    "generic_with_tags",
			withTag: true,
			want:    []string{"host", "region"},
		},
		{
			name:    "typed_with_tags",
			typed:   true,
			withTag: true,
			want:    []string{"host", "region"},
		},
		{
			name: "generic_without_tags",
		},
		{
			name:  "typed_without_tags",
			typed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := map[string]interface{}{
				"m": "cpu",
				"columns": map[string]interface{}{
					"time":   []interface{}{int64(1700000000000000)},
					"host":   []interface{}{"server01"},
					"region": []interface{}{"eu"},
					"value":  []interface{}{42.5},
				},
			}

			if tc.withTag {
				payload["tag_keys"] = []interface{}{"host", "region"}
			}

			data, err := msgpack.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			decoder := NewMessagePackDecoder(zerolog.Nop())
			decoder.SetTypedDecodeEnabled(tc.typed)

			result, err := decoder.Decode(data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			records, ok := result.([]interface{})
			if !ok || len(records) != 1 {
				t.Fatalf("expected one record, got %T: %v", result, result)
			}

			var tags []string
			switch record := records[0].(type) {
			case *models.ColumnarRecord:
				tags = record.TagColumns
			case *TypedColumnarRecord:
				if record.Batch == nil {
					t.Fatal("typed record has nil batch")
				}
				tags = record.Batch.TagColumns
			default:
				t.Fatalf("unexpected record type: %T", records[0])
			}

			if !reflect.DeepEqual(tags, tc.want) {
				t.Errorf("tag columns = %v, want %v", tags, tc.want)
			}
		})
	}
}
