package iceberg

import (
	"context"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestMeasurementsSkipSchemaAnchorsIssue914(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, k := range []string{"mydb/cpu/2026/07/14/15/a.parquet", "_schema/mydb/cpu.parquet", "_compaction_state/hourly/mydb/j.json"} {
		if err := backend.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].Database != "mydb" || ms[0].Measurement != "cpu" {
		t.Fatalf("measurements=%+v", ms)
	}
}
