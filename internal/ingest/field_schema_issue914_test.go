package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/storage"
)

type recordingRegistrar struct {
	mu    sync.Mutex
	calls []struct {
		db, meas string
		fields   []string
		tags     []string
	}
}

func (r *recordingRegistrar) EnsureFile(_ context.Context, db, meas string, schema *arrow.Schema, tags []string, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var names []string
	for _, f := range schema.Fields() {
		names = append(names, f.Name)
	}
	r.calls = append(r.calls, struct {
		db, meas string
		fields   []string
		tags     []string
	}{db, meas, names, tags})
	return nil
}

func ingestTestConfig() *config.IngestConfig {
	return &config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 2, FlushQueueSize: 8, ShardCount: 4, DataPageVersion: "2.0"}
}

func TestFlushRegistersFieldSchemaIssue914(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := NewArrowBuffer(ingestTestConfig(), backend, zerolog.Nop())
	defer buf.Close()
	rec := &recordingRegistrar{}
	buf.SetFieldSchema(rec)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	// Two hours in one batch: the multi-hour flush path registers per file.
	cols := map[string][]interface{}{
		"time":  {base, base + int64(2*time.Hour/time.Microsecond)},
		"value": {1.5, 2.5},
	}
	if err := buf.WriteColumnarDirectNoWAL(ctx, "db", "cpu", cols); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.calls) != 2 {
		t.Fatalf("expected one registration per written file, got %d: %+v", len(rec.calls), rec.calls)
	}
	for _, c := range rec.calls {
		if c.db != "db" || c.meas != "cpu" || len(c.fields) != 2 {
			t.Fatalf("registration %+v", c)
		}
	}
}

func TestFlushWritesStoredAnchorIssue914(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	reg := fieldschema.New(backend, nil, fieldschema.Options{Enabled: true}, zerolog.Nop())
	buf := NewArrowBuffer(ingestTestConfig(), backend, zerolog.Nop())
	defer buf.Close()
	buf.SetFieldSchema(reg)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	write := func(cols map[string][]interface{}) {
		t.Helper()
		if err := buf.WriteColumnarDirectNoWAL(ctx, "db", "cpu", cols); err != nil {
			t.Fatal(err)
		}
		if err := buf.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	write(map[string][]interface{}{"time": {base}, "value": {int64(1)}})
	fields, ok, err := reg.Fields(ctx, "db", "cpu")
	if err != nil || !ok || len(fields) != 2 || fields[1].Type != "BIGINT" {
		t.Fatalf("after first flush: %v %v %v", fields, ok, err)
	}
	// A later flush adds a column and writes the same one as a float: the
	// column is appended, the type stays BIGINT (narrowest wins).
	write(map[string][]interface{}{"time": {base + 1}, "value": {2.5}, "extra": {"x"}})
	fields, _, _ = reg.Fields(ctx, "db", "cpu")
	if len(fields) != 3 || fields[1].Name != "value" || fields[1].Type != "BIGINT" || fields[2].Name != "extra" {
		t.Fatalf("after second flush: %v", fields)
	}
	if ok, _ := backend.Exists(ctx, fieldschema.AnchorKey("db", "cpu")); !ok {
		t.Fatal("stored anchor missing")
	}
}
