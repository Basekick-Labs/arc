package ingest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// On a volume that folds names, a live write under another spelling of an
// existing measurement or database is a content refusal; a replayed WAL
// entry (skipWAL) was accepted when it was written and is not.
func TestLiveWriteRefusesAnotherSpelling_Issue1509(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if !backend.CaseFolds() {
		t.Skip("temp volume does not fold letter case")
	}
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4, FlushTimeoutSeconds: 2}, backend, zerolog.Nop())
	defer b.Close()
	ctx := context.Background()
	cols := func() map[string][]interface{} {
		return map[string][]interface{}{"time": {time.Now().UnixMicro()}, "v": {1.0}}
	}

	if err := b.WriteColumnarDirect(ctx, "db", "cpu", cols()); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backend.GetBasePath(), "db", "cpu")); err != nil {
		t.Fatalf("admitted measurement has no directory: %v", err)
	}
	for _, tc := range []struct{ db, meas string }{{"db", "CPU"}, {"DB", "cpu"}} {
		err := b.WriteColumnarDirect(ctx, tc.db, tc.meas, cols())
		if err == nil || !IsContentRefusal(err) || !storageSpellingText(err) {
			t.Errorf("%s/%s: got %v, want a content refusal naming the existing spelling", tc.db, tc.meas, err)
		}
		typed := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "v": []float64{1}}}
		if err := b.WriteTypedColumnarDirect(ctx, tc.db, tc.meas, typed, 1); err == nil || !IsContentRefusal(err) {
			t.Errorf("%s/%s typed: got %v, want a content refusal", tc.db, tc.meas, err)
		}
	}
	if err := b.WriteColumnarDirectReplay(ctx, "db", "CPU", cols(), ""); err != nil {
		t.Fatalf("WAL replay refused: %v", err)
	}
}

func storageSpellingText(err error) bool {
	return strings.Contains(err.Error(), "exists in storage as") && strings.Contains(err.Error(), "use the existing spelling")
}

// A write refused for its content does not claim the spelling: the name
// check runs last of the content refusals, and it creates the directories.
func TestContentRefusedWriteDoesNotClaimSpelling_Issue1509(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if !backend.CaseFolds() {
		t.Skip("temp volume does not fold letter case")
	}
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4, FlushTimeoutSeconds: 2}, backend, zerolog.Nop())
	defer b.Close()
	ctx := context.Background()
	mixed := map[string][]interface{}{"time": {time.Now().UnixMicro(), time.Now().UnixMicro()}, "v": {1.0, "x"}}
	if err := b.WriteColumnarDirect(ctx, "db", "CPU", mixed); err == nil || storageSpellingText(err) {
		t.Fatalf("mixed-type write: got %v, want a value refusal", err)
	}
	if _, err := os.Stat(filepath.Join(backend.GetBasePath(), "db", "CPU")); err == nil {
		entries, _ := os.ReadDir(filepath.Join(backend.GetBasePath(), "db"))
		t.Fatalf("a refused write created its measurement directory: %v", entries)
	}
	ok := map[string][]interface{}{"time": {time.Now().UnixMicro()}, "v": {1.0}}
	if err := b.WriteColumnarDirect(ctx, "db", "cpu", ok); err != nil {
		t.Fatalf("valid write after a refused one: %v", err)
	}
}
