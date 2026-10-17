package arcxrouter

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestEngineParquetEntries_PrunesToTimeRange(t *testing.T) {
	base := t.TempDir()
	local, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	for _, rel := range []string{
		"prod/cpu/2026/02/03/09/before.parquet",
		"prod/cpu/2026/02/03/10/in-range-a.parquet",
		"prod/cpu/2026/02/03/11/in-range-b.parquet",
		"prod/cpu/2026/02/03/12/upper-bound-hour.parquet",
		"prod/cpu/2026/02/03/13/after.parquet",
		"prod/cpu/2026/02/03/daily.parquet",
		"prod/cpu/2025/02/03/10/history.parquet",
	} {
		path := filepath.Join(base, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	whereText := "time >= '2026-02-03T10:15:00Z' AND time <= '2026-02-03T12:00:00Z'"
	entries, ok := engineParquetEntries(
		context.Background(), local, ShapeDateTruncCent, "prod", "cpu", whereText,
		nil, pruning.NewPartitionPruner(zerolog.Nop()),
	)
	if !ok {
		t.Fatal("expected the time range to produce a narrowed file list")
	}
	want := []string{
		"prod/cpu/2026/02/03/10/in-range-a.parquet",
		"prod/cpu/2026/02/03/11/in-range-b.parquet",
		"prod/cpu/2026/02/03/12/upper-bound-hour.parquet",
		"prod/cpu/2026/02/03/daily.parquet",
	}
	if len(entries) != len(want) {
		t.Fatalf("entries = %v, want %v", entries, want)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Fatalf("entries = %v, want %v", entries, want)
		}
	}
}

func TestEngineParquetEntries_DeclinesUnsafeOrUnboundedShapes(t *testing.T) {
	base := t.TempDir()
	local, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	pruner := pruning.NewPartitionPruner(zerolog.Nop())
	timeWhere := "time >= '2026-02-03T10:00:00Z' OR time < '2026-02-03T12:00:00Z'"

	tests := []struct {
		name             string
		shape, whereText string
		preds            []scanPred
	}{
		{
			name:      "non-date-trunc shape",
			shape:     ShapeScanAgg,
			whereText: timeWhere,
		},
		{
			name:  "unfiltered date-trunc shape",
			shape: ShapeDateTruncCent,
		},
		{
			name:      "scan boolean tree",
			shape:     ShapeScan,
			whereText: timeWhere,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if entries, ok := engineParquetEntries(
				context.Background(), local, tt.shape, "prod", "cpu", tt.whereText, tt.preds, pruner,
			); ok || entries != nil {
				t.Fatalf("expected no narrowing, got entries %v", entries)
			}
		})
	}
}

func TestEngineParquetEntries_UsesFlatScanTimeBounds(t *testing.T) {
	base := t.TempDir()
	local, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{
		"prod/cpu/2026/02/03/10/matching.parquet",
		"prod/cpu/2026/02/03/13/out-of-range.parquet",
		"prod/cpu/2025/02/03/10/history.parquet",
	} {
		path := filepath.Join(base, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	preds := []scanPred{
		{col: "host", op: "=", str: "web1", isStr: true},
		{col: "time", op: ">=", str: "2026-02-03T10:15:00Z", isStr: true},
		{col: "time", op: "<", str: "2026-02-03T12:00:00Z", isStr: true},
	}
	entries, ok := engineParquetEntries(
		context.Background(), local, ShapeScan, "prod", "cpu", "", preds,
		pruning.NewPartitionPruner(zerolog.Nop()),
	)
	if !ok {
		t.Fatal("expected flat scan time bounds to produce a narrowed file list")
	}
	if len(entries) != 1 || entries[0] != "prod/cpu/2026/02/03/10/matching.parquet" {
		t.Fatalf("entries = %v, want only the matching partition file", entries)
	}
}

func TestEngineParquetEntries_CancelledContextDeclines(t *testing.T) {
	base := t.TempDir()
	local, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	whereText := "time >= '2026-02-03T10:00:00Z' AND time < '2026-02-03T12:00:00Z'"
	if entries, ok := engineParquetEntries(
		ctx, local, ShapeDateTruncCent, "prod", "cpu", whereText,
		nil, pruning.NewPartitionPruner(zerolog.Nop()),
	); ok || entries != nil {
		t.Fatalf("expected cancelled range expansion to decline, got %v", entries)
	}
}
