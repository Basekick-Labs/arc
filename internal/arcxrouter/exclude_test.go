//go:build cgo && arcx_engine

package arcxrouter

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/arcxengine"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The engine lists the measurement itself; a file Deps.Exclude names (one
// side of an open compaction) is not in the list it is handed.
func TestBuildEngineSQLLeavesOutExcludedFiles(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "db", "m", "2026", "10", "20", "10")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"in.parquet", "out_b1_compacted.parquet"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	be, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	deps := Deps{Storage: be, Logger: zerolog.Nop(), Mode: ModeShadow,
		Exclude: func(_ context.Context, prefix string) []string {
			if prefix != "db/m/" {
				t.Errorf("prefix %q", prefix)
			}
			return []string{"db/m/2026/10/20/10/in.parquet"}
		}}
	d := Decision{Eligible: true, Shape: ShapeCountStar,
		Ctx: arcxengine.Context{Database: "db", Measurement: "m", TimeColumn: "time", AllowedDirs: []string{root + "/"}}}
	sql, ok := deps.buildEngineSQL(context.Background(), d)
	if !ok {
		t.Fatal("buildEngineSQL declined")
	}
	if strings.Contains(sql, "in.parquet") || !strings.Contains(sql, "out_b1_compacted.parquet") {
		t.Fatalf("engine SQL %q: want the output only", sql)
	}
}
