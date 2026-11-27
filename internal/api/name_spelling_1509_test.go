package api

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// On a volume that folds names, a transform is not cached: a query for a
// spelling accepted while no spelling of it existed must be refused once
// another spelling is created, not answered from a cached glob that DuckDB
// would fold onto the new directory (#1509).
func TestTransformNotCachedOnFoldingStorage_Issue1509(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if !backend.CaseFolds() {
		t.Skip("temp volume does not fold letter case")
	}
	h := &QueryHandler{
		storage:    backend,
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
		queryCache: database.NewQueryCache(time.Minute, 128),
		logger:     zerolog.Nop(),
	}
	const sql = "SELECT * FROM db.CPU"
	transform := func() error {
		ctx, _ := withStoragePathFailure(context.Background())
		_, _, _, err := h.getTransformedSQLWithRefs(ctx, sql, "")
		return err
	}
	if err := transform(); err != nil {
		t.Fatalf("no spelling exists yet: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(backend.GetBasePath(), "db", "cpu"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := transform(); !storage.IsNameSpellingError(err) {
		t.Fatalf("after cpu was created: got %v, want a spelling refusal", err)
	}
}
