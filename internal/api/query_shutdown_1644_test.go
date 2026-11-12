//go:build duckdb_arrow

package api

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1644, the issue's reproduction: an OSS node (no query registry) running a
// long query on /api/v1/query/arrow or /api/v1/query. The HTTP drain cancels
// the query when its grace is over and returns nil within its budget,
// instead of waiting for the query.
func TestShutdownEndsALongOSSQuery(t *testing.T) {
	for _, path := range []string{"/api/v1/query/arrow", "/api/v1/query"} {
		t.Run(strings.TrimPrefix(path, "/api/v1/"), func(t *testing.T) {
			tmpDir := t.TempDir()
			logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
			backend, err := storage.NewLocalBackend(tmpDir, logger)
			if err != nil {
				t.Fatal(err)
			}
			duckdb, err := database.New(&database.Config{
				MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: tmpDir,
			}, logger)
			if err != nil {
				t.Fatal(err)
			}
			s := newShutdownTestServer(t)
			h := &QueryHandler{
				db:         duckdb,
				logger:     logger,
				storage:    backend,
				queryCache: database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
				pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
			}
			h.SetLifetime(s.QueryLifetime())
			h.RegisterRoutes(s.app)
			addr := serve1644(t, s)

			done := make(chan int, 1)
			go func() {
				resp, err := http.Post("http://"+addr+path, "application/json",
					strings.NewReader(`{"sql":"SELECT sum(a.range * b.range) FROM range(200000) a, range(200000) b"}`))
				if err != nil {
					done <- -1
					return
				}
				resp.Body.Close()
				done <- resp.StatusCode
			}()
			time.Sleep(700 * time.Millisecond) // the query is running

			err, took := shutdownWithin(s, 2*time.Second)
			if err != nil {
				t.Fatalf("shutdown error: %v (took %v)", err, took)
			}
			if took > 2200*time.Millisecond {
				t.Fatalf("drain took %v", took)
			}
			select {
			case code := <-done:
				if code == http.StatusOK {
					t.Fatalf("the query completed (status %d) instead of being cancelled", code)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("client never got an answer")
			}
			// The query is over: closing the database does not wait on it.
			closed := make(chan struct{})
			go func() { duckdb.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("database Close waited on a query still running")
			}
		})
	}
}
