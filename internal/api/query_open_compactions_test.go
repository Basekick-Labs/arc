//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type fakeOpenCompactions struct {
	mu   sync.Mutex
	keys []string
	gen  uint64
}

func (f *fakeOpenCompactions) ExcludedFor(_ context.Context, prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, k := range f.keys {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out
}

func (f *fakeOpenCompactions) Generation() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gen
}

func (f *fakeOpenCompactions) set(keys ...string) {
	f.mu.Lock()
	f.keys = keys
	f.gen++
	f.mu.Unlock()
}

// A compaction output beside its inputs: a read counts its rows once,
// whichever side the registry names, on every wire format and through a warm
// cache; with no open job the read is as before (and doubles, which is the
// bug the registry exists to stop).
func TestQueryCountsAnOpenCompactionOnce(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	for _, at := range []string{"2026-10-20 10:10:00", "2026-10-20 10:20:00"} {
		ts, _ := time.Parse("2006-01-02 15:04:05", at)
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", "m", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{ts.UnixMicro()}, "v": []float64{1}},
		}, 1))
		require.NoError(t, buf.FlushAllAndWait(context.Background()))
	}
	hourKey := "db1/m/2026/10/20/10"
	inputs, err := filepath.Glob(filepath.Join(dataDir, hourKey, "*.parquet"))
	require.NoError(t, err)
	require.Len(t, inputs, 2)

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	// The compaction's output: both inputs' rows in one file.
	outKey := hourKey + "/m_20261020_110000_1_b1_compacted.parquet"
	_, err = duck.DB().Exec(fmt.Sprintf("COPY (SELECT * FROM read_parquet(['%s','%s'])) TO '%s' (FORMAT PARQUET)",
		inputs[0], inputs[1], filepath.Join(dataDir, outKey)))
	require.NoError(t, err)
	inKeys := []string{hourKey + "/" + filepath.Base(inputs[0]), hourKey + "/" + filepath.Base(inputs[1])}

	h := NewQueryHandler(duck, backend, logger, 0, 0)
	oc := &fakeOpenCompactions{}
	h.SetOpenCompactions(oc)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	defer app.Shutdown()
	client := &http.Client{Timeout: 20 * time.Second}
	status := func(route, sql string) (int, []byte) {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		resp, err := client.Post("http://"+ln.Addr().String()+route, "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	}
	count := func(sql string) float64 {
		code, out := status("/api/v1/query", sql)
		require.Equal(t, 200, code, string(out))
		var r struct{ Data [][]interface{} }
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		n, _ := r.Data[0][0].(float64)
		return n
	}
	for _, q := range []string{
		"SELECT COUNT(*) FROM db1.m",
		"SELECT COUNT(*) FROM db1.m WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 11:00:00'",
	} {
		require.Equal(t, float64(4), count(q), "precondition: with no open job the read doubles (%s)", q)
		oc.set(inKeys...) // output present: the inputs are left out
		require.Equal(t, float64(2), count(q), "warm cache must not survive the registry changing (%s)", q)
		oc.set(outKey) // output named as absent: the output is left out
		require.Equal(t, float64(2), count(q), q)
		oc.set()
	}
	// The other wire formats read through the same transform.
	oc.set(inKeys...)
	for _, route := range []string{"/api/v1/query/arrow", "/api/v1/query/msgpack"} {
		code, out := status(route, "SELECT COUNT(*) FROM db1.m")
		require.Equal(t, 200, code, "%s: %s", route, string(out))
	}
	// A file column in the data does not collide with the exclusion's.
	require.Equal(t, float64(2), count("SELECT COUNT(*) FROM db1.m WHERE v = 1"))
}
