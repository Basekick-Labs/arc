package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// toggleFailBackend fails every write while fail is set.
type toggleFailBackend struct {
	storage.Backend
	fail atomic.Bool
}

func (b *toggleFailBackend) Write(ctx context.Context, path string, data []byte) error {
	if b.fail.Load() {
		return errors.New("injected storage failure")
	}
	return b.Backend.Write(ctx, path, data)
}

func (b *toggleFailBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	if b.fail.Load() {
		return errors.New("injected storage failure")
	}
	return b.Backend.WriteReader(ctx, path, r, size)
}

// latchBuffer is a real ingest buffer whose storage can be made to fail.
func latchBuffer(t *testing.T) (*ingest.ArrowBuffer, *toggleFailBackend) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	backend := &toggleFailBackend{Backend: local}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1000000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 8, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })
	return buf, backend
}

// failAFlush makes a real flush of db/cpu fail, which sets the latch's
// flush-failure reason and moves its generation.
func failAFlush(t *testing.T, buf *ingest.ArrowBuffer, backend *toggleFailBackend) {
	t.Helper()
	require.NoError(t, failAFlushErr(buf, backend))
}

// failAFlushErr is failAFlush for a goroutine that is not the test's.
func failAFlushErr(buf *ingest.ArrowBuffer, backend *toggleFailBackend) error {
	backend.fail.Store(true)
	defer backend.fail.Store(false)
	before := buf.FlushFailureGeneration()
	ts := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC).UnixMicro()
	if err := buf.WriteColumnarDirect(context.Background(), "db", "cpu",
		map[string][]interface{}{"time": {ts, ts + 1}, "host": {"h1", "h1"}, "v": {int64(1), int64(2)}}); err != nil {
		return err
	}
	if err := buf.FlushAllAndWait(context.Background()); err == nil {
		return errors.New("the flush did not fail")
	}
	for deadline := time.Now().Add(5 * time.Second); buf.FlushFailureGeneration() == before; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			return errors.New("the flush failed without moving the failure generation")
		}
	}
	return nil
}

func sendLatchDelete(t *testing.T, h *DeleteHandler, body string) (int, DeleteResponse) {
	t.Helper()
	app := fiber.New()
	h.RegisterRoutes(app)
	req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 20000)
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	var out DeleteResponse
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

const latchDeleteBody = `{"database":"db","measurement":"cpu","where":"host = 'h1'","confirm":true}`

// #1938 W1: a flush that fails after the DELETE read the latch and before its
// flush started refused nothing: the flush read the generation itself, which
// already counted the failure, and the DELETE went ahead while the failed
// rows waited for a replay that would bring them back. The DELETE now holds
// its flush to the generation it checked.
func TestDeleteRefusesAFlushFailureBetweenTheCheckAndTheFlush1938(t *testing.T) {
	shortGiveUp(t, 100*time.Millisecond)
	root, backend, db := recordsDuckDB(t)
	const rel = "db/cpu/2026/10/11/10/cpu_a.parquet"
	writeRecordsParquet(t, db, root, rel)
	buf, bufBackend := latchBuffer(t)

	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetFlusher(buf)
	flushBeforeDeleteAfterState = func() {
		if err := failAFlushErr(buf, bufBackend); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { flushBeforeDeleteAfterState = nil })

	code, out := sendLatchDelete(t, h, latchDeleteBody)
	require.Equal(t, 503, code, "%+v", out)
	require.Contains(t, out.Error, errFlushBeforeDelete.Error())
	require.Equal(t, 4, countParquet(t, db, filepath.Join(root, rel), "true"), "the file was rewritten")
	require.Equal(t, 2, countParquet(t, db, filepath.Join(root, rel), "host = 'h1'"))
}

// The two reasons refuse with their own messages: a recovery pass that left
// files to replay is not a flush failure (#1170). DROP DATABASE goes through
// the same flushBeforeDelete.
func TestFlushBeforeDeleteNamesTheReason1938(t *testing.T) {
	t.Run("a flush failed", func(t *testing.T) {
		buf, backend := latchBuffer(t)
		failAFlush(t, buf, backend)
		_, err := flushBeforeDelete(context.Background(), buf, "db", "cpu")
		require.ErrorIs(t, err, errFlushBeforeDelete)
		require.Contains(t, err.Error(), "an earlier flush failed and its rows wait for WAL recovery")
	})
	t.Run("a recovery pass left files", func(t *testing.T) {
		buf, _ := latchBuffer(t)
		buf.MarkWALRecoveryPending()
		_, err := flushBeforeDelete(context.Background(), buf, "db", "cpu")
		require.ErrorIs(t, err, errFlushBeforeDelete)
		require.Contains(t, err.Error(), "a WAL recovery pass left files to replay")
		require.NotContains(t, err.Error(), "flush failed")
	})
	t.Run("both: the flush failure is named", func(t *testing.T) {
		buf, backend := latchBuffer(t)
		buf.MarkWALRecoveryPending()
		failAFlush(t, buf, backend)
		_, err := flushBeforeDelete(context.Background(), buf, "db", "cpu")
		require.Contains(t, err.Error(), "an earlier flush failed")
	})
	t.Run("clear", func(t *testing.T) {
		buf, _ := latchBuffer(t)
		_, err := flushBeforeDelete(context.Background(), buf, "db", "cpu")
		require.NoError(t, err)
	})
}

// End to end with a real buffer: a failed flush refuses the DELETE until a
// recovery pass resets the latch, then the DELETE runs. The pass itself is
// inline in cmd/arc; this calls ResetFlushFailure as it does.
func TestDeleteRefusedUntilTheLatchResets1938(t *testing.T) {
	shortGiveUp(t, 100*time.Millisecond)
	root, backend, db := recordsDuckDB(t)
	const rel = "db/cpu/2026/10/11/10/cpu_a.parquet"
	writeRecordsParquet(t, db, root, rel)
	buf, bufBackend := latchBuffer(t)
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	h.SetFlusher(buf)

	failAFlush(t, buf, bufBackend)
	code, out := sendLatchDelete(t, h, latchDeleteBody)
	require.Equal(t, 503, code, "%+v", out)
	require.Contains(t, out.Error, "an earlier flush failed")

	generation := buf.FlushFailureGeneration()
	require.True(t, buf.ResetFlushFailure(generation))
	code, out = sendLatchDelete(t, h, latchDeleteBody)
	require.Equal(t, 200, code, "%+v", out)
	require.EqualValues(t, 2, out.DeletedCount)

	// A pending recovery refuses with its own message.
	buf.MarkWALRecoveryPending()
	code, out = sendLatchDelete(t, h, latchDeleteBody)
	require.Equal(t, 503, code, "%+v", out)
	require.Contains(t, out.Error, "a WAL recovery pass left files to replay")
}
