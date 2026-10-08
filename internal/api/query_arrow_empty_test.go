//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/rs/zerolog"
)

func TestExecuteQueryArrow_EmptyResultReleasesContext(t *testing.T) {
	for _, query := range []string{"SELECT 1 AS id", "SELECT * FROM default.empty_measurement"} {
		t.Run(query, func(t *testing.T) {
			app, _ := newArrowRegistryRig(t, time.Hour, 0)
			oldStream, oldRelease := streamArrowIPCFunc, releaseArrowStreamResourcesFunc
			defer func() { streamArrowIPCFunc, releaseArrowStreamResourcesFunc = oldStream, oldRelease }()
			var streamCtx context.Context
			streamArrowIPCFunc = func(ctx context.Context, w *bufio.Writer, reader array.RecordReader, schema *arrow.Schema, casts *decimalCastInfo, dict bool, compression string, maxRows int, log zerolog.Logger) (int64, error) {
				streamCtx = ctx
				return oldStream(ctx, w, reader, schema, casts, dict, compression, maxRows, log)
			}
			var cancelCalled atomic.Bool
			var cleanupCtxErr error
			var logs bytes.Buffer
			cleaned := make(chan struct{})
			releaseArrowStreamResourcesFunc = func(reader array.RecordReader, conn interface{ Close() error }, cancel context.CancelFunc, log zerolog.Logger) {
				defer close(cleaned)
				// Release the test timer even when testing the broken cleanup.
				defer func() {
					if cancel != nil {
						cancel()
					}
				}()
				oldRelease(reader, conn, func() {
					cancelCalled.Store(true)
					if cancel != nil {
						cancel()
					}
				}, zerolog.New(&logs))
				cleanupCtxErr = streamCtx.Err()
			}
			req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"`+query+`"}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req, 10000)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status %d", resp.StatusCode)
			}
			select {
			case <-cleaned:
			case <-time.After(5 * time.Second):
				t.Fatal("stream cleanup did not run")
			}
			if !cancelCalled.Load() {
				t.Errorf("cleanup skipped cancel; ctx.Err=%v; cleanup log=%s", cleanupCtxErr, logs.String())
			}
			if cleanupCtxErr == nil {
				t.Error("completed query context remains live with its one-hour timer")
			}
			if strings.Contains(logs.String(), "cleanup panicked") {
				t.Error("normal empty success logged cleanup panic")
			}
		})
	}
}

func TestExecuteQueryArrow_MissingAnchorStillFailsAndRecovers(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	seedCustomerScenario(e)
	reg := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 16}, zerolog.Nop())
	e.h.SetQueryRegistry(reg)
	sql := rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), "")
	request := func() (int, []byte) {
		payload, _ := json.Marshal(map[string]string{"sql": sql})
		req := httptest.NewRequest("POST", "/api/v1/query/arrow", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := e.app.Test(req, 10000)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	if status, body := request(); status != 200 {
		t.Fatalf("warmup: %d %s", status, body)
	}
	path, ok := e.reg.Resolve(context.Background(), "sch", "multiday")
	if !ok {
		t.Fatal("no anchor")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if status, body := request(); status != 500 {
		t.Fatalf("missing anchor: %d %s", status, body)
	}
	status, body := request()
	if status != 200 {
		t.Fatalf("retry: %d %s", status, body)
	}
	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var rows int64
	for reader.Next() {
		rows += reader.Record().NumRows()
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("retry rows=%d", rows)
	}
	if reg.ActiveCount() != 0 {
		t.Fatal("active query leaked")
	}
	history := reg.GetHistory(10)
	if len(history) != 3 || history[1].Status != queryregistry.StatusFailed {
		t.Fatalf("history=%+v", history)
	}
}

func TestExecuteQueryArrow_EmptyIPCEncodingsAndMetrics(t *testing.T) {
	for _, compression := range []string{"", "zstd", "lz4"} {
		t.Run(compression, func(t *testing.T) {
			app, reg := newArrowRegistryRig(t, 30*time.Second, 0)
			beforeSuccess := queryMetricInt(t, "query_success_total")
			beforeErrors := queryMetricInt(t, "query_errors_total")
			req := httptest.NewRequest("POST", "/api/v1/query/arrow", strings.NewReader(`{"sql":"SELECT * FROM default.empty_measurement"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-arc-arrow-compression", compression)
			req.Header.Set("x-arc-arrow-dictionary", "true")
			resp, err := app.Test(req, 10000)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d body=%s", resp.StatusCode, body)
			}
			reader, err := ipc.NewReader(bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Release()
			if reader.Schema().NumFields() != 0 || reader.Next() || reader.Err() != nil {
				t.Fatal("invalid empty stream")
			}
			if v := resp.Trailer.Get(arrowStreamTruncatedTrailer); v != "" {
				t.Fatalf("unexpected truncation: %s", v)
			}
			if len(reg.GetHistory(10)) != 1 || reg.ActiveCount() != 0 {
				t.Fatal("registry mismatch")
			}
			if got := queryMetricInt(t, "query_success_total"); got != beforeSuccess+1 {
				t.Fatalf("success metric %d != %d", got, beforeSuccess+1)
			}
			if got := queryMetricInt(t, "query_errors_total"); got != beforeErrors {
				t.Fatalf("error metric %d != %d", got, beforeErrors)
			}
		})
	}
}
