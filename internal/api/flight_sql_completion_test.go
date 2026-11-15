//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func completionTestRecord(t *testing.T) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{0, 1}, nil)
	record := builder.NewRecordBatch()
	t.Cleanup(record.Release)
	return record
}

func completionTestClient(t *testing.T, handler http.Handler) *flightsql.Client {
	t.Helper()
	bridge := httptest.NewServer(handler)
	t.Cleanup(bridge.Close)
	service := &flightSQLService{baseURL: bridge.URL, client: bridge.Client()}
	server := flight.NewServerWithMiddleware(nil)
	require.NoError(t, server.Init("127.0.0.1:0"))
	server.RegisterFlightService(flightsql.NewFlightServer(service))
	done := make(chan error, 1)
	go func() { done <- server.Serve() }()
	t.Cleanup(func() { server.Shutdown(); require.NoError(t, <-done) })
	client, err := flightsql.NewClient(server.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Client.Close()) })
	return client
}

// Run through the actual Flight SQL gRPC adapter, not just its internal chunk
// channel, so a normal client's Err() must see a capped result as incomplete.
func TestFlightSQLCompletionSignals(t *testing.T) {
	for _, tc := range []struct {
		name, cap, truncated     string
		extra, broken, readError bool
		want                     codes.Code
	}{
		{name: "uncapped", want: codes.OK},
		{name: "capped", cap: "2", want: codes.ResourceExhausted},
		{name: "invalid cap", cap: "secret-invalid-value", want: codes.Internal},
		{name: "zero cap", cap: "0", want: codes.Internal},
		{name: "truncation overrides cap", cap: "2", truncated: "secret engine path", want: codes.Internal},
		{name: "unexpected body suffix", extra: true, want: codes.Internal},
		{name: "HTTP read error after IPC", readError: true, want: codes.Internal},
		{name: "broken IPC overrides cap", cap: "2", broken: true, want: codes.Unknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := completionTestRecord(t)
			client := completionTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
				w.Header().Add("Trailer", arrowRowsCappedTrailer)
				w.Header().Add("Trailer", arrowStreamTruncatedTrailer)
				var payload bytes.Buffer
				writer := ipc.NewWriter(&payload, ipc.WithSchema(record.Schema()))
				if err := writer.Write(record); err != nil {
					t.Error(err)
					return
				}
				if tc.broken {
					// Start an IPC continuation message but omit its declared metadata.
					_, _ = payload.Write([]byte{255, 255, 255, 255, 127, 0, 0, 0})
				} else if err := writer.Close(); err != nil {
					t.Error(err)
					return
				}
				if tc.extra {
					_, _ = payload.Write([]byte("x"))
				}
				if tc.readError {
					w.Header().Set("Content-Length", strconv.Itoa(payload.Len()+1))
				}
				_, _ = w.Write(payload.Bytes())
				w.Header().Set(arrowRowsCappedTrailer, tc.cap)
				w.Header().Set(arrowStreamTruncatedTrailer, tc.truncated)
			}))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			info, err := client.Execute(ctx, "SELECT * FROM range(10)")
			require.NoError(t, err)
			reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
			require.NoError(t, err)
			defer reader.Release()
			var rows int64
			for reader.Next() {
				rows += reader.RecordBatch().NumRows()
			}
			require.EqualValues(t, 2, rows)
			require.Equal(t, tc.want, status.Code(reader.Err()), "stream error: %v", reader.Err())
			if tc.want != codes.OK {
				require.NotContains(t, reader.Err().Error(), "secret")
			}
		})
	}
}

func TestFlightSQLCancellationClosesHTTPBody(t *testing.T) {
	record := completionTestRecord(t)
	disconnected := make(chan struct{})
	client := completionTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		writer := ipc.NewWriter(w, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			t.Error(err)
			return
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(disconnected)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := client.Execute(ctx, "SELECT * FROM range(10)")
	require.NoError(t, err)
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next())
	cancel()
	require.False(t, reader.Next())
	require.Equal(t, codes.Canceled, status.Code(reader.Err()))
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Flight cancellation did not cancel the HTTP bridge request")
	}
}

func TestFlightSQLTrailerDrainIsBounded(t *testing.T) {
	record := completionTestRecord(t)
	disconnected := make(chan struct{})
	client := completionTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		w.Header().Set("Trailer", arrowRowsCappedTrailer)
		writer := ipc.NewWriter(w, ipc.WithSchema(record.Schema()))
		if err := writer.Write(record); err != nil {
			t.Error(err)
			return
		}
		if err := writer.Close(); err != nil {
			t.Error(err)
			return
		}
		w.(http.Flusher).Flush()
		// Valid IPC is complete, but the HTTP trailer framing never completes.
		<-r.Context().Done()
		close(disconnected)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	info, err := client.Execute(ctx, "SELECT * FROM range(10)")
	require.NoError(t, err)
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	require.EqualValues(t, 2, rows)
	require.Equal(t, codes.DeadlineExceeded, status.Code(reader.Err()))
	require.NoError(t, ctx.Err(), "bridge completion deadline, not outer test deadline, must terminate the stream")
	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("trailer deadline did not close HTTP body")
	}
}
