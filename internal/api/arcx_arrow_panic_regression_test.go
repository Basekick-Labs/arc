//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// This source-level contract is necessary because the private arcx native
// library is not available in ordinary builds. It does not replace an
// arcx_engine integration test.
func TestArcxArrowPanicWiring(t *testing.T) {
	parse := func(path, name string) *ast.FuncDecl {
		t.Helper()
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Name.Name == name {
				return fn
			}
		}
		t.Fatalf("%s not found in %s", name, path)
		return nil
	}

	hook := parse("arcx_hook.go", "tryArcxRouterArrow")
	helper := parse("query_arrow.go", "serveArcxArrowIPCResult")
	hasCall := func(root ast.Node, name string) bool {
		found := false
		ast.Inspect(root, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				found = found || fn.Name == name
			case *ast.SelectorExpr:
				found = found || fn.Sel.Name == name
			}
			return true
		})
		return found
	}
	if !hasCall(hook.Body, "serveArcxArrowIPCResult") {
		t.Fatal("ArcX Arrow hook bypasses the tested response helper")
	}
	if !hasCall(helper.Body, "registerArrowResponseTrailers") {
		t.Error("ArcX Arrow response helper does not register its response trailers")
	}
	if !hasCall(helper.Body, "streamArrowIPCFunc") {
		t.Error("ArcX Arrow response helper does not use the shared IPC streamer")
	}

	var streamCall *ast.CallExpr
	ast.Inspect(helper.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if ok && selector.Sel.Name == "setBodyStreamWithTrailers" {
			streamCall = call
		}
		return true
	})
	if streamCall == nil || len(streamCall.Args) != 5 {
		t.Fatal("ArcX Arrow response helper lacks the panic-safe trailer stream")
	}
	onPanic, ok := streamCall.Args[3].(*ast.FuncLit)
	if !ok {
		t.Fatal("ArcX Arrow stream has no panic callback")
	}
	if !hasCall(onPanic.Body, "poisonArrowStream") || !hasCall(onPanic.Body, "setIfAbsent") {
		t.Error("panic callback does not poison the body and record the truncation reason")
	}
}

// Exercise the real shared stream/trailer implementation without the private
// arcx engine. A simulated panic must produce an undecodable IPC body and a
// non-empty Arc-Stream-Truncated trailer.
func TestArcxArrowPanicMarkerAndTrailer(t *testing.T) {
	metrics.Init(zerolog.Nop())

	var valid bytes.Buffer
	ipcWriter := ipc.NewWriter(&valid, ipc.WithSchema(arrow.NewSchema(nil, nil)))
	if err := ipcWriter.Close(); err != nil {
		t.Fatalf("construct IPC stream: %v", err)
	}
	endMarker := []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}
	if !bytes.HasSuffix(valid.Bytes(), endMarker) {
		t.Fatalf("IPC stream does not end with expected marker: %x", valid.Bytes())
	}
	partial := bytes.Clone(valid.Bytes()[:len(valid.Bytes())-len(endMarker)])

	original := streamArrowIPCFunc
	t.Cleanup(func() { streamArrowIPCFunc = original })
	streamArrowIPCFunc = func(_ context.Context, w *bufio.Writer, _ array.RecordReader, _ *arrow.Schema,
		_ *decimalCastInfo, _ bool, _ string, _ int, _ zerolog.Logger) (int64, error) {
		if _, err := w.Write(partial); err != nil {
			t.Errorf("write IPC prefix: %v", err)
			return 0, err
		}
		if err := w.Flush(); err != nil {
			t.Errorf("flush IPC prefix: %v", err)
			return 0, err
		}
		panic("simulated arcx stream failure")
	}

	schema := arrow.NewSchema(nil, nil)
	reader := newSimpleRecordReader(schema, nil)
	defer reader.Release()
	var ctx fasthttp.RequestCtx
	ctx.Response.Header.SetStatusCode(fasthttp.StatusOK)
	h := &QueryHandler{logger: zerolog.Nop()}
	failed := false
	h.serveArcxArrowIPCResult(
		&ctx, context.Background(), nil, reader, 10, time.Now(), nil, nil, nil,
		func(string) { failed = true },
	)

	var wire bytes.Buffer
	bw := bufio.NewWriter(&wire)
	if err := ctx.Response.Write(bw); err != nil {
		t.Fatalf("write HTTP response: %v", err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush HTTP response: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(wire.Bytes())), nil)
	if err != nil {
		t.Fatalf("parse HTTP response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("close response body: %v", closeErr)
		}
		t.Fatalf("read HTTP body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close response body: %v", err)
	}
	if !failed {
		t.Fatal("panic callback did not fail the query")
	}
	for _, name := range []string{arrowStreamTruncatedTrailer, arrowExecutionTimeTrailer, arrowRowsCappedTrailer} {
		if _, ok := resp.Trailer[http.CanonicalHeaderKey(name)]; !ok {
			t.Errorf("%s is absent from the response trailers", name)
		}
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got == "" {
		t.Fatal("panic response has no truncation trailer")
	}
	if got := resp.Trailer.Get(arrowExecutionTimeTrailer); got == "" {
		t.Fatal("panic response has no execution-time trailer")
	}

	decoded, err := ipc.NewReader(bytes.NewReader(body))
	if err == nil {
		defer decoded.Release()
		for decoded.Next() {
		}
		err = decoded.Err()
	}
	if err == nil {
		t.Fatal("Arrow decoder accepted a panicked stream as complete")
	}
}
