//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

// This source-level contract is necessary because the private arcx native
// library is not available in ordinary builds. It does not replace an
// arcx_engine integration test.
func TestArcxArrowPanicWiring(t *testing.T) {
	file, err := parser.ParseFile(
		token.NewFileSet(), "arcx_hook.go", nil, parser.AllErrors,
	)
	if err != nil {
		t.Fatalf("parse arcx_hook.go: %v", err)
	}

	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "tryArcxRouterArrow" {
			target = fn
			break
		}
	}
	if target == nil {
		t.Fatal("tryArcxRouterArrow not found")
	}

	var streamCall *ast.CallExpr
	ast.Inspect(target.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if ok && sel.Sel.Name == "setBodyStreamWithTrailers" {
			streamCall = call
		}
		return true
	})

	if streamCall == nil {
		t.Fatal("arcx Arrow path bypasses the panic-safe trailer stream")
	}
	if len(streamCall.Args) != 5 {
		t.Fatalf("stream wrapper has %d arguments, want 5", len(streamCall.Args))
	}

	onPanic, ok := streamCall.Args[3].(*ast.FuncLit)
	if !ok {
		t.Fatal("arcx Arrow stream has no panic callback")
	}
	writer, ok := streamCall.Args[4].(*ast.FuncLit)
	if !ok {
		t.Fatal("arcx Arrow stream has no writer callback")
	}

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

	if !hasCall(target.Body, "AddTrailer") {
		t.Error("arcx Arrow path does not register a trailer")
	}
	if !hasCall(onPanic.Body, "poisonArrowStream") {
		t.Error("panic callback does not poison the Arrow body")
	}
	if !hasCall(onPanic.Body, "setIfAbsent") {
		t.Error("panic callback does not record the truncation reason")
	}

	capturesWriter := false
	ast.Inspect(writer.Body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, lhs := range assign.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok || id.Name != "streamW" || i >= len(assign.Rhs) {
				continue
			}
			rhs, ok := assign.Rhs[i].(*ast.Ident)
			capturesWriter = capturesWriter || ok && rhs.Name == "w"
		}
		return true
	})
	if !capturesWriter {
		t.Error("stream writer is not captured for panic poisoning")
	}
}

// Exercise the real shared stream/trailer implementation without the private
// arcx engine. A simulated panic must produce an undecodable IPC body and a
// non-empty Arc-Stream-Truncated trailer.
func TestArcxArrowPanicMarkerAndTrailer(t *testing.T) {
	metrics.Init(zerolog.Nop())

	var valid bytes.Buffer
	ipcWriter := ipc.NewWriter(
		&valid,
		ipc.WithSchema(arrow.NewSchema(nil, nil)),
	)
	if err := ipcWriter.Close(); err != nil {
		t.Fatalf("construct IPC stream: %v", err)
	}

	endMarker := []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}
	data := valid.Bytes()
	if !bytes.HasSuffix(data, endMarker) {
		t.Fatalf("IPC stream does not end with expected marker: %x", data)
	}
	partial := bytes.Clone(data[:len(data)-len(endMarker)])

	var ctx fasthttp.RequestCtx
	ctx.Response.Header.SetStatusCode(fasthttp.StatusOK)
	if err := ctx.Response.Header.AddTrailer(arrowStreamTruncatedTrailer); err != nil {
		t.Fatalf("register trailer: %v", err)
	}

	h := &QueryHandler{logger: zerolog.Nop()}
	trailers := newResponseTrailers()
	var streamW *bufio.Writer

	h.setBodyStreamWithTrailers(
		&ctx,
		"arcx_serve_arrow_ipc",
		trailers,
		func() {
			poisonArrowStream(streamW, h.logger)
			trailers.setIfAbsent(
				arrowStreamTruncatedTrailer, "stream writer panicked",
			)
		},
		func(w *bufio.Writer) {
			streamW = w
			if _, err := w.Write(partial); err != nil {
				t.Errorf("write IPC prefix: %v", err)
				return
			}
			panic("simulated arcx stream failure")
		},
	)

	var wire bytes.Buffer
	bw := bufio.NewWriter(&wire)
	if err := ctx.Response.Write(bw); err != nil {
		t.Fatalf("write HTTP response: %v", err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush HTTP response: %v", err)
	}

	resp, err := http.ReadResponse(
		bufio.NewReader(bytes.NewReader(wire.Bytes())), nil,
	)
	if err != nil {
		t.Fatalf("parse HTTP response: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read HTTP body: %v", err)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got == "" {
		t.Fatal("panic response has no truncation trailer")
	}

	reader, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		return // The poisoned body was rejected during reader construction.
	}
	defer reader.Release()

	for reader.Next() {
	}
	if reader.Err() == nil {
		t.Fatal("Arrow decoder accepted a panicked stream as complete")
	}
}
