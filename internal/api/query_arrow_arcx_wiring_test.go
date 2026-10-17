//go:build duckdb_arrow

package api

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
)

// The native arcx dependency is not present in ordinary CI builds.
// Verify that its hook and the compiled stub keep identical signatures,
// and that the native source retains its essential lifecycle wiring.
func TestArcxArrowSourceWiring(t *testing.T) {
	load := func(path, name string) (*ast.FuncDecl, string) {
		t.Helper()

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != name {
				continue
			}

			var signature bytes.Buffer
			if err := format.Node(&signature, fset, fn.Type); err != nil {
				t.Fatalf("format signature in %s: %v", path, err)
			}
			return fn, signature.String()
		}

		t.Fatalf("%s missing from %s", name, path)
		return nil, ""
	}

	real, realSignature := load("arcx_hook.go", "tryArcxRouterArrow")
	_, stubSignature := load("arcx_hook_stub.go", "tryArcxRouterArrow")

	if realSignature != stubSignature {
		t.Fatalf(
			"native/stub signatures differ:\nnative: %s\nstub: %s",
			realSignature, stubSignature,
		)
	}

	calls := make(map[string]bool)
	args := make(map[string]bool)
	ast.Inspect(real.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		switch fn := call.Fun.(type) {
		case *ast.Ident:
			calls[fn.Name] = true
		case *ast.SelectorExpr:
			calls[fn.Sel.Name] = true
		}
		for _, arg := range call.Args {
			if id, ok := arg.(*ast.Ident); ok {
				args[id.Name] = true
			}
		}
		return true
	})

	if !calls["serveArcxArrowIPCResult"] {
		t.Fatal("native arcx Arrow hook does not use the tested IPC response helper")
	}
	if !args["governanceMaxRows"] {
		t.Fatal("native arcx Arrow hook does not pass the governance row cap")
	}
	if !args["onRows"] || !args["onDisconnect"] {
		t.Fatal("native arcx Arrow hook does not pass row and disconnect callbacks")
	}

	helper, _ := load("query_arrow.go", "serveArcxArrowIPCResult")
	helperCalls := make(map[string]bool)
	helperArgs := make(map[string]bool)
	ast.Inspect(helper.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			helperCalls[fn.Name] = true
		case *ast.SelectorExpr:
			helperCalls[fn.Sel.Name] = true
		}
		for _, arg := range call.Args {
			if id, ok := arg.(*ast.Ident); ok {
				helperArgs[id.Name] = true
			}
		}
		return true
	})
	for _, required := range []string{
		"registerArrowResponseTrailers",
		"setBodyStreamWithTrailers",
		"streamArrowIPCFunc",
		"rowCapReached",
		"poisonArrowStream",
	} {
		if !helperCalls[required] {
			t.Errorf("ArcX Arrow response helper missing call: %s", required)
		}
	}
	if !helperArgs["governanceMaxRows"] {
		t.Error("ArcX Arrow response helper does not pass the governance row cap to the IPC streamer")
	}

	queryArrow, _ := load("query_arrow.go", "executeQueryArrow")
	var dispatchHasCap, dispatchHasCallbacks bool
	ast.Inspect(queryArrow.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) <= 9 {
			return true
		}
		fn, ok := call.Fun.(*ast.Ident)
		if !ok || fn.Name != "arcxArrowDispatch" {
			return true
		}
		capArg, ok := call.Args[7].(*ast.Ident)
		dispatchHasCap = ok && capArg.Name == "governanceMaxRows"
		rowsArg, rowsOK := call.Args[8].(*ast.Ident)
		disconnectArg, disconnectOK := call.Args[9].(*ast.Ident)
		dispatchHasCallbacks = rowsOK && rowsArg.Name == "arcxOnRows" &&
			disconnectOK && disconnectArg.Name == "recordDisconnect"
		return false
	})
	if !dispatchHasCap {
		t.Error("Arrow query handler does not pass its governance cap to the ArcX hook")
	}
	if !dispatchHasCallbacks {
		t.Error("Arrow query handler does not pass row and disconnect callbacks to the ArcX hook")
	}
}
