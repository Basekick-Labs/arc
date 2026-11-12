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

	if !calls["checkEmittedQueryPermissions"] {
		t.Error("native arcx Arrow hook does not recheck the resolved scope")
	}
	if !calls["serveArcxArrowIPCResult"] {
		t.Fatal("native arcx Arrow hook does not use the tested IPC response helper")
	}
	if !args["governanceMaxRows"] {
		t.Fatal("native arcx Arrow hook does not pass the governance row cap")
	}
	if !args["onRows"] || !args["onDisconnect"] || !args["onComplete"] || !args["onFail"] {
		t.Fatal("native arcx Arrow hook does not pass row, disconnect, completion and failure callbacks")
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
		"setDirectBodyStream",
		"streamArrowIPCFunc",
		"rowCapReached",
		"poisonArrowStream",
		"onComplete",
		"onFail",
	} {
		if !helperCalls[required] {
			t.Errorf("ArcX Arrow response helper missing call: %s", required)
		}
	}
	if !helperArgs["governanceMaxRows"] {
		t.Error("ArcX Arrow response helper does not pass the governance row cap to the IPC streamer")
	}

	queryArrow, _ := load("query_arrow.go", "executeQueryArrow")
	var dispatchHasCap, dispatchHasCallbacks, dispatchHasStart bool
	ast.Inspect(queryArrow.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) <= 10 {
			return true
		}
		fn, ok := call.Fun.(*ast.Ident)
		if !ok || fn.Name != "arcxArrowDispatch" {
			return true
		}
		startArg, ok := call.Args[4].(*ast.Ident)
		dispatchHasStart = ok && startArg.Name == "start"
		capArg, ok := call.Args[8].(*ast.Ident)
		dispatchHasCap = ok && capArg.Name == "governanceMaxRows"
		rowsArg, rowsOK := call.Args[9].(*ast.Ident)
		disconnectArg, disconnectOK := call.Args[10].(*ast.Ident)
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
	if !dispatchHasStart {
		t.Error("Arrow query handler does not pass its start time to the ArcX hook")
	}

	// The hook times the query from the handler's start (as the DuckDB path
	// does), so it must not take a start of its own, and it must answer with
	// the helper's verdict (the helper declines a context that already ended).
	ast.Inspect(real.Body, func(node ast.Node) bool {
		if assign, ok := node.(*ast.AssignStmt); ok {
			for _, lhs := range assign.Lhs {
				if id, ok := lhs.(*ast.Ident); ok && id.Name == "start" {
					t.Error("native arcx Arrow hook assigns its own start; Arc-Execution-Time-Ms would not match DuckDB's origin")
				}
			}
		}
		return true
	})
	returnsHelper := false
	ast.Inspect(real.Body, func(node ast.Node) bool {
		ret, ok := node.(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return true
		}
		if call, ok := ret.Results[0].(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "serveArcxArrowIPCResult" {
				returnsHelper = true
				if len(call.Args) < 6 || !isIdent(call.Args[5], "start") {
					t.Error("native arcx Arrow hook does not hand the handler's start to the helper")
				}
			}
		}
		return true
	})
	if !returnsHelper {
		t.Error("native arcx Arrow hook does not return the helper's served/declined result")
	}

	// FFI lifetime: every arcx stream writer releases its reader and keeps it
	// reachable to the very end. The deferred block's LAST statement must be
	// runtime.KeepAlive(reader), after releaseArrowStreamResourcesFunc.
	checkWriterDefers := func(where string, fn *ast.FuncDecl) {
		t.Helper()
		writers := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "setDirectBodyStream" || len(call.Args) != 6 {
				return true
			}
			writer, ok := call.Args[5].(*ast.FuncLit)
			if !ok {
				t.Errorf("%s: a stream writer is not a function literal", where)
				return true
			}
			writers++
			var deferred *ast.FuncLit
			for _, stmt := range writer.Body.List {
				if d, ok := stmt.(*ast.DeferStmt); ok {
					if lit, ok := d.Call.Fun.(*ast.FuncLit); ok {
						deferred = lit
						break
					}
				}
			}
			if deferred == nil || len(deferred.Body.List) == 0 {
				t.Errorf("%s: stream writer has no deferred cleanup block", where)
				return true
			}
			stmts := deferred.Body.List
			isCall := func(stmt ast.Stmt, pkg, name string) bool {
				es, ok := stmt.(*ast.ExprStmt)
				if !ok {
					return false
				}
				c, ok := es.X.(*ast.CallExpr)
				if !ok {
					return false
				}
				switch f := c.Fun.(type) {
				case *ast.SelectorExpr:
					x, ok := f.X.(*ast.Ident)
					return ok && x.Name == pkg && f.Sel.Name == name &&
						len(c.Args) == 1 && isIdent(c.Args[0], "reader")
				case *ast.Ident:
					return pkg == "" && f.Name == name
				}
				return false
			}
			if !isCall(stmts[len(stmts)-1], "runtime", "KeepAlive") {
				t.Errorf("%s: runtime.KeepAlive(reader) is not the last statement of the writer's deferred block", where)
			}
			released := false
			for _, stmt := range stmts[:len(stmts)-1] {
				if es, ok := stmt.(*ast.ExprStmt); ok {
					if c, ok := es.X.(*ast.CallExpr); ok {
						if id, ok := c.Fun.(*ast.Ident); ok && id.Name == "releaseArrowStreamResourcesFunc" &&
							len(c.Args) > 0 && isIdent(c.Args[0], "reader") {
							released = true
						}
					}
				}
			}
			if !released {
				t.Errorf("%s: the writer's deferred block does not release the reader before KeepAlive", where)
			}
			return true
		})
		if writers == 0 {
			t.Errorf("%s: no stream writer found", where)
		}
	}
	checkWriterDefers("serveArcxArrowIPCResult", helper)
	serveResult, _ := load("arcx_hook.go", "serveArcxResult")
	checkWriterDefers("serveArcxResult", serveResult)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "arcx_hook.go", nil, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse arcx_hook.go: %v", err)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "tryArcxRouter" {
			continue
		}
		found := false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "checkEmittedQueryPermissions" {
				found = true
			}
			return true
		})
		if !found {
			t.Error("native arcx query hook does not recheck the resolved scope")
		}
		return
	}
	t.Error("tryArcxRouter missing from arcx_hook.go")
}

func isIdent(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}
