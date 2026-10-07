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
	load := func(path string) (*ast.FuncDecl, string) {
		t.Helper()

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "tryArcxRouterArrow" {
				continue
			}

			var signature bytes.Buffer
			if err := format.Node(&signature, fset, fn.Type); err != nil {
				t.Fatalf("format signature in %s: %v", path, err)
			}
			return fn, signature.String()
		}

		t.Fatalf("tryArcxRouterArrow missing from %s", path)
		return nil, ""
	}

	real, realSignature := load("arcx_hook.go")
	_, stubSignature := load("arcx_hook_stub.go")

	if realSignature != stubSignature {
		t.Fatalf(
			"native/stub signatures differ:\nnative: %s\nstub: %s",
			realSignature, stubSignature,
		)
	}

	calls := make(map[string]bool)
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
		return true
	})

	for _, required := range []string{
		"AddTrailer",
		"setBodyStreamWithTrailers",
		"poisonArrowStream",
		"onComplete",
		"onFail",
	} {
		if !calls[required] {
			t.Errorf("native arcx Arrow hook missing call: %s", required)
		}
	}
}
