package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// arcx_hook.go is tagged arcx_engine, which default CI never compiles, so the
// guard is pinned at source level: both arcx hooks return before building the
// router's dependencies (which read h.db) when the handler has no DuckDB (#1016).
func TestArcxHooksReturnWithoutDuckDB(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "arcx_hook.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	isNilDBCheck := func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok {
			return false
		}
		bin, ok := ifs.Cond.(*ast.BinaryExpr)
		if !ok || bin.Op != token.EQL {
			return false
		}
		sel, ok := bin.X.(*ast.SelectorExpr)
		nilIdent, isIdent := bin.Y.(*ast.Ident)
		if !ok || !isIdent || nilIdent.Name != "nil" || sel.Sel.Name != "db" {
			return false
		}
		if len(ifs.Body.List) != 1 {
			return false
		}
		ret, ok := ifs.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return false
		}
		f, ok := ret.Results[0].(*ast.Ident)
		return ok && f.Name == "false"
	}
	for _, name := range []string{"tryArcxRouter", "tryArcxRouterArrow"} {
		var fn *ast.FuncDecl
		for _, decl := range file.Decls {
			if d, ok := decl.(*ast.FuncDecl); ok && d.Name.Name == name {
				fn = d
			}
		}
		if fn == nil {
			t.Fatalf("%s not found", name)
		}
		guard, deps := -1, -1
		for i, stmt := range fn.Body.List {
			if guard < 0 && isNilDBCheck(stmt) {
				guard = i
			}
			if as, ok := stmt.(*ast.AssignStmt); ok && deps < 0 {
				if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name == "deps" {
					deps = i
				}
			}
		}
		if guard < 0 || deps < 0 || guard > deps {
			t.Errorf("%s: no `if h.db == nil { return false }` before deps is built (guard %d, deps %d)", name, guard, deps)
			continue
		}
		// Nothing before the guard may touch h.db (a method on a nil engine).
		for _, stmt := range fn.Body.List[:guard] {
			ast.Inspect(stmt, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "db" {
					t.Errorf("%s: h.db is used before the nil guard", name)
				}
				return true
			})
		}
	}
}
