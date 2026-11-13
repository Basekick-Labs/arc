package arcxrouter

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"testing"
)

// The real engine remains maintainer-only. This source contract verifies that
// its SQL builder uses the same file-selection helper the public tests exercise.
func TestBuildEngineSQL_UsesTestedFileSelection(t *testing.T) {
	file, err := parser.ParseFile(gotoken.NewFileSet(), "router.go", nil, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var call *ast.CallExpr
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "buildEngineSQL" {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			candidate, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := candidate.Fun.(*ast.Ident)
			if ok && name.Name == "engineQueryEntries" {
				call = candidate
			}
			return true
		})
	}
	if call == nil || len(call.Args) != 9 {
		t.Fatal("buildEngineSQL does not call the tested selector with pruning and exclusions")
	}
	for index, name := range map[int]string{7: "Pruner", 8: "Exclude"} {
		arg, ok := call.Args[index].(*ast.SelectorExpr)
		if !ok || arg.Sel.Name != name {
			t.Fatalf("file selector argument %d must be h.%s", index, name)
		}
		receiver, ok := arg.X.(*ast.Ident)
		if !ok || receiver.Name != "h" {
			t.Fatalf("file selector argument %d must use the handler's %s", index, name)
		}
	}
}
