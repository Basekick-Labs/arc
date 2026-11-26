package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// #1938 (W3): the periodic WAL recovery pass must read the failure generation
// it later hands to ResetFlushFailure BEFORE it rotates the WAL. A flush that
// fails after the rotation holds rows in the new active file, which the pass
// skips; a generation read after that failure already counts it, the reset
// succeeds, and the latch clears over rows that exist only in the WAL. A clean
// shutdown then purges every WAL file, and those rows with it.
//
// The pass is inline in main's maintenance goroutine, so this pins the order in
// the source: the read is an unconditional statement of the same block as the
// rotation's `if walWriter != nil`, before it (source order is execution order
// only for direct statements of one block), and the identifier the reset gets
// is that read's, resolved by object, not by name.
func TestWALRecoveryPassReadsTheGenerationBeforeTheRotation1938(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	pass := w3PeriodicRecoveryPass(file)
	if pass == nil {
		t.Fatal("periodic WAL recovery pass not found: no function literal calls both RotateContext and RecoverWithOptions with MinFileAge; if the pass moved, point this test at it")
	}

	rotation := w3EnclosingRotationIf(pass)
	if rotation == nil {
		t.Fatal("the pass has no `if walWriter != nil { … RotateContext … }` statement")
	}
	block := w3ParentBlock(pass, rotation)
	if block == nil {
		t.Fatal("the rotation's if statement is not a direct statement of a block")
	}

	var reads []*ast.CallExpr
	ast.Inspect(pass.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && w3Calls(call, "FlushFailureGeneration") {
			reads = append(reads, call)
		}
		return true
	})
	if len(reads) != 1 {
		t.Fatalf("the pass calls FlushFailureGeneration %d times, want exactly 1", len(reads))
	}

	readIdx, rotationIdx, barrierIdx := -1, -1, -1
	var generation *ast.Ident
	for i, stmt := range block.List {
		if stmt == ast.Stmt(rotation) {
			rotationIdx = i
		}
		if w3ContainsCall(stmt, "FlushAllAndWait") {
			barrierIdx = i
		}
		as, ok := stmt.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			continue
		}
		if call, ok := as.Rhs[0].(*ast.CallExpr); ok && call == reads[0] {
			id, ok := as.Lhs[0].(*ast.Ident)
			if !ok || id.Obj == nil {
				t.Fatalf("%s: the generation read does not assign a local identifier", fset.Position(as.Pos()))
			}
			readIdx, generation = i, id
		}
	}
	if readIdx < 0 {
		t.Fatalf("%s: the generation read is not a direct, unconditional statement of the block holding the rotation (%s)",
			fset.Position(reads[0].Pos()), fset.Position(rotation.Pos()))
	}
	if readIdx > rotationIdx {
		t.Fatalf("%s: the generation is read after the rotation (%s); a flush failing in between is absorbed by the reset (#1938)",
			fset.Position(reads[0].Pos()), fset.Position(rotation.Pos()))
	}
	if barrierIdx < 0 {
		t.Fatal("the pre-barrier FlushAllAndWait is not a direct statement of the rotation's block")
	}
	if readIdx > barrierIdx {
		t.Fatalf("%s: the generation is read after the pre-barrier", fset.Position(reads[0].Pos()))
	}

	var resets []*ast.CallExpr
	ast.Inspect(pass.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && w3Calls(call, "ResetFlushFailure") {
			resets = append(resets, call)
		}
		return true
	})
	if len(resets) != 1 {
		t.Fatalf("the pass calls ResetFlushFailure %d times, want exactly 1", len(resets))
	}
	arg, ok := resets[0].Args[0].(*ast.Ident)
	if !ok || arg.Obj == nil || arg.Obj != generation.Obj {
		t.Fatalf("%s: ResetFlushFailure is not given the generation read at %s",
			fset.Position(resets[0].Pos()), fset.Position(generation.Pos()))
	}
}

// w3PeriodicRecoveryPass returns the innermost function literal that rotates the
// WAL and runs a recovery with MinFileAge (startup recovery has none).
func w3PeriodicRecoveryPass(file *ast.File) *ast.FuncLit {
	var found *ast.FuncLit
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.FuncLit)
		if !ok {
			return true
		}
		if w3ContainsCall(lit.Body, "RotateContext") && w3ContainsRecoveryWithMinFileAge(lit.Body) {
			found = lit // Inspect is pre-order: a later match is nested in this one.
		}
		return true
	})
	return found
}

func w3ContainsRecoveryWithMinFileAge(n ast.Node) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !w3Calls(call, "RecoverWithOptions") {
			return !found
		}
		for _, a := range call.Args {
			ast.Inspect(a, func(n ast.Node) bool {
				if kv, ok := n.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "MinFileAge" {
						found = true
					}
				}
				return !found
			})
		}
		return !found
	})
	return found
}

// w3EnclosingRotationIf returns the `if walWriter != nil` statement whose body
// calls RotateContext.
func w3EnclosingRotationIf(pass *ast.FuncLit) *ast.IfStmt {
	var found *ast.IfStmt
	ast.Inspect(pass.Body, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || found != nil {
			return found == nil
		}
		be, ok := ifs.Cond.(*ast.BinaryExpr)
		if !ok || be.Op != token.NEQ {
			return true
		}
		x, okX := be.X.(*ast.Ident)
		y, okY := be.Y.(*ast.Ident)
		if okX && okY && x.Name == "walWriter" && y.Name == "nil" && w3ContainsCall(ifs.Body, "RotateContext") {
			found = ifs
		}
		return found == nil
	})
	return found
}

// w3ParentBlock returns the block that holds stmt as a direct statement.
func w3ParentBlock(root ast.Node, stmt ast.Stmt) *ast.BlockStmt {
	var found *ast.BlockStmt
	ast.Inspect(root, func(n ast.Node) bool {
		if b, ok := n.(*ast.BlockStmt); ok {
			for _, s := range b.List {
				if s == stmt {
					found = b
				}
			}
		}
		return found == nil
	})
	return found
}

func w3ContainsCall(n ast.Node, name string) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && w3Calls(call, name) {
			found = true
		}
		return !found
	})
	return found
}

func w3Calls(call *ast.CallExpr, name string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == name
}
