package sql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The guard: no log field carrying SQL-shaped content may be emitted without
// passing through ForLog (or SanitizeErrText for error strings). This is what
// keeps "site #34" from regressing after the sweep — a grep-shaped review
// missed `where`, `engine_sql`, `raw_json` and `diff` in this very tree, so the
// rule is AST-based and two-sided:
//
//	(a) key denylist  — Str("sql"|"query"|"where"|…, …)
//	(b) value taint   — any identifier in the value ending in sql/query/where/
//	                    clause (case-insensitive), whatever the key is called
//
// Exemptions: identifiers ending in ID/Id/Name (query_id, query_name), and an
// explicit allowlist with written justifications.
var guardKeyDenylist = map[string]bool{
	"sql": true, "query": true, "converted_sql": true, "engine_sql": true,
	"oracle_sql": true, "where": true, "statement": true, "stmt": true,
	"raw": true, "raw_json": true, "diff": true,
}

var guardValueTaint = regexp.MustCompile(`(?i)(sql|query|where|clause)$`)
var guardValueExempt = regexp.MustCompile(`(Id|ID|Name|Timeout|Count)$`)

// file:key → justification. Keep this SHORT; every entry is a reviewed decision.
var guardAllowlist = map[string]string{
	"internal/arcxrouter/router.go:diff": "value-free by construction — compare.go producers emit divergence class/position only, never values",
}

func TestNoUnmaskedSQLLogFields(t *testing.T) {
	root := filepath.Join("..", "..") // repo root from internal/sql
	var violations []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return nil // files behind build tags we can't satisfy still parse; real parse errors are CI's problem
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Str" {
				return true
			}
			keyLit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || keyLit.Kind != token.STRING {
				return true
			}
			key := strings.Trim(keyLit.Value, `"`)
			tainted := guardKeyDenylist[key]
			if !tainted {
				ast.Inspect(call.Args[1], func(vn ast.Node) bool {
					if id, ok := vn.(*ast.Ident); ok {
						if guardValueTaint.MatchString(id.Name) && !guardValueExempt.MatchString(id.Name) {
							tainted = true
						}
					}
					return true
				})
			}
			if !tainted {
				return true
			}
			if _, ok := guardAllowlist[rel+":"+key]; ok {
				return true
			}
			masked := false
			ast.Inspect(call.Args[1], func(vn ast.Node) bool {
				if id, ok := vn.(*ast.Ident); ok && (id.Name == "ForLog" || id.Name == "SanitizeErrText") {
					masked = true
				}
				return true
			})
			if !masked {
				pos := fset.Position(call.Pos())
				violations = append(violations,
					pos.Filename+":"+itoa(pos.Line)+" Str(\""+key+"\", …) without sqlutil.ForLog")
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range violations {
		t.Error(v)
	}
	if len(violations) > 0 {
		t.Fatalf("%d unmasked SQL log field(s); wrap with sqlutil.ForLog or add a justified allowlist entry", len(violations))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
