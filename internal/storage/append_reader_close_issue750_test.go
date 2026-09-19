package storage

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Pin the precise bug: an explicit Close plus a deferred Close causes
// the same descriptor to be closed twice on successful promotion.
func TestAppendReaderSingleCloseIssue750(t *testing.T) {
	parsed, err := parser.ParseFile(
		token.NewFileSet(), "local.go", nil, 0,
	)
	if err != nil {
		t.Fatal(err)
	}

	var method *ast.FuncDecl
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv != nil && fn.Name.Name == "AppendReader" {
			method = fn
			break
		}
	}
	if method == nil {
		t.Fatal("LocalBackend.AppendReader not found")
	}

	closes := 0
	var closePosition token.Pos
	var renamePosition token.Pos

	ast.Inspect(method.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}

		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if !ok {
			return true
		}

		if receiver.Name == "file" && selector.Sel.Name == "Close" {
			closes++
			closePosition = call.Pos()
		}
		if receiver.Name == "os" && selector.Sel.Name == "Rename" {
			renamePosition = call.Pos()
		}
		return true
	})

	if closes != 1 {
		t.Fatalf(
			"AppendReader calls file.Close %d times; want exactly once",
			closes,
		)
	}
	if !renamePosition.IsValid() || closePosition >= renamePosition {
		t.Fatal("staging file must close before promotion")
	}
}

func prepareAppendReaderIssue750(
	t *testing.T,
) (*LocalBackend, string, string, string) {
	t.Helper()

	backend, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	key := "db/cpu/example.parquet"
	finalPath := filepath.Join(
		backend.GetBasePath(), filepath.FromSlash(key),
	)
	stagingPath := finalPath + PartSuffix

	if err := os.MkdirAll(filepath.Dir(stagingPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stagingPath, []byte("prefix"), 0600); err != nil {
		t.Fatal(err)
	}

	return backend, key, stagingPath, finalPath
}

func TestAppendReaderPartialAndPromotionIssue750(t *testing.T) {
	backend, key, staging, final := prepareAppendReaderIssue750(t)
	ctx := context.Background()

	// A partial append must close the handle but retain the staging file.
	if err := backend.AppendReader(
		ctx, key, strings.NewReader("-middle"), 8,
	); err != nil {
		t.Fatal(err)
	}

	staged, err := os.ReadFile(staging)
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != "prefix-middle" {
		t.Fatalf("staging contents = %q", staged)
	}
	if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial append unexpectedly promoted file: %v", err)
	}

	// A complete append must close before promoting the staging file.
	if err := backend.AppendReader(
		ctx, key, strings.NewReader("-end"), 4,
	); err != nil {
		t.Fatal(err)
	}

	contents, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "prefix-middle-end" {
		t.Fatalf("final contents = %q", contents)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging file remains after promotion: %v", err)
	}
}

type appendIssue750FailReader struct {
	sent bool
	err  error
}

func (r *appendIssue750FailReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, r.err
	}
	r.sent = true
	return copy(p, "-partial"), nil
}

func TestAppendReaderCopyErrorLeavesStagingIssue750(t *testing.T) {
	backend, key, staging, final := prepareAppendReaderIssue750(t)
	sentinel := errors.New("simulated interrupted download")

	err := backend.AppendReader(
		context.Background(),
		key,
		&appendIssue750FailReader{err: sentinel},
		int64(len("-partial")),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("AppendReader error = %v; want wrapped read error", err)
	}

	staged, err := os.ReadFile(staging)
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != "prefix-partial" {
		t.Fatalf("staging contents = %q", staged)
	}
	if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed append unexpectedly promoted file: %v", err)
	}
}
