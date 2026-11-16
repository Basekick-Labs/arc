package arcxrouter

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// erroringReader is an empty engine reader whose stream ended with err.
type erroringReader struct {
	array.RecordReader
	err error
}

func (r *erroringReader) Err() error { return r.err }

func newErroringReader(t *testing.T, err error) *erroringReader {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rr, rerr := array.NewRecordReader(schema, nil)
	if rerr != nil {
		t.Fatal(rerr)
	}
	return &erroringReader{RecordReader: rr, err: err}
}

// planDir lays out base/db/m/<name> for each present file and returns base.
func planDir(t *testing.T, present ...string) string {
	t.Helper()
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, "db", "m"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range present {
		if err := os.WriteFile(filepath.Join(base, "db", "m", name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return base
}

// arcx's text for a file it could not open, as measured on 2026-11-16. Only
// the path inside it matters to the check.
func arcxOpenErr(path string) error {
	return fmt.Errorf("invalid argument: Compute error: execution error: open %s: No such file or directory (os error 2)", path)
}

func TestVanishCheckReader(t *testing.T) {
	entries := []string{"db/m/a.parquet", "db/m/b.parquet"}

	t.Run("a clean stream does not touch the file system", func(t *testing.T) {
		base := planDir(t) // both files missing
		called := 0
		r := wrapVanishCheck(newErroringReader(t, nil), base, entries, func(string, error) { called++ })
		if err := r.Err(); err != nil || called != 0 {
			t.Fatalf("Err = %v, onVanished calls = %d; want nil, 0", err, called)
		}
	})

	t.Run("an error naming a removed plan file is a FileVanishedError", func(t *testing.T) {
		base := planDir(t, "a.parquet") // b removed
		gone := base + "/db/m/b.parquet"
		inner := arcxOpenErr(gone)
		var gotPath string
		var gotCause error
		called := 0
		r := wrapVanishCheck(newErroringReader(t, inner), base, entries, func(p string, c error) { called++; gotPath, gotCause = p, c })
		err := r.Err()
		var fv *FileVanishedError
		if !errors.As(err, &fv) {
			t.Fatalf("Err = %v (%T), want *FileVanishedError", err, err)
		}
		if fv.Path != gone || fv.VanishedPath() != gone || fv.Cause != inner || !errors.Is(err, inner) {
			t.Fatalf("got path %q cause %v; want %q, the engine error", fv.Path, fv.Cause, gone)
		}
		if err.Error() != VanishedMessage {
			t.Fatalf("Error() = %q; the client text must be VanishedMessage, never the path", err.Error())
		}
		if again := r.Err(); again != err {
			t.Fatalf("second Err = %v, want the same error", again)
		}
		if called != 1 || gotPath != gone || gotCause != inner {
			t.Fatalf("onVanished calls = %d (%q, %v); want 1 call with the path and cause", called, gotPath, gotCause)
		}
	})

	t.Run("an Err call before the stream failed does not settle the answer", func(t *testing.T) {
		base := planDir(t, "a.parquet")
		er := newErroringReader(t, nil)
		r := wrapVanishCheck(er, base, entries, nil)
		if err := r.Err(); err != nil {
			t.Fatalf("Err on a clean stream = %v", err)
		}
		er.err = arcxOpenErr(base + "/db/m/b.parquet")
		var fv *FileVanishedError
		if err := r.Err(); !errors.As(err, &fv) {
			t.Fatalf("Err after the stream failed = %v, want *FileVanishedError", err)
		}
	})

	t.Run("an engine fault while every plan file exists stays the engine's error", func(t *testing.T) {
		base := planDir(t, "a.parquet", "b.parquet")
		inner := arcxOpenErr(base + "/db/m/b.parquet") // names it, but it is there
		r := wrapVanishCheck(newErroringReader(t, inner), base, entries, nil)
		if err := r.Err(); err != inner {
			t.Fatalf("Err = %v, want the engine error unchanged", err)
		}
	})

	t.Run("a removed plan file the error does not name is not the cause", func(t *testing.T) {
		base := planDir(t, "a.parquet") // b removed, but the error is about something else
		inner := errors.New("invalid argument: Compute error: execution error: decode failed")
		r := wrapVanishCheck(newErroringReader(t, inner), base, entries, nil)
		if err := r.Err(); err != inner {
			t.Fatalf("Err = %v, want the engine error unchanged", err)
		}
	})

	t.Run("a sibling path that merely starts with a plan path does not count", func(t *testing.T) {
		base := planDir(t) // 1.parquet missing; the error is about 11.parquet
		inner := arcxOpenErr(base + "/db/m/11.parquet")
		r := wrapVanishCheck(newErroringReader(t, inner), base, []string{"db/m/1.parquet"}, nil)
		if err := r.Err(); err != inner {
			t.Fatalf("Err = %v, want the engine error unchanged", err)
		}
	})

	t.Run("a dangling symlink is still there: an error, not a retry", func(t *testing.T) {
		base := planDir(t)
		link := base + "/db/m/a.parquet"
		if err := os.Symlink(base+"/nowhere.parquet", link); err != nil {
			t.Skip("symlinks unavailable:", err)
		}
		inner := arcxOpenErr(link)
		r := wrapVanishCheck(newErroringReader(t, inner), base, entries, nil)
		if err := r.Err(); err != inner {
			t.Fatalf("Err = %v, want the engine error unchanged", err)
		}
	})
}

// The router files carry the arcx_engine tag and default CI never compiles
// them, so the wiring is pinned at source level: both serve entries must hand
// out the checked reader, and a QueryStream failure must be offered to the
// removed-file check before it is logged as an engine ERROR.
func TestServePathsCheckForRemovedFiles(t *testing.T) {
	file, err := parser.ParseFile(gotoken.NewFileSet(), "router.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	calls := map[string]map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		seen := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					seen[sel.Sel.Name] = true
				}
			}
			return true
		})
		calls[fn.Name.Name] = seen
	}
	for _, fn := range []string{"runServe", "RunArrow"} {
		for _, want := range []string{"vanishChecked", "logRemovedBeforeRead"} {
			if !calls[fn][want] {
				t.Errorf("%s does not call %s", fn, want)
			}
		}
	}
}
