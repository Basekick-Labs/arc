package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

// Every logged error passes through the sanitiser, which masks each quoted
// span and keeps only the first line (#1113). A storage error that quotes a
// value loses that value in the log, and a lone quote masks everything after
// it. This guard keeps quotes out of every error format string in the package.
func TestStorageErrorStringsHoldNoQuotes_Issue1113(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 || !isErrorConstructor(call.Fun) {
				return true
			}
			format, ok := literalString(call.Args[0])
			if !ok {
				t.Errorf("%s: error format is not a string literal; the quote check cannot read it", fset.Position(call.Pos()))
				return true
			}
			if strings.ContainsAny(format, `'"`) || hasQVerb(format) {
				t.Errorf("%s: error text holds a quote or %%q; the log sanitiser masks quoted spans, and a lone quote or apostrophe masks the rest of the line: %s", fset.Position(call.Pos()), format)
			}
			return true
		})
	}
}

func isErrorConstructor(fun ast.Expr) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && ((pkg.Name == "fmt" && sel.Sel.Name == "Errorf") || (pkg.Name == "errors" && sel.Sel.Name == "New"))
}

// literalString folds a string literal or a + concatenation of them.
func literalString(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(v.Value)
		return s, err == nil
	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return "", false
		}
		l, ok := literalString(v.X)
		if !ok {
			return "", false
		}
		r, ok := literalString(v.Y)
		return l + r, ok
	case *ast.ParenExpr:
		return literalString(v.X)
	}
	return "", false
}

// hasQVerb reports a %q verb with any flags or width (%q, %+q, %#q, %-10q).
func hasQVerb(format string) bool {
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		j := i + 1
		if j < len(format) && format[j] == '%' {
			i = j
			continue
		}
		for j < len(format) && strings.IndexByte("+-# 0123456789.*[]", format[j]) >= 0 {
			j++
		}
		if j < len(format) && format[j] == 'q' {
			return true
		}
		i = j
	}
	return false
}

// The messages changed for #1113 read the same after the sanitiser.
func TestStorageErrorsSurviveLogging_Issue1113(t *testing.T) {
	b := &LocalBackend{}
	_, stagedErr := b.stagedPath("db/m/")
	_, dbErr := GetStoragePath(nil, "_internal", "cpu")
	_, prefixedErr := prefixObjectKey("p/", strings.Repeat("x/", 509)+"x")
	_, endpointErr := GCSEndpointURL("ftp://x")
	_, factoryErr := NewBackend(BackendSpec{Type: `it's`}, zerolog.Nop())
	for _, tc := range []struct {
		err  error
		want string
	}{
		{ValidateKey("db/m/"), "ends in a separator"},
		{ValidateKey("db/m/f" + PartSuffix), "reserved for write staging"},
		{ValidateKeySegment(`a\b`), "backslash"},
		{ValidateKeySegment("a/b"), "more than one path component"},
		{ValidateGlobSafe("db/cpu*/f"), "glob metacharacter U+002A"},
		{ValidateGlobSafe(`db/it's[1]/f`), "glob metacharacter U+005B"},
		{stagedErr, "ends in a separator"},
		{dbErr, "starting with _ is reserved"},
		{prefixedErr, "under the configured prefix"},
		{endpointErr, "not a usable http(s) URL"},
		{factoryErr, "use local, s3, minio, azure, azblob or gcs"},
	} {
		if tc.err == nil {
			t.Errorf("no error for the %q case", tc.want)
			continue
		}
		msg := tc.err.Error()
		if got := sqlutil.SanitizeErrText(msg); got != msg {
			t.Errorf("error changes when logged: %q -> %q", msg, got)
		}
		if !strings.Contains(msg, tc.want) {
			t.Errorf("error %q does not contain %q", msg, tc.want)
		}
	}
}

// A batch delete refuses each bad key on its own. The joined error is logged
// only up to its first line, so each refused key is logged with the key as a
// field, and named in the error text (#1113).
func TestPartitionValidKeysNamesEachRefusedKey_Issue1113(t *testing.T) {
	var logs bytes.Buffer
	logger := zerolog.New(&logs)
	bad1, bad2 := "db/m//f.parquet", `db/it's/`+strings.Repeat("y", MaxUsableKeySegmentLen+1)
	usable, rejected := partitionValidKeys("", []string{"db/m/ok.parquet", bad1, bad2}, logger, "Azure")
	if len(usable) != 1 || len(rejected) != 2 {
		t.Fatalf("usable=%v rejected=%v, want 1 usable and 2 refused", usable, rejected)
	}
	for i, key := range []string{bad1, bad2} {
		if !errors.Is(rejected[i], ErrInvalidPath) {
			t.Errorf("rejection %v does not match ErrInvalidPath", rejected[i])
		}
		if !strings.HasPrefix(rejected[i].Error(), key+": ") {
			t.Errorf("rejection %q does not name its key", rejected[i])
		}
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("logged %d lines, want one per refused key:\n%s", len(lines), logs.String())
	}
	for i, key := range []string{bad1, bad2} {
		field, _ := json.Marshal(key)
		if !strings.Contains(lines[i], `"key":`+string(field)) {
			t.Errorf("log line %q does not carry key %q as a field", lines[i], key)
		}
	}
}

// S3 refuses bad keys in its own loop; the same rule holds there (#1113).
func TestS3DeleteBatchNamesEachRefusedKey_Issue1113(t *testing.T) {
	var logs bytes.Buffer
	b := &S3Backend{logger: zerolog.New(&logs)}
	bad1, bad2 := "db/m//f.parquet", "db/m/./g.parquet"
	err := b.DeleteBatch(context.Background(), []string{bad1, bad2})
	if err == nil {
		t.Fatal("DeleteBatch accepted two invalid keys")
	}
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("error %v does not match ErrInvalidPath", err)
	}
	for _, key := range []string{bad1, bad2} {
		if !strings.Contains(err.Error(), key+": ") {
			t.Errorf("error %q does not name key %q", err, key)
		}
		field, _ := json.Marshal(key)
		if !strings.Contains(logs.String(), `"key":`+string(field)) {
			t.Errorf("no log line carries key %q as a field:\n%s", key, logs.String())
		}
	}
}
