package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/arcxrouter"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The client-facing text of an arcx removed-file error is the DuckDB path's
// vanished answer, word for word (#1357).
func TestArcxVanishedMessageMatchesDuckDBPath(t *testing.T) {
	if arcxrouter.VanishedMessage != vanishedPartitionMessage {
		t.Fatalf("arcxrouter.VanishedMessage = %q\nvanishedPartitionMessage = %q", arcxrouter.VanishedMessage, vanishedPartitionMessage)
	}
}

// drainErrorResponse runs respondArcxMsgPackDrainError on a request and
// returns the response and what onFail received.
func drainErrorResponse(t *testing.T, derr error, msgpackWire bool) (status int, retryAfter, body, failMsg string) {
	t.Helper()
	h := &QueryHandler{logger: zerolog.Nop()}
	app := fiber.New()
	app.Post("/q", func(c *fiber.Ctx) error {
		if msgpackWire {
			c.Locals(wireFormatLocalsKey, wireFormatMsgPack)
		}
		return h.respondArcxMsgPackDrainError(c, derr, "ts", time.Now(), func(m string) { failMsg = m })
	})
	resp, err := app.Test(httptest.NewRequest("POST", "/q", nil))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get(fiber.HeaderRetryAfter), string(b), failMsg
}

func TestRespondArcxMsgPackDrainError(t *testing.T) {
	const path = "/data/arc/db1/cpu/2026/11/16/12/f.parquet"
	// The shape drainArrowBatches returns: the reader's error, %w-wrapped.
	vanished := fmt.Errorf("arrow reader error after 2620000 rows: %w",
		&arcxrouter.FileVanishedError{Path: path, Cause: errors.New("open " + path + ": No such file or directory (os error 2)")})

	t.Run("a removed plan file answers the retryable 503", func(t *testing.T) {
		for _, wire := range []bool{true, false} {
			status, retryAfter, body, failMsg := drainErrorResponse(t, vanished, wire)
			if status != fiber.StatusServiceUnavailable || retryAfter != "1" {
				t.Fatalf("msgpack=%v: status %d Retry-After %q; want 503 and 1", wire, status, retryAfter)
			}
			if !strings.Contains(body, vanishedPartitionMessage) || strings.Contains(body, path) {
				t.Fatalf("msgpack=%v: body %q must carry the fixed message and no storage path", wire, body)
			}
			if failMsg != vanishedPartitionMessage {
				t.Fatalf("msgpack=%v: registry got %q", wire, failMsg)
			}
		}
	})

	t.Run("any other drain error is still a 500", func(t *testing.T) {
		derr := errors.New("arrow reader error after 10 rows: decode failed")
		status, retryAfter, body, failMsg := drainErrorResponse(t, derr, false)
		var r QueryResponse
		_ = json.Unmarshal([]byte(body), &r)
		if status != fiber.StatusInternalServerError || retryAfter != "" || r.Error != derr.Error() || failMsg != derr.Error() {
			t.Fatalf("status %d Retry-After %q error %q fail %q", status, retryAfter, r.Error, failMsg)
		}
	})

	t.Run("the query deadline is a 504", func(t *testing.T) {
		derr := fmt.Errorf("drain cancelled at row 5: %w", context.DeadlineExceeded)
		status, _, _, failMsg := drainErrorResponse(t, derr, true)
		if status != fiber.StatusGatewayTimeout || failMsg != "Query timed out" {
			t.Fatalf("status %d fail %q; want 504", status, failMsg)
		}
	})
}

// A removed file is the data changing under a query, logged at Warn on the
// JSON and Arrow stream paths, not as a server Error.
func TestStreamErrEventVanishedIsWarn(t *testing.T) {
	var buf bytes.Buffer
	h := &QueryHandler{logger: zerolog.New(&buf)}
	vanished := fmt.Errorf("arrow reader error after 1 rows: %w", &arcxrouter.FileVanishedError{Path: "/p.parquet", Cause: errors.New("x")})
	h.streamErrEvent(vanished).Msg("m")
	h.streamErrEvent(errors.New("engine fault")).Msg("m")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"level":"warn"`) || !strings.Contains(lines[1], `"level":"error"`) {
		t.Fatalf("levels: %v", lines)
	}
}

// arcx_hook.go is tagged arcx_engine, which default CI never compiles, so the
// msgpack drain's use of the helper is pinned at source level.
func TestArcxMsgPackDrainUsesVanishedHelper(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "arcx_hook.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "serveArcxResult" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "respondArcxMsgPackDrainError" {
					found = true
				}
			}
			return true
		})
	}
	if !found {
		t.Fatal("serveArcxResult no longer answers its msgpack drain error through respondArcxMsgPackDrainError")
	}
}
