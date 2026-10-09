package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func newScanTestApp(budget time.Duration, scan func(context.Context) (*tiering.ScanResult, error)) *fiber.App {
	h := &TieringHandler{
		logger:     zerolog.Nop(),
		scanBudget: func() time.Duration { return budget },
		scanTiers:  scan,
	}
	app := fiber.New()
	app.Post("/api/v1/tiering/scan", h.ScanFiles)
	return app
}

func postScan(t *testing.T, app *fiber.App) (int, map[string]any) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/api/v1/tiering/scan", nil), 5000)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("response is not JSON: %q", raw)
		}
	}
	return resp.StatusCode, body
}

// A scan that ran out of budget used to return a bare 500 with
// "context deadline exceeded" and no counts — the operator could not tell how
// far it got, or that no stale hot row had been retired (#1154).
func TestScanEndpointReturns503WithPartialResultOnDeadline(t *testing.T) {
	partial := &tiering.ScanResult{FilesScanned: 1234, FilesRegistered: 1200, Truncated: true}
	app := newScanTestApp(5*time.Second, func(context.Context) (*tiering.ScanResult, error) {
		return partial, context.DeadlineExceeded
	})

	status, body := postScan(t, app)
	if status != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	result, ok := body["result"].(map[string]any)
	if !ok {
		t.Fatalf("no result in the body; the partial counts were discarded: %v", body)
	}
	if result["files_scanned"] != float64(1234) {
		t.Errorf("files_scanned = %v, want 1234", result["files_scanned"])
	}
	if result["truncated"] != true {
		t.Errorf("truncated = %v, want true", result["truncated"])
	}
	msg, _ := body["message"].(string)
	if msg == "" {
		t.Error("no message telling the operator which key to raise")
	}
}

// context.Canceled on this path means the SERVER is shutting down: fasthttp
// closes RequestCtx.Done only on shutdown and never on a client disconnect.
// Telling the operator to raise their scan budget would invert the diagnosis.
func TestScanEndpointDistinguishesShutdownFromDeadline(t *testing.T) {
	app := newScanTestApp(5*time.Second, func(context.Context) (*tiering.ScanResult, error) {
		return &tiering.ScanResult{}, context.Canceled
	})

	status, body := postScan(t, app)
	if status != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	errText, _ := body["error"].(string)
	if errText == "" {
		t.Fatal("no error text")
	}
	// It must not blame the budget, which is the deadline's diagnosis.
	for _, forbidden := range []string{"budget", "scan_timeout"} {
		if contains(errText, forbidden) || contains(fmt.Sprint(body["message"]), forbidden) {
			t.Errorf("a shutdown response mentions %q, which tells the operator to change the wrong thing: %v", forbidden, body)
		}
	}
	if !contains(errText, "shutting down") {
		t.Errorf("error does not name shutdown: %q", errText)
	}
}

// A second concurrent scan is refused rather than left to pile up: the
// endpoint has no handler deadline and the client disconnecting does not
// cancel the scan, so retries would otherwise stack full scans.
func TestScanEndpointReturns409WhenAScanIsRunning(t *testing.T) {
	app := newScanTestApp(5*time.Second, func(context.Context) (*tiering.ScanResult, error) {
		return &tiering.ScanResult{}, tiering.ErrScanRunning
	})

	status, body := postScan(t, app)
	if status != fiber.StatusConflict {
		t.Fatalf("status = %d, want 409", status)
	}
	if msg, _ := body["message"].(string); !contains(msg, "status") {
		t.Errorf("the 409 should point at the status endpoint for the running scan's result: %v", body)
	}
}

// Anything that is not a context error keeps today's 500, so automation
// keying on a server error is unaffected by the narrower codes above.
func TestScanEndpointKeeps500ForANonContextError(t *testing.T) {
	app := newScanTestApp(5*time.Second, func(context.Context) (*tiering.ScanResult, error) {
		return &tiering.ScanResult{Errors: 3}, errors.New("storage backend exploded")
	})

	status, body := postScan(t, app)
	if status != fiber.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
	if errText, _ := body["error"].(string); !contains(errText, "exploded") {
		t.Errorf("the underlying cause is missing: %v", body)
	}
}

func TestScanEndpointReturnsTheResultOnSuccess(t *testing.T) {
	app := newScanTestApp(5*time.Second, func(context.Context) (*tiering.ScanResult, error) {
		return &tiering.ScanResult{FilesScanned: 9, FilesRegistered: 9}, nil
	})

	status, body := postScan(t, app)
	if status != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if body["files_scanned"] != float64(9) {
		t.Errorf("files_scanned = %v, want 9 at the top level on success", body["files_scanned"])
	}
	if _, present := body["truncated"]; present {
		t.Error("truncated should be omitted on a complete scan")
	}
}

// A truncation in the retirement tail returns no error of its own, so the
// handler sees err nil with the flag set. Answering 200 would report a scan
// that left stale hot rows behind as a clean one.
func TestScanEndpointReturns503OnANilErrorTruncation(t *testing.T) {
	app := newScanTestApp(5*time.Second, func(context.Context) (*tiering.ScanResult, error) {
		return &tiering.ScanResult{FilesScanned: 800, FilesRegistered: 800, Truncated: true}, nil
	})

	status, body := postScan(t, app)
	if status != fiber.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for a truncated scan that returned no error", status)
	}
	result, ok := body["result"].(map[string]any)
	if !ok {
		t.Fatalf("no result in the body: %v", body)
	}
	if result["truncated"] != true {
		t.Errorf("truncated = %v, want true", result["truncated"])
	}
	if result["files_scanned"] != float64(800) {
		t.Errorf("files_scanned = %v, want 800", result["files_scanned"])
	}
}

// The handler bounds the scan with the configured budget rather than a
// hardcoded 30 minutes.
func TestScanEndpointUsesTheConfiguredBudget(t *testing.T) {
	var seen time.Duration
	app := newScanTestApp(37*time.Second, func(ctx context.Context) (*tiering.ScanResult, error) {
		if deadline, ok := ctx.Deadline(); ok {
			seen = time.Until(deadline).Round(time.Second)
		}
		return &tiering.ScanResult{}, nil
	})

	if status, _ := postScan(t, app); status != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if seen != 37*time.Second {
		t.Fatalf("scan context deadline was %s away, want 37s from tiered_storage.scan_timeout", seen)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && strings.Contains(haystack, needle)
}
