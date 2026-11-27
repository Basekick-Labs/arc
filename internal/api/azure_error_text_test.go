package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

const apiTestSigMarker = "ZZAPISIGMARKER0123456789"

// azureFailingBackend is an Azure backend authorised by a signature
// (apiTestSigMarker), pointed at a server that answers every request with an
// error whose body repeats the request (hostile = true), or that drops the
// connection, so the client's own error quotes the request address.
func azureFailingBackend(t *testing.T, hostile bool) storage.Backend {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && !strings.Contains(r.URL.RawQuery, "comp=") {
			w.WriteHeader(http.StatusOK) // container probe at construction
			return
		}
		if hostile {
			w.Header().Set("x-ms-error-code", "InvalidQueryParameterValue")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("<Error><Code>InvalidQueryParameterValue</Code><Url>http://" + r.Host + r.URL.RequestURI() + "</Url></Error>"))
			return
		}
		if h, ok := w.(http.Hijacker); ok {
			if conn, _, err := h.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
	}))
	t.Cleanup(srv.Close)
	b, err := storage.NewAzureBlobBackend(&storage.AzureBlobConfig{
		AccountName: "acct", ContainerName: "c", Endpoint: srv.URL,
		SASToken: "sv=2021-12-02&ss=b&srt=sco&sp=r&se=2030-01-01T00%3A00%3A00Z&sig=" + apiTestSigMarker,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func readTokenApp(t *testing.T) (*auth.AuthManager, string) {
	t.Helper()
	am, err := auth.NewAuthManager(":memory:", time.Minute, 16, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	tok, err := am.CreateToken(context.Background(), "reader", "read-only", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	return am, tok
}

func getWithToken(t *testing.T, app *fiber.App, method, path, token string, body []byte) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-API-Key", token)
	}
	resp, err := app.Test(req, 60000)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// A storage failure on an Azure backend reaches a read-only caller as error
// text in several responses; none of them may carry the credential the
// backend is authorised with.
func TestAzureFailureTextCarriesNoCredential(t *testing.T) {
	for _, hostile := range []bool{true, false} {
		backend := azureFailingBackend(t, hostile)
		am, tok := readTokenApp(t)
		check := func(sink string, code int, body string) {
			t.Helper()
			if code < 400 {
				t.Errorf("%s (hostile=%v): status %d, want an error (vacuous check): %s", sink, hostile, code, body)
			}
			if strings.Contains(body, apiTestSigMarker) || strings.Contains(body, "sig=") {
				t.Errorf("%s (hostile=%v): credential in response: %s", sink, hostile, body)
			}
		}

		// GET /api/v1/databases
		app := fiber.New()
		app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
		NewDatabasesHandler(backend, nil, am, zerolog.Nop()).RegisterRoutes(app)
		code, body := getWithToken(t, app, http.MethodGet, "/api/v1/databases", tok, nil)
		check("GET /api/v1/databases", code, body)

		// SHOW DATABASES through the query endpoint.
		dir := t.TempDir()
		duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dir, TempDirectory: dir}, zerolog.Nop())
		if err != nil {
			t.Skipf("DuckDB: %v", err)
		}
		t.Cleanup(func() { duck.Close() })
		qapp := fiber.New()
		qapp.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
		qh := NewQueryHandler(duck, backend, zerolog.Nop(), 30, 1000)
		qh.authManager = am
		qh.RegisterRoutes(qapp)
		req, _ := json.Marshal(QueryRequest{SQL: "SHOW DATABASES"})
		code, body = getWithToken(t, qapp, http.MethodPost, "/api/v1/query", tok, req)
		check("SHOW DATABASES", code, body)

		// The reporter's case: reconciliation status after a failed run.
		rec, err := reconciliation.NewReconciler(reconciliation.Config{
			Enabled: true, BackendKind: reconciliation.BackendStandalone,
			MaxRunDuration: 30 * time.Second, MaxRootWalkDatabases: 1,
		}, nil, backend, nil, nil, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		sched, err := reconciliation.NewScheduler(reconciliation.SchedulerConfig{Reconciler: rec, Schedule: "* * * * *", Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		run, _ := sched.TriggerNow(ctx, true)
		cancel()
		if run == nil || len(run.Errors) == 0 {
			t.Fatalf("hostile=%v: reconciliation run recorded no error (vacuous check)", hostile)
		}
		rapp := fiber.New()
		rapp.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
		NewReconciliationHandler(sched, nil, am, zerolog.Nop()).RegisterRoutes(rapp)
		code, body = getWithToken(t, rapp, http.MethodGet, "/api/v1/reconciliation/status", tok, nil)
		if code != http.StatusOK || !strings.Contains(body, `"errors"`) {
			t.Errorf("reconciliation status (hostile=%v): %d %s", hostile, code, body)
		}
		if strings.Contains(body, apiTestSigMarker) || strings.Contains(body, "sig=") {
			t.Errorf("reconciliation status (hostile=%v): credential in response: %s", hostile, body)
		}
		if c, _ := getWithToken(t, rapp, http.MethodGet, "/api/v1/reconciliation/status", "", nil); c != http.StatusUnauthorized {
			t.Errorf("reconciliation status without a token: %d, want 401", c)
		}
	}
}
