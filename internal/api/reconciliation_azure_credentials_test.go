package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// These doubles exercise the shared-store reconciliation contract. They do not
// simulate Raft election or the startup wiring that assigns a compactor lease.
type azureStatusCoordinator struct {
	syncs  int
	writes int
}

func (c *azureStatusCoordinator) SyncManifest(ctx context.Context) error {
	c.syncs++
	return ctx.Err()
}

func (*azureStatusCoordinator) GetFileManifest() []*raft.FileEntry { return nil }

func (*azureStatusCoordinator) GetFileEntry(string) (*raft.FileEntry, bool) {
	return nil, false
}

func (c *azureStatusCoordinator) BatchFileOpsInManifest([]raft.BatchFileOp) error {
	c.writes++
	return fmt.Errorf("unexpected manifest mutation in credential test")
}

type azureStatusGate struct{ active bool }

func (g *azureStatusGate) ShouldRunStorageScan() bool   { return g.active }
func (g *azureStatusGate) ShouldRunManifestSweep() bool { return g.active }
func (*azureStatusGate) Role() string                   { return "compactor" }

// Keep this independent of DuckDB initialization: a skipped query test must not
// also skip the reconciliation status credential boundary. All credentials are
// synthetic and the real Azure SDK can contact only the loopback test endpoint.
func TestReconciliationAzureCredentialBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sharedKey bool
		echoError bool
	}{
		{name: "sas_transport"},
		{name: "sas_response_body", echoError: true},
		{name: "shared_key_transport", sharedKey: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const signature = "SYNTHETIC_STATUS_SIGNATURE_20261027"
			const keyMaterial = "SYNTHETIC_STATUS_SHARED_KEY_20261027"
			encodedKey := base64.StdEncoding.EncodeToString([]byte(keyMaterial))
			var listRequests, credentialRequests atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusOK) // backend construction probe
					return
				}
				if r.Method != http.MethodGet || r.URL.Query().Get("comp") != "list" {
					http.Error(w, "unexpected storage request", http.StatusBadRequest)
					return
				}
				listRequests.Add(1)
				if (!tc.sharedKey && r.URL.Query().Get("sig") == signature) ||
					(tc.sharedKey && strings.HasPrefix(r.Header.Get("Authorization"), "SharedKey probeaccount:")) {
					credentialRequests.Add(1)
				}
				if tc.echoError {
					w.Header().Set("Content-Type", "application/xml")
					w.Header().Set("x-ms-error-code", "InvalidQueryParameterValue")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = fmt.Fprintf(w, "<Error><Code>InvalidQueryParameterValue</Code><Message>request %s failed</Message></Error>",
						html.EscapeString("http://"+r.Host+r.URL.RequestURI()))
					return
				}
				hijacker, ok := w.(http.Hijacker)
				if !ok {
					t.Error("loopback server does not support connection hijacking")
					return
				}
				conn, _, err := hijacker.Hijack()
				if err != nil {
					t.Errorf("drop listing connection: %v", err)
					return
				}
				_ = conn.Close()
			}))
			t.Cleanup(srv.Close)

			cfg := storage.AzureBlobConfig{
				AccountName: "probeaccount", ContainerName: "probecontainer", Endpoint: srv.URL,
			}
			if tc.sharedKey {
				cfg.AccountKey = encodedKey
			} else {
				cfg.SASToken = "sv=2021-12-02&ss=b&srt=sco&sp=r&se=2030-01-01T00%3A00%3A00Z&sig=" + signature
			}
			backend, err := storage.NewAzureBlobBackend(&cfg, zerolog.Nop())
			if err != nil {
				t.Fatalf("create Azure backend: %v", err)
			}
			t.Cleanup(func() { _ = backend.Close() })
			coord := &azureStatusCoordinator{}
			gate := &azureStatusGate{}
			rec, err := reconciliation.NewReconciler(reconciliation.Config{
				Enabled: true, BackendKind: reconciliation.BackendShared,
				MaxRunDuration: 10 * time.Second, MaxRootWalkDatabases: 1,
			}, coord, backend, gate, nil, zerolog.Nop())
			if err != nil {
				t.Fatalf("create reconciler: %v", err)
			}
			scheduler, err := reconciliation.NewScheduler(reconciliation.SchedulerConfig{
				Reconciler: rec, Schedule: "* * * * *", Logger: zerolog.Nop(),
			})
			if err != nil {
				t.Fatalf("create scheduler: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			// Disable SDK retries per call, without changing package-global client
			// options or allowing a parent deadline to replace the transport error.
			ctx = policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
			if run, err := scheduler.TriggerNow(ctx, true); !errors.Is(err, reconciliation.ErrGated) || run != nil {
				t.Fatalf("inactive gate: run=%v err=%v, want nil run and ErrGated", run, err)
			}
			if listRequests.Load() != 0 || coord.syncs != 0 || rec.LastRun() != nil {
				t.Fatal("inactive gate performed work or retained a run")
			}
			gate.active = true
			var runIDs []string
			for range 2 {
				run, err := scheduler.TriggerNow(ctx, true)
				if err != nil {
					t.Fatalf("trigger dry-run reconciliation: %v", err)
				}
				if run == nil || run.Aborted || !run.WalkPartial || len(run.Errors) == 0 || run.FinishedAt.IsZero() {
					t.Fatalf("expected completed partial walk with retained listing error, got %+v", run)
				}
				if !run.DryRun || run.BackendKind != reconciliation.BackendShared {
					t.Fatal("test did not exercise shared-store dry-run reconciliation")
				}
				runIDs = append(runIDs, run.ID)
			}
			if ctx.Err() != nil || coord.syncs != 2 || coord.writes != 0 ||
				listRequests.Load() < 2 || credentialRequests.Load() != listRequests.Load() {
				t.Fatalf("listing path not established: context=%v syncs=%d writes=%d lists=%d credential_lists=%d",
					ctx.Err(), coord.syncs, coord.writes, listRequests.Load(), credentialRequests.Load())
			}

			am, err := auth.NewAuthManager(":memory:", time.Minute, 16, zerolog.Nop())
			if err != nil {
				t.Fatalf("create auth manager: %v", err)
			}
			t.Cleanup(func() { am.Close() })
			makeToken := func(permission string) string {
				t.Helper()
				token, err := am.CreateToken(ctx, permission, "synthetic status test", permission, nil)
				if err != nil {
					t.Fatalf("create %s token: %v", permission, err)
				}
				return token
			}
			readToken, writeToken := makeToken("read"), makeToken("write")
			app := fiber.New()
			app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
			NewReconciliationHandler(scheduler, nil, am, zerolog.Nop()).RegisterRoutes(app)
			getStatus := func(token string, want int) []byte {
				t.Helper()
				req := httptest.NewRequest(http.MethodGet, "/api/v1/reconciliation/status", nil)
				if token != "" {
					req.Header.Set("X-API-Key", token)
				}
				resp, err := app.Test(req, 5000)
				if err != nil {
					t.Fatalf("get status: %v", err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("read status: %v", err)
				}
				if resp.StatusCode != want {
					t.Fatalf("status=%d, want %d", resp.StatusCode, want)
				}
				return body
			}
			getStatus("", http.StatusUnauthorized)
			getStatus(writeToken, http.StatusForbidden)
			body := getStatus(readToken, http.StatusOK)
			var status struct {
				LastRun    *reconciliation.Run  `json:"last_run"`
				RecentRuns []reconciliation.Run `json:"recent_runs"`
			}
			if err := json.Unmarshal(body, &status); err != nil {
				t.Fatalf("decode status: %v", err)
			}
			if status.LastRun == nil || status.LastRun.ID != runIDs[1] || len(status.RecentRuns) != 2 {
				t.Fatal("status did not retain the last run and both recent runs")
			}
			checkRun := func(field string, run reconciliation.Run, wantID string) {
				t.Helper()
				if run.ID != wantID || len(run.Errors) == 0 || !run.WalkPartial {
					t.Fatalf("%s: expected the recorded partial listing run", field)
				}
				text := strings.Join(run.Errors, "\n")
				if !strings.Contains(text, "root list_directories:") || !strings.Contains(text, "failed to list Azure directories") {
					t.Errorf("%s: missing safe listing failure context", field)
				}
				for _, forbidden := range []string{signature, keyMaterial, encodedKey, "sig=", "sv=2021", "sp=r", "se=2030"} {
					if strings.Contains(text, forbidden) {
						t.Errorf("%s: credential disclosure: retained error contains %q", field, forbidden)
					}
				}
			}
			checkRun("last_run.errors", *status.LastRun, runIDs[1])
			for i, run := range status.RecentRuns {
				checkRun(fmt.Sprintf("recent_runs[%d].errors", i), run, runIDs[1-i])
			}
			for _, secret := range []string{signature, keyMaterial, encodedKey} {
				if strings.Contains(string(body), secret) {
					t.Errorf("status response contains synthetic credential %q", secret)
				}
			}
		})
	}
}
