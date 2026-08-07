package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestEdgeSync_SpokeToHubEndToEnd drives the real spoke agent against the real
// hub handlers over real HTTP.
//
// Every other test in this sequence exercises one side against an in-process
// stand-in. This is the only one that proves the two halves interoperate — the
// header names, the HMAC field order, the status-code mapping, and the resume
// offsets all have to agree, and each was written from the design doc rather
// than from the other side's code.
func TestEdgeSync_SpokeToHubEndToEnd(t *testing.T) {
	ctx := context.Background()

	// --- Hub ---
	hubDir, err := os.MkdirTemp("", "e2e-hub-*")
	if err != nil {
		t.Fatalf("hub dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(hubDir) })

	hubBackend, err := storage.NewLocalBackend(hubDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("hub backend: %v", err)
	}
	t.Cleanup(func() { hubBackend.Close() })

	hubIndex := newTestAPIHubIndex(t)
	receiver, err := edgesync.NewReceiver(edgesync.ReceiverConfig{
		Backend: hubBackend, Index: hubIndex, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	reconciler, err := edgesync.NewReconciler(edgesync.ReconcilerConfig{
		Index: hubIndex, Backend: hubBackend, MaxEntries: 100,
	})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}

	const spokeID, hubID, secret = "rocket-01", "ground-station", "e2e-shared-secret"
	handler, err := NewEdgeSyncHandler(EdgeSyncHandlerConfig{
		Receiver:     receiver,
		Reconciler:   reconciler,
		SpokeSecrets: StaticSpokeSecrets(map[string]string{spokeID: secret}),
		Replay:       security.NewNonceCache(security.HMACTimestampTolerance),
		HubID:        hubID,
		MaxFileBytes: 8 << 20,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true, BodyLimit: 32 << 20})
	handler.RegisterRoutes(app)

	// A real HTTP listener, so the transport exercises actual network I/O
	// rather than Fiber's in-process test harness.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := app.Test(r, testRequestTimeoutMS)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = copyBody(w, resp.Body)
	}))
	t.Cleanup(srv.Close)

	// --- Spoke ---
	spokeDir, err := os.MkdirTemp("", "e2e-spoke-*")
	if err != nil {
		t.Fatalf("spoke dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(spokeDir) })

	spokeBackend, err := storage.NewLocalBackend(spokeDir, zerolog.Nop())
	if err != nil {
		t.Fatalf("spoke backend: %v", err)
	}
	t.Cleanup(func() { spokeBackend.Close() })

	ledgerDB, err := sql.Open("sqlite3", spokeDir+"/ledger.db")
	if err != nil {
		t.Fatalf("ledger db: %v", err)
	}
	t.Cleanup(func() { ledgerDB.Close() })
	ledger, err := edgesync.NewLedger(ledgerDB, zerolog.Nop())
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	transport, err := edgesync.NewHTTPTransport(edgesync.HTTPTransportConfig{
		BaseURL: srv.URL, SpokeID: spokeID, Secret: secret,
	})
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	agent, err := edgesync.NewAgent(edgesync.AgentConfig{
		Ledger: ledger, Transport: transport, Backend: spokeBackend,
		HubID: hubID, SpokeID: spokeID, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("agent: %v", err)
	}

	// --- The spoke produces three files ---
	contents := map[string][]byte{}
	for i := 0; i < 3; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/1%d/cpu_%d.parquet", i, i)
		c := []byte(fmt.Sprintf("parquet payload number %d", i))
		contents[p] = c
		if err := spokeBackend.Write(ctx, p, c); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	res, err := agent.Run(ctx)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if res.Discovered != 3 {
		t.Errorf("discovered = %d, want 3", res.Discovered)
	}
	if res.Sent != 3 {
		t.Fatalf("sent = %d, want 3 (failed=%d partial=%d)", res.Sent, res.Failed, res.Partial)
	}

	// The hub must hold each file under the spoke's namespace, byte-identical.
	for p, want := range contents {
		got, err := hubBackend.Read(ctx, edgesync.NamespacedPath(spokeID, p))
		if err != nil {
			t.Errorf("hub is missing %s: %v", p, err)
			continue
		}
		if string(got) != string(want) {
			t.Errorf("%s: hub content differs from the spoke's", p)
		}
		// The digest the hub indexed must be the file's real one, or reconcile
		// would report it missing on the next pass.
		wantSHA := sha256.Sum256(want)
		held, err := hubIndex.Lookup(ctx, spokeID, []string{p})
		if err != nil {
			t.Errorf("%s: index lookup: %v", p, err)
		} else if held[p] != hex.EncodeToString(wantSHA[:]) {
			t.Errorf("%s: hub indexed digest %q, want %q", p, held[p], hex.EncodeToString(wantSHA[:]))
		}
	}

	// --- A second pass must be free ---
	res2, err := agent.Run(ctx)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if res2.Sent != 0 || res2.BytesSent != 0 {
		t.Errorf("second pass sent %d files / %d bytes; an already-synced corpus must cost nothing",
			res2.Sent, res2.BytesSent)
	}

	// --- A new file syncs incrementally ---
	newPath := "metrics/cpu/2026/08/07/19/cpu_new.parquet"
	newContent := []byte("a freshly compacted file")
	if err := spokeBackend.Write(ctx, newPath, newContent); err != nil {
		t.Fatalf("write new: %v", err)
	}
	res3, err := agent.Run(ctx)
	if err != nil {
		t.Fatalf("third sync: %v", err)
	}
	if res3.Discovered != 1 || res3.Sent != 1 {
		t.Errorf("incremental pass: discovered=%d sent=%d, want 1/1", res3.Discovered, res3.Sent)
	}

	// --- Status reflects reality ---
	st, err := agent.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Pending != 0 {
		t.Errorf("pending = %d after a full sync, want 0", st.Pending)
	}
	if st.Synced != 4 {
		t.Errorf("synced = %d, want 4", st.Synced)
	}
}

func copyBody(w http.ResponseWriter, r io.Reader) (int64, error) {
	return io.Copy(w, r)
}
