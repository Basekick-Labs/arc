package api

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// adminTestCipher is a real AES-256-GCM encryptor. Not a stub: a stub would
// let a plaintext-storage regression pass.
type adminTestCipher struct{ gcm cipher.AEAD }

func newAdminTestCipher(t *testing.T) *adminTestCipher {
	t.Helper()
	key := make([]byte, 32)
	rand.Read(key)
	block, _ := aes.NewCipher(key)
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	return &adminTestCipher{gcm: gcm}
}

func (c *adminTestCipher) Encrypt(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	nonce := make([]byte, c.gcm.NonceSize())
	io.ReadFull(rand.Reader, nonce)
	return base64.StdEncoding.EncodeToString(c.gcm.Seal(nonce, nonce, []byte(p), nil)), nil
}

func (c *adminTestCipher) Decrypt(ct string) (string, error) {
	if ct == "" {
		return "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(ct)
	if err != nil {
		return "", err
	}
	if len(raw) < c.gcm.NonceSize() {
		return "", errors.New("short")
	}
	out, err := c.gcm.Open(nil, raw[:c.gcm.NonceSize()], raw[c.gcm.NonceSize():], nil)
	return string(out), err
}

type adminRig struct {
	app      *fiber.App
	registry *edgesync.Registry
}

func newAdminRig(t *testing.T) *adminRig {
	t.Helper()

	f, err := os.CreateTemp("", "admin-registry-*.db")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	f.Close()
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close(); os.Remove(f.Name()) })

	reg, err := edgesync.NewRegistry(db, newAdminTestCipher(t), zerolog.Nop())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	h, err := NewEdgeSyncAdminHandler(reg, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &adminRig{app: app, registry: reg}
}

func (r *adminRig) do(t *testing.T, method, path string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.app.Test(req, 10_000)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 {
		json.Unmarshal(raw, &out)
	}
	return resp, out
}

func TestEdgeSyncAdmin_RegisterReturnsTheSecretOnce(t *testing.T) {
	rig := newAdminRig(t)

	resp, body := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07 Telemetry"})
	if resp.StatusCode != fiber.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%v", resp.StatusCode, body)
	}

	secret, _ := body["secret"].(string)
	if secret == "" {
		t.Fatal("registration did not return a secret")
	}
	if body["warning"] == nil {
		t.Error("the response does not warn that the secret is shown once")
	}

	// Every later read path must return metadata only. An operator who loses
	// the secret has to rotate, not retrieve.
	_, got := rig.do(t, http.MethodGet, "/api/v1/sync-spokes/rocket-01", nil)
	if _, leaked := got["secret"]; leaked {
		t.Error("GET returned the secret")
	}
	_, list := rig.do(t, http.MethodGet, "/api/v1/sync-spokes/", nil)
	if bytes.Contains([]byte(toJSON(list)), []byte(secret)) {
		t.Error("the spoke list leaked the secret")
	}
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func TestEdgeSyncAdmin_RegisteredSpokeCanAuthenticate(t *testing.T) {
	rig := newAdminRig(t)

	// The end-to-end property this PR exists for: a secret handed out by
	// registration must be the one the sync path recovers to verify an HMAC.
	_, body := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07"})
	issued, _ := body["secret"].(string)

	lookup := RegistrySpokeSecrets(rig.registry, zerolog.Nop())
	recovered, ok := lookup(context.Background(), "rocket-01")
	if !ok {
		t.Fatal("a registered spoke did not resolve to a secret")
	}
	if recovered != issued {
		t.Error("the recovered secret differs from the one issued at registration")
	}

	// An unregistered spoke must resolve to nothing, with no distinction the
	// caller could use to enumerate IDs.
	if _, ok := lookup(context.Background(), "rocket-99"); ok {
		t.Error("an unregistered spoke resolved to a secret")
	}
}

func TestEdgeSyncAdmin_DisabledSpokeStopsAuthenticating(t *testing.T) {
	rig := newAdminRig(t)
	lookup := RegistrySpokeSecrets(rig.registry, zerolog.Nop())

	rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07"})
	if _, ok := lookup(context.Background(), "rocket-01"); !ok {
		t.Fatal("a fresh spoke does not authenticate")
	}

	// Disabling is the reversible cut-off — it must take effect immediately.
	resp, _ := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/rocket-01/disable", nil)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("disable: status %d", resp.StatusCode)
	}
	if _, ok := lookup(context.Background(), "rocket-01"); ok {
		t.Error("a disabled spoke still authenticates")
	}

	rig.do(t, http.MethodPost, "/api/v1/sync-spokes/rocket-01/enable", nil)
	if _, ok := lookup(context.Background(), "rocket-01"); !ok {
		t.Error("re-enabling did not restore authentication")
	}
}

func TestEdgeSyncAdmin_RotateInvalidatesTheOldSecret(t *testing.T) {
	rig := newAdminRig(t)
	lookup := RegistrySpokeSecrets(rig.registry, zerolog.Nop())

	_, reg := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07"})
	old, _ := reg["secret"].(string)

	resp, rot := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/rocket-01/rotate", nil)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("rotate: status %d", resp.StatusCode)
	}
	rotated, _ := rot["secret"].(string)
	if rotated == "" || rotated == old {
		t.Fatal("rotation did not issue a new secret")
	}
	if rot["warning"] == nil {
		t.Error("rotation does not warn that the old secret stops working")
	}

	current, ok := lookup(context.Background(), "rocket-01")
	if !ok || current != rotated {
		t.Error("the rotated secret is not what the sync path recovers")
	}
	if current == old {
		t.Error("the old secret still authenticates")
	}
}

func TestEdgeSyncAdmin_DuplicateRegistrationIsRefused(t *testing.T) {
	rig := newAdminRig(t)

	_, first := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07"})
	original, _ := first["secret"].(string)

	// 409, not a silent reissue: replacing a live edge box's secret without
	// telling anyone would lock it out mid-mission.
	resp, _ := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07 again"})
	if resp.StatusCode != fiber.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}

	lookup := RegistrySpokeSecrets(rig.registry, zerolog.Nop())
	still, _ := lookup(context.Background(), "rocket-01")
	if still != original {
		t.Error("a refused duplicate registration changed the existing secret")
	}
}

func TestEdgeSyncAdmin_RejectsMaliciousSpokeIDs(t *testing.T) {
	rig := newAdminRig(t)

	// Registration is where a namespace-escaping ID must be caught: by the
	// time a sync request arrives, the ID is already HMAC-bound.
	for _, id := range []string{"", "..", "rocket/../other", ".sync-staging"} {
		t.Run(id, func(t *testing.T) {
			resp, _ := rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
				map[string]string{"spoke_id": id, "name": "Some Spoke"})
			if resp.StatusCode == fiber.StatusCreated {
				t.Errorf("spoke ID %q was accepted", id)
			}
		})
	}
}

func TestEdgeSyncAdmin_UnknownSpokeIs404(t *testing.T) {
	rig := newAdminRig(t)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/sync-spokes/nope"},
		{http.MethodPost, "/api/v1/sync-spokes/nope/rotate"},
		{http.MethodPost, "/api/v1/sync-spokes/nope/disable"},
		{http.MethodDelete, "/api/v1/sync-spokes/nope"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			resp, _ := rig.do(t, tc.method, tc.path, nil)
			if resp.StatusCode != fiber.StatusNotFound {
				t.Errorf("status = %d, want 404", resp.StatusCode)
			}
		})
	}
}

func TestEdgeSyncAdmin_DeleteKeepsReceivedFiles(t *testing.T) {
	rig := newAdminRig(t)
	rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07"})

	resp, body := rig.do(t, http.MethodDelete, "/api/v1/sync-spokes/rocket-01", nil)
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("delete: status %d", resp.StatusCode)
	}
	// Deleting a registration must not be mistaken for deleting the data the
	// hub was trusted with.
	if body["note"] == nil {
		t.Error("the delete response does not say received files are retained")
	}

	lookup := RegistrySpokeSecrets(rig.registry, zerolog.Nop())
	if _, ok := lookup(context.Background(), "rocket-01"); ok {
		t.Error("a deleted spoke still authenticates")
	}
}

func TestEdgeSyncAdmin_ListReportsActivity(t *testing.T) {
	rig := newAdminRig(t)
	rig.do(t, http.MethodPost, "/api/v1/sync-spokes/",
		map[string]string{"spoke_id": "rocket-01", "name": "Rocket 07"})

	if err := rig.registry.RecordActivity(context.Background(), "rocket-01", 3, 4096); err != nil {
		t.Fatalf("record activity: %v", err)
	}

	_, body := rig.do(t, http.MethodGet, "/api/v1/sync-spokes/", nil)
	spokes, _ := body["spokes"].([]any)
	if len(spokes) != 1 {
		t.Fatalf("list returned %d spokes, want 1", len(spokes))
	}
	first, _ := spokes[0].(map[string]any)
	if first["files_received"] != float64(3) {
		t.Errorf("files_received = %v, want 3", first["files_received"])
	}
	if first["last_seen_at"] == nil {
		t.Error("last_seen_at is not reported, so an operator cannot tell which spokes are dark")
	}
}

func TestNewEdgeSyncAdminHandler_RequiresRegistry(t *testing.T) {
	if _, err := NewEdgeSyncAdminHandler(nil, nil, zerolog.Nop()); err == nil {
		t.Error("an admin handler was created without a registry")
	}
}
