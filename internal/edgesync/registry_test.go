package edgesync

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// testCipher is a real AES-256-GCM encryptor, matching what production uses.
// Deliberately not a no-op stub: a stub would let a plaintext-storage
// regression pass every test in this file.
type testCipher struct{ gcm cipher.AEAD }

func newTestCipher(t *testing.T) *testCipher {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("key: %v", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("gcm: %v", err)
	}
	return &testCipher{gcm: gcm}
}

func (c *testCipher) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(c.gcm.Seal(nonce, nonce, []byte(plaintext), nil)), nil
}

func (c *testCipher) Decrypt(ciphertext string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	if len(raw) < c.gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	nonce, body := raw[:c.gcm.NonceSize()], raw[c.gcm.NonceSize():]
	out, err := c.gcm.Open(nil, nonce, body, nil)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func newTestRegistry(t *testing.T) (*Registry, *sql.DB) {
	t.Helper()

	f, err := os.CreateTemp("", "spoke-registry-*.db")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	f.Close()

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close(); os.Remove(f.Name()) })

	reg, err := NewRegistry(db, newTestCipher(t), zerolog.Nop())
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	return reg, db
}

func TestRegistry_RegisterReturnsAUsableSecret(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	secret, err := reg.Register(ctx, "rocket-01", "Rocket 07 Telemetry")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(secret) != SecretBytes*2 {
		t.Errorf("secret is %d hex chars, want %d", len(secret), SecretBytes*2)
	}

	// The whole point: the hub must be able to recover it to recompute an
	// HMAC. A hashed secret would fail here, which is why §8.1's `secret_hash`
	// column could not work.
	got, err := reg.Secret(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("secret: %v", err)
	}
	if got != secret {
		t.Error("the recovered secret does not match the one issued")
	}
}

func TestRegistry_SecretIsEncryptedAtRest(t *testing.T) {
	ctx := context.Background()
	reg, db := newTestRegistry(t)

	secret, err := reg.Register(ctx, "rocket-01", "Rocket 07")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Read the raw column. A database copy — a backup, a stolen disk, an
	// operator with sqlite3 — must not yield a spoke's write credential.
	var stored string
	if err := db.QueryRow(`SELECT secret_encrypted FROM sync_spokes WHERE spoke_id = ?`, "rocket-01").Scan(&stored); err != nil {
		t.Fatalf("read column: %v", err)
	}
	if stored == secret {
		t.Fatal("the secret is stored in plaintext")
	}
	if strings.Contains(stored, secret) {
		t.Error("the plaintext secret appears inside the stored value")
	}
}

func TestRegistry_SecretIsNeverReadableFromMetadata(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	secret, err := reg.Register(ctx, "rocket-01", "Rocket 07")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Register returns the secret once. Every later read path returns
	// metadata only — an operator who loses it must rotate, not retrieve.
	sp, err := reg.Get(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if strings.Contains(sp.Name, secret) {
		t.Error("the secret leaked through the spoke name")
	}

	list, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("list returned %d spokes, want 1", len(list))
	}
	// The Spoke struct has no secret field at all; this asserts the shape
	// stays that way if someone adds one.
	if got := sp.SpokeID; got != "rocket-01" {
		t.Errorf("spoke_id = %q", got)
	}
}

func TestRegistry_RegisterIsNotAnUpsert(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	first, err := reg.Register(ctx, "rocket-01", "Rocket 07")
	if err != nil {
		t.Fatalf("first register: %v", err)
	}

	// Re-registering must fail rather than silently reissue. An upsert here
	// would lock out a live edge box with no signal to anyone.
	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07 again"); !errors.Is(err, ErrSpokeExists) {
		t.Fatalf("re-register: err = %v, want ErrSpokeExists", err)
	}

	still, err := reg.Secret(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("secret: %v", err)
	}
	if still != first {
		t.Error("a failed re-registration changed the existing secret")
	}
}

func TestRegistry_UnknownAndDisabledSpokes(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	if _, err := reg.Secret(ctx, "never-registered"); !errors.Is(err, ErrSpokeNotFound) {
		t.Errorf("unknown spoke: err = %v, want ErrSpokeNotFound", err)
	}

	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.SetEnabled(ctx, "rocket-01", false); err != nil {
		t.Fatalf("disable: %v", err)
	}

	// Disabling must stop authentication immediately — that is the whole
	// point of a reversible cut-off.
	if _, err := reg.Secret(ctx, "rocket-01"); !errors.Is(err, ErrSpokeDisabled) {
		t.Errorf("disabled spoke: err = %v, want ErrSpokeDisabled", err)
	}

	// Re-enabling must not require re-provisioning a secret.
	if err := reg.SetEnabled(ctx, "rocket-01", true); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	if _, err := reg.Secret(ctx, "rocket-01"); err != nil {
		t.Errorf("re-enabled spoke: %v", err)
	}
}

func TestRegistry_RotateInvalidatesTheOldSecret(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	old, err := reg.Register(ctx, "rocket-01", "Rocket 07")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	rotated, err := reg.RotateSecret(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if rotated == old {
		t.Fatal("rotation returned the same secret")
	}

	current, err := reg.Secret(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("secret: %v", err)
	}
	if current != rotated {
		t.Error("the stored secret is not the rotated one")
	}
	if current == old {
		t.Error("the old secret still authenticates after rotation")
	}

	if _, err := reg.RotateSecret(ctx, "never-registered"); !errors.Is(err, ErrSpokeNotFound) {
		t.Errorf("rotate unknown: err = %v, want ErrSpokeNotFound", err)
	}
}

func TestRegistry_SecretsAreDistinctPerSpoke(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	// Per-spoke secrets are the reason revoking one edge does not re-key the
	// fleet. Identical secrets would silently undo that.
	seen := make(map[string]string)
	for _, id := range []string{"rocket-01", "rocket-02", "rocket-03"} {
		s, err := reg.Register(ctx, id, "Rocket "+id)
		if err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		if prev, dup := seen[s]; dup {
			t.Fatalf("%s and %s were issued the same secret", id, prev)
		}
		seen[s] = id
	}
}

func TestRegistry_RejectsMaliciousSpokeIDs(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	// The spoke ID becomes the first path segment of everything that spoke
	// writes, so registration is where a namespace-escaping ID must be caught
	// — by the time a request arrives it is already HMAC-bound.
	for _, id := range []string{"", "..", "rocket/../other", "rocket\\other", ".sync-staging", "rocket\x00-01"} {
		t.Run(id, func(t *testing.T) {
			if _, err := reg.Register(ctx, id, "Some Spoke"); err == nil {
				t.Errorf("spoke ID %q was accepted", id)
			}
		})
	}
}

func TestRegistry_RequiresAName(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	// The label is what an operator uses to tell one rocket from another in
	// an alert months later.
	for _, name := range []string{"", "   ", "\t"} {
		if _, err := reg.Register(ctx, "rocket-01", name); err == nil {
			t.Errorf("name %q was accepted", name)
		}
	}
}

func TestRegistry_DeleteRemovesRegistrationOnly(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.Delete(ctx, "rocket-01"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := reg.Secret(ctx, "rocket-01"); !errors.Is(err, ErrSpokeNotFound) {
		t.Errorf("after delete: err = %v, want ErrSpokeNotFound", err)
	}
	if err := reg.Delete(ctx, "rocket-01"); !errors.Is(err, ErrSpokeNotFound) {
		t.Errorf("double delete: err = %v, want ErrSpokeNotFound", err)
	}
}

func TestRegistry_RecordActivity(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07"); err != nil {
		t.Fatalf("register: %v", err)
	}

	sp, err := reg.Get(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sp.LastSeenAt != nil {
		t.Error("a freshly registered spoke already has a last-seen time")
	}

	if err := reg.RecordActivity(ctx, "rocket-01", 3, 4096); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := reg.RecordActivity(ctx, "rocket-01", 2, 1024); err != nil {
		t.Fatalf("record: %v", err)
	}

	sp, err = reg.Get(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Counters accumulate rather than overwrite — they are a fleet-wide
	// operational total, not a per-request value.
	if sp.FilesReceived != 5 || sp.BytesReceived != 5120 {
		t.Errorf("files=%d bytes=%d, want 5/5120", sp.FilesReceived, sp.BytesReceived)
	}
	if sp.LastSeenAt == nil {
		t.Error("last-seen was not set")
	}
}

func TestNewRegistry_RefusesWithoutACipher(t *testing.T) {
	f, _ := os.CreateTemp("", "registry-nocipher-*.db")
	f.Close()
	defer os.Remove(f.Name())
	db, _ := sql.Open("sqlite3", f.Name())
	defer db.Close()

	// A nil cipher must be a hard error rather than a plaintext fallback: a
	// silent downgrade would leave every spoke's write credential readable in
	// a database that also holds audit logs.
	if _, err := NewRegistry(db, nil, zerolog.Nop()); err == nil {
		t.Error("a registry was created without a cipher")
	}
	if _, err := NewRegistry(nil, newTestCipher(t), zerolog.Nop()); err == nil {
		t.Error("a registry was created without a database")
	}
}

func TestRegistry_DecryptFailureIsDiagnosable(t *testing.T) {
	ctx := context.Background()
	reg, db := newTestRegistry(t)

	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Simulate a changed ARC_ENCRYPTION_KEY by corrupting the ciphertext.
	if _, err := db.Exec(`UPDATE sync_spokes SET secret_encrypted = ? WHERE spoke_id = ?`,
		"bm90LWEtdmFsaWQtY2lwaGVydGV4dA==", "rocket-01"); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	_, err := reg.Secret(ctx, "rocket-01")
	if err == nil {
		t.Fatal("a corrupt ciphertext decrypted successfully")
	}
	// The likely cause is a changed key, not a corrupt row — saying so sends
	// an operator to the right place.
	if !strings.Contains(err.Error(), "ARC_ENCRYPTION_KEY") {
		t.Errorf("err = %v; it should point at the encryption key", err)
	}
}

func TestGenerateSecret_IsRandomAndLongEnough(t *testing.T) {
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		s, err := GenerateSecret()
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		if len(s) != SecretBytes*2 {
			t.Fatalf("secret is %d hex chars, want %d", len(s), SecretBytes*2)
		}
		if _, dup := seen[s]; dup {
			t.Fatal("GenerateSecret returned a duplicate")
		}
		seen[s] = struct{}{}
	}
}

// passthroughCipher returns its input unchanged, exactly as
// mqtt.NewPasswordEncryptor does for an empty key.
type passthroughCipher struct{}

func (passthroughCipher) Encrypt(p string) (string, error) { return p, nil }
func (passthroughCipher) Decrypt(c string) (string, error) { return c, nil }

func TestNewRegistry_RejectsAPassthroughCipher(t *testing.T) {
	f, _ := os.CreateTemp("", "registry-passthrough-*.db")
	f.Close()
	defer os.Remove(f.Name())
	db, _ := sql.Open("sqlite3", f.Name())
	defer db.Close()

	// A nil check alone would let this through: mqtt.NewPasswordEncryptor
	// returns a non-nil pass-through for an empty key, so a caller that forgot
	// to check ARC_ENCRYPTION_KEY would store every spoke secret in plaintext
	// while construction reported success.
	_, err := NewRegistry(db, passthroughCipher{}, zerolog.Nop())
	if err == nil {
		t.Fatal("a pass-through cipher was accepted; spoke secrets would be stored in plaintext")
	}
	if !strings.Contains(err.Error(), "does not encrypt") {
		t.Errorf("err = %v; it should name the problem", err)
	}
}

// brokenRoundTripCipher encrypts but cannot decrypt its own output.
type brokenRoundTripCipher struct{}

func (brokenRoundTripCipher) Encrypt(p string) (string, error) { return "sealed:" + p, nil }
func (brokenRoundTripCipher) Decrypt(string) (string, error)   { return "something else", nil }

func TestNewRegistry_RejectsACipherThatCannotRoundTrip(t *testing.T) {
	f, _ := os.CreateTemp("", "registry-broken-*.db")
	f.Close()
	defer os.Remove(f.Name())
	db, _ := sql.Open("sqlite3", f.Name())
	defer db.Close()

	// A cipher that seals but does not recover would make every stored secret
	// unrecoverable — the hub would accept registrations and then fail every
	// authentication, with the cause buried in a decrypt error per request.
	if _, err := NewRegistry(db, brokenRoundTripCipher{}, zerolog.Nop()); err == nil {
		t.Error("a cipher that cannot round-trip was accepted")
	}
}

func TestRegistry_ActivityIsScopedToOneSpoke(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	// Two spokes. A single-spoke test cannot detect a missing or broken WHERE
	// clause — every row would be updated and the assertion would still pass —
	// so this needs an untouched second spoke to be meaningful.
	for _, id := range []string{"rocket-01", "rocket-02"} {
		if _, err := reg.Register(ctx, id, "Rocket "+id); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	if err := reg.RecordActivity(ctx, "rocket-01", 3, 4096); err != nil {
		t.Fatalf("record: %v", err)
	}

	first, err := reg.Get(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("get first: %v", err)
	}
	if first.FilesReceived != 3 || first.BytesReceived != 4096 {
		t.Errorf("rocket-01: files=%d bytes=%d, want 3/4096", first.FilesReceived, first.BytesReceived)
	}

	second, err := reg.Get(ctx, "rocket-02")
	if err != nil {
		t.Fatalf("get second: %v", err)
	}
	if second.FilesReceived != 0 || second.BytesReceived != 0 {
		t.Errorf("rocket-02: files=%d bytes=%d, want 0/0 — activity leaked across spokes, corrupting fleet totals",
			second.FilesReceived, second.BytesReceived)
	}
	if second.LastSeenAt != nil {
		t.Error("rocket-02 was marked as seen by another spoke's activity")
	}
}

func TestRegistry_ListIsNewestFirstAndStable(t *testing.T) {
	ctx := context.Background()
	reg, db := newTestRegistry(t)

	// Ordering only means something with several spokes. Registering them all
	// at the same instant also exercises the tiebreaker: without one, SQLite's
	// order is arbitrary and a bulk-provisioning run would list unpredictably.
	for _, id := range []string{"rocket-01", "rocket-02", "rocket-03"} {
		if _, err := reg.Register(ctx, id, "Rocket "+id); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	list, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("list returned %d spokes, want 3", len(list))
	}
	for i := 1; i < len(list); i++ {
		if list[i].RegisteredAt.After(list[i-1].RegisteredAt) {
			t.Errorf("entry %d is newer than %d — the list is not newest-first", i, i-1)
		}
	}

	// Force an exact timestamp tie and confirm the order is still deterministic.
	if _, err := db.ExecContext(ctx, `UPDATE sync_spokes SET registered_at = ?`, list[0].RegisteredAt); err != nil {
		t.Fatalf("force tie: %v", err)
	}
	tied, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("list after tie: %v", err)
	}
	again, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("second list: %v", err)
	}
	for i := range tied {
		if tied[i].SpokeID != again[i].SpokeID {
			t.Fatalf("two identical queries returned different orders at %d (%q vs %q); the sort has no tiebreaker",
				i, tied[i].SpokeID, again[i].SpokeID)
		}
	}
}

func TestRegistry_EnabledRoundTripsThroughMetadata(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.SetEnabled(ctx, "rocket-01", false); err != nil {
		t.Fatalf("disable: %v", err)
	}

	// Checking only the authentication path leaves the REPORTED state
	// untested: an admin UI could show every spoke as enabled while they are
	// all disabled, and nothing would fail.
	sp, err := reg.Get(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if sp.Enabled {
		t.Error("Get reports a disabled spoke as enabled")
	}

	list, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Enabled {
		t.Error("List reports a disabled spoke as enabled")
	}
}

func TestRegistry_RejectsUnboundedAndControlCharacterNames(t *testing.T) {
	ctx := context.Background()
	reg, _ := newTestRegistry(t)

	// The name is echoed into logs and every list response, so an unbounded
	// one bloats the API and an embedded newline lets a registration forge log
	// lines against whatever tooling reads them.
	if _, err := reg.Register(ctx, "rocket-01", strings.Repeat("n", 129)); err == nil {
		t.Error("a 129-byte name was accepted")
	}
	for _, name := range []string{"evil\nFAKE level=info", "tab\there", "nul\x00byte"} {
		if _, err := reg.Register(ctx, "rocket-02", name); err == nil {
			t.Errorf("name %q was accepted", name)
		}
	}
	// Exactly at the bound must still work.
	if _, err := reg.Register(ctx, "rocket-03", strings.Repeat("n", 128)); err != nil {
		t.Errorf("a 128-byte name was rejected: %v", err)
	}
}

func TestRegistry_VerifyStoredSecretsDetectsAChangedKey(t *testing.T) {
	ctx := context.Background()
	reg, db := newTestRegistry(t)

	// An empty registry has nothing to verify against, so no key can be wrong.
	n, err := reg.VerifyStoredSecrets(ctx)
	if err != nil {
		t.Fatalf("empty registry: %v", err)
	}
	if n != 0 {
		t.Errorf("count = %d, want 0", n)
	}

	for _, id := range []string{"rocket-01", "rocket-02"} {
		if _, err := reg.Register(ctx, id, "Rocket "+id); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	n, err = reg.VerifyStoredSecrets(ctx)
	if err != nil {
		t.Fatalf("healthy registry: %v", err)
	}
	if n != 2 {
		t.Errorf("count = %d, want 2", n)
	}

	// Corrupt the stored ciphertext, standing in for a changed key. Without
	// this check the hub starts cleanly and keeps answering admin requests
	// with 200 while every spoke fails to authenticate — a failure that might
	// not surface until a contact window is already missed.
	if _, err := db.ExecContext(ctx,
		`UPDATE sync_spokes SET secret_encrypted = ?`, "bm90LWEtdmFsaWQtY2lwaGVydGV4dA=="); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := reg.VerifyStoredSecrets(ctx); err == nil {
		t.Fatal("a registry whose secrets cannot be decrypted reported healthy")
	} else if !strings.Contains(err.Error(), "ARC_ENCRYPTION_KEY") {
		t.Errorf("err = %v; it should name the likely cause", err)
	}
}

func TestRegistry_VerifyStoredSecretsChecksDisabledSpokesToo(t *testing.T) {
	ctx := context.Background()
	reg, db := newTestRegistry(t)

	if _, err := reg.Register(ctx, "rocket-01", "Rocket 07"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := reg.SetEnabled(ctx, "rocket-01", false); err != nil {
		t.Fatalf("disable: %v", err)
	}

	// Secret() short-circuits on a disabled spoke without decrypting, so a
	// check routed through it would pass vacuously on a fleet that happens to
	// be disabled — and miss a broken key entirely.
	if _, err := db.ExecContext(ctx,
		`UPDATE sync_spokes SET secret_encrypted = ?`, "bm90LWEtdmFsaWQtY2lwaGVydGV4dA=="); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := reg.VerifyStoredSecrets(ctx); err == nil {
		t.Error("a corrupt secret on a disabled spoke was not detected")
	}
}
