package license

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// writeOfflineFile marshals an OfflineLicenseFile to a temp path and returns it.
func writeOfflineFile(t *testing.T, lf OfflineLicenseFile) string {
	t.Helper()
	data, err := json.Marshal(lf)
	if err != nil {
		t.Fatalf("marshal offline file: %v", err)
	}
	path := filepath.Join(t.TempDir(), "license.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write offline file: %v", err)
	}
	return path
}

// TestLoadOfflineLicense_GoldenPath: a file signed by the trusted key, with the
// trusted key pinned, loads and yields an active enterprise license — no network.
func TestLoadOfflineLicense_GoldenPath(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)

	lic := sampleLicense()
	// Make sure it's unexpired relative to now so status is active.
	lic.IssuedAt = time.Now().UTC().Add(-24 * time.Hour)
	lic.ExpiresAt = time.Now().UTC().Add(365 * 24 * time.Hour)
	fileB64, sigB64 := signDetached(t, rsaKey1, lic)

	path := writeOfflineFile(t, OfflineLicenseFile{LicenseFile: fileB64, LicenseSignature: sigB64})

	c, err := LoadOfflineLicense(path, zerolog.Nop())
	if err != nil {
		t.Fatalf("LoadOfflineLicense: unexpected error: %v", err)
	}
	if !c.offline {
		t.Error("expected offline=true")
	}
	got := c.GetLicense()
	if got == nil {
		t.Fatal("expected a loaded license")
	}
	if got.Tier != "enterprise" {
		t.Errorf("tier = %q, want enterprise", got.Tier)
	}
	if got.Status != "active" {
		t.Errorf("status = %q, want active", got.Status)
	}
	// Offline client must never reach the network: ActivateOrVerify returns the
	// loaded license, and Activate/Verify are rejected.
	if _, err := c.ActivateOrVerify(t.Context()); err != nil {
		t.Errorf("ActivateOrVerify (offline) should succeed locally, got: %v", err)
	}
	if _, err := c.Verify(t.Context()); err == nil {
		t.Error("Verify on an offline client should error (no network)")
	}
	if _, err := c.Activate(t.Context()); err == nil {
		t.Error("Activate on an offline client should error (no network)")
	}
}

// TestLoadOfflineLicense_Rejects covers the fail-closed cases: any verification
// problem must return an error (caller disables enterprise features).
func TestLoadOfflineLicense_Rejects(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)

	validFile, validSig := signDetached(t, rsaKey1, sampleLicense())

	t.Run("tampered payload", func(t *testing.T) {
		// Re-sign-free tamper: flip a byte of the payload after signing.
		tampered := []byte(validFile)
		tampered[len(tampered)/2] ^= 0xFF
		path := writeOfflineFile(t, OfflineLicenseFile{LicenseFile: string(tampered), LicenseSignature: validSig})
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error on tampered payload")
		}
	})

	t.Run("wrong signing key", func(t *testing.T) {
		// Signed by rsaKey2 but rsaKey1 is pinned.
		f, s := signDetached(t, rsaKey2, sampleLicense())
		path := writeOfflineFile(t, OfflineLicenseFile{LicenseFile: f, LicenseSignature: s})
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error when signed by an untrusted key")
		}
	})

	t.Run("missing signature field", func(t *testing.T) {
		path := writeOfflineFile(t, OfflineLicenseFile{LicenseFile: validFile})
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error when license_signature is empty")
		}
	})

	t.Run("missing license_file field", func(t *testing.T) {
		path := writeOfflineFile(t, OfflineLicenseFile{LicenseSignature: validSig})
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error when license_file is empty")
		}
	})

	t.Run("not valid json", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "garbage.json")
		if err := os.WriteFile(path, []byte("not json at all"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error on non-JSON file")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "does-not-exist.json")
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error on missing file")
		}
	})

	t.Run("oversized file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "big.json")
		big := make([]byte, maxOfflineLicenseBytes+10)
		for i := range big {
			big[i] = 'a'
		}
		if err := os.WriteFile(path, big, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadOfflineLicense(path, zerolog.Nop()); err == nil {
			t.Error("expected error on oversized file")
		}
	})
}

// TestLoadOfflineLicense_Expired: a correctly-signed but expired license loads
// (signature is valid) but its status is "expired" so feature gates fall closed.
func TestLoadOfflineLicense_Expired(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)

	lic := sampleLicense()
	lic.IssuedAt = time.Now().UTC().Add(-400 * 24 * time.Hour)
	lic.ExpiresAt = time.Now().UTC().Add(-24 * time.Hour) // expired yesterday
	f, s := signDetached(t, rsaKey1, lic)
	path := writeOfflineFile(t, OfflineLicenseFile{LicenseFile: f, LicenseSignature: s})

	c, err := LoadOfflineLicense(path, zerolog.Nop())
	if err != nil {
		t.Fatalf("expected expired license to load (valid signature), got error: %v", err)
	}
	got := c.GetLicense()
	if got.Status == "active" {
		t.Errorf("expired license should not have active status, got %q", got.Status)
	}
	// Feature gates must be closed for an expired license.
	if c.CanUseClustering() {
		t.Error("expired license must not grant clustering")
	}
}

func TestLoadOfflineLicense_PathMessageMentionsFile(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	_, err := LoadOfflineLicense(filepath.Join(t.TempDir(), "nope.json"), zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "nope.json") {
		t.Errorf("error should name the path, got: %v", err)
	}
}
