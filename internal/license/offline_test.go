package license

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func writeOfflineFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "license.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestOfflineClientGrid pins the file loader's source-context rules: unbound
// site files load; bound files still bind; everything else fails closed.
func TestOfflineClientGrid(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	localFP, err := GenerateMachineFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	mint := func(fp string, d time.Duration) (string, string) {
		return signedPairKF(t, "ARC-ENT-SITE-KEY", fp, d)
	}

	t.Run("unbound site file loads with full entitlements", func(t *testing.T) {
		lf, sig := mint("", time.Hour)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`","summary":{"note":"ignored"}}`)
		c, err := NewOfflineClient(path, zerolog.Nop())
		if err != nil {
			t.Fatalf("unbound site file must load: %v", err)
		}
		lic := c.GetLicense()
		if lic.Tier != TierEnterprise || !lic.IsValid() {
			t.Fatalf("license: %+v", lic)
		}
	})

	t.Run("bound-to-this-machine file loads", func(t *testing.T) {
		lf, sig := mint(localFP, time.Hour)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`"}`)
		if _, err := NewOfflineClient(path, zerolog.Nop()); err != nil {
			t.Fatalf("bound-to-local file must load: %v", err)
		}
	})

	t.Run("bound-to-other-machine file rejected", func(t *testing.T) {
		lf, sig := mint("sha256:someoneelse", time.Hour)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`"}`)
		if _, err := NewOfflineClient(path, zerolog.Nop()); err == nil {
			t.Fatal("a pair bound to another machine must not load from file_path")
		}
	})

	t.Run("tampered signature rejected", func(t *testing.T) {
		lf, sig := mint("", time.Hour)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig[:len(sig)-4]+`AAAA"}`)
		if _, err := NewOfflineClient(path, zerolog.Nop()); err == nil {
			t.Fatal("tampered file must be rejected")
		}
	})

	t.Run("expired rejected", func(t *testing.T) {
		lf, sig := mint("", -time.Minute)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`"}`)
		if _, err := NewOfflineClient(path, zerolog.Nop()); err == nil {
			t.Fatal("expired file must be rejected")
		}
	})

	t.Run("missing file / malformed / empty fields rejected", func(t *testing.T) {
		if _, err := NewOfflineClient(filepath.Join(t.TempDir(), "nope.json"), zerolog.Nop()); err == nil {
			t.Fatal("missing file")
		}
		if _, err := NewOfflineClient(writeOfflineFile(t, "not json"), zerolog.Nop()); err == nil {
			t.Fatal("malformed")
		}
		if _, err := NewOfflineClient(writeOfflineFile(t, `{"license_file":""}`), zerolog.Nop()); err == nil {
			t.Fatal("empty fields")
		}
	})

	t.Run("periodic validation is a no-op in offline mode", func(t *testing.T) {
		lf, sig := mint("", time.Hour)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`"}`)
		c, err := NewOfflineClient(path, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		// Must not panic/spawn network activity; Stop must be clean.
		c.StartPeriodicValidation(time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		c.Stop()
	})

	t.Run("offline file rejected by the boot CACHE gates", func(t *testing.T) {
		// The two source contexts must not weaken each other: an unbound site
		// file dropped into license_cache.json fails the cache's non-empty
		// fingerprint gate.
		lf, sig := mint("", time.Hour)
		dir := t.TempDir()
		c := newTestClient(t, "http://127.0.0.1:1", dir)
		c.licenseKey = "ARC-ENT-SITE-KEY" // even with matching key...
		if err := os.WriteFile(filepath.Join(dir, cacheFileName),
			[]byte(`{"license_file":"`+lf+`","license_signature":"`+sig+`"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("unbound site file must be rejected as a cache")
		}
	})
}

// TestLicenseHealthStatus pins the /health license payload: source labels,
// read-time expired derivation (#603 lesson — health tells the truth even
// though enforcement is boot-time), and the site-license flag.
func TestLicenseHealthStatus(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)

	t.Run("offline site file reports file source + site flag", func(t *testing.T) {
		lf, sig := signedPairKF(t, "ARC-ENT-SITE-KEY", "", time.Hour)
		path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`"}`)
		c, err := NewOfflineClient(path, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		h := c.HealthStatus()
		if h.Source != "file" || !h.SiteLicense || h.Tier != string(TierEnterprise) || h.Status != "active" {
			t.Fatalf("health = %+v", h)
		}
	})

	t.Run("expired derives at read time", func(t *testing.T) {
		c := &Client{license: &License{Tier: TierEnterprise, Status: "active",
			ExpiresAt: time.Now().UTC().Add(50 * time.Millisecond)}, source: SourceCache}
		if h := c.HealthStatus(); h.Status != "active" || h.Source != "cache" {
			t.Fatalf("pre-expiry: %+v", h)
		}
		time.Sleep(60 * time.Millisecond)
		if h := c.HealthStatus(); h.Status != "expired" {
			t.Fatalf("post-expiry (same frozen license!): %+v", h)
		}
	})

	t.Run("nil license reports unlicensed", func(t *testing.T) {
		c := &Client{}
		if h := c.HealthStatus(); h.Tier != "oss" || h.Status != "unlicensed" {
			t.Fatalf("health = %+v", h)
		}
	})
}
