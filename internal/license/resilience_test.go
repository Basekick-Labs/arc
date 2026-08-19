package license

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// newTestClient builds a client against a fake server with the test keypair
// pinned and time compressed.
func newTestClient(t *testing.T, serverURL, cacheDir string) *Client {
	t.Helper()
	withTestPublicKey(t, &rsaKey1.PublicKey)
	origB, origA, origU := bootRetryPhaseBudget, bootRetryAttempts, bootRetryBackoffUnit
	bootRetryPhaseBudget, bootRetryAttempts, bootRetryBackoffUnit = 3*time.Second, 3, 5*time.Millisecond
	t.Cleanup(func() {
		bootRetryPhaseBudget, bootRetryAttempts, bootRetryBackoffUnit = origB, origA, origU
	})
	c, err := NewClient(&ClientConfig{LicenseKey: "ARC-ENT-TEST-KEY", CacheDir: cacheDir, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	c.serverURL = serverURL
	c.httpClient.Timeout = 2 * time.Second
	return c
}

// signedPairFor mints the detached pair the server would emit for this
// client's key+fingerprint, valid for `d`.
func signedPairKF(t *testing.T, key, fingerprint string, d time.Duration) (string, string) {
	t.Helper()
	return signDetached(t, rsaKey1, &SignedLicense{
		Version:            1,
		LicenseKey:         key,
		CustomerID:         "cust_test",
		CustomerName:       "Test Co",
		Tier:               TierEnterprise,
		MaxCores:           16,
		MaxMachines:        20,
		Features:           []string{"tiering"},
		MachineFingerprint: fingerprint,
		IssuedAt:           time.Now().UTC().Add(-time.Hour),
		ExpiresAt:          time.Now().UTC().Add(d),
	})
}

func signedPairFor(t *testing.T, c *Client, d time.Duration) (string, string) {
	t.Helper()
	return signedPairKF(t, c.licenseKey, c.fingerprint, d)
}

// TestFailureClassification pins the definitive-vs-transient table against
// real HTTP responses — the availability/security pivot of the boot path.
func TestFailureClassification(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		want    FailureClass
	}{
		{"500", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, ClassTransient},
		{"429 rate limited", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }, ClassTransient},
		{"404 ingress HTML (the field incident)", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			fmt.Fprint(w, "<html><body>404 page not found</body></html>")
		}, ClassTransient},
		{"404 empty body", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }, ClassTransient},
		{"404 protocol JSON (activate: license not found)", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"error":"license not found"}`)
		}, ClassDefinitive},
		{"403 protocol JSON (not active)", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(403)
			fmt.Fprint(w, `{"error":"license is not active"}`)
		}, ClassDefinitive},
		{"200 valid:false suspended", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"valid":false,"error":"license is suspended"}`)
		}, ClassDefinitive},
		{"200 valid:false unknown string defaults definitive", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"valid":false,"error":"some future rejection"}`)
		}, ClassDefinitive},
		{"200 malformed body (proxy default backend)", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "<html>default backend</html>")
		}, ClassTransient},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(tc.handler)
			defer ts.Close()
			c := newTestClient(t, ts.URL, "")
			_, err := c.Verify(context.Background())
			if err == nil {
				t.Fatal("want error")
			}
			if got := FailureClassOf(err); got != tc.want {
				t.Fatalf("class = %v, want %v (err: %v)", got, tc.want, err)
			}
		})
	}

	t.Run("transport error", func(t *testing.T) {
		c := newTestClient(t, "http://127.0.0.1:1", "")
		_, err := c.Verify(context.Background())
		if err == nil || FailureClassOf(err) != ClassTransient {
			t.Fatalf("connection refused must be transient, got %v", err)
		}
	})
}

// TestRevokedActivationReactivates pins the fix for the stale-activation
// reaper: the server revokes any activation without a heartbeat for 72h (and
// Arc never sends heartbeats), so "activation has been revoked" is ROUTINE on
// stable machines — the client must re-activate, exactly as it does for
// "machine not activated". Without this, every stable-fingerprint deployment
// crash-loops on its first restart >72h after activation.
func TestRevokedActivationReactivates(t *testing.T) {
	var activated bool
	var c *Client
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/verify"):
			fmt.Fprint(w, `{"valid":false,"error":"activation has been revoked"}`)
		case strings.HasSuffix(r.URL.Path, "/activate"):
			activated = true
			lf, sig := signedPairFor(t, c, time.Hour)
			fmt.Fprintf(w, `{"success":true,"license_file":%q,"license_signature":%q}`, lf, sig)
		}
	}))
	defer ts.Close()
	c = newTestClient(t, ts.URL, "")

	lic, err := c.ActivateOrVerify(context.Background())
	if err != nil {
		t.Fatalf("revoked activation must recover via re-activation: %v", err)
	}
	if !activated || lic.Tier != TierEnterprise {
		t.Fatalf("activate not attempted or wrong license: activated=%v", activated)
	}
}

// TestCacheGrid pins every rule of LoadCachedLicense — the rules that make a
// copied cache file useless (license-multiplication defense).
func TestCacheGrid(t *testing.T) {
	mk := func(t *testing.T) *Client {
		return newTestClient(t, "http://127.0.0.1:1", t.TempDir())
	}

	t.Run("save then load succeeds", func(t *testing.T) {
		c := mk(t)
		lf, sig := signedPairFor(t, c, time.Hour)
		c.saveCache(lf, sig)
		lic, err := c.LoadCachedLicense()
		if err != nil || lic.Tier != TierEnterprise {
			t.Fatalf("load: %v", err)
		}
	})

	t.Run("tampered pair rejected", func(t *testing.T) {
		c := mk(t)
		lf, sig := signedPairFor(t, c, time.Hour)
		c.saveCache(lf, sig[:len(sig)-4]+"AAAA")
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("tampered signature must be rejected")
		}
	})

	t.Run("expired rejected", func(t *testing.T) {
		c := mk(t)
		lf, sig := signedPairFor(t, c, -time.Minute)
		c.saveCache(lf, sig)
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("expired cache must be rejected — no grace beyond expires_at")
		}
	})

	t.Run("different key rejected and deleted", func(t *testing.T) {
		c := mk(t)
		lf, sig := signedPairKF(t, "ARC-ENT-OTHER-KEY", c.fingerprint, time.Hour)
		c.saveCache(lf, sig)
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("stale cache from a replaced key must not resurrect entitlements")
		}
		if _, err := c.LoadCachedLicense(); err == nil || !strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("mismatched cache must be deleted, second load: %v", err)
		}
	})

	t.Run("copied cache (foreign fingerprint) rejected", func(t *testing.T) {
		c := mk(t)
		lf, sig := signedPairKF(t, c.licenseKey, "sha256:someoneelsesmachine", time.Hour)
		c.saveCache(lf, sig)
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("a cache copied from another machine must be rejected")
		}
	})

	t.Run("UNBOUND pair rejected as cache", func(t *testing.T) {
		// bindFingerprint accepts empty-as-unbound for online responses; the
		// cache must NOT — an unbound blob in the cache slot would be a
		// universal license (this is also what keeps deliverable B's site
		// files out of the cache path).
		c := mk(t)
		lf, sig := signedPairKF(t, c.licenseKey, "", time.Hour)
		c.saveCache(lf, sig)
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("unbound pair must be rejected as cache")
		}
	})
}

// TestResilientBootFlows pins the boot routing: transient → retry → cache;
// definitive → OSS with the cache NEVER consulted.
func TestResilientBootFlows(t *testing.T) {
	t.Run("transient failures fall back to cache, fully licensed", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(503)
		}))
		defer ts.Close()
		c := newTestClient(t, ts.URL, t.TempDir())
		lf, sig := signedPairFor(t, c, time.Hour)
		c.saveCache(lf, sig)

		lic, src, err := c.ActivateOrVerifyResilient(context.Background())
		if err != nil {
			t.Fatalf("resilient: %v", err)
		}
		if src != SourceCache || lic.Tier != TierEnterprise {
			t.Fatalf("src=%v tier=%v", src, lic.Tier)
		}
	})

	t.Run("definitive rejection never consults cache", func(t *testing.T) {
		var verifyCalls int
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			verifyCalls++
			fmt.Fprint(w, `{"valid":false,"error":"license is suspended"}`)
		}))
		defer ts.Close()
		c := newTestClient(t, ts.URL, t.TempDir())
		lf, sig := signedPairFor(t, c, time.Hour)
		c.saveCache(lf, sig) // a valid cache exists — and must be ignored

		_, _, err := c.ActivateOrVerifyResilient(context.Background())
		if err == nil {
			t.Fatal("definitive rejection must fail to OSS despite a valid cache")
		}
		if FailureClassOf(err) != ClassDefinitive {
			t.Fatalf("class: %v", err)
		}
		if verifyCalls != 1 {
			t.Fatalf("definitive rejection must not be retried, calls=%d", verifyCalls)
		}
	})

	t.Run("transient without cache fails as today", func(t *testing.T) {
		c := newTestClient(t, "http://127.0.0.1:1", t.TempDir())
		if _, _, err := c.ActivateOrVerifyResilient(context.Background()); err == nil {
			t.Fatal("no server + no cache must fail (OSS fallback)")
		}
	})

	t.Run("server success writes cache usable on next boot", func(t *testing.T) {
		var c *Client
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lf, sig := signedPairFor(t, c, time.Hour)
			fmt.Fprintf(w, `{"valid":true,"success":true,"license_file":%q,"license_signature":%q}`, lf, sig)
		}))
		c = newTestClient(t, ts.URL, t.TempDir())
		if _, src, err := c.ActivateOrVerifyResilient(context.Background()); err != nil || src != SourceServer {
			t.Fatalf("online boot: %v src=%v", err, src)
		}
		ts.Close() // server gone
		lic, err := c.LoadCachedLicense()
		if err != nil || lic == nil {
			t.Fatalf("cache written by success must load: %v", err)
		}
	})
}
