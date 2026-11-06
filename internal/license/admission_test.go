package license

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func admissionResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func admissionClient(t *testing.T, policy *ExpiryPolicy) *Client {
	t.Helper()
	c, err := NewClient(&ClientConfig{LicenseKey: "ARC-ADMISSION-TEST", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	if policy == nil {
		policy = &ExpiryPolicy{Version: 1, Kind: "trial", TermEndsAt: time.Now().UTC().Add(time.Hour)}
	}
	c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
		lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: c.licenseKey,
			Tier: TierEnterprise, MachineFingerprint: c.fingerprint, Policy: policy, ExpiresAt: policy.cutoff(),
			Features: []string{FeatureClustering, FeatureCQScheduler, FeatureRetentionScheduler, FeatureEdgeSyncScheduler,
				FeatureRBAC, FeatureTieredStorage, FeatureAuditLogging, FeatureWriterFailover, FeatureQueryGovernance,
				FeatureQueryManagement, FeatureArcx, FeatureSharedStorageMultiWriter}})
		payload, _ := base64.StdEncoding.DecodeString(lf)
		keyHash, payloadHash := sha256.Sum256([]byte(c.licenseKey)), sha256.Sum256(payload)
		proof, err := json.Marshal(AuthorityTime{Version: 1, Purpose: "arc-license-time", Nonce: r.URL.Query().Get("time_nonce"),
			LicenseKeySHA256: hex.EncodeToString(keyHash[:]), MachineFingerprint: c.fingerprint,
			LicenseSHA256: hex.EncodeToString(payloadHash[:]), ServerTime: time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		tf, ts := signDetachedRaw(t, rsaKey1, proof)
		body, err := json.Marshal(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig, TimeFile: tf, TimeSignature: ts})
		if err != nil {
			t.Fatal(err)
		}
		return admissionResponse(http.StatusOK, string(body)), nil
	})
	if _, err := c.ActivateOrVerify(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

func assertAdmissionGates(t *testing.T, c *Client, want bool) {
	t.Helper()
	for name, gate := range map[string]func() bool{
		"valid": c.IsValid, "enterprise": c.IsEnterprise, "serving": c.ServingAllowed,
		"clustering": c.CanUseClustering, "cq": c.CanUseCQScheduler, "retention": c.CanUseRetentionScheduler,
		"edge": c.CanUseEdgeSyncScheduler, "rbac": c.CanManageRBAC, "tiering": c.CanUseTieredStorage,
		"migration": c.CanMigrateTieredStorage, "audit": c.CanUseAuditLogging, "failover": c.CanUseWriterFailover,
		"governance": c.CanUseQueryGovernance, "queries": c.CanUseQueryManagement, "arcx": c.CanUseArcx,
		"multiwriter": c.CanUseSharedStorageMultiWriter,
	} {
		if got := gate(); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestAdmissionProtocolTwoClassification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   FailureClass
	}{
		{"quota", 403, `{"error":"maximum machine activations reached"}`, ClassAdmissionDenied},
		{"unknown refusal", 403, `{"error":"installation policy not recognized"}`, ClassAdmissionDenied},
		{"quota with suffix is not key rejection", 403, `{"error":"maximum machine activations reached: different policy"}`, ClassAdmissionDenied},
		{"proxy text", 403, "maximum machine activations reached", ClassTransient},
		{"malformed", 403, `{"error":"maximum machine activations reached"`, ClassTransient},
		{"oversized", 403, `{"error":"license is suspended"}` + strings.Repeat(" ", 4096), ClassTransient},
		{"rate limit", 429, `{"error":"license is suspended"}`, ClassTransient},
		{"server failure", 503, `{"error":"license is suspended"}`, ClassTransient},
		{"missing route", 404, `{"error":"unknown route"}`, ClassTransient},
		{"missing key", 404, `{"error":"license not found"}`, ClassDefinitive},
		{"suspended", 403, `{"error":"license is suspended"}`, ClassDefinitive},
		{"inactive", 403, `{"error":"license is not active"}`, ClassDefinitive},
		{"expired", 403, `{"error":"license has expired"}`, ClassDefinitive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkHTTPStatusForProtocol(admissionResponse(tc.status, tc.body), 2); err == nil || FailureClassOf(err) != tc.want {
				t.Fatalf("wrong classification: %v", err)
			}
		})
	}
	if err := checkHTTPStatusForProtocol(admissionResponse(403, `{"error":"maximum machine activations reached"}`), 1); FailureClassOf(err) != ClassDefinitive {
		t.Fatal("changed legacy protocol-1 classification")
	}
}

func TestAdmissionDenialRetainsEvidenceButFencesEveryGate(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, operation := range []string{"verify", "activate", "combined", "resilient", "boot"} {
		t.Run(operation, func(t *testing.T) {
			c := admissionClient(t, nil)
			assertAdmissionGates(t, c, true)
			freshTransport, retained := c.httpClient.Transport, c.GetLicense()
			cache, err := os.ReadFile(c.cachePath())
			if err != nil {
				t.Fatal(err)
			}
			var pair cachedPair
			if err := json.Unmarshal(cache, &pair); err != nil {
				t.Fatal(err)
			}
			calls := 0
			c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if operation != "verify" && r.Method == http.MethodGet {
					return admissionResponse(200, `{"valid":false,"error":"machine not activated for this license"}`), nil
				}
				return admissionResponse(403, `{"error":"maximum machine activations reached"}`), nil
			})
			switch operation {
			case "verify":
				_, err = c.Verify(context.Background())
			case "activate":
				_, err = c.Activate(context.Background())
			case "combined":
				_, err = c.ActivateOrVerify(context.Background())
			case "resilient":
				_, _, err = c.ActivateOrVerifyResilient(context.Background())
			case "boot":
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, _, err = c.AwaitOnlineAuthority(ctx)
			}
			if err == nil || FailureClassOf(err) != ClassAdmissionDenied || calls > 2 {
				t.Fatalf("admission did not return directly: calls=%d err=%v", calls, err)
			}
			assertAdmissionGates(t, c, false)
			if c.GetLicense() != nil || c.PendingRuntimeLicense() != nil || !c.AdmissionBlocked() {
				t.Fatal("blocked grant exposed")
			}
			if c.license != retained || c.clock.source() != "license_server" || !c.clock.matchesLicense(pair.LicenseFile) {
				t.Fatal("admission destroyed signed policy/time evidence")
			}
			after, err := os.ReadFile(c.cachePath())
			if err != nil || !bytes.Equal(cache, after) {
				t.Fatalf("admission changed cache: %v", err)
			}
			if _, err := c.LoadCachedLicense(); err == nil || FailureClassOf(err) != ClassAdmissionDenied {
				t.Fatal("direct cache load reopened denied node")
			}
			if state := c.LifecycleState(); state.Phase != PhaseAdmissionBlocked || state.Valid {
				t.Fatalf("quota became expiry: %+v", state)
			}
			if health := c.HealthStatus(); health.Status != "admission_blocked" || !health.AdmissionBlocked {
				t.Fatalf("misleading health: %+v", health)
			}
			// Subsequent network failure must not turn admission denial into the
			// generic transient cache fallback of an already anchored process.
			c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) { return admissionResponse(503, "unavailable"), nil })
			if _, _, err := c.ActivateOrVerifyResilient(context.Background()); err == nil || FailureClassOf(err) != ClassAdmissionDenied {
				t.Fatal("outage bypassed admission fence")
			}
			c.httpClient.Transport = freshTransport
			if _, err := c.ActivateOrVerify(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertAdmissionGates(t, c, true)
			if c.AdmissionBlocked() || !c.LifecycleState().Valid {
				t.Fatal("fresh proof did not restore admission")
			}
		})
	}
}

func TestAdmissionUnknownVerdictDoesNotBecomeOSS(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, priorGrant := range []bool{false, true} {
		var c *Client
		if priorGrant {
			c = admissionClient(t, nil)
		} else {
			var err error
			c, err = NewClient(&ClientConfig{LicenseKey: "ARC-UNKNOWN-TEST", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
			if err != nil {
				t.Fatal(err)
			}
			if c.LifecycleState().Phase != PhaseAuthorityUnavailable {
				t.Fatal("missing grant implied expiry")
			}
		}
		c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) {
			return admissionResponse(200, `{"valid":false,"error":"new installation rule"}`), nil
		})
		if _, err := c.ActivateOrVerify(context.Background()); err == nil || FailureClassOf(err) != ClassAdmissionDenied {
			t.Fatalf("unknown verdict was terminal: %v", err)
		}
		if c.LifecycleState().Phase != PhaseAdmissionBlocked || c.ServingAllowed() {
			t.Fatal("unknown verdict authorized service or OSS handoff")
		}
	}
}

func TestAdmissionDoesNotPostponeSignedCutoffOrDefinitiveRejection(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, kind := range []string{"trial", "paid"} {
		policy := &ExpiryPolicy{Version: 1, Kind: kind, TermEndsAt: time.Now().UTC().Add(time.Hour)}
		if kind == "paid" {
			policy.GraceDays = 14
		}
		c := admissionClient(t, policy)
		c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) {
			return admissionResponse(403, `{"error":"maximum machine activations reached"}`), nil
		})
		if _, err := c.ActivateOrVerify(context.Background()); FailureClassOf(err) != ClassAdmissionDenied {
			t.Fatal(err)
		}
		// Advance the already authenticated monotonic clock through the exact
		// signed boundary; no new authority response is needed to reach expiry.
		tick := time.Now()
		c.clock.tick = func() time.Time { return tick }
		c.clock.at, c.clock.origin = tick, policy.cutoff().Add(-time.Nanosecond)
		if c.LifecycleState().Phase != PhaseAdmissionBlocked {
			t.Fatal("early cutoff")
		}
		tick = tick.Add(time.Nanosecond)
		if state := c.LifecycleState(); state.Phase != PhaseOSS || state.Reason != "signed_cutoff" {
			t.Fatalf("%s admission masked real cutoff: %+v", kind, state)
		}
	}
	c := admissionClient(t, nil)
	c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) {
		return admissionResponse(403, `{"error":"license is suspended"}`), nil
	})
	if _, err := c.ActivateOrVerify(context.Background()); FailureClassOf(err) != ClassDefinitive {
		t.Fatal(err)
	}
	if state := c.LifecycleState(); state.Phase != PhaseOSS || state.Reason != "key_rejected" {
		t.Fatalf("lost explicit rejection: %+v", state)
	}
	if _, err := os.Stat(c.cachePath()); !os.IsNotExist(err) {
		t.Fatal("key-wide rejection retained usable cache")
	}
}

func TestAdmissionRecoveryPreservesRuntimeRebuild(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	c := admissionClient(t, nil)
	fresh := c.httpClient.Transport
	c.RequireRuntimeRestart()
	c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) {
		return admissionResponse(403, `{"error":"maximum machine activations reached"}`), nil
	})
	if _, err := c.ActivateOrVerify(context.Background()); FailureClassOf(err) != ClassAdmissionDenied {
		t.Fatal(err)
	}
	if c.PendingRuntimeLicense() != nil {
		t.Fatal("blocked pending grant escaped fence")
	}
	c.httpClient.Transport = fresh
	if _, err := c.ActivateOrVerify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.AdmissionBlocked() || c.PendingRuntimeLicense() == nil || c.GetLicense() != nil || c.ServingAllowed() {
		t.Fatal("fresh admission bypassed required runtime reconstruction")
	}
	if !c.LifecycleState().Valid {
		t.Fatal("pending verified grant unavailable to coordinated recovery")
	}
}
