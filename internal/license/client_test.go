package license

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func assertRuntimeGates(t *testing.T, c *Client, want bool) {
	t.Helper()
	for name, gate := range map[string]func() bool{
		"valid": c.IsValid, "enterprise": c.IsEnterprise,
		"cq": c.CanUseCQScheduler, "retention": c.CanUseRetentionScheduler,
		"edge_sync": c.CanUseEdgeSyncScheduler, "tiering": c.CanUseTieredStorage,
		"clustering": c.CanUseClustering, "audit": c.CanUseAuditLogging,
		"failover": c.CanUseWriterFailover, "governance": c.CanUseQueryGovernance,
		"management": c.CanUseQueryManagement, "arcx": c.CanUseArcx,
		"multiwriter": c.CanUseSharedStorageMultiWriter,
	} {
		if got := gate(); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// runtimePair grants every Client gate so a false assertion cannot pass merely
// because the test license never carried the feature in the first place.
func runtimePair(t *testing.T, c *Client, expires time.Time) (string, string) {
	t.Helper()
	// A trial: it ends exactly at its signed expiry, which these tests pin.
	return signDetached(t, rsaKey1, &SignedLicense{
		Version: 1, LicenseKey: c.licenseKey, Tier: TierEnterprise, CustomerID: TrialCustomerPrefix + "runtime",
		MachineFingerprint: c.fingerprint, ExpiresAt: expires,
		Features: []string{FeatureTieredStorage, FeatureClustering, FeatureAuditLogging,
			FeatureWriterFailover, FeatureQueryGovernance, FeatureQueryManagement,
			FeatureArcx, FeatureSharedStorageMultiWriter},
	})
}

func seedRuntimeLicense(t *testing.T, c *Client) []byte {
	t.Helper()
	lf, sig := runtimePair(t, c, time.Now().Add(time.Hour))
	c.saveCache(lf, sig)
	if _, err := c.LoadCachedLicense(); err != nil {
		t.Fatal(err)
	}
	assertRuntimeGates(t, c, true)
	data, err := os.ReadFile(c.cachePath())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func periodicRuntimeCheck(t *testing.T, c *Client) {
	t.Helper()
	finished := make(chan struct{})
	checked := make(chan struct{})
	var once sync.Once
	c.logger = c.logger.Hook(zerolog.HookFunc(func(_ *zerolog.Event, _ zerolog.Level, message string) {
		if message == "Stopping periodic license validation" {
			close(finished)
		}
		if message == "Periodic license validation failed" || message == "License rejected; enterprise feature gates disabled" || message == "License verified successfully" || message == "License activated successfully" {
			once.Do(func() { close(checked) })
		}
	}))
	// Nop disables hooks as well; enable Debug so the stop observation runs.
	c.logger = c.logger.Level(zerolog.DebugLevel)
	c.StartPeriodicValidation(5 * time.Millisecond)
	t.Cleanup(func() {
		c.Stop()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("periodic worker did not finish before test globals are restored")
		}
	})
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("periodic validation did not finish an attempt")
	}
}

func TestRuntimePeriodicDefinitiveRejection(t *testing.T) {
	for _, reason := range []string{"license has expired", "license is suspended", "license not found", "license is not active"} {
		t.Run(reason, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(VerifyResponse{Valid: false, Error: reason})
			}))
			defer ts.Close()
			c := newTestClient(t, ts.URL, t.TempDir())
			seedRuntimeLicense(t, c)
			periodicRuntimeCheck(t, c)
			deadline := time.Now().Add(time.Second)
			for c.IsValid() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			assertRuntimeGates(t, c, false)
			// The cache operation follows clearing the pointer; wait for both.
			for time.Now().Before(deadline) {
				if _, err := os.Stat(c.cachePath()); os.IsNotExist(err) {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Error("definitively rejected license cache remains on disk")
		})
	}
}

func TestRuntimeTransientVerdictsPreserveLicenseAndCache(t *testing.T) {
	for _, body := range []string{"503", "{}", "null", `{"message":"proxy"}`, `{"valid":null}`, `{"valid":"false"}`, "timeout", "refused"} {
		t.Run(body, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch body {
				case "503":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "timeout":
					<-r.Context().Done()
				default:
					fmt.Fprint(w, body)
				}
			}))
			defer ts.Close()
			c := newTestClient(t, ts.URL, t.TempDir())
			before := seedRuntimeLicense(t, c)
			if body == "timeout" {
				c.httpClient.Timeout = 20 * time.Millisecond
			}
			if body == "refused" {
				ts.Close()
			}
			_, err := c.ActivateOrVerify(context.Background())
			if err == nil || FailureClassOf(err) != ClassTransient {
				t.Fatalf("want transient, got %v", err)
			}
			assertRuntimeGates(t, c, true)
			periodicRuntimeCheck(t, c)
			assertRuntimeGates(t, c, true)
			after, err := os.ReadFile(c.cachePath())
			if err != nil || string(after) != string(before) {
				t.Fatalf("transient failure changed cache: %v", err)
			}
		})
	}
}

func TestRuntimeSignedExpiryWithoutRevalidation(t *testing.T) {
	var requests atomic.Int32
	var lf, sig string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig})
	}))
	defer ts.Close()
	c := newTestClient(t, ts.URL, "")
	expires := time.Now().Add(2 * time.Second)
	lf, sig = runtimePair(t, c, expires)
	if _, err := c.ActivateOrVerify(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRuntimeGates(t, c, true)
	time.Sleep(time.Until(expires) + time.Millisecond)
	assertRuntimeGates(t, c, false)
	if requests.Load() != 1 {
		t.Fatal("expiry needed another server request")
	}
	if health := c.HealthStatus(); health.Status != "expired" || health.DaysRemaining != 0 {
		t.Fatalf("health disagrees with expiry: %+v", health)
	}
}

func TestRuntimeReactivationFinalOutcome(t *testing.T) {
	for _, reason := range []string{"machine not activated for this license", "activation has been revoked"} {
		for _, outcome := range []string{"success", "503", "{}", "null", `{"success":null}`, `{"success":"false"}`, `{"success":false,"error":"license is not active"}`, `{"success":false}`} {
			t.Run(reason+"/"+outcome, func(t *testing.T) {
				var activations atomic.Int32
				var lf, sig string
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/api/v1/verify" {
						_ = json.NewEncoder(w).Encode(VerifyResponse{Error: reason})
						return
					}
					activations.Add(1)
					switch outcome {
					case "success":
						_ = json.NewEncoder(w).Encode(ActivateResponse{Success: true, LicenseFile: lf, LicenseSignature: sig})
					case "503":
						w.WriteHeader(http.StatusServiceUnavailable)
					default:
						fmt.Fprint(w, outcome)
					}
				}))
				defer ts.Close()
				c := newTestClient(t, ts.URL, t.TempDir())
				before := seedRuntimeLicense(t, c)
				lf, sig = runtimePair(t, c, time.Now().Add(2*time.Hour))
				_, err := c.ActivateOrVerify(context.Background())
				if activations.Load() != 1 {
					t.Fatal("frozen rejection did not reactivate exactly once")
				}
				switch outcome {
				case "success":
					if err != nil {
						t.Fatal(err)
					}
					assertRuntimeGates(t, c, true)
					after, err := os.ReadFile(c.cachePath())
					if err != nil || string(after) == string(before) {
						t.Fatalf("success did not refresh cache: %v", err)
					}
				case `{"success":false,"error":"license is not active"}`, `{"success":false}`:
					if err == nil || FailureClassOf(err) != ClassDefinitive {
						t.Fatalf("want definitive, got %v", err)
					}
					assertRuntimeGates(t, c, false)
					if _, err := os.Stat(c.cachePath()); !os.IsNotExist(err) {
						t.Fatalf("cache survived definitive activation: %v", err)
					}
				default:
					if err == nil || FailureClassOf(err) != ClassTransient {
						t.Fatalf("want transient, got %v", err)
					}
					assertRuntimeGates(t, c, true)
					after, err := os.ReadFile(c.cachePath())
					if err != nil || string(after) != string(before) {
						t.Fatalf("transient activation changed cache: %v", err)
					}
				}
			})
		}
	}
}

func TestRuntimeBootRejectionCannotResurrectCache(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"valid":false,"error":"license is suspended"}`)
	}))
	defer ts.Close()
	c := newTestClient(t, ts.URL, t.TempDir())
	seedRuntimeLicense(t, c)
	if _, _, err := c.ActivateOrVerifyResilient(context.Background()); err == nil || FailureClassOf(err) != ClassDefinitive {
		t.Fatalf("want definitive boot rejection: %v", err)
	}
	ts.Close()
	restarted := newTestClient(t, ts.URL, c.cacheDir)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, _, err := restarted.ActivateOrVerifyResilient(ctx); err == nil {
		t.Fatal("outage restart resurrected rejected cache")
	}
	assertRuntimeGates(t, restarted, false)
}

func TestRuntimeCacheRemovalFailureStillInvalidates(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"valid":false,"error":"license not found"}`)
	}))
	defer ts.Close()
	c := newTestClient(t, ts.URL, t.TempDir())
	seedRuntimeLicense(t, c)
	if err := os.Remove(c.cachePath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(c.cachePath(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.cachePath()+"/block-removal", nil, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := c.ActivateOrVerify(context.Background())
	if err == nil || FailureClassOf(err) != ClassDefinitive {
		t.Fatalf("deletion changed rejection: %v", err)
	}
	assertRuntimeGates(t, c, false)
}

func TestRuntimeRejectionThenRecovery(t *testing.T) {
	var recovered atomic.Bool
	var lf, sig string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !recovered.Load() {
			fmt.Fprint(w, `{"valid":false,"error":"license not found"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig})
	}))
	defer ts.Close()
	c := newTestClient(t, ts.URL, t.TempDir())
	seedRuntimeLicense(t, c)
	if _, err := c.ActivateOrVerify(context.Background()); err == nil {
		t.Fatal("expected rejection")
	}
	assertRuntimeGates(t, c, false)
	lf, sig = runtimePair(t, c, time.Now().Add(time.Hour))
	recovered.Store(true)
	if _, err := c.ActivateOrVerify(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRuntimeGates(t, c, true)
	if _, err := os.Stat(c.cachePath()); err != nil {
		t.Fatal("recovered license not cached", err)
	}
}

func TestNewClient(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name      string
		cfg       *ClientConfig
		wantErr   bool
		errSubstr string
	}{
		{
			name: "valid config",
			cfg: &ClientConfig{
				LicenseKey: "ARC-ENT-1234-5678-90AB-CDEF",
				Logger:     logger,
			},
			wantErr: false,
		},
		{
			name: "missing license key",
			cfg: &ClientConfig{
				Logger: logger,
			},
			wantErr:   true,
			errSubstr: "license key is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if client == nil {
				t.Error("expected client, got nil")
			}
			if client.fingerprint == "" {
				t.Error("fingerprint should be set")
			}
		})
	}
}

func TestClient_GetFingerprint(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		LicenseKey: "ARC-ENT-1234-5678-90AB-CDEF",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	fp := client.GetFingerprint()
	if fp == "" {
		t.Error("fingerprint should not be empty")
	}
	if !ValidateFingerprint(fp) {
		t.Error("fingerprint should be valid")
	}
}

func TestClient_GetLicense_BeforeVerify(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		LicenseKey: "ARC-ENT-1234-5678-90AB-CDEF",
		Logger:     zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	license := client.GetLicense()
	if license != nil {
		t.Error("license should be nil before verify")
	}

	if client.IsValid() {
		t.Error("client should not be valid before verify")
	}
	if client.IsEnterprise() {
		t.Error("client should not be enterprise before verify")
	}
}

// Note: Tests that require a mock HTTP server (TestClient_Verify_*) have been removed
// because the license server URL is hardcoded to enterprise.basekick.net.
// Integration tests against the real server should be run separately.
