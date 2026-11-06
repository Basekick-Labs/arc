package license

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type replacementTransport func(*http.Request) (*http.Response, error)

func (f replacementTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Seed through the production verification path, including its nonce-bound
// signed time, so preservation tests cannot pass with an untrusted cache.
func grantedProtocolTwoClient(t *testing.T, policy *ExpiryPolicy) *Client {
	t.Helper()
	c, err := NewClient(&ClientConfig{LicenseKey: "ARC-TEST-V2-CACHE", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
		now := time.Now().UTC()
		if policy == nil {
			policy = &ExpiryPolicy{Version: 1, Kind: "trial", TermEndsAt: now.Add(time.Hour)}
		}
		lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: c.licenseKey,
			Tier: TierEnterprise, Features: []string{FeatureClustering}, MachineFingerprint: c.fingerprint,
			Policy: policy, ExpiresAt: policy.cutoff()})
		payload, _ := base64.StdEncoding.DecodeString(lf)
		keyHash, payloadHash := sha256.Sum256([]byte(c.licenseKey)), sha256.Sum256(payload)
		proof, err := json.Marshal(AuthorityTime{Version: 1, Purpose: "arc-license-time", Nonce: r.URL.Query().Get("time_nonce"),
			LicenseKeySHA256: hex.EncodeToString(keyHash[:]), MachineFingerprint: c.fingerprint,
			LicenseSHA256: hex.EncodeToString(payloadHash[:]), ServerTime: now})
		if err != nil {
			t.Fatal(err)
		}
		tf, ts := signDetachedRaw(t, rsaKey1, proof)
		body, err := json.Marshal(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig, TimeFile: tf, TimeSignature: ts})
		if err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	if _, err := c.ActivateOrVerify(context.Background()); err != nil || !c.CanUseClustering() {
		t.Fatalf("failed to establish fresh licensed runtime: %v", err)
	}
	return c
}

func TestProtocolTwoMissingRoutePreservesFreshGrantAndCache(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, operation := range []string{"verify", "activate", "activate_or_verify"} {
		for _, body := range []string{`{"error":"Cannot GET /api/v2/verify"}`, `{"error":"Cannot POST /api/v2/activate"}`, `{"error":"unknown route"}`, "unknown route", `<html>Not Found</html>`} {
			t.Run(operation+"/"+body, func(t *testing.T) {
				c := grantedProtocolTwoClient(t, nil)
				before := c.GetLicense()
				cache, err := os.ReadFile(c.cachePath())
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
					calls++
					if !strings.HasPrefix(r.URL.Path, "/api/v2/") {
						t.Errorf("unexpected legacy fallback: %s", r.URL.Path)
					}
					return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				switch operation {
				case "verify":
					_, err = c.Verify(context.Background())
				case "activate":
					_, err = c.Activate(context.Background())
				default:
					_, err = c.ActivateOrVerify(context.Background())
				}
				if err == nil || FailureClassOf(err) != ClassTransient || calls != 1 {
					t.Errorf("missing v2 route must be transient without fallback: calls=%d error=%v", calls, err)
				}
				after, readErr := os.ReadFile(c.cachePath())
				if c.GetLicense() != before || !c.CanUseClustering() || readErr != nil || !bytes.Equal(cache, after) {
					t.Fatalf("missing route changed entitlement/cache: readError=%v", readErr)
				}
				if _, err := c.LoadCachedLicense(); err != nil {
					t.Fatalf("missing route invalidated the existing authority anchor: %v", err)
				}
			})
		}
	}
}

func TestProtocolTwoExplicitRejectionsRemainDefinitive(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, activation := range []bool{false, true} {
		t.Run(fmt.Sprintf("activation=%v", activation), func(t *testing.T) {
			c := grantedProtocolTwoClient(t, nil)
			calls := 0
			c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				status, body := http.StatusOK, `{"valid":false,"error":"license not found"}`
				if activation {
					if calls == 1 {
						body = `{"valid":false,"error":"machine not activated for this license"}`
					} else {
						if r.Method != http.MethodPost || r.URL.Path != "/api/v2/activate" {
							t.Fatalf("unexpected activation request: %s %s", r.Method, r.URL.Path)
						}
						status, body = http.StatusNotFound, `{"error":"license not found"}`
					}
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			_, err := c.ActivateOrVerify(context.Background())
			if err == nil || FailureClassOf(err) != ClassDefinitive || c.GetLicense() != nil || c.CanUseClustering() {
				t.Fatalf("explicit rejection did not revoke entitlement: %v", err)
			}
			if _, err := os.Stat(c.cachePath()); !os.IsNotExist(err) {
				t.Fatalf("explicit rejection retained the cache: %v", err)
			}
		})
	}
}

func TestProtocolTwoFreshBootCannotUseMissingRouteOrForcedLegacyResponse(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, legacyResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("forcedLegacyResponse=%v", legacyResponse), func(t *testing.T) {
			c, err := NewClient(&ClientConfig{LicenseKey: "ARC-TEST-V2-BOOT", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
			if err != nil {
				t.Fatal(err)
			}
			lf, sig := signedPairKF(t, c.licenseKey, c.fingerprint, time.Hour)
			var cancelAttempt context.CancelFunc
			c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path != "/api/v2/verify" {
					t.Fatalf("client attempted fallback: %s", r.URL.Path)
				}
				status, body := http.StatusNotFound, []byte(`{"error":"Cannot GET /api/v2/verify"}`)
				if legacyResponse {
					// Model a URL-only rewrite to the old API. The client still
					// requires P2; a valid legacy signature is not fresh authority.
					r.URL.Path = "/api/v1/verify"
					status = http.StatusOK
					body, err = json.Marshal(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig})
					if err != nil {
						t.Fatal(err)
					}
				}
				if cancelAttempt != nil {
					cancelAttempt()
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
			})
			if lic, err := c.ActivateOrVerify(context.Background()); lic != nil || err == nil || FailureClassOf(err) != ClassTransient {
				t.Fatalf("missing P2 authority must remain transient: license=%v error=%v", lic, err)
			}
			if c.GetLicense() != nil || c.IsValid() || c.CanUseClustering() || c.policyProtocol != 2 || !c.requireAuthorityTime {
				t.Fatal("URL-only rewrite changed P2 requirements or enabled Enterprise")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancelAttempt = cancel
			lic, source, err := c.AwaitOnlineAuthority(ctx)
			if lic != nil || source != SourceServer || !errors.Is(err, context.Canceled) {
				t.Fatalf("fresh boot should wait for authority until cancellation, not return an OSS verdict: source=%s error=%v", source, err)
			}
			if _, err := os.Stat(c.cachePath()); !os.IsNotExist(err) {
				t.Fatalf("unproved response must not create a cache: %v", err)
			}
		})
	}
}

func TestProtocolTwoDaySevenPolicySurvivesLegacyOnlyResponse(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, verifyOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("verifyOnly=%v", verifyOnly), func(t *testing.T) {
			now := time.Now().UTC()
			term := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -7)
			c := grantedProtocolTwoClient(t, &ExpiryPolicy{Version: 1, Kind: "paid", TermEndsAt: term, GraceDays: 14})
			grant, state := c.GetLicense(), c.GetLicense().ExpiryState()
			if state.GraceDay != 7 || !state.WritersStopped || state.RestartIntervalSeconds != 14400 {
				t.Fatalf("fixture did not establish cumulative day 7: %+v", state)
			}
			cache, err := os.ReadFile(c.cachePath())
			if err != nil {
				t.Fatal(err)
			}
			lf, sig := signedPairKF(t, c.licenseKey, c.fingerprint, 365*24*time.Hour)
			calls := 0
			c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.Path != "/api/v2/verify" {
					t.Fatalf("client attempted fallback: %s", r.URL.Path)
				}
				r.URL.Path = "/api/v1/verify" // URL-only upstream rewrite, no change to trust requirements.
				body, err := json.Marshal(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig,
					Status: "active", DaysRemaining: 365, ExpiresAt: now.AddDate(1, 0, 0)})
				if err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
			})
			if verifyOnly {
				_, err = c.Verify(context.Background())
			} else {
				_, err = c.ActivateOrVerify(context.Background())
			}
			if err == nil || FailureClassOf(err) != ClassTransient || calls != 1 {
				t.Fatalf("legacy-only response was not transient without fallback: calls=%d error=%v", calls, err)
			}
			after, readErr := os.ReadFile(c.cachePath())
			if c.GetLicense() != grant || c.GetLicense().ExpiryState() != state || readErr != nil || !bytes.Equal(after, cache) {
				t.Fatalf("legacy response changed cumulative restrictions, signed grant, or cache: state=%+v error=%v", c.GetLicense().ExpiryState(), readErr)
			}
		})
	}
}

func TestClientVersionedRoutesNeverFallback(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, operation := range []string{"activate", "verify", "activate_or_verify"} {
			for _, responseBody := range []string{"unknown route", `{"error":"unknown route"}`} {
				t.Run(fmt.Sprintf("legacy=%v/%s/%s", legacy, operation, responseBody), func(t *testing.T) {
					c, err := NewClient(&ClientConfig{LicenseKey: "ARC-TEST-API-ROUTE", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
					if err != nil {
						t.Fatal(err)
					}
					version := "v2"
					if legacy {
						c.policyProtocol = 1
						version = "v1"
					}
					calls := 0
					c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
						calls++
						method, action := http.MethodGet, "verify"
						if operation == "activate" {
							method, action = http.MethodPost, "activate"
						}
						if r.Method != method || r.URL.Path != "/api/"+version+"/"+action {
							t.Errorf("request = %s %s, want %s /api/%s/%s", r.Method, r.URL.Path, method, version, action)
						}
						nonce := r.URL.Query().Get("time_nonce")
						if r.URL.Query().Has("license_protocol") {
							t.Error("API version must come from the route, not a selector")
						}
						if method == http.MethodPost {
							var body map[string]json.RawMessage
							if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
								t.Fatal(err)
							}
							if _, ok := body["license_protocol"]; ok {
								t.Error("activation sent a version selector")
							}
							if err := json.Unmarshal(body["time_nonce"], &nonce); err != nil {
								t.Fatal(err)
							}
						}
						decoded, err := base64.RawURLEncoding.DecodeString(nonce)
						if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != nonce {
							t.Errorf("request did not contain a canonical 256-bit nonce: %v", err)
						}
						return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(responseBody))}, nil
					})
					switch operation {
					case "activate":
						_, err = c.Activate(context.Background())
					case "verify":
						_, err = c.Verify(context.Background())
					default:
						_, err = c.ActivateOrVerify(context.Background())
					}
					if err == nil || calls != 1 || c.GetLicense() != nil {
						t.Fatalf("404 must fail without fallback: calls=%d error=%v", calls, err)
					}
					wantClass := ClassTransient
					if legacy && strings.HasPrefix(responseBody, "{") {
						wantClass = ClassDefinitive
					}
					if FailureClassOf(err) != wantClass {
						t.Fatalf("404 classification changed for legacy=%v: %v", legacy, err)
					}
				})
			}
		}
	}
}

func TestProtocolTwoClientRequiresFreshPolicyAndTime(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	for _, policy := range []bool{false, true} {
		c, err := NewClient(&ClientConfig{LicenseKey: "ARC-TEST-PROTOCOL-TWO", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		if !c.requireAuthorityTime || c.policyProtocol != 2 {
			t.Fatal("production client no longer requires fresh protocol-2 authority")
		}
		p := &ExpiryPolicy{Version: 1, Kind: "trial", TermEndsAt: time.Now().Add(time.Hour)}
		signed := &SignedLicense{Version: 1, LicenseKey: c.licenseKey, MachineFingerprint: c.fingerprint, ExpiresAt: p.cutoff()}
		if policy {
			signed.Policy = p
		}
		lf, sig := signDetached(t, rsaKey1, signed)
		c.httpClient.Transport = replacementTransport(func(*http.Request) (*http.Response, error) {
			body, _ := json.Marshal(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig})
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
		})
		if _, err := c.ActivateOrVerify(context.Background()); err == nil || FailureClassOf(err) != ClassTransient || c.GetLicense() != nil {
			t.Fatalf("missing policy/time accepted (policy=%v): %v", policy, err)
		}
		c.saveCache(lf, sig)
		if _, err := c.LoadCachedLicense(); err == nil {
			t.Fatal("fresh process used cache without signed current time")
		}
	}
}

func TestProtocolTwoActivationRecoveryStaysOnV2(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	c, err := NewClient(&ClientConfig{LicenseKey: "ARC-TEST-V2-ACTIVATION", CacheDir: t.TempDir(), Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	var requests []string
	var verifyNonce string
	c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		respond := func(body any) *http.Response {
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}
		}
		if len(requests) == 1 {
			if requests[0] != "GET /api/v2/verify" {
				t.Fatalf("initial request = %s", requests[0])
			}
			verifyNonce = r.URL.Query().Get("time_nonce")
			return respond(VerifyResponse{Error: "machine not activated for this license"}), nil
		}
		if len(requests) != 2 || requests[1] != "POST /api/v2/activate" {
			t.Fatalf("activation recovery left v2: %v", requests)
		}
		var request ActivateRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.TimeNonce == "" || request.TimeNonce == verifyNonce || request.LicenseKey != c.licenseKey || request.MachineFingerprint != c.fingerprint {
			t.Fatal("activation must have a fresh challenge and retain the configured key/fingerprint")
		}
		now := time.Now().UTC()
		policy := &ExpiryPolicy{Version: 1, Kind: "trial", TermEndsAt: now.Add(time.Hour)}
		lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: c.licenseKey,
			MachineFingerprint: c.fingerprint, Policy: policy, ExpiresAt: policy.cutoff()})
		payload, _ := base64.StdEncoding.DecodeString(lf)
		keyHash, payloadHash := sha256.Sum256([]byte(c.licenseKey)), sha256.Sum256(payload)
		proof, err := json.Marshal(AuthorityTime{Version: 1, Purpose: "arc-license-time", Nonce: request.TimeNonce,
			LicenseKeySHA256: hex.EncodeToString(keyHash[:]), MachineFingerprint: c.fingerprint,
			LicenseSHA256: hex.EncodeToString(payloadHash[:]), ServerTime: now})
		if err != nil {
			t.Fatal(err)
		}
		tf, ts := signDetachedRaw(t, rsaKey1, proof)
		return respond(ActivateResponse{Success: true, LicenseFile: lf, LicenseSignature: sig, TimeFile: tf, TimeSignature: ts}), nil
	})
	lic, err := c.ActivateOrVerify(context.Background())
	if err != nil || lic == nil || !lic.IsValid() || len(requests) != 2 {
		t.Fatalf("signed v2 activation recovery failed: requests=%v error=%v", requests, err)
	}
	if _, err := c.LoadCachedLicense(); err != nil {
		t.Fatalf("fresh v2 grant could not reload its cache: %v", err)
	}
}

// Exercise the real protocol-2 acceptance path without sockets: each response
// has a real detached license signature and request-bound signed time proof.
func TestReplacementProductionKeyPreservesInstallationAndReplacesTrialLimits(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	dir := t.TempDir()
	retained := map[string][]byte{"arc.toml": []byte("[cluster]\nnode_id = 'retained-node'\n"), "acknowledged-data": []byte("retained acknowledged rows")}
	for name, data := range retained {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	newClient := func(key, kind string, machines, cores int) *Client {
		c, err := NewClient(&ClientConfig{LicenseKey: key, CacheDir: dir, Logger: zerolog.Nop()})
		if err != nil {
			t.Fatal(err)
		}
		c.httpClient.Transport = replacementTransport(func(r *http.Request) (*http.Response, error) {
			q := r.URL.Query()
			if q.Get("license_key") != key || r.URL.Path != "/api/v2/verify" || q.Has("license_protocol") {
				t.Fatal("wrong configured key/protocol")
			}
			now := time.Now().UTC()
			p := &ExpiryPolicy{Version: 1, Kind: kind, TermEndsAt: now.AddDate(0, 0, 14)}
			if kind == "paid" {
				p.GraceDays = 14
			}
			lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: key, Tier: TierEnterprise,
				MachineFingerprint: c.fingerprint, Policy: p, ExpiresAt: p.cutoff(), MaxMachines: machines, MaxCores: cores,
				Features: []string{FeatureClustering, FeatureRBAC, FeatureTieredStorage}})
			payload, _ := base64.StdEncoding.DecodeString(lf)
			keyHash, payloadHash := sha256.Sum256([]byte(key)), sha256.Sum256(payload)
			proof, _ := json.Marshal(AuthorityTime{Version: 1, Purpose: "arc-license-time", Nonce: q.Get("time_nonce"),
				LicenseKeySHA256: hex.EncodeToString(keyHash[:]), MachineFingerprint: c.fingerprint,
				LicenseSHA256: hex.EncodeToString(payloadHash[:]), ServerTime: now})
			tf, ts := signDetachedRaw(t, rsaKey1, proof)
			body, _ := json.Marshal(VerifyResponse{Valid: true, LicenseFile: lf, LicenseSignature: sig, TimeFile: tf, TimeSignature: ts})
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
		})
		return c
	}
	trial := newClient("ARC-TEST-TRIAL", "trial", 10, 64)
	old, err := trial.ActivateOrVerify(context.Background())
	if err != nil || !old.IsValid() {
		t.Fatalf("trial acceptance: %v", err)
	}
	paid := newClient("ARC-TEST-SEPARATE-PAID", "paid", 2, 4)
	next, err := paid.ActivateOrVerify(context.Background())
	if err != nil {
		t.Fatalf("separate production key rejected: %v", err)
	}
	if next.LicenseKey == old.LicenseKey || next.Policy.Kind != "paid" || next.MaxMachines != 2 || next.MaxCores != 4 || !paid.IsValid() {
		t.Fatalf("replacement key did not replace trial entitlements: %+v", next)
	}
	if old.Policy.Kind != "trial" || old.MaxMachines != 10 || old.MaxCores != 64 {
		t.Fatal("trial grant was converted or mutated")
	}
	if _, err := paid.LoadCachedLicense(); err != nil {
		t.Fatalf("replacement cache rejected: %v", err)
	}
	for name, want := range retained {
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("installation file %s changed: %v", name, err)
		}
	}
}
