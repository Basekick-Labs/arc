package license

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// The agreed schedule: grace starts at the midnight UTC after expiry; days
// 1-3 full service, 4-6 warn, 7-14 writes stopped, day 15 expired. A trial
// ends at its signed expiry.
func TestGracePhases(t *testing.T) {
	paid := at("2026-06-10T13:00:00Z")
	for _, tc := range []struct {
		now   string
		phase Phase
		day   int
	}{
		{"2026-06-10T12:59:59Z", PhaseActive, 0},
		{"2026-06-10T13:00:00Z", PhaseGrace, 0},
		{"2026-06-10T23:59:59Z", PhaseGrace, 0},
		{"2026-06-11T00:00:00Z", PhaseGrace, 1},
		{"2026-06-13T23:59:59Z", PhaseGrace, 3},
		{"2026-06-14T00:00:00Z", PhaseGraceWarn, 4},
		{"2026-06-16T23:59:59Z", PhaseGraceWarn, 6},
		{"2026-06-17T00:00:00Z", PhaseWritesStopped, 7},
		{"2026-06-24T23:59:59Z", PhaseWritesStopped, 14},
		{"2026-06-25T00:00:00Z", PhaseExpired, 0},
	} {
		p, d := phaseAt(paid, false, at(tc.now))
		if p != tc.phase || d != tc.day {
			t.Errorf("paid at %s: %v day %d, want %v day %d", tc.now, p, d, tc.phase, tc.day)
		}
	}
	// Expiry at exactly midnight UTC: grace starts 24h later.
	midnight := at("2026-06-10T00:00:00Z")
	if p, d := phaseAt(midnight, false, at("2026-06-10T00:00:00Z")); p != PhaseGrace || d != 0 {
		t.Errorf("midnight expiry: %v day %d", p, d)
	}
	if s := scheduleFor(midnight, false); !s.GraceStart.Equal(at("2026-06-11T00:00:00Z")) || !s.Cutoff.Equal(at("2026-06-25T00:00:00Z")) {
		t.Errorf("midnight schedule: %+v", s)
	}
	// A non-UTC expiry is placed by its UTC day.
	if s := scheduleFor(at("2026-06-10T23:30:00-03:00"), false); !s.GraceStart.Equal(at("2026-06-12T00:00:00Z")) {
		t.Errorf("non-UTC expiry grace start: %v", s.GraceStart)
	}
	// Trials: no grace.
	if p, _ := phaseAt(paid, true, at("2026-06-10T12:59:59Z")); p != PhaseActive {
		t.Errorf("trial before expiry: %v", p)
	}
	if p, _ := phaseAt(paid, true, at("2026-06-10T13:00:00Z")); p != PhaseExpired {
		t.Errorf("trial at expiry: %v", p)
	}
}

// Status and days remaining: days to the signed expiry while active, to the
// cutoff during grace; the gates' validity follows the phase.
func TestGraceStatusAndValidity(t *testing.T) {
	exp := at("2026-06-10T13:00:00Z")
	for _, tc := range []struct {
		now    string
		status string
		days   int
		valid  bool
	}{
		{"2026-06-05T13:00:00Z", "active", 5, true},
		{"2026-06-12T00:00:00Z", "grace_period", 13, true},
		{"2026-06-20T00:00:00Z", "writes_stopped", 5, true},
		{"2026-06-25T00:00:00Z", "expired", 0, false},
	} {
		s, d := expiryStatus(exp, false, at(tc.now))
		if s != tc.status || d != tc.days || validAt(exp, false, at(tc.now)) != tc.valid {
			t.Errorf("%s: %s %d valid=%v, want %s %d %v", tc.now, s, d, validAt(exp, false, at(tc.now)), tc.status, tc.days, tc.valid)
		}
	}
}

func resetClock(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		clock.Store(nil)
		clockMu.Lock()
		clockPath, clockSave = "", time.Time{}
		clockMu.Unlock()
	})
}

func readMark(t *testing.T, dir string) (time.Time, bool) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, clockFileName))
	if err != nil {
		return time.Time{}, false
	}
	var f struct {
		HighWater time.Time `json:"high_water"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f.HighWater, true
}

// The mark comes only from server time (or a signed issue time) and advances
// by monotonic time: the host clock is never written into it, so a clock fast
// at startup and corrected later is not held against the license. A mark
// saved ahead of the wall clock (the clock was set back since) holds the
// license clock there; a corrupt or missing file means no mark.
func TestLicenseClockGuard(t *testing.T) {
	resetClock(t)
	dir := t.TempDir()

	// No file, no server time: nothing is saved, the license clock is the
	// wall clock. (A host clock fast at this point is therefore never kept.)
	if ahead := InitClock(dir); ahead != 0 {
		t.Fatalf("ahead %v with no file", ahead)
	}
	clockMu.Lock()
	clockSave = time.Time{}
	clockMu.Unlock()
	if err := saveClock(); err != nil {
		t.Fatal(err)
	}
	if _, ok := readMark(t, dir); ok {
		t.Fatal("a mark was saved before any server time was seen")
	}

	// Server time sets the mark; it is saved, and never lowered.
	server := time.Now().Add(-10 * time.Minute)
	AdvanceClock(server)
	AdvanceClock(server.Add(-time.Hour))
	if err := saveClock(); err != nil {
		t.Fatal(err)
	}
	m, ok := readMark(t, dir)
	if !ok || m.Before(server) || m.After(server.Add(time.Minute)) {
		t.Fatalf("saved mark %v (ok %v), want from the server time %v", m, ok, server)
	}
	if st, _ := os.Stat(filepath.Join(dir, clockFileName)); st.Mode().Perm() != 0o600 {
		t.Fatalf("mark file mode %v", st.Mode().Perm())
	}

	// A saved mark ahead of the wall clock: the clock was set back since.
	ahead := time.Now().Add(48 * time.Hour).UTC()
	b, _ := json.Marshal(map[string]time.Time{"high_water": ahead})
	if err := os.WriteFile(filepath.Join(dir, clockFileName), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if d := InitClock(dir); d < 47*time.Hour {
		t.Fatalf("ahead %v, want ~48h", d)
	}
	if got := licenseNow(); got.Before(ahead) {
		t.Fatalf("licenseNow %v is before the saved mark %v", got, ahead)
	}
	trial := &License{CustomerID: TrialCustomerPrefix + "x", ExpiresAt: time.Now().Add(24 * time.Hour)}
	if trial.IsValid() {
		t.Fatal("a clock set back made a license expired by the mark valid")
	}

	// A corrupt file: no mark.
	clock.Store(nil)
	if err := os.WriteFile(filepath.Join(dir, clockFileName), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	InitClock(dir)
	if d := licenseNow().Sub(time.Now()); d < -time.Second || d > time.Second {
		t.Fatalf("corrupt file: licenseNow off the wall clock by %v", d)
	}
}

// v1 refuses to re-activate a license past its term although it verifies it
// in grace: a paid license within its grace survives that refusal (the
// license and its cache stay), a trial or a suspension does not.
func TestReactivationRefusedPastTermKeepsGrace(t *testing.T) {
	for _, tc := range []struct {
		name     string
		trial    bool
		activate string
		keep     bool
	}{
		{"paid in grace, refused as expired", false, "license has expired", true},
		{"trial past expiry", true, "license has expired", false},
		{"paid in grace, suspended", false, "license is not active", false},
		{"paid in grace, refusal only containing the text", false, "license has expired; contact sales", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/verify") {
					_ = json.NewEncoder(w).Encode(map[string]any{"valid": false, "error": "machine not activated for this license"})
					return
				}
				w.WriteHeader(http.StatusForbidden)
				_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": tc.activate})
			}))
			defer ts.Close()
			dir := t.TempDir()
			c := newTestClient(t, ts.URL, dir)
			cust := "cust_test"
			if tc.trial {
				cust = TrialCustomerPrefix + "x"
			}
			lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: c.licenseKey, CustomerID: cust,
				Tier: TierEnterprise, MachineFingerprint: c.fingerprint, ExpiresAt: time.Now().Add(-48 * time.Hour)})
			c.saveCache(lf, sig)
			_, err := c.ActivateOrVerify(context.Background())
			_, statErr := os.Stat(c.cachePath())
			if tc.keep {
				if err != nil || c.GetLicense() == nil || statErr != nil {
					t.Fatalf("err=%v license=%v cache=%v: the paid license in grace was dropped", err, c.GetLicense(), statErr)
				}
				return
			}
			if err == nil || c.GetLicense() != nil || statErr == nil {
				t.Fatalf("err=%v license=%v cache=%v: should have been dropped", err, c.GetLicense(), statErr)
			}
		})
	}
}

// The watcher's state: writes stop on grace days 7-14 and resume on renewal;
// a listener is told the current state at registration and every change.
func TestWritesStoppedFollowsThePhase(t *testing.T) {
	c := &Client{license: &License{CustomerID: "cust", ExpiresAt: time.Now().Add(-9 * 24 * time.Hour)}}
	var got []bool
	c.OnWritesStoppedChange(func(s bool) { got = append(got, s) })
	if !c.WritesStopped() || len(got) != 1 || !got[0] {
		t.Fatalf("day ~9: stopped=%v calls=%v", c.WritesStopped(), got)
	}
	c.mu.Lock()
	c.license = &License{CustomerID: "cust", ExpiresAt: time.Now().Add(30 * 24 * time.Hour)} // renewed
	c.mu.Unlock()
	c.evaluatePhase()
	if c.WritesStopped() || len(got) != 2 || got[1] {
		t.Fatalf("renewed: stopped=%v calls=%v", c.WritesStopped(), got)
	}
	c.evaluatePhase() // no change: no call
	if len(got) != 2 {
		t.Fatalf("unchanged phase notified: %v", got)
	}
	c.mu.Lock()
	c.license = &License{CustomerID: "cust", ExpiresAt: time.Now().Add(-30 * 24 * time.Hour)} // past the cutoff
	c.mu.Unlock()
	c.evaluatePhase()
	if c.WritesStopped() {
		t.Fatal("past the cutoff the node runs as OSS and accepts writes")
	}
	var nilClient *Client
	if nilClient.WritesStopped() {
		t.Fatal("nil client stopped writes")
	}
}

// An offline license file loads through its grace period, and /health shows
// the grace fields; the boot path reports a license kept from the cache after
// a refused re-activation as coming from the cache.
func TestGraceOfflineFileHealthAndBootSource(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lf, sig := signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: "ARC-ENT-SITE-KEY", CustomerID: "cust_site",
		Tier: TierEnterprise, ExpiresAt: time.Now().Add(-2 * 24 * time.Hour)})
	path := writeOfflineFile(t, `{"license_file":"`+lf+`","license_signature":"`+sig+`"}`)
	c, err := NewOfflineClient(path, zerolog.Nop())
	if err != nil {
		t.Fatalf("offline file in grace refused: %v", err)
	}
	h := c.HealthStatus()
	if h.Status != "grace_period" || h.GraceDay == nil || h.WritesStopAt == nil || h.Cutoff == nil {
		t.Fatalf("health in grace: %+v", h)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/verify") {
			_ = json.NewEncoder(w).Encode(map[string]any{"valid": false, "error": "activation has been revoked"})
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "error": "license has expired"})
	}))
	defer ts.Close()
	cl := newTestClient(t, ts.URL, t.TempDir())
	lf, sig = signDetached(t, rsaKey1, &SignedLicense{Version: 1, LicenseKey: cl.licenseKey, CustomerID: "cust_test",
		Tier: TierEnterprise, MachineFingerprint: cl.fingerprint, ExpiresAt: time.Now().Add(-3 * 24 * time.Hour)})
	cl.saveCache(lf, sig)
	lic, src, err := cl.ActivateOrVerifyResilient(context.Background())
	if err != nil || lic == nil || src != SourceCache {
		t.Fatalf("boot in grace after a refused re-activation: lic=%v src=%s err=%v", lic, src, err)
	}
}
