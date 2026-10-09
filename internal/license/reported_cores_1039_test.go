package license

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/syscpu"
)

// captureActivateRequest runs Activate against a server that records the
// decoded request body and answers nothing usable.
//
// Activate is EXPECTED to return an error: the response carries no signed
// licence, so verification fails well after the request has been sent. The
// assertion is on what went out, which is the whole subject of #1039.
func captureActivateRequest(t *testing.T) (*ActivateRequest, map[string]json.RawMessage) {
	t.Helper()
	var got ActivateRequest
	raw := map[string]json.RawMessage{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read activation request: %v", err)
			return
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("decode activation request: %v", err)
		}
		// Also keep the raw keys. Decoding only into ActivateRequest shares the
		// struct tag between marshal and unmarshal, so a typo in it is
		// symmetric and invisible — and that tag is the whole cross-repo
		// contract with the activation server.
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Errorf("decode activation request keys: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":false,"error":"test server"}`))
	}))
	t.Cleanup(server.Close)

	c := newTestClient(t, server.URL, t.TempDir())
	_, _ = c.Activate(context.Background())
	return &got, raw
}

// Activation reports BOTH counts, and the usable one comes from the seam
// rather than from runtime.NumCPU().
//
// The sentinel is what makes this fail against the unfixed code. Asserting
// "usable_cores == syscpu.CoresAtStartup()" would recompute the production
// expression, and on any runner without a CPU quota that equals
// runtime.NumCPU() — so it would pass against a client that never learned
// about quotas at all. 4242 is a value no machine reports (#1030: licence core
// tests were green against the bug on a 4-vCPU runner for exactly this
// reason).
func TestActivateReportsMachineAndUsableCores(t *testing.T) {
	const sentinel = 4242
	original := usableCoresFn
	usableCoresFn = func() int { return sentinel }
	t.Cleanup(func() { usableCoresFn = original })

	req, raw := captureActivateRequest(t)

	// The wire name, not just the value: the activation server adds a column
	// keyed on this exact string.
	if got, ok := raw["usable_cores"]; !ok {
		t.Errorf("no usable_cores key on the wire; keys were %v", rawKeys(raw))
	} else if string(got) != "4242" {
		t.Errorf("wire usable_cores = %s, want 4242", got)
	}
	if _, ok := raw["cores"]; !ok {
		t.Errorf("the cores key disappeared from the wire; keys were %v", rawKeys(raw))
	}

	if req.UsableCores != sentinel {
		t.Errorf("usable_cores = %d, want %d from the seam", req.UsableCores, sentinel)
	}
	if req.Cores != runtime.NumCPU() {
		t.Errorf("cores = %d, want runtime.NumCPU() = %d: the machine figure must not move", req.Cores, runtime.NumCPU())
	}
}

// The seam must be wired to the SNAPSHOT, not to the live reader.
//
// This is the guard for the live hazard, and it is a function-identity check
// rather than a behavioural one for a reason. applyLicenseCoreLimits pins
// GOMAXPROCS down to the licence's core limit, and periodic re-validation
// re-activates long after that pin — a routine condition, not an edge case,
// because the server reaps activations that have sent no heartbeat. A client
// reading syscpu.EffectiveCores() would then post the LICENSED number on bare
// metal with no CPU quota at all.
//
// Driving that through the seam cannot show it: the seam is itself the
// function under swap, so changing it always changes the answer and the test
// would pass whichever function production used. What distinguishes the two is
// WHICH function is installed, so that is what this asserts. The behaviour
// that makes the snapshot a snapshot — surviving a GOMAXPROCS pin — is pinned
// by TestCoresAtStartupIgnoresALaterPin in internal/syscpu.
func TestUsableCoresSeamIsTheStartupSnapshot(t *testing.T) {
	got := reflect.ValueOf(usableCoresFn).Pointer()
	if want := reflect.ValueOf(syscpu.CoresAtStartup).Pointer(); got != want {
		live := reflect.ValueOf(syscpu.EffectiveCores).Pointer()
		if got == live {
			t.Fatal("usableCoresFn is syscpu.EffectiveCores: a re-activation after the license pins GOMAXPROCS would report the licensed core count as the node size (#1039)")
		}
		t.Fatal("usableCoresFn is not syscpu.CoresAtStartup; outbound reports must use the startup snapshot")
	}
}

// The machine fingerprint must keep reading runtime.NumCPU().
//
// getCPUInfo's "cores:N" feeds GenerateMachineFingerprint, which bindFingerprint
// compares on every activation and verification, so sweeping it to a
// quota-aware count invalidates every machine-bound licence in the field.
//
// This is a SOURCE-TEXT assertion on purpose. The behavioural form — call
// getCPUInfo and look for cores:<NumCPU> — is vacuous: on any machine without
// a CPU quota the swept and unswept expressions produce the identical string,
// so it passes before the sweep, after the sweep, and against the mutation it
// exists to catch.
func TestFingerprintStillReadsNumCPU(t *testing.T) {
	src, err := os.ReadFile("fingerprint.go")
	if err != nil {
		t.Fatalf("read fingerprint.go: %v", err)
	}
	text := string(src)

	fn := regexp.MustCompile(`(?s)func getCPUInfo\(\).*?\n}`).FindString(text)
	if fn == "" {
		t.Fatal("getCPUInfo not found in fingerprint.go; if it was renamed, re-point this guard rather than deleting it")
	}
	if !strings.Contains(fn, "runtime.NumCPU()") {
		t.Error("getCPUInfo no longer reads runtime.NumCPU(): this changes the machine fingerprint and invalidates every bound license")
	}
	for _, banned := range []string{"EffectiveCores", "CoresAtStartup", "GOMAXPROCS", "syscpu"} {
		if strings.Contains(text, banned) {
			t.Errorf("fingerprint.go references %q: the fingerprint must stay on runtime.NumCPU() (#1039)", banned)
		}
	}
}

func rawKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
