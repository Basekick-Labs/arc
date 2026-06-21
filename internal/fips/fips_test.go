package fips

import (
	"crypto/tls"
	"testing"
)

// TestHardenTLSConfig verifies build-variant-specific behavior: the fips build
// pins FIPS-approved version/ciphers/curves; the default build is a no-op that
// leaves Go's TLS defaults (notably X25519) untouched. The two postures must
// not mix.
func TestHardenTLSConfig(t *testing.T) {
	in := &tls.Config{Certificates: []tls.Certificate{{}}}
	out := HardenTLSConfig(in)

	if out != in {
		t.Error("HardenTLSConfig should return the passed config")
	}
	if len(out.Certificates) != 1 {
		t.Error("HardenTLSConfig must preserve caller-set Certificates")
	}

	// The TLS 1.2 floor is applied in BOTH builds (pre-existing behavior).
	if out.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2 (%x) in all builds", out.MinVersion, tls.VersionTLS12)
	}

	if !BuildTagged {
		// Default build: must keep Go's default cipher suites / curves
		// (including X25519). Pinning them here would silently change behavior
		// for non-FIPS users.
		if out.CipherSuites != nil || out.CurvePreferences != nil {
			t.Errorf("default build must keep default ciphers/curves, got CipherSuites=%v CurvePreferences=%v",
				out.CipherSuites, out.CurvePreferences)
		}
		return
	}

	// fips build: must pin approved parameters.
	if out.MinVersion < tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want >= TLS 1.2 (%x)", out.MinVersion, tls.VersionTLS12)
	}
	if len(out.CipherSuites) == 0 {
		t.Error("CipherSuites must be explicitly set in the fips build")
	}
	for _, cs := range out.CipherSuites {
		if !isApprovedSuite(cs) {
			t.Errorf("cipher suite %x is not in the FIPS-approved set", cs)
		}
	}
	if len(out.CurvePreferences) == 0 {
		t.Error("CurvePreferences must be explicitly set in the fips build")
	}
	for _, c := range out.CurvePreferences {
		if c == tls.X25519 {
			t.Error("X25519 is not a FIPS-approved curve and must not be present in the fips build")
		}
	}
}

func TestHardenTLSConfig_NilInput(t *testing.T) {
	out := HardenTLSConfig(nil)
	if out == nil {
		t.Fatal("HardenTLSConfig(nil) must allocate a config")
	}
	if out.MinVersion != tls.VersionTLS12 {
		t.Errorf("nil input must still get the TLS 1.2 floor, got %x", out.MinVersion)
	}
}

// TestBuildTagImpliesFIPSEnabled documents the invariant the main() startup
// guard enforces: in the fips build variant, the binary must run with the Go
// module in FIPS mode. Under `go test -tags=fips`, GODEBUG=fips140 is not set
// for the test binary, so we only assert the build-tag constant here; the
// runtime fail-closed behavior is exercised by the release CI smoke test that
// boots the actual GOFIPS140-built binary.
func TestBuildTagImpliesFIPSEnabled(t *testing.T) {
	if BuildTagged {
		t.Log("fips build tag active; release CI asserts fips140.Enabled() on the GOFIPS140 binary")
	} else {
		if Enabled() {
			t.Error("default build should not report FIPS enabled unless GOFIPS140 was set")
		}
	}
}

func isApprovedSuite(cs uint16) bool {
	for _, a := range approvedCipherSuites {
		if a == cs {
			return true
		}
	}
	return false
}
