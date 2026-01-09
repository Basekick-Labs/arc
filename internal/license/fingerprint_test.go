package license

import (
	"strings"
	"testing"
)

func TestGenerateMachineFingerprint(t *testing.T) {
	fingerprint, err := GenerateMachineFingerprint()
	if err != nil {
		t.Fatalf("GenerateMachineFingerprint() error = %v", err)
	}

	// Check format
	if !strings.HasPrefix(fingerprint, "sha256:") {
		t.Errorf("fingerprint should start with 'sha256:', got %s", fingerprint)
	}

	// Check length (sha256: + 64 hex chars = 71)
	if len(fingerprint) != 71 {
		t.Errorf("fingerprint length should be 71, got %d", len(fingerprint))
	}

	// Check it's valid hex after the prefix
	hexPart := strings.TrimPrefix(fingerprint, "sha256:")
	if len(hexPart) != 64 {
		t.Errorf("hex part should be 64 chars, got %d", len(hexPart))
	}

	for _, c := range hexPart {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("invalid hex character: %c", c)
		}
	}
}

func TestGenerateMachineFingerprint_Consistency(t *testing.T) {
	// Fingerprint should be consistent across calls
	fp1, err := GenerateMachineFingerprint()
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	fp2, err := GenerateMachineFingerprint()
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	if fp1 != fp2 {
		t.Errorf("fingerprints should be consistent, got %s and %s", fp1, fp2)
	}
}

func TestValidateFingerprint(t *testing.T) {
	tests := []struct {
		name        string
		fingerprint string
		want        bool
	}{
		{
			name:        "valid fingerprint",
			fingerprint: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			want:        true,
		},
		{
			name:        "missing prefix",
			fingerprint: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			want:        false,
		},
		{
			name:        "wrong prefix",
			fingerprint: "md5:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			want:        false,
		},
		{
			name:        "too short",
			fingerprint: "sha256:0123456789abcdef",
			want:        false,
		},
		{
			name:        "too long",
			fingerprint: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef00",
			want:        false,
		},
		{
			name:        "invalid hex chars",
			fingerprint: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg",
			want:        false,
		},
		{
			name:        "uppercase hex (should fail)",
			fingerprint: "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
			want:        false,
		},
		{
			name:        "empty string",
			fingerprint: "",
			want:        false,
		},
		{
			name:        "just prefix",
			fingerprint: "sha256:",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidateFingerprint(tt.fingerprint); got != tt.want {
				t.Errorf("ValidateFingerprint(%q) = %v, want %v", tt.fingerprint, got, tt.want)
			}
		})
	}
}

func TestGetMACAddresses(t *testing.T) {
	macs := getMACAddresses()
	// Should return some MACs on most systems (except in containers without network)
	// We just check it doesn't panic and returns a sorted list
	for i := 1; i < len(macs); i++ {
		if macs[i] < macs[i-1] {
			t.Error("MACs should be sorted")
		}
	}
}

func TestGetCPUInfo(t *testing.T) {
	cpuInfo := getCPUInfo()
	if cpuInfo == "" {
		t.Error("getCPUInfo should return non-empty string")
	}
	if !strings.HasPrefix(cpuInfo, "cores:") {
		t.Errorf("getCPUInfo should start with 'cores:', got %s", cpuInfo)
	}
}
