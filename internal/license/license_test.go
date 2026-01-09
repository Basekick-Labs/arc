package license

import (
	"testing"
	"time"
)

func TestLicense_IsValid(t *testing.T) {
	tests := []struct {
		name    string
		license *License
		want    bool
	}{
		{
			name:    "nil license",
			license: nil,
			want:    false,
		},
		{
			name: "active license",
			license: &License{
				Status: "active",
			},
			want: true,
		},
		{
			name: "grace period license",
			license: &License{
				Status: "grace_period",
			},
			want: true,
		},
		{
			name: "expired license",
			license: &License{
				Status: "expired",
			},
			want: false,
		},
		{
			name: "read only license",
			license: &License{
				Status: "read_only",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.license.IsValid(); got != tt.want {
				t.Errorf("License.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLicense_IsExpired(t *testing.T) {
	tests := []struct {
		name    string
		license *License
		want    bool
	}{
		{
			name:    "nil license",
			license: nil,
			want:    true,
		},
		{
			name: "active license",
			license: &License{
				Status: "active",
			},
			want: false,
		},
		{
			name: "expired license",
			license: &License{
				Status: "expired",
			},
			want: true,
		},
		{
			name: "read only license",
			license: &License{
				Status: "read_only",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.license.IsExpired(); got != tt.want {
				t.Errorf("License.IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLicense_HasFeature(t *testing.T) {
	license := &License{
		Status:   "active",
		Features: []string{"cq_scheduler", "retention_scheduler", "clustering"},
	}

	tests := []struct {
		name    string
		feature string
		want    bool
	}{
		{"has cq_scheduler", "cq_scheduler", true},
		{"has retention_scheduler", "retention_scheduler", true},
		{"has clustering", "clustering", true},
		{"no rbac", "rbac", false},
		{"no unknown", "unknown_feature", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := license.HasFeature(tt.feature); got != tt.want {
				t.Errorf("License.HasFeature(%q) = %v, want %v", tt.feature, got, tt.want)
			}
		})
	}
}

func TestLicense_HasFeature_InvalidLicense(t *testing.T) {
	// Nil license
	var nilLicense *License
	if nilLicense.HasFeature("any") {
		t.Error("nil license should not have any feature")
	}

	// Expired license
	expiredLicense := &License{
		Status:   "expired",
		Features: []string{"cq_scheduler"},
	}
	if expiredLicense.HasFeature("cq_scheduler") {
		t.Error("expired license should not have features")
	}
}

func TestLicense_IsEnterprise(t *testing.T) {
	tests := []struct {
		name    string
		license *License
		want    bool
	}{
		{
			name:    "nil license",
			license: nil,
			want:    false,
		},
		{
			name: "starter tier",
			license: &License{
				Status: "active",
				Tier:   TierStarter,
			},
			want: false,
		},
		{
			name: "professional tier",
			license: &License{
				Status: "active",
				Tier:   TierProfessional,
			},
			want: false,
		},
		{
			name: "enterprise tier",
			license: &License{
				Status: "active",
				Tier:   TierEnterprise,
			},
			want: true,
		},
		{
			name: "unlimited tier",
			license: &License{
				Status: "active",
				Tier:   TierUnlimited,
			},
			want: true,
		},
		{
			name: "enterprise tier but expired",
			license: &License{
				Status: "expired",
				Tier:   TierEnterprise,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.license.IsEnterprise(); got != tt.want {
				t.Errorf("License.IsEnterprise() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLicense_IsProfessional(t *testing.T) {
	tests := []struct {
		name    string
		license *License
		want    bool
	}{
		{
			name: "starter tier",
			license: &License{
				Status: "active",
				Tier:   TierStarter,
			},
			want: false,
		},
		{
			name: "professional tier",
			license: &License{
				Status: "active",
				Tier:   TierProfessional,
			},
			want: true,
		},
		{
			name: "enterprise tier",
			license: &License{
				Status: "active",
				Tier:   TierEnterprise,
			},
			want: true,
		},
		{
			name: "unlimited tier",
			license: &License{
				Status: "active",
				Tier:   TierUnlimited,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.license.IsProfessional(); got != tt.want {
				t.Errorf("License.IsProfessional() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLicense_CanUseCQScheduler(t *testing.T) {
	// All valid license tiers should be able to use CQ scheduler
	tests := []struct {
		name    string
		license *License
		want    bool
	}{
		{
			name:    "nil license",
			license: nil,
			want:    false,
		},
		{
			name: "starter tier",
			license: &License{
				Status: "active",
				Tier:   TierStarter,
			},
			want: true,
		},
		{
			name: "professional tier",
			license: &License{
				Status: "active",
				Tier:   TierProfessional,
			},
			want: true,
		},
		{
			name: "enterprise tier",
			license: &License{
				Status: "active",
				Tier:   TierEnterprise,
			},
			want: true,
		},
		{
			name: "unlimited tier",
			license: &License{
				Status: "active",
				Tier:   TierUnlimited,
			},
			want: true,
		},
		{
			name: "expired license",
			license: &License{
				Status: "expired",
				Tier:   TierEnterprise,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.license.CanUseCQScheduler(); got != tt.want {
				t.Errorf("License.CanUseCQScheduler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLicense_CanUseRetentionScheduler(t *testing.T) {
	// All valid license tiers should be able to use retention scheduler
	tests := []struct {
		name    string
		license *License
		want    bool
	}{
		{
			name:    "nil license",
			license: nil,
			want:    false,
		},
		{
			name: "starter tier",
			license: &License{
				Status: "active",
				Tier:   TierStarter,
			},
			want: true,
		},
		{
			name: "professional tier",
			license: &License{
				Status: "active",
				Tier:   TierProfessional,
			},
			want: true,
		},
		{
			name: "enterprise tier",
			license: &License{
				Status: "active",
				Tier:   TierEnterprise,
			},
			want: true,
		},
		{
			name: "expired license",
			license: &License{
				Status: "expired",
				Tier:   TierEnterprise,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.license.CanUseRetentionScheduler(); got != tt.want {
				t.Errorf("License.CanUseRetentionScheduler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTierFromString(t *testing.T) {
	tests := []struct {
		input string
		want  Tier
	}{
		{"starter", TierStarter},
		{"professional", TierProfessional},
		{"enterprise", TierEnterprise},
		{"unlimited", TierUnlimited},
		{"unknown", TierStarter},
		{"", TierStarter},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := TierFromString(tt.input); got != tt.want {
				t.Errorf("TierFromString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestLicenseStruct(t *testing.T) {
	license := &License{
		LicenseKey:    "ARC-ENT-1234-5678-90AB-CDEF",
		CustomerID:    "acme-corp",
		CustomerName:  "Acme Corporation",
		Tier:          TierEnterprise,
		MaxCores:      128,
		MaxMachines:   20,
		Features:      []string{"cq_scheduler", "retention_scheduler"},
		ExpiresAt:     time.Now().Add(365 * 24 * time.Hour),
		Status:        "active",
		DaysRemaining: 365,
	}

	if license.LicenseKey != "ARC-ENT-1234-5678-90AB-CDEF" {
		t.Error("LicenseKey mismatch")
	}
	if license.CustomerID != "acme-corp" {
		t.Error("CustomerID mismatch")
	}
	if license.Tier != TierEnterprise {
		t.Error("Tier mismatch")
	}
	if license.MaxCores != 128 {
		t.Error("MaxCores mismatch")
	}
	if license.MaxMachines != 20 {
		t.Error("MaxMachines mismatch")
	}
	if len(license.Features) != 2 {
		t.Error("Features count mismatch")
	}
	if license.DaysRemaining != 365 {
		t.Error("DaysRemaining mismatch")
	}
}
