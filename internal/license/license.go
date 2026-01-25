package license

import (
	"time"
)

// Tier represents the license tier
type Tier string

const (
	TierStarter      Tier = "starter"
	TierProfessional Tier = "professional"
	TierEnterprise   Tier = "enterprise"
	TierUnlimited    Tier = "unlimited"
)

// Feature constants for feature gating
const (
	FeatureCQScheduler        = "cq_scheduler"
	FeatureRetentionScheduler = "retention_scheduler"
	FeatureClustering         = "clustering"
	FeatureRBAC               = "rbac"
	FeatureTieredStorage      = "tiering"
	FeatureAutoAggregation    = "auto_aggregation"
)

// License represents a validated license
type License struct {
	LicenseKey    string    `json:"license_key"`
	CustomerID    string    `json:"customer_id"`
	CustomerName  string    `json:"customer_name"`
	Tier          Tier      `json:"tier"`
	MaxCores      int       `json:"max_cores"`
	MaxMachines   int       `json:"max_machines"`
	Features      []string  `json:"features"`
	ExpiresAt     time.Time `json:"expires_at"`
	Status        string    `json:"status"` // active, grace_period, read_only, expired
	DaysRemaining int       `json:"days_remaining"`
}

// IsValid returns true if the license is valid and not expired
func (l *License) IsValid() bool {
	if l == nil {
		return false
	}
	return l.Status == "active" || l.Status == "grace_period"
}

// IsExpired returns true if the license has expired
func (l *License) IsExpired() bool {
	if l == nil {
		return true
	}
	return l.Status == "expired" || l.Status == "read_only"
}

// HasFeature checks if the license includes a specific feature
func (l *License) HasFeature(feature string) bool {
	if l == nil || !l.IsValid() {
		return false
	}
	for _, f := range l.Features {
		if f == feature {
			return true
		}
	}
	return false
}

// IsEnterprise returns true if the license tier is enterprise or higher
func (l *License) IsEnterprise() bool {
	if l == nil || !l.IsValid() {
		return false
	}
	return l.Tier == TierEnterprise || l.Tier == TierUnlimited
}

// IsProfessional returns true if the license tier is professional or higher
func (l *License) IsProfessional() bool {
	if l == nil || !l.IsValid() {
		return false
	}
	return l.Tier == TierProfessional || l.Tier == TierEnterprise || l.Tier == TierUnlimited
}

// CanUseCQScheduler returns true if the license allows CQ scheduling
// All valid license tiers (starter, professional, enterprise, unlimited) include this feature
func (l *License) CanUseCQScheduler() bool {
	return l.IsValid()
}

// CanUseRetentionScheduler returns true if the license allows retention scheduling
// All valid license tiers (starter, professional, enterprise, unlimited) include this feature
func (l *License) CanUseRetentionScheduler() bool {
	return l.IsValid()
}

// CanUseTieredStorage returns true if the license allows tiered storage
// Requires enterprise license with the tiered_storage feature
func (l *License) CanUseTieredStorage() bool {
	return l.HasFeature(FeatureTieredStorage)
}

// TierFromString converts a string to a Tier
func TierFromString(s string) Tier {
	switch s {
	case "starter":
		return TierStarter
	case "professional":
		return TierProfessional
	case "enterprise":
		return TierEnterprise
	case "unlimited":
		return TierUnlimited
	default:
		return TierStarter
	}
}
