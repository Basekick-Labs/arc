package tiering

import (
	"testing"
)

func TestTier_String(t *testing.T) {
	// 2-tier system: hot and cold only
	tests := []struct {
		tier     Tier
		expected string
	}{
		{TierHot, "hot"},
		{TierCold, "cold"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.tier.String(); got != tt.expected {
				t.Errorf("Tier.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTier_IsValid(t *testing.T) {
	// 2-tier system: hot and cold only
	tests := []struct {
		tier     Tier
		expected bool
	}{
		{TierHot, true},
		{TierCold, true},
		{Tier("warm"), false},    // Warm tier removed in 2-tier system
		{Tier("invalid"), false},
		{Tier(""), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.tier), func(t *testing.T) {
			if got := tt.tier.IsValid(); got != tt.expected {
				t.Errorf("Tier.IsValid() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTierFromString(t *testing.T) {
	// 2-tier system: hot and cold only
	tests := []struct {
		input    string
		expected Tier
	}{
		{"hot", TierHot},
		{"cold", TierCold},
		{"warm", TierHot},    // Warm tier removed - defaults to hot
		{"invalid", TierHot}, // Default to hot
		{"", TierHot},        // Default to hot
		{"HOT", TierHot},     // Case sensitivity - defaults to hot
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := TierFromString(tt.input); got != tt.expected {
				t.Errorf("TierFromString(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
