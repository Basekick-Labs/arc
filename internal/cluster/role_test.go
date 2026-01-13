package cluster

import (
	"testing"
)

func TestNodeRoleIsValid(t *testing.T) {
	tests := []struct {
		role     NodeRole
		expected bool
	}{
		{RoleStandalone, true},
		{RoleWriter, true},
		{RoleReader, true},
		{RoleCompactor, true},
		{NodeRole("invalid"), false},
		{NodeRole(""), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			if got := tt.role.IsValid(); got != tt.expected {
				t.Errorf("NodeRole(%q).IsValid() = %v, want %v", tt.role, got, tt.expected)
			}
		})
	}
}

func TestValidRole(t *testing.T) {
	tests := []struct {
		role     string
		expected bool
	}{
		{"standalone", true},
		{"writer", true},
		{"reader", true},
		{"compactor", true},
		{"invalid", false},
		{"", false},
		{"WRITER", false}, // case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			if got := ValidRole(tt.role); got != tt.expected {
				t.Errorf("ValidRole(%q) = %v, want %v", tt.role, got, tt.expected)
			}
		})
	}
}

func TestParseRole(t *testing.T) {
	tests := []struct {
		input    string
		expected NodeRole
	}{
		{"standalone", RoleStandalone},
		{"writer", RoleWriter},
		{"reader", RoleReader},
		{"compactor", RoleCompactor},
		{"invalid", RoleStandalone}, // Falls back to standalone
		{"", RoleStandalone},        // Empty falls back to standalone
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseRole(tt.input); got != tt.expected {
				t.Errorf("ParseRole(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestRoleCapabilities(t *testing.T) {
	tests := []struct {
		role         NodeRole
		canIngest    bool
		canQuery     bool
		canCompact   bool
		canCoordinate bool
	}{
		{RoleStandalone, true, true, true, false},
		{RoleWriter, true, true, false, true},
		{RoleReader, false, true, false, false},
		{RoleCompactor, false, false, true, false},
		{NodeRole("invalid"), false, false, false, false}, // Unknown role has no capabilities
	}

	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			caps := tt.role.GetCapabilities()
			if caps.CanIngest != tt.canIngest {
				t.Errorf("GetCapabilities().CanIngest = %v, want %v", caps.CanIngest, tt.canIngest)
			}
			if caps.CanQuery != tt.canQuery {
				t.Errorf("GetCapabilities().CanQuery = %v, want %v", caps.CanQuery, tt.canQuery)
			}
			if caps.CanCompact != tt.canCompact {
				t.Errorf("GetCapabilities().CanCompact = %v, want %v", caps.CanCompact, tt.canCompact)
			}
			if caps.CanCoordinate != tt.canCoordinate {
				t.Errorf("GetCapabilities().CanCoordinate = %v, want %v", caps.CanCoordinate, tt.canCoordinate)
			}
		})
	}
}

func TestAllRoles(t *testing.T) {
	roles := AllRoles()
	if len(roles) != 4 {
		t.Errorf("AllRoles() returned %d roles, want 4", len(roles))
	}

	// Check all expected roles are present
	expected := map[NodeRole]bool{
		RoleStandalone: true,
		RoleWriter:     true,
		RoleReader:     true,
		RoleCompactor:  true,
	}

	for _, role := range roles {
		if !expected[role] {
			t.Errorf("AllRoles() contains unexpected role: %v", role)
		}
		delete(expected, role)
	}

	if len(expected) > 0 {
		t.Errorf("AllRoles() missing roles: %v", expected)
	}
}

func TestNodeRoleString(t *testing.T) {
	tests := []struct {
		role     NodeRole
		expected string
	}{
		{RoleStandalone, "standalone"},
		{RoleWriter, "writer"},
		{RoleReader, "reader"},
		{RoleCompactor, "compactor"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.role.String(); got != tt.expected {
				t.Errorf("NodeRole.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}
