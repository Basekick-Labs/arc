package auth

import "time"

// Organization represents a top-level tenant in RBAC
type Organization struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Enabled     bool      `json:"enabled"`
	Teams       []Team    `json:"teams,omitempty"` // Populated on request
}

// Team represents a group within an organization
type Team struct {
	ID             int64     `json:"id"`
	OrganizationID int64     `json:"organization_id"`
	Name           string    `json:"name"`
	Description    string    `json:"description,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	Enabled        bool      `json:"enabled"`
	Roles          []Role    `json:"roles,omitempty"` // Populated on request
}

// Role represents a set of permissions for a database pattern
type Role struct {
	ID                     int64                   `json:"id"`
	TeamID                 int64                   `json:"team_id"`
	DatabasePattern        string                  `json:"database_pattern"`         // e.g., "production", "*", "analytics_*"
	Permissions            []string                `json:"permissions"`              // ["read", "write", "delete"]
	CreatedAt              time.Time               `json:"created_at"`
	MeasurementPermissions []MeasurementPermission `json:"measurement_permissions,omitempty"` // Populated on request
}

// MeasurementPermission represents granular permissions at measurement level
type MeasurementPermission struct {
	ID                 int64     `json:"id"`
	RoleID             int64     `json:"role_id"`
	MeasurementPattern string    `json:"measurement_pattern"` // e.g., "metrics_*", "events_*"
	Permissions        []string  `json:"permissions"`
	CreatedAt          time.Time `json:"created_at"`
}

// TokenMembership links a token to a team for RBAC
type TokenMembership struct {
	ID        int64     `json:"id"`
	TokenID   int64     `json:"token_id"`
	TeamID    int64     `json:"team_id"`
	CreatedAt time.Time `json:"created_at"`
}

// EffectivePermission represents resolved permissions for a specific resource
type EffectivePermission struct {
	Database    string   `json:"database"`
	Measurement string   `json:"measurement,omitempty"`
	Permissions []string `json:"permissions"`
	Source      string   `json:"source"` // "token" (OSS) or "rbac" (Enterprise)
}

// PermissionCheckRequest represents a request to check permissions
type PermissionCheckRequest struct {
	TokenInfo   *TokenInfo
	Database    string
	Measurement string
	Permission  string // "read", "write", "delete", "admin"
}

// PermissionCheckResult represents the result of a permission check
type PermissionCheckResult struct {
	Allowed bool   `json:"allowed"`
	Source  string `json:"source"` // "token", "rbac", or "denied"
	Reason  string `json:"reason,omitempty"`
}

// CreateOrganizationRequest represents a request to create an organization
type CreateOrganizationRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// UpdateOrganizationRequest represents a request to update an organization
type UpdateOrganizationRequest struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	Enabled     *bool   `json:"enabled,omitempty"`
}

// CreateTeamRequest represents a request to create a team
type CreateTeamRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// UpdateTeamRequest represents a request to update a team
type UpdateTeamRequest struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	Enabled     *bool   `json:"enabled,omitempty"`
}

// CreateRoleRequest represents a request to create a role
type CreateRoleRequest struct {
	DatabasePattern string   `json:"database_pattern"`
	Permissions     []string `json:"permissions"`
}

// UpdateRoleRequest represents a request to update a role
type UpdateRoleRequest struct {
	DatabasePattern *string  `json:"database_pattern,omitempty"`
	Permissions     []string `json:"permissions,omitempty"`
}

// CreateMeasurementPermissionRequest represents a request to create measurement permissions
type CreateMeasurementPermissionRequest struct {
	MeasurementPattern string   `json:"measurement_pattern"`
	Permissions        []string `json:"permissions"`
}

// AddTokenToTeamRequest represents a request to add a token to a team
type AddTokenToTeamRequest struct {
	TeamID int64 `json:"team_id"`
}
