package tiering

import "time"

// Tier represents a storage tier
type Tier string

const (
	// TierHot represents local/fast storage for recent data
	TierHot Tier = "hot"
	// TierCold represents S3/Azure archive storage for historical data
	TierCold Tier = "cold"
)

// String returns the string representation of a Tier
func (t Tier) String() string {
	return string(t)
}

// IsValid returns true if the tier is a valid tier value
func (t Tier) IsValid() bool {
	switch t {
	case TierHot, TierCold:
		return true
	default:
		return false
	}
}

// TierFromString converts a string to a Tier
func TierFromString(s string) Tier {
	switch s {
	case "hot":
		return TierHot
	case "cold":
		return TierCold
	default:
		return TierHot // Default to hot tier
	}
}

// FileMetadata represents metadata about a file in the tiering system
type FileMetadata struct {
	ID            int64     `json:"id"`
	Path          string    `json:"path"`
	Database      string    `json:"database"`
	Measurement   string    `json:"measurement"`
	PartitionTime time.Time `json:"partition_time"`
	Tier          Tier      `json:"tier"`
	SizeBytes     int64     `json:"size_bytes"`
	CreatedAt     time.Time `json:"created_at"`
	MigratedAt    *time.Time `json:"migrated_at,omitempty"`
}

// MigrationCandidate represents a file that is eligible for tier migration
type MigrationCandidate struct {
	Path          string    `json:"path"`
	Database      string    `json:"database"`
	Measurement   string    `json:"measurement"`
	PartitionTime time.Time `json:"partition_time"`
	SizeBytes     int64     `json:"size_bytes"`
	CurrentTier   Tier      `json:"current_tier"`
	TargetTier    Tier      `json:"target_tier"`
	Age           time.Duration `json:"age"`
}

// MigrationRecord represents a completed or failed migration
type MigrationRecord struct {
	ID          int64      `json:"id"`
	FilePath    string     `json:"file_path"`
	Database    string     `json:"database"`
	FromTier    Tier       `json:"from_tier"`
	ToTier      Tier       `json:"to_tier"`
	SizeBytes   int64      `json:"size_bytes"`
	StartedAt   time.Time  `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// TieredPath represents a storage path with its tier information
type TieredPath struct {
	Path    string `json:"path"`
	Tier    Tier   `json:"tier"`
	Backend string `json:"backend"` // "local", "s3", "azure"
}

// TierStats holds statistics for a single tier
type TierStats struct {
	Tier        Tier   `json:"tier"`
	Enabled     bool   `json:"enabled"`
	Backend     string `json:"backend"`
	FileCount   int64  `json:"file_count"`
	TotalSizeMB int64  `json:"total_size_mb"`
}

// StatusResponse represents the tiering status API response
type StatusResponse struct {
	Enabled      bool                  `json:"enabled"`
	LicenseValid bool                  `json:"license_valid"`
	Reason       string                `json:"reason,omitempty"`
	Tiers        map[string]TierStats  `json:"tiers,omitempty"`
	Scheduler    *SchedulerStatus      `json:"scheduler,omitempty"`
}

// SchedulerStatus represents the migration scheduler status
type SchedulerStatus struct {
	Running  bool       `json:"running"`
	Schedule string     `json:"schedule"`
	NextRun  *time.Time `json:"next_run,omitempty"`
	LastRun  *time.Time `json:"last_run,omitempty"`
}
